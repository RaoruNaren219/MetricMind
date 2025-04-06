import asyncio
from typing import Dict, List, Any, Optional, Tuple, Set
import aiohttp
import json
from datetime import datetime
import hashlib
from functools import lru_cache
import time
import numpy as np
from dataclasses import dataclass
from enum import Enum
import os
import pathlib
import re

from metricmind.utils.config import Settings, get_settings
from metricmind.utils.logger import setup_logger
from metricmind.monitoring.system import SystemMonitor

logger = setup_logger(__name__)

class QueryStatus(Enum):
    """Enum for query execution status."""
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    RETRY = "retry"
    CANCELLED = "cancelled"

@dataclass
class QueryResult:
    """Data class for query execution results."""
    status: QueryStatus
    execution_time: float
    row_count: int
    data: List[Any]
    query_hash: str
    timestamp: str
    error: Optional[str] = None
    retry_count: int = 0
    memory_usage: Optional[float] = None
    cpu_usage: Optional[float] = None
    metrics: Optional['QueryMetrics'] = None

@dataclass
class QueryMetrics:
    """Data class for query performance metrics."""
    min_time: float
    max_time: float
    avg_time: float
    median_time: float
    std_dev: float
    success_rate: float
    row_count: int
    query_hash: str
    memory_usage: Optional[float] = None
    cpu_usage: Optional[float] = None
    execution_time: float
    data_size: int

class QueryExecutor:
    """Handles execution of TPC-DS queries against Dremio instances."""
    
    def __init__(self, settings=None):
        self.settings = settings or get_settings()
        self.logger = logger
        self.timeout = aiohttp.ClientTimeout(total=self.settings.REQUEST_TIMEOUT)
        self.max_retries = self.settings.MAX_RETRIES
        self.retry_delay = self.settings.RETRY_DELAY
        self._session = None
        self._monitor = SystemMonitor(interval=0.5)
        self._cancelled = False
        self._query_cache = {}
        
    async def __aenter__(self):
        """Create aiohttp session when entering context."""
        self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close aiohttp session when exiting context."""
        if self._session:
            await self._session.close()
            self._session = None
            
    def cancel(self):
        """Cancel ongoing operations."""
        self._cancelled = True
        self.logger.info("Query execution cancelled")
        
    async def _execute_query(
        self,
        query: str,
        instance: int,
        retry_count: int = 0
    ) -> QueryResult:
        """Execute a query on the specified Dremio instance."""
        if self._cancelled:
            return QueryResult(
                status=QueryStatus.FAILED,
                execution_time=0,
                row_count=0,
                data=[],
                query_hash=hash(query),
                timestamp=datetime.utcnow().isoformat(),
                error="Operation cancelled"
            )
            
        # Start system monitoring
        self._monitor.start()
        
        try:
            # Get instance-specific settings
            host = getattr(self.settings, f"DREMIO{instance}_HOST")
            port = getattr(self.settings, f"DREMIO{instance}_PORT")
            headers = self.settings.get_auth_headers(instance)
            
            # Prepare request
            url = f"http://{host}:{port}/api/v3/sql"
            data = {"sql": query}
            
            # Execute query with timeout
            start_time = time.time()
            async with self._session.post(
                url,
                json=data,
                headers=headers,
                timeout=self.settings.REQUEST_TIMEOUT
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    execution_time = time.time() - start_time
                    
                    # Get system metrics
                    metrics = self._monitor.get_summary()
                    
                    return QueryResult(
                        status=QueryStatus.SUCCESS,
                        execution_time=execution_time,
                        row_count=result.get("rowCount", 0),
                        data=result.get("data", []),
                        query_hash=hash(query),
                        timestamp=datetime.utcnow().isoformat(),
                        metrics=QueryMetrics(
                            execution_time=execution_time,
                            memory_usage=metrics.get("memory", {}).get("avg", 0),
                            cpu_usage=metrics.get("cpu", {}).get("avg", 0),
                            row_count=result.get("rowCount", 0),
                            data_size=len(str(result.get("data", [])))
                        )
                    )
                else:
                    error_text = await response.text()
                    raise Exception(f"Query failed with status {response.status}: {error_text}")
                    
        except asyncio.TimeoutError:
            if retry_count < self.max_retries:
                self.logger.warning(f"Query timeout, retrying ({retry_count + 1}/{self.max_retries})")
                await asyncio.sleep(self.retry_delay * (2 ** retry_count))
                return await self._execute_query(query, instance, retry_count + 1)
            return QueryResult(
                status=QueryStatus.TIMEOUT,
                execution_time=0,
                row_count=0,
                data=[],
                query_hash=hash(query),
                timestamp=datetime.utcnow().isoformat(),
                error="Query execution timed out"
            )
            
        except Exception as e:
            return QueryResult(
                status=QueryStatus.FAILED,
                execution_time=0,
                row_count=0,
                data=[],
                query_hash=hash(query),
                timestamp=datetime.utcnow().isoformat(),
                error=str(e)
            )
            
        finally:
            self._monitor.stop()
            
    async def run_benchmark(
        self,
        iterations: int = 3,
        query_filter: Optional[Set[str]] = None
    ) -> Dict:
        """Run benchmark comparison between Dremio instances."""
        results = {
            "metadata": {
                "start_time": time.time(),
                "status": "in_progress",
                "iterations": iterations
            },
            "queries": {},
            "resource_metrics": []
        }
        
        try:
            # Start system monitoring
            self._monitor.start()
            
            # Load queries
            queries_dir = pathlib.Path(self.settings.TPC_DS_QUERIES_DIR)
            query_files = list(queries_dir.glob("*.sql"))
            
            if not query_files:
                self.logger.warning("No query files found, using sample query")
                query_files = [pathlib.Path("sample.sql")]
                query_files[0].write_text("SELECT 1")
                
            # Filter queries if specified
            if query_filter:
                query_files = [f for f in query_files if f.name in query_filter]
                
            for query_file in query_files:
                query = query_file.read_text()
                query_results = {
                    "dremio1": [],
                    "dremio2": []
                }
                
                for _ in range(iterations):
                    if self._cancelled:
                        results["metadata"]["status"] = "cancelled"
                        return results
                        
                    # Execute on both instances
                    dremio1_result = await self._execute_query(query, 1)
                    dremio2_result = await self._execute_query(query, 2)
                    
                    query_results["dremio1"].append(dremio1_result)
                    query_results["dremio2"].append(dremio2_result)
                    
                # Calculate statistics
                results["queries"][query_file.name] = {
                    "dremio1": self._calculate_stats(query_results["dremio1"]),
                    "dremio2": self._calculate_stats(query_results["dremio2"])
                }
                
            # Calculate overall statistics
            results["metadata"].update({
                "status": "completed",
                "end_time": time.time(),
                "overall": self._calculate_overall_stats(results["queries"])
            })
            
        except Exception as e:
            self.logger.error(f"Benchmark failed: {e}")
            results["metadata"].update({
                "status": "failed",
                "error": str(e)
            })
            
        finally:
            self._monitor.stop()
            results["resource_metrics"] = self._monitor.get_metrics()
            
        return results
        
    def _calculate_stats(self, results: List[QueryResult]) -> Dict:
        """Calculate statistics for a list of query results."""
        if not results:
            return {}
            
        execution_times = [r.execution_time for r in results if r.status == QueryStatus.SUCCESS]
        memory_usage = [r.metrics.memory_usage for r in results if r.metrics]
        cpu_usage = [r.metrics.cpu_usage for r in results if r.metrics]
        
        return {
            "avg_execution_time": np.mean(execution_times) if execution_times else 0,
            "min_execution_time": min(execution_times) if execution_times else 0,
            "max_execution_time": max(execution_times) if execution_times else 0,
            "median_execution_time": np.median(execution_times) if execution_times else 0,
            "std_execution_time": np.std(execution_times) if execution_times else 0,
            "avg_memory_usage": np.mean(memory_usage) if memory_usage else 0,
            "avg_cpu_usage": np.mean(cpu_usage) if cpu_usage else 0,
            "success_rate": len(execution_times) / len(results),
            "error": next((r.error for r in results if r.error), None)
        }
        
    def _calculate_overall_stats(self, queries: Dict) -> Dict:
        """Calculate overall benchmark statistics."""
        ratios = []
        success_rates = []
        
        for query_data in queries.values():
            if "dremio1" in query_data and "dremio2" in query_data:
                d1_time = query_data["dremio1"].get("avg_execution_time", 0)
                d2_time = query_data["dremio2"].get("avg_execution_time", 0)
                
                if d1_time > 0:
                    ratios.append(d2_time / d1_time)
                    
                success_rates.append(
                    (query_data["dremio1"].get("success_rate", 0) +
                     query_data["dremio2"].get("success_rate", 0)) / 2
                )
                
        return {
            "avg_performance_ratio": np.mean(ratios) if ratios else 0,
            "median_performance_ratio": np.median(ratios) if ratios else 0,
            "success_rate": np.mean(success_rates) if success_rates else 0
        }
        
    @lru_cache(maxsize=1)
    def _load_tpcds_queries(self) -> Dict[str, str]:
        """Load TPC-DS queries from file with caching."""
        queries = {}
        
        # Check if queries directory exists
        queries_dir = pathlib.Path("queries")
        if not queries_dir.exists():
            self.logger.warning("Queries directory not found, using sample query")
            return self._get_sample_query()
            
        # Load queries from files
        for query_file in queries_dir.glob("*.sql"):
            query_name = query_file.stem
            with open(query_file, "r") as f:
                queries[query_name] = f.read()
                
        if not queries:
            self.logger.warning("No queries found in queries directory, using sample query")
            return self._get_sample_query()
            
        self.logger.info(f"Loaded {len(queries)} queries from {queries_dir}")
        return queries
        
    def _get_sample_query(self) -> Dict[str, str]:
        """Return a sample TPC-DS query."""
        return {
            "q1": """
            SELECT 
                l_returnflag,
                l_linestatus,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc,
                count(*) as count_order
            FROM
                lineitem
            WHERE
                l_shipdate <= date '1998-12-01' - interval '90' day
            GROUP BY
                l_returnflag,
                l_linestatus
            ORDER BY
                l_returnflag,
                l_linestatus;
            """
        } 