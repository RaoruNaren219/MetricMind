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

from metricmind.utils.config import Settings
from metricmind.utils.logger import setup_logger

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

class QueryExecutor:
    """Handles execution of TPC-DS queries against Dremio instances."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.logger = logger
        self.timeout = aiohttp.ClientTimeout(total=settings.REQUEST_TIMEOUT)
        self.max_retries = settings.MAX_RETRIES
        self.retry_delay = settings.RETRY_DELAY
        self._session = None
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
        dremio_host: str,
        dremio_port: int,
        username: str,
        password: Optional[str] = None,
        pat_token: Optional[str] = None,
        retry_count: int = 0
    ) -> QueryResult:
        """Execute a single query against a Dremio instance with retries."""
        if self._cancelled:
            return QueryResult(
                status=QueryStatus.CANCELLED,
                execution_time=0,
                row_count=0,
                data=[],
                query_hash=hashlib.md5(query.encode()).hexdigest(),
                timestamp=datetime.utcnow().isoformat(),
                error="Query execution cancelled",
                retry_count=retry_count
            )
            
        base_url = f"http://{dremio_host}:{dremio_port}"
        
        # Prepare authentication
        auth = None
        if pat_token:
            auth = aiohttp.BasicAuth(username, pat_token)
        elif password:
            auth = aiohttp.BasicAuth(username, password)
            
        # Calculate query hash for caching
        query_hash = hashlib.md5(query.encode()).hexdigest()
        
        # Check cache for identical queries
        cache_key = f"{query_hash}_{dremio_host}_{dremio_port}"
        if cache_key in self._query_cache:
            self.logger.info(f"Using cached result for query {query_hash}")
            cached_result = self._query_cache[cache_key]
            return QueryResult(
                status=cached_result.status,
                execution_time=cached_result.execution_time,
                row_count=cached_result.row_count,
                data=cached_result.data,
                query_hash=query_hash,
                timestamp=datetime.utcnow().isoformat(),
                retry_count=retry_count
            )
        
        try:
            # Use existing session or create a new one
            session = self._session or aiohttp.ClientSession(auth=auth, timeout=self.timeout)
            should_close = self._session is None
            
            try:
                start_time = time.time()
                
                # Submit query
                async with session.post(
                    f"{base_url}/api/v3/sql",
                    json={"sql": query}
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"Query execution failed: {error_text}")
                        
                    result = await response.json()
                    execution_time = time.time() - start_time
                    
                    # Extract resource usage if available
                    memory_usage = result.get("memoryUsage", None)
                    cpu_usage = result.get("cpuUsage", None)
                    
                    query_result = QueryResult(
                        status=QueryStatus.SUCCESS,
                        execution_time=execution_time,
                        row_count=result.get("rowCount", 0),
                        data=result.get("data", []),
                        query_hash=query_hash,
                        timestamp=datetime.utcnow().isoformat(),
                        retry_count=retry_count,
                        memory_usage=memory_usage,
                        cpu_usage=cpu_usage
                    )
                    
                    # Cache successful results
                    if query_result.status == QueryStatus.SUCCESS:
                        self._query_cache[cache_key] = query_result
                        
                    return query_result
            finally:
                if should_close and session:
                    await session.close()
                    
        except asyncio.TimeoutError:
            if retry_count < self.max_retries:
                self.logger.warning(f"Query execution timed out, retrying ({retry_count + 1}/{self.max_retries})")
                await asyncio.sleep(self.retry_delay * (2 ** retry_count))  # Exponential backoff
                return await self._execute_query(
                    query, dremio_host, dremio_port, username, password, pat_token, retry_count + 1
                )
            return QueryResult(
                status=QueryStatus.TIMEOUT,
                execution_time=0,
                row_count=0,
                data=[],
                query_hash=query_hash,
                timestamp=datetime.utcnow().isoformat(),
                error="Query execution timed out after maximum retries",
                retry_count=retry_count
            )
                
        except Exception as e:
            if retry_count < self.max_retries:
                self.logger.warning(f"Query execution failed, retrying ({retry_count + 1}/{self.max_retries}): {str(e)}")
                await asyncio.sleep(self.retry_delay * (2 ** retry_count))  # Exponential backoff
                return await self._execute_query(
                    query, dremio_host, dremio_port, username, password, pat_token, retry_count + 1
                )
            return QueryResult(
                status=QueryStatus.FAILED,
                execution_time=0,
                row_count=0,
                data=[],
                query_hash=query_hash,
                timestamp=datetime.utcnow().isoformat(),
                error=str(e),
                retry_count=retry_count
            )
                
    async def run_benchmark(self, iterations: int = 1, query_filter: Optional[Set[str]] = None) -> Dict[str, Any]:
        """Run benchmark comparison between two Dremio instances."""
        self.logger.info(f"Starting benchmark comparison with {iterations} iterations")
        
        # Load TPC-DS queries
        queries = self._load_tpcds_queries()
        
        # Filter queries if specified
        if query_filter:
            queries = {k: v for k, v in queries.items() if k in query_filter}
            self.logger.info(f"Filtered to {len(queries)} queries: {', '.join(queries.keys())}")
        
        results = {
            "dremio1": {},
            "dremio2": {},
            "comparison": {},
            "metadata": {
                "start_time": datetime.utcnow().isoformat(),
                "query_count": len(queries),
                "iterations": iterations,
                "filtered_queries": list(query_filter) if query_filter else None
            }
        }
        
        # Execute queries against both instances
        for query_name, query in queries.items():
            if self._cancelled:
                self.logger.info("Benchmark cancelled")
                results["metadata"]["status"] = "cancelled"
                results["metadata"]["end_time"] = datetime.utcnow().isoformat()
                return results
                
            self.logger.info(f"Executing query: {query_name}")
            
            # Run multiple iterations for each query
            dremio1_results = []
            dremio2_results = []
            
            for i in range(iterations):
                if self._cancelled:
                    self.logger.info("Benchmark cancelled")
                    results["metadata"]["status"] = "cancelled"
                    results["metadata"]["end_time"] = datetime.utcnow().isoformat()
                    return results
                    
                self.logger.info(f"Running iteration {i+1}/{iterations} for query {query_name}")
                
                try:
                    # Execute on Dremio 1
                    dremio1_result = await self._execute_query(
                        query,
                        self.settings.DREMIO1_HOST,
                        self.settings.DREMIO1_PORT,
                        self.settings.DREMIO1_USERNAME,
                        self.settings.DREMIO1_PASSWORD,
                        self.settings.DREMIO1_PAT_TOKEN
                    )
                    dremio1_results.append(dremio1_result)
                    
                    # Execute on Dremio 2
                    dremio2_result = await self._execute_query(
                        query,
                        self.settings.DREMIO2_HOST,
                        self.settings.DREMIO2_PORT,
                        self.settings.DREMIO2_USERNAME,
                        self.settings.DREMIO2_PASSWORD,
                        self.settings.DREMIO2_PAT_TOKEN
                    )
                    dremio2_results.append(dremio2_result)
                    
                except Exception as e:
                    self.logger.error(f"Error executing query {query_name} in iteration {i+1}: {str(e)}")
            
            # Calculate statistics for successful runs
            successful_dremio1 = [r for r in dremio1_results if r.status == QueryStatus.SUCCESS]
            successful_dremio2 = [r for r in dremio2_results if r.status == QueryStatus.SUCCESS]
            
            if successful_dremio1 and successful_dremio2:
                # Calculate execution time statistics
                dremio1_times = [r.execution_time for r in successful_dremio1]
                dremio2_times = [r.execution_time for r in successful_dremio2]
                
                # Calculate resource usage if available
                dremio1_memory = [r.memory_usage for r in successful_dremio1 if r.memory_usage is not None]
                dremio2_memory = [r.memory_usage for r in successful_dremio2 if r.memory_usage is not None]
                
                dremio1_cpu = [r.cpu_usage for r in successful_dremio1 if r.cpu_usage is not None]
                dremio2_cpu = [r.cpu_usage for r in successful_dremio2 if r.cpu_usage is not None]
                
                # Store results
                results["dremio1"][query_name] = QueryMetrics(
                    min_time=min(dremio1_times),
                    max_time=max(dremio1_times),
                    avg_time=np.mean(dremio1_times),
                    median_time=np.median(dremio1_times),
                    std_dev=np.std(dremio1_times),
                    success_rate=len(successful_dremio1) / iterations,
                    row_count=successful_dremio1[0].row_count,
                    query_hash=successful_dremio1[0].query_hash,
                    memory_usage=np.mean(dremio1_memory) if dremio1_memory else None,
                    cpu_usage=np.mean(dremio1_cpu) if dremio1_cpu else None
                )
                
                results["dremio2"][query_name] = QueryMetrics(
                    min_time=min(dremio2_times),
                    max_time=max(dremio2_times),
                    avg_time=np.mean(dremio2_times),
                    median_time=np.median(dremio2_times),
                    std_dev=np.std(dremio2_times),
                    success_rate=len(successful_dremio2) / iterations,
                    row_count=successful_dremio2[0].row_count,
                    query_hash=successful_dremio2[0].query_hash,
                    memory_usage=np.mean(dremio2_memory) if dremio2_memory else None,
                    cpu_usage=np.mean(dremio2_cpu) if dremio2_cpu else None
                )
                
                # Calculate comparison metrics
                results["comparison"][query_name] = {
                    "execution_time_diff": (
                        results["dremio2"][query_name].median_time - 
                        results["dremio1"][query_name].median_time
                    ),
                    "performance_ratio": (
                        results["dremio1"][query_name].median_time / 
                        results["dremio2"][query_name].median_time
                        if results["dremio2"][query_name].median_time > 0 else float('inf')
                    ),
                    "row_count_diff": (
                        results["dremio2"][query_name].row_count - 
                        results["dremio1"][query_name].row_count
                    ),
                    "data_consistency": (
                        results["dremio1"][query_name].query_hash == 
                        results["dremio2"][query_name].query_hash
                    ),
                    "stability_dremio1": results["dremio1"][query_name].std_dev / results["dremio1"][query_name].avg_time if results["dremio1"][query_name].avg_time > 0 else float('inf'),
                    "stability_dremio2": results["dremio2"][query_name].std_dev / results["dremio2"][query_name].avg_time if results["dremio2"][query_name].avg_time > 0 else float('inf'),
                    "memory_usage_ratio": (
                        results["dremio1"][query_name].memory_usage / 
                        results["dremio2"][query_name].memory_usage
                        if results["dremio1"][query_name].memory_usage and results["dremio2"][query_name].memory_usage and results["dremio2"][query_name].memory_usage > 0 else None
                    ),
                    "cpu_usage_ratio": (
                        results["dremio1"][query_name].cpu_usage / 
                        results["dremio2"][query_name].cpu_usage
                        if results["dremio1"][query_name].cpu_usage and results["dremio2"][query_name].cpu_usage and results["dremio2"][query_name].cpu_usage > 0 else None
                    )
                }
            else:
                # Handle case where all runs failed
                results["comparison"][query_name] = {
                    "error": "All query executions failed",
                    "status": "failed",
                    "dremio1_success_rate": len(successful_dremio1) / iterations if dremio1_results else 0,
                    "dremio2_success_rate": len(successful_dremio2) / iterations if dremio2_results else 0
                }
            
        # Calculate overall statistics
        successful_queries = [q for q, r in results["comparison"].items() if "error" not in r]
        if successful_queries:
            results["metadata"]["overall"] = {
                "avg_performance_ratio": np.mean([
                    results["comparison"][q]["performance_ratio"] 
                    for q in successful_queries
                ]),
                "median_performance_ratio": np.median([
                    results["comparison"][q]["performance_ratio"] 
                    for q in successful_queries
                ]),
                "success_rate": len(successful_queries) / len(queries),
                "avg_stability_dremio1": np.mean([
                    results["comparison"][q]["stability_dremio1"] 
                    for q in successful_queries
                ]),
                "avg_stability_dremio2": np.mean([
                    results["comparison"][q]["stability_dremio2"] 
                    for q in successful_queries
                ])
            }
            
        results["metadata"]["status"] = "cancelled" if self._cancelled else "completed"
        results["metadata"]["end_time"] = datetime.utcnow().isoformat()
        self.logger.info("Benchmark comparison completed")
        return results
        
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