import asyncio
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import httpx
from pydantic import BaseModel, Field

from metricmind.utils.config import get_settings
from metricmind.utils.logger import setup_logger
from metricmind.monitoring.system import SystemMonitor

logger = setup_logger(__name__)

class QueryStatus:
    """Query execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class QueryMetrics(BaseModel):
    """Query execution metrics."""
    execution_time: float = Field(default=0.0)
    memory_usage: float = Field(default=0.0)
    cpu_usage: float = Field(default=0.0)
    rows_processed: int = Field(default=0)
    bytes_processed: int = Field(default=0)

class QueryResult(BaseModel):
    """Query execution result."""
    status: str = Field(default=QueryStatus.PENDING)
    metrics: QueryMetrics = Field(default_factory=QueryMetrics)
    error: Optional[str] = Field(default=None)
    data: Optional[List[Dict]] = Field(default=None)
    query_hash: Optional[str] = Field(default=None)
    timestamp: Optional[datetime] = Field(default=None)

class QueryExecutor:
    """Executes queries and collects metrics."""
    
    def __init__(
        self,
        queries_dir: Path,
        output_dir: Path,
        iterations: int = 3,
        settings: Optional[Dict] = None
    ):
        self.queries_dir = queries_dir
        self.output_dir = output_dir
        self.iterations = iterations
        self.settings = settings or get_settings()
        self.logger = logger
        self._client = None
        self._monitor = SystemMonitor(interval=0.5)
        self._cache = {}
        self._cancelled = False
        
    async def __aenter__(self):
        """Initialize HTTP client."""
        self._client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            
    def cancel(self):
        """Cancel ongoing operations."""
        self._cancelled = True
        logger.info("Cancelling ongoing operations")
        
    async def run_benchmark(self) -> Dict[str, Any]:
        """Run benchmark comparison."""
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            
            results = {
                "queries": {},
                "resource_metrics": [],
                "metadata": {
                    "start_time": datetime.now().isoformat(),
                    "end_time": None,
                    "status": "running",
                    "overall": {}
                }
            }
            
            # Execute queries
            query_files = list(self.queries_dir.glob("*.sql"))
            for query_file in query_files:
                query_name = query_file.stem
                self.logger.info(f"Executing query: {query_name}")
                
                with open(query_file) as f:
                    query = f.read()
                    
                query_results = await self._execute_query(query, query_name)
                results["queries"][query_name] = query_results
                
            # Calculate overall metrics
            results["metadata"]["end_time"] = datetime.now().isoformat()
            results["metadata"]["status"] = "completed"
            results["metadata"]["overall"] = self._calculate_statistics(results["queries"])
            
            # Save results
            self._save_results(results)
            
            return results
        except Exception as e:
            self.logger.error(f"Benchmark failed: {str(e)}")
            raise
            
    async def _execute_query(
        self,
        query: str,
        query_name: str,
        retries: int = 3
    ) -> Dict[str, QueryResult]:
        """Execute query on both Dremio instances."""
        results = {}
        
        for instance in ["dremio1", "dremio2"]:
            result = QueryResult(
                status=QueryStatus.PENDING,
                query_hash=hashlib.md5(query.encode()).hexdigest(),
                timestamp=datetime.now()
            )
            
            try:
                for i in range(self.iterations):
                    start_time = time.time()
                    
                    # Execute query
                    response = await self._client.post(
                        f"{getattr(self.settings, f'{instance}_url')}/api/v3/sql",
                        json={"query": query},
                        headers=self.settings.get_auth_headers(1 if instance == "dremio1" else 2)
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        execution_time = time.time() - start_time
                        
                        result.metrics.execution_time += execution_time
                        result.metrics.rows_processed += len(data.get("rows", []))
                        result.metrics.bytes_processed += len(response.content)
                        
                        if i == 0:  # Store data only for first iteration
                            result.data = data.get("rows", [])
                    else:
                        raise Exception(f"Query failed with status {response.status_code}")
                        
                # Calculate averages
                result.metrics.execution_time /= self.iterations
                result.metrics.rows_processed //= self.iterations
                result.metrics.bytes_processed //= self.iterations
                result.status = QueryStatus.COMPLETED
                
            except Exception as e:
                self.logger.error(f"Query execution failed: {str(e)}")
                result.status = QueryStatus.FAILED
                result.error = str(e)
                
            results[instance] = result
            
        return results
        
    def _calculate_statistics(self, query_results: Dict[str, Dict[str, QueryResult]]) -> Dict[str, float]:
        """Calculate overall statistics from query results."""
        performance_ratios = []
        success_count = 0
        total_queries = len(query_results)
        
        for query_name, results in query_results.items():
            if "dremio1" in results and "dremio2" in results:
                d1_result = results["dremio1"]
                d2_result = results["dremio2"]
                
                if d1_result.status == QueryStatus.COMPLETED and d2_result.status == QueryStatus.COMPLETED:
                    ratio = d2_result.metrics.execution_time / d1_result.metrics.execution_time
                    performance_ratios.append(ratio)
                    success_count += 1
                    
        return {
            "avg_performance_ratio": sum(performance_ratios) / len(performance_ratios) if performance_ratios else 0,
            "median_performance_ratio": sorted(performance_ratios)[len(performance_ratios)//2] if performance_ratios else 0,
            "success_rate": success_count / total_queries if total_queries > 0 else 0
        }
        
    def _save_results(self, results: Dict[str, Any]) -> None:
        """Save benchmark results to file."""
        output_file = self.output_dir / f"benchmark_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)
            
        self.logger.info(f"Results saved to {output_file}")

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
            async with self._client.post(
                url,
                json=data,
                headers=headers,
                timeout=self.settings.REQUEST_TIMEOUT
            ) as response:
                if response.status_code == 200:
                    result = await response.json()
                    execution_time = time.time() - start_time
                    
                    # Get system metrics
                    metrics = self._monitor.get_summary()
                    
                    return QueryResult(
                        status=QueryStatus.COMPLETED,
                        execution_time=execution_time,
                        row_count=result.get("rowCount", 0),
                        data=result.get("data", []),
                        query_hash=hash(query),
                        timestamp=datetime.utcnow().isoformat(),
                        metrics=QueryMetrics(
                            execution_time=execution_time,
                            memory_usage=metrics.get("memory", {}).get("avg", 0),
                            cpu_usage=metrics.get("cpu", {}).get("avg", 0),
                            rows_processed=result.get("rowCount", 0),
                            bytes_processed=len(str(result.get("data", [])))
                        )
                    )
                else:
                    error_text = await response.text()
                    raise Exception(f"Query failed with status {response.status_code}: {error_text}")
                    
        except asyncio.TimeoutError:
            if retry_count < self.settings.MAX_RETRIES:
                logger.warning(f"Query timeout, retrying ({retry_count + 1}/{self.settings.MAX_RETRIES})")
                await asyncio.sleep(self.settings.RETRY_DELAY * (2 ** retry_count))
                return await self._execute_query(query, instance, retry_count + 1)
            return QueryResult(
                status=QueryStatus.FAILED,
                execution_time=0,
                row_count=0,
                error="Query execution timed out"
            )
            
        except Exception as e:
            return QueryResult(
                status=QueryStatus.FAILED,
                execution_time=0,
                row_count=0,
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
            queries_dir = Path(self.settings.TPC_DS_QUERIES_DIR)
            query_files = list(queries_dir.glob("*.sql"))
            
            if not query_files:
                logger.warning("No query files found, using sample query")
                query_files = [Path("sample.sql")]
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
            logger.error(f"Benchmark failed: {e}")
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
            
        execution_times = [r.execution_time for r in results if r.status == QueryStatus.COMPLETED]
        memory_usage = [r.metrics.memory_usage for r in results if r.metrics]
        cpu_usage = [r.metrics.cpu_usage for r in results if r.metrics]
        
        return {
            "avg_execution_time": float(np.mean(execution_times)) if execution_times else 0,
            "min_execution_time": float(min(execution_times)) if execution_times else 0,
            "max_execution_time": float(max(execution_times)) if execution_times else 0,
            "median_execution_time": float(np.median(execution_times)) if execution_times else 0,
            "std_execution_time": float(np.std(execution_times)) if execution_times else 0,
            "avg_memory_usage": float(np.mean(memory_usage)) if memory_usage else 0,
            "avg_cpu_usage": float(np.mean(cpu_usage)) if cpu_usage else 0,
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
            "avg_performance_ratio": float(np.mean(ratios)) if ratios else 0,
            "median_performance_ratio": float(np.median(ratios)) if ratios else 0,
            "success_rate": float(np.mean(success_rates)) if success_rates else 0
        } 