from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status
from pydantic import BaseModel, Field
import time
import json
from typing import Dict, List, Optional, Any, Union
import requests
from datadog import DogStatsd
import os
from dotenv import load_dotenv
import logging
from requests.exceptions import RequestException, Timeout, ConnectionError
import numpy as np
from datetime import datetime
import hashlib
import asyncio
from functools import lru_cache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("metricmind.log")
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="MetricMind", 
    description="Dremio Query Benchmarking System",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configuration
class Config:
    """Application configuration"""
    DREMIO1_HOST = os.getenv('DREMIO1_HOST', 'http://localhost:9047')
    DREMIO2_HOST = os.getenv('DREMIO2_HOST', 'http://localhost:9047')
    DREMIO1_PORT = os.getenv('DREMIO1_PORT', '9047')
    DREMIO2_PORT = os.getenv('DREMIO2_PORT', '9047')
    DREMIO1_USERNAME = os.getenv('DREMIO1_USERNAME')
    DREMIO2_USERNAME = os.getenv('DREMIO2_USERNAME')
    DREMIO1_PASSWORD = os.getenv('DREMIO1_PASSWORD')
    DREMIO2_PASSWORD = os.getenv('DREMIO2_PASSWORD')
    DREMIO1_TOKEN = os.getenv('DREMIO1_TOKEN')
    DREMIO1_AUTH_TYPE = os.getenv('DREMIO1_AUTH_TYPE', 'token')
    DREMIO2_AUTH_TYPE = os.getenv('DREMIO2_AUTH_TYPE', 'password')
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '1'))
    DD_HOST = os.getenv('DD_HOST', 'localhost')
    DD_PORT = int(os.getenv('DD_PORT', 8125))
    DD_ENABLED = os.getenv('DD_ENABLED', 'true').lower() == 'true'

# Initialize Datadog client with error handling
@lru_cache()
def get_datadog_client():
    """Get or create Datadog client with error handling"""
    if not Config.DD_ENABLED:
        logger.info("Datadog metrics disabled")
        return None
        
    try:
        client = DogStatsd(host=Config.DD_HOST, port=Config.DD_PORT)
        logger.info(f"Datadog client initialized: {Config.DD_HOST}:{Config.DD_PORT}")
        return client
    except Exception as e:
        logger.warning(f"Failed to initialize Datadog client: {e}")
        return None

# Query definitions
QUERIES = {
    "simple": """
    SELECT i_item_id, i_item_desc 
    FROM dremio2_source.item 
    WHERE i_current_price > 50
    """,
    "complex": """
    SELECT c_customer_id, SUM(ws_sales_price)
    FROM dremio2_source.web_sales ws
    JOIN dremio2_source.customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
    GROUP BY c_customer_id
    ORDER BY SUM(ws_sales_price) DESC
    LIMIT 10
    """,
    "custom": None  # Will be set by the user
}

# Models
class QueryRequest(BaseModel):
    """Request model for custom queries"""
    query: str = Field(..., description="SQL query to execute")
    source: str = Field("dremio1", description="Dremio source to use (dremio1 or dremio2)")

class QueryResponse(BaseModel):
    """Response model for query execution"""
    execution_time: float = Field(..., description="Query execution time in seconds")
    success: bool = Field(..., description="Whether the query executed successfully")
    error_message: Optional[str] = Field(None, description="Error message if query failed")
    result: Optional[List[Dict]] = Field(None, description="Query results")
    row_count: Optional[int] = Field(None, description="Number of rows in the result")
    payload_size: Optional[int] = Field(None, description="Size of the response payload in bytes")
    query_hash: Optional[str] = Field(None, description="Hash of the query result for comparison")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat(), description="Timestamp of the query execution")

class ComparisonResult(BaseModel):
    """Response model for query comparison"""
    dremio1: QueryResponse = Field(..., description="Query result from Dremio 1")
    dremio2: QueryResponse = Field(..., description="Query result from Dremio 2")
    match: bool = Field(..., description="Whether the results match")
    row_count_diff: Optional[int] = Field(None, description="Difference in row counts")
    column_diff: Optional[List[str]] = Field(None, description="Differences in columns")
    value_diff: Optional[Dict[str, List[Dict]]] = Field(None, description="Differences in values")
    similarity_score: Optional[float] = Field(None, description="Similarity score between results")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat(), description="Timestamp of the comparison")

class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str = Field(..., description="Overall health status")
    dremio1: Dict[str, Any] = Field(..., description="Health status of Dremio 1")
    dremio2: Dict[str, Any] = Field(..., description="Health status of Dremio 2")
    version: str = Field(..., description="API version")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat(), description="Timestamp of the health check")

# Utility functions
def send_metric(name: str, value: float, tags: List[str] = None):
    """Safely send metric to Datadog with error handling"""
    client = get_datadog_client()
    if client:
        try:
            client.gauge(name, value, tags=tags)
        except Exception as e:
            logger.warning(f"Failed to send metric {name} to Datadog: {e}")

def get_dremio_config(source: str) -> Dict[str, str]:
    """Get Dremio configuration for the specified source"""
    if source == "dremio1":
        return {
            "host": f"{Config.DREMIO1_HOST}:{Config.DREMIO1_PORT}",
            "username": Config.DREMIO1_USERNAME,
            "password": Config.DREMIO1_PASSWORD,
            "token": Config.DREMIO1_TOKEN,
            "auth_type": Config.DREMIO1_AUTH_TYPE
        }
    elif source == "dremio2":
        return {
            "host": f"{Config.DREMIO2_HOST}:{Config.DREMIO2_PORT}",
            "username": Config.DREMIO2_USERNAME,
            "password": Config.DREMIO2_PASSWORD,
            "auth_type": Config.DREMIO2_AUTH_TYPE
        }
    else:
        raise ValueError(f"Invalid Dremio source: {source}")

async def execute_query(query: str, source: str = "dremio1") -> QueryResponse:
    """
    Execute a query on the specified Dremio source and measure performance.
    
    Args:
        query: SQL query to execute
        source: Dremio source to use (dremio1 or dremio2)
        
    Returns:
        QueryResponse with execution metrics
    """
    start_time = time.time()
    retries = 0
    
    # Get Dremio configuration
    dremio_config = get_dremio_config(source)
    
    while retries < Config.MAX_RETRIES:
        try:
            # Prepare headers based on authentication type
            if dremio_config['auth_type'] == 'token':
                headers = {
                    "Authorization": f"Bearer {dremio_config['token']}",
                    "Content-Type": "application/json"
                }
            else:  # password authentication
                headers = {
                    "Content-Type": "application/json"
                }
                auth = (dremio_config['username'], dremio_config['password'])
            
            # Use asyncio to run the request in a non-blocking way
            response = await asyncio.to_thread(
                requests.post,
                f"{dremio_config['host']}/api/v3/sql",
                headers=headers,
                json={"sql": query},
                auth=auth if dremio_config['auth_type'] == 'password' else None,
                timeout=Config.REQUEST_TIMEOUT
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"Dremio API error: {response.text}"
                )
                
            result = response.json()
            execution_time = time.time() - start_time
            payload_size = len(response.content)
            
            # Calculate query hash for comparison
            query_hash = hashlib.md5(
                json.dumps(result.get('data', []), sort_keys=True).encode()
            ).hexdigest()
            
            # Send metrics to Datadog
            send_metric('query.execution_time', execution_time * 1000, [f'source:{source}'])
            send_metric('query.success', 1, [f'source:{source}'])
            send_metric('query.payload_size', payload_size, [f'source:{source}'])
            
            return QueryResponse(
                execution_time=execution_time,
                success=True,
                result=result.get('data', []),
                row_count=len(result.get('data', [])),
                payload_size=payload_size,
                query_hash=query_hash
            )
            
        except (RequestException, Timeout, ConnectionError) as e:
            retries += 1
            if retries == Config.MAX_RETRIES:
                logger.error(f"Failed to execute query after {Config.MAX_RETRIES} retries: {e}")
                return QueryResponse(
                    execution_time=time.time() - start_time,
                    success=False,
                    error_message=str(e)
                )
            await asyncio.sleep(Config.RETRY_DELAY)
            
        except Exception as e:
            logger.error(f"Unexpected error executing query: {e}")
            return QueryResponse(
                execution_time=time.time() - start_time,
                success=False,
                error_message=str(e)
            )

async def compare_results(dremio1_result: QueryResponse, dremio2_result: QueryResponse) -> ComparisonResult:
    """
    Compare results from both Dremio instances and identify differences.
    
    Args:
        dremio1_result: Query result from Dremio 1
        dremio2_result: Query result from Dremio 2
        
    Returns:
        ComparisonResult with detailed comparison
    """
    # Check if both queries were successful
    if not dremio1_result.success or not dremio2_result.success:
        return ComparisonResult(
            dremio1=dremio1_result,
            dremio2=dremio2_result,
            match=False
        )
    
    # Compare row counts
    row_count_match = dremio1_result.row_count == dremio2_result.row_count
    row_count_diff = abs(dremio1_result.row_count - dremio2_result.row_count) if not row_count_match else 0
    
    # Compare columns if data is available
    column_diff = []
    value_diff = {}
    
    if dremio1_result.result and dremio2_result.result:
        d1_columns = set(dremio1_result.result[0].keys()) if dremio1_result.result else set()
        d2_columns = set(dremio2_result.result[0].keys()) if dremio2_result.result else set()
        column_diff = list(d1_columns.symmetric_difference(d2_columns))
        
        # Compare values for matching columns
        common_columns = d1_columns.intersection(d2_columns)
        for col in common_columns:
            try:
                d1_values = [row[col] for row in dremio1_result.result]
                d2_values = [row[col] for row in dremio2_result.result]
                
                # Convert to numpy arrays for comparison
                d1_arr = np.array(d1_values, dtype=float)
                d2_arr = np.array(d2_values, dtype=float)
                
                # Calculate differences
                if len(d1_arr) > 0 and len(d2_arr) > 0:
                    diff_indices = np.where(np.abs(d1_arr - d2_arr) > 1e-10)[0]
                    if len(diff_indices) > 0:
                        value_diff[col] = [
                            {"index": int(i), "dremio1": float(d1_arr[i]), "dremio2": float(d2_arr[i])}
                            for i in diff_indices
                        ]
            except (ValueError, TypeError):
                # Handle non-numeric columns
                if d1_values != d2_values:
                    value_diff[col] = [
                        {"index": i, "dremio1": d1_values[i], "dremio2": d2_values[i]}
                        for i in range(min(len(d1_values), len(d2_values)))
                        if d1_values[i] != d2_values[i]
                    ]
    
    # Calculate similarity score
    similarity_score = None
    if dremio1_result.query_hash and dremio2_result.query_hash:
        similarity_score = 1.0 if dremio1_result.query_hash == dremio2_result.query_hash else 0.0
    
    # Determine if results match
    match = (row_count_match and 
            len(column_diff) == 0 and 
            len(value_diff) == 0 and 
            (similarity_score is None or similarity_score == 1.0))
    
    # Log comparison results
    if not match:
        logger.warning(
            f"Results do not match: Row count diff={row_count_diff}, "
            f"Column diff={column_diff}, Value diff={value_diff}"
        )
    
    return ComparisonResult(
        dremio1=dremio1_result,
        dremio2=dremio2_result,
        match=match,
        row_count_diff=row_count_diff if not row_count_match else None,
        column_diff=column_diff if column_diff else None,
        value_diff=value_diff if value_diff else None,
        similarity_score=similarity_score
    )

# API endpoints
@app.get("/query/simple", response_model=QueryResponse, summary="Execute simple query")
async def run_simple_query() -> QueryResponse:
    """
    Execute the simple query on Dremio 2 via Dremio 1.
    """
    logger.info("Executing simple query")
    return await execute_query(QUERIES["simple"])

@app.get("/query/complex", response_model=QueryResponse, summary="Execute complex query")
async def run_complex_query() -> QueryResponse:
    """
    Execute the complex query on Dremio 2 via Dremio 1.
    """
    logger.info("Executing complex query")
    return await execute_query(QUERIES["complex"])

@app.post("/query/custom", response_model=QueryResponse, summary="Execute custom query")
async def run_custom_query(query_request: QueryRequest) -> QueryResponse:
    """
    Execute a custom query on the specified Dremio source.
    """
    logger.info(f"Executing custom query on {query_request.source}")
    return await execute_query(query_request.query, query_request.source)

@app.get("/compare/query", response_model=ComparisonResult, summary="Compare query execution")
async def compare_queries() -> ComparisonResult:
    """
    Execute the same query on both local and remote Dremio instances and compare results.
    """
    logger.info("Comparing query execution between Dremio instances")
    
    # Execute query on both Dremio instances concurrently
    dremio1_task = asyncio.create_task(execute_query(QUERIES["complex"], "dremio1"))
    dremio2_task = asyncio.create_task(execute_query(QUERIES["complex"], "dremio2"))
    
    # Wait for both tasks to complete
    dremio1_result, dremio2_result = await asyncio.gather(dremio1_task, dremio2_task)
    
    # Compare results
    comparison = await compare_results(dremio1_result, dremio2_result)
    
    # Send comparison metrics to Datadog
    send_metric('comparison.match', 1 if comparison.match else 0)
    if comparison.row_count_diff is not None:
        send_metric('comparison.row_count_diff', comparison.row_count_diff)
    if comparison.similarity_score is not None:
        send_metric('comparison.similarity_score', comparison.similarity_score)
    
    return comparison

@app.get("/health", response_model=HealthResponse, summary="Health check")
async def health_check() -> HealthResponse:
    """
    Health check endpoint to verify the service is running and Dremio connections are working.
    """
    health_status = {
        "status": "healthy",
        "dremio1": {"status": "unknown"},
        "dremio2": {"status": "unknown"},
        "version": "1.0.0"
    }
    
    # Check Dremio 1 connection
    try:
        response = await asyncio.to_thread(
            requests.get,
            f"{Config.DREMIO1_HOST}/api/v3/catalog",
            headers={"Authorization": f"Bearer {Config.DREMIO1_TOKEN}"},
            timeout=5
        )
        health_status["dremio1"]["status"] = "healthy" if response.status_code == 200 else "unhealthy"
    except Exception as e:
        health_status["dremio1"]["status"] = "unhealthy"
        health_status["dremio1"]["error"] = str(e)
    
    # Check Dremio 2 connection
    try:
        response = await asyncio.to_thread(
            requests.get,
            f"{Config.DREMIO2_HOST}/api/v3/catalog",
            headers={"Authorization": f"Bearer {Config.DREMIO2_TOKEN}"},
            timeout=5
        )
        health_status["dremio2"]["status"] = "healthy" if response.status_code == 200 else "unhealthy"
    except Exception as e:
        health_status["dremio2"]["status"] = "unhealthy"
        health_status["dremio2"]["error"] = str(e)
    
    # Overall status is healthy only if both Dremio instances are healthy
    if health_status["dremio1"]["status"] != "healthy" or health_status["dremio2"]["status"] != "healthy":
        health_status["status"] = "degraded"
    
    return health_status

@app.get("/", summary="API root")
async def root() -> Dict[str, Any]:
    """
    API root endpoint with basic information.
    """
    return {
        "name": "MetricMind",
        "description": "Dremio Query Benchmarking System",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions"""
    logger.error(f"HTTP error: {exc.detail}")
    return {
        "error": exc.detail,
        "status_code": exc.status_code,
        "path": request.url.path
    }

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions"""
    logger.error(f"Unexpected error: {str(exc)}")
    return {
        "error": str(exc),
        "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
        "path": request.url.path
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 