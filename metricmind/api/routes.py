from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any

from metricmind.core.query_executor import QueryExecutor
from metricmind.core.data_generator import DataGenerator
from metricmind.utils.config import get_settings, Settings

router = APIRouter()

@router.post("/benchmark")
async def run_benchmark(
    settings: Settings = Depends(get_settings)
) -> Dict[str, Any]:
    """
    Run a benchmark comparison between two Dremio instances.
    """
    try:
        executor = QueryExecutor(settings)
        results = await executor.run_benchmark()
        return {
            "status": "success",
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/generate-data")
async def generate_test_data(
    settings: Settings = Depends(get_settings)
) -> Dict[str, Any]:
    """
    Generate TPC-DS test data for benchmarking.
    """
    try:
        generator = DataGenerator(settings)
        result = await generator.generate_data()
        return {
            "status": "success",
            "message": "Test data generated successfully",
            "details": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health")
async def health_check() -> Dict[str, str]:
    """
    Health check endpoint.
    """
    return {"status": "healthy"} 