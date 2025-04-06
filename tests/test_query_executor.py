import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from metricmind.core.query_executor import QueryExecutor, QueryStatus, QueryResult
from metricmind.utils.config import get_settings

@pytest.fixture
def settings():
    return get_settings()

@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.post = AsyncMock()
    return session

@pytest.fixture
def query_executor(settings, mock_session):
    with patch('aiohttp.ClientSession', return_value=mock_session):
        executor = QueryExecutor(settings)
        executor._session = mock_session
        return executor

@pytest.mark.asyncio
async def test_execute_query_success(query_executor, mock_session):
    # Mock successful response
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={
        "id": "test-query-id",
        "state": "COMPLETED",
        "rowCount": 100,
        "data": [{"col1": "val1"}, {"col1": "val2"}]
    })
    mock_session.post.return_value.__aenter__.return_value = mock_response
    
    result = await query_executor._execute_query("SELECT * FROM test")
    
    assert isinstance(result, QueryResult)
    assert result.status == QueryStatus.SUCCESS
    assert result.row_count == 100
    assert len(result.data) == 2
    assert result.error is None

@pytest.mark.asyncio
async def test_execute_query_failure(query_executor, mock_session):
    # Mock failed response
    mock_response = MagicMock()
    mock_response.status = 500
    mock_response.text = AsyncMock(return_value="Internal Server Error")
    mock_session.post.return_value.__aenter__.return_value = mock_response
    
    result = await query_executor._execute_query("SELECT * FROM test")
    
    assert isinstance(result, QueryResult)
    assert result.status == QueryStatus.FAILED
    assert result.error == "Internal Server Error"
    assert result.data is None

@pytest.mark.asyncio
async def test_execute_query_timeout(query_executor, mock_session):
    # Mock timeout
    mock_session.post.side_effect = asyncio.TimeoutError()
    
    result = await query_executor._execute_query("SELECT * FROM test")
    
    assert isinstance(result, QueryResult)
    assert result.status == QueryStatus.TIMEOUT
    assert "timeout" in result.error.lower()
    assert result.data is None

@pytest.mark.asyncio
async def test_run_benchmark(query_executor, mock_session):
    # Mock successful query execution
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={
        "id": "test-query-id",
        "state": "COMPLETED",
        "rowCount": 100,
        "data": [{"col1": "val1"}]
    })
    mock_session.post.return_value.__aenter__.return_value = mock_response
    
    result = await query_executor.run_benchmark(iterations=1)
    
    assert result["metadata"]["status"] == "completed"
    assert "overall" in result["metadata"]
    assert result["metadata"]["overall"]["success_rate"] == 1.0

@pytest.mark.asyncio
async def test_run_benchmark_with_filter(query_executor, mock_session):
    # Mock successful query execution
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={
        "id": "test-query-id",
        "state": "COMPLETED",
        "rowCount": 100,
        "data": [{"col1": "val1"}]
    })
    mock_session.post.return_value.__aenter__.return_value = mock_response
    
    query_filter = {"query1.sql"}
    result = await query_executor.run_benchmark(iterations=1, query_filter=query_filter)
    
    assert result["metadata"]["status"] == "completed"
    assert "overall" in result["metadata"]
    assert result["metadata"]["overall"]["success_rate"] == 1.0 