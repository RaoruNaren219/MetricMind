import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from metricmind.core.data_generator import DataGenerator, DataGenStatus, FileInfo
from metricmind.utils.config import get_settings

@pytest.fixture
def settings():
    return get_settings()

@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.post = AsyncMock()
    session.put = AsyncMock()
    return session

@pytest.fixture
def data_generator(settings, mock_session):
    with patch('aiohttp.ClientSession', return_value=mock_session):
        generator = DataGenerator(settings)
        generator._session = mock_session
        return generator

@pytest.mark.asyncio
async def test_check_hdfs_path_exists(data_generator, mock_session):
    # Mock successful response
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"FileStatus": {"type": "DIRECTORY"}})
    mock_session.post.return_value.__aenter__.return_value = mock_response
    
    exists = await data_generator._check_hdfs_path_exists("/test/path")
    assert exists is True

@pytest.mark.asyncio
async def test_check_hdfs_path_not_exists(data_generator, mock_session):
    # Mock 404 response
    mock_response = MagicMock()
    mock_response.status = 404
    mock_session.post.return_value.__aenter__.return_value = mock_response
    
    exists = await data_generator._check_hdfs_path_exists("/test/path")
    assert exists is False

@pytest.mark.asyncio
async def test_create_hdfs_path(data_generator, mock_session):
    # Mock successful response
    mock_response = MagicMock()
    mock_response.status = 201
    mock_session.put.return_value.__aenter__.return_value = mock_response
    
    success = await data_generator._create_hdfs_path("/test/path")
    assert success is True

@pytest.mark.asyncio
async def test_upload_to_hdfs_success(data_generator, mock_session):
    # Mock successful response
    mock_response = MagicMock()
    mock_response.status = 201
    mock_session.put.return_value.__aenter__.return_value = mock_response
    
    # Create a temporary test file
    test_file = Path("test.txt")
    test_file.write_text("test data")
    
    try:
        result = await data_generator._upload_to_hdfs(test_file, "/test/path/test.txt")
        assert isinstance(result, FileInfo)
        assert result.status == "success"
        assert result.error is None
    finally:
        test_file.unlink()

@pytest.mark.asyncio
async def test_upload_to_hdfs_failure(data_generator, mock_session):
    # Mock failed response
    mock_response = MagicMock()
    mock_response.status = 500
    mock_response.text = AsyncMock(return_value="Upload failed")
    mock_session.put.return_value.__aenter__.return_value = mock_response
    
    # Create a temporary test file
    test_file = Path("test.txt")
    test_file.write_text("test data")
    
    try:
        result = await data_generator._upload_to_hdfs(test_file, "/test/path/test.txt")
        assert isinstance(result, FileInfo)
        assert result.status == "failed"
        assert "Upload failed" in result.error
    finally:
        test_file.unlink()

@pytest.mark.asyncio
async def test_generate_data_success(data_generator, mock_session):
    # Mock successful responses
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"FileStatus": {"type": "DIRECTORY"}})
    mock_session.post.return_value.__aenter__.return_value = mock_response
    
    mock_upload_response = MagicMock()
    mock_upload_response.status = 201
    mock_session.put.return_value.__aenter__.return_value = mock_upload_response
    
    with patch('subprocess.run') as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        result = await data_generator.generate_data()
        
        assert result["status"] == DataGenStatus.SUCCESS
        assert result["stats"].successful_files > 0
        assert result["stats"].failed_files == 0

@pytest.mark.asyncio
async def test_generate_data_partial_success(data_generator, mock_session):
    # Mock mixed responses
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"FileStatus": {"type": "DIRECTORY"}})
    mock_session.post.return_value.__aenter__.return_value = mock_response
    
    # First upload succeeds, second fails
    mock_upload_responses = [
        MagicMock(status=201),
        MagicMock(status=500, text=AsyncMock(return_value="Upload failed"))
    ]
    mock_session.put.return_value.__aenter__.side_effect = mock_upload_responses
    
    with patch('subprocess.run') as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        result = await data_generator.generate_data()
        
        assert result["status"] == DataGenStatus.PARTIAL
        assert result["stats"].successful_files > 0
        assert result["stats"].failed_files > 0 