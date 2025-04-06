import pytest
import os
from pathlib import Path

@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables."""
    # Set test-specific environment variables
    os.environ["DREMIO1_HOST"] = "localhost"
    os.environ["DREMIO1_PORT"] = "9047"
    os.environ["DREMIO1_USERNAME"] = "test_user"
    os.environ["DREMIO1_PASSWORD"] = "test_pass"
    
    os.environ["DREMIO2_HOST"] = "localhost"
    os.environ["DREMIO2_PORT"] = "9048"
    os.environ["DREMIO2_USERNAME"] = "test_user"
    os.environ["DREMIO2_PASSWORD"] = "test_pass"
    
    os.environ["HDFS_URL"] = "http://localhost:9870"
    os.environ["HDFS_BASE_PATH"] = "/test"
    os.environ["HDFS_USERNAME"] = "test_user"
    os.environ["HDFS_PASSWORD"] = "test_pass"
    
    os.environ["TPC_DS_SCALE_FACTOR"] = "0.01"
    os.environ["TPC_DS_DATA_DIR"] = str(Path(__file__).parent / "data")
    os.environ["TPC_DS_QUERIES_DIR"] = str(Path(__file__).parent / "queries")
    
    os.environ["LOG_LEVEL"] = "DEBUG"
    os.environ["LOG_FILE"] = str(Path(__file__).parent / "test.log")
    
    # Create necessary directories
    data_dir = Path(__file__).parent / "data"
    queries_dir = Path(__file__).parent / "queries"
    data_dir.mkdir(exist_ok=True)
    queries_dir.mkdir(exist_ok=True)
    
    yield
    
    # Cleanup
    if data_dir.exists():
        for file in data_dir.glob("*"):
            file.unlink()
        data_dir.rmdir()
    
    if queries_dir.exists():
        for file in queries_dir.glob("*"):
            file.unlink()
        queries_dir.rmdir()
    
    log_file = Path(__file__).parent / "test.log"
    if log_file.exists():
        log_file.unlink() 