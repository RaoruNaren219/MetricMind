import os
import pytest
from pathlib import Path

from metricmind.utils.config import get_settings, Settings

def test_settings_singleton():
    """Test that get_settings returns the same instance."""
    settings1 = get_settings()
    settings2 = get_settings()
    assert settings1 is settings2

def test_settings_defaults():
    """Test default settings values."""
    settings = Settings()
    
    # Dremio settings
    assert settings.DREMIO1_HOST == "localhost"
    assert settings.DREMIO1_PORT == 9047
    assert settings.DREMIO2_HOST == "localhost"
    assert settings.DREMIO2_PORT == 9048
    
    # HDFS settings
    assert settings.HDFS_URL == "http://localhost:9870"
    assert settings.HDFS_BASE_PATH == "/data"
    
    # TPC-DS settings
    assert settings.TPC_DS_SCALE_FACTOR == 1.0
    assert isinstance(settings.TPC_DS_DATA_DIR, Path)
    assert isinstance(settings.TPC_DS_QUERIES_DIR, Path)
    
    # API settings
    assert settings.API_HOST == "localhost"
    assert settings.API_PORT == 8000
    assert settings.API_WORKERS == 4
    
    # Request settings
    assert settings.REQUEST_TIMEOUT == 300
    assert settings.MAX_RETRIES == 3
    assert settings.RETRY_DELAY == 1
    
    # Logging settings
    assert settings.LOG_LEVEL == "INFO"
    assert isinstance(settings.LOG_FILE, Path)
    
    # Cache settings
    assert settings.CACHE_ENABLED is True
    assert settings.CACHE_TTL == 3600
    assert settings.CACHE_MAX_SIZE == 1000
    
    # Benchmark settings
    assert settings.BENCHMARK_ITERATIONS == 3
    assert settings.BENCHMARK_TIMEOUT == 600
    assert settings.BENCHMARK_MEMORY_LIMIT == 1024
    assert settings.BENCHMARK_CPU_LIMIT == 2
    
    # Data generation settings
    assert settings.DATA_GEN_TIMEOUT == 3600
    assert settings.DATA_GEN_MEMORY_LIMIT == 2048
    assert settings.DATA_GEN_CPU_LIMIT == 4
    assert settings.DATA_GEN_PARALLEL_UPLOADS == 4

def test_settings_env_override():
    """Test that environment variables override defaults."""
    os.environ["DREMIO1_HOST"] = "test-host"
    os.environ["DREMIO1_PORT"] = "9999"
    os.environ["HDFS_URL"] = "http://test-hdfs:9870"
    os.environ["TPC_DS_SCALE_FACTOR"] = "2.0"
    os.environ["LOG_LEVEL"] = "DEBUG"
    
    settings = Settings()
    
    assert settings.DREMIO1_HOST == "test-host"
    assert settings.DREMIO1_PORT == 9999
    assert settings.HDFS_URL == "http://test-hdfs:9870"
    assert settings.TPC_DS_SCALE_FACTOR == 2.0
    assert settings.LOG_LEVEL == "DEBUG"
    
    # Clean up
    del os.environ["DREMIO1_HOST"]
    del os.environ["DREMIO1_PORT"]
    del os.environ["HDFS_URL"]
    del os.environ["TPC_DS_SCALE_FACTOR"]
    del os.environ["LOG_LEVEL"]

def test_settings_path_validation():
    """Test that paths are created if they don't exist."""
    test_data_dir = Path("/tmp/test_data")
    test_queries_dir = Path("/tmp/test_queries")
    
    os.environ["TPC_DS_DATA_DIR"] = str(test_data_dir)
    os.environ["TPC_DS_QUERIES_DIR"] = str(test_queries_dir)
    
    settings = Settings()
    
    assert test_data_dir.exists()
    assert test_queries_dir.exists()
    
    # Clean up
    test_data_dir.rmdir()
    test_queries_dir.rmdir()
    del os.environ["TPC_DS_DATA_DIR"]
    del os.environ["TPC_DS_QUERIES_DIR"]

def test_settings_auth_headers():
    """Test generation of authentication headers."""
    settings = Settings()
    
    # Test Dremio 1 headers
    headers1 = settings.get_auth_headers(1)
    assert "Authorization" in headers1
    
    # Test Dremio 2 headers
    headers2 = settings.get_auth_headers(2)
    assert "Authorization" in headers2
    
    # Test invalid instance
    with pytest.raises(ValueError):
        settings.get_auth_headers(3) 