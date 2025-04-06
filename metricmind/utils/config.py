from pydantic import BaseSettings
from functools import lru_cache
from typing import Optional, Dict, Any, List
import os
import pathlib

class Settings(BaseSettings):
    """Application settings."""
    
    # Dremio 1 Configuration
    DREMIO1_HOST: str = "localhost"
    DREMIO1_PORT: int = 9047
    DREMIO1_USERNAME: str = "admin"
    DREMIO1_PASSWORD: Optional[str] = None
    DREMIO1_PAT_TOKEN: Optional[str] = None
    
    # Dremio 2 Configuration
    DREMIO2_HOST: str = "localhost"
    DREMIO2_PORT: int = 9047
    DREMIO2_USERNAME: str = "admin"
    DREMIO2_PASSWORD: Optional[str] = None
    DREMIO2_PAT_TOKEN: Optional[str] = None
    
    # TPC-DS Configuration
    TPC_DS_SCALE_FACTOR: float = 1.0
    TPC_DS_DATA_DIR: str = "tpcds_data"
    TPC_DS_QUERIES_DIR: str = "queries"
    
    # HDFS Configuration
    HDFS_URL: str = "http://localhost:9870"
    HDFS_BASE_PATH: str = "/tpcds"
    HDFS_USERNAME: Optional[str] = None
    HDFS_PASSWORD: Optional[str] = None
    
    # API Configuration
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_WORKERS: int = 4
    API_RELOAD: bool = True
    
    # Request Configuration
    REQUEST_TIMEOUT: int = 300  # 5 minutes
    MAX_RETRIES: int = 3
    RETRY_DELAY: float = 1.0
    CHUNK_SIZE: int = 1024 * 1024  # 1MB
    
    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    LOG_FILE: Optional[str] = None
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Cache Configuration
    CACHE_ENABLED: bool = True
    CACHE_TTL: int = 3600  # 1 hour
    CACHE_MAX_SIZE: int = 1000
    
    # Benchmark Configuration
    BENCHMARK_ITERATIONS: int = 1
    BENCHMARK_TIMEOUT: int = 3600  # 1 hour
    BENCHMARK_MEMORY_LIMIT: Optional[int] = None
    BENCHMARK_CPU_LIMIT: Optional[int] = None
    
    # Data Generation Configuration
    DATA_GEN_TIMEOUT: int = 7200  # 2 hours
    DATA_GEN_MEMORY_LIMIT: Optional[int] = None
    DATA_GEN_CPU_LIMIT: Optional[int] = None
    DATA_GEN_PARALLEL_UPLOADS: int = 4
    
    class Config:
        """Pydantic configuration."""
        env_file = ".env"
        case_sensitive = True
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._validate_paths()
        
    def _validate_paths(self):
        """Validate and create necessary directories."""
        # Create data directory if it doesn't exist
        data_dir = pathlib.Path(self.TPC_DS_DATA_DIR)
        if not data_dir.exists():
            data_dir.mkdir(parents=True)
            
        # Create queries directory if it doesn't exist
        queries_dir = pathlib.Path(self.TPC_DS_QUERIES_DIR)
        if not queries_dir.exists():
            queries_dir.mkdir(parents=True)
            
        # Create log directory if specified
        if self.LOG_FILE:
            log_dir = pathlib.Path(self.LOG_FILE).parent
            if not log_dir.exists():
                log_dir.mkdir(parents=True)
                
    @property
    def dremio1_url(self) -> str:
        """Get Dremio 1 base URL."""
        return f"http://{self.DREMIO1_HOST}:{self.DREMIO1_PORT}"
        
    @property
    def dremio2_url(self) -> str:
        """Get Dremio 2 base URL."""
        return f"http://{self.DREMIO2_HOST}:{self.DREMIO2_PORT}"
        
    @property
    def hdfs_url(self) -> str:
        """Get HDFS base URL."""
        return self.HDFS_URL
        
    @property
    def api_url(self) -> str:
        """Get API base URL."""
        return f"http://{self.API_HOST}:{self.API_PORT}"
        
    def get_auth_headers(self, instance: str = "dremio1") -> Dict[str, str]:
        """Get authentication headers for Dremio instance."""
        if instance == "dremio1":
            username = self.DREMIO1_USERNAME
            password = self.DREMIO1_PAT_TOKEN or self.DREMIO1_PASSWORD
        else:
            username = self.DREMIO2_USERNAME
            password = self.DREMIO2_PAT_TOKEN or self.DREMIO2_PASSWORD
            
        if not password:
            raise ValueError(f"No password or PAT token provided for {instance}")
            
        return {
            "Authorization": f"Basic {username}:{password}"
        }
        
@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings() 