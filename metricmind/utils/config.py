import os
from pathlib import Path
from typing import Dict, Optional
from pydantic import BaseSettings, Field
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings(BaseSettings):
    # Dremio Configuration
    DREMIO1_HOST: str = Field(..., env="DREMIO1_HOST")
    DREMIO1_PORT: int = Field(..., env="DREMIO1_PORT")
    DREMIO1_USERNAME: str = Field(..., env="DREMIO1_USERNAME")
    DREMIO1_PASSWORD: Optional[str] = Field(None, env="DREMIO1_PASSWORD")
    DREMIO1_PAT_TOKEN: Optional[str] = Field(None, env="DREMIO1_PAT_TOKEN")
    
    DREMIO2_HOST: str = Field(..., env="DREMIO2_HOST")
    DREMIO2_PORT: int = Field(..., env="DREMIO2_PORT")
    DREMIO2_USERNAME: str = Field(..., env="DREMIO2_USERNAME")
    DREMIO2_PASSWORD: Optional[str] = Field(None, env="DREMIO2_PASSWORD")
    DREMIO2_PAT_TOKEN: Optional[str] = Field(None, env="DREMIO2_PAT_TOKEN")
    
    # TPC-DS Configuration
    TPC_DS_SCALE_FACTOR: int = Field(1, env="TPC_DS_SCALE_FACTOR")
    TPC_DS_DATA_DIR: Path = Field(Path("data"), env="TPC_DS_DATA_DIR")
    TPC_DS_QUERIES_DIR: Path = Field(Path("queries"), env="TPC_DS_QUERIES_DIR")
    
    # HDFS Configuration
    HDFS_URL: str = Field(..., env="HDFS_URL")
    HDFS_BASE_PATH: str = Field("/tpcds", env="HDFS_BASE_PATH")
    HDFS_USERNAME: str = Field(..., env="HDFS_USERNAME")
    HDFS_PASSWORD: Optional[str] = Field(None, env="HDFS_PASSWORD")
    
    # API Configuration
    API_HOST: str = Field("0.0.0.0", env="API_HOST")
    API_PORT: int = Field(8000, env="API_PORT")
    API_WORKERS: int = Field(4, env="API_WORKERS")
    API_RELOAD: bool = Field(True, env="API_RELOAD")
    
    # Request Configuration
    REQUEST_TIMEOUT: int = Field(300, env="REQUEST_TIMEOUT")
    MAX_RETRIES: int = Field(3, env="MAX_RETRIES")
    RETRY_DELAY: int = Field(5, env="RETRY_DELAY")
    CHUNK_SIZE: int = Field(8192, env="CHUNK_SIZE")
    
    # Logging Configuration
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    LOG_FILE: Path = Field(Path("logs/metricmind.log"), env="LOG_FILE")
    LOG_FORMAT: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        env="LOG_FORMAT"
    )
    
    # Cache Configuration
    CACHE_ENABLED: bool = Field(True, env="CACHE_ENABLED")
    CACHE_TTL: int = Field(3600, env="CACHE_TTL")
    CACHE_MAX_SIZE: int = Field(1000, env="CACHE_MAX_SIZE")
    
    # Benchmark Configuration
    BENCHMARK_ITERATIONS: int = Field(3, env="BENCHMARK_ITERATIONS")
    BENCHMARK_TIMEOUT: int = Field(3600, env="BENCHMARK_TIMEOUT")
    BENCHMARK_MEMORY_LIMIT: int = Field(4096, env="BENCHMARK_MEMORY_LIMIT")
    BENCHMARK_CPU_LIMIT: int = Field(2, env="BENCHMARK_CPU_LIMIT")
    
    # Data Generation Configuration
    DATA_GEN_TIMEOUT: int = Field(7200, env="DATA_GEN_TIMEOUT")
    DATA_GEN_MEMORY_LIMIT: int = Field(8192, env="DATA_GEN_MEMORY_LIMIT")
    DATA_GEN_CPU_LIMIT: int = Field(4, env="DATA_GEN_CPU_LIMIT")
    DATA_GEN_PARALLEL_UPLOADS: int = Field(4, env="DATA_GEN_PARALLEL_UPLOADS")
    
    # Output Configuration
    OUTPUT_DIR: Path = Field(Path("output"), env="OUTPUT_DIR")
    OUTPUT_FORMAT: str = Field("parquet", env="OUTPUT_FORMAT")
    OUTPUT_COMPRESSION: str = Field("snappy", env="OUTPUT_COMPRESSION")
    
    # Visualization Configuration
    VIZ_THEME: str = Field("light", env="VIZ_THEME")
    VIZ_WIDTH: int = Field(1200, env="VIZ_WIDTH")
    VIZ_HEIGHT: int = Field(800, env="VIZ_HEIGHT")
    VIZ_COLOR_PALETTE: str = Field("default", env="VIZ_COLOR_PALETTE")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        
    def get_auth_headers(self, instance: int) -> Dict[str, str]:
        """Get authentication headers for the specified Dremio instance."""
        if instance not in [1, 2]:
            raise ValueError("Instance must be 1 or 2")
            
        username = getattr(self, f"DREMIO{instance}_USERNAME")
        password = getattr(self, f"DREMIO{instance}_PASSWORD")
        pat_token = getattr(self, f"DREMIO{instance}_PAT_TOKEN")
        
        if pat_token:
            return {"Authorization": f"Bearer {pat_token}"}
        elif password:
            import base64
            auth = base64.b64encode(f"{username}:{password}".encode()).decode()
            return {"Authorization": f"Basic {auth}"}
        else:
            raise ValueError(f"No authentication method configured for Dremio instance {instance}")
            
    def ensure_directories(self):
        """Ensure all required directories exist."""
        directories = [
            self.TPC_DS_DATA_DIR,
            self.TPC_DS_QUERIES_DIR,
            self.LOG_FILE.parent,
            self.OUTPUT_DIR
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            
_settings: Optional[Settings] = None

def get_settings() -> Settings:
    """Get or create settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
        _settings.ensure_directories()
    return _settings 