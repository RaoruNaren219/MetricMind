import asyncio
import aiohttp
import aiofiles
import os
import shutil
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set
import hashlib
from functools import lru_cache
import time
import signal
import sys
from dataclasses import dataclass
from enum import Enum
import pathlib
import re
import json
import tempfile

from metricmind.utils.config import Settings
from metricmind.utils.logger import setup_logger

logger = setup_logger(__name__)

class DataGenStatus(Enum):
    """Enum for data generation status."""
    SUCCESS = "success"
    FAILED = "failed"
    IN_PROGRESS = "in_progress"
    CANCELLED = "cancelled"
    PARTIAL = "partial"

@dataclass
class FileInfo:
    """Data class for file information."""
    name: str
    hdfs_path: str
    size: int
    hash: str
    upload_time: float
    status: str
    error: Optional[str] = None

@dataclass
class DataGenStats:
    """Data class for data generation statistics."""
    total_files: int
    successful_files: int
    failed_files: int
    total_size: int
    total_upload_time: float
    avg_upload_speed: float
    start_time: str
    end_time: Optional[str] = None
    error: Optional[str] = None

class DataGenerator:
    """Handles TPC-DS data generation and upload to Dremio."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.logger = logger
        self.timeout = aiohttp.ClientTimeout(total=settings.REQUEST_TIMEOUT)
        self.max_retries = settings.MAX_RETRIES
        self.retry_delay = settings.RETRY_DELAY
        self.chunk_size = 1024 * 1024  # 1MB chunks for file uploads
        self._session = None
        self._cancelled = False
        self._stats = DataGenStats(
            total_files=0,
            successful_files=0,
            failed_files=0,
            total_size=0,
            total_upload_time=0,
            avg_upload_speed=0,
            start_time=datetime.utcnow().isoformat()
        )
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, sig, frame):
        """Handle termination signals."""
        self.logger.info(f"Received signal {sig}, cancelling data generation")
        self._cancelled = True
        
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
        self.logger.info("Data generation cancelled")
        
    async def _check_hdfs_path(self, path: str) -> bool:
        """Check if HDFS path exists."""
        try:
            # Use existing session or create a new one
            session = self._session or aiohttp.ClientSession(timeout=self.timeout)
            should_close = self._session is None
            
            try:
                async with session.get(f"{self.settings.HDFS_URL}/webhdfs/v1/{path}") as response:
                    return response.status == 200
            finally:
                if should_close and session:
                    await session.close()
        except Exception as e:
            self.logger.error(f"Error checking HDFS path: {str(e)}")
            return False
            
    async def _create_hdfs_path(self, path: str) -> bool:
        """Create HDFS path if it doesn't exist."""
        try:
            # Use existing session or create a new one
            session = self._session or aiohttp.ClientSession(timeout=self.timeout)
            should_close = self._session is None
            
            try:
                # Create directory
                async with session.put(
                    f"{self.settings.HDFS_URL}/webhdfs/v1/{path}",
                    params={"op": "MKDIRS"}
                ) as response:
                    if response.status not in (200, 201):
                        error_text = await response.text()
                        self.logger.error(f"Failed to create HDFS path: {error_text}")
                        return False
                    return True
            finally:
                if should_close and session:
                    await session.close()
        except Exception as e:
            self.logger.error(f"Error creating HDFS path: {str(e)}")
            return False
            
    async def _upload_to_hdfs(self, local_path: str, hdfs_path: str) -> Optional[FileInfo]:
        """Upload file to HDFS with chunked transfer."""
        if self._cancelled:
            return None
            
        try:
            # Check if HDFS path exists
            if not await self._check_hdfs_path(hdfs_path):
                self.logger.info(f"Creating HDFS path: {hdfs_path}")
                if not await self._create_hdfs_path(hdfs_path):
                    return None
                
            # Calculate file hash
            file_hash = hashlib.md5()
            async with aiofiles.open(local_path, 'rb') as f:
                while True:
                    chunk = await f.read(self.chunk_size)
                    if not chunk:
                        break
                    file_hash.update(chunk)
            
            # Use existing session or create a new one
            session = self._session or aiohttp.ClientSession(timeout=self.timeout)
            should_close = self._session is None
            
            try:
                # Get upload URL
                async with session.put(
                    f"{self.settings.HDFS_URL}/webhdfs/v1/{hdfs_path}",
                    params={"op": "CREATE", "overwrite": "true"}
                ) as response:
                    if response.status != 307:
                        error_text = await response.text()
                        self.logger.error(f"Failed to get upload URL: {error_text}")
                        return None
                    upload_url = response.headers["Location"]
                    
                # Upload file in chunks
                start_time = time.time()
                async with aiofiles.open(local_path, 'rb') as f:
                    while True:
                        if self._cancelled:
                            self.logger.info("Upload cancelled")
                            return None
                            
                        chunk = await f.read(self.chunk_size)
                        if not chunk:
                            break
                            
                        async with session.put(upload_url, data=chunk) as upload_response:
                            if upload_response.status != 201:
                                error_text = await upload_response.text()
                                self.logger.error(f"Failed to upload chunk: {error_text}")
                                return None
                                
                upload_time = time.time() - start_time
                
                return FileInfo(
                    name=os.path.basename(local_path),
                    hdfs_path=hdfs_path,
                    size=os.path.getsize(local_path),
                    hash=file_hash.hexdigest(),
                    upload_time=upload_time,
                    status="success"
                )
                
            finally:
                if should_close and session:
                    await session.close()
                    
        except Exception as e:
            self.logger.error(f"Error uploading to HDFS: {str(e)}")
            return FileInfo(
                name=os.path.basename(local_path),
                hdfs_path=hdfs_path,
                size=os.path.getsize(local_path),
                hash="",
                upload_time=0,
                status="failed",
                error=str(e)
            )
            
    async def _check_source_exists(self, source_name: str) -> bool:
        """Check if data source exists in Dremio."""
        try:
            auth = aiohttp.BasicAuth(
                self.settings.DREMIO1_USERNAME,
                self.settings.DREMIO1_PAT_TOKEN or self.settings.DREMIO1_PASSWORD
            )
            
            # Use existing session or create a new one
            session = self._session or aiohttp.ClientSession(auth=auth, timeout=self.timeout)
            should_close = self._session is None
            
            try:
                async with session.get(
                    f"http://{self.settings.DREMIO1_HOST}:{self.settings.DREMIO1_PORT}/api/v3/catalog"
                ) as response:
                    if response.status != 200:
                        return False
                        
                    sources = await response.json()
                    return any(s["name"] == source_name for s in sources)
            finally:
                if should_close and session:
                    await session.close()
                    
        except Exception as e:
            self.logger.error(f"Error checking source existence: {str(e)}")
            return False
            
    async def _register_in_dremio(self, source_name: str, hdfs_path: str) -> bool:
        """Register HDFS path as a data source in Dremio."""
        try:
            auth = aiohttp.BasicAuth(
                self.settings.DREMIO1_USERNAME,
                self.settings.DREMIO1_PAT_TOKEN or self.settings.DREMIO1_PASSWORD
            )
            
            source_config = {
                "name": source_name,
                "type": "HDFS",
                "config": {
                    "path": hdfs_path,
                    "format": "PARQUET"
                }
            }
            
            # Use existing session or create a new one
            session = self._session or aiohttp.ClientSession(auth=auth, timeout=self.timeout)
            should_close = self._session is None
            
            try:
                async with session.post(
                    f"http://{self.settings.DREMIO1_HOST}:{self.settings.DREMIO1_PORT}/api/v3/catalog",
                    json=source_config
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        self.logger.error(f"Failed to register source: {error_text}")
                        return False
                    return True
            finally:
                if should_close and session:
                    await session.close()
                    
        except Exception as e:
            self.logger.error(f"Error registering source: {str(e)}")
            return False
            
    async def generate_data(self) -> Dict[str, Any]:
        """Generate TPC-DS data and register in Dremio."""
        self.logger.info("Starting TPC-DS data generation")
        
        results = {
            "status": DataGenStatus.IN_PROGRESS,
            "start_time": datetime.utcnow().isoformat(),
            "files": [],
            "sources": [],
            "stats": self._stats
        }
        
        try:
            # Clean up existing data
            if os.path.exists(self.settings.TPC_DS_DATA_DIR):
                self.logger.info(f"Cleaning up existing data in {self.settings.TPC_DS_DATA_DIR}")
                shutil.rmtree(self.settings.TPC_DS_DATA_DIR)
            os.makedirs(self.settings.TPC_DS_DATA_DIR)
            
            # Generate data using dsdgen
            self.logger.info(f"Generating TPC-DS data with scale factor {self.settings.TPC_DS_SCALE_FACTOR}")
            process = await asyncio.create_subprocess_exec(
                "dsdgen",
                "-scale", str(self.settings.TPC_DS_SCALE_FACTOR),
                "-dir", self.settings.TPC_DS_DATA_DIR,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode()
                self.logger.error(f"Data generation failed: {error_msg}")
                results["status"] = DataGenStatus.FAILED
                results["error"] = error_msg
                self._stats.end_time = datetime.utcnow().isoformat()
                self._stats.error = error_msg
                return results
                
            # Upload files to HDFS
            files = os.listdir(self.settings.TPC_DS_DATA_DIR)
            self._stats.total_files = len(files)
            
            for filename in files:
                if self._cancelled:
                    self.logger.info("Data generation cancelled")
                    results["status"] = DataGenStatus.CANCELLED
                    self._stats.end_time = datetime.utcnow().isoformat()
                    return results
                    
                local_path = os.path.join(self.settings.TPC_DS_DATA_DIR, filename)
                hdfs_path = f"{self.settings.HDFS_BASE_PATH}/{filename}"
                
                self.logger.info(f"Uploading {filename} to HDFS")
                file_info = await self._upload_to_hdfs(local_path, hdfs_path)
                
                if file_info:
                    results["files"].append(file_info)
                    if file_info.status == "success":
                        self._stats.successful_files += 1
                        self._stats.total_size += file_info.size
                        self._stats.total_upload_time += file_info.upload_time
                    else:
                        self._stats.failed_files += 1
                        
            # Register sources in Dremio
            for file_info in results["files"]:
                if self._cancelled:
                    self.logger.info("Data generation cancelled")
                    results["status"] = DataGenStatus.CANCELLED
                    self._stats.end_time = datetime.utcnow().isoformat()
                    return results
                    
                if file_info.status != "success":
                    continue
                    
                source_name = f"tpcds_{os.path.splitext(file_info.name)[0]}"
                
                if not await self._check_source_exists(source_name):
                    self.logger.info(f"Registering source {source_name} in Dremio")
                    if await self._register_in_dremio(source_name, file_info.hdfs_path):
                        results["sources"].append({
                            "name": source_name,
                            "path": file_info.hdfs_path,
                            "size": file_info.size,
                            "hash": file_info.hash
                        })
                        
            # Calculate final statistics
            self._stats.avg_upload_speed = (
                self._stats.total_size / self._stats.total_upload_time
                if self._stats.total_upload_time > 0 else 0
            )
            self._stats.end_time = datetime.utcnow().isoformat()
            
            # Determine final status
            if self._cancelled:
                results["status"] = DataGenStatus.CANCELLED
            elif self._stats.failed_files > 0:
                results["status"] = DataGenStatus.PARTIAL
            else:
                results["status"] = DataGenStatus.SUCCESS
                
            self.logger.info("TPC-DS data generation completed")
            return results
            
        except Exception as e:
            self.logger.error(f"Error in data generation: {str(e)}")
            results["status"] = DataGenStatus.FAILED
            results["error"] = str(e)
            self._stats.end_time = datetime.utcnow().isoformat()
            self._stats.error = str(e)
            return results 