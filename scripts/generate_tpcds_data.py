import os
import subprocess
import requests
import time
import logging
import json
from typing import Dict, List, Optional, Tuple, Union
from dotenv import load_dotenv
from pathlib import Path
import hashlib
from requests.exceptions import RequestException, Timeout, ConnectionError
import backoff
import asyncio
import aiohttp
import aiofiles
import shutil
from datetime import datetime
import sys
import signal
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("tpcds_generator.log")
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class TPCDataGenerator:
    """Class to handle TPC-DS data generation and upload"""
    
    def __init__(self, scale_factor: int = 10):
        """
        Initialize the TPC-DS data generator
        
        Args:
            scale_factor: Scale factor for TPC-DS data (10 or 100)
        """
        self.scale_factor = scale_factor
        self.data_dir = Path("data/tpcds")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Dremio connection settings
        self.dremio1_host = os.getenv("DREMIO1_HOST", "http://localhost:9047")
        self.dremio2_host = os.getenv("DREMIO2_HOST", "http://localhost:9047")
        self.dremio1_token = os.getenv("DREMIO1_TOKEN")
        self.dremio2_token = os.getenv("DREMIO2_TOKEN")
        
        # Validate Dremio tokens
        if not self.dremio1_token or not self.dremio2_token:
            raise ValueError("Dremio tokens are required. Please set DREMIO1_TOKEN and DREMIO2_TOKEN environment variables.")
        
        # HDFS paths
        self.simple_hdfs_path = "/tpcds/simple"
        self.kerberos_hdfs_path = "/tpcds/kerberos"
        
        # Retry settings
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))
        self.retry_delay = int(os.getenv("RETRY_DELAY", "5"))
        self.request_timeout = int(os.getenv("REQUEST_TIMEOUT", "30"))
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self.shutdown_requested = False
    
    def _signal_handler(self, sig, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {sig}. Initiating graceful shutdown...")
        self.shutdown_requested = True
    
    @backoff.on_exception(backoff.expo, subprocess.CalledProcessError, max_tries=3)
    def generate_data(self) -> bool:
        """
        Generate TPC-DS data using dsdgen
        
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Generating TPC-DS data with scale factor {self.scale_factor}")
        
        try:
            # Check if dsdgen is available
            try:
                subprocess.run(["dsdgen", "-h"], capture_output=True, check=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                logger.error("dsdgen command not found. Please install TPC-DS tools.")
                return False
            
            # Clean up any existing data
            if self.data_dir.exists():
                logger.info("Cleaning up existing data directory")
                shutil.rmtree(self.data_dir)
                self.data_dir.mkdir(parents=True, exist_ok=True)
            
            # Run dsdgen to generate data
            logger.info("Running dsdgen to generate data")
            process = subprocess.Popen(
                [
                    "dsdgen",
                    "-scale", str(self.scale_factor),
                    "-dir", str(self.data_dir),
                    "-force"
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Monitor the process and log output
            while True:
                if self.shutdown_requested:
                    logger.info("Shutdown requested, terminating dsdgen process")
                    process.terminate()
                    return False
                
                output = process.stdout.readline()
                if output:
                    logger.info(f"dsdgen: {output.strip()}")
                
                error = process.stderr.readline()
                if error:
                    logger.error(f"dsdgen error: {error.strip()}")
                
                # Check if process has finished
                if process.poll() is not None:
                    break
                
                time.sleep(0.1)
            
            # Check process return code
            if process.returncode != 0:
                logger.error(f"dsdgen failed with return code {process.returncode}")
                return False
            
            # Verify data generation
            if not any(self.data_dir.iterdir()):
                logger.error("No data files were generated")
                return False
            
            # Log generated files
            files = list(self.data_dir.glob("*"))
            logger.info(f"Generated {len(files)} data files: {[f.name for f in files]}")
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating TPC-DS data: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during data generation: {e}")
            return False
    
    def _check_hdfs_path(self, hdfs_path: str) -> bool:
        """
        Check if path exists in HDFS
        
        Args:
            hdfs_path: HDFS path to check
            
        Returns:
            bool: True if path exists, False otherwise
        """
        try:
            result = subprocess.run(
                ["hdfs", "dfs", "-test", "-d", hdfs_path],
                capture_output=True,
                text=True
            )
            return result.returncode == 0
        except subprocess.CalledProcessError:
            return False
    
    @backoff.on_exception(backoff.expo, subprocess.CalledProcessError, max_tries=3)
    def upload_to_hdfs(self, hdfs_path: str, is_kerberos: bool = False) -> bool:
        """
        Upload data to HDFS
        
        Args:
            hdfs_path: HDFS path to upload to
            is_kerberos: Whether to use Kerberos authentication
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Uploading data to HDFS: {hdfs_path} (Kerberos: {is_kerberos})")
        
        try:
            # Check if path already exists
            if self._check_hdfs_path(hdfs_path):
                logger.warning(f"HDFS path {hdfs_path} already exists. Skipping upload.")
                return True
            
            # Create HDFS directory if it doesn't exist
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_path], check=True)
            
            # Upload files in chunks for large datasets
            files = list(self.data_dir.glob("*"))
            total_files = len(files)
            
            for i, file_path in enumerate(files, 1):
                if self.shutdown_requested:
                    logger.info("Shutdown requested, stopping HDFS upload")
                    return False
                
                if file_path.is_file():
                    logger.info(f"Uploading {file_path.name} to HDFS ({i}/{total_files})")
                    
                    # Use a temporary file for large uploads
                    temp_path = f"/tmp/{file_path.name}"
                    shutil.copy2(file_path, temp_path)
                    
                    try:
                        subprocess.run([
                            "hdfs", "dfs", "-put",
                            "-f", temp_path,
                            f"{hdfs_path}/{file_path.name}"
                        ], check=True)
                    finally:
                        # Clean up temporary file
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
            
            logger.info(f"Successfully uploaded {total_files} files to {hdfs_path}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error uploading to HDFS: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during HDFS upload: {e}")
            return False
    
    async def _check_source_exists(self, dremio_host: str, dremio_token: str, source_name: str) -> bool:
        """
        Check if data source already exists in Dremio
        
        Args:
            dremio_host: Dremio host URL
            dremio_token: Dremio authentication token
            source_name: Name of the data source
            
        Returns:
            bool: True if source exists, False otherwise
        """
        headers = {
            "Authorization": f"Bearer {dremio_token}",
            "Content-Type": "application/json"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{dremio_host}/api/v3/source",
                    headers=headers,
                    timeout=self.request_timeout
                ) as response:
                    if response.status == 200:
                        sources = await response.json()
                        return any(source["name"] == source_name for source in sources)
                    return False
        except Exception as e:
            logger.error(f"Error checking source existence: {e}")
            return False
    
    @backoff.on_exception(backoff.expo, RequestException, max_tries=3)
    async def register_in_dremio(self, dremio_host: str, dremio_token: str, source_name: str, hdfs_path: str) -> bool:
        """
        Register data source in Dremio
        
        Args:
            dremio_host: Dremio host URL
            dremio_token: Dremio authentication token
            source_name: Name for the data source
            hdfs_path: HDFS path where data is stored
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Registering data source {source_name} in Dremio at {dremio_host}")
        
        # Check if source already exists
        if await self._check_source_exists(dremio_host, dremio_token, source_name):
            logger.warning(f"Source {source_name} already exists in Dremio. Skipping registration.")
            return True
        
        headers = {
            "Authorization": f"Bearer {dremio_token}",
            "Content-Type": "application/json"
        }
        
        # Create source configuration
        source_config = {
            "name": source_name,
            "type": "HDFS",
            "config": {
                "path": hdfs_path,
                "propertyList": []
            }
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{dremio_host}/api/v3/source",
                    headers=headers,
                    json=source_config,
                    timeout=self.request_timeout
                ) as response:
                    if response.status == 200:
                        logger.info(f"Successfully registered source {source_name}")
                        return True
                    else:
                        response_text = await response.text()
                        logger.error(f"Failed to register source: {response_text}")
                        return False
        except Exception as e:
            logger.error(f"Error registering source: {e}")
            return False
    
    @backoff.on_exception(backoff.expo, RequestException, max_tries=3)
    async def test_dremio_connection(self, dremio_host: str, dremio_token: str) -> bool:
        """
        Test connection to Dremio
        
        Args:
            dremio_host: Dremio host URL
            dremio_token: Dremio authentication token
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        logger.info(f"Testing connection to Dremio at {dremio_host}")
        
        headers = {
            "Authorization": f"Bearer {dremio_token}",
            "Content-Type": "application/json"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{dremio_host}/api/v3/catalog",
                    headers=headers,
                    timeout=10
                ) as response:
                    if response.status == 200:
                        logger.info(f"Successfully connected to Dremio at {dremio_host}")
                        return True
                    else:
                        response_text = await response.text()
                        logger.error(f"Failed to connect to Dremio: {response_text}")
                        return False
        except Exception as e:
            logger.error(f"Error connecting to Dremio: {e}")
            return False
    
    async def run(self) -> bool:
        """
        Run the complete TPC-DS data generation and upload process
        
        Returns:
            bool: True if all steps successful, False otherwise
        """
        try:
            # Step 1: Generate TPC-DS data
            if not self.generate_data():
                logger.error("Failed to generate TPC-DS data")
                return False
            
            # Step 2: Upload to both HDFS clusters
            if not self.upload_to_hdfs(self.simple_hdfs_path):
                logger.error("Failed to upload to simple HDFS cluster")
                return False
            
            if not self.upload_to_hdfs(self.kerberos_hdfs_path, is_kerberos=True):
                logger.error("Failed to upload to Kerberos HDFS cluster")
                return False
            
            # Step 3: Test Dremio connections
            dremio1_conn = await self.test_dremio_connection(self.dremio1_host, self.dremio1_token)
            dremio2_conn = await self.test_dremio_connection(self.dremio2_host, self.dremio2_token)
            
            if not dremio1_conn:
                logger.error("Failed to connect to Dremio 1")
                return False
            
            if not dremio2_conn:
                logger.error("Failed to connect to Dremio 2")
                return False
            
            # Step 4: Register in Dremio 1
            dremio1_simple = await self.register_in_dremio(
                self.dremio1_host, 
                self.dremio1_token, 
                "tpcds_simple", 
                self.simple_hdfs_path
            )
            
            dremio1_kerberos = await self.register_in_dremio(
                self.dremio1_host, 
                self.dremio1_token, 
                "tpcds_kerberos", 
                self.kerberos_hdfs_path
            )
            
            if not dremio1_simple or not dremio1_kerberos:
                logger.error("Failed to register sources in Dremio 1")
                return False
            
            # Step 5: Register in Dremio 2
            dremio2_simple = await self.register_in_dremio(
                self.dremio2_host, 
                self.dremio2_token, 
                "tpcds_simple", 
                self.simple_hdfs_path
            )
            
            dremio2_kerberos = await self.register_in_dremio(
                self.dremio2_host, 
                self.dremio2_token, 
                "tpcds_kerberos", 
                self.kerberos_hdfs_path
            )
            
            if not dremio2_simple or not dremio2_kerberos:
                logger.error("Failed to register sources in Dremio 2")
                return False
            
            logger.info("TPC-DS data generation and registration completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Unexpected error during data generation and registration: {e}")
            return False

async def main():
    """Main function to run the TPC-DS data generation and upload process"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate and upload TPC-DS data")
    parser.add_argument("--scale", type=int, default=10, help="Scale factor for TPC-DS data (10 or 100)")
    parser.add_argument("--clean", action="store_true", help="Clean up existing data before generation")
    args = parser.parse_args()
    
    # Validate scale factor
    if args.scale not in [10, 100]:
        logger.error("Scale factor must be 10 or 100")
        return False
    
    try:
        # Create generator and run
        generator = TPCDataGenerator(scale_factor=args.scale)
        
        # Clean up if requested
        if args.clean and generator.data_dir.exists():
            logger.info("Cleaning up existing data directory")
            shutil.rmtree(generator.data_dir)
            generator.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Run the generator
        return await generator.run()
    except Exception as e:
        logger.error(f"Failed to run TPC-DS data generator: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1) 