import asyncio
import argparse
import sys
import logging
from typing import Optional, List

from metricmind.utils.config import get_settings
from metricmind.utils.logger import setup_logger
from metricmind.core.data_generator import DataGenerator
from metricmind.core.query_executor import QueryExecutor

logger = setup_logger(__name__)

async def generate_data(args: argparse.Namespace) -> None:
    """Generate TPC-DS data."""
    settings = get_settings()
    
    # Override settings with command line arguments
    if args.scale is not None:
        settings.TPC_DS_SCALE_FACTOR = args.scale
        
    logger.info(f"Generating TPC-DS data with scale factor {settings.TPC_DS_SCALE_FACTOR}")
    
    async with DataGenerator(settings) as generator:
        result = await generator.generate_data()
        
    if result["status"] == "success":
        logger.info("Data generation completed successfully")
        logger.info(f"Generated {result['stats'].successful_files} files")
        logger.info(f"Total size: {result['stats'].total_size / (1024*1024):.2f} MB")
        logger.info(f"Average upload speed: {result['stats'].avg_upload_speed / (1024*1024):.2f} MB/s")
    elif result["status"] == "partial":
        logger.warning("Data generation completed with some failures")
        logger.warning(f"Successful: {result['stats'].successful_files}, Failed: {result['stats'].failed_files}")
    else:
        logger.error(f"Data generation failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)

async def run_benchmark(args: argparse.Namespace) -> None:
    """Run benchmark comparison."""
    settings = get_settings()
    
    # Override settings with command line arguments
    if args.iterations is not None:
        settings.BENCHMARK_ITERATIONS = args.iterations
        
    logger.info(f"Running benchmark with {settings.BENCHMARK_ITERATIONS} iterations")
    
    # Parse query filter if provided
    query_filter = None
    if args.queries:
        query_filter = set(args.queries)
        logger.info(f"Filtering to queries: {', '.join(query_filter)}")
    
    async with QueryExecutor(settings) as executor:
        result = await executor.run_benchmark(
            iterations=settings.BENCHMARK_ITERATIONS,
            query_filter=query_filter
        )
        
    if result["metadata"]["status"] == "completed":
        logger.info("Benchmark completed successfully")
        
        # Print summary
        if "overall" in result["metadata"]:
            overall = result["metadata"]["overall"]
            logger.info(f"Average performance ratio: {overall['avg_performance_ratio']:.2f}")
            logger.info(f"Median performance ratio: {overall['median_performance_ratio']:.2f}")
            logger.info(f"Success rate: {overall['success_rate']*100:.1f}%")
    elif result["metadata"]["status"] == "cancelled":
        logger.warning("Benchmark was cancelled")
    else:
        logger.error("Benchmark failed")
        sys.exit(1)

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="MetricMind - Dremio Benchmarking Tool")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Generate data command
    generate_parser = subparsers.add_parser("generate-data", help="Generate TPC-DS data")
    generate_parser.add_argument("--scale", type=float, help="TPC-DS scale factor")
    
    # Benchmark command
    benchmark_parser = subparsers.add_parser("benchmark", help="Run benchmark comparison")
    benchmark_parser.add_argument("--iterations", type=int, help="Number of iterations")
    benchmark_parser.add_argument("--queries", nargs="+", help="Specific queries to run")
    
    return parser.parse_args()

async def main() -> None:
    """Main entry point."""
    args = parse_args()
    
    if args.command == "generate-data":
        await generate_data(args)
    elif args.command == "benchmark":
        await run_benchmark(args)
    else:
        logger.error("No command specified")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 