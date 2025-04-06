import argparse
import asyncio
import sys
from pathlib import Path
from typing import Optional, Dict, Any

from metricmind.core.data_generator import DataGenerator
from metricmind.core.query_executor import QueryExecutor
from metricmind.utils.config import get_settings
from metricmind.utils.logger import setup_logger
from metricmind.visualization.dashboard import BenchmarkDashboard

logger = setup_logger(__name__)

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="MetricMind CLI")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Generate data command
    generate_parser = subparsers.add_parser("generate", help="Generate TPC-DS data")
    generate_parser.add_argument(
        "--output-dir",
        type=str,
        default="data",
        help="Output directory for generated data"
    )
    generate_parser.add_argument(
        "--scale-factor",
        type=float,
        default=1.0,
        help="Scale factor for data generation"
    )
    
    # Run benchmark command
    benchmark_parser = subparsers.add_parser("benchmark", help="Run benchmark comparison")
    benchmark_parser.add_argument(
        "--dremio1-url",
        type=str,
        required=True,
        help="URL for first Dremio instance"
    )
    benchmark_parser.add_argument(
        "--dremio2-url",
        type=str,
        required=True,
        help="URL for second Dremio instance"
    )
    benchmark_parser.add_argument(
        "--queries-dir",
        type=str,
        default="queries",
        help="Directory containing TPC-DS queries"
    )
    benchmark_parser.add_argument(
        "--output-dir",
        type=str,
        default="results",
        help="Output directory for benchmark results"
    )
    benchmark_parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Number of iterations per query"
    )
    
    # Visualize command
    visualize_parser = subparsers.add_parser("visualize", help="Visualize benchmark results")
    visualize_parser.add_argument(
        "--results-dir",
        type=str,
        default="results",
        help="Directory containing benchmark results"
    )
    
    return parser.parse_args()

async def generate_data(args: argparse.Namespace) -> None:
    """Generate TPC-DS data."""
    try:
        generator = DataGenerator(
            output_dir=Path(args.output_dir),
            scale_factor=args.scale_factor
        )
        await generator.generate()
        logger.info(f"Data generated successfully in {args.output_dir}")
    except Exception as e:
        logger.error(f"Failed to generate data: {str(e)}")
        sys.exit(1)

async def run_benchmark(args: argparse.Namespace) -> None:
    """Run benchmark comparison."""
    try:
        settings = get_settings()
        settings.dremio1_url = args.dremio1_url
        settings.dremio2_url = args.dremio2_url
        
        executor = QueryExecutor(
            queries_dir=Path(args.queries_dir),
            output_dir=Path(args.output_dir),
            iterations=args.iterations
        )
        
        results = await executor.run_benchmark()
        logger.info(f"Benchmark completed successfully. Results saved in {args.output_dir}")
    except Exception as e:
        logger.error(f"Failed to run benchmark: {str(e)}")
        sys.exit(1)

def visualize_results(args: argparse.Namespace) -> None:
    """Visualize benchmark results."""
    try:
        results_dir = Path(args.results_dir)
        if not results_dir.exists():
            logger.error(f"Results directory {args.results_dir} does not exist")
            sys.exit(1)
            
        dashboard = BenchmarkDashboard()
        dashboard.render({
            "queries": {},
            "resource_metrics": [],
            "metadata": {"overall": {}}
        })
    except Exception as e:
        logger.error(f"Failed to visualize results: {str(e)}")
        sys.exit(1)

def main() -> None:
    """Main entry point."""
    args = parse_args()
    
    if args.command == "generate":
        asyncio.run(generate_data(args))
    elif args.command == "benchmark":
        asyncio.run(run_benchmark(args))
    elif args.command == "visualize":
        visualize_results(args)
    else:
        logger.error("No command specified")
        sys.exit(1)

if __name__ == "__main__":
    main() 