import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import sys
from io import StringIO

from metricmind.cli import main, generate_data, run_benchmark, parse_args

@pytest.mark.asyncio
async def test_generate_data_success():
    """Test successful data generation."""
    # Mock settings and data generator
    mock_settings = MagicMock()
    mock_settings.TPC_DS_SCALE_FACTOR = 1.0
    
    mock_generator = AsyncMock()
    mock_generator.generate_data.return_value = {
        "status": "success",
        "stats": MagicMock(
            successful_files=10,
            total_size=1024*1024,  # 1MB
            avg_upload_speed=1024*1024  # 1MB/s
        )
    }
    
    args = MagicMock()
    args.scale = None
    
    with patch("metricmind.cli.get_settings", return_value=mock_settings), \
         patch("metricmind.cli.DataGenerator", return_value=mock_generator), \
         patch("metricmind.cli.setup_logger"):
        await generate_data(args)
        
        # Verify generator was called
        mock_generator.generate_data.assert_called_once()

@pytest.mark.asyncio
async def test_generate_data_failure():
    """Test failed data generation."""
    # Mock settings and data generator
    mock_settings = MagicMock()
    mock_settings.TPC_DS_SCALE_FACTOR = 1.0
    
    mock_generator = AsyncMock()
    mock_generator.generate_data.return_value = {
        "status": "failed",
        "error": "Test error"
    }
    
    args = MagicMock()
    args.scale = None
    
    with patch("metricmind.cli.get_settings", return_value=mock_settings), \
         patch("metricmind.cli.DataGenerator", return_value=mock_generator), \
         patch("metricmind.cli.setup_logger"), \
         pytest.raises(SystemExit) as exc_info:
        await generate_data(args)
        assert exc_info.value.code == 1

@pytest.mark.asyncio
async def test_run_benchmark_success():
    """Test successful benchmark run."""
    # Mock settings and query executor
    mock_settings = MagicMock()
    mock_settings.BENCHMARK_ITERATIONS = 3
    
    mock_executor = AsyncMock()
    mock_executor.run_benchmark.return_value = {
        "metadata": {
            "status": "completed",
            "overall": {
                "avg_performance_ratio": 1.5,
                "median_performance_ratio": 1.4,
                "success_rate": 0.95
            }
        }
    }
    
    args = MagicMock()
    args.iterations = None
    args.queries = None
    
    with patch("metricmind.cli.get_settings", return_value=mock_settings), \
         patch("metricmind.cli.QueryExecutor", return_value=mock_executor), \
         patch("metricmind.cli.setup_logger"):
        await run_benchmark(args)
        
        # Verify executor was called
        mock_executor.run_benchmark.assert_called_once_with(
            iterations=3,
            query_filter=None
        )

@pytest.mark.asyncio
async def test_run_benchmark_failure():
    """Test failed benchmark run."""
    # Mock settings and query executor
    mock_settings = MagicMock()
    mock_settings.BENCHMARK_ITERATIONS = 3
    
    mock_executor = AsyncMock()
    mock_executor.run_benchmark.return_value = {
        "metadata": {
            "status": "failed"
        }
    }
    
    args = MagicMock()
    args.iterations = None
    args.queries = None
    
    with patch("metricmind.cli.get_settings", return_value=mock_settings), \
         patch("metricmind.cli.QueryExecutor", return_value=mock_executor), \
         patch("metricmind.cli.setup_logger"), \
         pytest.raises(SystemExit) as exc_info:
        await run_benchmark(args)
        assert exc_info.value.code == 1

def test_parse_args():
    """Test argument parsing."""
    # Test generate-data command
    sys.argv = ["metricmind", "generate-data", "--scale", "2.0"]
    args = parse_args()
    assert args.command == "generate-data"
    assert args.scale == 2.0
    
    # Test benchmark command
    sys.argv = ["metricmind", "benchmark", "--iterations", "5", "--queries", "q1.sql", "q2.sql"]
    args = parse_args()
    assert args.command == "benchmark"
    assert args.iterations == 5
    assert args.queries == ["q1.sql", "q2.sql"]
    
    # Test no command
    sys.argv = ["metricmind"]
    with pytest.raises(SystemExit) as exc_info:
        parse_args()
        assert exc_info.value.code == 1

@pytest.mark.asyncio
async def test_main():
    """Test main function."""
    # Mock the command functions
    with patch("metricmind.cli.generate_data", new_callable=AsyncMock) as mock_generate, \
         patch("metricmind.cli.run_benchmark", new_callable=AsyncMock) as mock_benchmark, \
         patch("metricmind.cli.parse_args") as mock_parse_args:
        
        # Test generate-data command
        mock_parse_args.return_value = MagicMock(command="generate-data")
        await main()
        mock_generate.assert_called_once()
        
        # Test benchmark command
        mock_parse_args.return_value = MagicMock(command="benchmark")
        await main()
        mock_benchmark.assert_called_once()
        
        # Test invalid command
        mock_parse_args.return_value = MagicMock(command=None)
        with pytest.raises(SystemExit) as exc_info:
            await main()
            assert exc_info.value.code == 1 