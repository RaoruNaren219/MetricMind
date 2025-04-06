import logging
import os
from pathlib import Path

from metricmind.utils.logger import setup_logger

def test_logger_creation():
    """Test basic logger creation."""
    logger = setup_logger("test_logger")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_logger"
    assert logger.level == logging.INFO

def test_logger_custom_level():
    """Test logger with custom level."""
    logger = setup_logger("test_logger", level=logging.DEBUG)
    assert logger.level == logging.DEBUG

def test_logger_handlers():
    """Test logger handlers."""
    logger = setup_logger("test_logger")
    
    # Should have at least one handler (console)
    assert len(logger.handlers) >= 1
    
    # Check console handler
    console_handler = next(h for h in logger.handlers if isinstance(h, logging.StreamHandler))
    assert console_handler.level == logging.NOTSET
    assert isinstance(console_handler.formatter, logging.Formatter)

def test_logger_file_handler():
    """Test logger with file handler."""
    log_file = Path("test.log")
    
    try:
        # Set environment variable for log file
        os.environ["LOG_FILE"] = str(log_file)
        
        logger = setup_logger("test_logger")
        
        # Should have two handlers (console and file)
        assert len(logger.handlers) >= 2
        
        # Check file handler
        file_handler = next(h for h in logger.handlers if isinstance(h, logging.FileHandler))
        assert file_handler.level == logging.NOTSET
        assert isinstance(file_handler.formatter, logging.Formatter)
        
        # Test logging to file
        test_message = "Test log message"
        logger.info(test_message)
        
        assert log_file.exists()
        with open(log_file) as f:
            log_content = f.read()
            assert test_message in log_content
    finally:
        # Clean up
        if log_file.exists():
            log_file.unlink()
        del os.environ["LOG_FILE"]

def test_logger_format():
    """Test logger format."""
    logger = setup_logger("test_logger")
    
    # Get console handler
    console_handler = next(h for h in logger.handlers if isinstance(h, logging.StreamHandler))
    formatter = console_handler.formatter
    
    # Test format string
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None
    )
    
    formatted = formatter.format(record)
    assert "test_logger" in formatted
    assert "INFO" in formatted
    assert "Test message" in formatted
    assert "test.py" in formatted 