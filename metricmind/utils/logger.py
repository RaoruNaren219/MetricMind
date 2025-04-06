import logging
import sys
from pathlib import Path
from typing import Optional

from metricmind.utils.config import get_settings

def setup_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """Set up a logger with the specified name and level.
    
    Args:
        name: The name of the logger.
        level: Optional logging level. If not provided, uses the level from settings.
        
    Returns:
        A configured logger instance.
    """
    settings = get_settings()
    logger = logging.getLogger(name)
    
    # Set log level
    if level is None:
        level = getattr(logging, settings.LOG_LEVEL.upper())
    logger.setLevel(level)
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if log file is specified
    if settings.LOG_FILE:
        log_path = Path(settings.LOG_FILE)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(settings.LOG_FILE)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger 