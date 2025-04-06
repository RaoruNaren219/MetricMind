import logging
import sys
from pathlib import Path
from typing import Optional
from loguru import logger

from metricmind.utils.config import get_settings

def setup_logger(name: Optional[str] = None) -> logger:
    """Set up and configure logger with the specified name."""
    settings = get_settings()
    
    # Remove default handler
    logger.remove()
    
    # Add console handler with custom format
    logger.add(
        sys.stderr,
        format=settings.LOG_FORMAT,
        level=settings.LOG_LEVEL,
        colorize=True
    )
    
    # Add file handler if log file is specified
    if settings.LOG_FILE:
        log_file = Path(settings.LOG_FILE)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.add(
            str(log_file),
            format=settings.LOG_FORMAT,
            level=settings.LOG_LEVEL,
            rotation="1 day",
            retention="1 week",
            compression="zip"
        )
        
    # Create a logger with the specified name
    if name:
        return logger.bind(name=name)
    return logger

class InterceptHandler(logging.Handler):
    """Intercept standard library logging and redirect to loguru."""
    
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
            
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
            
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )

def setup_standard_logging():
    """Set up standard library logging to use loguru."""
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    
    # Remove handlers from all loggers
    for name in logging.root.manager.loggerDict:
        logging.getLogger(name).handlers = []
        logging.getLogger(name).propagate = True 