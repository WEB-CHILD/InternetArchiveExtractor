"""
Logging configuration for  Internet Archive Extractor.
"""

import logging
import sys
from pathlib import Path
from contextlib import contextmanager


def setup_logging(log_level=logging.INFO, log_file=None):
    """
    Configure logging for the application.
    
    Args:
        log_level: The logging level (default: logging.INFO)
        log_file: Optional path to a log file. If None, only logs to console.
    """
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Remove existing handlers
    logger.handlers = []
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name):
    """
    Get a logger instance with the specified name.
    
    Args:
        name: The name for the logger (typically __name__)
    
    Returns:
        A logger instance
    """
    return logging.getLogger(name)


class LoggerWriter:
    """
    A file-like object that redirects writes to a logger.
    """
    def __init__(self, logger, level):
        """
        Initialize the LoggerWriter.
        
        Args:
            logger: The logger instance to write to
            level: The logging level to use (e.g., logging.INFO, logging.ERROR)
        """
        self.logger = logger
        self.level = level
        self.buffer = []
    
    def write(self, message):
        """
        Write a message to the logger.
        
        Args:
            message: The message to log
        """
        if message and message.strip():
            self.logger.log(self.level, message.strip())
    
    def flush(self):
        """
        Flush the buffer (no-op for logger).
        """
        pass


@contextmanager
def redirect_stdout_to_logger(logger, stdout_level=logging.INFO, stderr_level=logging.ERROR):
    """
    Context manager that redirects stdout and stderr to a logger.
    
    Args:
        logger: The logger instance to redirect output to
        stdout_level: The logging level for stdout messages (default: logging.INFO)
        stderr_level: The logging level for stderr messages (default: logging.ERROR)
    
    Usage:
        with redirect_stdout_to_logger(logger):
            # Any print statements or stdout output here will be logged
            some_function_with_output()
    """
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    
    try:
        sys.stdout = LoggerWriter(logger, stdout_level)
        sys.stderr = LoggerWriter(logger, stderr_level)
        yield
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr
