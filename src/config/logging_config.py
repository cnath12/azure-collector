import logging
import sys
import structlog
from typing import Any

def setup_logging(log_level: str = "INFO") -> None:
    """Configure structured logging for the application"""
    # Convert string level to integer
    numeric_level = getattr(logging, log_level.upper())
    
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer()
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

def get_logger(name: str) -> structlog.BoundLogger:
    """Get a logger instance with the given name"""
    return structlog.get_logger(name)

class LoggerMixin:
    """Mixin to add logging capabilities to any class"""
    
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.logger = get_logger(self.__class__.__name__)
        super().__init__(*args, **kwargs)

    def log_info(self, message: str, **kwargs: Any) -> None:
        """Log an info message"""
        self.logger.info(message, **kwargs)

    def log_error(self, message: str, error: Exception = None, **kwargs: Any) -> None:
        """Log an error message"""
        error_details = {
            'error_type': error.__class__.__name__,
            'error_message': str(error)
        } if error else {}
        self.logger.error(message, **{**error_details, **kwargs})

    def log_debug(self, message: str, **kwargs: Any) -> None:
        """Log a debug message"""
        self.logger.debug(message, **kwargs)