import logging
import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_log,
    after_log,
)
from typing import Type, Callable, TypeVar, Any
from functools import wraps

logger = structlog.get_logger(__name__)
T = TypeVar('T')

# Convert string log levels to integer
LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

def create_retry_decorator(
    max_attempts: int = 3,
    min_wait: float = 1,
    max_wait: float = 10,
    exception_types: tuple[Type[Exception], ...] = (Exception,),
    log_level: str = "DEBUG"
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Create a retry decorator with specified parameters
    
    Args:
        max_attempts: Maximum number of retry attempts
        min_wait: Minimum wait time between retries in seconds
        max_wait: Maximum wait time between retries in seconds
        exception_types: Tuple of exception types to retry on
        log_level: Logging level for retry attempts
    
    Returns:
        Retry decorator function
    """
    # Convert string log level to integer
    numeric_level = LOG_LEVEL_MAP.get(log_level.upper(), logging.DEBUG)
    
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=min_wait, max=max_wait),
        retry=retry_if_exception_type(exception_types),
        before=before_log(logger, numeric_level),
        after=after_log(logger, numeric_level),
    )

def with_retry(
    max_attempts: int = 3,
    min_wait: float = 1,
    max_wait: float = 10,
    exception_types: tuple[Type[Exception], ...] = (Exception,),
    log_level: str = "DEBUG"
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for retrying functions with exponential backoff
    
    Args:
        max_attempts: Maximum number of retry attempts
        min_wait: Minimum wait time between retries in seconds
        max_wait: Maximum wait time between retries in seconds
        exception_types: Tuple of exception types to retry on
        log_level: Logging level for retry attempts
    
    Returns:
        Decorated function with retry logic
    """
    retry_decorator = create_retry_decorator(
        max_attempts=max_attempts,
        min_wait=min_wait,
        max_wait=max_wait,
        exception_types=exception_types,
        log_level=log_level
    )
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return retry_decorator(func)(*args, **kwargs)
        return wrapper
    return decorator