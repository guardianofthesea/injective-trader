from __future__ import annotations
import asyncio
from asyncio.tasks import Task
import random
from datetime import datetime, timedelta
from typing import Callable, Optional, Dict, Any, TypeVar, Type, Awaitable, Coroutine
from dataclasses import dataclass, field
import grpc
from grpc import StatusCode
import logging
from enum import Enum
from pydantic import BaseModel
import traceback

T = TypeVar('T')

class RetryableErrorType(Enum):
    UNAVAILABLE = "unavailable"
    TIMEOUT = "timeout"
    NETWORK = "network"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    UNKNOWN = "unknown"


_STATUS_TO_ERROR: Dict[StatusCode, RetryableErrorType] = {
    StatusCode.UNAVAILABLE: RetryableErrorType.UNAVAILABLE,
    StatusCode.DEADLINE_EXCEEDED: RetryableErrorType.TIMEOUT,
    StatusCode.RESOURCE_EXHAUSTED: RetryableErrorType.RESOURCE_EXHAUSTED,
}

class RetryConfigModel(BaseModel):
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 32.0
    jitter: bool = True
    timeout: float = 30.0
    error_threshold: int = 10
    error_window: int = 60

@dataclass(slots=True)
class RetryStats:
    """Track retry statistics and patterns"""
    total_retries: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    error_counts: Dict[RetryableErrorType, int] = field(default_factory=lambda: {
        error_type: 0 for error_type in RetryableErrorType
    })

    def record_success(self):
        """Record successful operation"""
        self.success_count += 1
        self.last_success = datetime.now()

    def record_failure(self, error_type: RetryableErrorType):
        """Record failed operation with error type"""
        self.failure_count += 1
        self.last_failure = datetime.now()
        self.error_counts[error_type] += 1
        self.total_retries += 1

    def get_error_rate(self, window_seconds: int = 60) -> float:
        """Calculate error rate in the specified time window"""
        if not self.last_failure:
            return 0.0

        window_start = datetime.now() - timedelta(seconds=window_seconds)
        if self.last_failure < window_start:
            return 0.0

        total = self.success_count + self.failure_count
        return self.failure_count / total if total > 0 else 0.0

class RetryHandler:
    """Handles retry logic with configurable parameters and error tracking"""

    __slots__ = (
        "logger",
        "config",
        "stats",
        "_cleanup_tasks",
        "_next_cleanup_at",
    )

    def __init__(self, logger: logging.Logger, config: RetryConfigModel):
        self.logger = logger
        self.config = config
        self.stats = RetryStats()
        self._cleanup_tasks: set[asyncio.Task] = set()
        self._next_cleanup_at = datetime.now()+timedelta(seconds=60)

    @staticmethod
    def _classify_error(err: Exception) -> tuple[bool, RetryableErrorType]:
        """Return (is_retryable, mapped_type) with detailed classification."""
        if isinstance(err, asyncio.CancelledError):
            return False, RetryableErrorType.UNKNOWN

        if isinstance(err, (asyncio.TimeoutError, TimeoutError)):
            return True, RetryableErrorType.TIMEOUT

        if isinstance(err, grpc.aio._call.AioRpcError):
            return True, _STATUS_TO_ERROR.get(err.code(), RetryableErrorType.UNKNOWN)

        if isinstance(err, (ConnectionError, OSError)):
            return True, RetryableErrorType.NETWORK

        return False, RetryableErrorType.UNKNOWN

    def _format_error_details(self, error: Exception) -> str:
        """Format comprehensive error details including type, message, and traceback"""
        if not error:
            return "No error captured"

        error_type = type(error).__name__
        error_msg = str(error)

        # Get the chain of exceptions (for wrapped exceptions)
        context_chain = []
        current_error = error
        while current_error:
            context_chain.append(f"{type(current_error).__name__}: {str(current_error)}")
            current_error = getattr(current_error, '__context__', None)

        # Get traceback if available
        tb_str = ""
        if hasattr(error, '__traceback__') and error.__traceback__:
            tb_lines = traceback.format_tb(error.__traceback__)
            # Limit traceback to most relevant frames
            if len(tb_lines) > 10:
                tb_lines = tb_lines[-10:]  # Keep last 10 frames
            tb_str = "\nTraceback (most recent call last):\n" + "".join(tb_lines)

        # For gRPC errors, include additional details
        details = ""
        if hasattr(error, 'code'):
            details += f"\nStatus Code: {error.code()}"
        if hasattr(error, 'details'):
            details += f"\nError Details: {error.details()}"
        if hasattr(error, 'debug_error_string'):
            debug_str = error.debug_error_string()
            # Parse out useful info from debug string
            if "DEADLINE_EXCEEDED" in debug_str:
                details += "\nTimeout Type: gRPC deadline exceeded"
            elif "Failed to connect" in debug_str:
                details += "\nConnection Issue: Failed to establish connection"
            details += f"\nDebug Info: {debug_str[:500]}"  # Limit debug string length

        # For asyncio timeout errors, add context
        if isinstance(error, asyncio.TimeoutError):
            details += f"\nTimeout Type: asyncio.wait_for timeout ({self.config.timeout}s)"

        # Include exception chain if multiple exceptions
        chain_str = ""
        if len(context_chain) > 1:
            chain_str = "\nException Chain:\n" + " -> ".join(context_chain)

        return f"{error_type}: {error_msg}{details}{chain_str}{tb_str}"

    def _backoff(self, attempt: int, error_type: RetryableErrorType) -> float:
        """Calculate delay with exponential backoff and error-specific adjustments"""
        # Base exponential backoff
        delay = min(self.config.base_delay * (2**(attempt-1)), self.config.max_delay)
        delay *= {
            RetryableErrorType.RESOURCE_EXHAUSTED: 1.5,
            RetryableErrorType.UNAVAILABLE: 1.2,
            RetryableErrorType.NETWORK: 1.3,
            RetryableErrorType.TIMEOUT: 1.1,
        }.get(error_type, 1.0)
        # Add jitter if configured
        if self.config.jitter:
            delay *= (0.5 + random.random())

        return delay

    async def _maybe_cleanup_resources(self, context: Optional[Dict] = None):
        """Clean up resources from failed operations"""
        if datetime.now() < self._next_cleanup_at:
            return
        self._next_cleanup_at = datetime.now() + timedelta(seconds=60)
        done_tasks = { task for task in self._cleanup_tasks if task.done()}
        for task in done_tasks:
            self._cleanup_tasks.discard(task)
            try:
                await task
            except Exception as e:      # noqa: BLE001
                self.logger.warning("Cleanup task raised: %s", e)

    def _should_circuit_break(self) -> bool:
        """Determine if circuit breaker should trigger"""
        if not self.stats.last_failure:
            return False

        # Check error threshold in configured window
        recent = datetime.now() - self.stats.last_failure
        if recent.total_seconds() >= self.config.error_window:
            self.stats.failure_count = 0
            self.stats.last_failure = None
            return False

        if self.stats.failure_count < self.config.error_threshold:
            return False

        self.logger.debug(f"circuit break: {self.stats.failure_count} failures within {recent.total_seconds()}s")
        return True

    async def execute_with_retry(
        self,
        operation: Callable[..., Awaitable[T]],
        cleanup_handler: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
        error_handler: Optional[Callable] = None,
        context: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ) -> T:
        self.logger.debug(f"Starting execute_with_retry")
        """Execute operation with retry logic and resource management"""
        attempt = 0
        context = context or {}

        # Periodic cleanup
        await self._maybe_cleanup_resources(context)

        def get_op_name(op: Callable[..., Any]) -> str:
            if isinstance(op, asyncio.Task):
                return op.get_name()
            name = getattr(op, "__name__", None)
            return name if name is not None else str(op)

        op_name = get_op_name(operation)
        self.logger.debug(f"executing operaton: {op_name}")
        last_error: Exception | None = None
        error_history: list[tuple[int, Exception, RetryableErrorType]] = []

        max_retries= self.config.max_attempts

        for attempt in range(1, max_retries+ 1):
            if self._should_circuit_break():
                self.logger.info(
                    f"Circuit breaker triggered after {self.config.error_threshold} errors in {self.config.error_window}s"
                )
                if attempt == self.config.max_attempts:
                    raise RuntimeError(
                        f"Operation {op_name} aborted due to circuit breaker after {self.config.max_attempts} attempts."
                    )
                else:
                    self.logger.warning(
                        f"Skipping attempt {attempt}/{self.config.max_attempts} for {op_name} due to circuit breaker."
                    )
                    continue

            start_time = datetime.now()
            try:
                timeout = self.config.timeout
                self.logger.debug(f"Attempt {attempt}/{max_retries} for {op_name}, timeout: {timeout}s")

                if timeout > 0:
                    result = await asyncio.wait_for(
                        operation(*args, **kwargs),
                        timeout=timeout,
                    )
                else:
                    result = await operation(*args, **kwargs)
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                self.logger.debug(f"{op_name} succeeded in {duration:.2f}s")
                self.stats.record_success()
                return result

            except asyncio.CancelledError:
                raise

            except Exception as _last_error:
                last_error = _last_error
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds() if 'start_time' in locals() else 0

                retryable, last_error_type = self._classify_error(last_error)
                error_history.append((attempt, last_error, last_error_type))

                # Comprehensive error logging
                error_details = self._format_error_details(last_error)

                if not retryable:
                    self.logger.error(
                        f"{op_name} failed (non-retryable) after {duration:.2f}s\n"
                        f"Error Details:\n{error_details}"
                    )
                    raise

                self.stats.record_failure(last_error_type)

                # Log with full context
                self.logger.warning(
                    f"{op_name} failed on attempt {attempt}/{max_retries} "
                    f"after {duration:.2f}s\n"
                    f"Error Type: {last_error_type.value}\n"
                    f"Error Details:\n{error_details}"
                )

                # custom error hook
                if error_handler is not None:
                    can_retry, updated_ctx = await error_handler(last_error, context)
                    if updated_ctx:
                        context.update(updated_ctx)
                    if not can_retry:
                        raise

                # schedule async cleanup (fire‑and‑forget)
                if cleanup_handler is not None:
                    self._cleanup_tasks.add(
                        asyncio.create_task(cleanup_handler(context, last_error_type))
                    )

                # last attempt? → break early to raise final error
                if attempt == max_retries:
                    break

                delay = self._backoff(attempt, last_error_type)
                self.logger.info(
                    f"Retrying {op_name} in {delay:.2f}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(delay)

        # Format comprehensive error summary
        error_summary = self._format_error_summary(op_name, error_history, context)
        raise RuntimeError(error_summary)

    def _format_error_summary(self, op_name: str, error_history: list, context: Dict) -> str:
        """Format a comprehensive summary of all retry attempts"""
        summary_parts = [
            f"{op_name} failed after {self.config.max_attempts} attempts",
            f"Total timeout: {self.config.timeout}s per attempt",
        ]

        # Add context info if available
        if context:
            relevant_context = {k: v for k, v in context.items()
                              if k not in ['cleanup_handler', 'error_handler']}
            if relevant_context:
                summary_parts.append(f"Context: {relevant_context}")

        # Add error history
        summary_parts.append("\nError History:")
        for attempt, error, error_type in error_history:
            summary_parts.append(
                f"  Attempt {attempt}: {error_type.value} - {type(error).__name__}: {str(error)}"
            )

        # Add the most detailed error info from the last attempt
        if error_history:
            _, last_error, _ = error_history[-1]
            summary_parts.append("\nFinal Error Details:")
            summary_parts.append(self._format_error_details(last_error))

        return "\n".join(summary_parts)

    def get_stats(self) -> Dict[str, Any]:
        """Get current retry statistics"""
        return {
            "total_retries": self.stats.total_retries,
            "success_count": self.stats.success_count,
            "failure_count": self.stats.failure_count,
            "last_success": self.stats.last_success.isoformat() if self.stats.last_success else None,
            "last_failure": self.stats.last_failure.isoformat() if self.stats.last_failure else None,
            "error_counts": {
                error_type.value: count
                for error_type, count in self.stats.error_counts.items()
            },
            "error_rate": self.stats.get_error_rate(self.config.error_window)
        }


class RetryConfigs(BaseModel):
    """Container for all retry configurations"""
    DefaultConfig: RetryConfigModel
    ChainListener: Optional[RetryConfigModel] = None
    MessageBroadcaster: Optional[RetryConfigModel] = None

def get_retry_handler(logger, config: Dict, component_name: Optional[str] = None) -> RetryHandler:
    """
    Create a RetryHandler with configuration

    Args:
        logger: Logger instance
        config: Configuration dictionary from yaml
        component_name: Component name to get specific retry config
    """
    logger.debug(f"{component_name} retry_config: {config}")
    retry_config = config

    return RetryHandler(logger, RetryConfigModel(**retry_config))
