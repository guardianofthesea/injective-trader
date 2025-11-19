from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry._logs import set_logger_provider

import threading
import os
import logging
import queue
import signal
from logging.handlers import QueueHandler, QueueListener, TimedRotatingFileHandler
import functools
from typing import Tuple,Sequence


class BaseThreadFormatter(logging.Formatter):
    """
    Base formatter that handles thread tracking and provides core functionality
    for both standard and colored formatters.
    """

    _base_format = "%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(funcName)s() - %(message)s"

    def __init__(self, fmt=None):
        super().__init__(fmt or self._base_format)
        self.local = threading.local()
        self.local.thread_info = self._get_current_thread_info()
        self._get_current_thread_info = functools.lru_cache(maxsize=1)(
            self._get_current_thread_info
        )

    def _get_current_thread_info(self):
        """Get current thread information (cached per thread)"""
        thread = threading.current_thread()
        return f"{thread.name} ({threading.get_ident()})"

    def format(self, record):
        if not hasattr(record, "actthread"):
            try:
                if not hasattr(self.local, "thread_info"):
                    self.local.thread_info = self._get_current_thread_info()
                record.actthread = self.local.thread_info
            except Exception:
                record.actthread = (
                    f"{threading.current_thread().name} ({threading.get_ident()})"
                )

        try:
            return super().format(record)
        except KeyError as e:
            setattr(record, str(e).strip("'"), "unknown")
            return super().format(record)
        except ValueError as e:
            if "Formatting field not found in record" in str(e):
                import re

                match = re.search(r"'([^']+)'", str(e))
                if match:
                    missing_key = match.group(1)
                    setattr(record, missing_key, "unknown")
                    return super().format(record)
            raise


class StandardFormatter(BaseThreadFormatter):
    """A standard formatter that maintains thread tracking and detailed formatting for all log levels."""

    def __init__(self):
        super().__init__()


class ColoredFormatter(BaseThreadFormatter):
    """A colored formatter that applies different colors based on log level"""

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    def __init__(self):
        super().__init__(None)
        self.FORMATS = {
            logging.DEBUG: f"{self.grey}{self._base_format}{self.reset}",
            logging.INFO: f"{self.grey}{self._base_format}{self.reset}",
            logging.WARNING: f"{self.yellow}{self._base_format}{self.reset}",
            logging.ERROR: f"{self.red}{self._base_format}{self.reset}",
            logging.CRITICAL: f"{self.bold_red}{self._base_format}{self.reset}",
        }
        self._formatters = {
            level: logging.Formatter(fmt) for level, fmt in self.FORMATS.items()
        }

    def format(self, record):
        super().format(record)
        formatter = self._formatters.get(record.levelno, self._formatters[logging.INFO])
        try:
            return formatter.format(record)
        except KeyError as e:
            setattr(record, str(e).strip("'"), "unknown")
            return formatter.format(record)
        except ValueError as e:
            if "Formatting field not found in record" in str(e):
                import re

                match = re.search(r"'([^']+)'", str(e))
                if match:
                    missing_key = match.group(1)
                    setattr(record, missing_key, "unknown")
                    return formatter.format(record)
            raise


class ThreadLogger:
    """A logger that explicitly runs in a separate thread with pluggable handlers"""

    def __init__(self, *, name: str, signal_level: str = "INFO"):
        self.name = name
        self.log_queue = queue.Queue(-1)

        # Store all handlers
        self.handlers: list[logging.Handler] = []

        # Track current signal-controlled level
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if signal_level.upper() not in valid_levels:
            print(f"Invalid signal level '{signal_level}', defaulting to INFO")
        self.signal_level = getattr(logging, signal_level.upper(), logging.INFO)

        self._setup()
        self._setup_signal_handlers()

    def _setup(self):
        # Basic logger setup
        self.logger = logging.getLogger(self.name)
        self.logger.propagate = False
        self.logger.setLevel(logging.DEBUG)

        # Clear any existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Create the queue handler
        queue_handler = QueueHandler(self.log_queue)
        self.logger.addHandler(queue_handler)

        # Start with no handlers - user will add them
        self.listener = None

        self.logger.info(
            "Logger initialized - use add_handler() to add output handlers"
        )

    def add_handler(self, handler: logging.Handler):
        """Add a handler to the logger"""
        self.handlers.append(handler)
        self._restart_listener()
        self.logger.info(f"Handler added: {type(handler).__name__}")

    def remove_handler(self, handler: logging.Handler):
        """Remove a handler from the logger"""
        if handler in self.handlers:
            self.handlers.remove(handler)
            self._restart_listener()
            self.logger.info(f"Handler removed: {type(handler).__name__}")

    def _restart_listener(self):
        """Restart the listener with updated handlers"""
        if self.listener:
            self.listener.stop()

        # Start new listener (only if we have handlers)
        if self.handlers:
            self.listener = QueueListener(
                self.log_queue, *self.handlers, respect_handler_level=True
            )
            self.listener.start()
        else:
            self.listener = None

    def _setup_signal_handlers(self):
        """Setup signal handlers for SIGUSR1 and SIGUSR2"""
        if hasattr(signal, "SIGUSR1") and hasattr(signal, "SIGUSR2"):
            signal.signal(signal.SIGUSR1, self._handle_sigusr1)
            signal.signal(signal.SIGUSR2, self._handle_sigusr2)
            self.logger.info(
                "Signal handlers registered for SIGUSR1 (increase verbosity) and SIGUSR2 (decrease verbosity)"
            )
            self.logger.info(f"- PID: {os.getpid()}")
            self.logger.info(
                f"- Current signal level: {logging.getLevelName(self.signal_level)}"
            )
            self.logger.info(f"- Increase verbosity: kill -SIGUSR1 {os.getpid()}")
            self.logger.info(f"- Decrease verbosity: kill -SIGUSR2 {os.getpid()}")
        else:
            self.logger.warning("Platform does not support SIGUSR1/SIGUSR2 signals")

    def _handle_sigusr1(self, signum, frame):
        """Handle SIGUSR1 signal by increasing verbosity (decreasing log level) for all handlers"""
        if self.signal_level > logging.DEBUG:
            self.signal_level -= 10
            self._update_all_handler_levels()
            self.logger.critical(
                f"Log verbosity INCREASED to {logging.getLevelName(self.signal_level)} for all handlers"
            )
        else:
            self.logger.critical("Already at maximum verbosity (DEBUG level)")

    def _handle_sigusr2(self, signum, frame):
        """Handle SIGUSR2 signal by decreasing verbosity (increasing log level) for all handlers"""
        if self.signal_level < logging.CRITICAL:
            self.signal_level += 10
            self._update_all_handler_levels()
            self.logger.critical(
                f"Log verbosity DECREASED to {logging.getLevelName(self.signal_level)} for all handlers"
            )
        else:
            self.logger.critical("Already at minimum verbosity (CRITICAL level)")

    def _update_all_handler_levels(self):
        """Update log level for all handlers"""
        for handler in self.handlers:
            handler.setLevel(self.signal_level)
        self.logger.debug(
            f"Updated {len(self.handlers)} handlers to level {logging.getLevelName(self.signal_level)}"
        )

    def set_all_handler_levels(self, level):
        """Manually set log level for all handlers"""
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.INFO)

        self.signal_level = level
        self._update_all_handler_levels()
        self.logger.info(
            f"Manually set all handlers to level {logging.getLevelName(level)}"
        )

    def get_logger(self) -> logging.Logger:
        """Get the logger instance"""
        return self.logger

    def shutdown(self):
        """Shutdown the logger"""
        self.logger.info("Shutting down logger...")
        if self.listener:
            self.listener.stop()
            self.listener = None
            print("Logger shutdown complete")
            return True
        return False


# Helper functions to create common handlers
def create_console_handler(*, level=logging.INFO, colored: bool = True):
    """Create a console handler with optional coloring"""
    handler = logging.StreamHandler()
    handler.setLevel(level)

    if colored:
        formatter = ColoredFormatter()
    else:
        formatter = StandardFormatter()
    handler.setFormatter(formatter)
    return handler


def create_file_handler(
    *,
    log_file: str,
    level=logging.DEBUG,
    rotate: bool = True,
    when: str = "midnight",
    interval: int = 1,
    backup_count: int = 10,
):
    """Create a file handler with optional rotation"""
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    if rotate:
        handler = TimedRotatingFileHandler(
            log_file, when=when, interval=interval, backupCount=backup_count
        )
        handler.suffix = "%Y-%m-%d_%H-%M-%S.log"
    else:
        handler = logging.FileHandler(log_file)

    handler.setLevel(level)
    handler.setFormatter(StandardFormatter())
    return handler


def create_signoz_handler(
    *, endpoint: str, resource_attributes: None|dict[str,str], level=logging.INFO,
    headers:None|Sequence[Tuple[str,str]]=None, insecure: bool = False
):
    """Create a SigNoz/OpenTelemetry handler"""

    if resource_attributes is None:
        raise ValueError("resource_attributes cannot be None")

    resource = Resource.create(resource_attributes)
    logger_provider = LoggerProvider(resource=resource)
    set_logger_provider(logger_provider)

    exporter = OTLPLogExporter(endpoint=endpoint, headers=headers, insecure=insecure)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

    handler = LoggingHandler(level=level, logger_provider=logger_provider)
    return handler


# Example custom handlers
class SlackHandler(logging.Handler):
    """Example custom handler that sends logs to Slack"""

    def __init__(self, webhook_url: str, level=logging.ERROR):
        super().__init__(level)
        self.webhook_url = webhook_url
        self.setFormatter(StandardFormatter())

    def emit(self, record):
        # In a real implementation, you'd send to Slack API
        msg = self.format(record)
        print(f"SLACK MESSAGE to {self.webhook_url}: {msg}")


class DatabaseHandler(logging.Handler):
    """Example custom handler that stores logs in a database"""

    def __init__(self, db_connection_string: str, level=logging.INFO):
        super().__init__(level)
        self.db_connection = db_connection_string
        self.setFormatter(StandardFormatter())

    def emit(self, record):
        # In a real implementation, you'd insert into database
        msg = self.format(record)
        print(f"DATABASE INSERT to {self.db_connection}: {msg}")
