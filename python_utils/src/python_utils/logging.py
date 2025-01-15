import logging
import sys

import structlog
from structlog.contextvars import merge_contextvars
from structlog.dev import ConsoleRenderer, set_exc_info
from structlog.processors import StackInfoRenderer, TimeStamper, add_log_level

# Using this module allows us to be consistent with the logging configuration across all Python programs.


def configure_logging_early():
    """Configures standard Python logging module.

    By default 3rd party modules that are using standard Python logging module
    (logging.getLogger()) have their log lines dropped unless the module gets configured.

    Not dropping log lines from 3rd party modules is useful for debugging. E.g. this helps
    debugging errors in grpc servers if exceptions happen inside grpc module code.
    """
    logging.basicConfig(
        level=logging.WARNING,
        # This log message format is a bit similar to the default structlog format.
        format="%(asctime)s [%(levelname)s] %(message)s logger=%(name)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def configure_development_mode_logging():
    processors = [
        structlog_suppressor,
        merge_contextvars,
        add_log_level,
        StackInfoRenderer(),
        set_exc_info,
        TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        ConsoleRenderer(),
    ]
    structlog.configure(
        processors=processors,
    )


def configure_production_mode_logging():
    processors = [
        structlog_suppressor,
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ]
    structlog.configure(processors=processors)


_suppress_logging = False


def structlog_suppressor(logger, name, event_dict):
    global _suppress_logging
    return None if _suppress_logging else event_dict


def suppress():
    """Sets the log level for the root logger to the new level."""
    global _suppress_logging
    _suppress_logging = True
    logging.getLogger().setLevel(logging.CRITICAL)
