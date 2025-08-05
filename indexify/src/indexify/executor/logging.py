# Executor logging configuration.
import logging
import sys
import traceback
from typing import TextIO

import structlog


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


def configure_development_mode_logging(compact_tracebacks=True):
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        structlog.dev.ConsoleRenderer(
            exception_formatter=(
                _compact_traceback_formatter
                if compact_tracebacks
                else structlog.dev.rich_traceback
            ),
        ),
    ]
    structlog.configure(
        processors=processors,
    )


def configure_production_mode_logging():
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.dev.set_exc_info,
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        structlog.processors.JSONRenderer(),
    ]
    structlog.configure(processors=processors)


def _compact_traceback_formatter(
    sio: TextIO, exc_info: structlog.typing.ExcInfo
) -> None:
    print(
        "\n (only the frame where the exception was raised is printed by default)",
        file=sio,
    )
    traceback.print_exception(
        *exc_info,
        file=sio,
        limit=-1,  # Print only 1 frame where the exception was raised
        chain=False,
    )
