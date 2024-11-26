import logging
import sys

import structlog

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


def configure_production_logging():
    processors = [
        structlog.processors.dict_tracebacks,
        structlog.processors.JSONRenderer(),
    ]
    structlog.configure(processors=processors)
