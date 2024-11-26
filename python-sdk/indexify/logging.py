import logging
import sys


# Applies standard logging configuration for Python programs.
# This allows us to be consistent about logs formatting and not
# drop log lines while not being aware about this.
#
# This function should be called as early as possible in application code
# because loggers are usually created during loading of Python modules.
def configure():
    _configure_standard_logging()


def _configure_standard_logging():
    # Initialize standard Python logging so 3rd party modules that are not using
    # structlog will have their messages and exceptions logged. E.g. this helps
    # debugging errors in grpc servers if exceptions happen inside grpc module code.
    #
    # Without this call all log lines from 3rd party modules are just dropped if they
    # use loggers created using logging.getLogger().
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(message)s logger=%(name)s",  # Define the log message format
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
