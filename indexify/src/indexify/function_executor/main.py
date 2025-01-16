from indexify.utils.logging import (
    configure_development_mode_logging,
    configure_logging_early,
    configure_production_mode_logging,
)

configure_logging_early()

import argparse

import structlog

from .server import Server
from .service import Service

logger = structlog.get_logger(module=__name__)


def validate_args(args):
    if args.address is None:
        logger.error("--address argument is required")
        exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Runs Function Executor with the specified API server address"
    )
    parser.add_argument("--address", help="API server address to listen on", type=str)
    parser.add_argument(
        "-d", "--dev", help="Run in development mode", action="store_true"
    )
    args = parser.parse_args()

    if args.dev:
        configure_development_mode_logging()
    else:
        configure_production_mode_logging()
    validate_args(args)

    logger.info("starting function executor server", address=args.address)

    Server(
        server_address=args.address,
        service=Service(),
    ).run()


if __name__ == "__main__":
    main()
