"""Main entrypoint for the alerting MVP."""
import argparse
import asyncio
import logging
from typing import Awaitable, Callable


logger = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure basic logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Crypto alerting MVP controller")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single iteration of the pipeline and exit.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Start the long-running service loop.",
    )
    return parser.parse_args()


async def run_once() -> None:
    """Placeholder for single-iteration execution."""
    logger.info("Running one-shot pipeline (stub).")
    # Future tasks: aggregation, rule scanning, notifications


async def loop_forever() -> None:
    """Placeholder for the long-running service loop."""
    logger.info("Starting long-running service loop (stub).")
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("Service loop cancelled, shutting down.")
        raise


def run_async(entry: Callable[[], Awaitable[None]]) -> None:
    """Helper to run an async entrypoint."""
    try:
        asyncio.run(entry())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")


def main() -> None:
    """Program entrypoint."""
    configure_logging()
    args = parse_args()

    if args.once and args.loop:
        raise SystemExit("--once and --loop are mutually exclusive")

    if args.once:
        run_async(run_once)
    elif args.loop:
        run_async(loop_forever)
    else:
        raise SystemExit("Please specify either --once or --loop. Use --help for options.")


if __name__ == "__main__":
    main()
