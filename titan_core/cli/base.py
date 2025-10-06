"""
titan_core.cli.base

Shared CLI runner foundation for Titan Tools.

Provides:
 - Unified argument parsing (log level, dry-run, output)
 - Automatic logging setup
 - Safe execution wrapper (KeyboardInterrupt, exceptions)
 - Consistent exit codes across Titan CLI apps
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path
from typing import Callable, Optional

from titan_core.core.logging import setup_logging, get_logger
from titan_core.core.utils import ensure_dir

log = get_logger(__name__)


# ----------------------------------------------------------------------
# BASE PARSER FACTORY
# ----------------------------------------------------------------------

def build_base_parser(description: str = "Titan Tools command-line utility.") -> argparse.ArgumentParser:
    """
    Build a base parser preloaded with common global options.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set logging verbosity (default: INFO)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate actions without modifying files."
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output directory for reports or logs."
    )
    return parser


# ----------------------------------------------------------------------
# WRAPPER FUNCTION
# ----------------------------------------------------------------------

def run_cli(main_func: Callable[[argparse.Namespace], int], parser: argparse.ArgumentParser) -> None:
    """
    Execute a CLI command function safely with unified error handling.

    Args:
        main_func: The main function that takes parsed args and returns exit code.
        parser: Argument parser configured for this CLI.
    """
    args = parser.parse_args()

    # Configure logging
    setup_logging(args.log_level)
    log = get_logger(parser.prog)

    log.info(f"🚀 Running {parser.prog}")
    log.debug(f"Arguments: {args}")

    # Ensure output directory exists
    if args.output:
        ensure_dir(args.output)

    try:
        exit_code = main_func(args)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        log.warning("⚠️ Operation cancelled by user.")
        sys.exit(130)
    except Exception as e:
        log.error(f"❌ Unexpected error: {e}", exc_info=True)
        sys.exit(1)


# ----------------------------------------------------------------------
# EXAMPLE TEMPLATE (for reference)
# ----------------------------------------------------------------------

def example_cli(args: argparse.Namespace) -> int:
    """Example CLI entrypoint (for reference only)."""
    log.info(f"Dry run: {args.dry_run}")
    log.info(f"Output directory: {args.output}")
    return 0


if __name__ == "__main__":
    parser = build_base_parser("Example CLI runner for Titan Tools.")
    run_cli(example_cli, parser)
