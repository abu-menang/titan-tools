"""
titan_core.core.logging

Enhanced, typed logging for Titan Tools.

Features:
 - Custom TitanLogger subclass with Rich detection flag
 - Unified setup for Rich + standard logging
 - Colorized timestamps and tracebacks
 - Fully typed, no Pylance warnings
"""

from __future__ import annotations
import logging
import sys
from typing import Optional


# ----------------------------------------------------------------------
# RICH HANDLER CONFIGURATION
# ----------------------------------------------------------------------

try:
    from rich.logging import RichHandler
    RICH_AVAILABLE = True
except ImportError:
    RichHandler = None  # type: ignore[assignment]
    RICH_AVAILABLE = False


# ----------------------------------------------------------------------
# CUSTOM LOGGER CLASS
# ----------------------------------------------------------------------

class TitanLogger(logging.Logger):
    """Custom logger with Rich support flag."""
    rich_enabled: bool = False


# ----------------------------------------------------------------------
# BASE LOGGER SETUP
# ----------------------------------------------------------------------

def setup_logging(level: str = "INFO", use_rich: Optional[bool] = None) -> TitanLogger:
    """
    Configure and return a global Titan logger.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        use_rich: Force-enable/disable RichHandler. If None, auto-detect.
    """
    # Register our subclass globally
    logging.setLoggerClass(TitanLogger)
    logger: TitanLogger = logging.getLogger("titan")  # type: ignore[assignment]
    logger.setLevel(level.upper())

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    # Auto-detect if Rich can be used
    use_rich = RICH_AVAILABLE if use_rich is None else use_rich

    if use_rich and RichHandler:
        handler = RichHandler(
            rich_tracebacks=True,
            markup=True,
            show_time=True,
            show_path=False,
            log_time_format="[%X]",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.rich_enabled = True
    else:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.rich_enabled = False

    logger.propagate = False
    logger.debug(f"Logger initialized (Rich={'ON' if logger.rich_enabled else 'OFF'})")
    return logger


# ----------------------------------------------------------------------
# UTILITY ACCESSOR
# ----------------------------------------------------------------------

def get_logger(name: str = "titan") -> TitanLogger:
    """
    Retrieve a named TitanLogger instance.
    Automatically initializes logging if not yet configured.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        setup_logging()
    return logger  # type: ignore[return-value]


# ----------------------------------------------------------------------
# QUICK DEMO
# ----------------------------------------------------------------------

if __name__ == "__main__":
    log = setup_logging("DEBUG")
    log.info("🚀 Logging system initialized.")
    log.debug("This is a debug message.")
    log.warning("⚠️ Warning example.")
    log.error("❌ Error example.")
    print("Rich enabled:", log.rich_enabled)
