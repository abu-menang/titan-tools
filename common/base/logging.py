"""
common.base.logging

Enhanced, typed logging for Titan Tools.

Features:
 - Custom TitanLogger subclass with Rich detection flag
 - Unified setup for Rich + standard logging
 - Optional rotating file logging (per run)
 - Colorized, emoji-enhanced level output
 - Config-driven defaults (logging level, Rich toggle, log directory)
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, cast

# ----------------------------------------------------------------------
# RICH HANDLER CONFIGURATION
# ----------------------------------------------------------------------

try:
    from rich.logging import RichHandler
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:
    RichHandler = None  # type: ignore[assignment]
    Text = None  # type: ignore[assignment]
    RICH_AVAILABLE = False


# ----------------------------------------------------------------------
# LEVEL STYLE METADATA
# ----------------------------------------------------------------------

ANSI_RESET = "\033[0m"

LEVEL_STYLES: Dict[int, Dict[str, str]] = {
    logging.DEBUG: {"emoji": "🐛", "ansi": "\033[36m", "rich": "bright_cyan"},
    logging.INFO: {"emoji": "ℹ️", "ansi": "\033[32m", "rich": "green"},
    logging.WARNING: {"emoji": "⚠️", "ansi": "\033[33m", "rich": "yellow"},
    logging.ERROR: {"emoji": "❌", "ansi": "\033[31m", "rich": "red"},
    logging.CRITICAL: {"emoji": "💥", "ansi": "\033[95m", "rich": "bold magenta"},
}
DEFAULT_STYLE = LEVEL_STYLES[logging.INFO]

_ORIGINAL_RECORD_FACTORY = logging.getLogRecordFactory()


def _titan_record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
    record = _ORIGINAL_RECORD_FACTORY(*args, **kwargs)
    style = LEVEL_STYLES.get(record.levelno, DEFAULT_STYLE)
    record.level_emoji = style.get("emoji", "")  # type: ignore[attr-defined]
    record.level_color = style.get("ansi", "")  # type: ignore[attr-defined]
    record.level_rich_style = style.get("rich", "")  # type: ignore[attr-defined]
    return record


logging.setLogRecordFactory(_titan_record_factory)


# ----------------------------------------------------------------------
# FORMATTERS
# ----------------------------------------------------------------------

class ColorEmojiFormatter(logging.Formatter):
    """Console formatter that injects colored level names and emojis."""

    def format(self, record: logging.LogRecord) -> str:
        original_level_display = getattr(record, "level_display", None)
        level_name = record.levelname
        emoji = getattr(record, "level_emoji", "")
        ansi_color = getattr(record, "level_color", "")

        display = f"{emoji} {level_name}" if emoji else level_name
        if ansi_color:
            display = f"{ansi_color}{display}{ANSI_RESET}"

        record.level_display = display  # type: ignore[attr-defined]
        try:
            return super().format(record)
        finally:
            if original_level_display is None:
                delattr(record, "level_display")
            else:
                record.level_display = original_level_display  # type: ignore[attr-defined]


class EmojiFormatter(logging.Formatter):
    """File formatter that prefixes log lines with the computed emoji."""

    def format(self, record: logging.LogRecord) -> str:
        if not getattr(record, "level_emoji", ""):
            style = LEVEL_STYLES.get(record.levelno, DEFAULT_STYLE)
            record.level_emoji = style.get("emoji", "")  # type: ignore[attr-defined]
        return super().format(record)


if RICH_AVAILABLE and RichHandler is not None and Text is not None:

    class TitanRichHandler(RichHandler):
        """Rich console handler with emoji-enhanced level column."""

        def get_level_text(self, record: logging.LogRecord) -> Text:  # type: ignore[override]
            style = LEVEL_STYLES.get(record.levelno, DEFAULT_STYLE)
            emoji = getattr(record, "level_emoji", "")
            style_name = style.get("rich", "")

            text = Text()
            if emoji:
                text.append(f"{emoji} ", style=style_name or None)
            level_label = record.levelname
            if style_name:
                text.append(level_label, style=style_name)
            else:
                text.append(level_label)
            return text

else:  # pragma: no cover - fallback when Rich is unavailable
    TitanRichHandler = None  # type: ignore[assignment]


# ----------------------------------------------------------------------
# LOGGER CLASS
# ----------------------------------------------------------------------

class TitanLogger(logging.Logger):
    """Custom logger with Rich support flag and optional log file."""

    rich_enabled: bool = False
    log_file: Optional[Path] = None


# ----------------------------------------------------------------------
# CONFIG-DRIVEN DEFAULTS
# ----------------------------------------------------------------------

_DEFAULT_SETTINGS_CACHE: Dict[str, Any] | None = None


def _normalize_level(value: Any) -> str:
    if isinstance(value, str):
        candidate = value.strip().upper()
        if candidate in logging._nameToLevel:  # type: ignore[attr-defined]
            return candidate
    elif isinstance(value, int):
        label = logging.getLevelName(value)
        if isinstance(label, str):
            return label
    return "INFO"


def _normalize_use_rich(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"auto", "default", ""}:
            return None
        if lowered in {"true", "yes", "1", "on"}:
            return True
        if lowered in {"false", "no", "0", "off"}:
            return False
    return None


def _load_default_logging_settings() -> Dict[str, Any]:
    global _DEFAULT_SETTINGS_CACHE
    if _DEFAULT_SETTINGS_CACHE is None:
        try:
            from common.shared.loader import load_logging_config

            raw = load_logging_config(None)
        except Exception:
            raw = {}

        defaults = {
            "level": _normalize_level(raw.get("level")),
            "use_rich": _normalize_use_rich(raw.get("use_rich")),
            "log_dir": raw.get("log_dir"),
            "file_prefix": raw.get("file_prefix"),
        }
        _DEFAULT_SETTINGS_CACHE = defaults
    return dict(_DEFAULT_SETTINGS_CACHE)


def _resolve_log_dir(log_dir: Optional[Path | str], default: Optional[str]) -> Path:
    base = log_dir or default or "./logs"
    return Path(base).expanduser()


def _resolve_use_rich(value: Optional[bool]) -> bool:
    if value is None:
        return RICH_AVAILABLE
    return bool(value) and RICH_AVAILABLE


# ----------------------------------------------------------------------
# BASE LOGGER SETUP
# ----------------------------------------------------------------------

def setup_logging(
    level: str | int | None = None,
    use_rich: Optional[bool] = None,
    log_dir: Optional[Path | str] = None,
    file_prefix: Optional[str] = None,
) -> TitanLogger:
    """
    Configure and return a global Titan logger.

    Args:
        level: Desired logging level. Defaults to the value in config.yaml (INFO if unset).
        use_rich: Force-enable or disable Rich handler. None honors config/auto-detect.
        log_dir: Directory to store log files. Defaults to config.yaml or ./logs.
        file_prefix: Prefix for generated log filenames.
    """
    defaults = _load_default_logging_settings()
    resolved_level = _normalize_level(level if level is not None else defaults.get("level"))
    resolved_use_rich = _resolve_use_rich(use_rich if use_rich is not None else defaults.get("use_rich"))
    resolved_log_dir = _resolve_log_dir(log_dir, defaults.get("log_dir"))
    resolved_file_prefix = file_prefix or defaults.get("file_prefix") or "titan"

    logging.setLoggerClass(TitanLogger)
    logger = cast(TitanLogger, logging.getLogger("titan"))
    logger.setLevel(resolved_level)

    # Tear down any previous handlers so we can rebuild with new settings.
    if getattr(logger, "_initialized", False):
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Console Handler (Rich or ANSI)
    # ------------------------------------------------------------------
    if resolved_use_rich and TitanRichHandler is not None:
        console_handler = TitanRichHandler(
            rich_tracebacks=True,
            markup=True,
            show_time=True,
            show_level=True,
            show_path=False,
            log_time_format="[%X]",
        )
        logger.rich_enabled = True
    else:
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = ColorEmojiFormatter(
            fmt="%(asctime)s %(level_display)s %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        console_handler.setFormatter(console_formatter)
        logger.rich_enabled = False
    console_handler.setLevel(logging.NOTSET)
    logger.addHandler(console_handler)

    # ------------------------------------------------------------------
    # File Handler
    # ------------------------------------------------------------------
    resolved_log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = resolved_log_dir / f"{resolved_file_prefix}_{timestamp}.log"

    file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
    file_formatter = EmojiFormatter(
        fmt="%(asctime)s %(level_emoji)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.NOTSET)
    logger.addHandler(file_handler)
    logger.log_file = log_file_path

    # ------------------------------------------------------------------
    # Finalize
    # ------------------------------------------------------------------
    logger.propagate = False
    logger._initialized = True  # type: ignore[attr-defined]
    logger.debug(
        "Logger initialized at level %s (Rich=%s)",
        resolved_level,
        "ON" if logger.rich_enabled else "OFF",
    )
    logger.info("📄 Log file created at: %s", log_file_path.resolve())

    return logger  # type: ignore[return-value]


# ----------------------------------------------------------------------
# UTILITY ACCESSOR
# ----------------------------------------------------------------------

def get_logger(name: str = "titan") -> TitanLogger:
    """Retrieve a namespaced Titan logger (configured later via setup_logging)."""

    logging.setLoggerClass(TitanLogger)
    base = cast(TitanLogger, logging.getLogger("titan"))

    if not getattr(base, "_initialized", False) and not base.handlers:
        base.addHandler(logging.NullHandler())

    if not name or name == "titan":
        return base

    return cast(TitanLogger, base.getChild(name))


# ----------------------------------------------------------------------
# QUICK DEMO
# ----------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover - manual demo
    log = setup_logging("DEBUG")
    log.info("🚀 Logging system initialized.")
    log.debug("Debugging details here...")
    log.warning("⚠️ Warning example.")
    log.error("❌ Error example.")
    print("Rich enabled:", log.rich_enabled)
    print("Log file:", log.log_file)
