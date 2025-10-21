"""Command-line entry points for Titan Tools tasks.

These helpers mirror the legacy bash wrappers but can be installed as
`console_scripts`, making them runnable from any directory while supporting
shell auto-completion via ``argcomplete``.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:  # pragma: no cover - completion optional in tests
    import argcomplete
except Exception:  # pragma: no cover
    argcomplete = None  # type: ignore[assignment]

from common.base.logging import setup_logging
from common.shared.loader import load_task_config
from video.mkv_clean import vid_mkv_clean
from video.rename import vid_rename
from video.scan import vid_mkv_scan


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "config.yaml"


def _enable_autocomplete(parser: argparse.ArgumentParser) -> None:
    if argcomplete is not None:  # pragma: no branch
        argcomplete.autocomplete(parser)  # type: ignore[call-arg]


def _normalize_use_rich(value: Any) -> Optional[bool]:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "auto":
            return None
        if lowered in {"true", "yes", "1", "on"}:
            return True
        if lowered in {"false", "no", "0", "off"}:
            return False
    return value if isinstance(value, bool) else None


def _configure_logging(logging_cfg: Dict[str, Any]) -> None:
    if not logging_cfg:
        return

    setup_logging(
        level=logging_cfg.get("level"),
        use_rich=_normalize_use_rich(logging_cfg.get("use_rich")),
        log_dir=logging_cfg.get("log_dir"),
        file_prefix=logging_cfg.get("file_prefix"),
    )


def _resolve_config_path(explicit: Optional[str]) -> Optional[Path]:
    if explicit:
        return Path(explicit).expanduser().resolve()
    if DEFAULT_CONFIG_PATH.exists():  # pragma: no branch - default install path
        return DEFAULT_CONFIG_PATH
    return None


def _load_task_payload(task: str, config_arg: Optional[str]) -> Dict[str, Any]:
    config_path = _resolve_config_path(config_arg)
    raw = load_task_config(task, str(config_path) if config_path else None)
    payload: Dict[str, Any] = dict(raw)
    logging_cfg = payload.pop("__logging__", {}) or {}
    _configure_logging(logging_cfg)
    return payload


def _as_paths(values: Iterable[Any]) -> List[Path]:
    return [Path(str(item)).expanduser() for item in values]


def cli_vid_mkv_scan(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run the MKV scan workflow from YAML config.")
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Force dry-run behaviour regardless of config settings.",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Skip writing CSV outputs (prints logs only).",
    )
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg = _load_task_payload("vid_mkv_scan", args.config)
    roots = _as_paths(cfg.get("roots", []))
    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root = Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    vid_mkv_scan(
        roots=roots or None,
        output_dir=output_dir,
        output_root=output_root,
        write_csv_file=not args.no_write,
        dry_run=args.dry_run or dry_run_cfg,
    )
    return 0


def cli_vid_mkv_clean(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Clean MKVs using mkv_scan track definitions.")
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run behaviour.")
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg = _load_task_payload("vid_mkv_clean", args.config)
    roots = _as_paths(cfg.get("roots", [])) if cfg.get("roots") else None
    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    vid_mkv_clean(
        def_file=Path(cfg["definition"]) if cfg.get("definition") else None,
        roots=roots,
        output_dir=output_dir,
        output_root=Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None,
        dry_run=args.dry_run or dry_run_cfg,
    )
    return 0


def cli_vid_rename(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Rename videos using the configured mapping file.")
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run behaviour.")
    parser.add_argument(
        "--no-meta",
        action="store_true",
        help="Disable metadata title updates regardless of config.",
    )
    parser.add_argument(
        "--name-list",
        help="Override auto-detected mkv_scan_name_list CSV path.",
    )
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg = _load_task_payload("vid_rename", args.config)
    roots = _as_paths(cfg.get("roots", [])) if cfg.get("roots") else None
    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root = Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    update_metadata = not bool(cfg.get("no_meta", False)) and not args.no_meta
    if args.name_list:
        name_list_path = Path(args.name_list).expanduser().resolve()
    elif cfg.get("mapping"):
        name_list_path = Path(cfg["mapping"]).expanduser().resolve()
    else:
        name_list_path = None
    vid_rename(
        name_list_file=name_list_path,
        roots=roots,
        output_dir=output_dir,
        output_root=output_root,
        update_metadata=update_metadata,
        dry_run=args.dry_run or dry_run_cfg,
    )
    return 0


__all__ = [
    "cli_vid_mkv_clean",
    "cli_vid_mkv_scan",
    "cli_vid_rename",
]
