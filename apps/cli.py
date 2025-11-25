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
from video.mkv_clean import resolve_tracks_csvs, vid_mkv_clean
from video.rename import resolve_name_list_csvs, vid_rename
from video.scan import vid_mkv_scan
from video.hevc_convert import hevc_convert
from video.mkv_extract_subtitles import vid_mkv_extract_subs
from video.srt_clean import vid_srt_clean


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
        batch_size=cfg.get("batch_size"),
    )
    return 0


def cli_vid_mkv_clean(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Clean MKVs using mkv_scan track definitions.")
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run behaviour.")
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg = _load_task_payload("vid_mkv_clean", args.config)
    roots = _as_paths(cfg.get("roots", []))
    if not roots:
        raise SystemExit("vid_mkv_clean config requires at least one root")

    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root = Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    definition_override = cfg.get("definition")
    csv_parts = cfg.get("csv_part") or []

    if definition_override:
        targets = [Path(definition_override).expanduser().resolve()]
    else:
        part_sequence = csv_parts if csv_parts else None
        targets = resolve_tracks_csvs(roots, output_root, part_sequence)
        if not targets:
            raise SystemExit("Could not locate mkv_scan track CSVs for the requested configuration.")

    for definition_path in targets:
        vid_mkv_clean(
            def_file=definition_path,
            roots=roots,
            output_dir=output_dir,
            output_root=output_root,
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
    csv_parts = cfg.get("csv_part") or []
    if args.name_list:
        targets = [Path(args.name_list).expanduser().resolve()]
    else:
        mapping_override = cfg.get("mapping")
        if mapping_override:
            targets = [Path(mapping_override).expanduser().resolve()]
        else:
            part_sequence = csv_parts if csv_parts else None
            targets = resolve_name_list_csvs(roots or [], output_root, part_sequence)
            if not targets:
                raise SystemExit("Could not locate mkv_scan name list CSVs for the requested configuration.")

    for name_list_path in targets:
        vid_rename(
            name_list_file=name_list_path,
            roots=roots,
            output_dir=output_dir,
            output_root=output_root,
            update_metadata=update_metadata,
            dry_run=args.dry_run or dry_run_cfg,
        )
    return 0


def cli_vid_hevc_convert(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Convert non-HEVC MKVs to HEVC using ffmpeg.")
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument("--dry-run", action="store_true", help="Simulate conversions without writing files.")
    parser.add_argument("--preset", help="Override encoder preset (defaults to config value).")
    parser.add_argument("--crf", type=int, help="Override encoder CRF (defaults to config value).")
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg = _load_task_payload("vid_hevc_convert", args.config)
    roots = _as_paths(cfg.get("roots", []))
    if not roots:
        raise SystemExit("vid_hevc_convert config requires at least one root")

    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root = Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    csv_parts = cfg.get("csv_part") or [0]
    preset_cfg = args.preset or cfg.get("preset") or "slow"
    crf_cfg = args.crf if args.crf is not None else int(cfg.get("crf", 23))

    hevc_convert(
        roots=roots,
        output_dir=output_dir,
        output_root=output_root,
        csv_parts=csv_parts,
        dry_run=args.dry_run or dry_run_cfg,
        preset=preset_cfg,
        crf=crf_cfg,
    )
    return 0


def cli_vid_mkv_extract_subs(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract subtitle tracks listed in mkv_scan track reports.",
    )
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument(
        "--csv",
        action="append",
        help="Explicit mkv_scan_tracks CSV to consume; repeatable. Overrides config discovery.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate extraction without writing subtitle files.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite subtitle files if they already exist.",
    )
    parser.add_argument(
        "--mkvextract-bin",
        help="Override mkvextract binary path (defaults to config or PATH).",
    )
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg = _load_task_payload("vid_mkv_extract_subs", args.config)
    roots = _as_paths(cfg.get("roots", []))
    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root = Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    overwrite_cfg = bool(cfg.get("overwrite", False))
    csv_parts = cfg.get("csv_part") or []
    tracks_csv_types = cfg.get("tracks_csv_types")
    mkvextract_bin = (
        args.mkvextract_bin
        or cfg.get("mkvextract_bin")
        or "mkvextract"
    )

    targets: List[Path]
    if args.csv:
        targets = [Path(item).expanduser().resolve() for item in args.csv]
    else:
        definition_override = cfg.get("definition")
        if definition_override:
            targets = [Path(definition_override).expanduser().resolve()]
        else:
            if not roots:
                raise SystemExit("vid_mkv_extract_subs requires 'roots' when no CSV override is provided.")
            part_sequence = csv_parts if csv_parts else None
            targets = resolve_tracks_csvs(roots, output_root, part_sequence, tracks_csv_types)
            if not targets:
                raise SystemExit("Could not locate mkv_scan track CSVs for the requested configuration.")

    for csv_path in targets:
        summary = vid_mkv_extract_subs(
            csv_path=csv_path,
            output_dir=output_dir,
            mkvextract_bin=mkvextract_bin,
            overwrite=args.overwrite or overwrite_cfg,
            dry_run=args.dry_run or dry_run_cfg,
        )
        extracted = len(summary.get("extracted", []))
        skipped = len(summary.get("skipped", []))
        failed = len(summary.get("failed", []))
        print(
            f"Processed {csv_path}: extracted={extracted} skipped={skipped} failed={failed}",
        )
    return 0


def cli_vid_srt_clean(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Remove subtitle blocks that do not match the configured languages.",
    )
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument(
        "--root",
        action="append",
        help="Extra root directory to scan for SRT files (repeatable). Overrides config roots if provided.",
    )
    parser.add_argument(
        "--language",
        "-l",
        action="append",
        help="Language code to allow (repeatable). Overrides config languages when present.",
    )
    parser.add_argument(
        "--output-dir",
        help="Destination directory for filtered SRT files (defaults to config setting or same folder).",
    )
    parser.add_argument(
        "--suffix",
        help="Filename suffix appended when not overwriting in place.",
    )
    parser.add_argument(
        "--min-text-chars",
        type=int,
        help="Minimum character count required before language heuristics are applied.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Simulate the cleaning without writing files.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite SRT files in place.")
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg = _load_task_payload("vid_srt_clean", args.config)

    roots_cfg = _as_paths(cfg.get("roots", []))
    roots_override = _as_paths(args.root) if args.root else None
    roots = roots_override or roots_cfg
    if not roots:
        raise SystemExit("vid_srt_clean requires at least one root directory.")

    languages_cfg = list(cfg.get("languages", []))
    languages = args.language if args.language else languages_cfg
    if not languages:
        raise SystemExit("vid_srt_clean requires at least one language code.")

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else (
        Path(cfg["output_dir"]).expanduser() if cfg.get("output_dir") else None
    )
    file_suffix = args.suffix if args.suffix is not None else cfg.get("file_suffix", ".filtered")
    min_text_chars = args.min_text_chars if args.min_text_chars is not None else int(cfg.get("min_text_chars", 10))

    summary = vid_srt_clean(
        roots=roots,
        languages=languages,
        min_text_chars=min_text_chars,
        overwrite=args.overwrite or bool(cfg.get("overwrite", False)),
        output_dir=output_dir,
        file_suffix=file_suffix,
        dry_run=args.dry_run or bool(cfg.get("dry_run", False)),
    )

    processed = len(summary.get("processed", []))
    updated = len(summary.get("updated", []))
    skipped = len(summary.get("skipped", []))
    print(
        f"SRT clean complete: processed={processed} updated={updated} skipped={skipped}",
    )
    return 0


__all__ = [
    "cli_vid_mkv_clean",
    "cli_vid_mkv_extract_subs",
    "cli_vid_mkv_scan",
    "cli_vid_rename",
    "cli_vid_hevc_convert",
    "cli_vid_srt_clean",
]
