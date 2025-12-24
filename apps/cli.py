"""Command-line entry points for Titan Tools tasks.

These helpers mirror the legacy bash wrappers but can be installed as
`console_scripts`, making them runnable from any directory while supporting
shell auto-completion via ``argcomplete``.
"""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, cast

try:  # pragma: no cover - completion optional in tests
    import argcomplete  # type: ignore
except Exception:  # pragma: no cover
    argcomplete = None  # type: ignore[assignment]

from common.base.logging import setup_logging
from common.shared.loader import load_output_dirs, load_task_config
from video.rename import resolve_name_list_csvs, vid_rename
from common.utils.track_utils import resolve_tracks_csvs
from video.scanners.scan_tracks import vid_mkv_scan
from video.scanners.scan_hevc import vid_mkv_scan_hevc
from video.hevc_convert import hevc_convert
from video.mkv_extract_subtitles import vid_mkv_extract_subs
from video.srt_clean import vid_srt_clean
from video.cleaners.cleaner import run_cleaner


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


def _configure_logging(logging_cfg: Dict[str, Any], log_dir_override: Path | None = None) -> None:
    if not logging_cfg:
        return

    log_dir = str(log_dir_override) if log_dir_override else logging_cfg.get("log_dir")

    setup_logging(
        level=logging_cfg.get("level"),
        use_rich=_normalize_use_rich(logging_cfg.get("use_rich")),
        log_dir=log_dir,
        file_prefix=logging_cfg.get("file_prefix"),
    )


def _resolve_config_path(explicit: Optional[str]) -> Optional[Path]:
    if explicit:
        return Path(explicit).expanduser().resolve()
    if DEFAULT_CONFIG_PATH.exists():  # pragma: no branch - default install path
        return DEFAULT_CONFIG_PATH
    return None


def _load_task_payload(
    task: str,
    config_arg: Optional[str],
    *,
    configure_logging: bool = False,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    config_path = _resolve_config_path(config_arg)
    raw = load_task_config(task, str(config_path) if config_path else None)
    payload: Dict[str, Any] = dict(raw)
    logging_cfg = payload.pop("__logging__", {}) or {}
    if configure_logging:
        _configure_logging(logging_cfg)
    return payload, logging_cfg


def _resolve_required_output_dir(cfg: Dict[str, Any], target_dir_name: str) -> Path:
    output_root_val = cfg.get("tracks_root") or cfg.get("__output_root__") or cfg.get("output_root")
    output_root = Path(output_root_val) if output_root_val else None
    output_dir_cfg = cfg.get("output_dir")
    if output_dir_cfg:
        output_dir = Path(output_dir_cfg)
    elif output_root:
        output_dir = output_root / target_dir_name
    else:
        raise SystemExit("output_dir not provided and __output_root__ missing; cannot continue.")

    output_dir = output_dir.expanduser().resolve()
    if not output_dir.is_dir():
        raise SystemExit(f"output_dir not found: {output_dir}")
    return output_dir


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

    cfg, logging_cfg = _load_task_payload("vid_mkv_scan", args.config)
    roots = _as_paths(cfg.get("roots", []))
    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root_val = cfg.get("tracks_root") or cfg.get("__output_root__") or cfg.get("output_root")
    output_root = Path(output_root_val) if output_root_val else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    log_dir_override = (output_dir or output_root) / "logs" if (output_dir or output_root) else None
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)
    vid_mkv_scan(
        roots=roots or None,
        output_dir=output_dir,
        output_root=output_root,
        write_csv_file=not args.no_write,
        dry_run=args.dry_run or dry_run_cfg,
        batch_size=cfg.get("batch_size"),
    )
    return 0


def cli_vid_scan_hevc(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Scan for non-HEVC videos and emit reports.")
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
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Limit rows per CSV; if omitted uses config.",
    )
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg, logging_cfg = _load_task_payload("vid_scan_hevc", args.config)
    roots = _as_paths(cfg.get("roots", []))
    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root_val = cfg.get("hevc_root") or cfg.get("__output_root__") or cfg.get("output_root")
    output_root = Path(output_root_val) if output_root_val else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    log_dir_override = (output_dir or output_root) / "logs" if (output_dir or output_root) else None
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)
    vid_mkv_scan_hevc(
        roots=roots or None,
        output_dir=output_dir,
        output_root=output_root,
        write_csv_file=not args.no_write and bool(cfg.get("write_csv_file", True)),
        dry_run=args.dry_run or dry_run_cfg,
        batch_size=args.batch_size if args.batch_size is not None else cfg.get("batch_size"),
    )
    return 0


def cli_vid_mkv_clean(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Clean MKVs using mkv_scan track definitions.")
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run behaviour.")
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg, logging_cfg = _load_task_payload("vid_mkv_clean", args.config)
    roots = _as_paths(cfg.get("roots", []))
    if not roots:
        raise SystemExit("vid_mkv_clean config requires at least one root")

    try:
        from video import mkv_clean
    except Exception as exc:  # pragma: no cover - import should generally succeed
        raise SystemExit(f"Failed to import video.mkv_clean: {exc}") from exc

    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root = Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None
    log_dir_override = (output_dir or output_root) / "logs" if (output_dir or output_root) else None
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)
    dry_run_cfg = bool(cfg.get("dry_run", False))
    definition_override = cfg.get("definition")
    csv_parts = cfg.get("csv_part") or []

    if definition_override:
        targets = [Path(definition_override).expanduser().resolve()]
    else:
        resolver = getattr(mkv_clean, "resolve_tracks_csvs", None)
        if resolver is None:
            raise SystemExit("video.mkv_clean.resolve_tracks_csvs is missing; cannot locate track CSVs.")
        part_sequence = csv_parts if csv_parts else None
        targets = resolver(roots, output_root, part_sequence)
        if not targets:
            raise SystemExit("Could not locate mkv_scan track CSVs for the requested configuration.")

    for definition_path in targets:
        mkv_clean.vid_mkv_clean(
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

    cfg, logging_cfg = _load_task_payload("vid_rename", args.config)
    roots = _as_paths(cfg.get("roots", [])) if cfg.get("roots") else None
    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root = Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    log_dir_override = (output_dir or output_root) / "logs" if (output_dir or output_root) else None
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)
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
            roots=cast(List[Path | str], roots or []),
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

    cfg, logging_cfg = _load_task_payload("vid_hevc_convert", args.config)
    roots = _as_paths(cfg.get("roots", []))
    if not roots:
        raise SystemExit("vid_hevc_convert config requires at least one root")

    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root = Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    log_dir_override = (output_dir or output_root) / "logs" if (output_dir or output_root) else None
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)
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


def cli_vid_cleaner(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Discover clean_dir CSVs and run the cleaner workflow from YAML config."
    )
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run behaviour.")
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg, logging_cfg = _load_task_payload("vid_cleaner", args.config)
    roots = _as_paths(cfg.get("roots", []))
    out_dirs_cfg = load_output_dirs()
    target_dir_name = str(out_dirs_cfg.get("clean_dir") or "03_clean")
    output_dir = _resolve_required_output_dir(cfg, target_dir_name)
    output_root_val = cfg.get("tracks_root") or cfg.get("__output_root__") or cfg.get("output_root")
    output_root = Path(output_root_val) if output_root_val else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    log_dir_override = (output_dir or output_root) / "logs" if (output_dir or output_root) else None
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)

    run_cleaner(
        roots=roots or None,
        output_root=output_root,
        output_dir=output_dir,
        dry_run=args.dry_run or dry_run_cfg,
        clean_dir=target_dir_name,
    )
    return 0


def _cli_clean_target(
    argv: Optional[Iterable[str]],
    target_dir_key: str,
    default_dir: str,
    task_name: str,
    extra_tags: Optional[list[str]] = None,
) -> int:
    parser = argparse.ArgumentParser(
        description=f"Run cleaner workflow targeting {default_dir} directory.",
    )
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run behaviour.")
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg, logging_cfg = _load_task_payload("vid_cleaner", args.config)
    roots = _as_paths(cfg.get("roots", []))
    out_dirs_cfg = load_output_dirs()
    target_dir_name = str(out_dirs_cfg.get(target_dir_key) or default_dir)
    output_dir = _resolve_required_output_dir(cfg, target_dir_name)
    output_root_val = cfg.get("tracks_root") or cfg.get("__output_root__") or cfg.get("output_root")
    output_root = Path(output_root_val) if output_root_val else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    log_dir_override = (output_dir or output_root) / "logs" if (output_dir or output_root) else None
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)

    run_cleaner(
        roots=roots or None,
        output_root=output_root,
        output_dir=output_dir,
        dry_run=args.dry_run or dry_run_cfg,
        clean_dir=target_dir_name,
        extra_tags=extra_tags,
    )
    return 0


def cli_clean_01(argv: Optional[Iterable[str]] = None) -> int:
    return _cli_clean_target(argv, "no_sub_mkv_dir", "01_no_sub_mkv", "clean_01", extra_tags=["no_sub"])


def cli_clean_02(argv: Optional[Iterable[str]] = None) -> int:
    return _cli_clean_target(argv, "no_sub_vid_dir", "02_no_sub_vid", "clean_02", extra_tags=["no_sub"])


def cli_clean_01_hs(argv: Optional[Iterable[str]] = None) -> int:
    return _cli_clean_target(argv, "no_sub_mkv_dir", "01_no_sub_mkv", "clean_01_hs", extra_tags=["hard_sub"])


def cli_clean_02_hs(argv: Optional[Iterable[str]] = None) -> int:
    return _cli_clean_target(argv, "no_sub_vid_dir", "02_no_sub_vid", "clean_02_hs", extra_tags=["hard_sub"])


def cli_clean_03(argv: Optional[Iterable[str]] = None) -> int:
    return _cli_clean_target(argv, "clean_dir", "03_clean", "clean_03")


def cli_clean_04(argv: Optional[Iterable[str]] = None) -> int:
    return _cli_clean_target(argv, "convert_clean_dir", "04_convert_clean", "clean_04")


def cli_clean_05(argv: Optional[Iterable[str]] = None) -> int:
    return _cli_clean_target(argv, "merge_clean_dir", "05_merge_clean", "clean_05")


def cli_clean_06(argv: Optional[Iterable[str]] = None) -> int:
    return _cli_clean_target(argv, "convert_merge_clean_dir", "06_convert_merge_clean", "clean_06")


def _load_conv_cleaner():
    conv_path = Path(__file__).resolve().parents[1] / "video" / "cleaners" / "conv_clean.py"
    spec = importlib.util.spec_from_file_location("video.conv_clean", conv_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load conv_clean from {conv_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cli_vid_conv_cleaner(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Discover no_sub_vid_dir CSVs and run conversion cleaner workflow to MKV.",
    )
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run behaviour.")
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg, logging_cfg = _load_task_payload("vid_conv_cleaner", args.config)
    roots = _as_paths(cfg.get("roots", []))
    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root = Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    log_dir_override = (output_dir or output_root) / "logs" if (output_dir or output_root) else None
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)

    conv_mod = _load_conv_cleaner()
    run_conv_cleaner = getattr(conv_mod, "run_conv_cleaner", None)
    if run_conv_cleaner is None:
        raise ImportError("conv_clean.py missing run_conv_cleaner")

    run_conv_cleaner(
        roots=roots or None,
        output_root=output_root,
        output_dir=output_dir,
        dry_run=args.dry_run or dry_run_cfg,
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

    cfg, logging_cfg = _load_task_payload("vid_mkv_extract_subs", args.config)
    roots = _as_paths(cfg.get("roots", []))
    output_dir = Path(cfg["output_dir"]) if cfg.get("output_dir") else None
    output_root = Path(cfg["__output_root__"]) if cfg.get("__output_root__") else None
    dry_run_cfg = bool(cfg.get("dry_run", False))
    overwrite_cfg = bool(cfg.get("overwrite", False))
    log_dir_override = (output_dir or output_root) / "logs" if (output_dir or output_root) else None
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)
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

    cfg, logging_cfg = _load_task_payload("vid_srt_clean", args.config)

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
    output_root = Path(cfg["__output_root__"]).expanduser() if cfg.get("__output_root__") else None
    log_base = output_dir or output_root
    log_dir_override = log_base / "logs" if log_base else None
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)
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


def cli_vid_tagger(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Apply filesystem tags to files listed in CSVs.")
    parser.add_argument("--config", "-c", help="Path to configuration YAML (defaults to repo config).")
    parser.add_argument("--dry-run", action="store_true", help="Simulate tagging without writing tags.")
    _enable_autocomplete(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg, logging_cfg = _load_task_payload("vid_tagger", args.config)
    roots = _as_paths(cfg.get("roots", []))
    csv_dir_cfg = Path(cfg["csv_dir"]).expanduser() if cfg.get("csv_dir") else None
    log_dir_override = None
    if csv_dir_cfg:
        if csv_dir_cfg.is_absolute():
            log_dir_override = csv_dir_cfg / "logs"
        elif roots:
            log_dir_override = roots[0] / csv_dir_cfg / "logs"
    _configure_logging(logging_cfg, log_dir_override=log_dir_override)

    from video.tagger import tag_files_from_csv_dir

    res = tag_files_from_csv_dir(
        cfg["csv_dir"],
        roots,
        cfg.get("tags"),
        dry_run=args.dry_run or bool(cfg.get("dry_run", False)),
    )
    print(
        f"Tagging complete: csvs={res.get('csvs')} tagged={res.get('tagged')} "
        f"skipped={res.get('skipped')} missing={res.get('missing')}"
    )
    return 0


__all__ = [
    "cli_vid_cleaner",
    "cli_vid_conv_cleaner",
    "cli_vid_mkv_clean",
    "cli_vid_mkv_extract_subs",
    "cli_vid_mkv_scan",
    "cli_vid_scan_hevc",
    "cli_vid_rename",
    "cli_vid_hevc_convert",
    "cli_vid_srt_clean",
]
