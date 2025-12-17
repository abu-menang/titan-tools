"""
Encode videos listed in the latest non_HEVC CSV using HandBrake (H.265 NVENC 2160p4K).

- Discovers the newest `non_hevc_*.csv` under <root>/<hevc_root>/<encode_dir>/.
- Moves the original file into <root>/<hevc_root>/<encode_dir>/ori/.
- Writes the encoded MKV back to the original location (same stem, .mkv extension).
- Stores run logs under <root>/<hevc_root>/<encode_dir>/logs/.
- Clears existing tags and sets timestamp + FINAL on the encoded file.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from common.base.fs import ensure_dir
from common.base.logging import get_logger
from common.base.ops import move_file, run_command
from common.shared.loader import load_config, load_output_dirs
from common.shared.report import discover_latest_csvs, load_tabular_rows
from common.shared.utils import Progress
from common.utils.tag_utils import write_fs_tag

log = get_logger(__name__)

NON_HEVC_BASE_NAME = "non_hevc"
DEFAULT_HEVC_ROOT = Path("./00_hevc")


def _load_defaults(config_path: Optional[Path]) -> Tuple[List[Path], Path, str]:
    cfg = load_config(config_path)
    task_defaults: Dict[str, object] = {}
    if isinstance(cfg, dict):
        task_defaults = cfg.get("task_defaults") or {}
        if not isinstance(task_defaults, dict):
            task_defaults = {}

    roots = [
        Path(str(p)).expanduser().resolve()
        for p in task_defaults.get("roots") or []
        if str(p).strip()
    ]

    hevc_root_raw = task_defaults.get("hevc_root") or DEFAULT_HEVC_ROOT
    hevc_root = Path(str(hevc_root_raw)).expanduser()

    output_dirs = load_output_dirs()
    encode_dir_name = str(output_dirs.get("encode_dir") or "encode")

    return roots, hevc_root, encode_dir_name


def _resolve_encode_dir(root: Path, hevc_root: Path, encode_dir_name: str) -> Path:
    base = hevc_root
    if not base.is_absolute():
        base = (root / base).resolve()
    return base / encode_dir_name


def _configure_file_logging(base_dir: Path) -> logging.Handler:
    logs_dir = ensure_dir(base_dir / "logs")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = logs_dir / f"encoder_{timestamp}.log"
    handler = logging.FileHandler(log_path)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(handler)
    log.info("📝 Log file: %s", log_path)
    return handler


def _latest_non_hevc_csv(base_dir: Path) -> Optional[Path]:
    try:
        latest = discover_latest_csvs([base_dir], NON_HEVC_BASE_NAME, [0])
    except FileNotFoundError:
        return None
    return latest[0] if latest else None


def _extract_paths_from_csv(csv_path: Path) -> List[Path]:
    rows, _ = load_tabular_rows(csv_path)
    ordered: List[Path] = []
    seen = set()
    for row in rows:
        candidate: Optional[str] = None
        for key in ("output_path", "path", "input_path", "file"):
            val = str(row.get(key) or "").strip()
            if val:
                candidate = val
                break
        if not candidate:
            continue
        p = Path(candidate).expanduser()
        if p in seen:
            continue
        seen.add(p)
        ordered.append(p)
    return ordered


def _unique_backup_path(directory: Path, name: str) -> Path:
    target = directory / name
    counter = 1
    while target.exists():
        stem = Path(name).stem
        suffix = Path(name).suffix
        target = directory / f"{stem}_{counter:02d}{suffix}"
        counter += 1
    return target


def _encode_file(source: Path, base_dir: Path, *, dry_run: bool = False) -> Dict[str, str]:
    result = {"source": str(source), "status": "pending", "message": ""}

    if not source.exists():
        result.update({"status": "missing", "message": "source file not found"})
        log.warning("Missing source file: %s", source)
        return result

    ori_dir = ensure_dir(base_dir / "ori")
    backup_path = _unique_backup_path(ori_dir, source.name)
    output_path = source.with_suffix(".mkv")

    if output_path.exists():
        result.update({"status": "skipped", "message": "output already exists"})
        log.info("Skipping existing output for %s", source)
        return result

    if dry_run:
        log.info("[DRY-RUN] Would move %s → %s", source, backup_path)
        log.info("[DRY-RUN] Would encode %s → %s", backup_path, output_path)
        result.update({"status": "dry-run", "message": "skipped (dry-run)"})
        return result

    try:
        move_file(source, backup_path)
    except Exception as exc:
        result.update({"status": "error", "message": f"move failed: {exc}"})
        return result

    cmd = [
        "HandBrakeCLI",
        "-i",
        str(backup_path),
        "-o",
        str(output_path),
        "-Z",
        "H.265 NVENC 2160p4K",
        "-f",
        "mkv",
    ]
    log.info("🎞️ Encoding %s → %s", backup_path.name, output_path)
    code, out, err = run_command(cmd, capture=True, stream=False)
    if out:
        log.info(out)
    if err:
        log.error(err)

    if code != 0 or not output_path.exists():
        result.update({"status": "error", "message": f"encode failed (code={code})"})
        log.error("Encoding failed for %s (code=%s)", backup_path, code)
        if not source.exists():
            try:
                move_file(backup_path, source)
                log.info("Restored original to %s", source)
            except Exception as exc:  # pragma: no cover - safety
                log.error("Failed to restore original %s: %s", source, exc)
        return result

    timestamp_tag = datetime.now().strftime("%Y-%m-%d_%H-%M")
    try:
        write_fs_tag(output_path, "user.xdg.tags", "")
        if not write_fs_tag(output_path, "user.xdg.tags", f"{timestamp_tag},FINAL"):
            log.warning("Failed to tag encoded file: %s", output_path)
    except Exception as exc:  # pragma: no cover - safety
        log.warning("Tagging error for %s: %s", output_path, exc)

    result.update({"status": "encoded", "message": "success", "output": str(output_path)})
    log.info("✅ Encoded %s", output_path.name)
    return result


def process_root(
    root: Path,
    hevc_root: Path,
    encode_dir_name: str,
    *,
    csv_path: Optional[Path] = None,
    dry_run: bool = False,
) -> Dict[str, object]:
    base_dir = ensure_dir(_resolve_encode_dir(root, hevc_root, encode_dir_name))
    handler = _configure_file_logging(base_dir)

    try:
        target_csv = csv_path or _latest_non_hevc_csv(base_dir)
        if not target_csv:
            log.warning("No non_hevc CSV found under %s", base_dir)
            return {"root": str(root), "status": "no_csv"}

        log.info("Using CSV: %s", target_csv)
        targets = _extract_paths_from_csv(target_csv)
        if not targets:
            log.warning("CSV contains no valid paths: %s", target_csv)
            return {"root": str(root), "status": "empty_csv", "csv": str(target_csv)}

        results: List[Dict[str, str]] = []
        for path in Progress(targets, desc="Encoding"):
            results.append(_encode_file(path, base_dir, dry_run=dry_run))

        return {
            "root": str(root),
            "csv": str(target_csv),
            "status": "completed",
            "processed": len(results),
        }
    finally:
        log.removeHandler(handler)
        handler.close()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Encode videos to HEVC using HandBrake (NVENC 4K).")
    parser.add_argument("--config", "-c", type=Path, help="Path to config YAML (defaults to configs/config.yaml).")
    parser.add_argument("--root", action="append", type=Path, help="Root directory; may be provided multiple times.")
    parser.add_argument("--csv", type=Path, help="Explicit path to a non_hevc CSV.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate actions without moving/encoding.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    config_path = args.config or Path("configs/config.yaml")
    cfg_roots, hevc_root, encode_dir_name = _load_defaults(config_path)

    roots = [Path(p).expanduser().resolve() for p in (args.root or cfg_roots)]
    if not roots:
        raise SystemExit("No roots provided via --root or config task_defaults.roots")

    overall: List[Dict[str, object]] = []
    for root in roots:
        overall.append(process_root(root, hevc_root, encode_dir_name, csv_path=args.csv, dry_run=args.dry_run))

    log.info("Run summary: %s", overall)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
