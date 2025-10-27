"""
video.hevc_convert

Convert non-HEVC MKV files to HEVC using ffmpeg.

Workflow summary:
 - Discover mkv_scan_non_hevc report exports (optionally batched via csv_part)
 - Transcode each listed MKV to HEVC (libx265) while copying non-video streams
 - Record conversion outcomes and emit per-run reports under the configured output directory
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from common.base.file_io import open_file
from common.base.fs import ensure_dir, human_size
from common.base.logging import get_logger
from common.base.ops import run_command
from common.shared.report import discover_latest_csvs, export_report, load_tabular_rows
from common.shared.utils import Progress

log = get_logger(__name__)

NON_HEVC_BASE_NAME = "mkv_scan_non_hevc"


def resolve_non_hevc_csvs(
    roots: Iterable[Path],
    output_root: Optional[Path | str],
    csv_parts: Optional[Iterable[int]] = None,
) -> List[Path]:
    """Return the latest mkv_scan_non_hevc report exports for the selected parts."""

    report_dirs: List[Path] = []
    for root in roots:
        root = Path(root).expanduser().resolve()
        reports_dir = (root / output_root).resolve() if output_root else (root / "reports").resolve()
        if not reports_dir.exists():
            log.debug(f"Reports directory missing under {root}: {reports_dir}")
            continue
        report_dirs.append(reports_dir)

    part_sequence = list(csv_parts) if csv_parts is not None else [0]
    if not part_sequence:
        part_sequence = [0]

    try:
        return discover_latest_csvs(report_dirs, NON_HEVC_BASE_NAME, part_sequence)
    except FileNotFoundError as exc:
        log.error(str(exc))
        return []


def _load_non_hevc_rows(report_path: Path) -> List[Dict[str, str]]:
    rows, _ = load_tabular_rows(report_path)
    normalized: List[Dict[str, str]] = []
    for row in rows:
        file_value = (row.get("path") or row.get("file") or "").strip()
        if not file_value:
            continue
        row.setdefault("file", file_value)
        normalized.append(row)
    return normalized


def _suffix_for(path: Path) -> str:
    suffixes = "".join(path.suffixes)
    return suffixes if suffixes else ".mkv"


def _build_output_path(source: Path) -> Path:
    return source.with_name(f"{source.stem}_hevc{_suffix_for(source)}")


def hevc_convert(
    roots: Optional[Iterable[Path | str]] = None,
    output_dir: Optional[Path] = None,
    output_root: Optional[Path | str] = None,
    csv_parts: Optional[Iterable[int]] = None,
    *,
    dry_run: bool = False,
    preset: str = "slow",
    crf: int = 23,
) -> List[Dict[str, str]]:
    """Convert non-HEVC MKVs listed in the latest mkv_scan_non_hevc report exports."""

    resolved_roots = [Path(p).expanduser().resolve() for p in (roots or [Path.cwd()])]
    if not resolved_roots:
        log.error("❌ No roots available for HEVC conversion.")
        return []

    csv_paths = resolve_non_hevc_csvs(resolved_roots, output_root, csv_parts)
    if not csv_paths:
        log.error("❌ Could not locate any mkv_scan_non_hevc reports to process.")
        return []

    base_output_dir = ensure_dir(output_dir or Path("./reports"))
    overall_results: List[Dict[str, str]] = []

    for csv_path in csv_paths:
        rows = _load_non_hevc_rows(csv_path)
        if not rows:
            log.warning(f"⚠️ Non-HEVC report is empty — skipping: {csv_path}")
            continue

        csv_label = csv_path.stem.replace(NON_HEVC_BASE_NAME, "").strip("_") or "latest"
        run_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        run_dir_candidate = base_output_dir / f"{run_stamp}_hevc_convert_{csv_label}"
        counter = 1
        while run_dir_candidate.exists():
            run_dir_candidate = base_output_dir / f"{run_stamp}_hevc_convert_{csv_label}_{counter:02d}"
            counter += 1
        run_dir = ensure_dir(run_dir_candidate)

        log.info(f"🚀 Converting files from {csv_path}")
        conversion_results: List[Dict[str, str]] = []

        for row in Progress(rows, desc="Converting"):
            file_value = (row.get("file") or "").strip()
            if not file_value:
                continue

            source_path = Path(file_value).expanduser()
            output_path = _build_output_path(source_path)
            result: Dict[str, str] = {
                "source": str(source_path),
                "output": str(output_path),
                "status": "pending",
                "message": "",
            }

            if not source_path.exists():
                result.update({
                    "status": "missing",
                    "message": "source file not found",
                })
                conversion_results.append(result)
                continue

            if output_path.exists():
                result.update({
                    "status": "skipped",
                    "message": "output already exists",
                })
                conversion_results.append(result)
                continue

            cmd = [
                "ffmpeg",
                "-y",
                "-i",
                str(source_path),
                "-c:v",
                "libx265",
                "-preset",
                preset,
                "-crf",
                str(crf),
                "-c:a",
                "copy",
                str(output_path),
            ]

            if dry_run:
                log.info(f"[DRY-RUN] Would convert {source_path} → {output_path}")
                result.update({
                    "status": "dry-run",
                    "message": "conversion skipped (dry-run)",
                })
                conversion_results.append(result)
                continue

            code, _, err = run_command(cmd, capture=True, stream=False)
            if code == 0 and output_path.exists():
                output_size = human_size(output_path.stat().st_size)
                result.update({
                    "status": "converted",
                    "message": f"output size {output_size}",
                })
                log.info(f"✅ Converted {source_path.name} → {output_path.name}")
            else:
                error_message = (err or "ffmpeg failed").strip()
                result.update({
                    "status": "error",
                    "message": error_message,
                })
                if output_path.exists():
                    output_path.unlink(missing_ok=True)
                log.error(f"💥 Failed to convert {source_path.name}: {error_message}")

            conversion_results.append(result)

        export_report(
            conversion_results,
            base_name="hevc_convert",
            output_dir=run_dir,
            write_csv_file=True,
            dry_run=dry_run,
        )

        overall_results.extend(conversion_results)

    return overall_results


__all__ = [
    "hevc_convert",
    "resolve_non_hevc_csvs",
]
