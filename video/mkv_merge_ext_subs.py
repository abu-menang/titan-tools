"""
video.mkv_merge_ext_subs

Merge external subtitle definitions produced by scan-tracks into MKV files.

Supports two CSV sources:
 - mkv_ext_subs_*.csv (legacy: scan_mkv_ext_subs_*.csv): existing MKVs with matched external subtitles.
 - vid_ext_subs_*.csv (legacy: scan_non_mkv_ext_subs_*.csv): non-MKV videos to be remuxed to MKV with matched subs.

The CSV rows follow EXTERNAL_SUB_COLUMNS from video.scan. Track metadata (name/lang/default/forced)
is applied per row using mkvmerge track options.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from common.base.fs import ensure_dir, ensure_parent
from common.base.logging import get_logger
from common.base.ops import move_file, run_command
from common.shared.loader import load_task_config
from common.shared.report import discover_latest_csvs, load_tabular_rows
from common.shared.utils import Progress

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

@dataclass
class TrackSpec:
    track_id: str
    type: str
    lang: str
    name: str
    default: Optional[bool]
    forced: Optional[bool]
    input_path: Path

    def track_name(self) -> str:
        return self.name


@dataclass
class MergeJob:
    source: Path
    output: Path
    tracks: List[TrackSpec]


def _parse_bool(val: object) -> Optional[bool]:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    text = str(val).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _load_jobs_from_csv(csv_path: Path, *, output_dir: Optional[Path] = None) -> List[MergeJob]:
    rows, _ = load_tabular_rows(csv_path)
    grouped: Dict[str, List[dict]] = {}

    for row in rows:
        output_token = (row.get("output_path") or "").strip()
        if not output_token:
            # Fallback to output_filename if present
            fname = (row.get("output_filename") or "").strip()
            if not fname:
                log.debug(f"Skipping row without output info: {row}")
                continue
            output_token = fname

        grouped.setdefault(output_token, []).append(row)

    jobs: List[MergeJob] = []
    for output_token, job_rows in grouped.items():
        # Determine primary source from the first video row; fall back to audio row input_path.
        source_token = ""
        for candidate_type in ("video", "audio"):
            for r in job_rows:
                if (r.get("type") or "").lower() == candidate_type:
                    source_token = (r.get("input_path") or "").strip() or (r.get("output_path") or "").strip()
                    break
            if source_token:
                break
        if not source_token:
            log.warning(f"Cannot determine source for output '{output_token}' in {csv_path.name}; skipping.")
            continue

        source = Path(source_token).expanduser()
        output_candidate = Path(output_token)
        if output_dir:
            output = ensure_dir(output_dir) / output_candidate.name
        else:
            output = output_candidate.expanduser()

        tracks: List[TrackSpec] = []
        for r in sorted(job_rows, key=lambda x: (x.get("type", ""), x.get("id", ""))):
            ttype = (r.get("type") or "").strip().lower()
            tid = str(r.get("id") or "").strip()
            if ttype not in {"video", "audio", "subtitles"} or not tid:
                continue
            input_path = Path((r.get("input_path") or "").strip() or source)
            lang = (r.get("lang") or "").strip()
            name = (r.get("edited_name") or r.get("name") or "").strip()
            default = _parse_bool(r.get("default"))
            forced = _parse_bool(r.get("forced"))
            tracks.append(
                TrackSpec(
                    track_id=tid,
                    type=ttype,
                    lang=lang,
                    name=name,
                    default=default,
                    forced=forced,
                    input_path=input_path.expanduser(),
                )
            )

        if not tracks:
            log.warning(f"No usable tracks for output '{output}'; skipping.")
            continue

        jobs.append(MergeJob(source=source, output=output, tracks=tracks))

    return jobs


# ---------------------------------------------------------------------------
# mkvmerge command construction
# ---------------------------------------------------------------------------

def _bool_flag(value: Optional[bool]) -> Optional[str]:
    if value is None:
        return None
    return "yes" if value else "no"


def _build_mkvmerge_command(job: MergeJob, tmp_output: Path) -> Tuple[List[str], List[Path]]:
    cmd: List[str] = ["mkvmerge", "-o", str(tmp_output)]
    attached_inputs: List[Path] = []

    # First input is always the source video/audio container.
    # Apply metadata to video/audio tracks on input 0.
    for track in job.tracks:
        if track.type not in {"video", "audio"}:
            continue
        tid = track.track_id
        if track.name:
            cmd += ["--track-name", f"0:{track.name}"]
        if track.lang:
            cmd += ["--language", f"0:{track.lang}"]
        if track.default is not None:
            cmd += ["--default-track", f"0:{_bool_flag(track.default)}"]
        if track.forced is not None:
            cmd += ["--forced-track", f"0:{_bool_flag(track.forced)}"]
    cmd.append(str(job.source))

    # Additional inputs: external subtitle files, each as its own source.
    for track in job.tracks:
        if track.type != "subtitles":
            continue
        if not track.input_path:
            log.warning(f"Subtitle track missing input_path for {job.output.name}; skipping.")
            continue
        if track.lang:
            cmd += ["--language", f"0:{track.lang}"]
        if track.name:
            cmd += ["--track-name", f"0:{track.name}"]
        if track.default is not None:
            cmd += ["--default-track", f"0:{_bool_flag(track.default)}"]
        if track.forced is not None:
            cmd += ["--forced-track", f"0:{_bool_flag(track.forced)}"]
        cmd.append(str(track.input_path))
        attached_inputs.append(track.input_path)

    return cmd, attached_inputs


# ---------------------------------------------------------------------------
# CSV discovery
# ---------------------------------------------------------------------------

def resolve_convert_ext_subs_csvs(
    roots: Iterable[Path],
    output_root: Optional[Path | str],
    csv_parts: Optional[Iterable[int]] = None,
    sources: Optional[Iterable[str]] = None,
) -> List[Path]:
    """
    Discover external-subtitle merge definitions.

    sources can include: "mkv" (mkv_ext_subs), "non_mkv" (vid_ext_subs).
    If omitted, both are searched. Legacy scan_mkv_ext_subs/scan_non_mkv_ext_subs and
    mkv_scan_convert_ext_subs are also supported for backward compatibility.
    """
    normalized_sources = {str(s).strip().lower() for s in (sources or ["mkv", "non_mkv"])}
    base_names: List[str] = []
    if "mkv" in normalized_sources:
        base_names.append("mkv_ext_subs")
        base_names.append("scan_mkv_ext_subs")  # legacy
        base_names.append("mkv_scan_convert_ext_subs")  # legacy
    if "non_mkv" in normalized_sources:
        base_names.append("vid_ext_subs")
        base_names.append("scan_non_mkv_ext_subs")  # legacy

    report_dirs: List[Path] = []
    for root in roots:
        root = root.expanduser().resolve()
        reports_dir = (root / output_root).resolve() if output_root else (root / "reports").resolve()
        if reports_dir.exists():
            report_dirs.append(reports_dir)

    results: List[Path] = []
    for base_name in base_names:
        try:
            matches = discover_latest_csvs(report_dirs, base_name, csv_parts)
        except FileNotFoundError:
            matches = []
        for m in matches:
            if m not in results:
                results.append(m)
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def vid_mkv_merge_ext_subs(
    def_file: Path,
    output_dir: Optional[Path] = None,
    dry_run: bool = False,
) -> Dict[str, List[Path]]:
    """
    Merge or remux files using external subtitle definitions from a scan CSV.

    Returns a dict with keys: merged, failed, skipped.
    """
    csv_path = Path(def_file).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(f"Definition CSV not found: {csv_path}")

    output_dir_path = Path(output_dir).expanduser() if output_dir else None
    jobs = _load_jobs_from_csv(csv_path, output_dir=output_dir_path)
    if not jobs:
        log.warning(f"No merge jobs found in {csv_path}")
        return {"merged": [], "failed": [], "skipped": []}

    merged: List[Path] = []
    failed: List[Path] = []
    skipped: List[Path] = []

    for job in Progress(jobs, desc="Merging"):
        if not job.source.exists():
            log.warning(f"Source missing → {job.source}")
            failed.append(job.output)
            continue

        dest = job.output.expanduser()
        tmp_output = dest.with_suffix(dest.suffix + ".tmp")
        ensure_parent(tmp_output)

        cmd, _ = _build_mkvmerge_command(job, tmp_output)
        if dry_run:
            log.info(f"[DRY-RUN] Would run: {' '.join(cmd)}")
            skipped.append(dest)
            continue

        log.debug(f"Running mkvmerge: {' '.join(cmd)}")
        code, out, err = run_command(cmd, capture=True, stream=False)
        if code != 0:
            log.error(f"mkvmerge failed for {job.source.name}: {err.strip() if err else 'unknown error'}")
            if out:
                log.error(out)
            failed.append(dest)
            if tmp_output.exists():
                tmp_output.unlink(missing_ok=True)  # type: ignore[arg-type]
            continue

        try:
            move_file(tmp_output, dest, dry_run=False)
            merged.append(dest)
            log.info(f"✅ Merged → {dest}")
        except Exception as move_err:
            log.error(f"Move failed for {dest}: {move_err}")
            failed.append(dest)
            if tmp_output.exists():
                tmp_output.unlink(missing_ok=True)  # type: ignore[arg-type]

    return {"merged": merged, "failed": failed, "skipped": skipped}
