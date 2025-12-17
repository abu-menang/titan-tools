"""
Scan for non-HEVC video files and write only the non-HEVC reports.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Set

from common.base.fs import ensure_dir
from common.base.logging import get_logger
from common.shared.loader import load_scan_config
from common.shared.report import ColumnSpec, write_tabular_reports
from common.utils.fs_utils import iter_files
from common.utils.probe_utils import probe_mkvmerge
from common.utils.subtitle_utils import match_external_subs
from common.utils.tag_utils import read_fs_tags
from common.utils.track_utils import extract_tracks

log = get_logger(__name__)

_SCAN_CFG = load_scan_config(log)
MEDIA_TYPES = _SCAN_CFG.media_types
VIDEO_EXTS: set[str] = set(MEDIA_TYPES.video_exts)
MKV_EXTS: set[str] = {".mkv"}
SUBTITLE_EXTS: set[str] = set(MEDIA_TYPES.subtitle_exts)
TRACK_COLUMNS: List[ColumnSpec] = _SCAN_CFG.columns.get("track", [])
BASE_DIR_MAP = _SCAN_CFG.base_dir_map


@dataclass
class _ProbeResult:
    path: Path
    tracks: List[Dict[str, str]] = field(default_factory=list)
    failure_reason: Optional[str] = None


def vid_mkv_scan_hevc(
    roots: Optional[Iterable[Path | str]] = None,
    output_dir: Optional[Path] = None,
    output_root: Optional[Path] = None,
    write_csv_file: bool = True,
    dry_run: bool = False,
    batch_size: Optional[int] = None,  # kept for parity; unused
) -> List[Dict[str, object]]:
    roots = [Path(p).expanduser() for p in (roots or [Path.cwd()])]
    resolved_roots = [p.resolve() for p in roots]
    log.info("🎬 === Setup (Non-HEVC) ===")
    log.info("roots=%s", ",".join(str(r) for r in resolved_roots))
    log.info("output_dir=%s output_root=%s dry_run=%s", output_dir, output_root, dry_run)

    primary_root = resolved_roots[0] if resolved_roots else Path.cwd()
    encode_dir_name = BASE_DIR_MAP.get("encode_dir", "encode")

    def _resolve_base_dir() -> Path:
        if output_dir:
            base = Path(output_dir).expanduser()
            if not base.is_absolute():
                base = (primary_root / base).resolve()
            return base
        if output_root:
            base = Path(output_root).expanduser()
            if not base.is_absolute():
                base = (primary_root / base).resolve()
            return base
        return primary_root

    base_output_dir = _resolve_base_dir() / encode_dir_name
    if not dry_run:
        ensure_dir(base_output_dir)
    log.info("📁 report_dir=%s", base_output_dir)

    mkv_files: List[Path] = []
    vid_files: List[Path] = []
    sub_files: List[Path] = []
    tags_by_path: Dict[Path, str] = {}

    # Collect files and tags
    for f in iter_files(resolved_roots, exclude_dir=base_output_dir, include_all=True):
        if f.is_dir() or f.name.lower() == ".directory":
            continue
        suf = f.suffix.lower()
        if suf in MKV_EXTS:
            mkv_files.append(f)
        elif suf in VIDEO_EXTS:
            vid_files.append(f)
        elif suf in SUBTITLE_EXTS:
            sub_files.append(f)
        else:
            continue
        tags_raw, _ = read_fs_tags(f)
        rp = f.expanduser().resolve()
        tags_by_path[rp] = tags_raw or ""
        tags_by_path.setdefault(rp.with_suffix(".mkv"), tags_raw or "")

    def _tag_for_path(p: Path) -> str:
        rp = p.expanduser().resolve()
        return tags_by_path.get(rp) or tags_by_path.get(rp.with_suffix(".mkv")) or ""

    def _probe_list(files: List[Path]) -> List[_ProbeResult]:
        results: List[_ProbeResult] = []
        for p in files:
            code, payload, err = probe_mkvmerge(p)
            tag_val = _tag_for_path(p)
            tracks: List[Dict[str, str]] = []
            if payload:
                tracks = extract_tracks(p, payload)
                for tr in tracks:
                    tr["tags"] = tag_val
                results.append(_ProbeResult(path=p, tracks=tracks))
            else:
                results.append(_ProbeResult(path=p, failure_reason=err or "probe_failed"))
            if payload:
                has_hevc = any(
                    (t.get("type") or "").lower() == "video"
                    and "hevc" in (t.get("codec") or "").lower()
                    for t in tracks
                )
                log.info('🔍 probed "%s" hevc=%s', p, "yes" if has_hevc else "no")
        return results

    mkv_probe = [r for r in _probe_list(mkv_files) if not r.failure_reason]
    non_mkv_probe = [r for r in _probe_list(vid_files) if not r.failure_reason]
    sub_probe = [r for r in _probe_list(sub_files) if not r.failure_reason]

    mkv_ext_sub_rows, non_mkv_ext_sub_rows, unmatched_subs_paths = match_external_subs(mkv_probe + non_mkv_probe, sub_probe)

    def _apply_tags(rows: List[Dict[str, str]]):
        for r in rows:
            if r.get("tags") not in (None, ""):
                continue
            candidate = r.get("output_path") or r.get("path") or r.get("input_path")
            if not candidate:
                r["tags"] = ""
                continue
            try:
                r["tags"] = _tag_for_path(Path(str(candidate)))
            except Exception:
                r["tags"] = ""

    _apply_tags(mkv_ext_sub_rows)
    _apply_tags(non_mkv_ext_sub_rows)

    # Non-HEVC detection
    def _non_hevc(rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
        out: List[Dict[str, object]] = []
        by_file: Dict[str, Set[str]] = {}
        for r in rows:
            if (r.get("type") or "").lower() != "video":
                continue
            key = r.get("output_path") or r.get("path") or ""
            by_file.setdefault(key, set()).add(r.get("codec", ""))
        for path, codecs in by_file.items():
            if codecs and not any("hevc" in c.lower() for c in codecs):
                p = Path(path)
                out.append({
                    "tags": _tag_for_path(p),
                    "output_filename": p.name,
                    "type": "video",
                    "id": "",
                    "name": "",
                    "edited_name": "",
                    "lang": "",
                    "codec": ", ".join(sorted(codecs)),
                    "default": "",
                    "forced": "",
                    "encoding": "",
                    "input_path": path,
                    "output_path": path,
                })
        return out

    mkv_rows: List[Dict[str, str]] = [dict((k, str(v)) for k, v in r.items()) for r in _non_hevc(mkv_ext_sub_rows + [tr for r in mkv_probe for tr in r.tracks])]
    non_mkv_rows: List[Dict[str, str]] = [dict((k, str(v)) for k, v in r.items()) for r in _non_hevc(non_mkv_ext_sub_rows + [tr for r in non_mkv_probe for tr in r.tracks])]

    mkv_ext_rows: List[Dict[str, str]] = [dict((k, str(v)) for k, v in r.items()) for r in _non_hevc(mkv_ext_sub_rows)]
    vid_ext_rows: List[Dict[str, str]] = [dict((k, str(v)) for k, v in r.items()) for r in _non_hevc(non_mkv_ext_sub_rows)]

    written_reports: Dict[str, Dict[str, object]] = {}

    def _dedupe_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Remove duplicate files by output/path to avoid writing repeat entries."""
        seen: Set[str] = set()
        unique: List[Dict[str, str]] = []
        for row in rows:
            key = str(row.get("output_path") or row.get("path") or row.get("input_path") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            unique.append(row)
        return unique

    combined: List[Dict[str, str]] = _dedupe_rows(mkv_rows + non_mkv_rows + mkv_ext_rows + vid_ext_rows)

    if combined and write_csv_file:
        res = write_tabular_reports([combined], "non_hevc", TRACK_COLUMNS, output_dir=base_output_dir, dry_run=dry_run)
        written_reports["non_hevc"] = {
            "paths": res.csv_paths,
            "rows": len(combined),
            "dir": str(base_output_dir),
        }
        log.info("📊 non_hevc report saved (rows=%d)", len(combined))

    total_files = len(mkv_files) + len(vid_files) + len(sub_files)
    total_video_files = len(mkv_files) + len(vid_files)
    hevc_video_files = sum(
        1
        for r in mkv_probe + non_mkv_probe
        if any(
            (t.get("type") or "").lower() == "video"
            and "hevc" in (t.get("codec") or "").lower()
            for t in r.tracks
        )
    )
    non_hevc_video_files = sum(
        1
        for r in mkv_probe + non_mkv_probe
        if r.tracks
        and not any(
            (t.get("type") or "").lower() == "video"
            and "hevc" in (t.get("codec") or "").lower()
            for t in r.tracks
        )
    )
    log.info(
        "🧾 summary total_files=%d video_files=%d hevc_videos=%d non_hevc_videos=%d",
        total_files,
        total_video_files,
        hevc_video_files,
        non_hevc_video_files,
    )
    return combined


if __name__ == "__main__":
    vid_mkv_scan_hevc()
