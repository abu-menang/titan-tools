"""
video.scan (refactored)

Implements vid_mkv_scan with unified handling of MKV and non-MKV videos,
external subtitle matching, non-HEVC detection, per-bucket classification,
and CSV exports using the new report names/columns.
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from common.base.fs import ensure_dir
from common.base.file_io import open_file
from common.base.logging import get_logger
from common.shared.loader import load_scan_config, load_task_config, load_yaml_resource
from common.shared.report import ColumnSpec, write_tabular_reports, timestamped_filename
from common.utils.classify_utils import classify_tracks
from common.utils.fs_utils import iter_files
from common.utils.tag_utils import read_fs_tags
from common.utils.probe_utils import probe_mkvmerge
from common.utils.subtitle_utils import match_external_subs
from common.utils.track_utils import extract_tracks, flag_string

log = get_logger(__name__)

_SCAN_CFG = load_scan_config(log)
MEDIA_TYPES = _SCAN_CFG.media_types
VIDEO_EXTS: set[str] = set(MEDIA_TYPES.video_exts)
MKV_EXTS: set[str] = {".mkv"}
SUBTITLE_EXTS: set[str] = set(MEDIA_TYPES.subtitle_exts)
_COLUMNS = _SCAN_CFG.columns

def _require_cols(key: str) -> List[ColumnSpec]:
    cols = _COLUMNS.get(key)
    if not cols:
        log.error("Missing required column set '%s' in mkv_scan_columns.yaml", key)
        raise SystemExit(1)
    return cols


REPORT_DIR_MAP = _SCAN_CFG.report_dir_map
BASE_DIR_MAP = _SCAN_CFG.base_dir_map

NAME_LIST_COLUMNS: List[ColumnSpec] = _require_cols("name_list")
TRACK_COLUMNS: List[ColumnSpec] = _require_cols("track")
FAILURE_COLUMNS: List[ColumnSpec] = _require_cols("failure")
SKIPPED_COLUMNS: List[ColumnSpec] = _require_cols("skipped")
UNMATCHED_SUB_COLUMNS: List[ColumnSpec] = _require_cols("unmatched_subs")
GOOD_MKV_COLUMNS: List[ColumnSpec] = _require_cols("good_mkv")


@dataclass
class _ProbeResult:
    path: Path
    tracks: List[Dict[str, str]] = field(default_factory=list)
    failure_reason: Optional[str] = None


def vid_mkv_scan(
    roots: Optional[Iterable[Path | str]] = None,
    output_dir: Optional[Path] = None,
    output_root: Optional[Path] = None,
    write_csv_file: bool = True,
    dry_run: bool = False,
    batch_size: Optional[int] = None,
) -> List[Dict[str, object]]:
    roots = [Path(p).expanduser() for p in (roots or [Path.cwd()])]
    resolved_roots = [p.resolve() for p in roots]
    log.info("🎬 === Setup ===")
    log.info("roots=%s", ",".join(str(r) for r in resolved_roots))
    log.info("output_dir=%s output_root=%s dry_run=%s batch_size=%s", output_dir, output_root, dry_run, batch_size)

    primary_root = resolved_roots[0] if resolved_roots else Path.cwd()
    base_output_dir = output_dir or output_root or primary_root
    if not dry_run:
        ensure_dir(base_output_dir)
    log.info("📁 report_dir=%s", base_output_dir)

    mkv_files: List[Path] = []
    vid_files: List[Path] = []
    sub_files: List[Path] = []
    skip_files: List[Dict[str, str]] = []
    good_mkv_rows: List[Dict[str, str]] = []
    good_mkv_paths: Set[Path] = set()
    initial_scan_paths: List[Path] = []
    tags_by_path: Dict[Path, str] = {}

    start = time.perf_counter()
    # First pass: find good (tagged) MKVs anywhere under roots
    for f in iter_files(resolved_roots, exclude_dir=None, include_all=True):
        if f.is_file() and f.suffix.lower() in MKV_EXTS:
            tags_raw, tags = read_fs_tags(f)
            if tags and "final" in tags:
                good_mkv_rows.append({"filename": f.name, "tags": tags_raw or "", "path": str(f)})
                good_mkv_paths.add(f.resolve())
            rp = f.expanduser().resolve()
            tags_by_path[rp] = tags_raw or ""
            tags_by_path.setdefault(rp.with_suffix(".mkv"), tags_raw or "")

    # Second pass: regular collection, skipping already captured good MKVs
    for f in iter_files(resolved_roots, exclude_dir=base_output_dir, include_all=True):
        if f.is_dir():
            # Directories are ignored and should not show up in the skipped list.
            continue
        if f.name.lower() == ".directory":
            # KDE directory metadata files; treat like directories and ignore entirely.
            continue
        if f.resolve() in good_mkv_paths:
            continue
        suf = f.suffix.lower()
        if suf in MKV_EXTS:
            mkv_files.append(f)
        elif suf in VIDEO_EXTS:
            vid_files.append(f)
        elif suf in SUBTITLE_EXTS:
            sub_files.append(f)
        else:
            skip_files.append({"path": str(f), "filename": f.name, "skipped_reason": f"unsupported extension ({suf or 'none'})"})

    # Collect file-level tags for all discovered video files (MKV and otherwise)
    def _record_tags(p: Path):
        tags_raw, _ = read_fs_tags(p)
        rp = p.expanduser().resolve()
        tags_by_path[rp] = tags_raw or ""
        tags_by_path.setdefault(rp.with_suffix(".mkv"), tags_raw or "")

    for p in mkv_files + vid_files:
        _record_tags(p)
    for p in good_mkv_paths:
        _record_tags(p)

    def _tag_for_path(p: Path) -> str:
        rp = p.expanduser().resolve()
        return tags_by_path.get(rp) or tags_by_path.get(rp.with_suffix(".mkv")) or ""
    # Capture the raw files discovered before any matching/classification
    seen_paths: Set[Path] = set()
    for p in mkv_files + vid_files + sub_files + [Path(s["path"]) for s in skip_files if s.get("path")] + list(good_mkv_paths):
        rp = p.expanduser().resolve()
        if rp in seen_paths:
            continue
        seen_paths.add(rp)
        initial_scan_paths.append(rp)
    log.info("🎯 collected mkv=%d non_mkv=%d subs=%d skipped=%d", len(mkv_files), len(vid_files), len(sub_files), len(skip_files))

    def _probe_list(files: List[Path]) -> List[_ProbeResult]:
        results: List[_ProbeResult] = []
        for p in files:
            code, payload, err = probe_mkvmerge(p)
            tag_val = _tag_for_path(p)
            if payload:
                tracks = extract_tracks(p, payload)
                for tr in tracks:
                    tr["tags"] = tag_val
                results.append(_ProbeResult(path=p, tracks=tracks))
            else:
                results.append(_ProbeResult(path=p, failure_reason=err or "probe_failed"))
            if payload:
                by_type: Dict[str, int] = {"video": 0, "audio": 0, "subtitles": 0}
                for t in payload.get("tracks", []):
                    ttype = (t.get("type") or "").lower()
                    if ttype in by_type:
                        by_type[ttype] += 1
                log.info(
                    '🔍 probed "%s" video=%d audio=%d subs=%d',
                    p,
                    by_type["video"],
                    by_type["audio"],
                    by_type["subtitles"],
                )
        return results

    log.info("🧭 === Probing ===")
    mkv_probe = _probe_list(mkv_files)
    non_mkv_probe = _probe_list(vid_files)
    sub_probe = _probe_list(sub_files)

    failed_files = [
        {"path": str(r.path), "filename": r.path.name, "failure_reason": r.failure_reason}
        for r in mkv_probe + non_mkv_probe + sub_probe
        if r.failure_reason
    ]

    # Keep only successful probes for matching
    mkv_probe = [r for r in mkv_probe if not r.failure_reason]
    non_mkv_probe = [r for r in non_mkv_probe if not r.failure_reason]
    sub_probe = [r for r in sub_probe if not r.failure_reason]

    def _probe_is_broken(probe: _ProbeResult) -> bool:
        vids = sum(1 for t in probe.tracks if (t.get("type") or "").lower() == "video")
        auds = sum(1 for t in probe.tracks if (t.get("type") or "").lower() == "audio")
        return vids == 0 or auds == 0

    def _rows_for_probe(probe: _ProbeResult) -> List[Dict[str, str]]:
        if probe.tracks:
            return probe.tracks
        path = probe.path
        return [
            {
                "tags": _tag_for_path(path),
                "output_filename": path.with_suffix(".mkv").name,
                "output_path": str(path.with_suffix(".mkv")),
                "input_path": str(path),
                "filename": path.name,
                "type": "",
                "id": "",
                "name": "",
                "edited_name": "",
                "lang": "",
                "codec": "",
                "default": "",
                "forced": "",
                "encoding": "",
                "path": str(path),
            }
        ]

    broken_mkv_rows: List[Dict[str, str]] = []
    broken_vid_rows: List[Dict[str, str]] = []

    _kept_mkv: List[_ProbeResult] = []
    for r in mkv_probe:
        if _probe_is_broken(r):
            broken_mkv_rows.extend(_rows_for_probe(r))
        else:
            _kept_mkv.append(r)
    mkv_probe = _kept_mkv

    _kept_vid: List[_ProbeResult] = []
    for r in non_mkv_probe:
        if _probe_is_broken(r):
            broken_vid_rows.extend(_rows_for_probe(r))
        else:
            _kept_vid.append(r)
    non_mkv_probe = _kept_vid

    log.info("🔗 === Matching external subtitles ===")
    mkv_ext_sub_rows, non_mkv_ext_sub_rows, unmatched_subs_paths = match_external_subs(mkv_probe + non_mkv_probe, sub_probe)
    def _apply_tags(rows: List[Dict[str, str]]):
        for r in rows:
            current_tags = r.get("tags")
            if current_tags not in (None, ""):
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
    sub_files = unmatched_subs_paths

    # Remove matched videos from base lists
    matched_video_paths = {Path(r.get("input_path", "")) for r in mkv_ext_sub_rows + non_mkv_ext_sub_rows if r.get("type") == "video"}
    mkv_probe = [r for r in mkv_probe if r.path not in matched_video_paths]
    non_mkv_probe = [r for r in non_mkv_probe if r.path not in matched_video_paths]

    log.info("✅ === Classification ===")
    try:
        cfg = load_task_config("vid_mkv_scan", None)
    except Exception as exc:
        log.error("Failed to load vid_mkv_scan config: %s", exc)
        raise SystemExit(1)

    try:
        classification_cfg = load_yaml_resource("classification")
    except Exception as exc:
        log.error("Failed to load classification.yaml: %s", exc)
        raise SystemExit(1)
    if not isinstance(classification_cfg, Mapping):
        log.error("classification.yaml root must be a mapping")
        raise SystemExit(1)

    def _section_for_path(p: Path) -> Optional[str]:
        parts = [part.lower() for part in p.parts]
        for anchor in ("series", "movies"):
            if anchor in parts:
                idx = parts.index(anchor)
                if idx + 1 < len(parts):
                    return parts[idx + 1]
        return None

    section: Optional[str] = None
    for root in resolved_roots:
        section = _section_for_path(root)
        if section:
            break

    if section and section in classification_cfg:
        lang_cfg = classification_cfg.get(section, {})
        selected_section = section
    else:
        if "default" not in classification_cfg:
            log.error("classification.yaml missing 'default' section; cannot fall back for roots %s", resolved_roots)
            raise SystemExit(1)
        lang_cfg = classification_cfg.get("default", {})
        selected_section = "default"
        if section:
            log.info("Classification section '%s' not found; using default", section)

    if not isinstance(lang_cfg, Mapping):
        log.error("classification section '%s' must be a mapping", selected_section)
        raise SystemExit(1)

    def _to_lang_list(val: Any, key: str) -> List[str]:
        if val is None:
            log.error("classification section '%s' missing required key '%s'", selected_section, key)
            raise SystemExit(1)
        if isinstance(val, (list, tuple, set)):
            return [str(x).lower() for x in val if str(x).strip()]
        if isinstance(val, str):
            return [x.strip().lower() for x in val.split(",") if x.strip()]
        log.error("classification section '%s' key '%s' must be a list or string", selected_section, key)
        raise SystemExit(1)

    allowed_vid = _to_lang_list(lang_cfg.get("lang_vid"), "lang_vid")
    allowed_aud = _to_lang_list(lang_cfg.get("lang_aud"), "lang_aud")
    allowed_sub = _to_lang_list(lang_cfg.get("lang_sub"), "lang_sub")
    log.info("Using classification section=%s lang_vid=%s lang_aud=%s lang_sub=%s", selected_section, allowed_vid, allowed_aud, allowed_sub)

    mkv_files_ok, mkv_files_issues = classify_tracks([tr for r in mkv_probe for tr in r.tracks], allowed_vid, allowed_aud, allowed_sub)
    non_mkv_files_ok, non_mkv_files_issues = classify_tracks([tr for r in non_mkv_probe for tr in r.tracks], allowed_vid, allowed_aud, allowed_sub)
    mkv_ext_ok, mkv_ext_issues = classify_tracks(mkv_ext_sub_rows, allowed_vid, allowed_aud, allowed_sub)
    non_mkv_ext_ok, non_mkv_ext_issues = classify_tracks(non_mkv_ext_sub_rows, allowed_vid, allowed_aud, allowed_sub)

    def _split_name_mismatches(rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        grouped: Dict[str, List[Dict[str, str]]] = {}
        for r in rows:
            key = r.get("output_filename") or r.get("filename") or r.get("path") or r.get("input_path") or ""
            grouped.setdefault(key, []).append(r)

        def _norm(val: Optional[str]) -> str:
            return str(val).strip() if val is not None else ""

        ok: List[Dict[str, str]] = []
        mismatched: List[Dict[str, str]] = []
        for items in grouped.values():
            has_mismatch = any(_norm(i.get("name")) != _norm(i.get("edited_name")) for i in items)
            (mismatched if has_mismatch else ok).extend(items)
        return ok, mismatched

    mkv_files_ok, mkv_name_mismatch = _split_name_mismatches(mkv_files_ok)
    non_mkv_files_ok, non_mkv_name_mismatch = _split_name_mismatches(non_mkv_files_ok)
    mkv_ext_ok, mkv_ext_name_mismatch = _split_name_mismatches(mkv_ext_ok)
    non_mkv_ext_ok, non_mkv_ext_name_mismatch = _split_name_mismatches(non_mkv_ext_ok)

    log.info(
        "📦 classification totals: mkv_ok=%d mkv_issues=%d nonmkv_ok=%d nonmkv_issues=%d mkv_ext_ok=%d mkv_ext_issues=%d nonmkv_ext_ok=%d nonmkv_ext_issues=%d good_mkv=%d",
        len(mkv_files_ok),
        len(mkv_files_issues),
        len(non_mkv_files_ok),
        len(non_mkv_files_issues),
        len(mkv_ext_ok),
        len(mkv_ext_issues),
        len(non_mkv_ext_ok),
        len(non_mkv_ext_issues),
        len(good_mkv_rows),
    )

    log.info(
        "✏️ name mismatches moved: mkv=%d non_mkv=%d ext_sub_mkv=%d ext_sub_vid=%d",
        len(mkv_name_mismatch),
        len(non_mkv_name_mismatch),
        len(mkv_ext_name_mismatch),
        len(non_mkv_ext_name_mismatch),
    )

    written_reports: Dict[str, Dict[str, object]] = {}
    report_rows: Dict[str, List[Dict[str, str]]] = {}

    def _write(name: str, rows: List[List[Dict[str, str]]], cols: List[ColumnSpec]):
        if not rows or not write_csv_file:
            return
        total_rows = sum(len(r) for r in rows)
        if total_rows == 0:
            return
        dir_key = REPORT_DIR_MAP.get(name)
        base_dir_name = BASE_DIR_MAP.get(dir_key, dir_key) if dir_key else None
        out_dir = Path(base_output_dir)
        if base_dir_name:
            out_dir = out_dir / str(base_dir_name)
        res = write_tabular_reports(rows, name, cols, output_dir=out_dir, dry_run=dry_run)
        paths = res.csv_paths if isinstance(res.csv_paths, list) else [res.csv_paths]
        target = paths[0] if paths else "n/a"
        log.info("📊 %s report saved (rows=%d) → %s", name, total_rows, target)
        flattened: List[Dict[str, str]] = []
        for group in rows:
            flattened.extend(group)
        report_rows[name] = flattened
        written_reports[name] = {
            "paths": res.csv_paths,
            "rows": total_rows,
            "dir": str(base_dir_name) if base_dir_name else "base_output_dir",
        }

    _write("ext_sub_name_mismatch_mkv", [mkv_ext_name_mismatch], TRACK_COLUMNS)
    _write("ext_sub_ok_mkv", [mkv_ext_ok], TRACK_COLUMNS)
    _write("ext_sub_name_mismatch_vid", [non_mkv_ext_name_mismatch], TRACK_COLUMNS)
    _write("ext_sub_ok_vid", [non_mkv_ext_ok], TRACK_COLUMNS)
    _write("name_mismatch_mkv", [mkv_name_mismatch], TRACK_COLUMNS)
    _write("ok_mkv", [mkv_files_ok], TRACK_COLUMNS)
    _write("name_mismatch_vid", [non_mkv_name_mismatch], TRACK_COLUMNS)
    _write("ok_vid", [non_mkv_files_ok], TRACK_COLUMNS)
    if broken_mkv_rows:
        _write("broken_mkv", [broken_mkv_rows], TRACK_COLUMNS)
    if broken_vid_rows:
        _write("broken_vid", [broken_vid_rows], TRACK_COLUMNS)
    if good_mkv_rows:
        _write("good_mkv", [good_mkv_rows], GOOD_MKV_COLUMNS)
    if failed_files:
        _write("failures", [failed_files], FAILURE_COLUMNS)
    if skip_files:
        _write("skipped", [skip_files], SKIPPED_COLUMNS)
    unmatched_sub_rows = [{"path": str(p), "filename": Path(p).name} for p in unmatched_subs_paths]
    if unmatched_sub_rows:
        _write("unmatched_subs", [unmatched_sub_rows], UNMATCHED_SUB_COLUMNS)

    def _bucket_issue_files(rows: List[Dict[str, str]], suffix: str, prefix: str = "") -> Dict[str, List[Dict[str, str]]]:
        name = lambda base: f"{prefix}{base}_{suffix}"
        buckets: Dict[str, List[Dict[str, str]]] = {
            name("0_subs"): [],
            name("multi_subs"): [],
            name("multi_vids"): [],
            name("multi_aud"): [],
            name("lang_mismatch"): [],
            name("multi_issue"): [],
        }
        grouped: Dict[str, List[Dict[str, str]]] = {}
        for r in rows:
            key = r.get("output_filename") or r.get("filename") or r.get("path") or ""
            grouped.setdefault(key, []).append(r)

        def _lang_ok(lang: str, allowed: List[str]) -> bool:
            if not allowed:
                return True
            l = (lang or "").lower()
            return any(l.startswith(a) for a in allowed)

        for key, items in grouped.items():
            v = sum(1 for i in items if (i.get("type") or "").lower() == "video")
            a = sum(1 for i in items if (i.get("type") or "").lower() == "audio")
            s = sum(1 for i in items if (i.get("type") or "").lower() == "subtitles")
            lang_mismatch = False
            # Only consider audio/subtitles for lang mismatch here
            for i in items:
                ttype = (i.get("type") or "").lower()
                if ttype == "audio" and not _lang_ok(i.get("lang", ""), allowed_aud):
                    lang_mismatch = True
                    break
                if ttype == "subtitles" and not _lang_ok(i.get("lang", ""), allowed_sub):
                    lang_mismatch = True
                    break

            issues_hit: List[str] = []
            if v == 1 and a == 1 and s == 0:
                issues_hit.append(name("0_subs"))
            if v == 1 and a == 1 and s > 1:
                issues_hit.append(name("multi_subs"))
            if v > 1 and a == 1 and s == 1:
                issues_hit.append(name("multi_vids"))
            if v == 1 and a > 1 and s == 1:
                issues_hit.append(name("multi_aud"))
            if v == 1 and a == 1 and s == 1 and lang_mismatch:
                issues_hit.append(name("lang_mismatch"))

            if len(issues_hit) > 1:
                buckets[name("multi_issue")].extend(items)
            elif issues_hit:
                buckets[issues_hit[0]].extend(items)
        return buckets

    mkv_issue_buckets = _bucket_issue_files(mkv_files_issues, "mkv")
    vid_issue_buckets = _bucket_issue_files(non_mkv_files_issues, "vid")
    mkv_ext_issue_buckets = _bucket_issue_files(mkv_ext_issues, "mkv", prefix="ext_sub_")
    vid_ext_issue_buckets = _bucket_issue_files(non_mkv_ext_issues, "vid", prefix="ext_sub_")

    for name, rows in {**mkv_issue_buckets, **vid_issue_buckets, **mkv_ext_issue_buckets, **vid_ext_issue_buckets}.items():
        if rows:
            _write(name, [rows], TRACK_COLUMNS)

    # Human-readable summaries grouped by output dirs and CSV names
    try:
        def _file_buckets(rows: list[dict[str, str]]) -> dict[str, set[str]]:
            files: dict[str, set[str]] = {}
            for r in rows:
                fname = r.get("output_filename") or r.get("filename") or r.get("path") or r.get("input_path") or ""
                if not fname:
                    continue
                ttype = (r.get("type") or "").lower()
                files.setdefault(fname, set()).add(ttype)
            return files

        def _file_totals(rows: list[dict[str, str]]) -> tuple[int, int, int, int]:
            buckets = _file_buckets(rows)
            total_files = len(buckets)
            video_files = sum(1 for types in buckets.values() if "video" in types)
            sub_files_only = sum(1 for types in buckets.values() if "video" not in types and "subtitles" in types)
            other_files = total_files - video_files - sub_files_only
            return total_files, video_files, sub_files_only, other_files

        def _initial_totals(paths: list[Path]) -> tuple[int, int, int, int]:
            total_files = len(paths)
            video_files = 0
            sub_files_only = 0
            other_files = 0
            for p in paths:
                suf = p.suffix.lower()
                if suf in MKV_EXTS or suf in VIDEO_EXTS:
                    video_files += 1
                elif suf in SUBTITLE_EXTS:
                    sub_files_only += 1
                else:
                    other_files += 1
            return total_files, video_files, sub_files_only, other_files

        def _classification_for_path(path: Path) -> str:
            for name, meta in written_reports.items():
                rows = report_rows.get(name, [])
                for row in rows:
                    if row.get("path") == str(path) or row.get("input_path") == str(path):
                        return str(meta.get("dir") or "base_output_dir")
            return "NO CLASSIFICATION"

        all_report_rows: list[dict[str, str]] = []
        for rows in report_rows.values():
            all_report_rows.extend(rows)

        summary_path = timestamped_filename("scan_summary", "txt", base_output_dir)
        with open_file(summary_path, "w") as out:
            RESET = "\x1b[0m"
            BOLD = "\x1b[1m"
            CYAN = "\x1b[36m"

            out.write(f"{BOLD}{CYAN}📋 Scan Summary{RESET}\n")
            out.write(f"{BOLD}Generated:{RESET} " + summary_path.name + "\n\n")

            total_files, total_video_files, total_sub_files, total_other_files = _initial_totals(initial_scan_paths)
            totals_line = (
                f"{BOLD}{CYAN}Totals:{RESET} "
                f"all_files={total_files}, "
                f"video_files={total_video_files}, "
                f"sub_files={total_sub_files}, "
                f"other_files={total_other_files}, "
                f"tracks={len(all_report_rows)}, "
                f"failures={len(failed_files)}, "
                f"skipped={len(skip_files)}"
            )
            out.write(totals_line + "\n\n")

            out.write(f"{BOLD}{CYAN}All Scanned Files{RESET}\n")
            out.write("filename,path,classification\n")
            for p in sorted(initial_scan_paths):
                out.write(f"{p.name},{p},{_classification_for_path(p)}\n")
            out.write("\n")

            reports_by_dir: dict[str, list[str]] = {}
            for name, meta in written_reports.items():
                dir_name = str(meta.get("dir") or "base_output_dir")
                reports_by_dir.setdefault(dir_name, []).append(name)

            out.write(f"{BOLD}{CYAN}Outputs by directory{RESET}\n")
            for dir_name in sorted(reports_by_dir.keys()):
                out.write(f"{BOLD}{dir_name}:{RESET}\n")
                for report_name in sorted(reports_by_dir[dir_name]):
                    rows = report_rows.get(report_name, [])
                    files, vids, subs_only, others = _file_totals(rows)
                    out.write(
                        f"  {report_name}.csv rows={len(rows)} files={files} video_files={vids} sub_files={subs_only} other_files={others}\n"
                    )
                out.write("\n")

        log.info("Wrote summary → %s", summary_path)

        # HTML summary (best effort)
        try:
            html_path = timestamped_filename("scan_summary", "html", base_output_dir)
            html_parts: list[str] = []

            html_parts.append("<!doctype html>")
            html_parts.append("<html><head><meta charset=\"utf-8\"><title>MKV Scan Outputs</title>")
            html_parts.append(
                "<style>"
                "body{font-family:'Segoe UI',Helvetica,Arial,sans-serif;background:#f8fbff;color:#1a1d21;padding:18px;line-height:1.5;}"
                "h1{font-size:1.6rem;margin:0 0 8px;font-weight:700;color:#0b5ed7;}"
                "h2{font-size:1.2rem;margin:16px 0 8px;font-weight:700;color:#0f5132;}"
                "h3{font-size:1rem;margin:12px 0 6px;font-weight:700;color:#0b5ed7;}"
                ".summary-bar{margin:10px 0 14px;padding:10px 12px;background:#e7f1ff;border:1px solid #cfe2ff;border-radius:8px;font-size:0.95rem;}"
                ".summary-bar strong{color:#0b5ed7;}"
                ".tt-details{border:1px solid #ced4da;border-radius:8px;padding:6px 10px;margin:10px 0;background:#fff;}"
                ".tt-details > summary{cursor:pointer;font-weight:700;font-size:1rem;color:#fff;padding:6px 8px;border-radius:6px;background:linear-gradient(90deg,#10243f,#0b2f60);}"
                ".tt-subdetails{margin:8px 0;border:1px solid #e9ecef;border-radius:6px;padding:4px 6px;background:#fdfdff;}"
                ".tt-subdetails summary{cursor:pointer;font-weight:600;font-size:0.95rem;color:#0b2f60;padding:4px 6px;border-radius:4px;background:linear-gradient(90deg,#e7f1ff,#f2f7ff);}"
                ".stat-line{margin:4px 0;font-size:0.95rem;}"
                ".tt-grid td,.tt-grid th{border:1px solid #dee2e6;text-align:left;padding:6px 8px;}"
                ".tt-grid thead tr{background:linear-gradient(90deg,#0b294f,#0b2f60);color:#fff;}"
                ".tt-grid tbody tr:nth-child(even){background:#2d7fe0;color:#fff;}"
                "</style>"
            )
            html_parts.append("</head><body>")
            html_parts.append(f"<h1>📋 Scan Outputs</h1><p><strong>Generated:</strong> {html_path.name}</p>")

            totals_html = (
                f"<div class=\"summary-bar\">"
                f"All files: <strong>{total_files}</strong> &nbsp; "
                f"Video files: <strong>{total_video_files}</strong> &nbsp; "
                f"Sub files: <strong>{total_sub_files}</strong> &nbsp; "
                f"Other files: <strong>{total_other_files}</strong> &nbsp; "
                f"Tracks: <strong>{len(all_report_rows)}</strong> &nbsp; "
                f"Failures: <strong>{len(failed_files)}</strong> &nbsp; "
                f"Skipped: <strong>{len(skip_files)}</strong>"
                f"</div>"
            )
            html_parts.append(totals_html)

            # Pre-classification file list
            files_list_html = "".join(
                f"<tr><td>{p.name}</td><td>{p}</td><td>{_classification_for_path(p)}</td></tr>"
                for p in sorted(initial_scan_paths)
            )
            html_parts.append(
                f"<details class=\"tt-details\" open>"
                f"<summary>📂 All Scanned Files</summary>"
                f"<table class=\"tt-table tt-grid\"><thead><tr><th>filename</th><th>path</th><th>classification</th></tr></thead>"
                f"<tbody>{files_list_html}</tbody></table>"
                f"</details>"
            )

            reports_by_dir: dict[str, list[str]] = {}
            for name, meta in written_reports.items():
                dir_name = str(meta.get("dir") or "base_output_dir")
                reports_by_dir.setdefault(dir_name, []).append(name)

            for dir_name in sorted(reports_by_dir.keys()):
                detail_body: list[str] = []
                for report_name in sorted(reports_by_dir[dir_name]):
                    rows = report_rows.get(report_name, [])
                    files, vids, subs_only, others = _file_totals(rows)
                    detail_body.append(
                        f"<div class=\"tt-subdetails\"><summary>{report_name}.csv</summary>"
                        f"<div class=\"stat-line\">Rows: <strong>{len(rows)}</strong></div>"
                        f"<div class=\"stat-line\">Files: <strong>{files}</strong> (video: {vids}, subs: {subs_only}, other: {others})</div>"
                        f"</div>"
                    )
                html_parts.append(f"<details class=\"tt-details\" open><summary>📁 {dir_name}</summary>{''.join(detail_body)}</details>")

            try:
                csv_links: list[str] = []
                for label, info in written_reports.items():
                    paths = info.get("paths") if isinstance(info, dict) else None
                    if not paths:
                        continue
                    if not isinstance(paths, list):
                        paths = [paths]
                    for p in paths:
                        pname = getattr(p, "name", None) or str(p)
                        csv_links.append(f"<li><a href=\"{pname}\">{label} → {pname}</a></li>")
                if csv_links:
                    html_parts.append("<h2>CSV exports</h2><ul>")
                    html_parts.extend(csv_links)
                    html_parts.append("</ul>")
            except Exception:
                pass

            html_parts.append("</body></html>")
            html_content = "\n".join(html_parts)
            with open_file(html_path, "w") as h:
                h.write(html_content)
            log.info("Wrote HTML summary → %s", html_path)
        except Exception:
            log.exception("Failed to write HTML summary")
    except Exception:
        log.exception("Failed to write scan summary")

    elapsed = time.perf_counter() - start
    log.info("⏱️ elapsed=%.2fs", elapsed)
    combined: List[Dict[str, object]] = [dict(r) for r in mkv_files_ok]
    combined.extend(dict(r) for r in mkv_ext_ok)
    return combined
