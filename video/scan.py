"""
video.scan (refactored)

Implements vid_mkv_scan with unified handling of MKV and non-MKV videos,
external subtitle matching, non-HEVC detection, per-bucket classification,
and CSV exports using the new report names/columns.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from common.base.fs import ensure_dir
from common.base.file_io import open_file
from common.base.logging import get_logger
from common.shared.loader import load_media_types, load_task_config
from common.shared.report import ColumnSpec, write_tabular_reports, timestamped_filename
from common.utils.classify_utils import classify_tracks
from common.utils.column_utils import load_column_specs
from common.utils.fs_utils import iter_files
from common.utils.probe_utils import probe_mkvmerge
from common.utils.subtitle_utils import match_external_subs
from common.utils.track_utils import extract_tracks, flag_string

log = get_logger(__name__)

MEDIA_TYPES = load_media_types()
VIDEO_EXTS: set[str] = set(MEDIA_TYPES.video_exts)
MKV_EXTS: set[str] = {".mkv"}
SUBTITLE_EXTS: set[str] = set(MEDIA_TYPES.subtitle_exts)


_COLUMNS = load_column_specs("mkv_scan_columns")
NAME_LIST_COLUMNS: List[ColumnSpec] = _COLUMNS.get("name_list", [])
TRACK_COLUMNS: List[ColumnSpec] = _COLUMNS.get("track", [])
NON_MKV_TRACK_COLUMNS: List[ColumnSpec] = _COLUMNS.get("non_mkv_track", [])
FAILURE_COLUMNS: List[ColumnSpec] = _COLUMNS.get("failure", [])
SKIPPED_COLUMNS: List[ColumnSpec] = _COLUMNS.get("skipped", [])
NON_HEVC_COLUMNS: List[ColumnSpec] = _COLUMNS.get("non_hevc", [])
UNMATCHED_SUB_COLUMNS: List[ColumnSpec] = _COLUMNS.get("unmatched_subs", [])


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

    start = time.perf_counter()
    for f in iter_files(resolved_roots, exclude_dir=base_output_dir, include_all=True):
        suf = f.suffix.lower()
        if suf in MKV_EXTS:
            mkv_files.append(f)
        elif suf in VIDEO_EXTS:
            vid_files.append(f)
        elif suf in SUBTITLE_EXTS:
            sub_files.append(f)
        else:
            skip_files.append({"path": str(f), "filename": f.name, "skipped_reason": f"unsupported extension ({suf or 'none'})"})
    log.info("🎯 collected mkv=%d non_mkv=%d subs=%d skipped=%d", len(mkv_files), len(vid_files), len(sub_files), len(skip_files))

    def _probe_list(files: List[Path]) -> List[_ProbeResult]:
        results: List[_ProbeResult] = []
        for p in files:
            code, payload, err = probe_mkvmerge(p)
            if payload:
                tracks = extract_tracks(p, payload)
                results.append(_ProbeResult(path=p, tracks=tracks))
            else:
                results.append(_ProbeResult(path=p, failure_reason=err or "probe_failed"))
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

    log.info("🔗 === Matching external subtitles ===")
    mkv_ext_sub_rows, non_mkv_ext_sub_rows, unmatched_subs_paths = match_external_subs(mkv_probe + non_mkv_probe, sub_probe)
    sub_files = unmatched_subs_paths

    # Remove matched videos from base lists
    matched_video_paths = {Path(r.get("input_path", "")) for r in mkv_ext_sub_rows + non_mkv_ext_sub_rows if r.get("type") == "video"}
    mkv_probe = [r for r in mkv_probe if r.path not in matched_video_paths]
    non_mkv_probe = [r for r in non_mkv_probe if r.path not in matched_video_paths]

    log.info("🧊 === Non-HEVC detection ===")
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
                out.append({"filename": Path(path).name, "codecs": ", ".join(sorted(codecs)), "path": path})
        return out

    mkv_non_hevc_rows = _non_hevc(mkv_ext_sub_rows + [tr for r in mkv_probe for tr in r.tracks])
    non_mkv_non_hevc_rows = _non_hevc(non_mkv_ext_sub_rows + [tr for r in non_mkv_probe for tr in r.tracks])

    mkv_non_hevc_paths = {Path(str(r.get("path", ""))).resolve() for r in mkv_non_hevc_rows if r.get("path")}
    non_mkv_non_hevc_paths = {Path(str(r.get("path", ""))).resolve() for r in non_mkv_non_hevc_rows if r.get("path")}

    log.info("✅ === Classification ===")
    try:
        cfg = load_task_config("vid_mkv_scan", None)
    except Exception:
        cfg = {}
    def _norm_list(val, default):
        if val is None:
            val = default
        return [str(x).lower() for x in (val or []) if x]
    allowed_vid = _norm_list(cfg.get("lang_vid"), ["eng"])
    allowed_aud = _norm_list(cfg.get("lang_aud"), ["eng"])
    allowed_sub = _norm_list(cfg.get("lang_sub"), ["eng"])

    mkv_files_ok, mkv_files_issues = classify_tracks([tr for r in mkv_probe for tr in r.tracks], allowed_vid, allowed_aud, allowed_sub)
    non_mkv_files_ok, non_mkv_files_issues = classify_tracks([tr for r in non_mkv_probe for tr in r.tracks], allowed_vid, allowed_aud, allowed_sub)
    mkv_ext_ok, mkv_ext_issues = classify_tracks(mkv_ext_sub_rows, allowed_vid, allowed_aud, allowed_sub)
    non_mkv_ext_ok, non_mkv_ext_issues = classify_tracks(non_mkv_ext_sub_rows, allowed_vid, allowed_aud, allowed_sub)

    def _move_non_hevc(ok_rows: List[Dict[str, str]], issue_rows: List[Dict[str, str]], paths: Set[Path]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        def _row_path(r: Dict[str, str]) -> Path:
            # Prefer output_path so related subtitle rows move with their parent video
            candidate = r.get("output_path") or r.get("path") or r.get("input_path") or ""
            return Path(candidate).expanduser().resolve()
        keep: List[Dict[str, str]] = []
        moved: List[Dict[str, str]] = []
        for row in ok_rows:
            if _row_path(row) in paths:
                moved.append(row)
            else:
                keep.append(row)
        for row in issue_rows[:]:
            if _row_path(row) in paths:
                moved.append(row)
                issue_rows.remove(row)
        return keep, moved

    mkv_files_ok, mkv_files_issues_non_hevc = _move_non_hevc(mkv_files_ok, mkv_files_issues, mkv_non_hevc_paths)
    non_mkv_files_ok, non_mkv_files_issues_non_hevc = _move_non_hevc(non_mkv_files_ok, non_mkv_files_issues, non_mkv_non_hevc_paths)
    mkv_ext_ok, mkv_ext_issues_non_hevc = _move_non_hevc(mkv_ext_ok, mkv_ext_issues, mkv_non_hevc_paths)
    non_mkv_ext_ok, non_mkv_ext_issues_non_hevc = _move_non_hevc(non_mkv_ext_ok, non_mkv_ext_issues, non_mkv_non_hevc_paths)

    log.info("📦 classification totals: mkv_ok=%d mkv_issues=%d nonmkv_ok=%d nonmkv_issues=%d mkv_ext_ok=%d mkv_ext_issues=%d nonmkv_ext_ok=%d nonmkv_ext_issues=%d",
             len(mkv_files_ok), len(mkv_files_issues), len(non_mkv_files_ok), len(non_mkv_files_issues), len(mkv_ext_ok), len(mkv_ext_issues), len(non_mkv_ext_ok), len(non_mkv_ext_issues))

    written_reports: Dict[str, Dict[str, object]] = {}

    def _write(name: str, rows: List[List[Dict[str, str]]], cols: List[ColumnSpec]):
        if not rows or not write_csv_file:
            return
        total_rows = sum(len(r) for r in rows)
        if total_rows == 0:
            return
        log.info("📝 writing %s rows=%d", name, total_rows)
        res = write_tabular_reports(rows, name, cols, output_dir=base_output_dir, dry_run=dry_run)
        written_reports[name] = {"paths": res.csv_paths, "rows": total_rows}

    _write("mkv_ext_sub_ok", [mkv_ext_ok], TRACK_COLUMNS)
    _write("mkv_ext_sub_issues", [mkv_ext_issues], TRACK_COLUMNS)
    _write("mkv_ext_sub_issues_non_hevc", [mkv_ext_issues_non_hevc], TRACK_COLUMNS)
    _write("vid_ext_subs_ok", [non_mkv_ext_ok], NON_MKV_TRACK_COLUMNS)
    _write("vid_ext_subs_issues", [non_mkv_ext_issues], NON_MKV_TRACK_COLUMNS)
    _write("vid_ext_subs_issues_non_hevc", [non_mkv_ext_issues_non_hevc], NON_MKV_TRACK_COLUMNS)
    _write("mkv_files_ok", [mkv_files_ok], TRACK_COLUMNS)
    _write("mkv_files_issues", [mkv_files_issues], TRACK_COLUMNS)
    _write("mkv_files_issues_non_hevc", [mkv_files_issues_non_hevc], TRACK_COLUMNS)
    _write("vid_files_ok", [non_mkv_files_ok], NON_MKV_TRACK_COLUMNS)
    _write("vid_files_issues", [non_mkv_files_issues], NON_MKV_TRACK_COLUMNS)
    _write("vid_files_issues_non_hevc", [non_mkv_files_issues_non_hevc], NON_MKV_TRACK_COLUMNS)
    if failed_files:
        _write("failures", [failed_files], FAILURE_COLUMNS)
    if skip_files:
        _write("skipped", [skip_files], SKIPPED_COLUMNS)
    unmatched_sub_rows = [{"path": str(p), "filename": Path(p).name} for p in unmatched_subs_paths]
    if unmatched_sub_rows:
        _write("unmatched_subs", [unmatched_sub_rows], UNMATCHED_SUB_COLUMNS)

    # Human-readable summaries (best effort, matched to legacy format)
    try:
        unmatched_sub_files = list(unmatched_subs_paths)
        def _lang_matches(lang: str, allowed: List[str]) -> bool:
            if not allowed:
                return True
            l = (lang or "").lower()
            return any(l.startswith(a) for a in allowed)

        # Aggregate per-file track counts (MKV + vid + ext-sub rows)
        file_counts: Dict[str, Dict[str, int]] = {}
        actual_langs: Dict[str, Dict[str, Set[str]]] = {"video": {}, "audio": {}, "subtitles": {}}

        mkv_rows_all = [tr for r in mkv_probe for tr in r.tracks] + mkv_ext_sub_rows
        non_mkv_scan_rows = [tr for r in non_mkv_probe for tr in r.tracks] + non_mkv_ext_sub_rows
        all_rows = mkv_rows_all + non_mkv_scan_rows

        for r in all_rows:
            fname = r.get("output_filename") or r.get("filename") or r.get("path") or ""
            ttype = (r.get("type") or "").lower()
            if not fname or ttype not in {"video", "audio", "subtitles"}:
                continue
            counts = file_counts.setdefault(fname, {"video": 0, "audio": 0, "subtitles": 0})
            counts[ttype] += 1
            lang_val = (r.get("lang") or "und").lower() or "und"
            actual_langs[ttype].setdefault(fname, set()).add(lang_val)

        more_than_1_video = sorted(((f, c["video"]) for f, c in file_counts.items() if c["video"] > 1), key=lambda t: -t[1])
        more_than_1_audio = sorted(((f, c["audio"]) for f, c in file_counts.items() if c["audio"] > 1), key=lambda t: -t[1])
        more_than_1_subtitle = sorted(((f, c["subtitles"]) for f, c in file_counts.items() if c["subtitles"] > 1), key=lambda t: -t[1])

        no_video = sorted([f for f, c in file_counts.items() if c["video"] == 0])
        no_audio = sorted([f for f, c in file_counts.items() if c["audio"] == 0])
        no_subtitles = sorted([f for f, c in file_counts.items() if c["subtitles"] == 0])

        bad_vid = sorted([f for f, langs in actual_langs["video"].items() if not all(_lang_matches(l, allowed_vid) for l in langs)])
        bad_aud = sorted([f for f, langs in actual_langs["audio"].items() if not all(_lang_matches(l, allowed_aud) for l in langs)])
        bad_sub = sorted([f for f, langs in actual_langs["subtitles"].items() if not all(_lang_matches(l, allowed_sub) for l in langs)])

        summary_path = timestamped_filename("scan_summary", "txt", base_output_dir)
        with open_file(summary_path, "w") as out:
            RESET = "\x1b[0m"
            BOLD = "\x1b[1m"
            CYAN = "\x1b[36m"
            YELLOW = "\x1b[33m"

            out.write(f"{BOLD}{CYAN}📋 Scan Summary{RESET}\n")
            out.write(f"{BOLD}Generated:{RESET} " + summary_path.name + "\n\n")

            totals_line = (
                f"{BOLD}{CYAN}Totals:{RESET} "
                f"files={len(file_counts)}, "
                f"tracks={len(all_rows)}, "
                f"non_hevc={len(mkv_non_hevc_rows) + len(non_mkv_non_hevc_rows)}, "
                f"failures={len(failed_files)}, "
                f"skipped={len(skip_files)}, "
                f"non_mkv_ext_subs={len(non_mkv_ext_sub_rows)}, "
                f"mkv_ext_subs={len(mkv_ext_sub_rows)}, "
                f"non_mkv_non_hevc={len(non_mkv_non_hevc_rows)}"
            )
            out.write(totals_line + "\n\n")

            summary_rows: List[tuple[str, int, int, int, str]] = []
            for fname in sorted(file_counts.keys()):
                counts = file_counts[fname]
                lang_flags: List[str] = []
                if fname in bad_vid:
                    lang_flags.append("vid")
                if fname in bad_aud:
                    lang_flags.append("aud")
                if fname in bad_sub:
                    lang_flags.append("sub")
                lang_flag_str = ",".join(lang_flags) if lang_flags else ""
                summary_rows.append((fname, counts["video"], counts["audio"], counts["subtitles"], lang_flag_str))

            out.write(f"{BOLD}{CYAN}filename,video,audio,sub,lang_issues{RESET}\n")
            for fname, v, a, s, lang_flag_str in summary_rows:
                has_structural_problem = v == 0 or a == 0 or s == 0 or v > 1 or a > 1 or s > 1
                has_lang_problem = bool(lang_flag_str)
                line = f"{fname},{v},{a},{s},{lang_flag_str}"
                if has_structural_problem or has_lang_problem:
                    out.write(f"{YELLOW}{line}{RESET}\n")
                else:
                    out.write(f"{line}\n")
            out.write("\n")

        log.info("Wrote summary → %s", summary_path)

        # HTML summary (best effort)
        try:
            def _html_table_block(title: str, headers: List[str], rows: List[List[str]]) -> str:
                if not rows:
                    return "<p><em>None</em></p>"
                out = ["<table class=\"tt-table\">", "<thead><tr>"]
                for h in headers:
                    out.append(f"<th>{h}</th>")
                out.append("</tr></thead><tbody>")
                for row in rows:
                    out.append("<tr>")
                    for cell in row:
                        out.append(f"<td>{cell}</td>")
                    out.append("</tr>")
                out.append("</tbody></table>")
                return "".join(out)

            def _wrap_details(title: str, body: str, *, has_data: bool, open_state: bool = False) -> str:
                cls = "tt-details has-data" if has_data else "tt-details no-data"
                return f"<details class=\"{cls}\"{' open' if open_state else ''}><summary>{title}</summary>{body}</details>"

            def _wrap_subsection(title: str, body: str, *, has_data: bool, open_state: bool = False) -> str:
                cls = "tt-subdetails has-data" if has_data else "tt-subdetails no-data"
                return f"<details class=\"{cls}\"{' open' if open_state else ''}><summary>{title}</summary>{body}</details>"

            def _table_from_pairs(title: str, pairs: List[tuple[str, int]]) -> str:
                rows = [[fn, str(cnt)] for fn, cnt in pairs]
                return _html_table_block(title, ["filename", "count"], rows)

            def _lang_table(title: str, target: List[str], key: str, expected: str) -> str:
                lang_rows: List[List[str]] = []
                for fn in target:
                    langs = sorted(actual_langs.get(key, {}).get(fn, set()) or {"und"})
                    lang_rows.append([fn, expected, ", ".join(langs)])
                return _html_table_block(title, ["filename", "expected_lang", "actual_lang"], lang_rows)

            def _list_table(title: str, items: List[str]) -> str:
                rows = [[fn] for fn in items]
                return _html_table_block(title, ["filename"], rows)

            html_path = timestamped_filename("scan_summary", "html", base_output_dir)
            html_parts: List[str] = []

            html_parts.append("<!doctype html>")
            html_parts.append("<html><head><meta charset=\"utf-8\"><title>MKV Scan Tracks Summary</title>")
            html_parts.append(
                "<style>"
                "body{font-family:'Segoe UI',Helvetica,Arial,sans-serif;background:#f8fbff;color:#1a1d21;padding:18px;line-height:1.5;}"
                "h1{font-size:1.6rem;margin:0 0 8px;font-weight:700;color:#0b5ed7;}"
                "h2{font-size:1.2rem;margin:16px 0 8px;font-weight:700;color:#0f5132;}"
                "h3{font-size:1rem;margin:12px 0 6px;font-weight:700;color:#0b5ed7;}"
                "h4{font-size:0.95rem;margin:10px 0 6px;font-weight:700;color:#495057;}"
                ".summary-bar{margin:10px 0 14px;padding:10px 12px;background:#e7f1ff;border:1px solid #cfe2ff;border-radius:8px;font-size:0.95rem;}"
                ".summary-bar strong{color:#0b5ed7;}"
                ".tt-table{border-collapse:collapse;width:100%;margin:6px 0 12px;background:#fff;}"
                ".tt-table th,.tt-table td{border:1px solid #dee2e6;padding:6px 8px;font-size:0.9rem;}"
                ".tt-table thead tr{background:linear-gradient(90deg,#0b5ed7,#4e8df7);color:#fff;}"
                ".tt-table th{color:#fff;text-align:left;}"
                ".tt-table tr:nth-child(even){background:#f8f9fa;}"
                ".tt-table tr.warn td{background:#fff3cd;}"
                ".tt-details{border:1px solid #ced4da;border-radius:8px;padding:6px 10px;margin:10px 0;background:#fff;}"
                ".tt-details.has-data > summary{background:linear-gradient(90deg,#d0f0d0,#e8f7e8);border:1px solid #b2dfb2;}"
                ".tt-details.no-data > summary{background:linear-gradient(90deg,#f8f9fa,#eef2ff);border:1px solid #d1d5db;}"
                ".tt-details > summary{cursor:pointer;font-weight:700;font-size:1rem;color:#0b5ed7;padding:6px 8px;border-radius:6px;}"
                ".tt-subdetails{margin:8px 0;border:1px solid #e9ecef;border-radius:6px;padding:4px 6px;background:#fdfdff;}"
                ".tt-subdetails.has-data > summary{background:linear-gradient(90deg,#e6f4ea,#f1faf3);border:1px solid #b2dfb2;}"
                ".tt-subdetails.no-data > summary{background:linear-gradient(90deg,#f8f9fa,#f1f3f5);border:1px solid #d1d5db;}"
                ".tt-subdetails summary{cursor:pointer;font-weight:600;font-size:0.95rem;color:#495057;padding:4px 6px;border-radius:4px;}"
                "a{color:#0b5ed7;text-decoration:none;}a:hover{text-decoration:underline;}"
                "</style>"
            )
            html_parts.append("</head><body>")
            html_parts.append(f"<h1>📋 Scan Summary</h1><p><strong>Generated:</strong> {html_path.name}</p>")

            totals_html = (
                f"<div class=\"summary-bar\">"
                f"🎞️ Files scanned: <strong>{len(file_counts)}</strong> &nbsp; "
                f"🎚️ Tracks: <strong>{len(all_rows)}</strong> &nbsp; "
                f"🧊 Non-HEVC: <strong>{len(mkv_non_hevc_rows) + len(non_mkv_non_hevc_rows)}</strong> &nbsp; "
                f"⚠️ Failures: <strong>{len(failed_files)}</strong> &nbsp; "
                f"⏭️ Skipped: <strong>{len(skip_files)}</strong> &nbsp; "
                f"🧩 Non-MKV ext subs rows: <strong>{len(non_mkv_ext_sub_rows)}</strong> &nbsp; "
                f"🧩 MKV ext subs rows: <strong>{len(mkv_ext_sub_rows)}</strong> &nbsp; "
                f"🧊 Non-MKV non-HEVC: <strong>{len(non_mkv_non_hevc_rows)}</strong>"
                f"</div>"
            )
            html_parts.append(totals_html)

            expected_vid = ", ".join(allowed_vid) if allowed_vid else "(any)"
            expected_aud = ", ".join(allowed_aud) if allowed_aud else "(any)"
            expected_sub = ", ".join(allowed_sub) if allowed_sub else "(any)"

            mkv_body_parts: List[str] = []
            mkv_body_parts.append(
                _wrap_subsection(
                    "📄 Per-file summary",
                    _html_table_block(
                        "📄 Per-file summary",
                        ["filename", "video", "audio", "subtitles", "lang_issues"],
                        [
                            [fn, str(v), str(a), str(s), lang_flags]
                            for fn, v, a, s, lang_flags in summary_rows
                        ],
                    ),
                    has_data=bool(summary_rows),
                    open_state=True,
                )
            )
            mkv_body_parts.append(
                _wrap_subsection("🎞️ more_than_1_video", _table_from_pairs("🎞️ more_than_1_video", more_than_1_video), has_data=bool(more_than_1_video))
            )
            mkv_body_parts.append(
                _wrap_subsection("🔊 more_than_1_audio", _table_from_pairs("🔊 more_than_1_audio", more_than_1_audio), has_data=bool(more_than_1_audio))
            )
            mkv_body_parts.append(
                _wrap_subsection("📝 more_than_1_subtitle", _table_from_pairs("📝 more_than_1_subtitle", more_than_1_subtitle), has_data=bool(more_than_1_subtitle))
            )
            mkv_body_parts.append(_wrap_subsection("🚫 no_video", _list_table("🚫 no_video", no_video), has_data=bool(no_video)))
            mkv_body_parts.append(_wrap_subsection("🔇 no_audio", _list_table("🔇 no_audio", no_audio), has_data=bool(no_audio)))
            mkv_body_parts.append(_wrap_subsection("🈚 no_subtitles", _list_table("🈚 no_subtitles", no_subtitles), has_data=bool(no_subtitles)))
            mkv_body_parts.append(_wrap_subsection("⚠️ lang mismatch - video", _lang_table("⚠️ lang mismatch - video", bad_vid, "video", expected_vid), has_data=bool(bad_vid)))
            mkv_body_parts.append(_wrap_subsection("⚠️ lang mismatch - audio", _lang_table("⚠️ lang mismatch - audio", bad_aud, "audio", expected_aud), has_data=bool(bad_aud)))
            mkv_body_parts.append(_wrap_subsection("⚠️ lang mismatch - subtitles", _lang_table("⚠️ lang mismatch - subtitles", bad_sub, "subtitles", expected_sub), has_data=bool(bad_sub)))

            mkv_has_data = any(
                [
                    summary_rows,
                    more_than_1_video,
                    more_than_1_audio,
                    more_than_1_subtitle,
                    no_video,
                    no_audio,
                    no_subtitles,
                    bad_vid,
                    bad_aud,
                    bad_sub,
                ]
            )

            html_parts.append(
                _wrap_details("🎬 MKV files", "".join(mkv_body_parts), has_data=mkv_has_data, open_state=True)
            )

            # Non-MKV section (vid)
            nm_stats: Dict[str, Dict[str, object]] = {}
            for r in non_mkv_scan_rows:
                out_name = r.get("output_filename", "")
                t = (r.get("type") or "").lower()
                if not out_name or not t:
                    continue
                entry = nm_stats.setdefault(
                    out_name,
                    {
                        "video": 0,
                        "audio": 0,
                        "subtitles": 0,
                        "langs": {"video": set(), "audio": set(), "subtitles": set()},
                        "source_path": "",
                    },
                )
                entry[t] = int(entry[t]) + 1  # type: ignore[index]
                lang_val = (r.get("lang") or "und").strip() or "und"
                entry["langs"].setdefault(t, set()).add(lang_val)  # type: ignore[index]
                if t == "video" and r.get("input_path"):
                    entry["source_path"] = r.get("input_path", "")

            more_than_1_video_nm: List[tuple[str, int]] = []
            more_than_1_audio_nm: List[tuple[str, int]] = []
            more_than_1_subtitle_nm: List[tuple[str, int]] = []
            no_video_nm: List[str] = []
            no_audio_nm: List[str] = []
            no_subtitles_nm: List[str] = []
            bad_lang_vid_nm: List[str] = []
            bad_lang_aud_nm: List[str] = []
            bad_lang_sub_nm: List[str] = []
            summary_rows_nm: List[tuple[str, int, int, int, str, str]] = []

            for out_name, info in nm_stats.items():
                v_raw = info.get("video", 0)
                a_raw = info.get("audio", 0)
                s_raw = info.get("subtitles", 0)
                v = int(v_raw) if isinstance(v_raw, (int, float, str)) and str(v_raw).strip() != "" else 0
                a = int(a_raw) if isinstance(a_raw, (int, float, str)) and str(a_raw).strip() != "" else 0
                s = int(s_raw) if isinstance(s_raw, (int, float, str)) and str(s_raw).strip() != "" else 0
                langs_map: Dict[str, Set[str]] = info.get("langs", {})  # type: ignore[assignment]
                if v > 1:
                    more_than_1_video_nm.append((out_name, v))
                if a > 1:
                    more_than_1_audio_nm.append((out_name, a))
                if s > 1:
                    more_than_1_subtitle_nm.append((out_name, s))
                if v == 0:
                    no_video_nm.append(out_name)
                if a == 0:
                    no_audio_nm.append(out_name)
                if s == 0:
                    no_subtitles_nm.append(out_name)

                lang_flags_nm: List[str] = []
                if v > 0:
                    for lang_val in langs_map.get("video", set()) or {"und"}:
                        if not _lang_matches(lang_val, allowed_vid):
                            bad_lang_vid_nm.append(out_name)
                            lang_flags_nm.append("vid")
                            break
                if a > 0:
                    for lang_val in langs_map.get("audio", set()) or {"und"}:
                        if not _lang_matches(lang_val, allowed_aud):
                            bad_lang_aud_nm.append(out_name)
                            lang_flags_nm.append("aud")
                            break
                if s > 0:
                    for lang_val in langs_map.get("subtitles", set()) or {"und"}:
                        if not _lang_matches(lang_val, allowed_sub):
                            bad_lang_sub_nm.append(out_name)
                            lang_flags_nm.append("sub")
                            break

                summary_rows_nm.append(
                    (
                        out_name,
                        v,
                        a,
                        s,
                        ",".join(lang_flags_nm),
                        str(info.get("source_path", "") or ""),
                    )
                )

            def _lang_table_nm(title: str, target: List[str], key: str, expected: str) -> str:
                lang_rows_nm: List[List[str]] = []
                for fn in target:
                    langs_map = nm_stats.get(fn, {})
                    if isinstance(langs_map, dict):
                        langs_entry = langs_map.get("langs", {})
                        if isinstance(langs_entry, dict):
                            langs = sorted(langs_entry.get(key, set()) or {"und"})
                        else:
                            langs = ["und"]
                    else:
                        langs = ["und"]
                    lang_rows_nm.append([fn, expected, ", ".join(langs)])
                return _html_table_block(title, ["output_filename", "expected_lang", "actual_lang"], lang_rows_nm)

            non_mkv_body: List[str] = []
            non_mkv_body.append(
                _wrap_subsection(
                    "📄 Per-file summary",
                    _html_table_block(
                        "📄 Per-file summary",
                        ["output_filename", "video", "audio", "subtitles", "lang_issues", "source_path"],
                        [
                            [fn, str(v), str(a), str(s), lang_flags, src]
                            for fn, v, a, s, lang_flags, src in sorted(summary_rows_nm, key=lambda r: r[0])
                        ],
                    ),
                    has_data=bool(summary_rows_nm),
                    open_state=True,
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "🎞️ more_than_1_video",
                    _table_from_pairs("🎞️ more_than_1_video", sorted(more_than_1_video_nm, key=lambda r: r[0])),
                    has_data=bool(more_than_1_video_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "🔊 more_than_1_audio",
                    _table_from_pairs("🔊 more_than_1_audio", sorted(more_than_1_audio_nm, key=lambda r: r[0])),
                    has_data=bool(more_than_1_audio_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "📝 more_than_1_subtitle",
                    _table_from_pairs("📝 more_than_1_subtitle", sorted(more_than_1_subtitle_nm, key=lambda r: r[0])),
                    has_data=bool(more_than_1_subtitle_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "🚫 no_video",
                    _list_table("🚫 no_video", sorted(no_video_nm)),
                    has_data=bool(no_video_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "🔇 no_audio",
                    _list_table("🔇 no_audio", sorted(no_audio_nm)),
                    has_data=bool(no_audio_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "🈚 no_subtitles",
                    _list_table("🈚 no_subtitles", sorted(no_subtitles_nm)),
                    has_data=bool(no_subtitles_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "⚠️ lang mismatch - video",
                    _lang_table_nm("⚠️ lang mismatch - video", sorted(set(bad_lang_vid_nm)), "video", expected_vid),
                    has_data=bool(bad_lang_vid_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "⚠️ lang mismatch - audio",
                    _lang_table_nm("⚠️ lang mismatch - audio", sorted(set(bad_lang_aud_nm)), "audio", expected_aud),
                    has_data=bool(bad_lang_aud_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "⚠️ lang mismatch - subtitles",
                    _lang_table_nm("⚠️ lang mismatch - subtitles", sorted(set(bad_lang_sub_nm)), "subtitles", expected_sub),
                    has_data=bool(bad_lang_sub_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "📁 non_video_files",
                    _html_table_block(
                        "📁 non_video_files",
                        ["filename", "path"],
                        [
                            [p.name, str(p)]
                            for p in sorted(unmatched_sub_files)
                        ],
                    ),
                    has_data=bool(unmatched_sub_files),
                )
            )

            non_mkv_has_data = bool(
                summary_rows_nm
                or more_than_1_video_nm
                or more_than_1_audio_nm
                or more_than_1_subtitle_nm
                or no_video_nm
                or no_audio_nm
                or no_subtitles_nm
                or bad_lang_vid_nm
                or bad_lang_aud_nm
                or bad_lang_sub_nm
                or unmatched_sub_files
            )

            html_parts.append(
                _wrap_details("🧩 Non-MKV files", "".join(non_mkv_body), has_data=non_mkv_has_data, open_state=True)
            )

            # CSV links
            try:
                csv_links: List[str] = []
                for label, info in written_reports.items():
                    paths = info.get("paths") if isinstance(info, dict) else None
                    if not paths:
                        continue
                    if not isinstance(paths, list):
                        paths = [paths]
                    for p in paths:
                        name = p.name if isinstance(p, Path) else str(p)
                        csv_links.append(f"<li><a href=\"{name}\">{label} → {name}</a></li>")
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
