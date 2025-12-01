"""
Subtitle matching helpers shared across video tooling.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

from common.utils.track_utils import flag_string


def subtitle_matches(video: Path, sub: Path) -> bool:
    v = re.sub(r"[^a-z0-9]", "", video.stem.lower())
    s = re.sub(r"[^a-z0-9]", "", sub.stem.lower())
    return bool(v) and bool(s) and (v in s or s in v)


def match_external_subs(
    videos: List,
    subs: List,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Path]]:
    """
    Given probe results for videos and subtitles, attempt to match subs by filename stem.
    Returns:
      mkv_rows: rows for MKV videos with matched subs
      non_mkv_rows: rows for non-MKV videos with matched subs
      unmatched_sub_paths: list of subtitle paths that were not matched
    """
    mkv_rows: List[Dict[str, str]] = []
    non_mkv_rows: List[Dict[str, str]] = []
    matched_subs: Set[Path] = set()
    for v in videos:
        matched_for_video = [s for s in subs if subtitle_matches(v.path, s.path)]
        if not matched_for_video:
            continue

        dest_rows = mkv_rows if v.path.suffix.lower() == ".mkv" else non_mkv_rows
        def _next_track_id(rows: List[Dict[str, str]]) -> str:
            ids: List[int] = []
            for r in rows:
                try:
                    ids.append(int(str(r.get("id", "")).strip()))
                except Exception:
                    continue
            return str(max(ids) + 1 if ids else 0)

        for s in matched_for_video:
            matched_subs.add(s.path)
            for tr in s.tracks or [{"type": "subtitles", "lang": "und", "codec": "", "id": "", "name": "", "edited_name": "", "default": "", "forced": "", "encoding": "", "path": str(s.path)}]:
                base = tr.copy()
                base["default"] = flag_string(base.get("default", False))
                base["forced"] = flag_string(base.get("forced", False))
                # Assign subtitle track id after existing tracks on the target video
                base["id"] = _next_track_id(dest_rows + (v.tracks or []))
                base.update({
                    "output_filename": v.path.with_suffix(".mkv").name,
                    "output_path": str(v.path.with_suffix(".mkv")),
                    "input_path": str(s.path),
                })
                dest_rows.append(base)

        if v.tracks:
            for tr in v.tracks:
                base = tr.copy()
                base["default"] = flag_string(base.get("default", False))
                base["forced"] = flag_string(base.get("forced", False))
                base.update({
                    "output_filename": v.path.with_suffix(".mkv").name,
                    "output_path": str(v.path.with_suffix(".mkv")),
                    "input_path": str(v.path),
                })
                dest_rows.append(base)
    unmatched = [p for p in (s.path for s in subs) if p not in matched_subs]
    return mkv_rows, non_mkv_rows, unmatched
