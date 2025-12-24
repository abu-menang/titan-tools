"""
Track classification helpers shared across video tooling.
"""

from __future__ import annotations

from typing import Dict, List, Tuple


def classify_tracks(
    rows: List[Dict[str, str]],
    allowed_vid: List[str],
    allowed_aud: List[str],
    allowed_sub: List[str],
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Split tracks per file into ok vs issues buckets based on presence/count and language rules.
    """
    issues: List[Dict[str, str]] = []
    ok: List[Dict[str, str]] = []
    by_file: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        key = (r.get("output_filename") or r.get("filename") or r.get("path") or "").strip()
        by_file.setdefault(key, []).append(r)

    def _lang_ok(lang: str, allowed: List[str]) -> bool:
        if not allowed:
            return True
        l = (lang or "").lower()
        return any(l.startswith(a) for a in allowed)

    for key, items in by_file.items():
        v = sum(1 for i in items if (i.get("type") or "").lower() == "video")
        a = sum(1 for i in items if (i.get("type") or "").lower() == "audio")
        s = sum(1 for i in items if (i.get("type") or "").lower() == "subtitles")
        lang_issue = any(
            (i.get("type") or "").lower() == "video" and not _lang_ok(i.get("lang", ""), allowed_vid)
            or (i.get("type") or "").lower() == "audio" and not _lang_ok(i.get("lang", ""), allowed_aud)
            or (i.get("type") or "").lower() == "subtitles" and not _lang_ok(i.get("lang", ""), allowed_sub)
            for i in items
        )
        # 0-count cases are handled upstream (broken_*). Here only >1 counts or language issues mark as issues.
        has_issue = v > 1 or a > 1 or s == 0 or lang_issue
        if has_issue:
            issues.extend(items)
        else:
            ok.extend(items)
    return ok, issues
