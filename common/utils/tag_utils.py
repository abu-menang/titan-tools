"""
Helpers for working with filesystem-level tags (extended attributes).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Tuple

from common.base.logging import get_logger

log = get_logger(__name__)


def read_fs_tags(path: Path) -> Tuple[str, List[str]]:
    """
    Return the raw tag string(s) and normalized tag list from extended attributes.
    Only tags whose attribute keys contain 'tag' are considered.
    """
    if not path.is_file():
        return "", []
    if not hasattr(os, "listxattr") or not hasattr(os, "getxattr"):
        return "", []
    try:
        keys = os.listxattr(path)
    except Exception:
        return "", []

    raw_values: List[str] = []
    tags: List[str] = []
    for key in keys:
        if not any(token in key.lower() for token in ["tag"]):
            continue
        try:
            val = os.getxattr(path, key)
        except Exception:
            continue
        decoded = ""
        try:
            decoded = val.decode("utf-8", errors="ignore")
        except Exception:
            decoded = ""
        if not decoded:
            continue
        raw_values.append(decoded)
        parts = [p.strip() for p in decoded.replace(";", ",").split(",") if p.strip()]
        tags.extend(parts)
    return ", ".join(raw_values), [t.lower() for t in tags if t]


def write_fs_tag(path: Path, key: str, value: str) -> bool:
    """
    Write a tag value to the file's extended attributes.

    Returns True on success, False if the platform or filesystem does not support
    xattrs or if the write fails.
    """
    if not path.is_file():
        log.info("Tagging skipped (not a file): %s key=%s value=%s", path, key, value)
        return False
    if not hasattr(os, "setxattr"):
        log.info("Tagging unsupported on this platform for %s key=%s value=%s", path, key, value)
        return False
    try:
        os.setxattr(path, key, value.encode("utf-8", errors="ignore"))
        log.info("Tagged %s key=%s value=%s", path, key, value)
        return True
    except Exception:
        log.warning("Failed to tag %s key=%s value=%s", path, key, value)
        return False
