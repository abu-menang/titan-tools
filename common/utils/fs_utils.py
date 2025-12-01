"""
Shared filesystem utilities.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Iterator, Optional

from common.base.logging import get_logger

log = get_logger(__name__)


def path_is_relative_to(path: Path, ancestor: Optional[Path]) -> bool:
    if ancestor is None:
        return False
    try:
        path.relative_to(ancestor)
        return True
    except ValueError:
        return False


def iter_files(
    roots: Iterable[Path],
    exclude_dir: Optional[Path] = None,
    include_all: bool = True,
) -> Iterator[Path]:
    """
    Walk the provided roots and yield file paths, with optional exclusion.

    Mirrors the behavior previously embedded in video.scan._iter_files.
    """
    for root in roots:
        root = root.resolve()
        if not root.exists():
            log.warning("⚠️ missing_root path=%s", root)
            continue
        if root.is_file():
            if include_all:
                yield root
            continue
        resolved_exclude = exclude_dir.resolve() if exclude_dir else None
        apply_exclude = resolved_exclude is not None and resolved_exclude != root
        for dirpath, dirnames, filenames in os.walk(root):
            if apply_exclude:
                dirnames[:] = [d for d in dirnames if not path_is_relative_to((Path(dirpath) / d).resolve(), resolved_exclude)]
                filenames = [f for f in filenames if not path_is_relative_to((Path(dirpath) / f).resolve(), resolved_exclude)]
            for fname in filenames:
                yield Path(dirpath) / fname
