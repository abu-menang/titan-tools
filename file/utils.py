"""Shared helpers for filesystem tooling in the `file` package."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from common.base.fs import ensure_dir


def resolve_output_directory(
    root: Path,
    output_dir: Optional[Path],
    *,
    subdir: Optional[str] = None,
) -> Path:
    """Resolve an output directory rooted beneath the provided scan root.

    Args:
        root: Root directory being processed.
        output_dir: Optional path (relative or absolute). When omitted, defaults
            to the root directory itself.
        subdir: Optional directory to append underneath the resolved base.

    Returns:
        Path object pointing to an existing directory beneath ``root``.

    Raises:
        ValueError: If ``output_dir`` is absolute and not contained within ``root``.
        NotADirectoryError: If ``root`` is not a directory.
    """

    root = root.expanduser().resolve()
    if not root.is_dir():
        raise NotADirectoryError(f"Root path is not a directory: {root}")

    if output_dir is None:
        base = root
    else:
        candidate = output_dir.expanduser()
        if not candidate.is_absolute():
            base = root / candidate
        else:
            candidate = candidate.resolve()
            try:
                candidate.relative_to(root)
            except ValueError as exc:
                raise ValueError(
                    f"Output directory {candidate} must reside within root {root}"
                ) from exc
            base = candidate

    if subdir:
        base = base / subdir

    return ensure_dir(base)
