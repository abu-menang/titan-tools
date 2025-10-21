"""Filesystem helper utilities shared across common modules."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable


def ensure_dir(path: Path | str) -> Path:
    p = Path(path).expanduser()
    p.mkdir(parents=True, exist_ok=True)
    return p


def ensure_parent(path: Path | str) -> Path:
    return ensure_dir(Path(path).expanduser().parent)


def human_size(num: float, suffix: str = "B") -> str:
    units: Iterable[str] = ["", "K", "M", "G", "T", "P", "E", "Z"]
    value = float(num)
    for unit in units:
        if abs(value) < 1024.0:
            return f"{value:3.1f}{unit}{suffix}"
        value /= 1024.0
    return f"{value:.1f}Y{suffix}"
