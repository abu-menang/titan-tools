from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, List, Optional

TIMESTAMPED_CSV_RE = re.compile(r"^(?P<prefix>.+?)_(?P<ts>\d{4}-\d{2}-\d{2}_\d{6})\.csv$")


def latest_timestamped_csvs(csv_files: Iterable[Path]) -> List[Path]:
    """Return the latest timestamped CSV per prefix from a collection."""
    latest: dict[str, tuple[str, Path]] = {}
    for p in csv_files:
        m = TIMESTAMPED_CSV_RE.match(p.name)
        if not m:
            continue
        prefix = m.group("prefix")
        ts = m.group("ts")
        current = latest.get(prefix)
        if current is None or ts > current[0]:
            latest[prefix] = (ts, p)
    return [item[1] for item in latest.values()]


def discover_csvs(
    roots: Iterable[Path | str],
    output_root: Optional[Path | str] = None,
    target_dir: str | Path = "",
) -> List[Path]:
    """Collect timestamped CSVs under a target directory across roots."""
    roots_resolved = [Path(r).expanduser().resolve() for r in roots]
    collected: List[Path] = []
    for root in roots_resolved:
        base = Path(output_root).expanduser().resolve() if output_root else root
        target = base / str(target_dir)
        if not target.exists() or not target.is_dir():
            continue
        collected.extend(sorted(target.glob("*.csv")))
    latest = latest_timestamped_csvs(collected)
    return sorted(latest)


__all__ = ["latest_timestamped_csvs", "discover_csvs"]
