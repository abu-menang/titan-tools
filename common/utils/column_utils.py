"""
Column spec loading helpers shared across reporting scripts.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence

from common.shared.loader import load_yaml_resource
from common.shared.report import ColumnSpec


def build_column_spec(raw: Mapping[str, Any]) -> ColumnSpec:
    key = str(raw.get("key") or raw.get("id") or raw.get("name") or "").strip()
    if not key:
        raise ValueError("Column definition is missing a 'key'")
    header = str(raw.get("header") or key)
    width_val = raw.get("width")
    width = int(width_val) if width_val not in (None, "") else None
    return ColumnSpec(key, header, width=width)


def load_column_specs(resource_name: str) -> Dict[str, List[ColumnSpec]]:
    data = load_yaml_resource(resource_name)
    if not isinstance(data, Mapping):
        raise ValueError(f"{resource_name} YAML root must be a mapping")
    columns: Dict[str, List[ColumnSpec]] = {}
    for section, raw_cols in data.items():
        if not isinstance(raw_cols, Sequence):
            raise ValueError(f"Section '{section}' in {resource_name} must be a list")
        cols: List[ColumnSpec] = []
        for raw in raw_cols:
            if not isinstance(raw, Mapping):
                raise ValueError(f"Entries in section '{section}' must be mappings")
            cols.append(build_column_spec(raw))
        columns[section] = cols
    return columns
