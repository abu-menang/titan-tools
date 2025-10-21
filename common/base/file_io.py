"""Utility helpers for performing file I/O with consistent defaults."""

from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional

import yaml


DEFAULT_ENCODING = "utf-8"


def _to_path(path: Path | str) -> Path:
    return Path(path).expanduser()


def read_text(path: Path | str, encoding: str = DEFAULT_ENCODING) -> str:
    return _to_path(path).read_text(encoding=encoding)


def write_text(path: Path | str, content: str, encoding: str = DEFAULT_ENCODING) -> None:
    _to_path(path).write_text(content, encoding=encoding)


def read_bytes(path: Path | str) -> bytes:
    return _to_path(path).read_bytes()


def write_bytes(path: Path | str, payload: bytes) -> None:
    _to_path(path).write_bytes(payload)


@contextmanager
def open_file(
    path: Path | str,
    mode: str = "r",
    *,
    encoding: str = DEFAULT_ENCODING,
    newline: Optional[str] = None,
) -> Iterator[Any]:
    path_obj = _to_path(path)
    kwargs: dict[str, Any] = {}
    is_binary = "b" in mode
    if is_binary:
        if newline is not None:
            raise ValueError("newline is not supported in binary mode")
    else:
        kwargs["encoding"] = encoding
        kwargs["newline"] = newline
    with open(path_obj, mode, **kwargs) as handle:
        yield handle


def read_json(path: Path | str) -> Any:
    with open_file(path, "r") as handle:
        return json.load(handle)


def write_json(
    path: Path | str,
    payload: Any,
    *,
    indent: int = 2,
    sort_keys: bool = False,
) -> None:
    with open_file(path, "w") as handle:
        json.dump(payload, handle, indent=indent, sort_keys=sort_keys)


def read_yaml(path: Path | str) -> Mapping[str, Any] | list[Any]:
    with open_file(path, "r") as handle:
        data = yaml.safe_load(handle)
    return data if data is not None else {}


def write_yaml(path: Path | str, payload: Mapping[str, Any] | list[Any]) -> None:
    with open_file(path, "w") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)
