from __future__ import annotations

import base64
import json
import subprocess
import sys
from pathlib import Path
import textwrap

import pytest

from common.shared.loader import load_task_config


def _write_config(tmp_path: Path, filename: str, content: str) -> Path:
    path = tmp_path / filename
    path.write_text(content, encoding="utf-8")
    return path


def _wrap_task_config(
    task: str,
    body: str,
    logging_body: str | None = None,
    defaults_body: str | None = None,
) -> str:
    logging_block = (logging_body or "level: INFO").strip()
    body_block = body.strip()
    parts: list[str] = []
    if defaults_body:
        parts.append("task_defaults:\n")
        parts.append(textwrap.indent(defaults_body.strip(), "  "))
        parts.append("\n")
    parts.append("logging:\n")
    parts.append(textwrap.indent(logging_block, "  "))
    parts.append("\ntasks:\n")
    parts.append(f"  {task}:\n")
    parts.append(textwrap.indent(body_block, "    "))
    parts.append("\n")
    return "".join(parts)


def test_load_task_config_mkv_clean(tmp_path: Path) -> None:
    output_root = tmp_path / "reports"
    defaults = (
        f"roots:\n  - '{tmp_path}'\n"
        "output_root: './reports'\n"
    )

    cfg_path = _write_config(
        tmp_path,
        "clean.yaml",
        _wrap_task_config(
            "vid_mkv_clean",
            (
                "dry_run: true\n"
            ),
            defaults_body=defaults,
        ),
    )

    config = load_task_config("vid_mkv_clean", cfg_path)
    assert config["roots"] == [str(tmp_path)]
    assert config["output_dir"] == str(output_root.resolve())
    assert config["dry_run"] is True
    logging_cfg = config.get("__logging__", {})
    assert logging_cfg.get("level") == "INFO"
    assert logging_cfg.get("log_dir") == str((output_root / "logs").resolve())


def test_load_task_config_scan(tmp_path: Path) -> None:
    output_root = tmp_path / "reports"
    defaults = (
        f"roots:\n  - '{tmp_path}'\n"
        "output_root: './reports'\n"
    )
    cfg_path = _write_config(
        tmp_path,
        "scan.yaml",
        _wrap_task_config(
            "vid_mkv_scan",
            (
                "dry_run: false\n"
            ),
            defaults_body=defaults,
        ),
    )

    config = load_task_config("vid_mkv_scan", cfg_path)
    assert config["roots"] == [str(tmp_path)]
    assert config["output_dir"] == str(output_root.resolve())
    assert config["dry_run"] is False


def test_load_task_config_rename(tmp_path: Path) -> None:
    output_root = tmp_path / "reports"
    defaults = (
        f"roots:\n  - '{tmp_path}'\n"
        "output_root: './reports'\n"
    )

    cfg_path = _write_config(
        tmp_path,
        "rename.yaml",
        _wrap_task_config(
            "vid_rename",
            (
                "dry_run: true\n"
                "no_meta: true\n"
            ),
            defaults_body=defaults,
        ),
    )

    config = load_task_config("vid_rename", cfg_path)
    assert config["roots"] == [str(tmp_path)]
    assert config["output_dir"] == str(output_root.resolve())
    assert config["dry_run"] is True
    assert config["no_meta"] is True


def test_load_task_config_file_scan(tmp_path: Path) -> None:
    defaults = (
        f"roots:\n  - '{tmp_path}'\n"
        "output_root: './reports'\n"
    )

    cfg_path = _write_config(
        tmp_path,
        "file_scan.yaml",
        _wrap_task_config(
            "file_scan",
            (
                "base_name: 'inventory'\n"
            ),
            defaults_body=defaults,
        ),
    )

    config = load_task_config("file_scan", cfg_path)
    assert config["roots"] == [str(tmp_path)]
    assert config["output_dir"] == str((tmp_path / "reports").resolve())
    assert config["base_name"] == "inventory"


def test_load_task_config_file_rename(tmp_path: Path) -> None:
    defaults = (
        f"roots:\n  - '{tmp_path}'\n"
        "output_root: './reports'\n"
    )

    cfg_path = _write_config(
        tmp_path,
        "file_rename.yaml",
        _wrap_task_config(
            "file_rename",
            (
                "base_name: 'file_scan'\n"
                "dry_run: true\n"
            ),
            defaults_body=defaults,
        ),
    )

    config = load_task_config("file_rename", cfg_path)
    assert config["roots"] == [str(tmp_path)]
    assert config["output_dir"] == str((tmp_path / "reports").resolve())
    assert config["base_name"] == "file_scan"
    assert config["dry_run"] is True
    assert config.get("mapping") is None


def test_load_task_config_logging_override(tmp_path: Path) -> None:
    output_root = tmp_path / "reports"
    defaults = (
        f"roots:\n  - '{tmp_path}'\n"
        "output_root: './reports'\n"
    )
    cfg_path = _write_config(
        tmp_path,
        "scan_override.yaml",
        _wrap_task_config(
            "vid_mkv_scan",
            (
                "logging:\n"
                "  file_prefix: per_task\n"
            ),
            logging_body="level: WARNING\nfile_prefix: global_default",
            defaults_body=defaults,
        ),
    )

    config = load_task_config("vid_mkv_scan", cfg_path)
    logging_cfg = config.get("__logging__", {})

    assert logging_cfg.get("level") == "WARNING"
    assert logging_cfg.get("file_prefix") == "per_task"
    assert config["output_dir"] == str((output_root / "vid_mkv_scan").resolve())
    assert logging_cfg.get("log_dir") == str((output_root / "logs").resolve())


def test_load_task_config_logging_requires_mapping(tmp_path: Path) -> None:
    defaults = (
        f"roots:\n  - '{tmp_path}'\n"
        "output_root: './reports'\n"
    )
    cfg_path = _write_config(
        tmp_path,
        "invalid_logging.yaml",
        _wrap_task_config(
            "vid_mkv_clean",
            (
                "logging: not_a_mapping\n"
            ),
            defaults_body=defaults,
        ),
    )

    with pytest.raises(ValueError):
        load_task_config("vid_mkv_clean", cfg_path)


def test_cli_base64_encoding(tmp_path: Path) -> None:
    output_root = tmp_path / "reports"
    defaults = (
        f"roots:\n  - '{tmp_path}'\n"
        "output_root: './reports'\n"
    )
    cfg_path = _write_config(
        tmp_path,
        "clean.yaml",
        _wrap_task_config(
            "vid_mkv_clean",
            defaults_body=defaults,
        ),
    )

    output = subprocess.check_output(
        [sys.executable, "-m", "common.shared.loader", "vid_mkv_clean", str(cfg_path)],
        cwd=Path(__file__).resolve().parent.parent.parent,
    )

    payload = base64.b64decode(output.strip()).decode("utf-8")
    config = json.loads(payload)

    assert config["roots"] == [str(tmp_path)]
    logging_cfg = config.get("__logging__", {})
    assert logging_cfg.get("level") == "INFO"
    assert config["output_dir"] == str(output_root.resolve())
    assert logging_cfg.get("log_dir") == str((output_root / "logs").resolve())
