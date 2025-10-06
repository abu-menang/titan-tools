"""
titan_core.core

Titan Core unified interface.

Provides a single import path for all common core utilities:
  - Logging
  - File system operations
  - Utilities (paths, progress, timing)
  - Reporting
"""

from titan_core.core.logging import setup_logging, get_logger, TitanLogger
from titan_core.core.utils import (
    ensure_dir,
    safe_filename,
    human_size,
    path_exists,
    move_file,
    copy_file,
    remove_file,
    Progress,
    timeit,
    clear_console,
    confirm,
)
from titan_core.core.ops import (
    run_command,
    move_to_trash,
    remove_dir,
    copy_tree,
    file_info,
)
from titan_core.core.report import (
    write_json,
    write_csv,
    summarize_counts,
    export_report,
    timestamped_filename,
)

__all__ = [
    # Logging
    "setup_logging",
    "get_logger",
    "TitanLogger",

    # Utilities
    "ensure_dir",
    "safe_filename",
    "human_size",
    "path_exists",
    "move_file",
    "copy_file",
    "remove_file",
    "Progress",
    "timeit",
    "clear_console",
    "confirm",

    # Operations
    "run_command",
    "move_to_trash",
    "remove_dir",
    "copy_tree",
    "file_info",

    # Reporting
    "write_json",
    "write_csv",
    "summarize_counts",
    "export_report",
    "timestamped_filename",
]
