#!/usr/bin/env python3
"""
Titan Tools Command-Line Utility
--------------------------------
Provides version info, environment diagnostics,
and discovery of installed Titan-based tools.
"""

import importlib.metadata
import sys
import os
from pathlib import Path
from titan_core.common import color_text


def get_titan_tools_version():
    try:
        return importlib.metadata.version("titan-tools")
    except importlib.metadata.PackageNotFoundError:
        return "unknown (editable/dev mode)"


def list_installed_tools():
    """Search /usr/local/bin and ~/.local/bin for Titan-based scripts."""
    search_paths = [
        Path("/usr/local/bin"),
        Path("/usr/local/sbin"),
        Path.home() / ".local" / "bin",
    ]

    titan_tools = []
    for base in search_paths:
        if not base.exists():
            continue
        for item in sorted(base.iterdir()):
            if item.is_file() and item.name.startswith(("file-", "mkv-", "titan-")):
                titan_tools.append(item)
    return titan_tools


def show_info():
    version = get_titan_tools_version()
    pkg_path = Path(__file__).resolve().parent
    print(color_text("🧱 Titan Tools Environment", "cyan"))
    print(f"📦 Version: {version}")
    print(f"📂 Package Location: {pkg_path}")
    print(f"🐍 Python: {sys.executable}")
    print(f"🌍 PYTHONPATH: {os.environ.get('PYTHONPATH', '(not set)')}\n")


def show_installed_tools():
    tools = list_installed_tools()
    if not tools:
        print(color_text("⚠️  No Titan-based tools found in /usr/local/bin or ~/.local/bin.", "yellow"))
        return

    print(color_text("🧰 Installed Titan Tools:", "cyan"))
    for tool in tools:
        size = tool.stat().st_size / 1024
        print(f"  🔹 {tool.name:<20} ({size:.1f} KB)  →  {tool}")
    print("")


def show_help():
    print("Usage:")
    print("  titan_tools            Show Titan Tools version and environment info")
    print("  titan_tools list       List installed Titan-based scripts")
    print("  titan_tools help       Show this help message")
    print("")


def main():
    args = sys.argv[1:]
    if not args:
        # Default behavior: show info
        show_info()
        sys.exit(0)

    cmd = args[0].lower()

    if cmd in {"list", "ls"}:
        show_installed_tools()
    elif cmd in {"info", "--version", "-v"}:
        show_info()
    elif cmd in {"help", "-h"}:
        show_help()
    else:
        print(color_text(f"⚠️  Unknown command: {cmd}", "yellow"))
        print("Try: titan_tools help")
        sys.exit(1)


if __name__ == "__main__":
    main()
