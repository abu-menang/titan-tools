"""
titan_core.cli

CLI entrypoint and command registry for Titan Tools.
"""

import importlib
from pathlib import Path
from titan_core.core.logging import get_logger

log = get_logger(__name__)

def discover_commands() -> list[str]:
    """Discover all CLI modules under titan_core.cli."""
    cli_dir = Path(__file__).parent
    commands = []
    for path in cli_dir.glob("*.py"):
        if path.name not in {"__init__.py", "base.py"}:
            commands.append(path.stem)
    return commands

def load_command(name: str):
    """Dynamically import a CLI command by name."""
    try:
        module = importlib.import_module(f"titan_core.cli.{name}")
        log.debug(f"Loaded CLI command module: {name}")
        return module
    except ModuleNotFoundError:
        log.error(f"❌ Command not found: {name}")
        return None

__all__ = ["discover_commands", "load_command"]
