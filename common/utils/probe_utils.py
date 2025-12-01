"""
Shared probing utilities.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Tuple

from common.base.ops import run_command


def probe_mkvmerge(path: Path) -> Tuple[int, Optional[dict], str]:
    """
    Run mkvmerge -J against the given path and return (code, payload, error_message).
    Payload is parsed JSON on success; error_message contains stderr or parse failure.
    """
    code, out, err = run_command(["mkvmerge", "-J", str(path)], capture=True, stream=False)
    if code != 0 or not out:
        return code, None, err or "mkvmerge returned no output"
    try:
        payload = json.loads(out)
        return code, payload, ""
    except json.JSONDecodeError:
        return code, None, "invalid JSON from mkvmerge"
