import json, subprocess
from titan_tools.common import color_text

def get_metadata_title(filepath, logger=None):
    """Extract Title metadata using ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(filepath)],
            capture_output=True, text=True, check=True
        )
        meta = json.loads(result.stdout or "{}")
        return meta.get("format", {}).get("tags", {}).get("title", "") or ""
    except Exception as e:
        if logger: logger.warning(f"Metadata extraction failed for {filepath}: {e}")
        return ""

def run_mkvmerge_json(filepath):
    """Return mkvmerge -J JSON output."""
    try:
        res = subprocess.run(["mkvmerge", "-J", str(filepath)], capture_output=True, text=True, check=True)
        return json.loads(res.stdout)
    except Exception as e:
        return {"error": str(e)}
