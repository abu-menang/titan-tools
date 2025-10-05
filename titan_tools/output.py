from pathlib import Path
from titan_tools.common import now_ts, color_text

def make_output_dir(base_dir, suffix):
    """Create a timestamped output directory."""
    run_dir = Path(base_dir) / f"{now_ts()}_{suffix}"
    run_dir.mkdir(parents=True, exist_ok=True)
    print(color_text(f"📁 Output folder: {run_dir}", "cyan"))
    return run_dir
