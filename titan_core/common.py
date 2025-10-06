import sys
import datetime

def now_ts():
    """Return current timestamp string like 20251005-123456"""
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

def color_text(text, color):
    """Return ANSI-colored text for terminal output."""
    colors = {
        "red": "\033[91m", "green": "\033[92m", "yellow": "\033[93m",
        "cyan": "\033[96m", "blue": "\033[94m", "magenta": "\033[95m",
        "reset": "\033[0m",
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"

def print_progress(current, total, width=40):
    """Inline progress bar with count and percentage."""
    if total <= 0:
        return
    percent = (current / total) * 100
    filled = int(width * current // total)
    bar = "#" * filled + "-" * (width - filled)
    sys.stdout.write(f"\r[{bar}] {percent:5.1f}% ({current}/{total})")
    sys.stdout.flush()
    if current == total:
        sys.stdout.write("\n")
