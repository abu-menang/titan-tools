import sys, logging
from titan_tools.common import color_text

def setup_logger(name, log_path):
    """Create a file + console logger."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(color_text(f"🪵 Log started: {log_path}", "cyan"))
    return logger
