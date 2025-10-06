import csv, json
from titan_tools.common import color_text

def write_csv(rows, headers, path, logger):
    """Write data rows to a CSV file."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        if headers:
            writer.writerow(headers)
        writer.writerows(rows)
    logger.info(color_text(f"✅ CSV written: {path}", "green"))

def write_json(data, path, logger):
    """Write structured data to a JSON file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(color_text(f"✅ JSON written: {path}", "green"))
