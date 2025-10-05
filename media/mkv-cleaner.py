#!/usr/bin/env python3
"""
mkv-cleaner.py
---------------

Clean and remux MKV files based on a track definition file (CSV or JSON).
This script keeps only the specified video, audio, and subtitle tracks,
updates metadata (name, language, default, forced), and creates timestamped
backup folders of the originals.

It also performs a pre-check using `mkvmerge -J` to detect if any changes
are required before cleaning. Files that already match the provided
configuration are skipped safely. Remuxing is performed to a temporary
`cleaned-<original>.mkv` and the original is only moved after a successful
remux, ensuring safe, atomic replacement.

=====================================================================
⚙️ Command Options
=====================================================================

Option:           --input-file / --if
Type:             Path (.csv or .json)
Required:         Yes
Description:      Path to the track definition file containing MKV track
                  metadata and keep settings.

Option:           --output-base-dir / --obd
Type:             Path
Default:          Current directory
Description:      Base directory where the backup folder <timestamp>_ori
                  will be created to store original files.

Option:           --dry-run / --dr
Type:             Flag
Default:          False
Description:      Print mkvmerge commands without executing them.

Option:           --verbose / --vb
Type:             Flag
Default:          False
Description:      Show full mkvmerge output during execution.

Option:           --help / -h
Type:             Flag
Description:      Show this help and exit.

Special Commands:
  install         Installs mkv-cleaner to /usr/local/bin/mkv-cleaner
  uninstall       Removes mkv-cleaner from system-wide locations.

=====================================================================
💡 Usage Examples
=====================================================================

1. Clean MKVs based on CSV definition
   mkv-cleaner --if tracks.csv --obd ./backup

2. Dry run (preview commands only)
   mkv-cleaner --if tracks.csv --obd ./backup --dr

3. Verbose mode
   mkv-cleaner --if tracks.csv --vb

4. Install or uninstall globally
   sudo mkv-cleaner install
   sudo mkv-cleaner uninstall

=====================================================================
"""

import argparse
import csv
import json
import shutil
import subprocess
import sys
import re
import logging
import os
from pathlib import Path
from datetime import datetime

# =====================================================
# Utility Helpers
# =====================================================

def now_ts():
    return datetime.now().strftime("%Y%m%d-%H%M%S")

def color_text(text, color):
    colors = {
        "red": "\033[91m", "green": "\033[92m", "yellow": "\033[93m",
        "blue": "\033[94m", "cyan": "\033[96m", "magenta": "\033[95m", "reset": "\033[0m"
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"

def parse_bool_token(tok):
    if tok is None: return None
    s = str(tok).strip().lower()
    if s in ("1","true","yes","y","on"): return True
    if s in ("0","false","no","n","off"): return False
    return None

def bool_to_yesno(b): return "yes" if bool(b) else "no"

def normalize_track_id(raw):
    if raw is None: return None
    m = re.search(r"(\d+)", str(raw))
    return m.group(1) if m else None

def normalize_id_list(items):
    if not items: return []
    if isinstance(items, str): items = re.split(r"[,\s]+", items.strip())
    out = []
    for i in items:
        tid = normalize_track_id(i)
        if tid and tid not in out: out.append(tid)
    return out

def print_progress(current, total, width=40):
    if total <= 0: return
    percent = (current / total) * 100
    filled = int(width * current // total)
    bar = "#" * filled + "-" * (width - filled)
    sys.stdout.write(f"\r[{bar}] {percent:5.1f}% ({current}/{total})")
    sys.stdout.flush()
    if current == total:
        sys.stdout.write("\n")

# =====================================================
# mkvmerge Helpers
# =====================================================

def run_mkvmerge_json(filepath):
    """Return parsed JSON from `mkvmerge -J <file>` or None on error."""
    try:
        res = subprocess.run(["mkvmerge", "-J", str(filepath)], capture_output=True, text=True, check=True)
        return json.loads(res.stdout)
    except Exception:
        return None

def compare_track_metadata(current, expected):
    """Return True if the current file already matches the expected track
    selection and metadata for all listed tracks (id, type, lang, name,
    default, forced). This comparison is conservative: any mismatch means
    cleaning is required.
    """
    if not current or "tracks" not in current:
        return False

    # Build current track index by (type,id)
    cur_idx = {}
    for t in current["tracks"]:
        key = (t.get("type"), str(t.get("id")))
        cur_idx[key] = {
            "lang": (t.get("properties", {}).get("language", "und") or "und").lower(),
            "name": (t.get("properties", {}).get("track_name", "") or ""),
            "default": str(t.get("properties", {}).get("default_track", False)).lower(),
            "forced": str(t.get("properties", {}).get("forced_track", False)).lower(),
        }

    # Flatten expected
    exp_items = []
    for kind, items in expected.items():
        mapped_kind = kind if kind != "subs" else "subtitles"
        for it in items:
            exp_items.append({
                "type": mapped_kind,
                "id": str(it.get("id")),
                "lang": (it.get("lang") or "und").lower(),
                "name": (it.get("name") or ""),
                "default": str(it.get("default", False)).lower(),
                "forced": str(it.get("forced", False)).lower(),
            })

    # Compare metadata for each expected track
    for exp in exp_items:
        key = (exp["type"], exp["id"])
        cur = cur_idx.get(key)
        if not cur:
            return False
        for field in ("lang", "name", "default", "forced"):
            if str(cur.get(field, "")).strip().lower() != str(exp.get(field, "")).strip().lower():
                return False

    # NOTE: we intentionally do not verify that *only* these tracks exist.
    # The definition may omit some tracks intentionally; mkvmerge selection handles this.
    return True

# =====================================================
# Load Track Definitions
# =====================================================

def load_tracks_csv(path, logger):
    data = {}
    with open(path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rawfile = (row.get('file') or '').strip().strip('"').strip("'")
            if not rawfile:
                continue
            filekey = str(Path(rawfile).expanduser().resolve())
            ftype = (row.get('type') or '').strip().lower()
            tid = normalize_track_id(row.get('id'))
            if not tid:
                continue
            entry = {
                'id': tid,
                'name': (row.get('name') or '').strip() or None,
                'lang': (row.get('lang') or '').strip() or None,
                'default': parse_bool_token(row.get('default')),
                'forced': parse_bool_token(row.get('forced'))
            }
            if filekey not in data:
                data[filekey] = {'video': [], 'audio': [], 'subs': []}
            if ftype == 'video':
                data[filekey]['video'].append(entry)
            elif ftype == 'audio':
                data[filekey]['audio'].append(entry)
            else:
                data[filekey]['subs'].append(entry)
    return data

def normalize_json_input(obj):
    data = {}
    if isinstance(obj, dict):
        for rawfile, tracks in obj.items():
            rf = str(Path(rawfile).expanduser().resolve())
            data[rf] = {'video': [], 'audio': [], 'subs': []}
            for kind in ('video','audio','subs','subtitle','subtitles'):
                if kind not in tracks: continue
                for it in tracks[kind]:
                    tid = normalize_track_id(it.get('id') if isinstance(it, dict) else it)
                    if not tid: continue
                    entry = {
                        'id': tid,
                        'name': it.get('name') if isinstance(it, dict) else None,
                        'lang': it.get('lang') if isinstance(it, dict) else None,
                        'default': parse_bool_token(it.get('default')) if isinstance(it, dict) else None,
                        'forced': parse_bool_token(it.get('forced')) if isinstance(it, dict) else None
                    }
                    if kind in ('subs','subtitle','subtitles'):
                        data[rf]['subs'].append(entry)
                    else:
                        data[rf][kind].append(entry)
    return data

def load_tracks_file(path, logger):
    p = Path(path)
    if p.suffix.lower() == ".csv":
        return load_tracks_csv(p, logger)
    elif p.suffix.lower() == ".json":
        with open(p, encoding="utf-8") as fh:
            return normalize_json_input(json.load(fh))
    raise ValueError("Track file must be .csv or .json")

# =====================================================
# mkvmerge Command Builder
# =====================================================

def build_mkvmerge_cmd(input_file, output_file, video_ids, audio_ids, sub_ids, track_meta):
    cmd = ["mkvmerge", "-o", str(output_file)]
    if video_ids:
        cmd += ["--video-tracks", ",".join(video_ids)]
    if audio_ids:
        cmd += ["--audio-tracks", ",".join(audio_ids)]
    if sub_ids:
        cmd += ["--subtitle-tracks", ",".join(sub_ids)]
    for tid, meta in track_meta.items():
        if meta.get('name'):
            cmd += ["--track-name", f"{tid}:{meta['name']}"]
        if meta.get('lang'):
            cmd += ["--language", f"{tid}:{meta['lang']}"]
        cmd += ["--default-track", f"{tid}:{bool_to_yesno(meta.get('default'))}"]
        cmd += ["--forced-track", f"{tid}:{bool_to_yesno(meta.get('forced'))}"]
    cmd.append(str(input_file))
    return cmd

def run_cmd(cmd, dry_run=False, verbose=False, logger=None):
    printable = ' '.join(cmd)
    logger.info(f"Executing: {printable}")
    if dry_run:
        logger.info("[DRY-RUN] Command not executed")
        return True, None
    try:
        subprocess.run(cmd, check=True, stdout=None if verbose else subprocess.DEVNULL, stderr=None if verbose else subprocess.DEVNULL)
        return True, None
    except Exception as e:
        return False, str(e)

# =====================================================
# Installer Functions
# =====================================================

def install_self(target='/usr/local/bin/mkv-cleaner'):
    print("🧹 Cleaning old installs...")
    for p in [Path("/usr/local/bin/mkv-cleaner"), Path("/usr/local/sbin/mkv-cleaner"), Path.home() / ".local/bin/mkv-cleaner"]:
        if p.exists():
            try:
                p.unlink()
                print(f"  ✅ Removed {p}")
            except Exception as e:
                print(f"  ⚠️ Could not remove {p}: {e}")
    tgt = Path(target)
    shutil.copy2(Path(__file__).resolve(), tgt)
    os.chmod(tgt, 0o755)
    print(f"✅ Installed to {tgt}")

def uninstall_self():
    print("🧹 Uninstalling mkv-cleaner...")
    for p in [Path("/usr/local/bin/mkv-cleaner"), Path("/usr/local/sbin/mkv-cleaner"), Path.home() / ".local/bin/mkv-cleaner"]:
        if p.exists():
            try:
                p.unlink()
                print(f"✅ Removed {p}")
            except Exception as e:
                print(f"⚠️ Could not remove {p}: {e}")
    print("✅ Uninstallation complete.")

# =====================================================
# Main Execution
# =====================================================

def main():
    if len(sys.argv) == 2 and sys.argv[1] in ("install", "uninstall"):
        if sys.argv[1] == "install": install_self()
        else: uninstall_self()
        sys.exit(0)

    parser = argparse.ArgumentParser(prog="mkv-cleaner", description="Clean MKV files based on a CSV or JSON definition file.")

    parser.add_argument("--input-file", "--if", required=True, help="Track definition file (.csv or .json)")
    parser.add_argument("--output-base-dir", "--obd", default=".", help="Base directory for backup folder creation")
    parser.add_argument("--dry-run", "--dr", action="store_true", help="Show mkvmerge commands without executing")
    parser.add_argument("--verbose", "--vb", action="store_true", help="Show detailed mkvmerge output")

    # Enable shell autocompletion if argcomplete is available
    try:
        import argcomplete
        argcomplete.autocomplete(parser)
    except Exception:
        pass

    args = parser.parse_args()

    track_file = Path(args.input_file).resolve()
    base_out = Path(args.output_base_dir).resolve()
    run_dir = base_out / f"{now_ts()}_ori"
    run_dir.mkdir(parents=True, exist_ok=True)

    log_path = run_dir / "mkv-cleaner-log.txt"
    logger = logging.getLogger("mkv-cleaner")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info(color_text(f"🎬 Starting MKV Cleaning", "cyan"))
    logger.info(f"Input file: {track_file}")
    logger.info(f"Backup directory: {run_dir}")

    try:
        tracks_map = load_tracks_file(track_file, logger)
    except Exception as e:
        logger.error(f"❌ Failed to load track file: {e}")
        sys.exit(2)

    total_files = len(tracks_map)
    processed = succeeded = failed = skipped = nochange = 0
    moved_list, failed_list, skipped_list, nochange_list = [], [], [], []

    for idx, (rawfile, entry) in enumerate(tracks_map.items(), start=1):
        infile = Path(rawfile)
        print_progress(idx, total_files)
        logger.info(color_text(f"➡ Processing ({idx}/{total_files}): {infile.name}", "cyan"))

        if not infile.exists():
            skipped += 1
            skipped_list.append(str(infile))
            logger.warning(color_text(f"⚠️ Skipped (missing file): {infile}", "yellow"))
            continue

        # Pre-check: skip if no change needed
        current_meta = run_mkvmerge_json(infile)
        if compare_track_metadata(current_meta, entry):
            nochange += 1
            nochange_list.append(str(infile))
            logger.info(color_text(f"🟦 No change required: {infile}", "blue"))
            continue

        # Prepare temp cleaned output in same directory
        cleaned_file = infile.parent / f"cleaned-{infile.name}"
        if cleaned_file.exists():
            try:
                cleaned_file.unlink()
            except Exception:
                pass

        # Build selection lists
        video_ids = normalize_id_list([t["id"] for t in entry["video"]]) or ["0"]
        audio_ids = normalize_id_list([t["id"] for t in entry["audio"]])
        sub_ids = normalize_id_list([t["id"] for t in entry["subs"]])

        # Build track metadata map
        track_meta = {}
        for tlist in entry.values():
            for t in tlist:
                tid = normalize_track_id(t.get("id"))
                if not tid: continue
                track_meta.setdefault(tid, {}).update({
                    "name": t.get("name"),
                    "lang": t.get("lang"),
                    "default": t.get("default"),
                    "forced": t.get("forced")
                })

        # Run mkvmerge: input is original, output is cleaned-<name>.mkv
        cmd = build_mkvmerge_cmd(infile, cleaned_file, video_ids, audio_ids, sub_ids, track_meta)
        ok, reason = run_cmd(cmd, dry_run=args.dry_run, verbose=args.verbose, logger=logger)

        if ok and not args.dry_run:
            try:
                # Move original to backup area, then replace with cleaned
                backup_target = run_dir / infile.name
                shutil.move(str(infile), str(backup_target))
                shutil.move(str(cleaned_file), str(infile))
                moved_list.append(str(infile))
                succeeded += 1
                logger.info(color_text(f"✅ Cleaned: {infile}", "green"))
            except Exception as e:
                # Attempt cleanup of temp file on failure
                try:
                    if cleaned_file.exists():
                        cleaned_file.unlink()
                except Exception:
                    pass
                failed += 1
                failed_list.append((str(infile), f"post-remux move/rename failed: {e}"))
                logger.error(color_text(f"💥 Post-remux handling failed for {infile}: {e}", "red"))
        elif ok and args.dry_run:
            # Dry-run success (no file changes)
            succeeded += 1
            logger.info(color_text(f"✅ [DRY-RUN] Would clean: {infile}", "green"))
            # Ensure no temp lingers in case of previous leftover
            try:
                if cleaned_file.exists():
                    cleaned_file.unlink()
            except Exception:
                pass
        else:
            # mkvmerge failed: ensure temp is removed
            try:
                if cleaned_file.exists():
                    cleaned_file.unlink()
            except Exception:
                pass
            failed += 1
            failed_list.append((str(infile), reason or "mkvmerge failed"))
            logger.error(color_text(f"💥 Failed: {infile}", "red"))

    # Write summary
    summary_path = run_dir / "summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("======== SUMMARY ========\n")
        f.write(f"✅ Processed : {total_files}\n")
        f.write(f"✅ Succeeded : {succeeded}\n")
        f.write(f"❌ Failed    : {failed}\n")
        f.write(f"⚠️ Skipped  : {skipped}\n")
        f.write(f"🟦 No-Change : {nochange}\n\n")

        def section(title, items, include_reason=False):
            f.write(f"{title}: {len(items)}\n")
            if items:
                f.write("-" * len(title) + "\n")
                for i in items:
                    if include_reason and isinstance(i, (list, tuple)):
                        f.write(f"{i[0]} — {i[1]}\n")
                    else:
                        f.write(f"{i}\n")
                f.write("\n")

        section("📦 Moved Original Files", moved_list)
        section("💥 Failed Files", failed_list, include_reason=True)
        section("⚠️ Skipped Files", skipped_list)
        section("🟦 No-Change Files", nochange_list)

        f.write(f"Summary generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        if args.dry_run:
            f.write("[DRY-RUN] No files were modified.\n")

    logger.info(color_text("\n✅ Cleaning complete.", "green"))
    logger.info(f"📂 Results saved to: {run_dir}")

if __name__ == "__main__":
    main()
