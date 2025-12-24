# Titan Tools

Shared automation toolkit for cleaning, scanning, renaming, and reporting on media
libraries. Reusable logic lives under `common/` and `video/`, while the
`apps/` directory contains thin wrappers that read YAML configs and execute the
workflows.

## Repository layout

- `apps/vid-*` – executable shims that activate `.venv`, load a YAML config, and
  run the associated workflow module.
- `apps/cli.py` – Python entry points exposed as `console_scripts` for global use.
- `common/base/` – foundational helpers (logging, filesystem, caching, ops).
- `common/shared/` – higher-level utilities (YAML loader, reporting, misc helpers).
- `configs/` – example YAML configuration files; edit these to tweak behaviour.
- `scripts/setup_env.sh` – helper to create a project-local virtualenv and
  install the package in editable mode.
- `video/` – domain-specific media processing modules reused by the runners.
- `tests/` – pytest coverage for the YAML-driven execution flow.

## Quick start

Bootstrap the virtual environment and install dependencies:

```bash
./scripts/setup_env.sh
```

Afterwards, run any tool directly; each command reads `configs/config.yaml` by
default (selecting the relevant `tasks.<name>` section). Provide an alternate
config path as the first argument to override:

```bash
./apps/vid-mkv-clean                # uses configs/config.yaml (task: vid_mkv_clean)
./apps/vid-mkv-extract-subs         # extract subtitle tracks from mkv_scan reports
./apps/vid-srt-clean                # remove non-target language blocks from SRT files
./apps/scan-tracks custom_config.yaml
./apps/vid-hevc-convert             # transcode non-HEVC MKVs via mkv_scan_non_hevc reports
```

If you install the project (`pip install -e .`), the commands `scan-tracks`,
`vid-mkv-clean`, and `vid-rename` will be available globally and benefit from
shell tab-completion (via `argcomplete`). Enable completions by running
`eval "$(register-python-argcomplete scan-tracks)"` (repeat for the remaining
commands or use the global activator).

To validate or inspect a configuration without running a workflow, use the
shared loader module:

```bash
python -m common.shared.loader vid_mkv_clean configs/config.yaml --format json
```

## Config-driven execution

Command-line flags are no longer required. Instead, adjust the YAML file for
the task. Example (`configs/config.yaml`):

```yaml
logging:
  level: INFO
  use_rich: auto

tasks:
  vid_mkv_clean:

    # One or more directories containing the MKV files that should be processed.
    roots:
      - "./media/input"

    # When true, perform a dry run without modifying any files.
    dry_run: true
```

Each tool follows the same pattern—edit the relevant YAML, then rerun the
script. Missing required keys raise a friendly error before any work begins.
All outputs are written under each root's `task_defaults.output_root`
(default `./reports`), so per-task `output_dir` entries are optional.

## Available commands

- `vid-mkv-clean` – remux MKVs based on the latest mkv_scan track export (or an optional CSV override).
- `vid-mkv-extract-subs` – extract subtitle tracks referenced in mkv_scan track reports.
- `vid-srt-clean` – scan SRT files and strip subtitle blocks that do not match the configured languages.
- `scan-tracks` – scan MKVs, export track metadata, name lists, and non-HEVC reports.
- `vid-rename` – apply edits from the latest mkv_scan name list (rename + metadata).
- `vid-hevc-convert` – convert MKVs listed in the latest mkv_scan_non_hevc report to HEVC (libx265).
- `file-scan` – produce a CSV inventory of files/directories for manual editing.
- `file-rename` – apply pending renames from the latest file_scan CSV.

## Testing

Activate `.venv` and run the pytest suite:

```bash
pytest
```

## Packaging

The project is compliant with PEP 621 metadata in `pyproject.toml`. Editable
installs expose the console scripts globally while the repository shims mirror
the same entry points for local development.
