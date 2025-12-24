# YAML Workflow Guide

Titan Tools commands are driven exclusively via YAML configurations. This guide
shows how to execute each workflow, customise the configs, and leverage the
shared loader utility.

## Running commands

- Repository shims (e.g. `./apps/vid-mkv-clean`) activate `.venv` automatically
  and run the workflow with the default config in `configs/config.yaml`.
- Passing a path as the first argument switches to a different YAML file that
  follows the same structure (logging + `tasks.<name>`).
- All scripts now use `common.shared.loader` internally to validate the input
  before executing.

```bash
./apps/scan-tracks                      # defaults to configs/config.yaml (task: vid_mkv_scan)
./apps/scan-tracks custom_config.yaml   # use a custom configuration
./apps/vid-hevc-convert                  # convert non-HEVC MKVs using mkv_scan_non_hevc exports
```

To inspect or lint a configuration without executing the workflow:

```bash
python -m common.shared.loader vid_rename configs/config.yaml --format json
```

## Configuration keys

| Command        | Required keys | Optional keys |
|----------------|----------------|----------------|
| `vid-mkv-clean` | *(none)*       | `definition`, `csv_part`, `roots`, `dry_run` |
| `scan-tracks`  | `roots`        | `dry_run`, `batch_size` |
| `vid-rename`    | `roots`        | `dry_run`, `no_meta`, `mapping`, `csv_part` |
| `file-scan`     | `roots`        | `base_name`, `batch_size` |
| `file-rename`   | `roots`        | `base_name`, `dry_run`, `csv_part` |
| `vid-hevc-convert` | `roots`     | `dry_run`, `preset`, `crf`, `csv_part` |

The `vid_rename` task automatically uses the most recent `mkv_scan_name_list_*.csv`
located under each root's reports directory. Provide a custom path via the CLI
(`--name-list`), keep using the legacy `mapping` key, or supply `csv_part`
values when you need to target specific batch exports.

Similarly, `vid_mkv_clean` will discover the latest `mkv_scan_tracks_*.csv`
beneath each root (inside `reports/`). Supplying `csv_part` or the legacy
`definition` setting lets you point at a specific CSV/JSON when desired.

The `vid_hevc_convert` task consumes the newest `mkv_scan_non_hevc_*.csv`
export to transcode listed MKVs to HEVC. Override the processed batches via
`csv_part`, and tweak encoder behaviour with `preset` / `crf` overrides.

All tools write logs and generated artifacts beneath each root's `output_root`
(default `./reports`). Explicit `output_dir` overrides are no longer required.

## Scan summary file

When you run `scan-tracks` a human-readable summary is produced alongside the
CSV exports. It is written to the same `output_root` and is named like:

```
mkv_scan_tracks_summary_<timestamp>.txt
```

The summary contains emoji-marked sections and ANSI colour escape codes so it
renders with colours when viewed in a terminal (for example: `cat` or `less`)
that supports ANSI sequences. Example sections include:

- `more_than_1_video` — files with more than one video track (filename -> count)
- `more_than_1_audio` — files with more than one audio track (filename -> count)
- `more_than_1_subtitle` — files with more than one subtitle track (filename -> count)
- `no_video`, `no_audio`, `no_subtitles`, `no_eng_subtitles` — problem lists

Example (plain text):

```
📋 MKV Scan Tracks Summary
Generated: mkv_scan_tracks_summary_2025-10-24_094915.txt

🎞️ more_than_1_video
rwrewt.mkv -> 2
fcc.mkv -> 3

🔊 more_than_1_audio
ngcvngf.mkv -> 3

📝 more_than_1_subtitle
gfgf.mkv -> 2

❌ no_video
abc.mkv
xyz.mkv

❌ no_audio
treg.mkv
heh.mkv

❗ no_subtitles
gvweg.mkv

⚠️ no_eng_subtitles
gregr.mkv
```

Tip: view the file in a terminal to see coloured output; the file contains ANSI
escape sequences that add emphasis to headers and problematic rows.

The file-level utilities mirror this behaviour: `file-scan` stores CSV exports
under `<root>/reports/<base_name>/`, and `file-rename` consumes the most recent
CSV from that location to apply renames. Override the consumed CSV via the
`csv_part` config key when you want to replay particular batch files.

### Batched scan exports

Scans can now emit multiple CSV files per run. Set `batch_size` on
`vid_mkv_scan` or `file_scan` (or rely on the shared default) to cap the number
of source files per CSV (omit or set to 0 to disable batching). Each batch
bundles every row associated with those files (directories plus per-track or
per-entry rows) and is suffixed with `_partNN`. Downstream tools
(`vid-mkv-clean`, `vid-rename`, `file-rename`) will automatically operate on the
newest export when no override is provided; specify `csv_part` in the
corresponding task config to target particular batch files (for example,
`csv_part: [1, 2, 3]`).

- Path values can be a string or a list. They are normalised to absolute paths.
- Boolean flags (`dry_run`, `no_meta`) accept YAML truthy/falsy values.
- Missing required fields raise a `ValueError` before any files are touched.

### Example config (excerpt)

`configs/config.yaml`

```yaml
logging:
  level: INFO
  use_rich: auto

shared:
  batch_size: 250
  # csv_part: [1, 2, 3]

tasks:
  vid_mkv_clean:
    roots:
      - "./media/input"
    dry_run: true
    # csv_part: [1]

  vid_rename:
    roots:
      - "./media/input"
    dry_run: false
    no_meta: false
    # csv_part: [1, 2]

  file_scan:
    roots:
      - "./media/input"
    base_name: "file_scan"

  file_rename:
    roots:
      - "./media/input"
    base_name: "file_scan"
    dry_run: false

  vid_mkv_scan:
    roots:
      - "./media/input"

  vid_hevc_convert:
    roots:
      - "./media/input"
    preset: "slow"
    crf: 23
```

## Customising workflows

1. Copy the template YAML from `configs/` to a new location.
2. Edit paths, flags, or optional settings.
3. Execute the corresponding script with the new file as the argument.

If anything is misconfigured, the shared loader explains the problem and exits
with a non-zero status.
