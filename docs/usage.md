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
./apps/vid-mkv-scan                      # defaults to configs/config.yaml (task: vid_mkv_scan)
./apps/vid-mkv-scan custom_config.yaml   # use a custom configuration
```

To inspect or lint a configuration without executing the workflow:

```bash
python -m common.shared.loader vid_rename configs/config.yaml --format json
```

## Configuration keys

| Command        | Required keys            | Optional keys                     |
|----------------|--------------------------|-----------------------------------|
| `vid-mkv-clean` | *(none)*                 | `definition`, `roots`, `dry_run`            |
| `vid-mkv-scan`  | `roots`                  | `dry_run`                         |
| `vid-rename`    | `roots`                  | `dry_run`, `no_meta`              |
| `file-scan`     | `roots`                  | `base_name`                       |
| `file-rename`   | `roots`                  | `base_name`, `dry_run`            |

The `vid_rename` task automatically uses the most recent `mkv_scan_name_list_*.csv`
located under each root's reports directory. Provide a custom path via the CLI
(`--name-list`) or legacy `mapping` config key only when you need to override
this discovery.

Similarly, `vid_mkv_clean` will discover the latest `mkv_scan_tracks_*.csv`
beneath each root (inside `reports/`). Supplying the optional `definition`
setting lets you point at a specific CSV/JSON when desired.

All tools write logs and generated artifacts beneath each root's `output_root`
(default `./reports`). Explicit `output_dir` overrides are no longer required.

The file-level utilities mirror this behaviour: `file-scan` stores CSV exports
under `<root>/reports/<base_name>/`, and `file-rename` consumes the most recent
CSV from that location to apply renames.

- Path values can be a string or a list. They are normalised to absolute paths.
- Boolean flags (`dry_run`, `no_meta`) accept YAML truthy/falsy values.
- Missing required fields raise a `ValueError` before any files are touched.

### Example config (excerpt)

`configs/config.yaml`

```yaml
logging:
  level: INFO
  use_rich: auto

tasks:
  vid_mkv_clean:
    roots:
      - "./media/input"
    dry_run: true

  vid_rename:
    roots:
      - "./media/input"
    dry_run: false
    no_meta: false

  file_scan:
    roots:
      - "./media/input"
    base_name: "file_scan"

  file_rename:
    roots:
      - "./media/input"
    base_name: "file_scan"
    dry_run: false
```

## Customising workflows

1. Copy the template YAML from `configs/` to a new location.
2. Edit paths, flags, or optional settings.
3. Execute the corresponding script with the new file as the argument.

If anything is misconfigured, the shared loader explains the problem and exits
with a non-zero status.
