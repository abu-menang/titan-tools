"""
Shared configuration loading and validation helpers.

Provides:
 - `load_config`: basic YAML loader
 - `load_yaml_resource`: load arbitrary YAML files from configs/
 - `load_media_types`: cached helper for media extension metadata
 - `load_task_config`: validated configuration for a given task
 - `cli_main`: command-line entry point exposed as the `titan-config` script
"""

from __future__ import annotations

import argparse
import base64
import json
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from common.shared.report import ColumnSpec

from common.base.file_io import read_yaml


ConfigDict = Dict[str, Any]

DEFAULT_CONFIG_FILENAME = "config.yaml"
LOGGING_SECTION_KEY = "logging"
TASKS_SECTION_KEY = "tasks"
TASK_DEFAULTS_KEY = "task_defaults"
SHARED_SECTION_KEY = "shared"
CONFIGS_DIR = Path(__file__).resolve().parents[2] / "configs"


TASK_SCHEMAS: Dict[str, Dict[str, Iterable[str]]] = {
    "vid_cleaner": {
        "required": [],
        "optional": ["roots", "output_dir", "dry_run", "clean_dir_key"],
    },
    "vid_mkv_clean": {
        "required": [],
        "optional": ["definition", "roots", "output_dir", "dry_run", "csv_part", "tracks_csv_types"],
    },
    "vid_mkv_merge_ext_subs": {
        "required": [],
        "optional": ["definition", "roots", "output_dir", "dry_run", "csv_part", "sources"],
    },
    "vid_mkv_extract_subs": {
        "required": [],
        "optional": [
            "definition",
            "roots",
            "output_dir",
            "dry_run",
            "csv_part",
            "tracks_csv_types",
            "mkvextract_bin",
            "overwrite",
        ],
    },
    "vid_srt_clean": {
        "required": ["roots", "languages"],
        "optional": ["output_dir", "dry_run", "overwrite", "file_suffix", "min_text_chars"],
    },
    "vid_mkv_scan": {
        "required": ["roots"],
        "optional": ["output_dir", "dry_run", "batch_size", "lang_vid", "lang_aud", "lang_sub"],
    },
    "vid_mkv_scan_v2": {
        "required": ["roots"],
        "optional": ["output_dir", "dry_run", "batch_size", "lang_vid", "lang_aud", "lang_sub"],
    },
    "vid_scan_hevc": {
        "required": ["roots"],
        "optional": ["output_dir", "dry_run", "batch_size", "lang_vid", "lang_aud", "lang_sub"],
    },
    "vid_rename": {
        "required": ["roots"],
        "optional": ["output_dir", "dry_run", "no_meta", "mapping", "csv_part"],
    },
    "vid_conv_cleaner": {
        "required": [],
        "optional": ["roots", "output_dir", "dry_run"],
    },
    "vid_tagger": {
        "required": ["csv_dir"],
        "optional": ["tags", "dry_run", "roots"],
    },
    "file_scan": {
        "required": ["roots"],
        "optional": ["output_dir", "base_name", "batch_size"],
    },
    "file_rename": {
        "required": ["roots"],
        "optional": ["output_dir", "base_name", "dry_run", "csv_part"],
    },
    "vid_hevc_convert": {
        "required": ["roots"],
        "optional": ["output_dir", "dry_run", "preset", "crf", "csv_part"],
    },
}

FIELD_ALIASES = {
    "root": "roots",
    "output": "output_dir",
}

SINGLE_PATH_FIELDS = {"definition", "output_dir", "mapping"}
SINGLE_PATH_FIELDS = {"definition", "output_dir", "mapping", "csv_dir"}
MULTI_PATH_FIELDS = {"roots"}
BOOLEAN_FIELDS = {"dry_run", "no_meta", "overwrite"}
INTEGER_FIELDS = {"batch_size", "crf", "min_text_chars"}
INTEGER_LIST_FIELDS = {"csv_part"}
YES_NO_FIELDS = set()
LOGGING_ALLOWED_KEYS = {"level", "use_rich", "log_dir", "file_prefix"}
TASK_DEFAULT_ALLOWED_KEYS = {"roots", "output_root", "tracks_root", "hevc_root"}
SHARED_ALLOWED_KEYS = {"batch_size", "csv_part"}

YES_VALUES = {"1", "true", "yes", "y", "on"}
NO_VALUES = {"0", "false", "no", "n", "off"}


def load_config(path: str | Path | None) -> Mapping[str, Any] | Dict[str, Any]:
    if not path:
        return {}

    cfg_path = Path(path).expanduser()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {cfg_path}")

    data = read_yaml(cfg_path)
    if data is None:
        return {}
    if not isinstance(data, Mapping):
        raise ValueError(f"Configuration root must be a mapping in {cfg_path}")

    return data or {}


def load_yaml_resource(
    name: str | Path,
    *,
    config_dir: str | Path | None = None,
) -> Any:
    """
    Load an arbitrary YAML file located under the repository configs directory
    (or a caller-provided directory).
    """
    base = Path(config_dir).expanduser() if config_dir else CONFIGS_DIR
    candidate = Path(name)
    if not candidate.is_absolute():
        candidate = base / candidate
    if not candidate.suffix:
        candidate = candidate.with_suffix(".yaml")
    if not candidate.exists():
        raise FileNotFoundError(f"YAML resource not found: {candidate}")
    return read_yaml(candidate)


def load_output_dirs(config_dir: str | Path | None = None) -> Dict[str, Any]:
    """
    Load the output_dirs YAML mapping. Returns an empty dict on error.
    """
    try:
        data = load_yaml_resource("output_dirs", config_dir=config_dir)
        if data is None:
            return {}
        if not isinstance(data, Mapping):
            raise ValueError("output_dirs YAML must contain a mapping at the root.")
        return dict(data)
    except Exception:
        return {}


@dataclass(frozen=True)
class MediaTypes:
    video_exts: frozenset[str]
    audio_exts: frozenset[str]
    image_exts: frozenset[str]
    doc_exts: frozenset[str]
    subtitle_exts: frozenset[str]

    @property
    def all_known_exts(self) -> frozenset[str]:
        return frozenset(
            self.video_exts
            | self.audio_exts
            | self.image_exts
            | self.doc_exts
            | self.subtitle_exts
        )


def _normalize_exts(values: Iterable[str]) -> frozenset[str]:
    return frozenset(v.strip().lower() for v in values if v)


def _coerce_yes_no(value: object, key: str, config_path: Path) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    if text in YES_VALUES:
        return True
    if text in NO_VALUES:
        return False
    raise ValueError(
        f"Configuration '{config_path}' field '{key}' must be 'y' or 'n' (case-insensitive)."
    )


@lru_cache(maxsize=1)
def load_media_types(config_path: str | Path | None = None) -> MediaTypes:
    """Load media extension metadata from YAML (defaults to configs/media_types.yaml)."""
    if config_path is not None:
        resource_path = Path(config_path).expanduser()
        if not resource_path.suffix:
            resource_path = resource_path.with_suffix(".yaml")
        if not resource_path.exists():
            raise FileNotFoundError(f"Media types file not found: {resource_path}")
        data = read_yaml(resource_path)
    else:
        data = load_yaml_resource("media_types")

    if not isinstance(data, Mapping):
        raise ValueError("Media types YAML root must be a mapping.")

    def _get_list(key: str) -> Sequence[str]:
        value = data.get(key, [])
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return value
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        raise ValueError(f"Media types key '{key}' must be a list or comma-separated string.")

    return MediaTypes(
        video_exts=_normalize_exts(_get_list("video_exts")),
        audio_exts=_normalize_exts(_get_list("audio_exts")),
        image_exts=_normalize_exts(_get_list("image_exts")),
        doc_exts=_normalize_exts(_get_list("doc_exts")),
        subtitle_exts=_normalize_exts(_get_list("subtitle_exts")),
    )


@dataclass(frozen=True)
class ScanConfig:
    media_types: MediaTypes
    columns: Dict[str, List["ColumnSpec"]]
    report_dir_map: Mapping[str, str]
    base_dir_map: Dict[str, str]


def load_scan_config(log=None) -> ScanConfig:
    """
    Centralized loader for video scan configuration pieces:
      - media types/extensions
      - mkv_scan_columns (column specs)
      - output_dirs (report directory mapping)

    Raises SystemExit(1) if required pieces cannot be loaded/validated.
    """
    try:
        media_types = load_media_types()
    except Exception as exc:
        if log:
            log.error("Failed to load media_types.yaml: %s", exc)
        raise SystemExit(1)

    try:
        from common.utils.column_utils import load_column_specs  # local import to avoid cycles

        columns = load_column_specs("mkv_scan_columns")
    except Exception as exc:
        if log:
            log.error("Failed to load mkv_scan_columns.yaml: %s", exc)
        raise SystemExit(1)

    try:
        output_dirs = load_yaml_resource("output_dirs")
    except Exception as exc:
        if log:
            log.error("Failed to load output_dirs.yaml: %s", exc)
        raise SystemExit(1)

    if not isinstance(output_dirs, Mapping):
        if log:
            log.error("output_dirs.yaml root must be a mapping")
        raise SystemExit(1)

    report_dir_map = output_dirs.get("reports", {})
    if not isinstance(report_dir_map, Mapping):
        if log:
            log.error("output_dirs.yaml 'reports' must be a mapping")
        raise SystemExit(1)

    base_dir_map = {k: v for k, v in output_dirs.items() if k != "reports"}
    return ScanConfig(
        media_types=media_types,
        columns=columns,
        report_dir_map=report_dir_map,
        base_dir_map=base_dir_map,
    )


def load_logging_config(config_path: str | Path | None = None) -> Dict[str, Any]:
    root = load_config(config_path)
    return _extract_logging_settings(root)


def load_task_config(task: str, config_path: str | Path | None = None) -> ConfigDict:
    if task not in TASK_SCHEMAS:
        raise ValueError(f"Unknown task '{task}'. Expected one of: {', '.join(sorted(TASK_SCHEMAS))}")

    resolved_path = _resolve_config_path(task, config_path)
    root_config = dict(load_config(resolved_path))
    task_config_raw = _extract_task_config(root_config, task, resolved_path)
    task_defaults = _extract_task_defaults(root_config, resolved_path)
    task_logging_override: Dict[str, Any] = {}
    if "logging" in task_config_raw:
        logging_payload = task_config_raw.pop("logging")
        if not isinstance(logging_payload, Mapping):
            raise ValueError(
                f"Task '{task}' logging section must be a mapping in {resolved_path}"
            )
        task_logging_override = dict(logging_payload)
        invalid_logging_keys = [
            key for key in task_logging_override if key not in LOGGING_ALLOWED_KEYS
        ]
        if invalid_logging_keys:
            invalid_keys = ", ".join(sorted(invalid_logging_keys))
            raise ValueError(
                f"Task '{task}' logging section contains unsupported keys in {resolved_path}: {invalid_keys}"
            )
    default_roots = task_defaults.get("roots")
    if default_roots is not None and "roots" not in task_config_raw:
        task_config_raw["roots"] = deepcopy(default_roots)
    config = _apply_aliases(task_config_raw)

    schema = TASK_SCHEMAS[task]
    required = set(schema.get("required", []))
    optional = set(schema.get("optional", []))
    allowed_keys = required | optional

    missing = [key for key in required if not config.get(key)]
    if missing:
        raise ValueError(
            f"Configuration '{resolved_path}' missing required fields for task '{task}': {', '.join(missing)}"
        )

    unexpected = [key for key in config if key not in allowed_keys]
    if unexpected:
        raise ValueError(
            f"Configuration '{resolved_path}' contains unsupported keys for task '{task}': {', '.join(unexpected)}"
        )

    provided_keys = set(config.keys())

    normalized: ConfigDict = {}
    for key in allowed_keys:
        if key not in config:
            continue
        value = config[key]

        if key in SINGLE_PATH_FIELDS:
            normalized[key] = _normalize_single_path(value)
        elif key in MULTI_PATH_FIELDS:
            normalized[key] = _normalize_multi_path(value)
        elif key in BOOLEAN_FIELDS:
            normalized[key] = bool(value)
        elif key in INTEGER_FIELDS:
            if value is None or value == "":
                normalized[key] = None
            else:
                normalized[key] = _coerce_int(value, key, resolved_path)
        elif key in INTEGER_LIST_FIELDS:
            normalized[key] = _normalize_int_list(value, key, resolved_path)
        elif key in YES_NO_FIELDS:
            normalized[key] = _coerce_yes_no(value, key, resolved_path)
        else:
            normalized[key] = value

    shared_settings = _extract_shared_settings(root_config, resolved_path)

    if "batch_size" in allowed_keys and "batch_size" not in provided_keys:
        shared_batch = shared_settings.get("batch_size")
        if shared_batch is not None:
            normalized["batch_size"] = shared_batch

    if "csv_part" in allowed_keys and "csv_part" not in provided_keys:
        shared_parts = shared_settings.get("csv_part")
        if shared_parts is not None:
            normalized["csv_part"] = list(shared_parts)

    normalized["__task__"] = task
    normalized["__config_path__"] = str(resolved_path)
    primary_root = _determine_primary_root(normalized, task_defaults)
    output_dir_resolved = _normalize_output_dir(
        normalized.get("output_dir"),
        task_defaults.get("output_root"),
        primary_root,
        task,
        resolved_path,
    )
    if output_dir_resolved is not None:
        normalized["output_dir"] = output_dir_resolved
    else:
        normalized.pop("output_dir", None)
    if task_defaults.get("output_root"):
        normalized["__output_root__"] = task_defaults["output_root"]
    for custom_root_key in ("tracks_root", "hevc_root"):
        if task_defaults.get(custom_root_key):
            normalized[custom_root_key] = task_defaults[custom_root_key]
    if "__output_root__" not in normalized and task_defaults.get("hevc_root"):
        normalized["__output_root__"] = task_defaults["hevc_root"]
    logging_settings = _extract_logging_settings(root_config)
    merged_logging = dict(logging_settings) if logging_settings else {}
    if task_logging_override:
        merged_logging.update(task_logging_override)
    log_root = (
        normalized.get("output_dir")
        or normalized.get("__output_root__")
        or normalized.get("tracks_root")
        or task_defaults.get("tracks_root")
        or task_defaults.get("output_root")
    )
    merged_logging = _apply_logging_defaults(
        merged_logging,
        primary_root,
        normalized.get("output_dir"),
        log_root,
    )
    if merged_logging:
        normalized["__logging__"] = merged_logging
    return normalized


def _apply_aliases(config: Mapping[str, Any]) -> ConfigDict:
    result: ConfigDict = {}
    for key, value in config.items():
        canonical = FIELD_ALIASES.get(key, key)
        result[canonical] = value
    return result


def _normalize_single_path(value: Any) -> str:
    if value is None:
        raise ValueError("Expected a path value, received None")
    return str(Path(value).expanduser())


def _normalize_multi_path(value: Any) -> list[str]:
    if value is None:
        raise ValueError("Expected a list of paths, received None")
    if isinstance(value, (list, tuple, set)):
        values = value
    else:
        values = [value]
    if not values:
        raise ValueError("Expected at least one path entry")
    return [str(Path(item).expanduser()) for item in values]


def _coerce_int(value: Any, field: str, config_path: Path) -> int:
    if isinstance(value, bool):
        raise ValueError(
            f"Configuration '{config_path}' field '{field}' must be an integer."
        )
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Configuration '{config_path}' field '{field}' must be an integer."
        ) from exc


def _normalize_int_list(value: Any, field: str, config_path: Path) -> List[int]:
    if value is None:
        return []

    items: List[Any] = []
    if isinstance(value, str):
        tokens = [token.strip() for token in value.split(",") if token.strip()]
        items.extend(tokens)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            if isinstance(item, str) and "," in item:
                tokens = [token.strip() for token in item.split(",") if token.strip()]
                items.extend(tokens)
            else:
                items.append(item)
    else:
        items.append(value)

    return [_coerce_int(item, field, config_path) for item in items]


def _resolve_config_path(task: str, config_path: str | Path | None) -> Path:
    if config_path:
        return Path(config_path).expanduser()

    candidate = CONFIGS_DIR / DEFAULT_CONFIG_FILENAME
    if not candidate.exists():
        raise FileNotFoundError(
            f"No configuration path provided and default file not found: {candidate}"
        )
    return candidate


def _extract_task_config(root: Mapping[str, Any], task: str, config_path: Path) -> ConfigDict:
    if TASKS_SECTION_KEY in root:
        tasks_section = root.get(TASKS_SECTION_KEY) or {}
        if not isinstance(tasks_section, Mapping):
            raise ValueError(f"'tasks' section must be a mapping in {config_path}")
        if task not in tasks_section:
            raise ValueError(
                f"Configuration '{config_path}' missing task '{task}' under 'tasks' section"
            )
        task_payload = tasks_section[task]
        if not isinstance(task_payload, Mapping):
            raise ValueError(f"Task '{task}' entry must be a mapping in {config_path}")
        return dict(task_payload)

    # Fallback to legacy single-task files.
    return dict(root)


def _extract_logging_settings(root: Mapping[str, Any]) -> Dict[str, Any]:
    section = root.get(LOGGING_SECTION_KEY, {})
    return dict(section) if isinstance(section, Mapping) else {}


def _extract_task_defaults(root: Mapping[str, Any], config_path: Path) -> Dict[str, Any]:
    section = root.get(TASK_DEFAULTS_KEY, {})
    if not section:
        return {}
    if not isinstance(section, Mapping):
        raise ValueError(f"'{TASK_DEFAULTS_KEY}' section must be a mapping in {config_path}")

    invalid = [key for key in section if key not in TASK_DEFAULT_ALLOWED_KEYS]
    if invalid:
        invalid_keys = ", ".join(sorted(invalid))
        raise ValueError(
            f"'{TASK_DEFAULTS_KEY}' contains unsupported keys in {config_path}: {invalid_keys}"
        )

    defaults: Dict[str, Any] = {}
    if "roots" in section:
        normalized_roots = _normalize_multi_path(section["roots"])
        defaults["roots"] = normalized_roots
        if normalized_roots:
            defaults["primary_root"] = str(Path(normalized_roots[0]).expanduser().resolve())

    def _resolve_default_path(value: Any, key: str) -> str:
        if value is None:
            raise ValueError(f"'{key}' in {TASK_DEFAULTS_KEY} cannot be null in {config_path}")
        candidate = Path(str(value)).expanduser()
        if candidate.is_absolute():
            return str(candidate.resolve())
        base_root = defaults.get("primary_root")
        if not base_root:
            raise ValueError(
                f"'{key}' in {TASK_DEFAULTS_KEY} is relative but no root path is available to anchor it"
            )
        return str((Path(base_root) / candidate).resolve())

    if "output_root" in section:
        defaults["output_root"] = _resolve_default_path(section["output_root"], "output_root")
    for key in ("tracks_root", "hevc_root"):
        if key in section:
            defaults[key] = _resolve_default_path(section[key], key)
    return defaults


def _extract_shared_settings(root: Mapping[str, Any], config_path: Path) -> Dict[str, Any]:
    section = root.get(SHARED_SECTION_KEY, {})
    if not section:
        return {"csv_part": [0]}
    if not isinstance(section, Mapping):
        raise ValueError(f"'{SHARED_SECTION_KEY}' section must be a mapping in {config_path}")

    invalid = [key for key in section if key not in SHARED_ALLOWED_KEYS]
    if invalid:
        invalid_keys = ", ".join(sorted(invalid))
        raise ValueError(
            f"'{SHARED_SECTION_KEY}' contains unsupported keys in {config_path}: {invalid_keys}"
        )

    settings: Dict[str, Any] = {}

    if "batch_size" in section:
        batch_value = section.get("batch_size")
        if batch_value is not None and batch_value != "":
            settings["batch_size"] = _coerce_int(batch_value, "batch_size", config_path)

    if "csv_part" in section:
        csv_value = section.get("csv_part")
        normalized_parts = _normalize_int_list(csv_value, "csv_part", config_path) if csv_value is not None else []
        settings["csv_part"] = normalized_parts or [0]
    else:
        settings.setdefault("csv_part", [0])

    return settings


def _determine_primary_root(
    normalized: Mapping[str, Any],
    defaults: Mapping[str, Any],
) -> Optional[str]:
    roots = normalized.get("roots") or defaults.get("roots") or []
    if isinstance(roots, (list, tuple)):
        candidate = roots[0] if roots else None
    else:
        candidate = roots
    if not candidate:
        candidate = defaults.get("primary_root")
    if not candidate:
        return None
    return str(Path(str(candidate)).expanduser())


def _normalize_output_dir(
    value: Any,
    output_root: Optional[str],
    primary_root: Optional[str],
    task: str,
    config_path: Path,
) -> Optional[str]:
    base_root = Path(primary_root).expanduser() if primary_root else None
    resolved_output_root: Optional[Path] = None
    if output_root:
        candidate_root = Path(str(output_root)).expanduser()
        if not candidate_root.is_absolute():
            if not base_root:
                raise ValueError(
                    f"'output_root' in {config_path} is relative but no root path is available to anchor it"
                )
            candidate_root = base_root / candidate_root
        resolved_output_root = candidate_root.resolve()

    raw_value_path: Optional[Path] = None
    if value is not None:
        raw_value_path = Path(str(value)).expanduser()
        if raw_value_path.is_absolute():
            return str(raw_value_path.resolve())

    if resolved_output_root:
        if raw_value_path:
            trimmed = Path(*[part for part in raw_value_path.parts if part not in {"", "."}])
            return str((resolved_output_root / trimmed).resolve())
        return str(resolved_output_root)

    if raw_value_path is None:
        return None

    return str(raw_value_path.resolve())


def _apply_logging_defaults(
    logging_cfg: Dict[str, Any],
    primary_root: Optional[str],
    output_dir: Optional[str],
    output_root: Optional[str],
) -> Dict[str, Any]:
    if not logging_cfg and not (primary_root or output_dir or output_root):
        return {}

    cfg = dict(logging_cfg)
    resolved_output_dir = Path(output_dir).expanduser().resolve() if output_dir else None
    resolved_output_root = (
        Path(output_root).expanduser().resolve()
        if output_root
        else resolved_output_dir
    )
    base_root = (
        resolved_output_root
        or (Path(primary_root).expanduser().resolve() if primary_root else None)
    )
    log_dir_value = cfg.get("log_dir")

    if log_dir_value:
        path = Path(str(log_dir_value)).expanduser()
        if path.is_absolute():
            cfg["log_dir"] = str(path.resolve())
        elif base_root:
            cfg["log_dir"] = str((base_root / path).resolve())
        else:
            cfg["log_dir"] = str(path.resolve())
    elif base_root:
        cfg["log_dir"] = str((base_root / "logs").resolve())

    return cfg


def cli_main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Load and validate Titan Tools YAML configs.")
    parser.add_argument("task", help=f"Task identifier ({', '.join(sorted(TASK_SCHEMAS))})")
    parser.add_argument("config_path", nargs="?", help="Path to YAML file (defaults to configs/config.yaml)")
    parser.add_argument(
        "--format",
        choices={"b64", "json"},
        default="b64",
        help="Output format: base64-encoded JSON (default) or raw JSON.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    config = load_task_config(args.task, args.config_path)
    payload = json.dumps(config)

    if args.format == "json":
        print(payload)
    else:
        encoded = base64.b64encode(payload.encode("utf-8")).decode("utf-8")
        print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(cli_main())
