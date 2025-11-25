"""
video.mkv_extract_subtitles

Utility helpers to extract subtitle tracks reported by mkv_scan
into standalone subtitle files (SRT/ASS/SUP depending on codec).

Typical workflow:
 - Run vid_mkv_scan to generate mkv_scan_tracks_ok/issues CSV
 - Feed the CSV into vid_mkv_extract_subs to dump subtitle streams
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from common.base.fs import ensure_parent
from common.base.logging import get_logger
from common.base.ops import run_command
from common.shared.report import load_tabular_rows

log = get_logger(__name__)


@dataclass(frozen=True)
class SubtitleTrack:
    source: Path
    track_id: str
    lang: str
    codec: str
    name: str
    edited_name: str


_FALLBACK_EXT = ".srt"
_CODEC_EXTENSION_MAP: Dict[str, str] = {
    "S_TEXT/UTF8": ".srt",
    "S_TEXT/UTF-8": ".srt",
    "S_TEXT/ASS": ".ass",
    "S_TEXT/SSA": ".ass",
    "S_TEXT/USF": ".usf",
    "S_HDMV/PGS": ".sup",
    "S_VOBSUB": ".sub",
}


def _sanitize_component(text: str) -> str:
    """Return a filesystem-safe ASCII component."""
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.replace(" ", "_")
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", ascii_text)
    return cleaned.strip("_").lower()


def _deduce_extension(codec: str) -> str:
    if not codec:
        return _FALLBACK_EXT
    normalized = codec.strip().upper()
    return _CODEC_EXTENSION_MAP.get(normalized, _FALLBACK_EXT)


def _construct_output_path(
    mkv_path: Path,
    track: SubtitleTrack,
    output_dir: Optional[Path],
) -> Path:
    base_dir = Path(output_dir).expanduser() if output_dir else mkv_path.parent

    stem_parts: List[str] = [mkv_path.stem, f"track{track.track_id}"]

    lang = (track.lang or "").strip()
    if lang:
        stem_parts.append(_sanitize_component(lang))

    editable = track.edited_name or track.name
    if editable:
        sanitized = _sanitize_component(editable)
        if sanitized:
            stem_parts.append(sanitized)

    stem = ".".join(part for part in stem_parts if part)
    extension = _deduce_extension(track.codec)
    return (base_dir / stem).with_suffix(extension)


def _load_subtitle_tracks(csv_path: Path | str) -> List[SubtitleTrack]:
    rows, _ = load_tabular_rows(Path(csv_path))
    tracks: List[SubtitleTrack] = []
    for row in rows:
        track_type = (row.get("type") or "").strip().lower()
        if track_type != "subtitles":
            continue
        raw_id = (row.get("id") or "").strip()
        if not raw_id:
            log.debug(f"Skipping subtitle row without track id: {row}")
            continue

        file_token = (row.get("path") or row.get("file") or "").strip()
        if not file_token:
            log.debug(f"Skipping subtitle row without source path: {row}")
            continue

        source = Path(file_token).expanduser()
        track = SubtitleTrack(
            source=source,
            track_id=raw_id,
            lang=(row.get("lang") or "").strip(),
            codec=(row.get("codec") or "").strip(),
            name=(row.get("name") or "").strip(),
            edited_name=(row.get("edited_name") or "").strip(),
        )
        tracks.append(track)

    return tracks


def extract_subtitles_for_track(
    track: SubtitleTrack,
    output_dir: Optional[Path | str] = None,
    mkvextract_bin: str = "mkvextract",
    overwrite: bool = False,
    dry_run: bool = False,
) -> Tuple[str, Path]:
    """
    Extract a single subtitle track via mkvextract.

    Returns (status, output_path) where status ∈ {"extracted", "skipped", "failed"}.
    """
    mkv_path = track.source.expanduser().resolve()
    if not mkv_path.exists():
        log.warning(f"Subtitle source missing → {mkv_path}")
        return "failed", mkv_path

    output_dir_path = Path(output_dir).expanduser() if output_dir else None
    output_path = _construct_output_path(mkv_path, track, output_dir_path)

    if output_path.exists() and not overwrite:
        log.info(f"Skipping existing subtitle: {output_path}")
        return "skipped", output_path

    command: Sequence[str] = [
        mkvextract_bin,
        "tracks",
        str(mkv_path),
        f"{track.track_id}:{output_path}",
    ]

    if dry_run:
        log.info(f"[DRY-RUN] Would run: {' '.join(command)}")
        return "extracted", output_path

    ensure_parent(output_path)

    code, out, err = run_command(list(command), capture=True)
    if code != 0:
        log.error(
            "mkvextract failed (code=%s) for %s track %s → %s",
            code,
            mkv_path,
            track.track_id,
            output_path,
        )
        if err:
            log.error(err)
        if out:
            log.error(out)
        return "failed", output_path

    log.info(f"Extracted subtitles → {output_path}")
    return "extracted", output_path


def vid_mkv_extract_subs(
    csv_path: Path | str,
    output_dir: Optional[Path | str] = None,
    mkvextract_bin: str = "mkvextract",
    overwrite: bool = False,
    dry_run: bool = False,
) -> Dict[str, List[Path]]:
    """
    High-level helper that extracts all subtitle tracks listed in a mkv_scan CSV.

    Returns a dict with summary lists:
      {
        "extracted": [...],
        "skipped": [...],
        "failed": [...],
      }
    """
    subtitle_tracks = _load_subtitle_tracks(csv_path)
    if not subtitle_tracks:
        log.warning(f"No subtitle tracks found in {csv_path}")
        return {"extracted": [], "skipped": [], "failed": []}

    extracted: List[Path] = []
    skipped: List[Path] = []
    failed: List[Path] = []

    for track in subtitle_tracks:
        status, path = extract_subtitles_for_track(
            track,
            output_dir=output_dir,
            mkvextract_bin=mkvextract_bin,
            overwrite=overwrite,
            dry_run=dry_run,
        )
        if status == "extracted":
            extracted.append(path)
        elif status == "skipped":
            skipped.append(path)
        else:
            failed.append(path)

    summary = {
        "extracted": extracted,
        "skipped": skipped,
        "failed": failed,
    }

    log.info(
        "Subtitle extraction finished — extracted=%d, skipped=%d, failed=%d",
        len(extracted),
        len(skipped),
        len(failed),
    )
    return summary
