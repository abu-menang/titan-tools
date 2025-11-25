"""
video.srt_clean

Filter subtitle blocks in SRT files based on language detection.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from common.base.fs import ensure_parent
from common.base.logging import get_logger

log = get_logger(__name__)


LANG_ALIASES: Dict[str, str] = {
    "en": "en",
    "eng": "en",
    "english": "en",
    "es": "es",
    "spa": "es",
    "spanish": "es",
    "fr": "fr",
    "fra": "fr",
    "fre": "fr",
    "french": "fr",
    "de": "de",
    "ger": "de",
    "deu": "de",
    "german": "de",
    "it": "it",
    "ita": "it",
    "italian": "it",
    "pt": "pt",
    "por": "pt",
    "portuguese": "pt",
    "hi": "hi",
    "hin": "hi",
    "mal": "ml",
    "malayalam": "ml",
    "ml": "ml",
    "tam": "ta",
    "ta": "ta",
    "tamil": "ta",
    "tel": "te",
    "te": "te",
    "telugu": "te",
    "kan": "kn",
    "kannada": "kn",
    "kn": "kn",
    "jpn": "ja",
    "ja": "ja",
    "jp": "ja",
    "japanese": "ja",
    "zh": "zh",
    "zho": "zh",
    "cmn": "zh",
    "chs": "zh",
    "cnt": "zh",
    "cn": "zh",
    "chi": "zh",
    "zh-cn": "zh",
    "zh-tw": "zh",
    "ko": "ko",
    "kor": "ko",
    "korean": "ko",
    "ru": "ru",
    "rus": "ru",
    "russian": "ru",
    "ar": "ar",
    "ara": "ar",
    "arabic": "ar",
    "he": "he",
    "heb": "he",
    "hebrew": "he",
    "el": "el",
    "ell": "el",
    "greek": "el",
    "th": "th",
    "tha": "th",
    "thai": "th",
}


SCRIPT_ALIASES: Dict[str, set[str]] = {
    "en": {"latin"},
    "es": {"latin"},
    "fr": {"latin"},
    "de": {"latin"},
    "it": {"latin"},
    "pt": {"latin"},
    "hi": {"devanagari"},
    "ml": {"malayalam"},
    "ta": {"tamil"},
    "te": {"telugu"},
    "kn": {"kannada"},
    "ja": {"hiragana", "katakana", "cjk"},
    "zh": {"cjk"},
    "ko": {"hangul"},
    "ru": {"cyrillic"},
    "ar": {"arabic"},
    "he": {"hebrew"},
    "el": {"greek"},
    "th": {"thai"},
}


SCRIPT_RANGES: Dict[str, Tuple[Tuple[int, int], ...]] = {
    "latin": (
        (0x0041, 0x005A),
        (0x0061, 0x007A),
        (0x00C0, 0x00FF),
        (0x0100, 0x024F),
        (0x1E00, 0x1EFF),
    ),
    "cjk": (
        (0x4E00, 0x9FFF),
        (0x3400, 0x4DBF),
        (0x20000, 0x2A6DF),
        (0x2A700, 0x2B73F),
        (0x2B740, 0x2B81F),
        (0x2B820, 0x2CEAF),
        (0xF900, 0xFAFF),
        (0x2F800, 0x2FA1F),
    ),
    "hiragana": ((0x3040, 0x309F),),
    "katakana": (
        (0x30A0, 0x30FF),
        (0x31F0, 0x31FF),
        (0xFF65, 0xFF9F),
    ),
    "hangul": (
        (0xAC00, 0xD7A3),
        (0x1100, 0x11FF),
        (0x3130, 0x318F),
    ),
    "devanagari": ((0x0900, 0x097F),),
    "malayalam": ((0x0D00, 0x0D7F),),
    "tamil": ((0x0B80, 0x0BFF),),
    "telugu": ((0x0C00, 0x0C7F),),
    "kannada": ((0x0C80, 0x0CFF),),
    "cyrillic": (
        (0x0400, 0x04FF),
        (0x0500, 0x052F),
        (0x2DE0, 0x2DFF),
        (0xA640, 0xA69F),
    ),
    "arabic": (
        (0x0600, 0x06FF),
        (0x0750, 0x077F),
        (0x08A0, 0x08FF),
    ),
    "hebrew": ((0x0590, 0x05FF),),
    "greek": (
        (0x0370, 0x03FF),
        (0x1F00, 0x1FFF),
    ),
    "thai": ((0x0E00, 0x0E7F),),
}

DEFAULT_ALLOWED_CATEGORIES = {"common"}
DEFAULT_MATCH_THRESHOLD = 0.6


@dataclass
class SrtBlock:
    index: str
    timing: str
    lines: List[str]

    def text(self) -> str:
        return " ".join(self.lines).strip()


def _normalize_language_code(code: str) -> Optional[str]:
    if not code:
        return None
    lower = code.strip().lower()
    return LANG_ALIASES.get(lower, lower[:2] if len(lower) >= 2 else None)


def _normalize_allowed_languages(languages: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    for lang in languages:
        normalized_code = _normalize_language_code(str(lang))
        if not normalized_code:
            continue
        if normalized_code not in normalized:
            normalized.append(normalized_code)
    return normalized


def _languages_to_categories(languages: Iterable[str]) -> set[str]:
    allowed_categories: set[str] = set(DEFAULT_ALLOWED_CATEGORIES)
    for lang in languages:
        scripts = SCRIPT_ALIASES.get(lang)
        if scripts:
            allowed_categories.update(scripts)
        else:
            log.warning(f"No script mapping configured for language '{lang}'; defaulting to Latin.")
            allowed_categories.add("latin")
    return allowed_categories


def _char_category(ch: str) -> Optional[str]:
    code_point = ord(ch)
    for category, ranges in SCRIPT_RANGES.items():
        for start, end in ranges:
            if start <= code_point <= end:
                return category
    if ch.isdigit() or ch.isspace():
        return "common"
    if ch in {".", ",", "!", "?", ":", ";", "-", "'", '"', "…", "—", "(", ")", "[", "]"}:
        return "common"
    if ch.isascii() and ch.isalpha():
        # Fallback: treat ASCII letters as Latin.
        return "latin"
    return "other"


def _categorize_text(text: str) -> Dict[str, int]:
    categories: Dict[str, int] = {}
    for ch in text:
        category = _char_category(ch)
        if category is None:
            continue
        categories[category] = categories.get(category, 0) + 1
    return categories


def _parse_srt(content: str) -> List[SrtBlock]:
    lines = content.splitlines()
    blocks: List[SrtBlock] = []
    i = 0
    total_lines = len(lines)

    while i < total_lines:
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        index = line
        i += 1
        if i >= total_lines:
            break
        timing = lines[i].rstrip("\n")
        i += 1

        text_lines: List[str] = []
        while i < total_lines and lines[i].strip():
            text_lines.append(lines[i])
            i += 1

        # Skip empty separators
        while i < total_lines and not lines[i].strip():
            i += 1

        blocks.append(SrtBlock(index=index, timing=timing, lines=text_lines))

    return blocks


def _blocks_to_srt(blocks: Sequence[SrtBlock]) -> str:
    output_lines: List[str] = []
    for idx, block in enumerate(blocks, start=1):
        output_lines.append(str(idx))
        output_lines.append(block.timing)
        output_lines.extend(block.lines)
        output_lines.append("")
    return "\n".join(output_lines).strip() + "\n"


def clean_srt_blocks(
    blocks: Sequence[SrtBlock],
    allowed_languages: Sequence[str],
    min_text_chars: int = 10,
) -> Tuple[List[SrtBlock], int]:
    normalized_languages = _normalize_allowed_languages(allowed_languages)
    if not normalized_languages:
        raise ValueError("At least one allowed language must be provided.")

    allowed_categories = _languages_to_categories(normalized_languages)
    keep: List[SrtBlock] = []
    removed = 0

    for block in blocks:
        text = block.text()
        normalized_text = re.sub(r"\s+", " ", text)
        if len(normalized_text) < min_text_chars:
            keep.append(block)
            continue

        category_counts = _categorize_text(normalized_text)
        total_letters = sum(
            count for category, count in category_counts.items() if category not in DEFAULT_ALLOWED_CATEGORIES
        )

        if total_letters == 0:
            keep.append(block)
            continue

        allowed_count = sum(
            count for category, count in category_counts.items() if category in allowed_categories
        )
        ratio = allowed_count / max(total_letters, 1)

        if ratio >= DEFAULT_MATCH_THRESHOLD:
            keep.append(block)
        else:
            removed += 1
            log.debug(
                "Removing block (allowed ratio %.2f < %.2f): '%s'",
                ratio,
                DEFAULT_MATCH_THRESHOLD,
                text,
            )

    return keep, removed


def _gather_srt_files(roots: Iterable[Path]) -> List[Path]:
    files: List[Path] = []
    for root in roots:
        root_path = Path(root).expanduser()
        if root_path.is_dir():
            files.extend(sorted(root_path.rglob("*.srt")))
    return files


def clean_srt_file(
    path: Path,
    allowed_languages: Sequence[str],
    *,
    min_text_chars: int = 10,
    overwrite: bool = False,
    output_dir: Optional[Path] = None,
    file_suffix: str = ".filtered",
    dry_run: bool = False,
) -> Tuple[Path, int, int]:
    """Clean a single SRT file. Returns (output_path, removed_blocks, total_blocks)."""

    srt_path = Path(path).expanduser()
    if not srt_path.exists():
        raise FileNotFoundError(srt_path)

    content = srt_path.read_text(encoding="utf-8", errors="replace")
    blocks = _parse_srt(content)

    allowed = _normalize_allowed_languages(allowed_languages)
    if not allowed:
        raise ValueError("No valid languages supplied for cleaning.")

    filtered_blocks, removed = clean_srt_blocks(blocks, allowed, min_text_chars=min_text_chars)
    total = len(blocks)

    if removed == 0:
        log.info(f"No changes required for {srt_path}")
        return srt_path, 0, total

    output_path: Path
    if overwrite:
        output_path = srt_path
    else:
        target_dir = Path(output_dir).expanduser() if output_dir else srt_path.parent
        suffix = file_suffix or ".filtered"
        output_path = target_dir / f"{srt_path.stem}{suffix}{srt_path.suffix}"

    if dry_run:
        log.info(
            f"[DRY-RUN] Would update {srt_path} → {output_path} (removed {removed}/{total} blocks)",
        )
        return output_path, removed, total

    ensure_parent(output_path)
    new_content = _blocks_to_srt(filtered_blocks)
    output_path.write_text(new_content, encoding="utf-8")
    log.info(f"Cleaned {srt_path} → {output_path} (removed {removed}/{total} blocks)")
    return output_path, removed, total


def vid_srt_clean(
    roots: Sequence[Path],
    *,
    languages: Sequence[str],
    min_text_chars: int = 10,
    overwrite: bool = False,
    output_dir: Optional[Path] = None,
    file_suffix: str = ".filtered",
    dry_run: bool = False,
) -> Dict[str, List[Path]]:
    if not languages:
        raise ValueError("vid_srt_clean requires at least one language in configuration.")

    normalized_languages = _normalize_allowed_languages(languages)
    if not normalized_languages:
        raise ValueError("vid_srt_clean configuration did not produce valid language codes.")

    roots_list = [Path(r).expanduser() for r in roots]
    srt_files = _gather_srt_files(roots_list)
    if not srt_files:
        log.warning("No SRT files discovered for cleaning.")
        return {"processed": [], "updated": [], "skipped": []}

    processed: List[Path] = []
    updated: List[Path] = []
    skipped: List[Path] = []

    for srt_file in srt_files:
        try:
            output_path, removed, total = clean_srt_file(
                srt_file,
                normalized_languages,
                min_text_chars=min_text_chars,
                overwrite=overwrite,
                output_dir=output_dir,
                file_suffix=file_suffix,
                dry_run=dry_run,
            )
            processed.append(srt_file)
            if removed > 0:
                updated.append(output_path)
            else:
                skipped.append(srt_file)
        except Exception as exc:
            log.error(f"Failed to clean {srt_file}: {exc}")

    log.info(
        "SRT cleaning completed — processed=%d updated=%d skipped=%d",
        len(processed),
        len(updated),
        len(skipped),
    )
    return {
        "processed": processed,
        "updated": updated,
        "skipped": skipped,
    }


__all__ = [
    "SrtBlock",
    "clean_srt_blocks",
    "clean_srt_file",
    "vid_srt_clean",
]
