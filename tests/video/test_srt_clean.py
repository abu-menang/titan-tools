from __future__ import annotations

from video.srt_clean import SrtBlock, clean_srt_blocks


def _block(text: str, index: int = 1, timing: str = "00:00:00,000 --> 00:00:02,000") -> SrtBlock:
    return SrtBlock(index=str(index), timing=timing, lines=text.splitlines())


def test_clean_srt_blocks_keeps_latin_and_removes_cjk_when_eng_configured() -> None:
    english = _block("This is an English sentence.")
    japanese = _block("これは日本語の字幕です。")
    numeric = _block("123")

    filtered, removed = clean_srt_blocks(
        [english, japanese, numeric],
        allowed_languages=["eng"],
        min_text_chars=5,
    )

    assert english in filtered
    assert japanese not in filtered
    assert numeric in filtered  # too short to classify, should be kept
    assert removed == 1


def test_clean_srt_blocks_keeps_japanese_and_drops_english_when_ja_configured() -> None:
    english = _block("Random English line for karaoke.")
    japanese = _block("さくら舞い散る季節。")

    filtered, removed = clean_srt_blocks(
        [english, japanese],
        allowed_languages=["ja"],
        min_text_chars=5,
    )

    assert japanese in filtered
    assert english not in filtered
    assert removed == 1
