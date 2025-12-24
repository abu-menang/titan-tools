from __future__ import annotations

from common.shared.loader import load_media_types


def test_load_media_types_defaults() -> None:
    media_types = load_media_types()
    # basic sanity: ensure well-known entries exist
    assert ".mkv" in media_types.video_exts
    assert ".mp3" in media_types.audio_exts
    assert ".srt" in media_types.subtitle_exts
    # union should contain elements from each category
    assert media_types.all_known_exts.issuperset(
        {".mkv", ".mp3", ".jpg", ".pdf", ".srt"}
    )
