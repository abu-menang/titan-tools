from __future__ import annotations

from pathlib import Path

from video.scan import _extract_track_rows


def test_extract_track_rows_generates_expected_fields() -> None:
    sample = {
        "tracks": [
            {
                "id": 0,
                "type": "video",
                "codec": "V_MPEGH/ISO/HEVC",
                "properties": {
                    "language": "eng",
                    "width": 1920,
                    "height": 1080,
                    "nominal_frame_rate": "24000/1001",
                    "default_track": True,
                    "forced_track": False,
                },
            },
            {
                "id": 1,
                "type": "audio",
                "codec": "A_AAC",
                "properties": {
                    "language": "jpn",
                    "audio_channels": 2,
                    "audio_sampling_frequency": 48000,
                    "default_track": True,
                    "forced_track": False,
                },
            },
            {
                "id": 2,
                "type": "subtitles",
                "codec": "S_TEXT/ASS",
                "properties": {
                    "language": "eng",
                    "default_track": False,
                    "forced_track": False,
                },
            },
        ]
    }

    rows = _extract_track_rows(Path("/media/example.mkv"), sample, 4096)

    assert len(rows) == 3

    video_row = rows[0]
    audio_row = rows[1]
    subtitle_row = rows[2]

    assert video_row["path"] == "/media/example.mkv"
    assert video_row["filename"] == "example.mkv"
    assert video_row["type"] == "video"
    assert video_row["width"] == "1920"
    assert video_row["height"] == "1080"
    assert video_row["default"] == "true"
    assert video_row["forced"] == "false"
    assert video_row["edited_name"] == "example"

    assert audio_row["type"] == "audio"
    assert audio_row["channels"] == "2"
    assert audio_row["sample_rate"] == "48000"
    assert audio_row["edited_name"] == "JPN (A_AAC)"

    assert subtitle_row["type"] == "subtitles"
    assert subtitle_row["lang"] == "eng"
    assert subtitle_row["edited_name"] == "ENG (S_TEXT/ASS)"
