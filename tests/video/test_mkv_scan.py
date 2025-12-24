from __future__ import annotations

from pathlib import Path

from video.scanners.scan_tracks import _build_external_subtitle_rows, _extract_track_rows


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
    assert video_row["id"] == "0"
    assert video_row["filename"] == "example.mkv"
    assert video_row["type"] == "video"
    assert video_row["width"] == "1920"
    assert video_row["height"] == "1080"
    assert video_row["default"] == "yes"
    assert video_row["forced"] == "no"
    assert video_row["edited_name"] == "example"
    assert video_row["encoding"] == ""

    assert audio_row["type"] == "audio"
    assert audio_row["id"] == "1"
    assert audio_row["channels"] == "2"
    assert audio_row["sample_rate"] == "48000"
    assert audio_row["edited_name"] == "JPN (A_AAC)"
    assert audio_row["default"] == "yes"
    assert audio_row["forced"] == "no"
    assert audio_row["encoding"] == ""

    assert subtitle_row["type"] == "subtitles"
    assert subtitle_row["id"] == "2"
    assert subtitle_row["lang"] == "eng"
    assert subtitle_row["edited_name"] == "ENG (S_TEXT/ASS)"
    assert subtitle_row["default"] == "no"
    assert subtitle_row["forced"] == "no"


def test_build_external_subtitle_rows_matches_with_language_suffix(tmp_path: Path) -> None:
    video_path = tmp_path / "Show.S01E01.mp4"
    video_path.touch()

    local_sub = tmp_path / "Show.S01E01.eng.srt"
    local_sub.touch()

    other_dir = tmp_path / "subs"
    other_dir.mkdir()
    other_sub = other_dir / "Show.S01E01.srt"
    other_sub.touch()

    rows = _build_external_subtitle_rows([video_path], [local_sub, other_sub])
    assert len(rows) == 4  # video + audio + 2 subs
    subtitle_rows = [r for r in rows if r["type"] == "subtitles"]
    assert subtitle_rows
    assert subtitle_rows[0]["input_path"] == str(local_sub)
    assert subtitle_rows[0]["edited_name"].startswith("ENG")
    assert rows[0]["output_filename"] == "Show.S01E01.mkv"
    assert rows[0]["type"] == "video"
    assert rows[1]["type"] == "audio"


def test_build_external_subtitle_rows_handles_unmatched_video(tmp_path: Path) -> None:
    video_path = tmp_path / "Movie.mp4"
    video_path.touch()

    rows = _build_external_subtitle_rows([video_path], [])
    # still emits video + audio entries even if no subs were found
    assert len(rows) == 2
    assert rows[0]["type"] == "video"
    assert rows[1]["type"] == "audio"
