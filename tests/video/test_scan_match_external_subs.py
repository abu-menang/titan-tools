from __future__ import annotations

from pathlib import Path

from video.scanners.scan_tracks import _ProbeResult, _iter_files, _match_external_subs


def test_match_external_subs_skips_videos_without_matches(tmp_path: Path) -> None:
    video = _ProbeResult(path=tmp_path / "Movie.mkv", tracks=[{"type": "video", "id": "0"}])

    mkv_rows, non_mkv_rows, unmatched = _match_external_subs([video], [])

    assert mkv_rows == []
    assert non_mkv_rows == []
    assert unmatched == []


def test_match_external_subs_returns_unmatched_subs(tmp_path: Path) -> None:
    video = _ProbeResult(path=tmp_path / "Movie.mkv", tracks=[{"type": "video", "id": "0"}])
    stray_sub = _ProbeResult(path=tmp_path / "Other.srt", tracks=[])

    mkv_rows, non_mkv_rows, unmatched = _match_external_subs([video], [stray_sub])

    assert mkv_rows == []
    assert non_mkv_rows == []
    assert unmatched == [stray_sub.path]


def test_match_external_subs_collects_rows_for_matching_files(tmp_path: Path) -> None:
    video = _ProbeResult(
        path=tmp_path / "Show.S01E01.mkv",
        tracks=[{"type": "video", "id": "0", "lang": "eng"}],
    )
    ext_sub = _ProbeResult(
        path=tmp_path / "Show.S01E01.eng.srt",
        tracks=[{"type": "subtitles", "id": "1", "lang": "eng"}],
    )

    mkv_rows, non_mkv_rows, unmatched = _match_external_subs([video], [ext_sub])

    assert non_mkv_rows == []
    assert unmatched == []
    # One subtitle row + one video track row for the matched file
    assert len(mkv_rows) == 2
    assert {row["type"] for row in mkv_rows} == {"video", "subtitles"}


def test_match_external_subs_routes_non_mkv_rows(tmp_path: Path) -> None:
    video = _ProbeResult(path=tmp_path / "Clip.mp4", tracks=[{"type": "video", "id": "0", "lang": "eng"}])
    ext_sub = _ProbeResult(path=tmp_path / "Clip.srt", tracks=[{"type": "subtitles", "id": "2"}])

    mkv_rows, non_mkv_rows, unmatched = _match_external_subs([video], [ext_sub])

    assert mkv_rows == []
    assert unmatched == []
    assert len(non_mkv_rows) == 2
    assert {row["type"] for row in non_mkv_rows} == {"video", "subtitles"}


def test_iter_files_does_not_exclude_root_when_same_as_output(tmp_path: Path) -> None:
    sample = tmp_path / "movie.mkv"
    sample.touch()

    files = list(_iter_files([tmp_path], exclude_dir=tmp_path))

    assert sample in files


def test_iter_files_excludes_nested_output_dir(tmp_path: Path) -> None:
    keep = tmp_path / "keep.mkv"
    keep.touch()
    out_dir = tmp_path / "00_reports"
    out_dir.mkdir()
    skip = out_dir / "skip.txt"
    skip.touch()

    files = list(_iter_files([tmp_path], exclude_dir=out_dir))

    assert keep in files
    assert skip not in files
