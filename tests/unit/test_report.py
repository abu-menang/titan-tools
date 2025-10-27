from __future__ import annotations

from pathlib import Path

import os
import time

from common.shared.report import discover_latest_csvs, write_chunked_csvs


def test_write_chunked_csvs_single_chunk(tmp_path: Path) -> None:
    rows = [[{"value": 1}]]

    paths = write_chunked_csvs(rows, "chunk_test", output_dir=tmp_path, dry_run=False)

    assert len(paths) == 1
    assert paths[0].parent == tmp_path
    assert paths[0].suffix == ".csv"
    assert "_part" not in paths[0].stem
    assert paths[0].exists()


def test_write_chunked_csvs_multiple_chunks(tmp_path: Path) -> None:
    rows = [
        [{"value": 1}],
        [{"value": 2}],
    ]

    paths = write_chunked_csvs(rows, "chunk_test_multi", output_dir=tmp_path, dry_run=False)

    assert len(paths) == 2
    assert all(path.parent == tmp_path for path in paths)
    assert paths[0].suffix == ".csv"
    assert "_part01" in paths[0].stem
    assert "_part02" in paths[1].stem
    assert all(path.exists() for path in paths)


def test_discover_latest_csvs(tmp_path: Path) -> None:
    base_name = "mkv_scan_name_list"
    file_a = tmp_path / f"{base_name}_20240101.csv"
    file_b = tmp_path / f"{base_name}_20240102.csv"
    file_a.write_text("a", encoding="utf-8")
    time.sleep(0.01)
    file_b.write_text("b", encoding="utf-8")

    latest = discover_latest_csvs([tmp_path], base_name)
    assert latest == [file_b.resolve()]

    part1 = tmp_path / f"{base_name}_20240103_part01.csv"
    time.sleep(0.01)
    part1.write_text("p1", encoding="utf-8")
    os.utime(part1, (part1.stat().st_atime, part1.stat().st_mtime + 1))

    selected = discover_latest_csvs([tmp_path], base_name, [1])
    assert selected == [part1.resolve()]

    try:
        discover_latest_csvs([tmp_path], base_name, [2])
    except FileNotFoundError as exc:
        assert "part 02" in str(exc)
    else:  # pragma: no cover - safeguard
        assert False, "Expected FileNotFoundError for missing part"

    # Ensure CSV discovery works even if CSV is stored under csv/ subdir
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir(exist_ok=True)
    paired_stem = f"{base_name}_20240104"
    time.sleep(0.01)
    csv_file = csv_dir / f"{paired_stem}.csv"
    csv_file.write_text("csv", encoding="utf-8")
    # Ensure this csv mirror has a later modification time than earlier parts
    try:
        os.utime(csv_file, (csv_file.stat().st_atime, part1.stat().st_mtime + 1))
    except Exception:
        # On filesystems with coarse timestamps, best-effort; tests will still be valid
        pass

    latest_report = discover_latest_csvs([tmp_path], base_name)
    assert latest_report == [csv_file.resolve()]

    time.sleep(0.01)
    part_csv = csv_dir / f"{base_name}_20240105_part02.csv"
    part_csv.write_text("partcsv", encoding="utf-8")
    os.utime(part_csv, (part_csv.stat().st_atime, part_csv.stat().st_mtime + 1))

    selected_part = discover_latest_csvs(
        [tmp_path],
        base_name,
        [2],
    )
    assert selected_part == [part_csv.resolve()]
