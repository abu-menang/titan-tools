# vid-mkv-scan flow (editable table)

| Step | What it does | Key inputs / decisions | Outputs | Your suggested improvement |
| --- | --- | --- | --- | --- |
| 1 | Launch wrapper script `apps/vid-mkv-scan`; load YAML via `common.shared.loader` and configure logging | Config file (default `configs/config.yaml`), env overrides (`VENV_DIR`, `PYTHON_BIN`) | Parsed task config passed to `video.scan.vid_mkv_scan` |  |
| 2 | Resolve scan roots (default CWD); pick report directory (explicit `output_dir` > task `output_root` > first root); create it unless `dry_run` | `roots`, `output_dir`, `output_root`, `dry_run` | `base_output_dir` ready for writes |  |
| 3 | Walk filesystem under roots (excluding output dir); collect MKVs, other video, subtitles; log unsupported/skipped items | Root paths, extension filters (`MKV_EXTS`, `VIDEO_EXTS`, `SUBTITLE_EXTS`) | Lists of MKVs, non-MKV videos, subtitle sidecars, skipped rows |  |
| 4 | Per MKV: stat size, derive names (`_build_name_list_row`), run `mkvmerge -J`, parse JSON | File path, mkvmerge availability | Track rows (codec/lang/flags/props), name rows; failures recorded on errors |  |
| 5 | Mark non-HEVC MKVs (no video codec contains “hevc”) | Parsed track codecs, file size | Non-HEVC rows |  |
| 6 | Build directory rows for parent folders (for name lists) | Seen directories | Directory rows + index map |  |
| 7 | Normalize skipped list to remove duplicates covered elsewhere | Skipped entries, collected paths | Cleaned skipped rows |  |
| 8 | Map subtitles to videos (MKV and non-MKV) and find unmatched subs | Subtitle/video filename matching | External subtitle rows (for MKV and non-MKV), unmatched subtitle list |  |
| 9 | Load language whitelists (`lang_vid/aud/sub`; default `["eng"]`; empty disables check) | Task config | Allowed language sets per type |  |
| 10 | Classify MKVs: count tracks by type; flag files with 0/>1 per type or language mismatch; split into OK vs Issues | Track rows, language rules | OK/Issues file groups, language mismatch lists |  |
| 11 | Classify non-MKV scan rows similarly (structure + language) | Non-MKV rows, language rules | OK/Issues groups for non-MKV; non-MKV non-HEVC rows |  |
| 12 | Batch rows if `batch_size` > 0 | `batch_size` | Chunked row lists for writing |  |
| 13 | Write CSV reports (unless `dry_run` or `write_csv_file=False`): mkv_ok, mkv_issues, mkv_non_hevc, mkv_failures, mkv_skipped; non_mkv_ok, non_mkv_issues, non_mkv_non_hevc; non_mkv_ext_subs; mkv_ext_subs | Chunked rows, `base_output_dir` | CSV files with paths + row counts |  |
| 14 | Write human-readable summaries in `output_dir`: text and best-effort HTML with tables/links | Aggregated counts, language flags, report metadata | `scan_summary_*.txt` and `scan_summary_*.html` |  |
| 15 | Return consolidated MKV track rows to caller | Track rows | In-memory list of track dicts |  |
