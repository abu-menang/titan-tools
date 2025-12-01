flowchart TD
    A0[Start vid_mkv_scan] --> A1[Import media types\nload_media_types -> MEDIA_TYPES\nVIDEO_EXTS/SUBTITLE_EXTS derive]
    A1 --> B1[Resolve roots/args\nchoose primary_root\nset base_output_dir\nensure_dir if !dry_run]
    B1 -->|log 🎬 roots/output_dir/output_root/dry_run/batch_size/report_dir| B2[Init lists\nmkv_files\nnon_mkv_video_files\nsubtitle_files\nskipped_rows]
    B2 --> C1[Walk roots via _iter_files\nexclude base_output_dir\nbucket by extension]
    C1 -->|append| C2[mkv_files list]
    C1 -->|append| C3[non_mkv_video_files list]
    C1 -->|append| C4[subtitle_files list]
    C1 -->|append skipped rows\nunsupported ext| C5[skipped_rows list]
    C5 -->|log 🎯 counts mkv/non_mkv/subs/skipped| D0[Probe phase]

    D0 --> D1[_probe_list on mkv_files\nrun mkvmerge -J\nparse tracks or failure_reason]
    D0 --> D2[_probe_list on non_mkv_video_files]
    D0 --> D3[_probe_list on subtitle_files]
    D1 -->|push| E1[mkv_probe list of _ProbeResult]
    D2 -->|push| E2[non_mkv_probe list]
    D3 -->|push| E3[sub_probe list]
    E1 & E2 & E3 --> F1[Collect failures\nfailed_files rows from *_probe.failure_reason]
    F1 --> G1[Filter probes\nremove failed entries]
    G1 -->|success-only| H0[Subtitle matching]

    H0 --> H1[_match_external_subs\nstem-compare]
    H1 -->|append| H2[mkv_ext_sub_rows list\n(video + matched subs rows)]
    H1 -->|append| H3[non_mkv_ext_sub_rows list]
    H1 -->|collect| H4[unmatched_subs_paths list]
    H1 -->|derive| H5[matched_video_paths set]
    H5 -->|remove matched videos| H6[pruned mkv_probe/non_mkv_probe]
    H6 --> I0[Non-HEVC detection]

    I0 --> I1[_non_hevc over mkv rows]
    I0 --> I2[_non_hevc over non-mkv rows]
    I1 -->|append| I3[mkv_non_hevc_rows list]
    I2 -->|append| I4[non_mkv_non_hevc_rows list]
    I3 -->|paths set| I5[mkv_non_hevc_paths]
    I4 -->|paths set| I6[non_mkv_non_hevc_paths]
    I6 --> J0[Classification setup]

    J0 --> J1[load_task_config('vid_mkv_scan')\nfrom configs/vid_mkv_scan/config.yaml]
    J1 -->|normalize| J2[allowed_vid/allowed_aud/allowed_sub lists (defaults eng)]
    J2 -->|log ✅ totals later| K0[Classify rows]

    K0 --> K1[_classify mkv_probe tracks ->\nmkv_files_ok / mkv_files_issues]
    K0 --> K2[_classify non_mkv_probe tracks ->\nnon_mkv_files_ok / non_mkv_files_issues]
    K0 --> K3[_classify mkv_ext_sub_rows ->\nmkv_ext_ok / mkv_ext_issues]
    K0 --> K4[_classify non_mkv_ext_sub_rows ->\nnon_mkv_ext_ok / non_mkv_ext_issues]
    K1 & K2 & K3 & K4 -->|log ✅ classification totals| L0[Non-HEVC reassignment]

    L0 --> L1[_move_non_hevc\nfrom ok/issue into *_issues_non_hevc using paths]
    L1 -->|results| L2[mkv_files_issues_non_hevc\nnon_mkv_files_issues_non_hevc\nmkv_ext_issues_non_hevc\nnon_mkv_ext_issues_non_hevc]
    L1 -->|keep| L3[updated ok lists]

    %% CSV writes
    L3 --> M0[Write CSV reports if write_csv_file]
    L2 --> M0
    M0 -->|write_tabular_reports TRACK_COLUMNS| M1[mkv_ext_sub_ok CSV\nmkv_ext_sub_issues CSV\nmkv_ext_sub_issues_non_hevc CSV]
    M0 -->|NON_MKV_TRACK_COLUMNS| M2[vid_ext_subs_ok/vid_ext_subs_issues/vid_ext_subs_issues_non_hevc CSVs]
    M0 -->|TRACK_COLUMNS| M3[mkv_files_ok/mkv_files_issues/mkv_files_issues_non_hevc CSVs]
    M0 -->|NON_MKV_TRACK_COLUMNS| M4[vid_files_ok/vid_files_issues/vid_files_issues_non_hevc CSVs]
    M0 -->|conditional| M5[failures CSV]
    M0 -->|conditional| M6[skipped CSV]
    M0 -->|record| M7[written_reports dict (paths + rows)]
    M0 -->|log 📝 writing <name> rows=<n>| N0[Summaries]

    %% Text summary
    N0 --> N1[Build aggregates\nfile_counts, actual_langs, summary_rows,\nunmatched_subtitle_files list]
    N1 --> N2[Write scan_summary_<ts>.txt\nwith totals, per-file counts, lang flags]
    N2 -->|log path| N3[HTML summary]

    %% HTML summary
    N3 --> N4[Build HTML tables\nlang mismatches, >1 tracks, missing tracks,\nnon-HEVC, unmatched subs, CSV links (written_reports)]
    N4 --> N5[Write scan_summary_<ts>.html]
    N5 -->|log path| O0[Finish]

    %% Final
    O0 --> O1[Log elapsed seconds ⏱️]
    O1 --> O2[Return mkv_files_ok + mkv_ext_ok]
    O2 --> O3[End]
