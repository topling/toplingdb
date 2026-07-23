#!/usr/bin/env python3
"""Anchored patch: add -report_bench_start_time flag to upstream RocksDB db_bench_tool.cc.

Two anchored insertions:
  1. After 'DEFINE_bool(histogram, ...' — insert the gflag definition.
  2. Before the result-line fprintf in Stats::Report() — insert timestamp printing.

Exits non-zero if any anchor is missing (hard failure) or already patched (idempotent).
"""
import re
import sys

FLAG_DEF = (
    '\n'
    'DEFINE_bool(report_bench_start_time, false,\n'
    '            "Prefix each benchmark result line with the ISO 8601 UTC start "\n'
    '            "time of that benchmark (microsecond precision)");\n'
)

TIMESTAMP_CODE = (
    '    if (FLAGS_report_bench_start_time) {\n'
    '      time_t secs = static_cast<time_t>(start_ / 1000000);\n'
    '      int usecs = static_cast<int>(start_ % 1000000);\n'
    '      struct tm t;\n'
    '      gmtime_r(&secs, &t);\n'
    '      fprintf(stdout,\n'
    '              "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ ",\n'
    '              t.tm_year + 1900, t.tm_mon + 1, t.tm_mday,\n'
    '              t.tm_hour, t.tm_min, t.tm_sec, usecs);\n'
    '    }\n'
)

ALREADY_PATCHED_MARKER = 'report_bench_start_time'


def patch(path: str) -> None:
    with open(path, 'r') as f:
        src = f.read()

    if ALREADY_PATCHED_MARKER in src:
        print(f"[patch_db_bench_time] already patched: {path}")
        return

    # --- Anchor 1: insert flag definition after DEFINE_bool(histogram ---
    anchor1 = re.search(
        r'(DEFINE_bool\(histogram,\s*false,\s*"[^"]*"\);)',
        src,
    )
    if not anchor1:
        print(f"[patch_db_bench_time] FATAL: anchor 'DEFINE_bool(histogram' not found in {path}", file=sys.stderr)
        sys.exit(1)

    insert_pos1 = anchor1.end()
    src = src[:insert_pos1] + FLAG_DEF + src[insert_pos1:]

    # --- Anchor 2: insert timestamp code before the result-line fprintf ---
    anchor2 = re.search(
        r'([ \t]+fprintf\(stdout,\s*\n\s*"%-12s : %11\.\d+f micros/op)',
        src,
    )
    if not anchor2:
        print(f"[patch_db_bench_time] FATAL: anchor 'fprintf(stdout, \"%-12s : %11.Nf micros/op' not found in {path}", file=sys.stderr)
        sys.exit(1)

    insert_pos2 = anchor2.start()
    src = src[:insert_pos2] + TIMESTAMP_CODE + src[insert_pos2:]

    with open(path, 'w') as f:
        f.write(src)

    print(f"[patch_db_bench_time] patched successfully: {path}")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/db_bench_tool.cc>", file=sys.stderr)
        sys.exit(1)
    patch(sys.argv[1])
