#!/usr/bin/env bash
# Build .github/scripts/sample_statm_fdcache from sample_statm_fdcache.cpp if missing or stale.
# Prints the absolute path of the binary on stdout.
# Also runs parse/SVG unit checks for the four-line fd_cache series.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${SCRIPT_DIR}/sample_statm_fdcache.cpp"
BIN="${SCRIPT_DIR}/sample_statm_fdcache"
if [[ ! -x "$BIN" || "$SRC" -nt "$BIN" ]]; then
  c++ -O2 -Wall -Wextra -Werror -std=c++17 -o "$BIN" "$SRC"
fi
python3 "${SCRIPT_DIR}/test_bench_rss_series.py" >/dev/null
printf '%s\n' "$BIN"
