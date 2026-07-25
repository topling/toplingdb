#!/usr/bin/env bash
# Build .github/scripts/sample_statm from sample_statm.c if missing or stale.
# Prints the absolute path of the binary on stdout.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${SCRIPT_DIR}/sample_statm.c"
BIN="${SCRIPT_DIR}/sample_statm"
if [[ ! -x "$BIN" || "$SRC" -nt "$BIN" ]]; then
  cc -O2 -Wall -Wextra -Werror -o "$BIN" "$SRC"
fi
printf '%s\n' "$BIN"
