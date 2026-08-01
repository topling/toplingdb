#!/usr/bin/env bash
# Thin wrapper; canonical script is tracked under .github/scripts/.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec "$ROOT/.github/scripts/run_local_simple_top_pages.sh" "$@"
