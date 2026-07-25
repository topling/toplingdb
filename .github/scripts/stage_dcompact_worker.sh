#!/usr/bin/env bash
# Install/stage dcompact_worker + CI yaml into an existing Topling prefix.
# Does NOT compile — caller must have already built `dcompact_worker`.
#
# Usage:
#   stage_dcompact_worker.sh <topling-prefix>
set -euo pipefail

PREFIX="${1:?topling prefix required (contains bin/ lib/)}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
YAML_SRC="$REPO_ROOT/.github/bench-conf/db_bench_enterprise_dcompact_ci.yaml"

test -d "$PREFIX/bin"
test -f "$YAML_SRC"

# Locate worker built by `make dcompact_worker` (Makefile copies to OBJ_DIR and
# builds under sideplugin/topling-dcompact/tools/dcompact/${ORIG_OBJ_DIR}/).
WORKER_SRC=""
if [[ -x "$REPO_ROOT/dcompact_worker.exe" ]]; then
  WORKER_SRC="$REPO_ROOT/dcompact_worker.exe"
else
  WORKER_SRC="$(find "$REPO_ROOT/sideplugin/topling-dcompact/tools/dcompact" \
    -name 'dcompact_worker.exe' -type f 2>/dev/null | head -1 || true)"
fi
if [[ -z "${WORKER_SRC}" || ! -x "$WORKER_SRC" ]]; then
  # Last resort: OBJ_DIR copy
  WORKER_SRC="$(find "$REPO_ROOT" -path '*/rls/*/dcompact_worker.exe' -type f \
    ! -path '*/sideplugin/*' 2>/dev/null | head -1 || true)"
fi
if [[ -z "${WORKER_SRC}" || ! -x "$WORKER_SRC" ]]; then
  echo "FAIL: dcompact_worker.exe not found; build with: make dcompact_worker DEBUG_LEVEL=0" >&2
  exit 1
fi

mkdir -p "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/toplingdb-conf"

# Shared lib required at runtime (static `make install` may omit librocksdb.so).
if [[ ! -e "$PREFIX/lib/librocksdb.so" && ! -e "$PREFIX/lib/librocksdb.so.8" ]]; then
  echo "install-shared into ${PREFIX}"
  make -C "$REPO_ROOT" install-shared PREFIX="$PREFIX" DEBUG_LEVEL=0 UPDATE_REPO=0
fi

cp -a "$WORKER_SRC" "$PREFIX/bin/dcompact_worker.exe"
chmod +x "$PREFIX/bin/dcompact_worker.exe"
cp -a "$YAML_SRC" "$PREFIX/toplingdb-conf/db_bench_enterprise_dcompact_ci.yaml"

export LD_LIBRARY_PATH="$PREFIX/lib:${LD_LIBRARY_PATH:-}"
missing="$(ldd "$PREFIX/bin/dcompact_worker.exe" | grep 'not found' || true)"
if [[ -n "$missing" ]]; then
  echo "FAIL: dcompact_worker has unresolved libs:" >&2
  echo "$missing" >&2
  ldd "$PREFIX/bin/dcompact_worker.exe" >&2 || true
  exit 1
fi

echo "staged dcompact_worker + CI yaml -> ${PREFIX}"
ldd "$PREFIX/bin/dcompact_worker.exe" || true
