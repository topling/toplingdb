#!/usr/bin/env bash
# Build upstream facebook/rocksdb db_bench + memtablerep_bench into a staging prefix.
# Usage: build_upstream_rocksdb.sh <git-ref> <dest-dir> [extra env assignments...]
# Example: build_upstream_rocksdb.sh v8.10.2 "$BUNDLE/rocksdb-v8.10"
#          build_upstream_rocksdb.sh main "$BUNDLE/rocksdb-master"
set -euo pipefail

REF="${1:?git ref required (tag/branch/sha)}"
DEST="${2:?destination directory required}"
shift 2 || true

SRC_DIR="${RUNNER_TEMP:-/tmp}/rocksdb-src-${REF//\//_}"
rm -rf "$SRC_DIR" "$DEST"
mkdir -p "$SRC_DIR" "$DEST/bin" "$DEST/lib"

echo "Cloning facebook/rocksdb @ ${REF} -> ${SRC_DIR}"
if [[ "$REF" == "master" || "$REF" == "latest" ]]; then
  # Upstream default branch is main; keep "master"/"latest" as aliases.
  if ! git clone --depth 1 --branch main https://github.com/facebook/rocksdb.git "$SRC_DIR"; then
    git clone --depth 1 --branch master https://github.com/facebook/rocksdb.git "$SRC_DIR"
  fi
else
  git clone --depth 1 --branch "$REF" https://github.com/facebook/rocksdb.git "$SRC_DIR"
fi


# Allow caller to inject CC/CXX/CPU/EXTRA_* / PORTABLE via environment.
# Default PORTABLE=haswell matches ToplingDB plain (-march=haswell) and avoids
# upstream's -march=native, which can pull AVX-512 on capable build hosts and
# SIGILL on run hosts without AVX-512. Override e.g. PORTABLE=skylake-avx512.
export DEBUG_LEVEL="${DEBUG_LEVEL:-0}"
export PORTABLE="${PORTABLE:-haswell}"
# Resolve script dir before cd: BASH_SOURCE may be a relative path.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SRC_DIR"
python3 "$SCRIPT_DIR/patch_db_bench_time.py" "$SRC_DIR/tools/db_bench_tool.cc"
echo "Building upstream rocksdb with PORTABLE=${PORTABLE} DEBUG_LEVEL=${DEBUG_LEVEL}"
make db_bench memtablerep_bench -j"$(nproc)" DEBUG_LEVEL="$DEBUG_LEVEL" PORTABLE="$PORTABLE"

test -x "$SRC_DIR/db_bench"
test -x "$SRC_DIR/memtablerep_bench"
cp -a "$SRC_DIR/db_bench" "$SRC_DIR/memtablerep_bench" "$DEST/bin/"

# Shared libs if present (static-linked benches may have none)
shopt -s nullglob
for f in "$SRC_DIR"/librocksdb.so*; do
  cp -a "$f" "$DEST/lib/"
done
shopt -u nullglob

SHA="$(git -C "$SRC_DIR" rev-parse HEAD)"
python3 - <<PY
import json
from pathlib import Path
meta = {
    "engine": "rocksdb",
    "ref": "${REF}",
    "git_sha": "${SHA}",
}
Path("${DEST}/engine-meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
PY

echo "Staged upstream rocksdb ${REF} (${SHA}) -> ${DEST}"
ldd "$DEST/bin/db_bench" || true

# Drop clone + object tree; only DEST (bins/libs) is needed for the artifact.
rm -rf "$SRC_DIR"
