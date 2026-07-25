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
mkdir -p "$DEST/bin" "$DEST/lib"

# Resolve clone ref: upstream default branch is main; keep "master"/"latest" aliases.
CLONE_REF="$REF"
if [[ "$REF" == "master" || "$REF" == "latest" ]]; then
  CLONE_REF="main"
fi

# Local: prefer git worktree from this repo's `facebook` remote (objects already
# present). CI / fresh checkout: fall back to git clone (no facebook objects).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
USED_WORKTREE=0
if git -C "$REPO_ROOT" remote get-url facebook >/dev/null 2>&1; then
  echo "Using local facebook remote worktree @ ${CLONE_REF} -> ${SRC_DIR}"
  # Ensure ref is available locally; shallow fetch the tip if needed.
  if ! git -C "$REPO_ROOT" rev-parse --verify "facebook/${CLONE_REF}" >/dev/null 2>&1; then
    # Also try REF as-is for tags like v8.10.2
    if ! git -C "$REPO_ROOT" rev-parse --verify "facebook/${REF}" >/dev/null 2>&1 \
      && ! git -C "$REPO_ROOT" rev-parse --verify "${REF}" >/dev/null 2>&1; then
      git -C "$REPO_ROOT" fetch --depth 1 facebook "$CLONE_REF" \
        || git -C "$REPO_ROOT" fetch --depth 1 facebook "refs/tags/${REF}:refs/tags/${REF}" \
        || true
    fi
  fi
  WT_REF=""
  if git -C "$REPO_ROOT" rev-parse --verify "facebook/${CLONE_REF}" >/dev/null 2>&1; then
    WT_REF="facebook/${CLONE_REF}"
  elif git -C "$REPO_ROOT" rev-parse --verify "facebook/${REF}" >/dev/null 2>&1; then
    WT_REF="facebook/${REF}"
  elif git -C "$REPO_ROOT" rev-parse --verify "refs/tags/${REF}" >/dev/null 2>&1; then
    WT_REF="${REF}"
  elif git -C "$REPO_ROOT" rev-parse --verify "${REF}" >/dev/null 2>&1; then
    WT_REF="${REF}"
  fi
  if [[ -n "$WT_REF" ]]; then
    # Drop stale worktree registrations left by interrupted runs.
    git -C "$REPO_ROOT" worktree prune 2>/dev/null || true
    rm -rf "$SRC_DIR"
    if git -C "$REPO_ROOT" worktree add --detach "$SRC_DIR" "$WT_REF"; then
      USED_WORKTREE=1
    else
      echo "WARN: worktree add failed; falling back to git clone" >&2
    fi
  fi
fi

if [[ "$USED_WORKTREE" != "1" ]]; then
  mkdir -p "$SRC_DIR"
  echo "Cloning facebook/rocksdb @ ${CLONE_REF} -> ${SRC_DIR}"
  if [[ "$REF" == "master" || "$REF" == "latest" ]]; then
    if ! git clone --depth 1 --branch main https://github.com/facebook/rocksdb.git "$SRC_DIR"; then
      git clone --depth 1 --branch master https://github.com/facebook/rocksdb.git "$SRC_DIR"
    fi
  else
    git clone --depth 1 --branch "$REF" https://github.com/facebook/rocksdb.git "$SRC_DIR"
  fi
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
BUILD_TARGETS=(db_bench memtablerep_bench)
if [[ "${PATCH_COMPACTION_SERVICE:-0}" == "1" ]]; then
  python3 "$SCRIPT_DIR/patch_db_bench_compaction_service.py" "$SRC_DIR" "$REF"
  BUILD_TARGETS+=(remote_compact_broker remote_compact_worker)
fi
echo "Building upstream rocksdb with PORTABLE=${PORTABLE} DEBUG_LEVEL=${DEBUG_LEVEL}"
make "${BUILD_TARGETS[@]}" -j"$(nproc)" DEBUG_LEVEL="$DEBUG_LEVEL" PORTABLE="$PORTABLE"

test -x "$SRC_DIR/db_bench"
test -x "$SRC_DIR/memtablerep_bench"
cp -a "$SRC_DIR/db_bench" "$SRC_DIR/memtablerep_bench" "$DEST/bin/"
if [[ "${PATCH_COMPACTION_SERVICE:-0}" == "1" ]]; then
  test -x "$SRC_DIR/remote_compact_broker"
  test -x "$SRC_DIR/remote_compact_worker"
  cp -a "$SRC_DIR/remote_compact_broker" "$SRC_DIR/remote_compact_worker" "$DEST/bin/"
fi

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

# Drop source tree; only DEST (bins/libs) is needed for the artifact.
if [[ "$USED_WORKTREE" == "1" ]]; then
  git -C "$REPO_ROOT" worktree remove --force "$SRC_DIR" 2>/dev/null \
    || rm -rf "$SRC_DIR"
else
  rm -rf "$SRC_DIR"
fi
