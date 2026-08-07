#!/usr/bin/env bash
# Run ToplingDB db_bench under CPUQuota cgroup with a same-host dcompact_worker
# on remaining cores. Shared by local full-pipeline runs and CI (dcompact variant).
#
# Engines: zipkeyonly + zipkeyvalue (prebaked yaml; L6 differs).
#
# Usage:
#   PREFIX=/path/to/topling/install NUM=1000000 WRITE_BUFFER_SIZE=33554432 \
#     .github/scripts/run_dcompact_bench.sh
#
# Env:
#   PREFIX              Topling install prefix (bin/, lib/, toplingdb-conf/)
#   YAML_ZIPKEYONLY     Prebaked zipkeyonly yaml (default: PREFIX/.../dcompact_zipkeyonly.yaml)
#   YAML_ZIPKEYVALUE    Prebaked zipkeyvalue yaml (.../dcompact_zipkeyvalue.yaml)
#   NUM                 db_bench -num (default 1000000 local; CI overrides)
#   VALUE_SIZE          db_bench -value_size (default 50)
#   WRITE_BUFFER_SIZE   bytes; if set, rewrite yaml temp copy write_buffer_size
#   LOGDIR_BASE         Parent of per-engine log dirs (default: logs)
#   CPU_QUOTA           write-side db_bench systemd CPUQuota (default 50%)
#   WORKER_PORT         dcompact_worker listen port (default 8080)
#   MAX_PARALLEL_COMPACTIONS  (default 4)
#   CI                  Set to 1 for GitHub Actions (sudo systemd-run --uid=...)
#   SKIP_VERIFY         Set to 1 to skip post-run evidence checks
#   ENGINES             Space-separated: "zipkeyonly" and/or "zipkeyvalue"
#                       (default: both)
#
# Runtime yaml grafts (temp copy only):
#   max_background_compactions = max(1, min(13, ceil(nproc - db_cpu)))
#   worker_port / optional write_buffer_size
#   fillrandom: --prefix-level-writers 3 simple
#   fillseq:    --prefix-level-writers 6 zipkeyonly  (keeps L6)
set -euo pipefail

PREFIX="${PREFIX:?PREFIX (Topling install root) required}"
YAML_ZIPKEYONLY="${YAML_ZIPKEYONLY:-$PREFIX/toplingdb-conf/db_bench_enterprise_dcompact_zipkeyonly.yaml}"
YAML_ZIPKEYVALUE="${YAML_ZIPKEYVALUE:-$PREFIX/toplingdb-conf/db_bench_enterprise_dcompact_zipkeyvalue.yaml}"
NUM="${NUM:-1000000}"
VALUE_SIZE="${VALUE_SIZE:-50}"
LOGDIR_BASE="${LOGDIR_BASE:-logs}"
CPU_QUOTA="${CPU_QUOTA:-50%}"
# DB path must match yaml databases.*.path. hoster_root=/dev/shm — NEVER rm -rf hoster.
DB_PATH="${DB_PATH:-/dev/shm/db_bench_enterprise}"
WORKER_PORT="${WORKER_PORT:-8080}"
MAX_PARALLEL_COMPACTIONS="${MAX_PARALLEL_COMPACTIONS:-4}"
WORKER_DB_ROOT="${WORKER_DB_ROOT:-/tmp/dcompact-worker}"
NFS_MOUNT_ROOT="${NFS_MOUNT_ROOT:-/dev}"
ENGINES="${ENGINES:-zipkeyonly zipkeyvalue}"

test -x "$PREFIX/bin/db_bench"
test -x "$PREFIX/bin/dcompact_worker.exe" || test -x "$PREFIX/bin/dcompact_worker"
test -f "$YAML_ZIPKEYONLY"
test -f "$YAML_ZIPKEYVALUE"

# Path invariants (fail fast before starting worker); check zipkeyonly yaml.
python3 - "$YAML_ZIPKEYONLY" "$DB_PATH" "$NFS_MOUNT_ROOT" <<'PY'
import sys
from pathlib import Path
yaml_path, db_path, nfs_root = sys.argv[1], sys.argv[2], sys.argv[3]
text = Path(yaml_path).read_text(encoding="utf-8")
hoster = None
instance = None
for line in text.splitlines():
    s = line.strip()
    if s.startswith("hoster_root:"):
        hoster = s.split(":", 1)[1].strip().strip("'\"")
    elif s.startswith("instance_name:"):
        instance = s.split(":", 1)[1].strip().strip("'\"")
if not hoster or not instance:
    sys.exit(f"FAIL: missing hoster_root/instance_name in {yaml_path}")
if not (db_path.startswith(hoster + "/") and len(db_path) > len(hoster) + 1):
    sys.exit(f"FAIL: db_path={db_path!r} must be strict child of hoster_root={hoster!r}")
expected = nfs_root.rstrip("/") + "/" + instance
if hoster != expected:
    sys.exit(
        f"FAIL: hoster_root={hoster!r} != NFS_MOUNT_ROOT/instance={expected!r}"
    )
if Path(db_path).name != "db_bench_enterprise":
    sys.exit(f"FAIL: db path basename must be db_bench_enterprise, got {Path(db_path).name}")
print(f"path invariants OK: hoster={hoster} db={db_path} nfs={nfs_root}/{instance}")
PY

WORKER_BIN="$PREFIX/bin/dcompact_worker.exe"
if [[ ! -x "$WORKER_BIN" ]]; then
  WORKER_BIN="$PREFIX/bin/dcompact_worker"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/ensure_sample_statm_fdcache.sh" >/dev/null
SAMPLE_STATM="$SCRIPT_DIR/run_sample_statm_fdcache.sh"
chmod +x "$SAMPLE_STATM"
TMP_YAML_DIR="$(mktemp -d)"
WORKER_PID=""
WORKER_LOG="${LOGDIR_BASE}/dcompact_worker.log"
mkdir -p "$LOGDIR_BASE" "$WORKER_DB_ROOT"

NPROC="$(nproc)"
# db_cpu from CPU_QUOTA percent; MBC = max(1, min(13, ceil(nproc - db_cpu)))
MBC="$(python3 - "$CPU_QUOTA" "$NPROC" <<'PY'
import math, re, sys
q, nproc = sys.argv[1], int(sys.argv[2])
m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)%", q.strip())
if not m:
    sys.exit(f"FAIL: CPUQuota must look like 50%, got {q!r}")
db_cpu = float(m.group(1)) / 100.0
if nproc <= 0:
    sys.exit(f"FAIL: nproc must be > 0, got {nproc}")
if db_cpu >= nproc:
    sys.exit(f"FAIL: db_cpu={db_cpu} >= nproc={nproc}")
print(max(1, min(13, math.ceil(nproc - db_cpu))))
PY
)"
echo "graft mbc: nproc=${NPROC} cpu_quota=${CPU_QUOTA} max_background_compactions=${MBC}"

cleanup() {
  if [[ -n "${WORKER_PID}" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
    curl -fsS "http://127.0.0.1:${WORKER_PORT}/shutdown" >/dev/null 2>&1 || true
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP_YAML_DIR"
  # Safety: only remove DB path, never hoster_root (/dev/shm).
  rm -rf "$DB_PATH"
}
trap cleanup EXIT

export LD_LIBRARY_PATH="$PREFIX/lib:${LD_LIBRARY_PATH:-}"
export TOPLINGDB_GetContext_sampling="${TOPLINGDB_GetContext_sampling:-kNone}"
export ROCKSDB_KICK_OUT_OPTIONS_FILE="${ROCKSDB_KICK_OUT_OPTIONS_FILE:-1}"

make_yaml_for_engine() {
  local eng="$1"
  local src
  case "$eng" in
    zipkeyonly) src="$YAML_ZIPKEYONLY" ;;
    zipkeyvalue) src="$YAML_ZIPKEYVALUE" ;;
    *) echo "FAIL: unknown engine $eng (want zipkeyonly|zipkeyvalue)" >&2; exit 1 ;;
  esac
  local out="$TMP_YAML_DIR/${eng}.yaml"
  local graft_args=(
    --set-max-background-compactions "$MBC"
    --worker-port "$WORKER_PORT"
  )
  if [[ -n "${WRITE_BUFFER_SIZE:-}" ]]; then
    graft_args+=(--write-buffer-size-bytes "$WRITE_BUFFER_SIZE")
  fi
  python3 "$SCRIPT_DIR/graft_bench_yaml.py" "${graft_args[@]}" --out "$out" "$src"
  echo "$out"
}

start_worker() {
  # Same-host identity: hoster_root=/dev/shm, NFS_MOUNT_ROOT=/dev, instance=shm
  # => worker prefix == /dev/shm. NFS_DYNAMIC_MOUNT=0: no mount.
  export NFS_DYNAMIC_MOUNT=0
  export NFS_MOUNT_ROOT
  export WORKER_DB_ROOT
  export MAX_PARALLEL_COMPACTIONS
  export DictZipBlobStore_zipThreads="${DictZipBlobStore_zipThreads:-4}"

  mkdir -p "$(dirname "$WORKER_LOG")"
  : >"$WORKER_LOG"
  "$WORKER_BIN" -D "listening_ports=${WORKER_PORT}" \
    -D "document_root=${WORKER_DB_ROOT}" \
    >>"$WORKER_LOG" 2>&1 &
  WORKER_PID=$!

  local i
  for i in $(seq 1 60); do
    # Prefer /stat: some builds have no /probe (404). Do not use curl -f.
    if curl -sS -o /dev/null -w '%{http_code}' \
         "http://127.0.0.1:${WORKER_PORT}/stat?html=0" 2>/dev/null | grep -q '^200$'; then
      echo "dcompact_worker ready on :${WORKER_PORT} (pid=${WORKER_PID})"
      return 0
    fi
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
      echo "dcompact_worker exited early; log:" >&2
      cat "$WORKER_LOG" >&2 || true
      return 1
    fi
    sleep 0.5
  done
  echo "dcompact_worker did not become ready within 30s" >&2
  cat "$WORKER_LOG" >&2 || true
  return 1
}

# Explicit branches: local always --user; CI always sudo --uid (no probing).
run_under_cpu_quota() {
  local series="$1"
  local log="$2"
  local time_file="$3"
  shift 3
  # Absolute paths: systemd-run scope may not keep the caller's cwd.
  series="$(realpath -m "$series")"
  log="$(realpath -m "$log")"
  time_file="$(realpath -m "$time_file")"
  mkdir -p "$(dirname "$series")" "$(dirname "$log")" "$(dirname "$time_file")"
  # run_sample_statm_fdcache inside the scope so its child is db_bench
  # (no time/wrapper hop). Preserve CACHED_PAGES_USE_SYS for optional
  # drop_caches + SYS_CACHED_OF_EMPTY path.
  if [[ "${CI:-0}" == "1" ]]; then
    sudo --preserve-env=CACHED_PAGES_USE_SYS \
      systemd-run --scope --uid="$(id -u)" -p "CPUQuota=${CPU_QUOTA}" -- \
      "$SAMPLE_STATM" "$series" "$time_file" "$@" \
      >"$log" 2>&1
  else
    systemd-run --user --scope -p "CPUQuota=${CPU_QUOTA}" -- \
      "$SAMPLE_STATM" "$series" "$time_file" "$@" \
      >"$log" 2>&1
  fi
}

record_rss() {
  local logdir="$1"
  local label="$2"
  local time_file="${logdir}/time-${label}.txt"
  if [[ -f "$time_file" ]]; then
    local kb
    kb=$(grep -oP 'max_rss_kb=\K\d+' "$time_file")
    echo "max_rss_bytes=$((kb * 1024))" >"${logdir}/rss_usage-${label}.txt"
  fi
}

record_shm() {
  local logdir="$1"
  local label="$2"
  {
    echo "apparent_bytes=$(du -sb "$DB_PATH" | awk '{print $1}')"
    echo "allocated_bytes=$(du -s -B1 "$DB_PATH" | awk '{print $1}')"
  } | tee "${logdir}/shm_usage-${label}.txt"
  du -sh --apparent-size "$DB_PATH" || true
  du -sh "$DB_PATH" || true
  df -h /dev/shm
}

# Archive RocksDB INFO LOG before DB_PATH is deleted (per db_bench suite).
save_db_log() {
  local logdir="$1"
  local label="$2"
  test -f "$DB_PATH/LOG" || { echo "FAIL: missing $DB_PATH/LOG after suite=${label}" >&2; return 1; }
  cp -f "$DB_PATH/LOG" "${logdir}/LOG-${label}"
}

verify_dcompact_evidence() {
  local logdir="$1"
  local stat_json
  stat_json=$(curl -fsS "http://127.0.0.1:${WORKER_PORT}/stat?html=0&verbose=3")
  echo "$stat_json" | tee "${logdir}/dcompact_stat.json" >/dev/null
  local finished
  finished=$(python3 -c '
import json,sys
j=json.load(sys.stdin)
c=j.get("Vars",{}).get("Compactions",{})
print(int(c.get("finished",0) or 0))
' <<<"$stat_json")
  echo "dcompact_worker finished=${finished}"
  if [[ "${finished}" -le 0 ]]; then
    if ! grep -Eiq 'dcompact|/dcompact|job-' "$WORKER_LOG"; then
      echo "FAIL: worker finished=0 and no /dcompact activity in ${WORKER_LOG}" >&2
      return 1
    fi
  fi
  echo "dcompact evidence OK (finished=${finished})"
}

prepare_db() {
  rm -rf "$DB_PATH"
  mkdir -p "$DB_PATH"
  if [[ -d "$PREFIX/site" ]]; then
    cp -a "$PREFIX/site/." "$DB_PATH/"
  fi
}

run_engine_suite() {
  local eng="$1"
  local yaml
  yaml="$(make_yaml_for_engine "$eng")"
  local logdir="${LOGDIR_BASE}/${eng}"
  mkdir -p "$logdir"
  cp -a "$yaml" "${logdir}/db_bench.yaml"
  # Share one worker log pointer for evidence; also keep a per-engine copy link.
  ln -sfn "$(realpath "$WORKER_LOG")" "${logdir}/dcompact_worker.log" 2>/dev/null || true

  echo "=== engine=${eng} NUM=${NUM} CPU_QUOTA=${CPU_QUOTA} yaml=${yaml} ==="

  prepare_db
  local yaml_fr="${logdir}/db_bench-fillrandom.yaml"
  python3 "$SCRIPT_DIR/graft_bench_yaml.py" \
    --prefix-level-writers 3 simple \
    --out "$yaml_fr" \
    "$yaml"
  local args_fr=(
    -json "$yaml_fr"
    -num="$NUM"
    -key_size=8
    -value_size="$VALUE_SIZE"
    -batch_size=1000
    -benchmarks=fillrandom,flush,compact,readseq,readseq,readseq,readrandom
    -enable_zero_copy
    -progress_reports=false
    -report_bench_start_time
    -compact_target_level=6
  )
  run_under_cpu_quota \
    "${logdir}/statm_series-fillrandom.txt" \
    "${logdir}/db_bench-fillrandom.log" \
    "${logdir}/time-fillrandom.txt" \
    "$PREFIX/bin/db_bench" "${args_fr[@]}"
  cat "${logdir}/db_bench-fillrandom.log"
  record_rss "$logdir" fillrandom
  record_shm "$logdir" fillrandom
  save_db_log "$logdir" fillrandom
  rm -rf "$DB_PATH"

  prepare_db
  local yaml_fs="${logdir}/db_bench-fillseq.yaml"
  python3 "$SCRIPT_DIR/graft_bench_yaml.py" \
    --prefix-level-writers 6 zipkeyonly \
    --target-file-size-base 128M \
    --target-file-size-multiplier 1 \
    --out "$yaml_fs" \
    "$yaml"
  local args_fs=(
    -json "$yaml_fs"
    -num="$NUM"
    -key_size=8
    -value_size="$VALUE_SIZE"
    -batch_size=1000
    -benchmarks=fillseq,flush,compact,readseq,readseq,readseq,readrandom
    -enable_zero_copy
    -progress_reports=false
    -report_bench_start_time
    -compact_target_level=6
  )
  run_under_cpu_quota \
    "${logdir}/statm_series-fillseq.txt" \
    "${logdir}/db_bench.log" \
    "${logdir}/time-fillseq.txt" \
    "$PREFIX/bin/db_bench" "${args_fs[@]}"
  cat "${logdir}/db_bench.log"
  record_rss "$logdir" fillseq
  record_shm "$logdir" fillseq
  save_db_log "$logdir" fillseq

  if [[ "${SKIP_VERIFY:-0}" != "1" ]]; then
    verify_dcompact_evidence "$logdir"
  fi

  for f in "${logdir}"/db_bench*.log; do
    test -s "$f" || { echo "empty bench log: $f" >&2; exit 1; }
    grep -Eiq 'ops/sec|us/op|micros/op' "$f" \
      || { echo "bench log lacks metrics: $f" >&2; exit 1; }
  done

  # Only remove DB path — never hoster_root (/dev/shm).
  rm -rf "$DB_PATH"
}

start_worker

for eng in $ENGINES; do
  run_engine_suite "$eng"
done

echo "run_dcompact_bench.sh OK (NUM=${NUM} VALUE_SIZE=${VALUE_SIZE} CPU_QUOTA=${CPU_QUOTA} ENGINES=${ENGINES})"
