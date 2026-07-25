#!/usr/bin/env bash
# Run upstream RocksDB db_bench under CPUQuota with CompactionService broker
# outside the cgroup (spool IPC). Used by dcompact CI variant Phase 2.
#
# Env:
#   PREFIX       rocksdb engine prefix (bin/, lib/)
#   LOGDIR       log directory
#   NUM          db_bench -num
#   CPU_QUOTA    systemd CPUQuota (default 25%)
#   CACHE_SIZE   -cache_size bytes
#   WRITE_BUFFER_SIZE  bytes (default 67108864)
#   DB_PATH      default /dev/shm/db_bench_rocksdb_cs
#   SPOOL_DIR    default /tmp/rocksdb-cs-spool
#   CI           1 = sudo systemd-run --uid=...
set -euo pipefail

PREFIX="${PREFIX:?PREFIX required}"
LOGDIR="${LOGDIR:?LOGDIR required}"
NUM="${NUM:?NUM required}"
CPU_QUOTA="${CPU_QUOTA:-25%}"
CACHE_SIZE="${CACHE_SIZE:?CACHE_SIZE required}"
WRITE_BUFFER_SIZE="${WRITE_BUFFER_SIZE:-67108864}"
DB_PATH="${DB_PATH:-/dev/shm/db_bench_rocksdb_cs}"
SPOOL_DIR="${SPOOL_DIR:-/tmp/rocksdb-cs-spool}"

test -x "$PREFIX/bin/db_bench"
test -x "$PREFIX/bin/remote_compact_broker"
test -x "$PREFIX/bin/remote_compact_worker"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$LOGDIR"
export LD_LIBRARY_PATH="$PREFIX/lib:${LD_LIBRARY_PATH:-}"

BROKER_PID=""
cleanup() {
  if [[ -n "${BROKER_PID}" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$DB_PATH"
}
trap cleanup EXIT

rm -rf "$SPOOL_DIR" "$DB_PATH"
mkdir -p "$SPOOL_DIR" "$DB_PATH"

# Broker MUST start outside the write-side cgroup so worker forks use remaining cores.
: >"${LOGDIR}/remote_compact_broker.log"
"$PREFIX/bin/remote_compact_broker" "$SPOOL_DIR" "$PREFIX/bin/remote_compact_worker" \
  >>"${LOGDIR}/remote_compact_broker.log" 2>&1 &
BROKER_PID=$!
sleep 0.5
if ! kill -0 "$BROKER_PID" 2>/dev/null; then
  echo "FAIL: remote_compact_broker exited early" >&2
  cat "${LOGDIR}/remote_compact_broker.log" >&2 || true
  exit 1
fi
echo "remote_compact_broker ready pid=${BROKER_PID} spool=${SPOOL_DIR}"

run_under_cpu_quota() {
  local series="$1" log="$2" time_file="$3"
  shift 3
  if [[ "${CI:-0}" == "1" ]]; then
    python3 "$SCRIPT_DIR/sample_rss.py" \
      --series "$series" --log "$log" -- \
      /usr/bin/time -f 'max_rss_kb=%M' -o "$time_file" -- \
      sudo systemd-run --scope --uid="$(id -u)" -p "CPUQuota=${CPU_QUOTA}" -- "$@"
  else
    python3 "$SCRIPT_DIR/sample_rss.py" \
      --series "$series" --log "$log" -- \
      /usr/bin/time -f 'max_rss_kb=%M' -o "$time_file" -- \
      systemd-run --user --scope -p "CPUQuota=${CPU_QUOTA}" -- "$@"
  fi
}

record_rss() {
  local label="$1"
  local time_file="${LOGDIR}/time-${label}.txt"
  if [[ -f "$time_file" ]]; then
    local kb
    kb=$(grep -oP 'max_rss_kb=\K\d+' "$time_file")
    echo "max_rss_bytes=$((kb * 1024))" >"${LOGDIR}/rss_usage-${label}.txt"
  fi
}

record_shm() {
  local label="$1"
  {
    echo "apparent_bytes=$(du -sb "$DB_PATH" | awk '{print $1}')"
    echo "allocated_bytes=$(du -s -B1 "$DB_PATH" | awk '{print $1}')"
  } | tee "${LOGDIR}/shm_usage-${label}.txt"
}

echo "cache_size_bytes=$CACHE_SIZE" >"${LOGDIR}/bench_settings.txt"
echo "cpu_quota=$CPU_QUOTA" >>"${LOGDIR}/bench_settings.txt"
echo "compact_mode=compaction_service_spool" >>"${LOGDIR}/bench_settings.txt"
echo "write_buffer_size=$WRITE_BUFFER_SIZE" >>"${LOGDIR}/bench_settings.txt"
echo "spool_dir=$SPOOL_DIR" >>"${LOGDIR}/bench_settings.txt"

run_suite() {
  local suite="$1"
  local log_name="$2"
  rm -rf "$DB_PATH"
  mkdir -p "$DB_PATH"
  local args=(
    -db="$DB_PATH" -num="$NUM" -key_size=8 -value_size=15 -batch_size=1000
    -cache_size="$CACHE_SIZE"
    -write_buffer_size="$WRITE_BUFFER_SIZE"
    -compaction_spool_dir="$SPOOL_DIR"
    -benchmarks="${suite},flush,compact,readseq,readseq,readseq,readrandom,readrandom,readrandom"
    -progress_reports=false -report_bench_start_time
  )
  run_under_cpu_quota \
    "${LOGDIR}/rss_series-${suite}.txt" \
    "${LOGDIR}/${log_name}" \
    "${LOGDIR}/time-${suite}.txt" \
    "$PREFIX/bin/db_bench" "${args[@]}"
  cat "${LOGDIR}/${log_name}"
  record_rss "$suite"
  record_shm "$suite"
}

run_suite fillrandom db_bench-fillrandom.log
rm -rf "$DB_PATH"
run_suite fillseq db_bench.log

# Evidence: at least one DONE job in spool
done_count=$(find "$SPOOL_DIR" -name state -print0 2>/dev/null \
  | xargs -0 grep -l '^DONE$' 2>/dev/null | wc -l || echo 0)
echo "compaction_service done_jobs=${done_count}"
echo "done_jobs=${done_count}" >"${LOGDIR}/compaction_service_stat.txt"
if [[ "${done_count}" -le 0 ]]; then
  echo "FAIL: no DONE compaction jobs in ${SPOOL_DIR}" >&2
  find "$SPOOL_DIR" -type f | head -50 >&2 || true
  exit 1
fi

for f in "${LOGDIR}"/db_bench*.log; do
  test -s "$f"
  grep -Eiq 'ops/sec|us/op|micros/op' "$f"
  grep -Eiq 'CompactionService spool' "$f" \
    || { echo "FAIL: missing CompactionService spool banner in $f" >&2; exit 1; }
done

echo "run_rocksdb_cs_bench.sh OK (NUM=${NUM} CPU_QUOTA=${CPU_QUOTA} done_jobs=${done_count})"
