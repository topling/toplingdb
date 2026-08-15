#!/usr/bin/env bash
# Run upstream RocksDB db_bench under CPUQuota with CompactionService broker
# outside the cgroup (spool IPC). Used by dcompact CI variant Phase 2.
#
# Env:
#   PREFIX       rocksdb engine prefix (bin/, lib/)
#   LOGDIR       log directory
#   NUM          db_bench -num
#   VALUE_SIZE   db_bench -value_size (default 50)
#   CPU_QUOTA    systemd CPUQuota (default 50%)
#   CACHE_SIZE   -cache_size bytes
#   WRITE_BUFFER_SIZE  bytes (default 67108864)
#   DB_PATH      default /dev/shm/db_bench_rocksdb_cs
#   SPOOL_DIR    default /dev/shm/rocksdb-cs-spool (must share FS with DB_PATH;
#                Install uses rename — EXDEV if spool is on /tmp and DB on /dev/shm)
#   CI           1 = sudo systemd-run --uid=...
set -euo pipefail

PREFIX="${PREFIX:?PREFIX required}"
LOGDIR="${LOGDIR:?LOGDIR required}"
NUM="${NUM:?NUM required}"
VALUE_SIZE="${VALUE_SIZE:-50}"
CPU_QUOTA="${CPU_QUOTA:-50%}"
CACHE_SIZE="${CACHE_SIZE:?CACHE_SIZE required}"
WRITE_BUFFER_SIZE="${WRITE_BUFFER_SIZE:-67108864}"
DB_PATH="${DB_PATH:-/dev/shm/db_bench_rocksdb_cs}"
SPOOL_DIR="${SPOOL_DIR:-/dev/shm/rocksdb-cs-spool}"

test -x "$PREFIX/bin/db_bench"
test -x "$PREFIX/bin/remote_compact_broker"
test -x "$PREFIX/bin/remote_compact_worker"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/ensure_sample_statm_fdcache.sh" >/dev/null
SAMPLE_STATM="$SCRIPT_DIR/run_sample_statm_fdcache.sh"
chmod +x "$SAMPLE_STATM"
mkdir -p "$LOGDIR"
export LD_LIBRARY_PATH="$PREFIX/lib:${LD_LIBRARY_PATH:-}"

BROKER_PID=""
cleanup() {
  local ec=$?
  if [[ "$ec" -ne 0 ]]; then
    echo "FAIL: run_rocksdb_cs_bench.sh exiting ec=${ec}; dumping diagnostics" >&2
    ls -la "$LOGDIR" >&2 || true
    for f in "$LOGDIR"/db_bench*.log "$LOGDIR"/remote_compact_broker.log \
             "$LOGDIR"/time-*.txt "$LOGDIR"/compaction_service_stat.txt; do
      if [[ -f "$f" ]]; then
        echo "----- ${f} -----" >&2
        cat "$f" >&2 || true
      fi
    done
    echo "----- spool ${SPOOL_DIR} -----" >&2
    find "$SPOOL_DIR" -type f -print 2>/dev/null | head -80 >&2 || true
    for f in "$SPOOL_DIR"/*/*/state "$SPOOL_DIR"/*/*/error.txt; do
      if [[ -f "$f" ]]; then
        echo "----- ${f} -----" >&2
        cat "$f" >&2 || true
      fi
    done
  fi
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
  # Absolute paths: systemd-run scope may not keep the caller's cwd.
  series="$(realpath -m "$series")"
  log="$(realpath -m "$log")"
  time_file="$(realpath -m "$time_file")"
  mkdir -p "$(dirname "$series")" "$(dirname "$log")" "$(dirname "$time_file")"
  # run_sample_statm_fdcache inside the scope so its child is db_bench;
  # stdbuf line-buffers log. Preserve CACHED_PAGES_USE_SYS for optional
  # drop_caches + SYS_CACHED_OF_EMPTY path.
  if [[ "${CI:-0}" == "1" ]]; then
    sudo --preserve-env=CACHED_PAGES_USE_SYS \
      systemd-run --scope --uid="$(id -u)" -p "CPUQuota=${CPU_QUOTA}" -- \
      "$SAMPLE_STATM" "$series" "$time_file" \
      stdbuf -oL -eL "$@" \
      >"$log" 2>&1
  else
    systemd-run --user --scope -p "CPUQuota=${CPU_QUOTA}" -- \
      "$SAMPLE_STATM" "$series" "$time_file" \
      stdbuf -oL -eL "$@" \
      >"$log" 2>&1
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

save_db_log() {
  local label="$1"
  test -f "$DB_PATH/LOG" || { echo "FAIL: missing $DB_PATH/LOG after suite=${label}" >&2; return 1; }
  cp -f "$DB_PATH/LOG" "${LOGDIR}/LOG-${label}"
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
    -db="$DB_PATH" -num="$NUM" -key_size=8 -value_size="$VALUE_SIZE" -batch_size=1000
    -cache_size="$CACHE_SIZE"
    -write_buffer_size="$WRITE_BUFFER_SIZE"
    -compaction_spool_dir="$SPOOL_DIR"
    -benchmarks="${suite},flush,compact,readseq,readseq,readseq,readrandom"
    -progress_reports=false -report_bench_start_time
    -compact_target_level=6
  )
  echo "=== starting suite=${suite} NUM=${NUM} ==="
  set +e
  run_under_cpu_quota \
    "${LOGDIR}/statm_series-${suite}.txt" \
    "${LOGDIR}/${log_name}" \
    "${LOGDIR}/time-${suite}.txt" \
    "$PREFIX/bin/db_bench" "${args[@]}"
  local rc=$?
  set -e
  echo "=== suite=${suite} rc=${rc} log=${LOGDIR}/${log_name} ==="
  if [[ -f "${LOGDIR}/${log_name}" ]]; then
    cat "${LOGDIR}/${log_name}"
  else
    echo "FAIL: missing bench log ${LOGDIR}/${log_name}" >&2
  fi
  if [[ "$rc" -ne 0 ]]; then
    echo "FAIL: db_bench suite=${suite} exited ${rc}" >&2
    return "$rc"
  fi
  record_rss "$suite"
  record_shm "$suite"
  save_db_log "$suite"
}

run_suite fillrandom db_bench-fillrandom.log
rm -rf "$DB_PATH"
run_suite fillseq db_bench.log

# Evidence: at least one DONE job in spool (avoid xargs+grep pipefail pitfalls)
done_count=0
while IFS= read -r -d '' st; do
  if [[ "$(cat "$st" 2>/dev/null || true)" == "DONE" ]]; then
    done_count=$((done_count + 1))
  fi
done < <(find "$SPOOL_DIR" -name state -print0 2>/dev/null || true)
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
