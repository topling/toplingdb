#!/usr/bin/env bash
# Local Topling-only score pages, same pipeline as db_bench-run.yml:
#   logs -> bench_logs_to_pages.py emit -> merge -> site/index.html
#
# RocksDB engines are intentionally skipped (too slow locally).
# Score page still uses the workflow HTML generator; RocksDB columns stay empty.
#
# Usage (from repo root):
#   NUM=20000000 .github/scripts/run_local_simple_top_pages.sh prepare-yaml
#   NUM=20000000 .github/scripts/run_local_simple_top_pages.sh run-topling
#   NUM=20000000 .github/scripts/run_local_simple_top_pages.sh run-dictzip10
#   .github/scripts/run_local_simple_top_pages.sh emit
#   NUM=20000000 .github/scripts/run_local_simple_top_pages.sh all
#
# Convenience wrapper: _local_simple_top/gen_pages.sh -> this script.
#
# Env:
#   NUM=20000000          key count (CI uses 100000000; local shm often needs smaller)
#   VALUE_SIZE=50         db_bench -value_size (align with db_bench-run.yml)
#   RUN_MEMTABLE=0|1      run memtablerep_bench after topling suite (default 0)
#   RUN_ID=local-simpletop
# Sampler: sample_statm_fdcache (same as CI db_bench-run).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOCAL="$ROOT/_local_simple_top"
LOGROOT="$LOCAL/logs"
YAML_BASE="$LOCAL/toplingdb-conf/db_bench_enterprise.yaml"
YAML_DZ10="$LOCAL/toplingdb-conf/db_bench_enterprise_dictzip10.yaml"
DB_PATH=/dev/shm/db_bench_enterprise
PAGES_EMIT="$LOCAL/_pages"
PAGES_SITE="$LOCAL/site_pages"
NUM="${NUM:-20000000}"
VALUE_SIZE="${VALUE_SIZE:-50}"
RUN_MEMTABLE="${RUN_MEMTABLE:-0}"
RUN_ID="${RUN_ID:-local-simpletop}"

export LD_LIBRARY_PATH="$ROOT:${LD_LIBRARY_PATH:-}"
export TOPLINGDB_GetContext_sampling=kNone
export ROCKSDB_KICK_OUT_OPTIONS_FILE=1

usage() {
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
  echo "usage: $0 {prepare-yaml|run-topling|run-dictzip10|emit|all}" >&2
}

require_db_bench() {
  test -x "$ROOT/db_bench" || {
    echo "missing $ROOT/db_bench; build with:" >&2
    echo "  make db_bench -j\$(nproc) DEBUG_LEVEL=0 UPDATE_REPO=0" >&2
    exit 1
  }
}

prepare_yaml() {
  test -f "$YAML_BASE" || {
    echo "missing $YAML_BASE" >&2
    exit 1
  }
  mkdir -p "$(dirname "$YAML_DZ10")"
  cp -a "$YAML_BASE" "$YAML_DZ10"
  # Align with db_bench-run.yml: enable DictZip for small CI values (default 50).
  sed -i 's/^\([[:space:]]*minDictZipValueSize:[[:space:]]*\)3000/\110/' "$YAML_DZ10"
  grep -E 'minDictZipValueSize:[[:space:]]*10$' "$YAML_DZ10"
  grep -E 'class: SimpleTopTable' "$YAML_BASE"
  grep -E 'level_writers: \[fast,' "$YAML_BASE" | head -1
  echo "prepared $YAML_DZ10"
}

prepare_db() {
  rm -rf "$DB_PATH"
  mkdir -p "$DB_PATH"
  if [ -d "$LOCAL/site" ]; then
    cp -a "$LOCAL/site/." "$DB_PATH/"
  fi
}

record_shm() {
  local logdir="$1" label="$2"
  {
    echo "apparent_bytes=$(du -sb "$DB_PATH" | awk '{print $1}')"
    echo "allocated_bytes=$(du -s -B1 "$DB_PATH" | awk '{print $1}')"
  } | tee "${logdir}/shm_usage-${label}.txt"
}

record_rss() {
  local logdir="$1" label="$2"
  local time_file="${logdir}/time-${label}.txt"
  if [ -f "$time_file" ]; then
    local kb
    kb=$(grep -oP 'max_rss_kb=\K\d+' "$time_file")
    echo "max_rss_bytes=$((kb * 1024))" > "${logdir}/rss_usage-${label}.txt"
  fi
}

save_db_log() {
  local logdir="$1" label="$2"
  test -f "$DB_PATH/LOG" || {
    echo "FAIL: missing $DB_PATH/LOG after ${label}" >&2
    return 1
  }
  cp -f "$DB_PATH/LOG" "${logdir}/LOG-${label}"
}

backfill_rss_from_time() {
  local logdir="$1"
  local label
  for label in fillrandom fillrandom-omit fillseq fillseq-omit; do
    if [ -f "${logdir}/time-${label}.txt" ] && [ ! -f "${logdir}/rss_usage-${label}.txt" ]; then
      record_rss "$logdir" "$label"
    fi
  done
}

run_memtablerep_if_requested() {
  local logdir="$1"
  if [ "$RUN_MEMTABLE" != "1" ]; then
    echo "skip memtablerep_bench (RUN_MEMTABLE=$RUN_MEMTABLE)"
    return 0
  fi
  if [ ! -x "$ROOT/memtablerep_bench" ]; then
    echo "WARN: memtablerep_bench missing, skip" >&2
    return 0
  fi
  local mt=(
    -benchmarks=fillrandom,readrandom
    -item_size=0
    -num_operations="$NUM"
    -write_buffer_size=536870912
  )
  "$ROOT/memtablerep_bench" "${mt[@]}" -memtablerep=skiplist \
    2>&1 | tee "${logdir}/memtablerep_bench-skiplist.log"
  "$ROOT/memtablerep_bench" "${mt[@]}" \
    -memtablerep='cspp:{"mem_cap":"16G","use_hugepage":false}' \
    2>&1 | tee "${logdir}/memtablerep_bench-cspp.log"
}

# Mirror db_bench-run.yml run_topling_suite (no RocksDB).
run_topling_suite() {
  local eng_id="$1"
  local yaml="$2"
  local want_memtable="$3"
  local logdir="$LOGROOT/${eng_id}"
  mkdir -p "$logdir"
  require_db_bench
  test -f "$yaml"

  # Pass 1: fillrandom + omit
  prepare_db
  local args_fr=(
    -json "$yaml"
    -num="$NUM"
    -key_size=8
    -value_size="$VALUE_SIZE"
    -batch_size=1000
    -benchmarks=fillrandom,flush,compact,readseq,readseq,readseq,readrandom
    -enable_zero_copy
    -progress_reports=false
    -report_bench_start_time
  )
  "$ROOT/.github/scripts/ensure_sample_statm_fdcache.sh" >/dev/null
  "$ROOT/.github/scripts/sample_statm_fdcache" \
    "${logdir}/statm_series-fillrandom.txt" "${logdir}/time-fillrandom.txt" \
    "$ROOT/db_bench" "${args_fr[@]}" \
    >"${logdir}/db_bench-fillrandom.log" 2>&1
  save_db_log "$logdir" fillrandom

  local args_omit=(
    -json "$yaml"
    -num="$NUM"
    -key_size=8
    -value_size="$VALUE_SIZE"
    -batch_size=1000
    -benchmarks=nextwithkey,nextwithkey,nextwithkey,readseq,readseq,readseq
    -scan_omit_key -scan_omit_value
    -use_existing_db=1
    -enable_zero_copy
    -progress_reports=false
  )
  /usr/bin/time -f 'max_rss_kb=%M' -o "${logdir}/time-fillrandom-omit.txt" -- \
    "$ROOT/db_bench" "${args_omit[@]}" >"${logdir}/db_bench-fillrandom-omit.log" 2>&1
  save_db_log "$logdir" fillrandom-omit
  record_rss "$logdir" fillrandom
  record_rss "$logdir" fillrandom-omit
  record_shm "$logdir" fillrandom
  rm -rf "$DB_PATH"

  # Pass 2: fillseq + omit  (workflow names this db_bench.log)
  prepare_db
  local args_fs=(
    -json "$yaml"
    -num="$NUM"
    -key_size=8
    -value_size="$VALUE_SIZE"
    -batch_size=1000
    -benchmarks=fillseq,flush,compact,readseq,readseq,readseq,readrandom
    -enable_zero_copy
    -progress_reports=false
    -report_bench_start_time
  )
  "$ROOT/.github/scripts/sample_statm_fdcache" \
    "${logdir}/statm_series-fillseq.txt" "${logdir}/time-fillseq.txt" \
    "$ROOT/db_bench" "${args_fs[@]}" \
    >"${logdir}/db_bench.log" 2>&1
  # Keep an explicit alias for humans.
  cp -f "${logdir}/db_bench.log" "${logdir}/db_bench-fillseq.log"
  save_db_log "$logdir" fillseq
  /usr/bin/time -f 'max_rss_kb=%M' -o "${logdir}/time-fillseq-omit.txt" -- \
    "$ROOT/db_bench" "${args_omit[@]}" >"${logdir}/db_bench-fillseq-omit.log" 2>&1
  save_db_log "$logdir" fillseq-omit
  record_rss "$logdir" fillseq
  record_rss "$logdir" fillseq-omit
  record_shm "$logdir" fillseq

  if [ "$want_memtable" = "1" ]; then
    run_memtablerep_if_requested "$logdir"
  fi
  rm -rf "$DB_PATH"

  local f
  for f in \
    "${logdir}/db_bench-fillrandom.log" \
    "${logdir}/db_bench-fillrandom-omit.log" \
    "${logdir}/db_bench.log" \
    "${logdir}/db_bench-fillseq-omit.log"
  do
    test -s "$f" || {
      echo "empty bench log: $f" >&2
      exit 1
    }
    grep -Eiq 'ops/sec|us/op|micros/op|Elapsed time' "$f" || {
      echo "bench log lacks metrics: $f" >&2
      exit 1
    }
  done
  echo "suite ok: $eng_id -> $logdir"
}

write_runner_env() {
  local mem_total_kb cpu_model os_pretty
  mkdir -p "$LOGROOT"
  mem_total_kb=$(grep MemTotal /proc/meminfo | awk '{print $2}')
  os_pretty=$(grep -oP 'PRETTY_NAME="\K[^"]+' /etc/os-release 2>/dev/null || echo unknown)
  cpu_model=$(lscpu 2>/dev/null | grep -oP 'Model name:\s+\K.*' || echo unknown)
  {
    echo "os_pretty_name=${os_pretty}"
    echo "kernel=$(uname -r)"
    echo "cpu_model=${cpu_model}"
    echo "cpu_count=$(nproc)"
    echo "mem_total_bytes=$((mem_total_kb * 1024))"
    echo "shm_size_bytes=$(df -B1 /dev/shm | awk 'NR==2{print $2}')"
  } | tee "$LOGROOT/runner_env.txt"
}

normalize_logs_for_emit() {
  local eng
  for eng in topling topling-dictzip10; do
    local logdir="$LOGROOT/$eng"
    mkdir -p "$logdir"
    if [ -f "$logdir/db_bench-fillseq.log" ] && [ ! -f "$logdir/db_bench.log" ]; then
      cp -a "$logdir/db_bench-fillseq.log" "$logdir/db_bench.log"
    fi
    backfill_rss_from_time "$logdir"
  done
}

emit_pages() {
  normalize_logs_for_emit
  write_runner_env

  test -f "$LOGROOT/topling/db_bench.log" || {
    echo "missing $LOGROOT/topling/db_bench.log (fillseq suite)" >&2
    exit 1
  }
  test -f "$LOGROOT/topling-dictzip10/db_bench.log" || {
    echo "missing $LOGROOT/topling-dictzip10/db_bench.log; run: $0 run-dictzip10" >&2
    exit 1
  }

  rm -rf "$PAGES_EMIT"
  mkdir -p "$PAGES_EMIT"
  python3 "$ROOT/.github/scripts/bench_logs_to_pages.py" emit \
    --variant plain \
    --run-id "$RUN_ID" \
    --log-root "$LOGROOT" \
    --actions-run-url "file://${LOGROOT}" \
    --out "$PAGES_EMIT"

  # Same as workflow "Merge into gh-pages tree", but local site_pages/.
  mkdir -p "$PAGES_SITE"
  python3 "$ROOT/.github/scripts/bench_logs_to_pages.py" merge \
    --merge-into "$PAGES_SITE" \
    --from "$PAGES_EMIT" \
    --variant plain

  local run_html
  run_html=$(python3 - <<PY
from pathlib import Path
import json
meta = json.loads(Path("$PAGES_EMIT/run-meta.json").read_text())
p = Path("$PAGES_SITE") / "runs" / meta["run_dir"] / "index.html"
print(p.resolve())
PY
)
  echo "SCORE_PAGE_HOME=$PAGES_SITE/index.html"
  echo "SCORE_PAGE_RUN=$run_html"
}

mode="${1:-}"
case "$mode" in
  prepare-yaml)
    prepare_yaml
    ;;
  run-topling)
    prepare_yaml
    run_topling_suite topling "$YAML_BASE" 1
    ;;
  run-dictzip10)
    prepare_yaml
    run_topling_suite topling-dictzip10 "$YAML_DZ10" 0
    ;;
  emit)
    emit_pages
    ;;
  all)
    prepare_yaml
    run_topling_suite topling "$YAML_BASE" 1
    run_topling_suite topling-dictzip10 "$YAML_DZ10" 0
    emit_pages
    ;;
  ""|-h|--help|help)
    usage
    exit 2
    ;;
  *)
    echo "unknown mode: $mode" >&2
    usage
    exit 2
    ;;
esac

echo "DONE mode=$mode"
