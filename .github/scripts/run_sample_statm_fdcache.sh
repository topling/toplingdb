#!/usr/bin/env bash
# Wrapper around sample_statm_fdcache.
#
# If CACHED_PAGES_USE_SYS is truthy (1/true/yes/on), drop page cache, set
# SYS_CACHED_OF_EMPTY from /proc/meminfo Cached (kB), then run the sampler so
# its pagecache column is system Cached growth. Otherwise unset that env and
# use the default per-fd cachestat path.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${SCRIPT_DIR}/sample_statm_fdcache"

if [[ ! -x "$BIN" ]]; then
  "$SCRIPT_DIR/ensure_sample_statm_fdcache.sh" >/dev/null
fi

use_sys=0
case "${CACHED_PAGES_USE_SYS:-0}" in
  1|true|TRUE|yes|YES|on|ON) use_sys=1 ;;
esac

if [[ "$use_sys" -eq 1 ]]; then
  sync
  sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  SYS_CACHED_OF_EMPTY="$(awk '/^Cached:/{print $2; exit}' /proc/meminfo)"
  export SYS_CACHED_OF_EMPTY
  echo "run_sample_statm_fdcache: SYS_CACHED_OF_EMPTY=${SYS_CACHED_OF_EMPTY} kB (after drop_caches)" >&2
else
  unset SYS_CACHED_OF_EMPTY || true
fi

exec "$BIN" "$@"
