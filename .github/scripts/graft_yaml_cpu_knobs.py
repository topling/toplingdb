#!/usr/bin/env python3
# Usage: graft_yaml_cpu_knobs.py <yaml> <db_cpu_quota> <worker_cpu_quota>
#   max_level1_subcompactions  = min(7,  ceil(db_cpu))
#   max_background_flushes     = 1
#   max_background_compactions = min(13, ceil(worker_cpu))
# Logs to stderr only.
import math
import re
import sys

if len(sys.argv) != 4:
    sys.exit("Usage: graft_yaml_cpu_knobs.py <yaml> <db_cpu_quota> <worker_cpu_quota>")

path, db_q, worker_q = sys.argv[1], sys.argv[2], sys.argv[3]

def cpu_from_quota(q):
    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)%", q.strip())
    if not m:
        sys.exit(f"FAIL: CPUQuota must look like 50% or 350%, got {q!r}")
    return float(m.group(1)) / 100.0

db_cpu = cpu_from_quota(db_q)
worker_cpu = cpu_from_quota(worker_q)
max_l1 = min(7, math.ceil(db_cpu))
max_flush = 1
max_compact = min(13, math.ceil(worker_cpu))

with open(path, encoding="utf-8") as f:
    text = f.read()
subs = {
    "max_level1_subcompactions": max_l1,
    "max_background_flushes": max_flush,
    "max_background_compactions": max_compact,
}
for key, val in subs.items():
    pat = re.compile(
        rf"^([ \t]*{re.escape(key)}:[ \t]*)(-?\d+)([ \t]*(?:#.*)?)?$",
        re.MULTILINE,
    )
    text, n = pat.subn(rf"\g<1>{val}\g<3>", text, count=1)
    if n != 1:
        sys.exit(f"FAIL: expected exactly one {key}: in {path}, got {n}")
with open(path, "w", encoding="utf-8") as f:
    f.write(text)
print(
    f"yaml cpu knobs: db_cpu={db_cpu} worker_cpu={worker_cpu} "
    f"max_level1_subcompactions={max_l1} "
    f"max_background_flushes={max_flush} "
    f"max_background_compactions={max_compact}",
    file=sys.stderr,
)
