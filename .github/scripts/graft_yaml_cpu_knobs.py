#!/usr/bin/env python3
# Usage: graft_yaml_cpu_knobs.py <yaml> <db_cpu_quota> <nproc>
#   db_cpu = CPUQuota percent / 100  (e.g. 50% -> 0.5)
#   worker_cpu = nproc - db_cpu       (remaining host CPUs)
#   max_level1_subcompactions  = min(7,  ceil(db_cpu))
#   max_background_flushes     = 1
#   max_background_compactions = min(13, ceil(worker_cpu))
#   dcompact_min_level         = 2  (L0→L1 must stay on DB: with
#                                    memtable_as_log_index, CSPPMemTable SST
#                                    WAL blob numbers are allocated on DB only;
#                                    shipping L0→L1 to worker mis-allocates)
# Logs to stderr only.
import math
import re
import sys

if len(sys.argv) != 4:
    sys.exit("Usage: graft_yaml_cpu_knobs.py <yaml> <db_cpu_quota> <nproc>")

path, db_q, nproc_s = sys.argv[1], sys.argv[2], sys.argv[3]

m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)%", db_q.strip())
if not m:
    sys.exit(f"FAIL: db CPUQuota must look like 50%, got {db_q!r}")
db_cpu = float(m.group(1)) / 100.0

try:
    nproc = int(nproc_s)
except ValueError:
    sys.exit(f"FAIL: nproc must be an int, got {nproc_s!r}")
if nproc <= 0:
    sys.exit(f"FAIL: nproc must be > 0, got {nproc}")
if db_cpu >= nproc:
    sys.exit(f"FAIL: db_cpu={db_cpu} >= nproc={nproc}")

worker_cpu = nproc - db_cpu
max_l1 = min(7, math.ceil(db_cpu))
max_flush = 1
max_compact = min(13, math.ceil(worker_cpu))
dcompact_min_level = 2

with open(path, encoding="utf-8") as f:
    text = f.read()
for key, val in (
    ("max_level1_subcompactions", max_l1),
    ("max_background_flushes", max_flush),
    ("max_background_compactions", max_compact),
    ("dcompact_min_level", dcompact_min_level),
):
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
    f"yaml cpu knobs: nproc={nproc} db_cpu={db_cpu} worker_cpu={worker_cpu} "
    f"max_level1_subcompactions={max_l1} "
    f"max_background_flushes={max_flush} "
    f"max_background_compactions={max_compact} "
    f"dcompact_min_level={dcompact_min_level}",
    file=sys.stderr,
)
