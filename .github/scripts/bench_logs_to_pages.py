#!/usr/bin/env python3
"""Parse db_bench / memtablerep_bench logs and maintain GitHub Pages site trees.

Supports multi-engine comparison (topling / topling-dictzip10 / rocksdb-*).
"""

from __future__ import annotations

import argparse
import html
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

DB_BENCH_RE = re.compile(
    r"^(?P<name>\S+)\s*:\s*"
    r"(?P<micros>[\d.]+)\s+micros/op\s+"
    r"(?P<ops>\d+)\s+ops/sec\s+"
    r"(?P<seconds>[\d.]+)\s+seconds\s+"
    r"(?P<operations>\d+)\s+operations;"
    r"(?P<extra>.*)$"
)
RUNNING_RE = re.compile(r"^Running\s+(?P<name>\S+)\s*$")
METRIC_RE = re.compile(
    r"^(?P<label>Total bytes written|Write throughput|write us/op|"
    r"Total bytes read|Read throughput|read us/op|Elapsed time|"
    r"Number of threads)\s*:\s*(?P<value>.+)$"
)

ENGINES = ("topling", "topling-dictzip10", "rocksdb-v8.10", "rocksdb-master")
TOPLING_ENGINES = ("topling", "topling-dictzip10")
ROCKSDB_ENGINES = ("rocksdb-v8.10", "rocksdb-master")
# memtablerep is identical across Topling yaml variants; only default topling runs it.
MEMTABLE_ENGINES = ("topling", "rocksdb-v8.10", "rocksdb-master")
ENGINE_LABELS = {
    "topling": "ToplingDB",
    "topling-dictzip10": "ToplingDB minDictZip=10",
    "rocksdb-v8.10": "RocksDB v8.10",
    "rocksdb-master": "RocksDB master",
}
# Short labels for ratio column headers.
RATIO_BASE_LABELS = {
    "topling": "Topling",
    "topling-dictzip10": "dictzip10",
}
RATIO_OTHER_LABELS = {
    "rocksdb-v8.10": "v8.10",
    "rocksdb-master": "master",
}


def parse_shm_usage(text: str) -> Optional[Dict[str, int]]:
    apparent: Optional[int] = None
    allocated: Optional[int] = None
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("apparent_bytes="):
            apparent = int(line.split("=", 1)[1])
        elif line.startswith("allocated_bytes="):
            allocated = int(line.split("=", 1)[1])
    if apparent is None or allocated is None:
        return None
    return {"apparent_bytes": apparent, "allocated_bytes": allocated}


def format_iec(num_bytes: int) -> str:
    """IEC human-readable (1024), one decimal place, e.g. 850.0MiB / 1.2GiB."""
    n = float(num_bytes)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    idx = 0
    while n >= 1024.0 and idx < len(units) - 1:
        n /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(num_bytes)}B"
    return f"{n:.1f}{units[idx]}"


SHM_WORKLOADS = ("fillrandom", "fillseq")


def load_shm_usages(eng_dir: Path) -> Dict[str, Optional[Dict[str, int]]]:
    """Load per-workload shm usage; legacy shm_usage.txt maps to fillseq."""
    out: Dict[str, Optional[Dict[str, int]]] = {}
    for wl in SHM_WORKLOADS:
        path = eng_dir / f"shm_usage-{wl}.txt"
        if path.is_file():
            out[wl] = parse_shm_usage(
                path.read_text(encoding="utf-8", errors="replace")
            )
        else:
            out[wl] = None
    legacy = eng_dir / "shm_usage.txt"
    if out.get("fillseq") is None and legacy.is_file():
        out["fillseq"] = parse_shm_usage(
            legacy.read_text(encoding="utf-8", errors="replace")
        )
    return out


def _size_ratio_cell(
    base_bytes: Optional[int], other_bytes: Optional[int]
) -> str:
    """ratio = other / base; &lt;1 means other uses less space (better compression)."""
    if base_bytes is None or other_bytes is None or base_bytes <= 0:
        return "—"
    ratio = other_bytes / base_bytes
    cls = "faster" if ratio < 1.0 else "slower"
    return f'<span class="{cls}">{ratio:.2f}x</span>'


def build_shm_usage_table(
    usages: Dict[str, Dict[str, Optional[Dict[str, int]]]],
) -> str:
    """Wide space compare: allocated bytes per engine + dictzip10/Topling size ratio."""

    def _bytes(eng: str, wl: str, key: str) -> Optional[int]:
        u = (usages.get(eng) or {}).get(wl)
        if not u:
            return None
        return u.get(key)

    headers = ["workload"]
    for e in ENGINES:
        headers.append(ENGINE_LABELS[e])
    headers.append("dictzip10 / Topling (space)")

    rows_html = []
    for wl in SHM_WORKLOADS:
        cells = [f"<td>{html.escape(wl)}</td>"]
        for e in ENGINES:
            b = _bytes(e, wl, "allocated_bytes")
            cells.append(
                f"<td>{html.escape(format_iec(b)) if b is not None else 'n/a'}</td>"
            )
        cells.append(
            f"<td>{_size_ratio_cell(_bytes('topling', wl, 'allocated_bytes'), _bytes('topling-dictzip10', wl, 'allocated_bytes'))}</td>"
        )
        rows_html.append("<tr>" + "".join(cells) + "</tr>")
    if not rows_html:
        rows_html.append(
            f'<tr><td colspan="{len(headers)}"><em>no rows</em></td></tr>'
        )
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    return (
        "<table>\n<thead><tr>"
        + th
        + "</tr></thead>\n<tbody>\n"
        + "\n".join(rows_html)
        + "\n</tbody>\n</table>"
    )


def parse_db_bench(text: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for line in text.splitlines():
        m = DB_BENCH_RE.match(line.strip())
        if not m:
            continue
        rows.append(
            {
                "benchmark": m.group("name"),
                "micros/op": m.group("micros"),
                "ops/sec": m.group("ops"),
                "seconds": m.group("seconds"),
                "operations": m.group("operations"),
                "extra": m.group("extra").strip(),
            }
        )
    return rows


def parse_memtablerep(text: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    current: Optional[str] = None
    for line in text.splitlines():
        stripped = line.strip()
        rm = RUNNING_RE.match(stripped)
        if rm:
            current = rm.group("name")
            continue
        mm = METRIC_RE.match(stripped)
        if mm and current:
            rows.append(
                {
                    "benchmark": current,
                    "metric": mm.group("label"),
                    "value": mm.group("value").strip(),
                }
            )
    return rows


def _table(headers: List[str], rows: List[Dict[str, str]], keys: List[str]) -> str:
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body_parts = []
    for row in rows:
        tds = "".join(f"<td>{html.escape(str(row.get(k, '')))}</td>" for k in keys)
        body_parts.append(f"<tr>{tds}</tr>")
    if not body_parts:
        body_parts.append(
            f'<tr><td colspan="{len(headers)}"><em>no rows parsed</em></td></tr>'
        )
    return (
        "<table>\n<thead><tr>"
        + th
        + "</tr></thead>\n<tbody>\n"
        + "\n".join(body_parts)
        + "\n</tbody>\n</table>"
    )


def _page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; line-height: 1.45; }}
    table {{ border-collapse: collapse; margin: 1rem 0 2rem; width: 100%; }}
    th, td {{ border: 1px solid #ccc; padding: 0.4rem 0.6rem; text-align: left; }}
    th {{ background: #f4f4f4; }}
    h1, h2, h3 {{ margin-top: 1.5rem; }}
    a {{ color: #0645ad; }}
    .meta {{ color: #555; font-size: 0.9rem; }}
    .faster {{ color: #0a7a28; font-weight: 600; }}
    .slower {{ color: #a30d0d; }}
  </style>
</head>
<body>
{body}
</body>
</html>
"""


def _ops_by_benchmark(rows: List[Dict[str, str]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows:
        try:
            out[row["benchmark"]] = int(row["ops/sec"])
        except (KeyError, ValueError):
            continue
    return out


def _seconds_by_benchmark(rows: List[Dict[str, str]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for row in rows:
        try:
            out[row["benchmark"]] = float(row["seconds"])
        except (KeyError, ValueError):
            continue
    return out


def _metric_map(rows: List[Dict[str, str]]) -> Dict[str, str]:
    """Key: benchmark|metric -> value."""
    out: Dict[str, str] = {}
    for row in rows:
        out[f"{row['benchmark']}|{row['metric']}"] = row["value"]
    return out


def _time_ratio_cell(topling_s: Optional[float], other_s: Optional[float]) -> str:
    """ratio = other_seconds / topling_seconds; >=1 means ToplingDB faster."""
    if topling_s is None or other_s is None or topling_s <= 0:
        return "—"
    ratio = other_s / topling_s
    cls = "faster" if ratio >= 1.0 else "slower"
    return f'<span class="{cls}">{ratio:.2f}x</span>'


def _subject_time_ratio_cell(
    baseline_s: Optional[float], subject_s: Optional[float]
) -> str:
    """ratio = subject / baseline; color is about the subject (dictzip10).

    >1 → subject slower (red); <1 → subject faster (green).
    """
    if baseline_s is None or subject_s is None or baseline_s <= 0:
        return "—"
    ratio = subject_s / baseline_s
    if ratio > 1.0:
        cls = "slower"
    elif ratio < 1.0:
        cls = "faster"
    else:
        return f"{ratio:.2f}x"
    return f'<span class="{cls}">{ratio:.2f}x</span>'


def _ratio_pairs() -> List[Tuple[str, str]]:
    """(base_topling_eng, rocksdb_eng) for time-ratio columns."""
    pairs: List[Tuple[str, str]] = []
    for base in TOPLING_ENGINES:
        for other in ROCKSDB_ENGINES:
            pairs.append((base, other))
    return pairs


def build_db_bench_compare(
    engines: Dict[str, List[Dict[str, str]]],
) -> str:
    """Wide comparison: ops/sec + dictzip10/Topling time + rocksdb/topling* time."""
    ops_by = {e: _ops_by_benchmark(engines.get(e, [])) for e in ENGINES}
    sec_by = {e: _seconds_by_benchmark(engines.get(e, [])) for e in ENGINES}
    key_sets = [set(m.keys()) for m in ops_by.values() if m]
    names = sorted(set().union(*key_sets)) if key_sets else []
    ratio_pairs = _ratio_pairs()
    headers = ["benchmark"] + [
        f"{ENGINE_LABELS[e]} ops/sec" for e in ENGINES
    ]
    # Direct Topling-vs-dictzip10 cost: >1 means minDictZip=10 is slower.
    headers.append("dictzip10 time / Topling")
    for base, other in ratio_pairs:
        headers.append(
            f"{RATIO_OTHER_LABELS[other]} time / {RATIO_BASE_LABELS[base]}"
        )
    rows_html = []
    for name in names:
        cells = [f"<td>{html.escape(name)}</td>"]
        for e in ENGINES:
            v = ops_by[e].get(name)
            cells.append(f"<td>{v if v is not None else '—'}</td>")
        cells.append(
            f"<td>{_subject_time_ratio_cell(sec_by['topling'].get(name), sec_by['topling-dictzip10'].get(name))}</td>"
        )
        for base, other in ratio_pairs:
            cells.append(
                f"<td>{_time_ratio_cell(sec_by[base].get(name), sec_by[other].get(name))}</td>"
            )
        rows_html.append("<tr>" + "".join(cells) + "</tr>")
    if not rows_html:
        rows_html.append(
            f'<tr><td colspan="{len(headers)}"><em>no rows parsed</em></td></tr>'
        )
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    return (
        "<table>\n<thead><tr>"
        + th
        + "</tr></thead>\n<tbody>\n"
        + "\n".join(rows_html)
        + "\n</tbody>\n</table>"
    )


def build_topling_omit_compare(
    engines: Dict[str, List[Dict[str, str]]],
) -> str:
    """Parallel Topling variants lazy-load demo (ops/sec; last run per name)."""
    ops_by = {e: _ops_by_benchmark(engines.get(e, [])) for e in TOPLING_ENGINES}
    key_sets = [set(m.keys()) for m in ops_by.values() if m]
    names = sorted(set().union(*key_sets)) if key_sets else []
    headers = ["benchmark"] + [
        f"{ENGINE_LABELS[e]} ops/sec" for e in TOPLING_ENGINES
    ]
    rows_html = []
    for name in names:
        cells = [f"<td>{html.escape(name)}</td>"]
        for e in TOPLING_ENGINES:
            v = ops_by[e].get(name)
            cells.append(f"<td>{v if v is not None else '—'}</td>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")
    if not rows_html:
        rows_html.append(
            f'<tr><td colspan="{len(headers)}"><em>no rows parsed</em></td></tr>'
        )
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    return (
        "<table>\n<thead><tr>"
        + th
        + "</tr></thead>\n<tbody>\n"
        + "\n".join(rows_html)
        + "\n</tbody>\n</table>"
    )


def build_memtable_compare(
    engines: Dict[str, List[Dict[str, str]]],
) -> str:
    maps = {e: _metric_map(engines.get(e, [])) for e in MEMTABLE_ENGINES}
    key_sets = [set(m.keys()) for m in maps.values() if m]
    keys = sorted(set().union(*key_sets)) if key_sets else []
    headers = ["benchmark", "metric"] + [
        ENGINE_LABELS[e] for e in MEMTABLE_ENGINES
    ]
    rows_html = []
    for key in keys:
        bench, metric = key.split("|", 1)
        cells = [
            f"<td>{html.escape(bench)}</td>",
            f"<td>{html.escape(metric)}</td>",
        ]
        for e in MEMTABLE_ENGINES:
            cells.append(f"<td>{html.escape(maps[e].get(key, '—'))}</td>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")
    if not rows_html:
        rows_html.append(
            f'<tr><td colspan="{len(headers)}"><em>no rows parsed</em></td></tr>'
        )
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    return (
        "<table>\n<thead><tr>"
        + th
        + "</tr></thead>\n<tbody>\n"
        + "\n".join(rows_html)
        + "\n</tbody>\n</table>"
    )


def _load_engine_logs(log_root: Path) -> Dict[str, Dict[str, Any]]:
    """Load per-engine parsed rows from log-root/<engine>/..."""
    result: Dict[str, Dict[str, Any]] = {}
    for eng in ENGINES:
        eng_dir = log_root / eng
        db_path = eng_dir / "db_bench.log"
        skip_path = eng_dir / "memtablerep_bench-skiplist.log"
        cspp_path = eng_dir / "memtablerep_bench-cspp.log"
        if not db_path.is_file():
            continue
        db_rows = parse_db_bench(
            db_path.read_text(encoding="utf-8", errors="replace")
        )
        fr_path = eng_dir / "db_bench-fillrandom.log"
        fr_rows: List[Dict[str, str]] = []
        if fr_path.is_file():
            fr_rows = parse_db_bench(
                fr_path.read_text(encoding="utf-8", errors="replace")
            )
        omit_fr_rows: List[Dict[str, str]] = []
        omit_fs_rows: List[Dict[str, str]] = []
        omit_fr = eng_dir / "db_bench-fillrandom-omit.log"
        omit_fs = eng_dir / "db_bench-fillseq-omit.log"
        if omit_fr.is_file():
            omit_fr_rows = parse_db_bench(
                omit_fr.read_text(encoding="utf-8", errors="replace")
            )
        if omit_fs.is_file():
            omit_fs_rows = parse_db_bench(
                omit_fs.read_text(encoding="utf-8", errors="replace")
            )
        skiplist_rows: List[Dict[str, str]] = []
        cspp_rows: List[Dict[str, str]] = []
        if skip_path.is_file():
            skiplist_rows = parse_memtablerep(
                skip_path.read_text(encoding="utf-8", errors="replace")
            )
        if cspp_path.is_file():
            cspp_rows = parse_memtablerep(
                cspp_path.read_text(encoding="utf-8", errors="replace")
            )
        result[eng] = {
            "db_bench": db_rows,
            "db_bench_fillrandom": fr_rows,
            "db_bench_omit_fillrandom": omit_fr_rows,
            "db_bench_omit_fillseq": omit_fs_rows,
            "memtablerep_skiplist": skiplist_rows,
            "memtablerep_cspp": cspp_rows,
            "shm_usage": load_shm_usages(eng_dir),
        }
    for req in TOPLING_ENGINES:
        if req not in result:
            raise SystemExit(f"missing required logs under {log_root}/{req}/")
    return result


def emit(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    log_root = Path(args.log_root)
    engines_data = _load_engine_logs(log_root)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir_name = f"{ts}-{args.variant}-{args.run_id}"
    run_dir = out / "runs" / run_dir_name
    raw_dir = run_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Copy raw logs preserving engine subdirs
    for eng, data in engines_data.items():
        eng_raw = raw_dir / eng
        eng_raw.mkdir(parents=True, exist_ok=True)
        src = log_root / eng
        for name in (
            "db_bench.log",
            "db_bench-fillrandom.log",
            "db_bench-fillrandom-omit.log",
            "db_bench-fillseq-omit.log",
            "memtablerep_bench-skiplist.log",
            "memtablerep_bench-cspp.log",
            "shm_usage.txt",
            "shm_usage-fillrandom.txt",
            "shm_usage-fillseq.txt",
        ):
            p = src / name
            if p.is_file():
                shutil.copy2(p, eng_raw / name)

    db_compare = build_db_bench_compare(
        {e: engines_data.get(e, {}).get("db_bench", []) for e in ENGINES}
    )
    fr_compare = build_db_bench_compare(
        {
            e: engines_data.get(e, {}).get("db_bench_fillrandom", [])
            for e in ENGINES
        }
    )
    skip_compare = build_memtable_compare(
        {
            e: engines_data.get(e, {}).get("memtablerep_skiplist", [])
            for e in MEMTABLE_ENGINES
        }
    )
    omit_fr_table = build_topling_omit_compare(
        {
            e: engines_data.get(e, {}).get("db_bench_omit_fillrandom") or []
            for e in TOPLING_ENGINES
        }
    )
    omit_fs_table = build_topling_omit_compare(
        {
            e: engines_data.get(e, {}).get("db_bench_omit_fillseq") or []
            for e in TOPLING_ENGINES
        }
    )
    shm_usages = {
        e: engines_data.get(e, {}).get("shm_usage") or {} for e in ENGINES
    }
    shm_table = build_shm_usage_table(shm_usages)

    db_bench_detail_keys = [
        "benchmark",
        "micros/op",
        "ops/sec",
        "seconds",
        "operations",
        "extra",
    ]

    # Per-engine detail tables
    detail_parts = []
    for eng in ENGINES:
        data = engines_data.get(eng)
        if not data:
            detail_parts.append(
                f"<h3>{html.escape(ENGINE_LABELS[eng])}</h3><p><em>no logs</em></p>"
            )
            continue
        detail_parts.append(f"<h3>{html.escape(ENGINE_LABELS[eng])}</h3>")
        if data.get("db_bench_fillrandom"):
            detail_parts.append(
                "<h4>db_bench (fillrandom+compact+readseq/readrandom)</h4>"
            )
            detail_parts.append(
                _table(
                    db_bench_detail_keys,
                    data["db_bench_fillrandom"],
                    db_bench_detail_keys,
                )
            )
        detail_parts.append("<h4>db_bench (fillseq suite)</h4>")
        detail_parts.append(
            _table(db_bench_detail_keys, data["db_bench"], db_bench_detail_keys)
        )
        if eng in TOPLING_ENGINES:
            if data.get("db_bench_omit_fillrandom"):
                detail_parts.append(
                    "<h4>db_bench omit lazy-load (fillrandom DB)</h4>"
                )
                detail_parts.append(
                    _table(
                        db_bench_detail_keys,
                        data["db_bench_omit_fillrandom"],
                        db_bench_detail_keys,
                    )
                )
            if data.get("db_bench_omit_fillseq"):
                detail_parts.append(
                    "<h4>db_bench omit lazy-load (fillseq DB)</h4>"
                )
                detail_parts.append(
                    _table(
                        db_bench_detail_keys,
                        data["db_bench_omit_fillseq"],
                        db_bench_detail_keys,
                    )
                )
        if data.get("memtablerep_skiplist"):
            detail_parts.append("<h4>memtablerep_bench (skiplist)</h4>")
            detail_parts.append(
                _table(
                    ["benchmark", "metric", "value"],
                    data["memtablerep_skiplist"],
                    ["benchmark", "metric", "value"],
                )
            )
        if data.get("memtablerep_cspp"):
            detail_parts.append("<h4>memtablerep_bench (cspp, ToplingDB only)</h4>")
            detail_parts.append(
                _table(
                    ["benchmark", "metric", "value"],
                    data["memtablerep_cspp"],
                    ["benchmark", "metric", "value"],
                )
            )

    raw_links = " | ".join(
        f'<a href="raw/{eng}/db_bench.log">{html.escape(ENGINE_LABELS[eng])} db_bench</a>'
        for eng in ENGINES
        if (raw_dir / eng / "db_bench.log").is_file()
    )

    body = f"""
  <h1>Bench run: {html.escape(args.variant)} / {html.escape(str(args.run_id))}</h1>
  <p class="meta">generated (UTC): {html.escape(datetime.now(timezone.utc).isoformat())}</p>
  <p>{raw_links}</p>
  <h2>/dev/shm usage (space; after db_bench + omit, before delete)</h2>
  <p class="meta">实际占盘 = allocated blocks (IEC). Parallel engines: default Topling (minDictZipValueSize=3000) vs minDictZip=10. Space ratio = dictzip10 / Topling; &lt;1 means minDictZip=10 uses less space.</p>
  {shm_table}
  <h2>Comparison: db_bench fillrandom suite (perf)</h2>
  <p class="meta">Benchmarks aligned (includes fillrandom/compact/readseq/readrandom). minDictZip=10 raises compression but typically slows compact and reads. Column &quot;dictzip10 time / Topling&quot; = subject/baseline seconds; &gt;1 (red) means minDictZip=10 is slower, &lt;1 (green) means faster. RocksDB time ratios use rocksdb_seconds / topling*_seconds; &gt;1 (green) means that Topling variant is faster. Values show ops/sec.</p>
  {fr_compare}
  <h2>Comparison: db_bench fillseq suite (perf)</h2>
  <p class="meta">Same as fillrandom suite. Watch compact / readseq / readrandom rows for minDictZip=10 cost vs space savings above.</p>
  {db_compare}
  <h2>Lazy load demo (ToplingDB only, scan_omit)</h2>
  <p class="meta">Not a fair comparison vs RocksDB. Parallel Topling variants (default vs minDictZip=10). Throughput is a lazy-load demo (scan_omit_key/value).</p>
  <h3>fillrandom-omit</h3>
  {omit_fr_table}
  <h3>fillseq-omit</h3>
  {omit_fs_table}
  <h2>Comparison: memtablerep_bench (skiplist)</h2>
  {skip_compare}
  <h2>Per-engine details</h2>
  {"".join(detail_parts)}
"""
    (run_dir / "index.html").write_text(
        _page(f"Bench {args.variant} {args.run_id}", body), encoding="utf-8"
    )

    meta = {
        "variant": args.variant,
        "run_id": str(args.run_id),
        "run_dir": run_dir_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "engines": {
            eng: {
                "db_bench": engines_data.get(eng, {}).get("db_bench", []),
                "memtablerep_skiplist": engines_data.get(eng, {}).get(
                    "memtablerep_skiplist", []
                ),
                "memtablerep_cspp": engines_data.get(eng, {}).get(
                    "memtablerep_cspp", []
                ),
                "db_bench_fillrandom": engines_data.get(eng, {}).get(
                    "db_bench_fillrandom", []
                ),
                "db_bench_omit_fillrandom": engines_data.get(eng, {}).get(
                    "db_bench_omit_fillrandom", []
                ),
                "db_bench_omit_fillseq": engines_data.get(eng, {}).get(
                    "db_bench_omit_fillseq", []
                ),
                "shm_usage": engines_data.get(eng, {}).get("shm_usage")
                or {wl: None for wl in SHM_WORKLOADS},
            }
            for eng in ENGINES
        },
        # Keep legacy flat fields for topling (homepage snippets)
        "db_bench": engines_data.get("topling", {}).get("db_bench", []),
        "memtablerep_skiplist": engines_data.get("topling", {}).get(
            "memtablerep_skiplist", []
        ),
        "memtablerep_cspp": engines_data.get("topling", {}).get(
            "memtablerep_cspp", []
        ),
    }
    (out / "run-meta.json").write_text(
        json.dumps(meta, indent=2) + "\n", encoding="utf-8"
    )


def _render_latest_section(variant: str, entry: Optional[Dict[str, Any]]) -> str:
    if not entry:
        return f"<h2>Latest: {html.escape(variant)}</h2><p><em>no runs yet</em></p>"
    run_dir = entry["run_dir"]
    engines = entry.get("engines") or {
        "topling": {
            "db_bench": entry.get("db_bench", []),
            "memtablerep_skiplist": entry.get("memtablerep_skiplist", []),
            "memtablerep_cspp": entry.get("memtablerep_cspp", []),
        }
    }
    db_compare = build_db_bench_compare(
        {e: engines.get(e, {}).get("db_bench", []) for e in ENGINES}
    )
    skip_compare = build_memtable_compare(
        {
            e: engines.get(e, {}).get("memtablerep_skiplist", [])
            for e in MEMTABLE_ENGINES
        }
    )
    return f"""
  <h2>Latest: {html.escape(variant)}</h2>
  <p class="meta">run_id={html.escape(str(entry.get('run_id', '')))} |
     <a href="runs/{html.escape(run_dir)}/index.html">full report</a> |
     {html.escape(str(entry.get('timestamp', '')))}</p>
  <h3>db_bench comparison (time ratio = rocksdb / topling*)</h3>
  {db_compare}
  <h3>memtablerep_bench skiplist comparison</h3>
  {skip_compare}
"""


def _render_history(history: List[Dict[str, Any]]) -> str:
    if not history:
        return "<ul><li><em>empty</em></li></ul>"
    items = []
    for entry in history:
        run_dir = entry.get("run_dir", "")
        items.append(
            "<li>"
            f'{html.escape(str(entry.get("timestamp", "")))} — '
            f'<strong>{html.escape(str(entry.get("variant", "")))}</strong> '
            f'run_id={html.escape(str(entry.get("run_id", "")))} — '
            f'<a href="runs/{html.escape(run_dir)}/index.html">report</a>'
            "</li>"
        )
    return "<ul>\n" + "\n".join(items) + "\n</ul>"


def _latest_by_variant(
    history: List[Dict[str, Any]],
) -> Dict[str, Optional[Dict[str, Any]]]:
    latest: Dict[str, Optional[Dict[str, Any]]] = {"plain": None, "avx512": None}
    for entry in history:
        v = entry.get("variant")
        if v in latest and latest[v] is None:
            latest[v] = entry
    return latest


def merge(args: argparse.Namespace) -> None:
    merge_into = Path(args.merge_into)
    from_dir = Path(args.from_dir)
    merge_into.mkdir(parents=True, exist_ok=True)

    meta_path = from_dir / "run-meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if args.variant:
        meta["variant"] = args.variant

    run_dir_name = meta["run_dir"]
    src_run = from_dir / "runs" / run_dir_name
    dst_run = merge_into / "runs" / run_dir_name
    if not src_run.is_dir():
        raise SystemExit(f"missing emit run dir: {src_run}")
    dst_run.parent.mkdir(parents=True, exist_ok=True)
    if dst_run.exists():
        shutil.rmtree(dst_run)
    shutil.copytree(src_run, dst_run)

    history_path = merge_into / "history.json"
    if history_path.is_file():
        history = json.loads(history_path.read_text(encoding="utf-8"))
        if not isinstance(history, list):
            history = []
    else:
        history = []

    history_entry = {
        "variant": meta["variant"],
        "run_id": meta["run_id"],
        "run_dir": run_dir_name,
        "timestamp": meta.get("timestamp")
        or datetime.now(timezone.utc).isoformat(),
        "engines": meta.get("engines", {}),
        "db_bench": meta.get("db_bench", []),
        "memtablerep_skiplist": meta.get("memtablerep_skiplist", []),
        "memtablerep_cspp": meta.get("memtablerep_cspp", []),
    }
    history.insert(0, history_entry)
    history_path.write_text(json.dumps(history, indent=2) + "\n", encoding="utf-8")

    (merge_into / ".nojekyll").write_text("", encoding="utf-8")

    latest = _latest_by_variant(history)
    body = f"""
  <h1>ToplingDB vs RocksDB bench results</h1>
  <p class="meta">Updated (UTC): {html.escape(datetime.now(timezone.utc).isoformat())}</p>
  {_render_latest_section("plain", latest["plain"])}
  {_render_latest_section("avx512", latest["avx512"])}
  <h2>History</h2>
  {_render_history(history)}
"""
    (merge_into / "index.html").write_text(
        _page("ToplingDB vs RocksDB bench results", body), encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_emit = sub.add_parser("emit", help="Parse multi-engine logs into _pages fragment")
    p_emit.add_argument("--variant", required=True, choices=["plain", "avx512"])
    p_emit.add_argument("--run-id", required=True)
    p_emit.add_argument(
        "--log-root",
        required=True,
        help="Directory with topling/, topling-dictzip10/, rocksdb-*/ log subdirs",
    )
    p_emit.add_argument("--out", required=True)
    p_emit.set_defaults(func=emit)

    p_merge = sub.add_parser("merge", help="Merge emit output into gh-pages tree")
    p_merge.add_argument("--merge-into", required=True)
    p_merge.add_argument("--from", dest="from_dir", required=True)
    p_merge.add_argument("--variant", required=True, choices=["plain", "avx512"])
    p_merge.set_defaults(func=merge)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
