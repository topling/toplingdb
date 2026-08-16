#!/usr/bin/env python3
"""Parse db_bench / memtablerep_bench logs and maintain GitHub Pages site trees.

Supports multi-engine comparison (zipkeyonly / zipkeyvalue / rocksdb-*).
"""

from __future__ import annotations

import argparse
import html
import json
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_RSS_SCRIPTS = Path(__file__).resolve().parent
if str(_RSS_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_RSS_SCRIPTS))
from bench_pages_common import (  # noqa: E402
    build_rss_svg_section as _common_rss_svg_section,
    build_source_links as _common_source_links,
    combine_db_bench_logs as _combine_db_bench_logs,
    entry_has_info_logs as _entry_has_info_logs,
    fmt_utc as _fmt_utc,
    href as _href,
    page as _page,
)
from bench_rss_chart import parse_rss_series  # noqa: E402


DB_BENCH_RE = re.compile(
    r"^(?:(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\s+)?"
    r"(?P<name>\S+)\s*:\s*"
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

ENGINES = ("zipkeyonly", "zipkeyvalue", "rocksdb-v8.10", "rocksdb-master")
TOPLING_ENGINES = ("zipkeyonly", "zipkeyvalue")
ROCKSDB_ENGINES = ("rocksdb-v8.10", "rocksdb-master")
# Per-pass runtime yamls only (no baseline db_bench.yaml).
YAML_USED_NAMES = (
    "db_bench-fillrandom.yaml",
    "db_bench-fillseq.yaml",
)
ENGINE_LABELS = {
    "zipkeyonly": "ToplingDB zipkeyonly",
    "zipkeyvalue": "ToplingDB zipkeyvalue",
    "rocksdb-v8.10": "RocksDB v8.10",
    "rocksdb-master": "RocksDB master",
}
# Short labels for ratio column headers.
RATIO_BASE_LABELS = {
    "zipkeyonly": "zipkeyonly",
    "zipkeyvalue": "zipkeyvalue",
}
RATIO_OTHER_LABELS = {
    "rocksdb-v8.10": "v8.10",
    "rocksdb-master": "master",
}
def set_rocksdb_master_label(sha: Optional[str]) -> None:
    """Set RocksDB master display labels; include short git SHA when known."""
    if sha:
        short = str(sha).strip()[:8]
        if short:
            ENGINE_LABELS["rocksdb-master"] = f"RocksDB master ({short})"
            RATIO_OTHER_LABELS["rocksdb-master"] = f"master ({short})"
            return
    ENGINE_LABELS["rocksdb-master"] = "RocksDB master"
    RATIO_OTHER_LABELS["rocksdb-master"] = "master"


def apply_engine_meta(meta_roots: List[Path]) -> Optional[str]:
    """Annotate RocksDB master label with short git SHA from engine-meta.json.

    Searches each root for rocksdb-master/engine-meta.json. Returns full SHA or None.
    """
    for root in meta_roots:
        path = root / "rocksdb-master" / "engine-meta.json"
        if not path.is_file():
            continue
        try:
            meta = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        sha = str(meta.get("git_sha") or "").strip()
        if not sha:
            continue
        set_rocksdb_master_label(sha)
        return sha
    set_rocksdb_master_label(None)
    return None


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


RSS_WORKLOADS = ("fillrandom", "fillseq", "fillrandom-omit", "fillseq-omit")
RSS_WORKLOAD_LABELS = {
    "fillrandom": "fillrandom",
    "fillseq": "fillseq",
    "fillrandom-omit": "fillrandom scan-omit-value",
    "fillseq-omit": "fillseq scan-omit-value",
}
RSS_WORKLOAD_TIPS = {
    "fillrandom-omit": (
        "restart process with reuse db data of fillrandom, "
        "scan without access value, benefited by lazy load value (ToplingDB feature)"
    ),
    "fillseq-omit": (
        "restart process with reuse db data of fillseq, "
        "scan without access value, benefited by lazy load value (ToplingDB feature)"
    ),
}


def parse_rss_usage(text: str) -> Optional[int]:
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("max_rss_bytes="):
            return int(line.split("=", 1)[1])
    return None


def load_rss_usages(eng_dir: Path) -> Dict[str, Optional[int]]:
    out: Dict[str, Optional[int]] = {}
    for wl in RSS_WORKLOADS:
        path = eng_dir / f"rss_usage-{wl}.txt"
        if path.is_file():
            out[wl] = parse_rss_usage(
                path.read_text(encoding="utf-8", errors="replace")
            )
        else:
            out[wl] = None
    return out


def load_bench_settings(eng_dir: Path) -> Dict[str, str]:
    path = eng_dir / "bench_settings.txt"
    out: Dict[str, str] = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def load_runner_env(log_root: Path) -> Dict[str, str]:
    path = log_root / "runner_env.txt"
    out: Dict[str, str] = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def _size_ratio_cell(
    base_bytes: Optional[int], other_bytes: Optional[int]
) -> str:
    """ratio = other / base; &lt;1 less (green), &gt;1 more (red), equal neutral."""
    if base_bytes is None or other_bytes is None or base_bytes <= 0:
        return "—"
    ratio = other_bytes / base_bytes
    text = f"{ratio:.2f}x"
    if other_bytes < base_bytes:
        return f'<span class="faster">{text}</span>'
    if other_bytes > base_bytes:
        return f'<span class="slower">{text}</span>'
    return text


def build_shm_usage_table(
    usages: Dict[str, Dict[str, Optional[Dict[str, int]]]],
) -> str:
    """Wide space compare: allocated bytes per engine + ratios vs RocksDB v8.10."""

    def _bytes(eng: str, wl: str, key: str) -> Optional[int]:
        u = (usages.get(eng) or {}).get(wl)
        if not u:
            return None
        return u.get(key)

    headers = ["workload"]
    for e in ENGINES:
        headers.append(ENGINE_LABELS[e])
    headers.append("zipkeyonly / v8.10 (space)")
    headers.append("zipkeyvalue / v8.10 (space)")

    rows_html = []
    for wl in SHM_WORKLOADS:
        cells = [f"<td>{html.escape(wl)}</td>"]
        for e in ENGINES:
            b = _bytes(e, wl, "allocated_bytes")
            cells.append(
                f"<td>{html.escape(format_iec(b)) if b is not None else 'n/a'}</td>"
            )
        cells.append(
            f"<td>{_size_ratio_cell(_bytes('rocksdb-v8.10', wl, 'allocated_bytes'), _bytes('zipkeyonly', wl, 'allocated_bytes'))}</td>"
        )
        cells.append(
            f"<td>{_size_ratio_cell(_bytes('rocksdb-v8.10', wl, 'allocated_bytes'), _bytes('zipkeyvalue', wl, 'allocated_bytes'))}</td>"
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


def build_rss_usage_table(
    rss_data: Dict[str, Dict[str, Optional[int]]],
    derived_engines: Optional[set] = None,
) -> str:
    """Peak RSS compare: absolute + ratio vs RocksDB v8.10. derived_engines marks omit cells."""
    if derived_engines is None:
        derived_engines = set()

    headers = ["workload"]
    for e in ENGINES:
        headers.append(ENGINE_LABELS[e])
    headers.append("zipkeyonly / v8.10 (RSS)")
    headers.append("zipkeyvalue / v8.10 (RSS)")

    workloads = sorted(
        {wl for eng_data in rss_data.values() for wl in eng_data if eng_data.get(wl) is not None}
    )
    if not workloads:
        workloads = list(RSS_WORKLOADS)

    rows_html = []
    for wl in workloads:
        label = html.escape(RSS_WORKLOAD_LABELS.get(wl, wl))
        tip = RSS_WORKLOAD_TIPS.get(wl)
        if tip:
            cells = [
                f'<td><abbr title="{html.escape(tip, quote=True)}">{label}</abbr></td>'
            ]
        else:
            cells = [f"<td>{label}</td>"]
        for e in ENGINES:
            b = (rss_data.get(e) or {}).get(wl)
            if b is None:
                text = "n/a"
            else:
                text = format_iec(b)
                if e in derived_engines and wl.endswith("-omit"):
                    text += " (=readseq)"
            cells.append(f"<td>{html.escape(text)}</td>")
        v810_bytes = (rss_data.get("rocksdb-v8.10") or {}).get(wl)
        topling_bytes = (rss_data.get("zipkeyonly") or {}).get(wl)
        dz10_bytes = (rss_data.get("zipkeyvalue") or {}).get(wl)
        cells.append(f"<td>{_size_ratio_cell(v810_bytes, topling_bytes)}</td>")
        cells.append(f"<td>{_size_ratio_cell(v810_bytes, dz10_bytes)}</td>")
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
        row = {
            "benchmark": m.group("name"),
            "micros/op": m.group("micros"),
            "ops/sec": m.group("ops"),
            "seconds": m.group("seconds"),
            "operations": m.group("operations"),
            "extra": m.group("extra").strip(),
        }
        ts = m.group("ts")
        if ts:
            row["ts"] = ts
        rows.append(row)
    return rows


def _iso_to_epoch(ts: str) -> float:
    """Parse ISO 8601 UTC timestamp to epoch seconds."""
    ts = ts.rstrip("Z")
    if "." in ts:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
    return dt.replace(tzinfo=timezone.utc).timestamp()


def compute_bench_segments(
    bench_rows: List[Dict[str, str]],
    start_epoch: float,
    total_duration: float,
) -> List[Tuple[str, float, float, bool]]:
    """Compute (name, rel_start_sec, rel_end_sec, estimated) for each bench item.

    If rows have 'ts' fields, use them directly.  Otherwise fall back to
    prefix-sum estimation with flush derived from gaps.
    """
    segments: List[Tuple[str, float, float, bool]] = []
    has_ts = any(r.get("ts") for r in bench_rows)

    if has_ts:
        for i, row in enumerate(bench_rows):
            ts_str = row.get("ts")
            if not ts_str:
                continue
            secs = float(row.get("seconds", "0"))
            ts_epoch = _iso_to_epoch(ts_str)
            rel_start = ts_epoch - start_epoch
            rel_end = rel_start + secs
            name = row["benchmark"]
            segments.append((name, rel_start, rel_end, False))
        # Derive flush segment from gap between fill* end and compact start
        fill_end = None
        compact_start = None
        for s in segments:
            if s[0].startswith("fill"):
                fill_end = s[2]
            elif s[0] == "compact" and fill_end is not None:
                compact_start = s[1]
                break
        if fill_end is not None and compact_start is not None and compact_start > fill_end:
            flush_idx = 0
            for j, s in enumerate(segments):
                if s[0] == "compact":
                    flush_idx = j
                    break
            segments.insert(flush_idx, ("flush", fill_end, compact_start, False))
    else:
        cursor = 0.0
        for row in bench_rows:
            name = row["benchmark"]
            secs = float(row.get("seconds", "0"))
            segments.append((name, cursor, cursor + secs, True))
            cursor += secs
        # Remaining time assigned to startup
        if cursor < total_duration:
            segments.insert(0, ("startup", 0, total_duration - cursor, True))

    return segments


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


def _operations_by_benchmark(rows: List[Dict[str, str]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows:
        try:
            out[row["benchmark"]] = int(row["operations"])
        except (KeyError, ValueError):
            continue
    return out


def _readseq_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Keep only readseq lines (for RocksDB lazy-load baseline from main suite)."""
    return [r for r in rows if r.get("benchmark") == "readseq"]


def _metric_map(rows: List[Dict[str, str]]) -> Dict[str, str]:
    """Key: benchmark|metric -> value."""
    out: Dict[str, str] = {}
    for row in rows:
        out[f"{row['benchmark']}|{row['metric']}"] = row["value"]
    return out


def _time_ratio_cell(topling_s: Optional[float], other_s: Optional[float]) -> str:
    """ratio = other_seconds / topling_seconds; >1 Topling faster, <1 slower, equal neutral."""
    if topling_s is None or other_s is None or topling_s <= 0:
        return "—"
    ratio = other_s / topling_s
    text = f"{ratio:.2f}x"
    if other_s > topling_s:
        return f'<span class="faster">{text}</span>'
    if other_s < topling_s:
        return f'<span class="slower">{text}</span>'
    return text


def _subject_time_ratio_cell(
    baseline_s: Optional[float], subject_s: Optional[float]
) -> str:
    """ratio = subject / baseline; zipkey* vs zipkey* stays neutral black."""
    if baseline_s is None or subject_s is None or baseline_s <= 0:
        return "—"
    return f"{subject_s / baseline_s:.2f}x"


def _ops_ratio_cell(topling_ops: Optional[int], other_ops: Optional[int]) -> str:
    """ratio = Topling ops/sec / other ops/sec; >1 Topling faster."""
    if (
        topling_ops is None
        or other_ops is None
        or topling_ops <= 0
        or other_ops <= 0
    ):
        return "—"
    return _time_ratio_cell(1 / topling_ops, 1 / other_ops)


def _ratio_pairs() -> List[Tuple[str, str]]:
    """(base_topling_eng, rocksdb_eng) for performance-ratio columns."""
    pairs: List[Tuple[str, str]] = []
    for base in TOPLING_ENGINES:
        for other in ROCKSDB_ENGINES:
            pairs.append((base, other))
    return pairs


_BENCH_ROW_ORDER = (
    "fillrandom",
    "fillseq",
    "flush",
    "compact",
    "readseq",
    "readrandom",
)


def _bench_row_names(names: set) -> List[str]:
    rank = {n: i for i, n in enumerate(_BENCH_ROW_ORDER)}
    return sorted(names, key=lambda n: (rank.get(n, len(_BENCH_ROW_ORDER)), n))


def build_db_bench_compare(
    engines: Dict[str, List[Dict[str, str]]],
) -> str:
    """Wide comparison: ops/sec plus speed ratios (ops/sec, or 1/seconds for compact).

    compact rows display operations/seconds instead of ops/sec.
    """
    ops_by = {e: _ops_by_benchmark(engines.get(e, [])) for e in ENGINES}
    sec_by = {e: _seconds_by_benchmark(engines.get(e, [])) for e in ENGINES}
    operations_by = {e: _operations_by_benchmark(engines.get(e, [])) for e in ENGINES}
    key_sets = [set(m.keys()) for m in ops_by.values() if m]
    names = _bench_row_names(set().union(*key_sets)) if key_sets else []
    ratio_pairs = _ratio_pairs()
    headers = ["benchmark"] + [
        f"{ENGINE_LABELS[e]} ops/sec" for e in ENGINES
    ]
    headers.append("zipkeyonly / zipkeyvalue")
    for base, other in ratio_pairs:
        headers.append(
            f"{RATIO_BASE_LABELS[base]} / {RATIO_OTHER_LABELS[other]}"
        )
    rows_html = []
    for name in names:
        is_compact = name == "compact"
        cells = [f"<td>{html.escape(name)}</td>"]
        for e in ENGINES:
            if is_compact:
                n_ops = operations_by[e].get(name)
                secs = sec_by[e].get(name)
                if n_ops is not None and secs is not None:
                    cells.append(f"<td>{n_ops}/{secs:.1f}s</td>")
                else:
                    cells.append("<td>—</td>")
            else:
                v = ops_by[e].get(name)
                cells.append(f"<td>{v if v is not None else '—'}</td>")
        if is_compact:
            subject_ratio = _subject_time_ratio_cell(
                sec_by["zipkeyonly"].get(name), sec_by["zipkeyvalue"].get(name)
            )
        else:
            subject_ratio = _subject_time_ratio_cell(
                ops_by["zipkeyvalue"].get(name), ops_by["zipkeyonly"].get(name)
            )
        cells.append(f"<td>{subject_ratio}</td>")
        for base, other in ratio_pairs:
            ratio = (
                _time_ratio_cell(sec_by[base].get(name), sec_by[other].get(name))
                if is_compact
                else _ops_ratio_cell(ops_by[base].get(name), ops_by[other].get(name))
            )
            cells.append(f"<td>{ratio}</td>")
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


LAZY_ENGINES = ("zipkeyonly", "zipkeyvalue", "rocksdb-v8.10")


def _hl(text: str, kind: str) -> str:
    """Color a short phrase: kind is 'faster' (green) or 'slower' (red)."""
    return f'<span class="{kind}">{html.escape(text)}</span>'


def _color_sign() -> str:
    return (
        'color sign: '
        '<span class="faster"><strong>ToplingDB</strong></span> is '
        '<span class="faster"><strong>Better</strong></span>/<span class="slower"><strong>Worse</strong></span>'
    )


def build_lazy_load_compare(
    engines: Dict[str, List[Dict[str, str]]],
) -> str:
    """Lazy-load / scan compare; RocksDB v8.10 is the baseline."""
    ops_by = {e: _ops_by_benchmark(engines.get(e, [])) for e in LAZY_ENGINES}
    key_sets = [set(m.keys()) for m in ops_by.values() if m]
    names = []
    if key_sets:
        found = set().union(*key_sets)
        for preferred in ("readseq", "nextwithkey"):
            if preferred in found:
                names.append(preferred)
                found.remove(preferred)
        names.extend(sorted(found))
    headers = ["benchmark"] + [
        f"{ENGINE_LABELS[e]} ops/sec" for e in LAZY_ENGINES
    ]
    headers.extend(
        [
            "zipkeyonly / v8.10",
            "zipkeyvalue / v8.10",
        ]
    )
    def _lazy_ops(eng: str, bench: str) -> Tuple[Optional[int], bool]:
        v = ops_by[eng].get(bench)
        if v is None and eng in ROCKSDB_ENGINES and bench == "nextwithkey":
            v = ops_by[eng].get("readseq")
            return v, v is not None
        return v, False

    rows_html = []
    for name in names:
        cells = [f"<td>{html.escape(name)}</td>"]
        for e in LAZY_ENGINES:
            v, derived = _lazy_ops(e, name)
            if v is None:
                cells.append("<td>—</td>")
            elif derived:
                cells.append(f"<td>{html.escape(str(v))} (=readseq)</td>")
            else:
                cells.append(f"<td>{v}</td>")
        # Baseline = v8.10; green when zipkey* is faster (ratio > 1).
        zipkeyonly_ops = _lazy_ops("zipkeyonly", name)[0]
        zipkeyvalue_ops = _lazy_ops("zipkeyvalue", name)[0]
        rocksdb_ops = _lazy_ops("rocksdb-v8.10", name)[0]
        cells.append(
            f"<td>{_ops_ratio_cell(zipkeyonly_ops, rocksdb_ops)}</td>"
        )
        cells.append(
            f"<td>{_ops_ratio_cell(zipkeyvalue_ops, rocksdb_ops)}</td>"
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


_METRIC_NUM_RE = re.compile(r"^\s*([+-]?(?:\d+\.?\d*|\.\d+))")


def _metric_number(value: str) -> Optional[float]:
    m = _METRIC_NUM_RE.match(value or "")
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _throughput_ratio_cell(
    baseline: Optional[float], subject: Optional[float]
) -> str:
    """ratio = subject / baseline; >1 means subject higher throughput (green)."""
    if baseline is None or subject is None or baseline <= 0:
        return "—"
    ratio = subject / baseline
    if ratio > 1.0:
        cls = "faster"
    elif ratio < 1.0:
        cls = "slower"
    else:
        return f"{ratio:.2f}x"
    return f'<span class="{cls}">{ratio:.2f}x</span>'


def _cost_ratio_cell(baseline: Optional[float], subject: Optional[float]) -> str:
    """ratio = subject / baseline for cost metrics (us/op, elapsed); <1 green."""
    if baseline is None or subject is None or baseline <= 0:
        return "—"
    ratio = subject / baseline
    if ratio < 1.0:
        cls = "faster"
    elif ratio > 1.0:
        cls = "slower"
    else:
        return f"{ratio:.2f}x"
    return f'<span class="{cls}">{ratio:.2f}x</span>'


# Metrics that show CSPP advantage; higher-is-better vs lower-is-better.
_CSPP_METRICS_HIGH = (
    "Write throughput",
    "Read throughput",
)
_CSPP_METRICS_LOW = (
    "Elapsed time",
    "write us/op",
    "read us/op",
)


def build_cspp_memtable_compare(
    cspp_rows: List[Dict[str, str]],
    skiplist_topling: List[Dict[str, str]],
    skiplist_v810: List[Dict[str, str]],
) -> str:
    """Highlight CSPPMemTable vs skiplist; RocksDB v8.10 skiplist is baseline."""
    cspp = _metric_map(cspp_rows)
    skip_t = _metric_map(skiplist_topling)
    skip_r = _metric_map(skiplist_v810)
    interesting = set(_CSPP_METRICS_HIGH) | set(_CSPP_METRICS_LOW)
    keys = sorted(
        k
        for k in set(cspp) | set(skip_t) | set(skip_r)
        if k.split("|", 1)[-1] in interesting
    )
    headers = [
        "benchmark",
        "metric",
        "CSPP (ToplingDB)",
        "skiplist (ToplingDB)",
        "skiplist (RocksDB v8.10)",
        "CSPP / v8.10",
    ]
    rows_html = []
    for key in keys:
        bench, metric = key.split("|", 1)
        c_raw = cspp.get(key, "—")
        t_raw = skip_t.get(key, "—")
        r_raw = skip_r.get(key, "—")
        c_n, r_n = _metric_number(c_raw), _metric_number(r_raw)
        if metric in _CSPP_METRICS_HIGH:
            ratio_html = _throughput_ratio_cell(r_n, c_n)
        else:
            ratio_html = _cost_ratio_cell(r_n, c_n)
        rows_html.append(
            "<tr>"
            f"<td>{html.escape(bench)}</td>"
            f"<td>{html.escape(metric)}</td>"
            f"<td>{html.escape(c_raw)}</td>"
            f"<td>{html.escape(t_raw)}</td>"
            f"<td>{html.escape(r_raw)}</td>"
            f"<td>{ratio_html}</td>"
            "</tr>"
        )
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
        if eng == "rocksdb-v8.10":
            # Reuse readseq×3 from the main fill* suites (no separate omit/scan pass).
            omit_fr_rows = _readseq_rows(fr_rows)
            omit_fs_rows = _readseq_rows(db_rows)
        else:
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
            "rss_usage": load_rss_usages(eng_dir),
            "bench_settings": load_bench_settings(eng_dir),
        }
    for req in TOPLING_ENGINES:
        if req not in result:
            raise SystemExit(f"missing required logs under {log_root}/{req}/")
    return result


def _stage_avg_rss_bytes(
    series_path: Path,
    bench_rows: List[Dict[str, str]],
    stage: str,
) -> Optional[int]:
    """Average process RSS during `stage` (e.g. readrandom)."""
    if not series_path.is_file() or not bench_rows:
        return None
    text = series_path.read_text(encoding="utf-8", errors="replace")
    start_epoch, page_size, samples = parse_rss_series(text)
    if not samples:
        return None
    total_dur = samples[-1][0] - start_epoch
    windows = [
        (t0, t1)
        for name, t0, t1, _est in compute_bench_segments(
            bench_rows, start_epoch, total_dur
        )
        if name == stage
    ]
    if not windows:
        return None
    t0, t1 = windows[-1]
    rss_pages = [
        rss
        for epoch, rss, _shared, _pc in samples
        if t0 <= (epoch - start_epoch) <= t1
    ]
    if not rss_pages:
        return None
    return int(sum(p * page_size for p in rss_pages) / len(rss_pages))


def _readrandom_zkv_highlight(
    engines: Dict[str, Any],
    log_root: Path,
) -> str:
    """readrandom (fillrandom suite): zipkeyvalue RSS% and ops/sec vs RocksDB v8.10."""
    zkv_rows = (engines.get("zipkeyvalue") or {}).get("db_bench_fillrandom") or []
    rocks_rows = (engines.get("rocksdb-v8.10") or {}).get("db_bench_fillrandom") or []
    zkv_ops = _ops_by_benchmark(zkv_rows).get("readrandom")
    rocks_ops = _ops_by_benchmark(rocks_rows).get("readrandom")
    if not zkv_ops or not rocks_ops:
        return ""
    zkv_rss = _stage_avg_rss_bytes(
        log_root / "zipkeyvalue" / "statm_series-fillrandom.txt",
        zkv_rows,
        "readrandom",
    )
    rocks_rss = _stage_avg_rss_bytes(
        log_root / "rocksdb-v8.10" / "statm_series-fillrandom.txt",
        rocks_rows,
        "readrandom",
    )
    if not zkv_rss or not rocks_rss:
        return ""
    mem_pct = round(100.0 * zkv_rss / rocks_rss)
    perf_x = zkv_ops / rocks_ops
    return (
        " On readrandom (the fairest comparison), ToplingDB zipkeyvalue is"
        f" <strong>{mem_pct}% memory / {perf_x:.2f}x speed</strong>"
        " vs RocksDB v8.10."
    )


def _build_runner_section(
    runner_env: Dict[str, Any],
    cache_size_bytes: Optional[int],
    dataset_bytes: Optional[int],
    dataset_estimated: bool,
    dcompact_href: str = "dcompact/index.html",
    readrandom_note: str = "",
) -> str:
    if not runner_env:
        return "<h2>Runner hardware/software</h2><p><em>n/a</em></p>"
    rows = [
        ("OS", runner_env.get("os_pretty_name", "n/a")),
        ("Kernel", runner_env.get("kernel", "n/a")),
        ("CPU", runner_env.get("cpu_model", "n/a")),
        ("Logical CPUs", runner_env.get("cpu_count", "n/a")),
    ]
    mem_str = runner_env.get("mem_total_bytes")
    if mem_str:
        try:
            rows.append(("Memory", format_iec(int(mem_str))))
        except (ValueError, TypeError):
            rows.append(("Memory", str(mem_str)))
    shm_str = runner_env.get("shm_size_bytes")
    if shm_str:
        try:
            rows.append(("/dev/shm", format_iec(int(shm_str))))
        except (ValueError, TypeError):
            rows.append(("/dev/shm", str(shm_str)))
    if cache_size_bytes is not None:
        rows.append(("RocksDB block cache", format_iec(cache_size_bytes)))
    trs = "\n".join(
        f"<tr><td><strong>{html.escape(str(k))}</strong></td><td>{html.escape(str(v))}</td></tr>"
        for k, v in rows
    )
    section = f"""<h2>Runner hardware/software</h2>
<table>
<tbody>
{trs}
</tbody>
</table>"""
    num = runner_env.get("num")
    key_size = runner_env.get("key_size")
    value_size = runner_env.get("value_size")
    if num and key_size and value_size:
        section += (
            f'\n<p class="meta">num={html.escape(str(num))} | '
            f'key_size={html.escape(str(key_size))} | '
            f'value_size={html.escape(str(value_size))}</p>'
        )
    if cache_size_bytes is not None and dataset_bytes is not None:
        cache_iec = format_iec(cache_size_bytes)
        ds_iec = format_iec(dataset_bytes)
        est_note = " (estimated)" if dataset_estimated else ""
        if cache_size_bytes >= dataset_bytes:
            items = []
            note = readrandom_note.strip()
            if note:
                items.append(note)
            items.append(
                "In production the working set typically does not fit, so cache misses dominate."
                " The same RAM holds more ToplingDB data, which cuts I/O and widens the gap."
            )
            items.append(
                f'<a href="{html.escape(dcompact_href)}">ToplingDB dcompact</a>'
                " can offload most of that CPU and memory cost."
            )
            lis = "".join(f"<li>{it}</li>\n" for it in items)
            section += (
                f'\n<p class="meta"><strong>On-disk DB size{est_note} ({ds_iec}) ≤ block cache ({cache_iec})</strong>'
                " — the dataset fits in cache, so this bench is a CPU and memory comparison.</p>"
                f'\n<ul class="meta">\n{lis}</ul>'
            )
        else:
            section += f'\n<p class="meta" style="color:#a30d0d"><strong>On-disk DB size{est_note} ({ds_iec}) &gt; block cache ({cache_iec})</strong> — cache cannot hold the entire dataset.</p>'
    return section


def _build_source_links(
    raw_dir: Path,
    href_prefix: str,
    actions_run_url: str = "",
    has_info_logs: bool = False,
) -> str:
    return _common_source_links(
        raw_dir,
        href_prefix,
        ENGINES,
        ENGINE_LABELS,
        TOPLING_ENGINES,
        YAML_USED_NAMES,
        actions_run_url=actions_run_url,
        has_info_logs=has_info_logs,
        artifact_label="DB INFO LOGs + bench yamls (Actions artifact)",
    )


def _build_rss_svg_section(
    log_root: Path,
    engines_data: Dict[str, Any],
    heading: str = "h3",
) -> str:
    return _common_rss_svg_section(
        log_root,
        engines_data,
        ENGINES,
        ENGINE_LABELS,
        compute_bench_segments,
        heading=heading,
    )


def _build_per_engine_details(engines_data: Dict[str, Any]) -> str:
    db_bench_detail_keys = [
        "benchmark",
        "micros/op",
        "ops/sec",
        "seconds",
        "operations",
        "extra",
    ]
    detail_parts: List[str] = []
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
    return "".join(detail_parts)


def emit(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    log_root = Path(args.log_root)
    meta_roots = [log_root]
    if getattr(args, "engine_meta_root", None):
        meta_roots.insert(0, Path(args.engine_meta_root))
    master_sha = apply_engine_meta(meta_roots)
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
            "rss_usage-fillrandom.txt",
            "rss_usage-fillseq.txt",
            "rss_usage-fillrandom-omit.txt",
            "rss_usage-fillseq-omit.txt",
            "statm_series-fillrandom.txt",
            "statm_series-fillseq.txt",
            "time-fillrandom.txt",
            "time-fillseq.txt",
            "time-fillrandom-omit.txt",
            "time-fillseq-omit.txt",
            "bench_settings.txt",
            *YAML_USED_NAMES,
            "engine-meta.json",
        ):
            p = src / name
            if p.is_file():
                shutil.copy2(p, eng_raw / name)
        # Prefer engine-meta from build prefix when not already under logs/.
        if args.engine_meta_root:
            src_meta = Path(args.engine_meta_root) / eng / "engine-meta.json"
            dst_meta = eng_raw / "engine-meta.json"
            if src_meta.is_file() and not dst_meta.is_file():
                shutil.copy2(src_meta, dst_meta)
            log_meta = log_root / eng / "engine-meta.json"
            if src_meta.is_file() and not log_meta.is_file():
                log_meta.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_meta, log_meta)
        _combine_db_bench_logs(eng_raw)

    runner_env_src = log_root / "runner_env.txt"
    if runner_env_src.is_file():
        shutil.copy2(runner_env_src, raw_dir / "runner_env.txt")

    runner_env = load_runner_env(log_root)
    bs = load_bench_settings(log_root)
    for k in ("num", "key_size", "value_size"):
        v = bs.get(k)
        if not v:
            raise SystemExit(
                f"missing required {k}= in {log_root / 'bench_settings.txt'}"
            )
        runner_env[k] = v

    bench_settings: Dict[str, Dict[str, str]] = {
        e: engines_data.get(e, {}).get("bench_settings") or {} for e in ENGINES
    }

    v810_shm = engines_data.get("rocksdb-v8.10", {}).get("shm_usage") or {}
    dataset_bytes: Optional[int] = None
    dataset_estimated = False
    for wl in SHM_WORKLOADS:
        u = v810_shm.get(wl)
        if u and u.get("allocated_bytes"):
            b = u["allocated_bytes"]
            if dataset_bytes is None or b > dataset_bytes:
                dataset_bytes = b
    if dataset_bytes is None:
        dataset_bytes = 100000000 * (8 + 15)
        dataset_estimated = True

    cache_size_bytes: Optional[int] = None
    cs_str = bench_settings.get("rocksdb-v8.10", {}).get("cache_size_bytes")
    if cs_str:
        try:
            cache_size_bytes = int(cs_str)
        except ValueError:
            pass

    actions_run_url = getattr(args, "actions_run_url", None) or ""
    has_info_logs = False
    for eng in ENGINES:
        for p in (log_root / eng).glob("LOG-*"):
            if p.is_file():
                has_info_logs = True
                break
        if has_info_logs:
            break
    if has_info_logs and not actions_run_url:
        raise SystemExit(
            "LOG-* present under --log-root but --actions-run-url was not provided; "
            "refusing to emit pages without an external link (LOGs must not go into gh-pages)"
        )

    source_links = _build_source_links(
        raw_dir, "raw", actions_run_url, has_info_logs
    )
    runner_html = _build_runner_section(
        runner_env,
        cache_size_bytes,
        dataset_bytes,
        dataset_estimated,
        dcompact_href="../../dcompact/index.html",
        readrandom_note=_readrandom_zkv_highlight(engines_data, log_root),
    )

    body = f"""
  <h1>Result table: {html.escape(args.variant)} / {html.escape(str(args.run_id))}</h1>
  <p class="meta">
    <a href="../../index.html">← plain home</a> |
    <a href="../../dcompact/index.html">dcompact →</a>
  </p>
  <p class="meta">generated (UTC): {html.escape(_fmt_utc())}</p>
  {source_links}
  {runner_html}
  {_build_per_engine_details(engines_data)}
"""
    (run_dir / "index.html").write_text(
        _page(
            f"Result table {args.variant} {args.run_id}",
            body,
            include_chart_js=False,
        ),
        encoding="utf-8",
    )

    runner_env_meta = {
        **runner_env,
        "cache_size_bytes": cache_size_bytes,
        "dataset_bytes": dataset_bytes,
        "dataset_estimated": dataset_estimated,
    }

    meta = {
        "variant": args.variant,
        "run_id": str(args.run_id),
        "run_dir": run_dir_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "rocksdb_master_git_sha": master_sha,
        "actions_run_url": actions_run_url,
        "has_info_logs": has_info_logs,
        "runner_env": runner_env_meta,
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
                "rss_usage": engines_data.get(eng, {}).get("rss_usage")
                or {wl: None for wl in RSS_WORKLOADS},
                "bench_settings": engines_data.get(eng, {}).get(
                    "bench_settings", {}
                ),
            }
            for eng in ENGINES
        },
        "db_bench": engines_data.get("zipkeyonly", {}).get("db_bench", []),
        "memtablerep_skiplist": engines_data.get("zipkeyonly", {}).get(
            "memtablerep_skiplist", []
        ),
        "memtablerep_cspp": engines_data.get("zipkeyonly", {}).get(
            "memtablerep_cspp", []
        ),
    }
    (out / "run-meta.json").write_text(
        json.dumps(meta, indent=2) + "\n", encoding="utf-8"
    )


def _render_latest_section(
    variant: str,
    entry: Optional[Dict[str, Any]],
    pages_root: Optional[Path] = None,
) -> str:
    if not entry:
        set_rocksdb_master_label(None)
        return f"<h2>Latest: {html.escape(variant)}</h2><p><em>no runs yet</em></p>"
    run_dir = entry["run_dir"]
    sha = entry.get("rocksdb_master_git_sha")
    if sha:
        set_rocksdb_master_label(str(sha))
    elif pages_root is not None:
        apply_engine_meta([pages_root / "runs" / run_dir / "raw"])
    else:
        set_rocksdb_master_label(None)
    engines = entry.get("engines") or {
        "zipkeyonly": {
            "db_bench": entry.get("db_bench", []),
            "memtablerep_skiplist": entry.get("memtablerep_skiplist", []),
            "memtablerep_cspp": entry.get("memtablerep_cspp", []),
        }
    }

    runner_env_data = entry.get("runner_env") or {}
    cache_size_bytes: Optional[int] = None
    dataset_bytes: Optional[int] = None
    dataset_estimated = False
    if runner_env_data:
        try:
            cache_size_bytes = int(runner_env_data.get("cache_size_bytes") or 0) or None
        except (ValueError, TypeError):
            pass
        try:
            dataset_bytes = int(runner_env_data.get("dataset_bytes") or 0) or None
        except (ValueError, TypeError):
            pass
        dataset_estimated = bool(runner_env_data.get("dataset_estimated"))
    raw_for_note = (
        pages_root / "runs" / run_dir / "raw" if pages_root is not None else None
    )
    readrandom_note = (
        _readrandom_zkv_highlight(engines, raw_for_note) if raw_for_note else ""
    )
    runner_html = _build_runner_section(
        runner_env_data,
        cache_size_bytes,
        dataset_bytes,
        dataset_estimated,
        readrandom_note=readrandom_note,
    )

    shm_usages = {
        e: engines.get(e, {}).get("shm_usage") or {} for e in ENGINES
    }
    shm_table = build_shm_usage_table(shm_usages)

    rss_data: Dict[str, Dict[str, Optional[int]]] = {}
    rss_derived_engines: set = set()
    for e in ENGINES:
        eng_rss_raw = engines.get(e, {}).get("rss_usage") or {}
        rss_data[e] = {wl: v for wl, v in eng_rss_raw.items()}
        if e in ROCKSDB_ENGINES:
            for src, dst in (("fillrandom", "fillrandom-omit"), ("fillseq", "fillseq-omit")):
                if rss_data[e].get(dst) is None and rss_data[e].get(src) is not None:
                    rss_data[e][dst] = rss_data[e][src]
            if (
                rss_data[e].get("fillrandom-omit") is not None
                or rss_data[e].get("fillseq-omit") is not None
            ):
                rss_derived_engines.add(e)
    rss_table = build_rss_usage_table(rss_data, rss_derived_engines)

    db_compare_fs = build_db_bench_compare(
        {e: engines.get(e, {}).get("db_bench", []) for e in ENGINES}
    )
    fr_compare = build_db_bench_compare(
        {e: engines.get(e, {}).get("db_bench_fillrandom", []) for e in ENGINES}
    )

    omit_fr_table = build_lazy_load_compare(
        {e: engines.get(e, {}).get("db_bench_omit_fillrandom") or [] for e in LAZY_ENGINES}
    )
    omit_fs_table = build_lazy_load_compare(
        {e: engines.get(e, {}).get("db_bench_omit_fillseq") or [] for e in LAZY_ENGINES}
    )

    t_eng = engines.get("zipkeyonly") or {}
    r_eng = engines.get("rocksdb-v8.10") or {}
    cspp_compare = build_cspp_memtable_compare(
        t_eng.get("memtablerep_cspp") or [],
        t_eng.get("memtablerep_skiplist") or [],
        r_eng.get("memtablerep_skiplist") or [],
    )

    cache_meta = (
        f"RocksDB block cache = half physical memory ({html.escape(format_iec(cache_size_bytes))})."
        if cache_size_bytes
        else "RocksDB block cache size: n/a."
    )

    source_links = ""
    rss_svg_section = ""
    if pages_root is not None:
        raw_dir = pages_root / "runs" / run_dir / "raw"
        href_prefix = f"runs/{run_dir}/raw"
        source_links = _build_source_links(
            raw_dir,
            href_prefix,
            str(entry.get("actions_run_url") or ""),
            _entry_has_info_logs(entry),
        )
        rss_svg_section = _build_rss_svg_section(raw_dir, engines)

    return f"""
  <h2>Latest: {html.escape(variant)}</h2>
  <p class="meta">run_id={html.escape(str(entry.get('run_id', '')))} |
     <a href="{_href("runs", run_dir, "index.html")}">result table</a> |
     {html.escape(_fmt_utc(entry.get('timestamp', '')))}</p>
  {source_links}
  {runner_html}
  <h3>/dev/shm usage (disk space; after db_bench)</h3>
  <p class="meta">Allocated disk usage (IEC blocks). zipkeyonly does not compress values (speed-optimized). RocksDB uses per-level compression (L0 none, L1-L5 Snappy, L6 Zstd), corresponding to the ToplingDB zipkeyvalue variant's level_writers (lightweight upper levels, heavyweight L6). Space ratio = engine / v8.10. {_color_sign()}.</p>
  {shm_table}
  <h3>Peak RSS (RAM; during db_bench)</h3>
  <p class="meta">RSS is <strong>R</strong>esident <strong>S</strong>et <strong>S</strong>ize. {cache_meta} Ratio = engine / v8.10. {_color_sign()}. RocksDB omit cells are =readseq (same scan; no omit). scan-omit-value: restart process, reuse fill* data, scan without access value, benefited by lazy load value (ToplingDB feature).</p>
  {rss_table}
  {rss_svg_section}
  <h3>Comparison: db_bench fillrandom suite (perf)</h3>
  <p class="meta">Benchmarks: fillrandom, flush, compact, readseq×3, readrandom. RocksDB uses per-level compression (L0 none, L1-L5 Snappy, L6 Zstd), corresponding to the ToplingDB zipkeyvalue variant's level_writers (lightweight upper levels, heavyweight L6). compact row shows operations/time. {_color_sign()}.</p>
  {fr_compare}
  <h3>Comparison: db_bench fillseq suite (perf)</h3>
  <p class="meta">Same as fillrandom. RocksDB fillseq benefits from shortcuts: <code>trivial_move</code> on non-overlapping SSTs; <code>refit level</code> skips zstd on L6: faster, larger size. Seqno-zeroing compact still runs.</p>
  {db_compare_fs}
  <h3>Lazy load demo (scan; RocksDB v8.10 baseline)</h3>
  <p class="meta">zipkey* needs an extra omit pass: scan_omit_key/value enables lazy value load (no real value load). RocksDB has no lazy load, so the baseline is readseq×3 already present in the main fill* suite (no extra pass). RocksDB nextwithkey cells are =readseq. master omitted here (v8.10 is the stronger RocksDB baseline). {_color_sign()}.</p>
  <h4>scan-omit-value on data from fillrandom</h4>
  {omit_fr_table}
  <h4>scan-omit-value on data from fillseq</h4>
  {omit_fs_table}
  <h3>memtablerep_bench: CSPPMemTable advantage</h3>
  <p class="meta">Focus: {_hl('CSPP (ToplingDB)', 'faster')} vs skiplist. Baseline = RocksDB v8.10 skiplist. {_color_sign()}.</p>
  {cspp_compare}
"""


def _render_history(history: List[Dict[str, Any]]) -> str:
    if not history:
        return "<ul><li><em>empty</em></li></ul>"
    items = []
    for entry in history:
        run_dir = entry.get("run_dir", "")
        items.append(
            "<li>"
            f'{html.escape(_fmt_utc(entry.get("timestamp", "")))} — '
            f'<strong>{html.escape(str(entry.get("variant", "")))}</strong> '
            f'run_id={html.escape(str(entry.get("run_id", "")))} — '
            f'<a href="{_href("runs", run_dir, "index.html")}">result table</a>'
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
        "rocksdb_master_git_sha": meta.get("rocksdb_master_git_sha"),
        "actions_run_url": meta.get("actions_run_url") or "",
        "has_info_logs": _entry_has_info_logs(meta),
        "runner_env": meta.get("runner_env"),
        "engines": meta.get("engines", {}),
        "db_bench": meta.get("db_bench", []),
        "memtablerep_skiplist": meta.get("memtablerep_skiplist", []),
        "memtablerep_cspp": meta.get("memtablerep_cspp", []),
    }
    history.insert(0, history_entry)
    history_path.write_text(json.dumps(history, indent=2) + "\n", encoding="utf-8")

    (merge_into / ".nojekyll").write_text("", encoding="utf-8")

    latest = _latest_by_variant(history)
    plain_section = _render_latest_section("plain", latest["plain"], merge_into)
    history_plain = [e for e in history if e.get("variant") != "avx512"]
    body = f"""
  <h1>ToplingDB vs RocksDB bench results</h1>
  <p class="meta">
    <a href="dcompact/index.html">dcompact bench →</a>
    — offloads most CPU and memory cost.
  </p>
  <p class="meta">Updated (UTC): {html.escape(_fmt_utc())}</p>
  {plain_section}
  <h2>History</h2>
  {_render_history(history_plain)}
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
        help="Directory with zipkeyonly/, zipkeyvalue/, rocksdb-*/ log subdirs",
    )
    p_emit.add_argument(
        "--engine-meta-root",
        default=None,
        help="Optional prefix containing <engine>/engine-meta.json (build artifact)",
    )
    p_emit.add_argument(
        "--actions-run-url",
        default=None,
        help="GitHub Actions run URL for linking DB INFO LOGs (Actions artifact); "
        "required when LOG-* files exist under --log-root",
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
