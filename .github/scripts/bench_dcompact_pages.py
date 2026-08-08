#!/usr/bin/env python3
"""Parse db_bench logs for dcompact bench variant and maintain GitHub Pages fragments.

Topling write-side runs under a CPU cgroup quota; compaction offloads to
dcompact_worker on remaining cores. RocksDB uses CompactionService spool +
out-of-cgroup broker/worker on remaining cores.
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
from bench_rss_chart import (  # noqa: E402
    RSS_CHART_JS as _RSS_CHART_JS,
    RSS_LINE_COLORS,
    build_rss_svg,
    parse_pagecache_src,
    parse_rss_series,
)


DB_BENCH_RE = re.compile(
    r"^(?:(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\s+)?"
    r"(?P<name>\S+)\s*:\s*"
    r"(?P<micros>[\d.]+)\s+micros/op\s+"
    r"(?P<ops>\d+)\s+ops/sec\s+"
    r"(?P<seconds>[\d.]+)\s+seconds\s+"
    r"(?P<operations>\d+)\s+operations;"
    r"(?P<extra>.*)$"
)

ENGINES = ("zipkeyonly", "zipkeyvalue", "rocksdb-v8.10", "rocksdb-master")
TOPLING_ENGINES = ("zipkeyonly", "zipkeyvalue")
ROCKSDB_ENGINES = ("rocksdb-v8.10", "rocksdb-master")
ENGINE_LABELS = {
    "zipkeyonly": "ToplingDB zipkeyonly",
    "zipkeyvalue": "ToplingDB zipkeyvalue",
    "rocksdb-v8.10": "RocksDB v8.10",
    "rocksdb-master": "RocksDB master",
}
RATIO_BASE_LABELS = {
    "zipkeyonly": "zipkeyonly",
    "zipkeyvalue": "zipkeyvalue",
}
RATIO_OTHER_LABELS = {
    "rocksdb-v8.10": "v8.10",
    "rocksdb-master": "master",
}
SHM_WORKLOADS = ("fillrandom", "fillseq")
RSS_WORKLOADS = ("fillrandom", "fillseq")
YAML_RAW_NAME = "db_bench.yaml"

def compute_bench_segments(
    bench_rows: List[Dict[str, str]],
    start_epoch: float,
    total_duration: float,
) -> List[Tuple[str, float, float, bool]]:
    """Compute (name, rel_start_sec, rel_end_sec, estimated) for each bench item."""
    segments: List[Tuple[str, float, float, bool]] = []
    has_ts = any(r.get("ts") for r in bench_rows)

    if has_ts:
        for row in bench_rows:
            ts_str = row.get("ts")
            if not ts_str:
                continue
            secs = float(row.get("seconds", "0"))
            ts_epoch = _iso_to_epoch(ts_str)
            rel_start = ts_epoch - start_epoch
            rel_end = rel_start + secs
            name = row["benchmark"]
            segments.append((name, rel_start, rel_end, False))
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
        if cursor < total_duration:
            segments.insert(0, ("startup", 0, total_duration - cursor, True))

    return segments


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
    .rss-chart-wrap {{ margin: 0.75rem 0 1.25rem; }}
  </style>
</head>
<body>
{body}
{_RSS_CHART_JS}
</body>
</html>
"""


def _hl(text: str, kind: str) -> str:
    """Color a short phrase: kind is 'faster' (green) or 'slower' (red)."""
    return f'<span class="{kind}">{html.escape(text)}</span>'


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
    """ratio = subject / baseline; color is about the subject (zipkeyvalue)."""
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
    """Wide comparison: ops/sec + zipkeyvalue/zipkeyonly time + rocksdb/zipkey* time."""
    ops_by = {e: _ops_by_benchmark(engines.get(e, [])) for e in ENGINES}
    sec_by = {e: _seconds_by_benchmark(engines.get(e, [])) for e in ENGINES}
    operations_by = {e: _operations_by_benchmark(engines.get(e, [])) for e in ENGINES}
    key_sets = [set(m.keys()) for m in ops_by.values() if m]
    names = sorted(set().union(*key_sets)) if key_sets else []
    ratio_pairs = _ratio_pairs()
    headers = ["benchmark"] + [
        f"{ENGINE_LABELS[e]} ops/sec" for e in ENGINES
    ]
    headers.append("zipkeyvalue time / zipkeyonly")
    for base, other in ratio_pairs:
        headers.append(
            f"{RATIO_OTHER_LABELS[other]} time / {RATIO_BASE_LABELS[base]}"
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
        cells.append(
            f"<td>{_subject_time_ratio_cell(sec_by['zipkeyonly'].get(name), sec_by['zipkeyvalue'].get(name))}</td>"
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


def _load_engine_logs(log_root: Path) -> Dict[str, Dict[str, Any]]:
    """Load per-engine parsed rows from log-root/<engine>/... (db_bench only)."""
    result: Dict[str, Dict[str, Any]] = {}
    for eng in ENGINES:
        eng_dir = log_root / eng
        db_path = eng_dir / "db_bench.log"
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
        result[eng] = {
            "db_bench": db_rows,
            "db_bench_fillrandom": fr_rows,
            "shm_usage": load_shm_usages(eng_dir),
            "rss_usage": load_rss_usages(eng_dir),
            "bench_settings": load_bench_settings(eng_dir),
        }
    for req in TOPLING_ENGINES:
        if req not in result:
            raise SystemExit(f"missing required logs under {log_root}/{req}/")
    return result


def _build_dcompact_bench_notes(runner_env: Dict[str, str]) -> str:
    """Page header notes for dcompact CPU quota and compaction modes."""
    cpu_quota = runner_env.get("cpu_quota_write", "n/a")
    topling_mode = runner_env.get("compact_mode_topling", "dcompact_worker")
    rocksdb_mode = runner_env.get(
        "compact_mode_rocksdb", "compaction_service_spool"
    )
    return f"""<h2>Bench configuration (dcompact)</h2>
  <p class="meta">
    Write-side db_bench CPU quota: <strong>{html.escape(cpu_quota)}</strong>
    (from <code>runner_env.txt</code> <code>cpu_quota_write</code>).
    Topling compact: <strong>{html.escape(topling_mode)}</strong> —
    <code>dcompact_worker</code> runs outside the write cgroup and uses remaining CPU cores.
    RocksDB compact: <strong>{html.escape(rocksdb_mode)}</strong> —
    CompactionService spool + out-of-cgroup broker/worker on remaining cores.
  </p>"""


def _build_yaml_config_links(raw_dir: Path) -> str:
    parts: List[str] = []
    for eng in TOPLING_ENGINES:
        if (raw_dir / eng / YAML_RAW_NAME).is_file():
            label = html.escape(ENGINE_LABELS[eng])
            parts.append(f'<a href="raw/{eng}/{YAML_RAW_NAME}">{label} yaml</a>')
    if not parts:
        return ""
    return (
        '<p class="meta"><strong>ToplingDB bench yaml</strong> '
        "(runtime graft): "
        + " | ".join(parts)
        + "</p>"
    )


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

    for eng in ENGINES:
        eng_raw = raw_dir / eng
        eng_raw.mkdir(parents=True, exist_ok=True)
        src = log_root / eng
        for name in (
            "db_bench.log",
            "db_bench-fillrandom.log",
            "shm_usage.txt",
            "shm_usage-fillrandom.txt",
            "shm_usage-fillseq.txt",
            "rss_usage-fillrandom.txt",
            "rss_usage-fillseq.txt",
            "statm_series-fillrandom.txt",
            "statm_series-fillseq.txt",
            "time-fillrandom.txt",
            "time-fillseq.txt",
            "bench_settings.txt",
            YAML_RAW_NAME,
            "engine-meta.json",
        ):
            p = src / name
            if p.is_file():
                shutil.copy2(p, eng_raw / name)
        if args.engine_meta_root:
            src_meta = Path(args.engine_meta_root) / eng / "engine-meta.json"
            dst_meta = eng_raw / "engine-meta.json"
            if src_meta.is_file() and not dst_meta.is_file():
                shutil.copy2(src_meta, dst_meta)
            log_meta = log_root / eng / "engine-meta.json"
            if src_meta.is_file() and not log_meta.is_file():
                log_meta.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_meta, log_meta)

    runner_env_src = log_root / "runner_env.txt"
    if runner_env_src.is_file():
        shutil.copy2(runner_env_src, raw_dir / "runner_env.txt")

    runner_env = load_runner_env(log_root)

    rss_data: Dict[str, Dict[str, Optional[int]]] = {
        e: engines_data.get(e, {}).get("rss_usage") or {} for e in ENGINES
    }
    rss_table = build_rss_usage_table(rss_data)

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

    db_compare = build_db_bench_compare(
        {e: engines_data.get(e, {}).get("db_bench", []) for e in ENGINES}
    )
    fr_compare = build_db_bench_compare(
        {
            e: engines_data.get(e, {}).get("db_bench_fillrandom", [])
            for e in ENGINES
        }
    )
    shm_usages = {
        e: engines_data.get(e, {}).get("shm_usage") or {} for e in ENGINES
    }
    shm_table = build_shm_usage_table(shm_usages)

    rss_svg_parts: List[str] = []
    pagecache_src = ""
    for eng in ENGINES:
        eng_dir = log_root / eng
        for suite, log_name, bench_key in [
            ("fillrandom", "db_bench-fillrandom.log", "db_bench_fillrandom"),
            ("fillseq", "db_bench.log", "db_bench"),
        ]:
            series_path = eng_dir / f"statm_series-{suite}.txt"
            if not series_path.is_file():
                continue
            series_text = series_path.read_text(encoding="utf-8", errors="replace")
            if not pagecache_src:
                pagecache_src = parse_pagecache_src(series_text)
            start_epoch, page_size, samples = parse_rss_series(series_text)
            if not samples:
                continue
            bench_rows = engines_data.get(eng, {}).get(bench_key, [])
            total_dur = (samples[-1][0] - start_epoch) if samples else 0
            segments = compute_bench_segments(bench_rows, start_epoch, total_dur)
            if segments and segments[0][1] > 0.5:
                segments.insert(0, ("startup", 0, segments[0][1], False))
            svg = build_rss_svg(
                samples, page_size, start_epoch, segments,
                f"{ENGINE_LABELS[eng]} — {suite} suite RAM",
            )
            if svg:
                rss_svg_parts.append(svg)
    rss_svg_section = ""
    if rss_svg_parts:
        if pagecache_src == "meminfo":
            pagecache_meta = (
                "Sampled once per second from /proc/statm plus system Cached "
                "(/proc/meminfo) growth since drop_caches baseline "
                "(workflow cached_pages_use_sys / SYS_CACHED_OF_EMPTY). "
            )
            pagecache_li = (
                f'<li><span style="color:{RSS_LINE_COLORS["pagecache"]};font-weight:600">pagecache</span>: '
                "system-wide /proc/meminfo Cached minus the post-drop_caches baseline "
                "(excluding the inherent consumption of an empty system).</li>\n"
            )
            anony_pc_li = (
                f'<li><span style="color:{RSS_LINE_COLORS["anony+pc"]};font-weight:600">anony+pc</span>: '
                "anony+pagecache (sum of process anonymous RSS and system Cached growth; "
                "not a disjoint partition).</li>\n"
            )
        else:
            pagecache_meta = (
                "Sampled once per second from /proc/statm plus open-file page cache. "
            )
            pagecache_li = (
                f'<li><span style="color:{RSS_LINE_COLORS["pagecache"]};font-weight:600">pagecache</span>: '
                "kernel file page cache for regular files the process currently has open "
                "(cachestat(2) on /proc/pid/fd, deduped by inode; covers buffered readwrite "
                "and mmap). Pages brought in only via buffered I/O are not charged to process RSS; "
                "mmap'd file pages can appear in both pagecache and RSS/shared, so series may "
                "overlap.</li>\n"
            )
            anony_pc_li = (
                f'<li><span style="color:{RSS_LINE_COLORS["anony+pc"]};font-weight:600">anony+pc</span>: '
                "anony+pagecache (sum of process anonymous RSS and open-file page cache; "
                "not a disjoint partition).</li>\n"
            )
        rss_svg_section = (
            '<h2>RSS over time</h2>\n'
            f'<p class="meta">{pagecache_meta}'
            'Colored bands show benchmark segments (start time from db_bench output).</p>\n'
            '<ul class="meta">\n'
            f'<li><span style="color:{RSS_LINE_COLORS["rss"]};font-weight:600">rss</span>: '
            'resident set size (pages currently in RAM for the process); '
            'rss = shared + anony.</li>\n'
            f'<li><span style="color:{RSS_LINE_COLORS["shared"]};font-weight:600">shared</span>: '
            'shared resident pages; mostly readonly (cheap; OS prefers reclaiming these, '
            'no swap needed).</li>\n'
            f'<li><span style="color:{RSS_LINE_COLORS["anony"]};font-weight:600">anony</span>: '
            'rss - shared; '
            'mostly readwrite anonymous pages (costly, needs swap).</li>\n'
            + pagecache_li
            + anony_pc_li
            + '</ul>\n'
            + "\n".join(rss_svg_parts)
        )

    db_bench_detail_keys = [
        "benchmark",
        "micros/op",
        "ops/sec",
        "seconds",
        "operations",
        "extra",
    ]

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

    raw_link_parts: List[str] = []
    for eng in ENGINES:
        label = html.escape(ENGINE_LABELS[eng])
        if (raw_dir / eng / "db_bench.log").is_file():
            raw_link_parts.append(
                f'<a href="raw/{eng}/db_bench.log">{label} db_bench</a>'
            )
    actions_run_url = getattr(args, "actions_run_url", None) or ""
    log_artifact_names: List[str] = []
    for eng in ENGINES:
        label = ENGINE_LABELS[eng]
        for p in sorted((log_root / eng).glob("LOG-*")):
            if p.is_file():
                log_artifact_names.append(f"{label} {p.name}")
    if log_artifact_names and actions_run_url:
        names = ", ".join(html.escape(n) for n in log_artifact_names)
        raw_link_parts.append(
            f'<a href="{html.escape(actions_run_url)}#artifacts">'
            f"DB INFO LOGs (Actions artifact)</a>"
            f'<span class="meta"> — {names}</span>'
        )
    elif log_artifact_names and not actions_run_url:
        raise SystemExit(
            "LOG-* present under --log-root but --actions-run-url was not provided; "
            "refusing to emit pages without an external link (LOGs must not go into gh-pages)"
        )
    raw_links = " | ".join(raw_link_parts)
    yaml_links = _build_yaml_config_links(raw_dir)

    runner_html = _build_runner_section(runner_env, cache_size_bytes, dataset_bytes, dataset_estimated)
    dcompact_notes = _build_dcompact_bench_notes(runner_env)

    body = f"""
  <h1>Bench run: {html.escape(args.variant)} / {html.escape(str(args.run_id))}</h1>
  <p class="meta">
    <a href="../../index.html">← plain home</a> |
    <a href="../../dcompact/index.html">← dcompact</a>
  </p>
  <p class="meta">generated (UTC): {html.escape(datetime.now(timezone.utc).isoformat())}</p>
  <p>{raw_links}</p>
  {yaml_links}
  {dcompact_notes}
  {runner_html}
  <h2>/dev/shm usage (space; after db_bench, before delete)</h2>
  <p class="meta">Allocated disk usage (IEC blocks). RocksDB uses default Snappy compression. Space ratio = engine / v8.10; {_hl('<1 = less space than RocksDB', 'faster')}, {_hl('>1 = larger', 'slower')}.</p>
  {shm_table}
  <h2>Peak RSS (memory; during db_bench)</h2>
  <p class="meta">Peak resident set size. RocksDB block cache = half physical memory ({html.escape(format_iec(cache_size_bytes) if cache_size_bytes else 'n/a')}). Ratio = engine / v8.10; {_hl('<1 = less memory', 'faster')}, {_hl('>1 = more memory', 'slower')}.</p>
  {rss_table}
  {rss_svg_section}
  <h2>Comparison: db_bench fillrandom suite (perf)</h2>
  <p class="meta">Benchmarks: fillrandom, flush, compact, readseq×3, readrandom. RocksDB uses default Snappy compression. compact row shows operations/seconds. RocksDB time / Topling*: {_hl('>1 = that Topling variant faster', 'faster')}. Values show ops/sec.</p>
  {fr_compare}
  <h2>Comparison: db_bench fillseq suite (perf)</h2>
  <p class="meta">Same as fillrandom. Watch {_hl('compact / readseq / readrandom', 'slower')} cost for minDictZip=10 vs {_hl('space savings', 'faster')} above.</p>
  {db_compare}
  <h2>Per-engine details</h2>
  {"".join(detail_parts)}
"""
    (run_dir / "index.html").write_text(
        _page(f"Bench {args.variant} {args.run_id}", body), encoding="utf-8"
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
        "runner_env": runner_env_meta,
        "engines": {
            eng: {
                "db_bench": engines_data.get(eng, {}).get("db_bench", []),
                "db_bench_fillrandom": engines_data.get(eng, {}).get(
                    "db_bench_fillrandom", []
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
    }
    (out / "run-meta.json").write_text(
        json.dumps(meta, indent=2) + "\n", encoding="utf-8"
    )


def _render_dcompact_section(
    entry: Optional[Dict[str, Any]],
    pages_root: Optional[Path] = None,
) -> str:
    if not entry:
        set_rocksdb_master_label(None)
        return "<h2>Latest dcompact run</h2><p><em>no runs yet</em></p>"
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

    runner_env_plain = {
        k: str(v) for k, v in runner_env_data.items()
        if k not in ("cache_size_bytes", "dataset_bytes", "dataset_estimated")
    }
    dcompact_notes = _build_dcompact_bench_notes(runner_env_plain)
    runner_html = _build_runner_section(
        runner_env_data, cache_size_bytes, dataset_bytes, dataset_estimated
    )

    shm_usages = {
        e: engines.get(e, {}).get("shm_usage") or {} for e in ENGINES
    }
    shm_table = build_shm_usage_table(shm_usages)

    rss_data: Dict[str, Dict[str, Optional[int]]] = {
        e: engines.get(e, {}).get("rss_usage") or {} for e in ENGINES
    }
    rss_table = build_rss_usage_table(rss_data)

    db_compare_fs = build_db_bench_compare(
        {e: engines.get(e, {}).get("db_bench", []) for e in ENGINES}
    )
    fr_compare = build_db_bench_compare(
        {e: engines.get(e, {}).get("db_bench_fillrandom", []) for e in ENGINES}
    )

    cache_iec = format_iec(cache_size_bytes) if cache_size_bytes else "n/a"

    return f"""
  <h2>Latest dcompact run</h2>
  <p class="meta">run_id={html.escape(str(entry.get('run_id', '')))} |
     <a href="../runs/{html.escape(run_dir)}/index.html">full report</a> |
     {html.escape(str(entry.get('timestamp', '')))}</p>
  {dcompact_notes}
  {runner_html}
  <h3>/dev/shm usage (disk; ratio vs RocksDB v8.10)</h3>
  <p class="meta">RocksDB uses default Snappy compression. Ratio = engine / v8.10; {_hl('<1 = less space', 'faster')}, {_hl('>1 = larger', 'slower')}.</p>
  {shm_table}
  <h3>Peak RSS (memory; ratio vs RocksDB v8.10)</h3>
  <p class="meta">RocksDB block cache = half physical memory ({html.escape(cache_iec)}). Ratio = engine / v8.10; {_hl('<1 = less memory', 'faster')}, {_hl('>1 = more memory', 'slower')}.</p>
  {rss_table}
  <h3>db_bench fillrandom suite (time ratio = rocksdb / zipkey*)</h3>
  <p class="meta">compact row shows operations/seconds. RocksDB uses default Snappy compression.</p>
  {fr_compare}
  <h3>db_bench fillseq suite</h3>
  {db_compare_fs}
"""


def _render_dcompact_history(history: List[Dict[str, Any]]) -> str:
    dcompact_runs = [e for e in history if e.get("variant") == "dcompact"]
    if not dcompact_runs:
        return "<ul><li><em>empty</em></li></ul>"
    items = []
    for entry in dcompact_runs:
        run_dir = entry.get("run_dir", "")
        items.append(
            "<li>"
            f'{html.escape(str(entry.get("timestamp", "")))} — '
            f'run_id={html.escape(str(entry.get("run_id", "")))} — '
            f'<a href="../runs/{html.escape(run_dir)}/index.html">report</a>'
            "</li>"
        )
    return "<ul>\n" + "\n".join(items) + "\n</ul>"


def _latest_dcompact(history: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for entry in history:
        if entry.get("variant") == "dcompact":
            return entry
    return None


def _ensure_home_dcompact_nav(home_path: Path) -> None:
    """If plain home exists, ensure it links to dcompact/ (idempotent)."""
    if not home_path.is_file():
        return
    text = home_path.read_text(encoding="utf-8")
    if 'href="dcompact/index.html"' in text:
        return
    nav = (
        '\n  <p class="meta">\n'
        '    <a href="dcompact/index.html">dcompact bench →</a>\n'
        "  </p>"
    )
    new, n = re.subn(r"(<h1>[^<]*</h1>)", r"\1" + nav, text, count=1)
    if n == 1:
        home_path.write_text(new, encoding="utf-8")


def merge(args: argparse.Namespace) -> None:
    merge_into = Path(args.merge_into)
    from_dir = Path(args.from_dir)
    merge_into.mkdir(parents=True, exist_ok=True)

    meta_path = from_dir / "run-meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

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
        "runner_env": meta.get("runner_env"),
        "engines": meta.get("engines", {}),
        "db_bench": meta.get("db_bench", []),
        "memtablerep_skiplist": meta.get("memtablerep_skiplist", []),
        "memtablerep_cspp": meta.get("memtablerep_cspp", []),
    }
    history.insert(0, history_entry)
    history_path.write_text(json.dumps(history, indent=2) + "\n", encoding="utf-8")

    (merge_into / ".nojekyll").write_text("", encoding="utf-8")

    latest = _latest_dcompact(history)
    dcompact_section = _render_dcompact_section(latest, merge_into)
    dcompact_body = f"""
  <h1>ToplingDB dcompact bench results</h1>
  <p class="meta">
    <a href="../index.html">← plain home</a>
  </p>
  <p class="meta">Updated (UTC): {html.escape(datetime.now(timezone.utc).isoformat())}</p>
  {dcompact_section}
  <h2>History</h2>
  {_render_dcompact_history(history)}
"""
    dcompact_dir = merge_into / "dcompact"
    dcompact_dir.mkdir(parents=True, exist_ok=True)
    (dcompact_dir / "index.html").write_text(
        _page("ToplingDB dcompact bench results", dcompact_body), encoding="utf-8"
    )
    _ensure_home_dcompact_nav(merge_into / "index.html")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_emit = sub.add_parser("emit", help="Parse dcompact bench logs into _pages fragment")
    p_emit.add_argument("--variant", required=True, choices=["dcompact"])
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

    p_merge = sub.add_parser("merge", help="Merge dcompact emit output into gh-pages tree")
    p_merge.add_argument("--merge-into", required=True)
    p_merge.add_argument("--from", dest="from_dir", required=True)
    p_merge.set_defaults(func=merge)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
