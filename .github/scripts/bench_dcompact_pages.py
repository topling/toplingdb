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
import math
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

_SEGMENT_COLORS = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
]


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
    """Annotate RocksDB master label with short git SHA from engine-meta.json."""
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
    """ratio = other / base; <1 means other uses less space (better compression)."""
    if base_bytes is None or other_bytes is None or base_bytes <= 0:
        return "—"
    ratio = other_bytes / base_bytes
    cls = "faster" if ratio < 1.0 else "slower"
    return f'<span class="{cls}">{ratio:.2f}x</span>'


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
    headers.append("Topling / v8.10 (space)")
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
) -> str:
    """Peak RSS compare: absolute + ratio vs RocksDB v8.10."""
    headers = ["workload"]
    for e in ENGINES:
        headers.append(ENGINE_LABELS[e])
    headers.append("Topling / v8.10 (RSS)")
    headers.append("zipkeyvalue / v8.10 (RSS)")

    workloads = sorted(
        {wl for eng_data in rss_data.values() for wl in eng_data if eng_data.get(wl) is not None}
    )
    if not workloads:
        workloads = list(RSS_WORKLOADS)

    rows_html = []
    for wl in workloads:
        cells = [f"<td>{html.escape(wl)}</td>"]
        for e in ENGINES:
            b = (rss_data.get(e) or {}).get(wl)
            cells.append(
                f"<td>{html.escape(format_iec(b)) if b is not None else 'n/a'}</td>"
            )
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


def parse_pagecache_src(text: str) -> str:
    """Return pagecache source from series header: 'meminfo', 'cachestat', or ''."""
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("#"):
            continue
        for part in line[1:].split():
            if part.startswith("pagecache_src="):
                return part.split("=", 1)[1]
            if part.startswith("cachestat="):
                return "cachestat"
        break
    return ""


def parse_rss_series(
    text: str,
) -> Tuple[float, int, List[Tuple[float, int, int, int]]]:
    """Parse statm/rss series -> (start_epoch, page_size, [(epoch, rss, shared, pagecache)...]).

    New format: <epoch> <size> <resident> <shared> <text> <lib> <data> <dt> [<pagecache>]
    Legacy:     <epoch> <resident>  (shared/pagecache treated as 0)
    Pages are converted by the caller; anony = rss - shared.
    """
    start_epoch = 0.0
    page_size = 4096
    samples: List[Tuple[float, int, int, int]] = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#"):
            for part in line[1:].split():
                if part.startswith("start_epoch="):
                    start_epoch = float(part.split("=", 1)[1])
                elif part.startswith("page_size="):
                    page_size = int(part.split("=", 1)[1])
            continue
        if not line:
            continue
        parts = line.split()
        if len(parts) >= 4:
            pagecache = int(parts[8]) if len(parts) >= 9 else 0
            samples.append(
                (float(parts[0]), int(parts[2]), int(parts[3]), pagecache)
            )
        elif len(parts) == 3:
            # epoch size resident (shared missing)
            samples.append((float(parts[0]), int(parts[2]), 0, 0))
        elif len(parts) >= 2:
            # legacy: epoch resident
            samples.append((float(parts[0]), int(parts[1]), 0, 0))
    return start_epoch, page_size, samples


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


def _pow10_tick_step(data_max: float, target_ticks: float = 5.0) -> float:
    """Nice axis-extent unit: 1, 10, 100, 1000, ... (grid/labels use half of this)."""
    if data_max <= 0:
        return 1.0
    rough = data_max / target_ticks
    exp = math.floor(math.log10(max(rough, 1e-12)))
    step = 10.0 ** exp
    # Prefer fewer coarse ticks over many 100-unit labels (e.g. 1888 → 1000).
    while data_max / step > 8:
        step *= 10.0
    return step


def _ceil_to_step(value: float, step: float) -> float:
    if step <= 0:
        return max(value, 1.0)
    return max(step, math.ceil(value / step - 1e-12) * step)


def _axis_multiples(step: float, axis_max: float, include_zero: bool) -> List[float]:
    vals: List[float] = []
    k = 0 if include_zero else 1
    while True:
        v = k * step
        if v > axis_max + 1e-9:
            break
        vals.append(v)
        k += 1
    return vals


def build_rss_svg(
    samples: List[Tuple[float, int, int, int]],
    page_size: int,
    start_epoch: float,
    segments: List[Tuple[str, float, float, bool]],
    title: str,
) -> str:
    """SVG: rss/shared/anony/pagecache/anony+pc over time with segment bands."""
    if not samples:
        return ""

    mib = page_size / (1024 * 1024)
    xs = [t - start_epoch for t, _, _, _ in samples]
    ys_rss = [res * mib for _, res, _, _ in samples]
    ys_shared = [shr * mib for _, _, shr, _ in samples]
    ys_anony = [max(0, res - shr) * mib for _, res, shr, _ in samples]
    ys_pagecache = [fdc * mib for _, _, _, fdc in samples]
    ys_anony_pc = [a + f for a, f in zip(ys_anony, ys_pagecache)]

    x_data = max(xs) if xs else 1.0
    y_data = (
        max(ys_rss + ys_shared + ys_anony + ys_pagecache + ys_anony_pc)
        if samples
        else 1.0
    )
    if x_data <= 0:
        x_data = 1.0
    if y_data <= 0:
        y_data = 1.0
    # Axis extent from 10^n units; grid + labels at half-step (5/50/500...).
    x_unit = _pow10_tick_step(x_data)
    y_unit = _pow10_tick_step(y_data)
    x_max = _ceil_to_step(x_data, x_unit)
    y_max = _ceil_to_step(y_data, y_unit)
    x_grid = x_unit / 2.0
    y_grid = y_unit / 2.0

    margin_l, margin_r, margin_t, margin_b = 70, 20, 40, 50
    chart_w, chart_h = 800, 300
    svg_w = margin_l + chart_w + margin_r
    svg_h = margin_t + chart_h + margin_b

    def tx(v: float) -> float:
        return margin_l + (v / x_max) * chart_w if x_max else margin_l

    # Inset the plotable y-range so a series at data peak (common once block
    # cache fills) is not glued to the top edge — that reads as "no line" on
    # long flat plateaus (e.g. RocksDB fillseq readrandom). Same for tiny shared.
    _pad_top, _pad_bot = 8.0, 4.0
    _y_usable = chart_h - _pad_top - _pad_bot

    def ty(v: float) -> float:
        return margin_t + _pad_top + _y_usable * (1.0 - v / y_max)

    parts: List[str] = []
    parts.append('<div class="rss-chart-wrap">')
    parts.append(
        f'<svg class="rss-chart" xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {svg_w} {svg_h}" overflow="visible" '
        f'style="max-width:{svg_w}px;width:100%;height:auto;'
        f'font-family:system-ui,sans-serif;font-size:11px;cursor:crosshair">'
    )
    parts.append(f'<text x="{svg_w // 2}" y="18" text-anchor="middle" '
                 f'font-size="13" font-weight="600">{html.escape(title)}</text>')

    for idx, (name, s_start, s_end, est) in enumerate(segments):
        color = _SEGMENT_COLORS[idx % len(_SEGMENT_COLORS)]
        sx1 = tx(max(s_start, 0))
        sx2 = tx(min(s_end, x_max))
        if sx2 <= sx1:
            continue
        parts.append(
            f'<rect x="{sx1:.1f}" y="{margin_t}" width="{sx2 - sx1:.1f}" '
            f'height="{chart_h}" fill="{color}" opacity="0.15"/>'
        )
        label = name
        if est:
            label += " (est.)"
        mid_x = (sx1 + sx2) / 2
        parts.append(
            f'<text x="{mid_x:.1f}" y="{margin_t + chart_h + 14}" '
            f'text-anchor="middle" font-size="9" fill="{color}" '
            f'transform="rotate(-30 {mid_x:.1f} {margin_t + chart_h + 14})">'
            f'{html.escape(label)}</text>'
        )

    # Axes (full plot frame so series never looks like the chart border)
    parts.append(
        f'<line x1="{margin_l}" y1="{margin_t}" x2="{margin_l}" '
        f'y2="{margin_t + chart_h}" stroke="#666" stroke-width="1"/>'
    )
    parts.append(
        f'<line x1="{margin_l + chart_w}" y1="{margin_t}" '
        f'x2="{margin_l + chart_w}" y2="{margin_t + chart_h}" stroke="#666" stroke-width="1"/>'
    )
    parts.append(
        f'<line x1="{margin_l}" y1="{margin_t}" '
        f'x2="{margin_l + chart_w}" y2="{margin_t}" stroke="#666" stroke-width="1"/>'
    )
    parts.append(
        f'<line x1="{margin_l}" y1="{margin_t + chart_h}" '
        f'x2="{margin_l + chart_w}" y2="{margin_t + chart_h}" stroke="#666" stroke-width="1"/>'
    )

    # Grid at 5/50/500.... Skip x==x_max so the light grid does not paint over
    # the right frame; y==y_max is inset by padT so it stays inside the plot.
    for val in _axis_multiples(y_grid, y_max, include_zero=True):
        yp = ty(val)
        parts.append(
            f'<line x1="{margin_l}" y1="{yp:.1f}" '
            f'x2="{margin_l + chart_w}" y2="{yp:.1f}" '
            f'stroke="#e6e6e6" stroke-width="1"/>'
        )
    for val in _axis_multiples(x_grid, x_max, include_zero=False):
        if val >= x_max - 1e-9:
            continue
        xp = tx(val)
        parts.append(
            f'<line x1="{xp:.1f}" y1="{margin_t}" '
            f'x2="{xp:.1f}" y2="{margin_t + chart_h}" '
            f'stroke="#e6e6e6" stroke-width="1"/>'
        )

    # Label every grid line (5/50/500...), including 0 and axis max.
    for val in _axis_multiples(y_grid, y_max, include_zero=True):
        yp = ty(val)
        parts.append(
            f'<line x1="{margin_l - 4}" y1="{yp:.1f}" '
            f'x2="{margin_l}" y2="{yp:.1f}" stroke="#666"/>'
        )
        parts.append(
            f'<text x="{margin_l - 6}" y="{yp + 3:.1f}" '
            f'text-anchor="end" font-size="10">{val:.0f}</text>'
        )
    parts.append(
        f'<text x="14" y="{margin_t + chart_h // 2}" '
        f'text-anchor="middle" font-size="11" '
        f'transform="rotate(-90 14 {margin_t + chart_h // 2})">MiB</text>'
    )
    for val in _axis_multiples(x_grid, x_max, include_zero=True):
        xp = tx(val)
        parts.append(
            f'<line x1="{xp:.1f}" y1="{margin_t + chart_h}" '
            f'x2="{xp:.1f}" y2="{margin_t + chart_h + 4}" stroke="#666"/>'
        )
        parts.append(
            f'<text x="{xp:.1f}" y="{margin_t + chart_h + 38}" '
            f'text-anchor="middle" font-size="10">{val:.0f}</text>'
        )
    parts.append(
        f'<text x="{margin_l + chart_w // 2}" y="{svg_h - 2}" '
        f'text-anchor="middle" font-size="11">Time (s)</text>'
    )

    # Draw sum last so anony+pc stays visible above other series.
    series = (
        ("anony", ys_anony, "#c11618"),
        ("rss", ys_rss, "#1558a8"),
        ("shared", ys_shared, "#258825"),
        ("pagecache", ys_pagecache, "#a05a00"),
        ("anony+pc", ys_anony_pc, "#6b21a8"),
    )
    for label, ys, color in series:
        points = " ".join(f"{tx(x):.1f},{ty(y):.1f}" for x, y in zip(xs, ys))
        parts.append(
            f'<polyline points="{points}" fill="none" stroke="{color}" '
            f'stroke-width="2"/>'
        )
    # Legend above the plot (under title) so it never overlaps series at y_max.
    legend = (
        ("rss", "#1558a8"),
        ("shared", "#258825"),
        ("anony", "#c11618"),
        ("pagecache", "#a05a00"),
        ("anony+pc", "#6b21a8"),
    )
    # Per-label width: swatch+gap (18) + ~7px/char at size 9 + trailing pad.
    # Generous vs system-ui so "pagecache" / "anony+pc" do not collide.
    item_widths = [18 + 7 * len(label) + 14 for label, _ in legend]
    x = margin_l + chart_w - sum(item_widths)
    ly = 30
    for (label, color), w in zip(legend, item_widths):
        parts.append(
            f'<line x1="{x}" y1="{ly}" x2="{x + 14}" y2="{ly}" '
            f'stroke="{color}" stroke-width="2"/>'
        )
        parts.append(
            f'<text x="{x + 18}" y="{ly + 3}" font-size="9" fill="#333">'
            f'{label}</text>'
        )
        x += w

    parts.append(
        f'<g class="rss-crosshair" style="display:none">'
        f'<line class="rss-vline" y1="{margin_t}" y2="{margin_t + chart_h}" '
        f'stroke="#555" stroke-width="1" stroke-dasharray="4 3"/>'
        f'<g class="rss-marks"></g>'
        f"</g>"
    )
    parts.append(
        f'<rect class="rss-hit" x="{margin_l}" y="{margin_t}" '
        f'width="{chart_w}" height="{chart_h}" fill="transparent"/>'
    )
    parts.append("</svg>")
    ys_by_name = {
        "rss": ys_rss,
        "shared": ys_shared,
        "anony": ys_anony,
        "pagecache": ys_pagecache,
        "anony+pc": ys_anony_pc,
    }
    chart_data = {
        "xs": [round(v, 3) for v in xs],
        "series": [
            {
                "name": label,
                "ys": [round(v, 2) for v in ys_by_name[label]],
                "color": color,
            }
            for label, color in legend
        ],
        "layout": {
            "ml": margin_l,
            "mt": margin_t,
            "cw": chart_w,
            "ch": chart_h,
            "xMax": round(x_max, 3),
            "yMax": round(y_max, 3),
            "padT": _pad_top,
            "padB": _pad_bot,
        },
    }
    parts.append(
        '<script type="application/json" class="rss-chart-data">'
        + json.dumps(chart_data, separators=(",", ":"))
        + "</script>"
    )
    parts.append("</div>")
    return "\n".join(parts)


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


_RSS_CHART_JS = r"""
<script>
(function () {
  function nearestIdx(xs, x) {
    var lo = 0, hi = xs.length - 1;
    if (x <= xs[0]) return 0;
    if (x >= xs[hi]) return hi;
    while (lo < hi - 1) {
      var mid = (lo + hi) >> 1;
      if (xs[mid] <= x) lo = mid; else hi = mid;
    }
    return (x - xs[lo] <= xs[hi] - x) ? lo : hi;
  }
  function initWrap(wrap) {
    var svg = wrap.querySelector("svg.rss-chart");
    var dataEl = wrap.querySelector(".rss-chart-data");
    if (!svg || !dataEl) return;
    var data = JSON.parse(dataEl.textContent);
    var xs = data.xs, series = data.series, L = data.layout;
    var hit = svg.querySelector(".rss-hit");
    var ch = svg.querySelector(".rss-crosshair");
    var vline = svg.querySelector(".rss-vline");
    var marks = svg.querySelector(".rss-marks");
    if (!hit || !ch || !vline || !marks) return;
    function hide() { ch.style.display = "none"; }
    function show(ev) {
      var ctm = svg.getScreenCTM();
      if (!ctm) return;
      var pt = svg.createSVGPoint();
      pt.x = ev.clientX; pt.y = ev.clientY;
      var p = pt.matrixTransform(ctm.inverse());
      if (p.x < L.ml || p.x > L.ml + L.cw || p.y < L.mt || p.y > L.mt + L.ch) {
        hide(); return;
      }
      var xVal = L.xMax ? ((p.x - L.ml) / L.cw) * L.xMax : 0;
      var i = nearestIdx(xs, xVal);
      var xp = L.xMax ? L.ml + (xs[i] / L.xMax) * L.cw : L.ml;
      vline.setAttribute("x1", xp.toFixed(1));
      vline.setAttribute("x2", xp.toFixed(1));
      while (marks.firstChild) marks.removeChild(marks.firstChild);
      var NS = "http://www.w3.org/2000/svg";
      series.forEach(function (s, si) {
        var y = s.ys[i];
        var padT = L.padT || 0, padB = L.padB || 0;
        var usable = L.ch - padT - padB;
        var yp = L.mt + padT + usable * (1 - y / L.yMax);
        var dot = document.createElementNS(NS, "circle");
        dot.setAttribute("cx", xp.toFixed(1));
        dot.setAttribute("cy", yp.toFixed(1));
        dot.setAttribute("r", "3.5");
        dot.setAttribute("fill", s.color);
        marks.appendChild(dot);
        var tip = s.name + " (" + xs[i].toFixed(1) + "s, " + y.toFixed(1) + ")";
        var tipW = Math.max(72, 6.4 * tip.length + 10);
        var tipH = 16;
        var tipX = Math.min(Math.max(xp + 8, L.ml), L.ml + L.cw - tipW);
        var tipY = yp - tipH - 6 - si * (tipH + 2);
        if (tipY < L.mt) tipY = yp + 8 + si * (tipH + 2);
        var rect = document.createElementNS(NS, "rect");
        rect.setAttribute("x", tipX.toFixed(1));
        rect.setAttribute("y", tipY.toFixed(1));
        rect.setAttribute("width", tipW.toFixed(1));
        rect.setAttribute("height", tipH.toFixed(1));
        rect.setAttribute("rx", "3");
        rect.setAttribute("fill", "#fff");
        rect.setAttribute("stroke", s.color);
        rect.setAttribute("stroke-width", "1");
        rect.setAttribute("opacity", "0.95");
        marks.appendChild(rect);
        var text = document.createElementNS(NS, "text");
        text.setAttribute("x", (tipX + tipW / 2).toFixed(1));
        text.setAttribute("y", (tipY + 12).toFixed(1));
        text.setAttribute("text-anchor", "middle");
        text.setAttribute("font-size", "10");
        text.setAttribute("fill", "#222");
        text.textContent = tip;
        marks.appendChild(text);
      });
      ch.style.display = "";
    }
    hit.addEventListener("mousemove", show);
    hit.addEventListener("mouseleave", hide);
  }
  document.querySelectorAll(".rss-chart-wrap").forEach(initWrap);
})();
</script>
"""


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


def _build_runner_section(
    runner_env: Dict[str, Any],
    cache_size_bytes: Optional[int],
    dataset_bytes: Optional[int],
    dataset_estimated: bool,
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
    if cache_size_bytes is not None and dataset_bytes is not None:
        cache_iec = format_iec(cache_size_bytes)
        ds_iec = format_iec(dataset_bytes)
        est_note = " (estimated)" if dataset_estimated else ""
        if cache_size_bytes >= dataset_bytes:
            section += f'\n<p class="meta"><strong>On-disk DB size{est_note} ({ds_iec}) ≤ block cache ({cache_iec})</strong> — cache can hold the entire dataset.</p>'
        else:
            section += f'\n<p class="meta" style="color:#a30d0d"><strong>On-disk DB size{est_note} ({ds_iec}) &gt; block cache ({cache_iec})</strong> — cache cannot hold the entire dataset.</p>'
    return section


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
                f"{ENGINE_LABELS[eng]} — {suite} suite RSS",
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
                '<li><span style="color:#a05a00;font-weight:600">pagecache</span>: '
                "system-wide /proc/meminfo Cached minus the post-drop_caches baseline "
                "(excluding the inherent consumption of an empty system).</li>\n"
            )
            anony_pc_li = (
                '<li><span style="color:#6b21a8;font-weight:600">anony+pc</span>: '
                "anony+pagecache (sum of process anonymous RSS and system Cached growth; "
                "not a disjoint partition).</li>\n"
            )
        else:
            pagecache_meta = (
                "Sampled once per second from /proc/statm plus open-file page cache. "
            )
            pagecache_li = (
                '<li><span style="color:#a05a00;font-weight:600">pagecache</span>: '
                "kernel file page cache for regular files the process currently has open "
                "(cachestat(2) on /proc/pid/fd, deduped by inode; covers buffered readwrite "
                "and mmap). Pages brought in only via buffered I/O are not charged to process RSS; "
                "mmap'd file pages can appear in both pagecache and RSS/shared, so series may "
                "overlap.</li>\n"
            )
            anony_pc_li = (
                '<li><span style="color:#6b21a8;font-weight:600">anony+pc</span>: '
                "anony+pagecache (sum of process anonymous RSS and open-file page cache; "
                "not a disjoint partition).</li>\n"
            )
        rss_svg_section = (
            '<h2>RSS over time</h2>\n'
            f'<p class="meta">{pagecache_meta}'
            'Colored bands show benchmark segments (start time from db_bench output).</p>\n'
            '<ul class="meta">\n'
            '<li><span style="color:#1558a8;font-weight:600">rss</span>: '
            'resident set size (pages currently in RAM for the process); '
            'rss = shared + anony.</li>\n'
            '<li><span style="color:#258825;font-weight:600">shared</span>: '
            'shared resident pages; mostly readonly (cheap; OS prefers reclaiming these, '
            'no swap needed).</li>\n'
            '<li><span style="color:#c11618;font-weight:600">anony</span>: '
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
