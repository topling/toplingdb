#!/usr/bin/env python3
"""Unit checks for statm series parse + RSS SVG (pagecache / anony+pc)."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def check(mod) -> None:
    text = (
        "# start_epoch=100.0  page_size=4096  cachestat=ok  "
        "fields=size,resident,shared,text,lib,data,dt,pagecache\n"
        "100.0 1000 800 200 1 0 50 0 512\n"
        "101.0 1000 900 250 1 0 50 0 1024\n"
        "102.0 1000 850 220 1 0 50 0 2048\n"
    )
    assert mod.parse_pagecache_src(text) == "cachestat"
    meminfo_hdr = (
        "# start_epoch=100.0  page_size=4096  pagecache_src=meminfo  "
        "sys_cached_of_empty_kb=100  "
        "fields=size,resident,shared,text,lib,data,dt,pagecache\n"
    )
    assert mod.parse_pagecache_src(meminfo_hdr) == "meminfo"
    start, page_size, samples = mod.parse_rss_series(text)
    assert start == 100.0, start
    assert page_size == 4096, page_size
    assert samples[0] == (100.0, 800, 200, 512), samples[0]
    assert samples[-1][3] == 2048, samples[-1]

    # Legacy full statm without pagecache column.
    _, _, legacy = mod.parse_rss_series("10.0 100 80 20 1 0 5 0\n")
    assert legacy[0] == (10.0, 80, 20, 0), legacy[0]

    svg = mod.build_rss_svg(
        samples, page_size, start, [("fill", 0.0, 2.0, False)], "test"
    )
    assert "pagecache" in svg
    assert "anony+pc" in svg
    assert mod.RSS_LINE_COLORS["pagecache"] in svg
    assert mod.RSS_LINE_COLORS["anony+pc"] in svg
    assert svg.count("<polyline") == 5

    legend_order = ("rss", "shared", "anony", "pagecache", "anony+pc")
    header_y = mod.RSS_HEADER_Y
    gap = mod.RSS_SWATCH_TEXT_GAP
    # Every swatch→label gap must be identical (fixed start-anchor offset).
    gaps = []
    for label in legend_order:
        lm = re.search(
            rf'<line x1="([\d.]+)" y1="[\d.]+" x2="([\d.]+)" y2="[\d.]+" '
            rf'stroke="[^"]+" stroke-width="3"/>\s*'
            rf'<text x="([\d.]+)" y="{header_y}" text-anchor="start" '
            rf'font-size="13\.5"[^>]*>'
            rf'{re.escape(label)}</text>',
            svg,
        )
        assert lm, label
        x2, tx = float(lm.group(2)), float(lm.group(3))
        gaps.append(tx - x2)
    assert len(set(round(g, 3) for g in gaps)) == 1, gaps
    assert abs(gaps[0] - float(gap)) < 1e-6, gaps[0]

    # Packed block ends at plot frame right (margin_l + chart_w).
    plot_right = float(mod.RSS_MARGIN_L + mod.RSS_CHART_W)
    last_w = float(mod.RSS_LEGEND_TEXT_W["anony+pc"])
    last = re.search(
        rf'<line x1="([\d.]+)"[^/]*/>\s*'
        rf'<text x="([\d.]+)" y="{header_y}" text-anchor="start"[^>]*>'
        rf'anony\+pc</text>',
        svg,
    )
    assert last
    assert abs(float(last.group(2)) + last_w - plot_right) < 1e-6


def main() -> int:
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))
    chart = load("bench_rss_chart", here / "bench_rss_chart.py")
    check(chart)
    print("OK bench_rss_chart")
    for name in ("bench_logs_to_pages", "bench_dcompact_pages"):
        mod = load(name, here / f"{name}.py")
        # Pages import the shared module (importlib may load a second copy).
        assert mod.build_rss_svg.__module__ == "bench_rss_chart"
        assert mod.RSS_LINE_COLORS == chart.RSS_LINE_COLORS
        print(f"OK {name} (imports shared chart)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
