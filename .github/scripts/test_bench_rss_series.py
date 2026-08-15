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
    assert "rotate(-30" not in svg
    assert ">fill</text>" in svg
    assert ">2.0</text>" in svg
    # No leader labels: x-axis numbers sit just under the strip.
    axis_y = (
        mod.RSS_MARGIN_T + mod.RSS_CHART_H + mod.RSS_STRIP_H + 16
    )
    assert f'y="{axis_y}"' in svg

    merged_svg = mod.build_rss_svg(
        samples,
        page_size,
        start,
        [
            ("readseq", 0.0, 0.4, False),
            ("readseq", 0.4, 0.8, False),
            ("readseq", 0.8, 1.2, False),
            ("readrandom", 1.2, 2.0, False),
        ],
        "merge",
    )
    assert "readseq×3" in merged_svg
    assert ">1.2</text>" in merged_svg
    assert merged_svg.count(">readseq</text>") == 0
    assert ">readrandom</text>" in merged_svg

    rs3 = mod._text_w("readseq×3", 12.0)
    assert abs(rs3 - 56.73) < 0.2, rs3

    # Narrow cell may still keep a two-line in-strip label if it does not
    # overlap the neighbor's name (58.5px readseq×3 vs a distant readrandom).
    tight_svg = mod.build_rss_svg(
        samples,
        page_size,
        start,
        [
            ("readseq", 0.0, 0.0325, False),
            ("readseq", 0.0325, 0.065, False),
            ("readseq", 0.065, 0.0975, False),
            ("readrandom", 0.0975, 2.0, False),
        ],
        "tight",
    )
    assert ">readseq×3</text>" in tight_svg
    assert "readseq×3 0.1" not in tight_svg

    # Adjacent short cells with long names: one label must leave the strip.
    overlap_svg = mod.build_rss_svg(
        samples,
        page_size,
        start,
        [
            ("compact", 0.0, 0.03, False),
            ("startup", 0.03, 0.06, False),
            ("fill", 0.06, 2.0, False),
        ],
        "overlap",
    )
    assert "compact 0.0" in overlap_svg or "startup 0.0" in overlap_svg
    strip_ys = [
        float(y)
        for y in re.findall(
            rf'<rect x="[^"]+" y="([\d.]+)" width="[^"]+" '
            rf'height="{mod.RSS_STRIP_H}"',
            svg,
        )
    ]
    assert strip_ys
    plot_bottom = float(mod.RSS_MARGIN_T + mod.RSS_CHART_H)
    assert all(y >= plot_bottom - 0.5 for y in strip_ys), strip_ys

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
        assert (
            mod._fmt_utc("2026-08-14T13:15:15.583652+00:00")
            == "2026-08-14 13:15:15+00:00"
        )
        assert mod._fmt_utc("2026-08-14 13:15:15+00:00") == "2026-08-14 13:15:15+00:00"
        assert mod._fmt_utc("") == ""
        assert mod._href("raw", "zipkeyonly", "db_bench.log") == "raw/zipkeyonly/db_bench.log"
        assert (
            mod._href("../runs/a b/raw", "zipkeyonly", "x.yaml")
            == "../runs/a%20b/raw/zipkeyonly/x.yaml"
        )
        assert not mod._artifact_log_link("https://example/run", False)
        assert "artifacts" in mod._artifact_log_link("https://example/run", True)
        bare = mod._page("t", "<p>x</p>", include_chart_js=False)
        assert "initWrap" not in bare
        full = mod._page("t", "<p>x</p>", include_chart_js=True)
        assert "initWrap" in full
        print(f"OK {name} (imports shared chart)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
