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
    assert "#a05a00" in svg
    assert "#6b21a8" in svg
    assert svg.count("<polyline") == 5

    # Legend text x: each label's slot covers prior label length so long names
    # (pagecache → anony+pc) stay separated.
    legend_order = ("rss", "shared", "anony", "pagecache", "anony+pc")
    xs_by_label = {
        m.group(2): float(m.group(1))
        for m in re.finditer(
            r'<text x="([\d.]+)" y="33" font-size="9"[^>]*>([^<]+)</text>', svg
        )
    }
    assert all(name in xs_by_label for name in legend_order), xs_by_label
    for prev, cur in zip(legend_order, legend_order[1:]):
        # text starts at swatch+4; next item is at least 18+7*len(prev)+14 later
        # from the same item origin → text-to-text gap >= 7*len(prev)+14.
        min_gap = 7 * len(prev) + 14
        gap = xs_by_label[cur] - xs_by_label[prev]
        assert gap >= min_gap, (prev, cur, gap, min_gap)


def main() -> int:
    here = Path(__file__).resolve().parent
    for name in ("bench_logs_to_pages", "bench_dcompact_pages"):
        check(load(name, here / f"{name}.py"))
        print(f"OK {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
