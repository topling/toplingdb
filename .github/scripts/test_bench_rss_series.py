#!/usr/bin/env python3
"""Unit checks for statm series parse + four-line RSS SVG (fd_cache)."""

from __future__ import annotations

import importlib.util
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
        "fields=size,resident,shared,text,lib,data,dt,fd_cache\n"
        "100.0 1000 800 200 1 0 50 0 512\n"
        "101.0 1000 900 250 1 0 50 0 1024\n"
        "102.0 1000 850 220 1 0 50 0 2048\n"
    )
    start, page_size, samples = mod.parse_rss_series(text)
    assert start == 100.0, start
    assert page_size == 4096, page_size
    assert samples[0] == (100.0, 800, 200, 512), samples[0]
    assert samples[-1][3] == 2048, samples[-1]

    # Legacy full statm without fd_cache column.
    _, _, legacy = mod.parse_rss_series("10.0 100 80 20 1 0 5 0\n")
    assert legacy[0] == (10.0, 80, 20, 0), legacy[0]

    svg = mod.build_rss_svg(
        samples, page_size, start, [("fill", 0.0, 2.0, False)], "test"
    )
    assert "fd_cache" in svg
    assert "#a05a00" in svg
    assert svg.count("<polyline") == 4


def main() -> int:
    here = Path(__file__).resolve().parent
    for name in ("bench_logs_to_pages", "bench_dcompact_pages"):
        check(load(name, here / f"{name}.py"))
        print(f"OK {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
