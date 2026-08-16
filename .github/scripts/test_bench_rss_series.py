#!/usr/bin/env python3
"""Unit checks for statm series parse + RSS SVG (pagecache / anony+pc)."""

from __future__ import annotations

import argparse
import importlib.util
import re
import sys
import tempfile
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


_DB_BENCH_LINE = (
    "fillseq : 1.0 micros/op 1000 ops/sec 1.0 seconds 1000 operations; x\n"
)
_STATM_SERIES = (
    "# start_epoch=100.0  page_size=4096  cachestat=ok  "
    "fields=size,resident,shared,text,lib,data,dt,pagecache\n"
    "100.0 1000 800 200 1 0 50 0 512\n"
    "101.0 1000 900 250 1 0 50 0 1024\n"
)


def _write_min_logs(log_root: Path) -> None:
    log_root.mkdir(parents=True, exist_ok=True)
    (log_root / "bench_settings.txt").write_text(
        "num=1000\nkey_size=8\nvalue_size=16\n", encoding="utf-8"
    )
    (log_root / "runner_env.txt").write_text(
        "os_pretty_name=TestOS\nkernel=1.0\ncpu_model=TestCPU\ncpu_count=2\n",
        encoding="utf-8",
    )
    for eng in ("zipkeyonly", "zipkeyvalue"):
        eng_dir = log_root / eng
        eng_dir.mkdir(parents=True, exist_ok=True)
        (eng_dir / "db_bench-fillrandom.log").write_text(
            "$ fillrandom\n" + _DB_BENCH_LINE, encoding="utf-8"
        )
        (eng_dir / "db_bench-fillrandom-omit.log").write_text(
            "$ fillrandom-omit\n", encoding="utf-8"
        )
        (eng_dir / "db_bench.log").write_text(
            "$ fillseq\n" + _DB_BENCH_LINE, encoding="utf-8"
        )
        (eng_dir / "db_bench-fillseq-omit.log").write_text(
            "$ fillseq-omit\n", encoding="utf-8"
        )
        (eng_dir / "statm_series-fillseq.txt").write_text(
            _STATM_SERIES, encoding="utf-8"
        )


def check_pages_contract(mod, variant: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        log_root = tmp_path / "logs"
        emit_out = tmp_path / "emit"
        site = tmp_path / "site"
        _write_min_logs(log_root)
        emit_args = argparse.Namespace(
            variant=variant,
            run_id="id with space",
            log_root=str(log_root),
            engine_meta_root=None,
            actions_run_url="",
            out=str(emit_out),
        )
        mod.emit(emit_args)
        run_dirs = list((emit_out / "runs").iterdir())
        assert len(run_dirs) == 1, run_dirs
        result_html = (run_dirs[0] / "index.html").read_text(encoding="utf-8")
        assert "initWrap" not in result_html
        assert "Comparison:" not in result_html
        assert "Result table:" in result_html
        assert "db_bench-all.log" in result_html
        combined = (
            run_dirs[0] / "raw" / "zipkeyonly" / "db_bench-all.log"
        ).read_text(encoding="utf-8")
        assert combined.index("$ fillrandom\n") < combined.index("$ fillrandom-omit\n")
        assert combined.index("$ fillrandom-omit\n") < combined.index("$ fillseq\n")
        assert combined.index("$ fillseq\n") < combined.index("$ fillseq-omit\n")

        merge_kw = {
            "merge_into": str(site),
            "from_dir": str(emit_out),
        }
        if variant != "dcompact":
            merge_kw["variant"] = variant
        mod.merge(argparse.Namespace(**merge_kw))
        if variant == "dcompact":
            home = (site / "dcompact" / "index.html").read_text(encoding="utf-8")
            result_href = "../runs/"
        else:
            home = (site / "index.html").read_text(encoding="utf-8")
            result_href = "runs/"
        assert "initWrap" in home
        assert "Comparison:" in home
        assert "RAM usage over time" in home
        assert "result table" in home
        assert f"{result_href}" in home
        assert "id%20with%20space/index.html" in home


def check_ratio_normalization(mod) -> None:
    def row(ops_per_sec: int, seconds: float, operations: int):
        return {
            "benchmark": "readseq",
            "ops/sec": str(ops_per_sec),
            "seconds": str(seconds),
            "operations": str(operations),
        }

    engines = {
        "zipkeyonly": [row(23399410, 2.137, 50000000)],
        "zipkeyvalue": [row(16861410, 2.965, 50000000)],
        "rocksdb-v8.10": [row(9910866, 3.189, 31608738)],
        "rocksdb-master": [row(8339629, 3.790, 31604185)],
    }
    table = mod.build_db_bench_compare(engines)
    assert "zipkeyonly / v8.10" in table
    assert "zipkeyonly / zipkeyvalue" in table
    assert '<span class="faster">2.36x</span>' in table
    assert '<span class="faster">2.81x</span>' in table
    assert '<span class="faster">1.70x</span>' in table
    assert '<span class="faster">2.02x</span>' in table
    assert "<td>1.39x</td>" in table
    assert "1.49x" not in table

    unequal_ops = {
        "zipkeyonly": [row(20000000, 1.0, 20000000)],
        "zipkeyvalue": [row(10000000, 4.0, 40000000)],
        "rocksdb-v8.10": [row(5000000, 2.0, 10000000)],
        "rocksdb-master": [row(4000000, 2.0, 8000000)],
    }
    unequal_table = mod.build_db_bench_compare(unequal_ops)
    assert "<td>2.00x</td>" in unequal_table
    assert "<td>4.00x</td>" not in unequal_table

    if hasattr(mod, "build_lazy_load_compare"):
        lazy = mod.build_lazy_load_compare(
            {
                "zipkeyonly": [row(36570960, 1.367, 50000000)],
                "zipkeyvalue": [row(37165215, 1.345, 50000000)],
                "rocksdb-v8.10": [row(9910866, 3.189, 31608738)],
            }
        )
        assert "zipkeyonly / v8.10" in lazy
        assert "zipkeyvalue / v8.10" in lazy
        assert '<span class="faster">3.69x</span>' in lazy
        assert '<span class="faster">3.75x</span>' in lazy
        assert "2.33x" not in lazy


def main() -> int:
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))
    chart = load("bench_rss_chart", here / "bench_rss_chart.py")
    check(chart)
    print("OK bench_rss_chart")
    common = load("bench_pages_common", here / "bench_pages_common.py")
    assert common.build_rss_svg.__module__ == "bench_rss_chart"
    assert common.RSS_LINE_COLORS == chart.RSS_LINE_COLORS
    assert (
        common.fmt_utc("2026-08-14T13:15:15.583652+00:00")
        == "2026-08-14 13:15:15+00:00"
    )
    assert common.fmt_utc("2026-08-14 13:15:15+00:00") == "2026-08-14 13:15:15+00:00"
    assert common.fmt_utc("") == ""
    assert common.href("raw", "zipkeyonly", "db_bench.log") == "raw/zipkeyonly/db_bench.log"
    assert (
        common.href("../runs/a b/raw", "zipkeyonly", "x.yaml")
        == "../runs/a%20b/raw/zipkeyonly/x.yaml"
    )
    assert not common.artifact_log_link("https://example/run", False)
    assert "artifacts" in common.artifact_log_link("https://example/run", True)
    bare = common.page("t", "<p>x</p>", include_chart_js=False)
    assert "initWrap" not in bare
    full = common.page("t", "<p>x</p>", include_chart_js=True)
    assert "initWrap" in full
    with tempfile.TemporaryDirectory() as tmp:
        eng_raw = Path(tmp) / "zipkeyonly"
        eng_raw.mkdir()
        source_logs = (
            ("db_bench-fillrandom.log", "$ fillrandom\nfillrandom output\n"),
            ("db_bench-fillrandom-omit.log", "$ fillrandom-omit\nomit output\n"),
            ("db_bench.log", "$ fillseq\nfillseq output\n"),
            ("db_bench-fillseq-omit.log", "$ fillseq-omit\nomit output\n"),
        )
        for name, content in source_logs:
            (eng_raw / name).write_text(content, encoding="utf-8")
        common.combine_db_bench_logs(eng_raw)
        combined = (eng_raw / "db_bench-all.log").read_text(encoding="utf-8")
        expected = "\n\n".join(
            content.rstrip("\n") for _, content in source_logs
        ) + "\n"
        assert combined == expected
        links = common.raw_db_bench_link_parts(
            Path(tmp), "raw", ("zipkeyonly",), {"zipkeyonly": "ToplingDB"}
        )
        assert links == ['<a href="raw/zipkeyonly/db_bench-all.log">ToplingDB</a>']
    with tempfile.TemporaryDirectory() as tmp:
        log_root = Path(tmp)
        eng_dir = log_root / "zipkeyonly"
        eng_dir.mkdir()
        (eng_dir / "statm_series-fillseq.txt").write_text(
            _STATM_SERIES, encoding="utf-8"
        )
        bad = common.build_rss_svg_section(
            log_root,
            {"zipkeyonly": {"db_bench": [], "db_bench_fillrandom": []}},
            ("zipkeyonly",),
            {"zipkeyonly": "Z"},
            lambda *_a, **_k: [],
            heading="<script>",
        )
        assert "<h3>RAM usage over time</h3>" in bad
        assert "<script>" not in bad
    print("OK bench_pages_common")
    for name, variant in (
        ("bench_logs_to_pages", "plain"),
        ("bench_dcompact_pages", "dcompact"),
    ):
        mod = load(name, here / f"{name}.py")
        assert mod._fmt_utc.__module__ == "bench_pages_common"
        assert mod._page.__module__ == "bench_pages_common"
        if name == "bench_logs_to_pages":
            html = mod._build_runner_section(
                {"os_pretty_name": "TestOS"}, None, None, False
            )
            assert "num=" not in html
            assert "TestOS" in html
        check_ratio_normalization(mod)
        check_pages_contract(mod, variant)
        print(f"OK {name} (imports shared pages chrome; emit/merge contract)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
