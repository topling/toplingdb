#!/usr/bin/env python3
"""Shared HTML chrome for bench Pages emitters (plain + dcompact)."""

from __future__ import annotations

import html
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import quote

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from bench_rss_chart import (  # noqa: E402
    RSS_CHART_JS,
    RSS_LINE_COLORS,
    build_rss_svg,
    parse_pagecache_src,
    parse_rss_series,
)

SegmentFn = Callable[[List[Dict[str, str]], float, float], List[Any]]


def stage_window_rss_bytes(
    series_path: Path,
    bench_rows: Sequence[Mapping[str, str]],
    stage: str,
    compute_segments: SegmentFn,
    *,
    how: str = "avg",
) -> Optional[int]:
    """RSS bytes in the last `stage` window; how is 'avg' or 'max'."""
    if how not in ("avg", "max"):
        raise ValueError(f"how must be avg or max, got {how!r}")
    if not series_path.is_file() or not bench_rows:
        return None
    text = series_path.read_text(encoding="utf-8", errors="replace")
    start_epoch, page_size, samples = parse_rss_series(text)
    if not samples:
        return None
    total_dur = samples[-1][0] - start_epoch
    windows = [
        (t0, t1)
        for name, t0, t1, *_rest in compute_segments(
            list(bench_rows), start_epoch, total_dur
        )
        if name == stage
    ]
    if not windows:
        return None
    t0, t1 = windows[-1]
    # Half-open [t0, t1) so a sample at the next stage's start is not
    # attributed here. Fall back to closed if the window has no samples
    # (short stage / 1Hz lands only on t1).
    rss_pages = [
        rss
        for epoch, rss, *_rest in samples
        if t0 <= (epoch - start_epoch) < t1
    ]
    if not rss_pages:
        rss_pages = [
            rss
            for epoch, rss, *_rest in samples
            if t0 <= (epoch - start_epoch) <= t1
        ]
    if not rss_pages:
        return None
    if how == "max":
        return int(max(rss_pages) * page_size)
    return int(sum(p * page_size for p in rss_pages) / len(rss_pages))


SUITE_READRANDOM = (
    ("fillrandom", "db_bench_fillrandom", "fillrandom-readrandom"),
    ("fillseq", "db_bench", "fillseq-readrandom"),
)


def attach_suite_readrandom_rss(
    rss_data: Dict[str, Dict[str, Optional[int]]],
    engines: Mapping[str, Any],
    raw_dir: Path,
    engine_ids: Sequence[str],
    compute_segments: SegmentFn,
) -> None:
    """Add per-suite readrandom peak RSS from statm series."""
    for suite, bench_key, dest_key in SUITE_READRANDOM:
        for e in engine_ids:
            rows = (engines.get(e) or {}).get(bench_key) or []
            peak = stage_window_rss_bytes(
                raw_dir / e / f"statm_series-{suite}.txt",
                rows,
                "readrandom",
                compute_segments,
                how="max",
            )
            if peak is not None:
                rss_data.setdefault(e, {})[dest_key] = peak


SHM_SUITE_LABELS = {
    "fillrandom": "fillrandom suite",
    "fillseq": "fillseq suite",
}
RSS_WORKLOAD_ORDER = (
    "fillrandom",
    "fillrandom-readrandom",
    "fillrandom-omit",
    "fillseq",
    "fillseq-readrandom",
    "fillseq-omit",
)
RSS_WORKLOAD_LABELS = {
    "fillrandom": "fillrandom suite peak",
    "fillseq": "fillseq suite peak",
    "fillrandom-readrandom": "fillrandom suite readrandom",
    "fillseq-readrandom": "fillseq suite readrandom",
    "fillrandom-omit": "fillrandom scan-omit-value",
    "fillseq-omit": "fillseq scan-omit-value",
}
RSS_WORKLOAD_TIPS = {
    "fillrandom-readrandom": (
        "peak RSS during the readrandom stage of the fillrandom suite"
    ),
    "fillseq-readrandom": (
        "peak RSS during the readrandom stage of the fillseq suite"
    ),
    "fillrandom-omit": (
        "restart process with reuse db data of fillrandom, "
        "scan without access value, benefited by lazy load value (ToplingDB feature)"
    ),
    "fillseq-omit": (
        "restart process with reuse db data of fillseq, "
        "scan without access value, benefited by lazy load value (ToplingDB feature)"
    ),
}


def _center_at(text: str, mid: int) -> str:
    return " " * (mid - len(text) // 2) + text


def _overlay(width: int, *parts: tuple[str, int]) -> str:
    buf = [" "] * width
    for text, start in parts:
        buf[start : start + len(text)] = text
    return "".join(buf).rstrip()


def _zipkeyonly_rss_tip() -> str:
    lead = "Not include (value's) shared RSS — "
    chain = "mmap slice as source → direct → user slice as target"
    mmap_w, dir_w = len("mmap slice"), len("direct")
    mmap_col = len(lead) + chain.index("mmap slice")
    dir_col = len(lead) + chain.index("direct")
    width = len(lead) + len(chain)
    return "\n".join(
        (
            lead + chain,
            _overlay(
                width,
                ("^" * mmap_w, mmap_col),
                ("^" * dir_w, dir_col),
            ),
            _overlay(
                width,
                ("not zipped", mmap_col),
                ("not touched", dir_col),
            ),
            _overlay(width, ("not bring to shared RSS", dir_col)),
        )
    )


def _zipkeyvalue_rss_tip() -> str:
    lead = "Include (value's) shared RSS — "
    chain = "mmap slice as source → decompress → user slice as target"
    mmap_w, dec_w = len("mmap slice"), len("decompress")
    mmap_col = len(lead) + chain.index("mmap slice")
    dec_col = len(lead) + chain.index("decompress")
    dec_mid = dec_col + dec_w // 2
    width = len(lead) + len(chain)
    return "\n".join(
        (
            lead + chain,
            _overlay(
                width,
                ("^" * mmap_w, mmap_col),
                ("^" * dec_w, dec_col),
            ),
            _overlay(
                width,
                ("zipped", mmap_col),
                ("read source", dec_mid - len("read source") // 2),
            ),
            _center_at("bring mmap to", dec_mid),
            _center_at("shared RSS", dec_mid),
        )
    )


_ROCKSDB_RSS_TIP = (
    "mainly block cache;\n"
    "file page cache is not counted in shared/RSS, "
    "so actual RAM is larger, as the chart below shows"
)
RSS_READRANDOM_ENGINE_TIPS = {
    "zipkeyonly": _zipkeyonly_rss_tip(),
    "zipkeyvalue": _zipkeyvalue_rss_tip(),
    "rocksdb-v8.10": _ROCKSDB_RSS_TIP,
    "rocksdb-master": _ROCKSDB_RSS_TIP,
}


def rss_engine_cell_tip(wl: str, engine: str) -> Optional[str]:
    """Engine value-cell tip on suite readrandom rows only."""
    if not wl.endswith("-readrandom"):
        return None
    return RSS_READRANDOM_ENGINE_TIPS.get(engine)


def _tip_aria_label(tip: str) -> str:
    parts = []
    for line in tip.split("\n"):
        text = line.strip()
        if not text or set(text) <= {"^", " "}:
            continue
        parts.append(text.rstrip(";"))
    return "; ".join(p for p in parts if p)


def format_tip_abbr(inner: str, tip: str) -> str:
    """Shared CSS tip chrome (same border/cursor/direction as other table tips)."""
    label = html.escape(_tip_aria_label(tip), quote=True)
    return (
        f'<abbr class="tip" tabindex="0" aria-label="{label}">{inner}'
        f'<span class="tip-box" aria-hidden="true">{html.escape(tip)}</span>'
        f"</abbr>"
    )


def format_engine_tip_abbr(inner: str, tip: str, engine: str) -> str:
    """Same tip chrome for every engine; Topling diagram tips add mono + line-height."""
    if engine not in ("zipkeyonly", "zipkeyvalue") or "^" not in tip:
        return format_tip_abbr(inner, tip)
    lines = tip.split("\n")
    if len(lines) < 2:
        return format_tip_abbr(inner, tip)
    chain, mark, *rest = lines
    parts = [
        f'<span class="tip-pre-line">{html.escape(chain)}</span>',
        f'<span class="tip-pre-mark">{html.escape(mark)}</span>',
    ]
    parts.extend(
        f'<span class="tip-pre-line">{html.escape(row)}</span>' for row in rest
    )
    label = html.escape(_tip_aria_label(tip), quote=True)
    return (
        f'<abbr class="tip tip-pre" tabindex="0" aria-label="{label}">{inner}'
        f'<span class="tip-box" aria-hidden="true">'
        f"{''.join(parts)}"
        f"</span></abbr>"
    )


TIP_SHIFT_JS = r"""
<script>
(function () {
  var m = 8;
  function placeTip(box) {
    var vw = document.documentElement.clientWidth;
    box.style.transform = "";
    var r = box.getBoundingClientRect();
    var dx = 0;
    if (r.right > vw - m) dx -= Math.ceil(r.right - (vw - m));
    if (r.left + dx < m) dx += Math.ceil(m - (r.left + dx));
    if (dx) box.style.transform = "translateX(" + dx + "px)";
  }
  function onTip(ev) {
    var a = ev.target.closest && ev.target.closest("abbr.tip");
    if (!a) return;
    var box = a.querySelector(":scope > .tip-box");
    if (box) placeTip(box);
  }
  function replaceOpen() {
    document.querySelectorAll(
      "abbr.tip:hover > .tip-box, abbr.tip:focus-within > .tip-box"
    ).forEach(placeTip);
  }
  document.addEventListener("mouseenter", onTip, true);
  document.addEventListener("focusin", onTip);
  addEventListener("resize", replaceOpen);
  document.addEventListener("scroll", replaceOpen, true);
})();
</script>
"""


def fmt_utc(value: Any = None) -> str:
    """UTC for humans: date time to the second, space instead of T."""
    if value is None:
        dt = datetime.now(timezone.utc)
    else:
        text = str(value).strip()
        if not text:
            return ""
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text.replace("T", " ", 1)
    return dt.replace(microsecond=0).isoformat().replace("T", " ", 1)


def href(*parts: str) -> str:
    segs: List[str] = []
    for part in parts:
        for seg in str(part).replace("\\", "/").split("/"):
            if not seg:
                continue
            if seg in (".", ".."):
                segs.append(seg)
            else:
                segs.append(quote(seg, safe="-_.~"))
    return html.escape("/".join(segs), quote=True)


def page(title: str, body: str, include_chart_js: bool = True) -> str:
    chart_js = RSS_CHART_JS if include_chart_js else ""
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
    .meta {{ color: #000; font-size: 0.9rem; }}
    .faster {{ color: #0a7a28; font-weight: 600; }}
    .slower {{ color: #a30d0d; font-weight: 600; }}
    .rss-chart-wrap {{ margin: 0.75rem 0 1.25rem; }}
    abbr.tip {{ position: relative; text-decoration: underline dotted; }}
    abbr.tip .tip-box {{
      visibility: hidden; position: absolute; left: 0; bottom: calc(100% + 0.5rem);
      z-index: 8; padding: 0.4em 0.55em; background: InfoBackground; color: InfoText;
      border: 1px solid #000; border-radius: 0.5em;
      font-size: 0.75em; line-height: 1.15; white-space: pre-wrap;
      pointer-events: none; box-sizing: border-box; width: max-content;
      max-width: 36rem;
    }}
    abbr.tip:hover .tip-box, abbr.tip:focus .tip-box, abbr.tip:focus-within .tip-box {{
      visibility: visible;
    }}
    abbr.tip-pre .tip-box {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      white-space: pre; max-width: none;
    }}
    abbr.tip-pre .tip-pre-line {{ display: block; }}
    abbr.tip-pre .tip-pre-line:first-child {{ padding-bottom: 0.45em; }}
    abbr.tip-pre .tip-pre-mark {{ display: block; line-height: 0.55; }}
  </style>
</head>
<body>
{body}
{TIP_SHIFT_JS}
{chart_js}
</body>
</html>
"""


def artifact_log_link(
    actions_run_url: str,
    has_info_logs: bool,
    label: str = "DB INFO LOGs + bench yamls (Actions artifact)",
) -> str:
    if not has_info_logs or not actions_run_url:
        return ""
    return (
        f'<a href="{html.escape(actions_run_url, quote=True)}#artifacts">'
        f"{html.escape(label)}</a>"
    )


def entry_has_info_logs(entry: Mapping[str, Any]) -> bool:
    if "has_info_logs" in entry:
        return bool(entry.get("has_info_logs"))
    return bool(entry.get("artifact_names"))


def build_yaml_config_links(
    raw_dir: Path,
    href_prefix: str,
    topling_engines: Sequence[str],
    yaml_names: Sequence[str],
) -> str:
    parts: List[str] = []
    prefix = href_prefix.rstrip("/")
    for eng in topling_engines:
        eng_dir = raw_dir / eng
        for name in yaml_names:
            if (eng_dir / name).is_file():
                parts.append(
                    f'<a href="{href(prefix, eng, name)}">{html.escape(eng)} {name}</a>'
                )
    if not parts:
        return ""
    return (
        '<p class="meta"><strong>ToplingDB bench yaml</strong>: '
        + " | ".join(parts)
        + "</p>"
    )


def raw_db_bench_link_parts(
    raw_dir: Path,
    href_prefix: str,
    engines: Sequence[str],
    engine_labels: Mapping[str, str],
) -> List[str]:
    parts: List[str] = []
    prefix = href_prefix.rstrip("/")
    for eng in engines:
        eng_raw = raw_dir / eng
        name = "db_bench-all.log"
        if not (eng_raw / name).is_file():
            name = "db_bench.log"
        if (eng_raw / name).is_file():
            label = html.escape(engine_labels[eng])
            parts.append(f'<a href="{href(prefix, eng, name)}">{label}</a>')
    return parts


def combine_db_bench_logs(engine_raw: Path) -> None:
    """Combine the benchmark suites into the single raw log linked by Pages."""
    sources = (
        "db_bench-fillrandom.log",
        "db_bench-fillrandom-omit.log",
        "db_bench.log",
        "db_bench-fillseq-omit.log",
    )
    chunks = [
        (engine_raw / name).read_bytes().rstrip(b"\n")
        for name in sources
        if (engine_raw / name).is_file()
    ]
    if chunks:
        (engine_raw / "db_bench-all.log").write_bytes(b"\n\n".join(chunks) + b"\n")


def build_source_links(
    raw_dir: Path,
    href_prefix: str,
    engines: Sequence[str],
    engine_labels: Mapping[str, str],
    topling_engines: Sequence[str],
    yaml_names: Sequence[str],
    actions_run_url: str = "",
    has_info_logs: bool = False,
    artifact_label: str = "DB INFO LOGs + bench yamls (Actions artifact)",
) -> str:
    raw_parts = raw_db_bench_link_parts(
        raw_dir, href_prefix, engines, engine_labels
    )
    artifact = artifact_log_link(actions_run_url, has_info_logs, artifact_label)
    if artifact:
        raw_parts.append(artifact)
    chunks: List[str] = []
    if raw_parts:
        chunks.append(
            '<p class="meta"><strong>raw logs</strong>: '
            + " | ".join(raw_parts)
            + "</p>"
        )
    yaml_links = build_yaml_config_links(
        raw_dir, href_prefix, topling_engines, yaml_names
    )
    if yaml_links:
        chunks.append(yaml_links)
    return "\n  ".join(chunks)


def build_rss_svg_section(
    log_root: Path,
    engines_data: Mapping[str, Any],
    engines: Iterable[str],
    engine_labels: Mapping[str, str],
    compute_segments: SegmentFn,
    heading: str = "h3",
) -> str:
    if heading not in ("h2", "h3"):
        heading = "h3"
    rss_svg_parts: List[str] = []
    pagecache_src = ""
    for eng in engines:
        eng_dir = log_root / eng
        for suite, bench_key in [
            ("fillrandom", "db_bench_fillrandom"),
            ("fillseq", "db_bench"),
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
            segments = compute_segments(bench_rows, start_epoch, total_dur)
            if segments and segments[0][1] > 0.5:
                segments.insert(0, ("startup", 0, segments[0][1], False))
            svg = build_rss_svg(
                samples, page_size, start_epoch, segments,
                f"{engine_labels[eng]} — {suite} suite RAM",
            )
            if svg:
                rss_svg_parts.append(svg)
    if not rss_svg_parts:
        return ""
    if pagecache_src == "meminfo":
        pagecache_how = (
            "plus system Cached (/proc/meminfo) growth since drop_caches baseline "
            "(workflow cached_pages_use_sys / SYS_CACHED_OF_EMPTY)"
        )
        pagecache_li = (
            f'<li><span style="color:{RSS_LINE_COLORS["pagecache"]};font-weight:600">pagecache</span>: '
            "system-wide /proc/meminfo Cached minus the post-drop_caches baseline "
            "(excluding the inherent consumption of an empty system). "
            "When reads hit page cache, they avoid disk I/O, and the DB "
            "benefits in its own way.</li>\n"
        )
        anony_pc_li = (
            f'<li><span style="color:{RSS_LINE_COLORS["anony+pc"]};font-weight:600">anony+pc</span>: '
            "anony+pagecache (sum of process anonymous RSS and system Cached growth; "
            "not a disjoint partition).</li>\n"
        )
    else:
        pagecache_how = "plus open-file page cache"
        pagecache_li = (
            f'<li><span style="color:{RSS_LINE_COLORS["pagecache"]};font-weight:600">pagecache</span>: '
            "kernel file page cache for regular files this process currently has open "
            "(cachestat(2) on /proc/pid/fd, deduped by inode; covers buffered read/write "
            "and mmap). Pages brought in only via buffered I/O are not charged to process RSS; "
            "mmap'd file pages may appear in both pagecache and RSS/shared, so the series can "
            "overlap. When reads hit this cache, they avoid disk I/O, and the DB "
            "benefits in its own way.</li>\n"
        )
        anony_pc_li = (
            f'<li><span style="color:{RSS_LINE_COLORS["anony+pc"]};font-weight:600">anony+pc</span>: '
            "anony+pagecache (sum of process anonymous RSS and open-file page cache; "
            "not a disjoint partition).</li>\n"
        )
    return (
        f"<{heading}>RAM usage over time</{heading}>\n"
        f'<p class="meta">Sampled once per second from /proc/statm {pagecache_how}. '
        "The bar below each plot shows stage name + duration "
        "(consecutive repeats merge, e.g. readseq×3); "
        "colored bands mark the same intervals. "
        "Stage names may overflow into a neighbor cell; a label under the bar "
        "is used only when names would overlap.</p>\n"
        '<ul class="meta">\n'
        f'<li><span style="color:{RSS_LINE_COLORS["rss"]};font-weight:600">rss</span>: '
        "resident set size (pages currently in RAM for the process); "
        "rss = shared + anony.</li>\n"
        f'<li><span style="color:{RSS_LINE_COLORS["shared"]};font-weight:600">shared</span>: '
        "shared resident pages; mostly readonly (cheap; OS prefers reclaiming these, "
        "no swap needed).</li>\n"
        f'<li><span style="color:{RSS_LINE_COLORS["anony"]};font-weight:600">anony</span>: '
        "rss - shared; "
        "mostly readwrite anonymous pages (costly, needs swap).</li>\n"
        + pagecache_li
        + anony_pc_li
        + "</ul>\n"
        + "\n".join(rss_svg_parts)
    )
