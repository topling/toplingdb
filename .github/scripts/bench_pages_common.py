#!/usr/bin/env python3
"""Shared HTML chrome for bench Pages emitters (plain + dcompact)."""

from __future__ import annotations

import html
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Sequence
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
    .slower {{ color: #a30d0d; }}
    .rss-chart-wrap {{ margin: 0.75rem 0 1.25rem; }}
  </style>
</head>
<body>
{body}
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
        if (raw_dir / eng / "db_bench.log").is_file():
            label = html.escape(engine_labels[eng])
            parts.append(f'<a href="{href(prefix, eng, "db_bench.log")}">{label}</a>')
    return parts


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
