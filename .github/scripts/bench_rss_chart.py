#!/usr/bin/env python3
"""Shared RSS time-series chart helpers for bench Pages emitters."""

from __future__ import annotations

import html
import json
import math
from typing import List, Tuple

# RSS series: matplotlib tab10 qualitative hues (even hue spacing on white;
# anony keeps high-salience red as the focus line).
RSS_LINE_COLORS = {
    "rss": "#1f77b4",
    "shared": "#2ca02c",
    "anony": "#d62728",
    "pagecache": "#ff7f0e",
    "anony+pc": "#9467bd",
}


# Layout + fonts at 1.5× the original SVG sizes.
RSS_MARGIN_L, RSS_MARGIN_R, RSS_MARGIN_T, RSS_MARGIN_B = 105, 30, 44, 75
RSS_CHART_W, RSS_CHART_H = 1200, 450
RSS_HEADER_Y = 30
RSS_SWATCH_LEN, RSS_SWATCH_TEXT_GAP, RSS_LEGEND_ITEM_PAD = 28, 3, 16
# system-ui 13.5px packing widths (includes ~1ch slack so the block flushes
# to the plot frame's right edge without a separate inset).
RSS_LEGEND_TEXT_W = {
    "rss": 20,
    "shared": 44,
    "anony": 38,
    "pagecache": 66,
    "anony+pc": 66,  # 58 glyph estimate + 8px (~1ch) packing slack
}


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
            # full statm: resident=parts[2], shared=parts[3], optional pagecache=parts[8]
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


_SEGMENT_COLORS = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
]


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

    # Layout + fonts at 1.5× the original SVG sizes.
    margin_l, margin_r, margin_t, margin_b = (
        RSS_MARGIN_L, RSS_MARGIN_R, RSS_MARGIN_T, RSS_MARGIN_B
    )
    chart_w, chart_h = RSS_CHART_W, RSS_CHART_H
    svg_w = margin_l + chart_w + margin_r
    svg_h = margin_t + chart_h + margin_b

    def tx(v: float) -> float:
        return margin_l + (v / x_max) * chart_w if x_max else margin_l

    # Inset the plotable y-range so a series at data peak (common once block
    # cache fills) is not glued to the top edge — that reads as "no line" on
    # long flat plateaus (e.g. RocksDB fillseq readrandom). Same for tiny shared.
    _pad_top, _pad_bot = 12.0, 6.0
    _y_usable = chart_h - _pad_top - _pad_bot

    def ty(v: float) -> float:
        return margin_t + _pad_top + _y_usable * (1.0 - v / y_max)

    parts: List[str] = []
    parts.append('<div class="rss-chart-wrap">')
    parts.append(
        f'<svg class="rss-chart" xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {svg_w} {svg_h}" overflow="visible" '
        f'style="max-width:{svg_w}px;width:100%;height:auto;'
        f'font-family:system-ui,sans-serif;font-size:16.5px;cursor:crosshair">'
    )

    # Segment bands
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
            f'<text x="{mid_x:.1f}" y="{margin_t + chart_h + 21}" '
            f'text-anchor="middle" font-size="13.5" fill="{color}" '
            f'transform="rotate(-30 {mid_x:.1f} {margin_t + chart_h + 21})">'
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
            f'<line x1="{margin_l - 6}" y1="{yp:.1f}" '
            f'x2="{margin_l}" y2="{yp:.1f}" stroke="#666"/>'
        )
        parts.append(
            f'<text x="{margin_l - 9}" y="{yp + 4.5:.1f}" '
            f'text-anchor="end" font-size="15">{val:.0f}</text>'
        )
    parts.append(
        f'<text x="21" y="{margin_t + chart_h // 2}" '
        f'text-anchor="middle" font-size="16.5" '
        f'transform="rotate(-90 21 {margin_t + chart_h // 2})">MiB</text>'
    )
    for val in _axis_multiples(x_grid, x_max, include_zero=True):
        xp = tx(val)
        parts.append(
            f'<line x1="{xp:.1f}" y1="{margin_t + chart_h}" '
            f'x2="{xp:.1f}" y2="{margin_t + chart_h + 6}" stroke="#666"/>'
        )
        parts.append(
            f'<text x="{xp:.1f}" y="{margin_t + chart_h + 57}" '
            f'text-anchor="middle" font-size="15">{val:.0f}</text>'
        )
    parts.append(
        f'<text x="{margin_l + chart_w // 2}" y="{svg_h - 3}" '
        f'text-anchor="middle" font-size="16.5">Time (s)</text>'
    )

    # Draw sum last so anony+pc stays visible above other series.
    # anony stroke-width 3; other series at 1. Legend swatches match anony.
    sw_anony, sw_other, sw_legend = 3, 1, 3
    series = (
        ("anony", ys_anony, RSS_LINE_COLORS["anony"]),
        ("rss", ys_rss, RSS_LINE_COLORS["rss"]),
        ("shared", ys_shared, RSS_LINE_COLORS["shared"]),
        ("pagecache", ys_pagecache, RSS_LINE_COLORS["pagecache"]),
        ("anony+pc", ys_anony_pc, RSS_LINE_COLORS["anony+pc"]),
    )
    for label, ys, color in series:
        points = " ".join(f"{tx(x):.1f},{ty(y):.1f}" for x, y in zip(xs, ys))
        sw = sw_anony if label == "anony" else sw_other
        parts.append(
            f'<polyline points="{points}" fill="none" stroke="{color}" '
            f'stroke-width="{sw}"/>'
        )
    # Title (left) + legend (right) on one header row above the plot.
    legend = (
        ("rss", RSS_LINE_COLORS["rss"]),
        ("shared", RSS_LINE_COLORS["shared"]),
        ("anony", RSS_LINE_COLORS["anony"]),
        ("pagecache", RSS_LINE_COLORS["pagecache"]),
        ("anony+pc", RSS_LINE_COLORS["anony+pc"]),
    )
    # Compact legend: shorter swatch, tight fixed gap (equal for every item).
    # Text is start-anchored at swatch_end+gap so line↔label spacing never drifts.
    swatch_len = RSS_SWATCH_LEN
    swatch_text_gap = RSS_SWATCH_TEXT_GAP
    item_pad = RSS_LEGEND_ITEM_PAD
    legend_text_w = RSS_LEGEND_TEXT_W
    item_widths = []
    for i, (label, _) in enumerate(legend):
        w = swatch_len + swatch_text_gap + legend_text_w[label]
        if i + 1 < len(legend):
            w += item_pad
        item_widths.append(w)
    header_y = RSS_HEADER_Y
    parts.append(
        f'<text x="{margin_l}" y="{header_y}" text-anchor="start" '
        f'font-size="19.5" font-weight="600">{html.escape(title)}</text>'
    )
    # Flush legend to the plot frame's right edge (not the SVG viewBox).
    # RSS_LEGEND_TEXT_W already includes ~1ch system-ui packing slack.
    plot_right = float(margin_l + chart_w)
    x = plot_right - sum(item_widths)
    ly = header_y - 5
    for (label, color), w in zip(legend, item_widths):
        parts.append(
            f'<line x1="{x:.1f}" y1="{ly}" x2="{x + swatch_len:.1f}" y2="{ly}" '
            f'stroke="{color}" stroke-width="{sw_legend}"/>'
        )
        parts.append(
            f'<text x="{x + swatch_len + swatch_text_gap:.1f}" y="{header_y}" '
            f'text-anchor="start" font-size="13.5" fill="#333">{label}</text>'
        )
        x += w

    # Crosshair (dashed vline + intersection tips); hit rect on top for mouse
    parts.append(
        f'<g class="rss-crosshair" style="display:none">'
        f'<line class="rss-vline" y1="{margin_t}" y2="{margin_t + chart_h}" '
        f'stroke="#555" stroke-width="1" stroke-dasharray="6 4.5"/>'
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


RSS_CHART_JS = r"""
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
      var tipGap = 5;
      var tipH = 30;
      var tips = [];
      series.forEach(function (s) {
        var y = s.ys[i];
        var padT = L.padT || 0, padB = L.padB || 0;
        var usable = L.ch - padT - padB;
        var yp = L.mt + padT + usable * (1 - y / L.yMax);
        var tip = s.name + " (" + xs[i].toFixed(1) + "s, " + y.toFixed(1) + ")";
        var tipW = Math.max(108, 9.6 * tip.length + 15);
        var tipX = Math.min(Math.max(xp + 12, L.ml), L.ml + L.cw - tipW);
        tips.push({
          s: s, yp: yp, tip: tip, tipW: tipW, tipH: tipH, tipX: tipX,
          tipY: yp - tipH - 9
        });
      });
      tips.sort(function (a, b) { return a.tipY - b.tipY || a.yp - b.yp; });
      for (var k = 1; k < tips.length; k++) {
        var minY = tips[k - 1].tipY + tips[k - 1].tipH + tipGap;
        if (tips[k].tipY < minY) tips[k].tipY = minY;
      }
      var plotBottom = L.mt + L.ch;
      var last = tips[tips.length - 1];
      var overflow = (last.tipY + last.tipH) - plotBottom;
      if (overflow > 0) {
        for (k = 0; k < tips.length; k++) tips[k].tipY -= overflow;
      }
      if (tips[0].tipY < L.mt) {
        var shift = L.mt - tips[0].tipY;
        for (k = 0; k < tips.length; k++) tips[k].tipY += shift;
      }
      for (k = 1; k < tips.length; k++) {
        minY = tips[k - 1].tipY + tips[k - 1].tipH + tipGap;
        if (tips[k].tipY < minY) tips[k].tipY = minY;
      }
      tips.forEach(function (t) {
        var dot = document.createElementNS(NS, "circle");
        dot.setAttribute("cx", xp.toFixed(1));
        dot.setAttribute("cy", t.yp.toFixed(1));
        dot.setAttribute("r", "5.25");
        dot.setAttribute("fill", t.s.color);
        marks.appendChild(dot);
        // Frame = outer rounded color rect minus inner rounded white rect.
        // Intentionally non-concentric: thicker L/R band (swLR) than T/B (swTB),
        // with outerRx/innerRx chosen for the visual look (not geometric inset).
        var swTB = 1.5, swLR = 10, outerRx = 8, innerRx = 6;
        var bx = t.tipX, by = t.tipY, bw = t.tipW, bh = t.tipH;
        var outer = document.createElementNS(NS, "rect");
        outer.setAttribute("x", bx.toFixed(1));
        outer.setAttribute("y", by.toFixed(1));
        outer.setAttribute("width", bw.toFixed(1));
        outer.setAttribute("height", bh.toFixed(1));
        outer.setAttribute("rx", String(outerRx));
        outer.setAttribute("fill", t.s.color);
        outer.setAttribute("opacity", "0.95");
        marks.appendChild(outer);
        var inner = document.createElementNS(NS, "rect");
        inner.setAttribute("x", (bx + swLR).toFixed(1));
        inner.setAttribute("y", (by + swTB).toFixed(1));
        inner.setAttribute("width", (bw - 2 * swLR).toFixed(1));
        inner.setAttribute("height", (bh - 2 * swTB).toFixed(1));
        inner.setAttribute("rx", String(innerRx));
        inner.setAttribute("fill", "#fff");
        marks.appendChild(inner);
        var text = document.createElementNS(NS, "text");
        text.setAttribute("x", (bx + bw / 2).toFixed(1));
        text.setAttribute("y", (by + bh / 2).toFixed(1));
        text.setAttribute("text-anchor", "middle");
        text.setAttribute("dominant-baseline", "middle");
        text.setAttribute("font-size", "15");
        text.setAttribute("fill", "#222");
        text.textContent = t.tip;
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


