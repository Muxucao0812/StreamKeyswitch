#!/usr/bin/env python3

import argparse
import csv
import glob
import html
from pathlib import Path


METHOD_ORDER = [
    "poseidon",
    "fab",
    "fast",
    "ola",
    "hera",
    "cinnamon_oa",
    "cinnamon_ib",
    "cinnamon_output_aggregation",
    "cinnamon_input_broadcast",
    "cinnamon",
]

COLORS = [
    "#4E79A7",
    "#F28E2B",
    "#E15759",
    "#76B7B2",
    "#59A14F",
    "#EDC948",
    "#B07AA1",
    "#FF9DA7",
]


def method_from_path(path: Path) -> str:
    stem = path.stem
    if stem.startswith("metrics_"):
        return stem[len("metrics_") :]
    return stem


def parse_metric(path: Path, metric: str) -> float:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        last_row = None
        for row in reader:
            last_row = row
    if last_row is None:
        raise ValueError(f"{path}: csv has no data row")
    if metric not in last_row:
        raise ValueError(f"{path}: metric '{metric}' not found in header")
    return float(last_row[metric])


def sort_items(items):
    order_map = {name: idx for idx, name in enumerate(METHOD_ORDER)}

    def key_fn(item):
        name = item[0]
        return (order_map.get(name, 10_000), name)

    return sorted(items, key=key_fn)


def metric_label(metric: str) -> str:
    mapping = {
        "mean_latency": "Mean Latency (ns)",
        "p95_latency": "P95 Latency (ns)",
        "p99_latency": "P99 Latency (ns)",
        "max_latency": "Max Latency (ns)",
    }
    return mapping.get(metric, metric)


def fmt_value(value: float) -> str:
    if value >= 1000:
        return f"{value:.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.3f}"


def build_svg(items, metric: str) -> str:
    count = len(items)
    width = max(960, 180 + count * 120)
    height = 560

    margin_left = 90
    margin_right = 30
    margin_top = 70
    margin_bottom = 110

    plot_left = margin_left
    plot_top = margin_top
    plot_right = width - margin_right
    plot_bottom = height - margin_bottom
    plot_width = plot_right - plot_left
    plot_height = plot_bottom - plot_top

    values = [value for _, value in items]
    max_value = max(values) if values else 1.0
    if max_value <= 0:
        max_value = 1.0
    y_max = max_value * 1.15

    slot = plot_width / max(1, count)
    bar_width = min(64.0, slot * 0.62)

    out = []
    out.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">')
    out.append('<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>')
    out.append(
        f'<text x="{width/2:.1f}" y="36" text-anchor="middle" font-size="24" '
        'font-family="Maple Mono, Cascadia Code, monospace" fill="#1f2937">'
        f"{html.escape(metric_label(metric))} by Method</text>"
    )

    # Grid + Y ticks
    ticks = 5
    for t in range(ticks + 1):
        ratio = t / ticks
        y = plot_bottom - ratio * plot_height
        value = ratio * y_max
        out.append(
            f'<line x1="{plot_left}" y1="{y:.2f}" x2="{plot_right}" y2="{y:.2f}" '
            'stroke="#e5e7eb" stroke-width="1"/>'
        )
        out.append(
            f'<text x="{plot_left - 10}" y="{y + 4:.2f}" text-anchor="end" '
            'font-size="12" font-family="Maple Mono, Cascadia Code, monospace" fill="#6b7280">'
            f"{html.escape(fmt_value(value))}</text>"
        )

    # Axes
    out.append(
        f'<line x1="{plot_left}" y1="{plot_bottom}" x2="{plot_right}" y2="{plot_bottom}" '
        'stroke="#374151" stroke-width="2"/>'
    )
    out.append(
        f'<line x1="{plot_left}" y1="{plot_top}" x2="{plot_left}" y2="{plot_bottom}" '
        'stroke="#374151" stroke-width="2"/>'
    )

    # Bars + labels
    for idx, (name, value) in enumerate(items):
        center_x = plot_left + (idx + 0.5) * slot
        bar_h = 0.0 if y_max <= 0 else (value / y_max) * plot_height
        x = center_x - bar_width / 2
        y = plot_bottom - bar_h
        color = COLORS[idx % len(COLORS)]

        out.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width:.2f}" height="{bar_h:.2f}" '
            f'fill="{color}" rx="8" ry="8"/>'
        )
        out.append(
            f'<text x="{center_x:.2f}" y="{y - 8:.2f}" text-anchor="middle" '
            'font-size="12" font-family="Maple Mono, Cascadia Code, monospace" fill="#111827">'
            f"{html.escape(fmt_value(value))}</text>"
        )
        out.append(
            f'<text x="{center_x:.2f}" y="{plot_bottom + 26:.2f}" text-anchor="middle" '
            'font-size="13" font-family="Maple Mono, Cascadia Code, monospace" fill="#111827">'
            f"{html.escape(name)}</text>"
        )

    out.append(
        f'<text x="{plot_left + plot_width / 2:.2f}" y="{height - 28}" text-anchor="middle" '
        'font-size="13" font-family="Maple Mono, Cascadia Code, monospace" fill="#6b7280">Method</text>'
    )
    out.append(
        f'<text x="26" y="{plot_top + plot_height / 2:.2f}" text-anchor="middle" '
        'font-size="13" font-family="Maple Mono, Cascadia Code, monospace" fill="#6b7280" '
        f'transform="rotate(-90 26 {plot_top + plot_height / 2:.2f})">{html.escape(metric_label(metric))}</text>'
    )
    out.append("</svg>")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description="Draw latency chart from per-method CSV files.")
    parser.add_argument("--input-glob", default="results/metrics_*.csv")
    parser.add_argument(
        "--metric",
        default="mean_latency",
        choices=["mean_latency", "p95_latency", "p99_latency", "max_latency"],
    )
    parser.add_argument("--output", default="results/latency_mean.svg")
    args = parser.parse_args()

    paths = sorted(Path(p) for p in glob.glob(args.input_glob))
    if not paths:
        raise SystemExit(f"No CSV files matched: {args.input_glob}")

    items = []
    for path in paths:
        try:
            value = parse_metric(path, args.metric)
        except Exception as exc:  # pylint: disable=broad-except
            raise SystemExit(str(exc)) from exc
        items.append((method_from_path(path), value))

    items = sort_items(items)
    svg = build_svg(items, args.metric)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(svg, encoding="utf-8")
    print(f"[draw] wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
