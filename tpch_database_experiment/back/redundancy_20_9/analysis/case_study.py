#!/usr/bin/env python3
"""Create a one-sample table and diagram for all effective R strategies."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import tempfile
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (
    SCRIPT_DIR.parent
    / "results"
    / "new2"
    / "tpch-sf-01"
    / "new_redundancy_results.json"
)

# Representative masks for the 16 effective R redundancy cases.
STRATEGIES = (
    ("NR", 128),
    ("CNR", 160),
    ("OCNR", 176),
    ("LOCNR", 178),
    ("SNR", 192),
    ("PSSNR", 200),
    ("LPSSNR", 201),
    ("CNR + SNR", 224),
    ("CNR + PSSNR", 232),
    ("CNR + LPSSNR", 233),
    ("OCNR + SNR", 240),
    ("LOCNR + SNR", 242),
    ("OCNR + PSSNR", 248),
    ("OCNR + LPSSNR", 249),
    ("LOCNR + PSSNR", 250),
    ("LOCNR + LPSSNR", 251),
)


class CaseStudyError(RuntimeError):
    pass


def samples_by_group(samples: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """Index samples by their shared experiment group number."""
    return {int(sample["group"]): sample for sample in samples}


def select_group(
    baseline: dict[int, dict[str, Any]],
    strategy_samples: dict[int, dict[int, dict[str, Any]]],
    requested: int | None,
    seed: int | None,
) -> tuple[int, list[int]]:
    """Choose a group available in the baseline and every completed strategy."""
    eligible = set(baseline)
    for samples in strategy_samples.values():
        eligible &= set(samples)
    choices = sorted(eligible)
    if not choices:
        raise CaseStudyError("No paired R sample is available")
    if requested is not None:
        if requested not in eligible:
            raise CaseStudyError(
                f"Sample group {requested} is unavailable; choose one of {choices}"
            )
        return requested, choices

    rng = random.Random(seed) if seed is not None else random.SystemRandom()
    return rng.choice(choices), choices


def same_update(left: dict[str, Any], right: dict[str, Any]) -> bool:
    """Confirm that two timings belong to the same logical R update."""
    fields = ("group", "key", "a0", "a1")
    return all(left.get(field) == right.get(field) for field in fields)


def build_rows(
    configs: dict[int, dict[str, Any]],
    strategy_samples: dict[int, dict[int, dict[str, Any]]],
    baseline_sample: dict[str, Any],
    group: int,
) -> list[dict[str, Any]]:
    normalized_ms = float(baseline_sample["elapsed_ms"])
    rows: list[dict[str, Any]] = []

    for name, mask in STRATEGIES:
        row: dict[str, Any] = {
            "name": name,
            "normalized_ms": normalized_ms,
        }
        config = configs.get(mask)
        if config is None:
            rows.append(row)
            continue

        sample = strategy_samples[mask][group]
        if not same_update(sample, baseline_sample):
            raise CaseStudyError(f"{name} group {group} is not paired with the baseline")

        denormalized_ms = float(sample["elapsed_ms"])
        delta = config["structural_delta"]
        row.update(
            {
                "denormalized_ms": denormalized_ms,
                "update_delta_ms": denormalized_ms - normalized_ms,
                "normalization_ms": float(config["normalization"]["elapsed_ms"]),
                "denormalized_copies": int(sample["touched_nodes"]),
                "delta_nodes": int(delta["delta_nodes"]),
                "delta_relationships": int(delta["delta_relationships"]),
                "delta_cells": int(delta["delta_property_cells"]),
            }
        )
        rows.append(row)
    rows.sort(key=lambda row: row.get("denormalized_copies", float("inf")))
    return rows


def compact(value: float, _: float | None = None) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:g}M"
    if value >= 1_000:
        return f"{value / 1_000:g}K"
    return f"{value:g}"


def draw_diagram(rows: list[dict[str, Any]], output: Path) -> None:
    complete = [row for row in rows if "denormalized_copies" in row]
    if not complete:
        raise CaseStudyError("No completed R strategy is available for the diagram")

    cache = Path(tempfile.gettempdir()) / "tpch_case_study_diagram_cache"
    cache.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(cache / "matplotlib"))
    os.environ.setdefault("XDG_CACHE_HOME", str(cache))
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.colors import Normalize
        from matplotlib.ticker import FuncFormatter, MaxNLocator
    except ImportError as exc:
        raise CaseStudyError("Matplotlib is required to draw the diagram") from exc

    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 12,
            "axes.labelsize": 12,
            "xtick.labelsize": 11,
            "ytick.labelsize": 11,
        }
    )
    figure, axes = plt.subplots(
        1, 2, figsize=(6.8, 2.65), sharex=True, layout="constrained"
    )
    series = (
        ([row["update_delta_ms"] / 1_000 for row in complete],
         r"Saving, $\Delta_U$ (s)"),
        ([row["normalization_ms"] / 1_000 for row in complete],
         "Normalization time (s)"),
    )
    copies = [row["denormalized_copies"] for row in complete]
    cells = [row["delta_cells"] for row in complete]
    color_norm = Normalize(vmin=min(cells), vmax=max(cells))
    for axis, (values, ylabel) in zip(axes, series):
        points = axis.scatter(
            copies,
            values,
            c=cells,
            cmap="viridis",
            norm=color_norm,
            s=27,
            edgecolors="white",
            linewidths=0.45,
        )
        axis.set_xlabel("REGION copies updated")
        axis.set_ylabel(ylabel)
        axis.xaxis.set_major_formatter(FuncFormatter(compact))
        axis.yaxis.set_major_formatter(FuncFormatter(compact))
        axis.grid(True, color="#D8D8D8", linewidth=0.55)
        axis.set_axisbelow(True)
        for spine in axis.spines.values():
            spine.set_color("#777777")
            spine.set_linewidth(0.65)

    colorbar = figure.colorbar(points, ax=axes, fraction=0.035, pad=0.025)
    colorbar.ax.set_xlabel(r"$\Delta C$", labelpad=6)
    colorbar.locator = MaxNLocator(nbins=4)
    colorbar.formatter = FuncFormatter(compact)
    colorbar.update_ticks()
    colorbar.ax.tick_params(labelsize=11)

    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=300, bbox_inches="tight")
    plt.close(figure)


def format_ms(value: float | None, signed: bool = False) -> str:
    if value is None:
        return "N/A"
    return f"{value:+,.6f}" if signed else f"{value:,.6f}"


def format_count(value: int | None) -> str:
    if value is None:
        return "N/A"
    return "0" if value == 0 else f"{value:+,}"


def render_markdown(
    report: dict[str, Any],
    rows: list[dict[str, Any]],
    group: int,
    eligible_groups: list[int],
    baseline_sample: dict[str, Any],
    requested_group: int | None,
    seed: int | None,
) -> str:
    key = ", ".join(
        f"{name} = {value}" for name, value in baseline_sample["key"].items()
    )
    if requested_group is not None:
        selection = "selected by --sample"
    elif seed is not None:
        selection = f"random seed {seed}"
    else:
        selection = "system random"
    completed = sum("denormalized_ms" in row for row in rows)
    repeat_count = int(report.get("update_repeat_count", 1))

    lines = [
        "# R Redundancy Update Case Study",
        "",
        "- Paths: `L → O → C → N → R` and `L → PS → S → N → R`",
        f"- Database: `{report.get('database', 'N/A')}`",
        (
            f"- Selected sample: group `{group}` of `{len(eligible_groups)}` "
            f"eligible paired R samples ({selection})"
        ),
        f"- Update key: `{key}`; property: `r_comment`",
        f"- Completed target strategies: `{completed}/{len(STRATEGIES)}`",
        "",
        (
            "Every row uses one paired update case. Each reported update time is the "
            f"median of its `{repeat_count}` repeated measurements; the "
            f"`{len(eligible_groups)}` update cases are not aggregated. "
            "`Δ Update Time = De-normalized median − Normalized median`, so a "
            "positive value is time saved by normalization."
        ),
        "",
        (
            "`R Copies Updated` is the de-normalized `touched_nodes` value. Rows "
            "are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized "
            "− Normalized."
        ),
        "",
        (
            "| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | "
            "Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | "
            "Normalization Time (ms) | R Copies Updated | "
            "ΔN | ΔE | ΔC |"
        ),
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for number, row in enumerate(rows, start=1):
        denormalized_copies: int | None = row.get("denormalized_copies")
        copies = "N/A" if denormalized_copies is None else f"{denormalized_copies:,}"
        lines.append(
            "| "
            + " | ".join(
                (
                    str(number),
                    row["name"],
                    format_ms(row.get("denormalized_ms")),
                    format_ms(row["normalized_ms"]),
                    format_ms(row.get("update_delta_ms"), signed=True),
                    format_ms(row.get("normalization_ms")),
                    copies,
                    format_count(row.get("delta_nodes")),
                    format_count(row.get("delta_relationships")),
                    format_count(row.get("delta_cells")),
                )
            )
            + " |"
        )

    missing = [row["name"] for row in rows if "denormalized_ms" not in row]
    if missing:
        lines.extend(
            [
                "",
                "`N/A` means that this strategy is not yet in the input JSON: "
                + ", ".join(missing)
                + ".",
            ]
        )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the R-update case study")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument(
        "--output",
        type=Path,
        help="default: case_study.md beside the input JSON",
    )
    parser.add_argument("--sample", type=int, help="use this sample group")
    parser.add_argument("--seed", type=int, help="seed the random selection")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = args.input.expanduser().resolve()
    output_path = (
        args.output.expanduser().resolve()
        if args.output
        else input_path.with_name("case_study.md")
    )
    diagram_path = output_path.with_name("case_study_delta_vs_copies.png")

    try:
        report = json.loads(input_path.read_text(encoding="utf-8"))
        baseline = samples_by_group(
            report["normalized_baseline"]["updates"]["R"]["samples"]
        )
        target_masks = {mask for _, mask in STRATEGIES}
        configs = {
            int(config["mask"]): config
            for config in report["configurations"]
            if int(config["mask"]) in target_masks
        }
        strategy_samples = {
            mask: samples_by_group(config["updates"]["R"]["samples"])
            for mask, config in configs.items()
        }
        group, eligible = select_group(
            baseline, strategy_samples, args.sample, args.seed
        )
        rows = build_rows(configs, strategy_samples, baseline[group], group)
        markdown = render_markdown(
            report, rows, group, eligible, baseline[group], args.sample, args.seed
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown, encoding="utf-8")
        draw_diagram(rows, diagram_path)
    except (CaseStudyError, KeyError, TypeError, ValueError, json.JSONDecodeError, OSError) as exc:
        print(f"Case study failed: {exc}", file=sys.stderr)
        return 1

    print(f"Selected sample group {group}")
    print(f"Wrote {output_path}")
    print(f"Wrote {diagram_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
