#!/usr/bin/env python3
"""Export the nine embedding cases as a Markdown median-summary table."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import fmean, stdev
from typing import Any, Sequence


EMBEDDING_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = Path(__file__).resolve().parents[0]
DEFAULT_INPUT = EMBEDDING_DIR / "embedding_results_undirection.json"
DEFAULT_OUTPUT = REPOSITORY_ROOT / "result.md"

CASE_ORDER = (
    "order_customer",
    "order_detail_product",
    "order_detail_order",
    "product_supplier",
    "order_employee",
    "order_shipper",
    "product_category",
    "employee_territory_employee",
    "territory_region",
)

METHODS = (
    ("property_hash", "Property Hash"),
    ("fast_rp", "FastRP"),
    ("node2vec", "Node2Vec"),
    ("hash_gnn", "HashGNN"),
    ("graph_sage", "GraphSAGE"),
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write normalized and denormalized median similarities for all "
            "nine embedding cases to a Markdown table."
        )
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--precision",
        type=int,
        default=6,
        help="Number of digits after the decimal point (default: 6).",
    )
    args = parser.parse_args(argv)
    if not 0 <= args.precision <= 15:
        parser.error("--precision must be between 0 and 15")
    return args


def load_experiments(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    experiments = payload.get("experiments")
    if not isinstance(experiments, dict):
        raise ValueError(f"Missing object 'experiments' in {path}")
    return experiments


def case_runs(
    experiments: dict[str, Any],
    case_name: str,
) -> list[dict[str, Any]]:
    case_result = experiments[case_name]
    runs = case_result.get("runs")
    if runs is None:
        return [case_result]
    if not isinstance(runs, list) or not runs:
        raise ValueError(f"experiments.{case_name}.runs must be a non-empty list")
    return runs


def seed_summary(
    runs: list[dict[str, Any]],
    method_name: str,
    representation: str,
    section: str = "similarities",
) -> tuple[float, float]:
    values = []
    for run in runs:
        try:
            value = run[section][method_name][representation]["median"]
        except (KeyError, TypeError) as exc:
            raise ValueError(
                f"Missing median in {section} for "
                f"{method_name}/{representation}"
            ) from exc
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise ValueError(f"Invalid median for {method_name}/{representation}")
        values.append(float(value))
    return fmean(values), stdev(values) if len(values) > 1 else 0.0


def table_lines(
    experiments: dict[str, Any],
    precision: int,
    show_sd: bool,
    section: str = "similarities",
) -> list[str]:
    headers = ["Case"]
    for _, label in METHODS:
        headers.extend((f"{label} Norm med.", f"{label} Denorm med."))

    lines = [
        "| " + " | ".join(headers) + " |",
        "| :--- | " + " | ".join("---:" for _ in headers[1:]) + " |",
    ]
    for case_name in CASE_ORDER:
        cells = [f"`{case_name}`"]
        runs = case_runs(experiments, case_name)
        for method_name, _ in METHODS:
            for representation in ("normalized", "denormalized"):
                center, spread = seed_summary(
                    runs, method_name, representation, section
                )
                value = f"{center:.{precision}f}"
                if show_sd:
                    value += f" ± {spread:.{precision}f}"
                cells.append(value)
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def build_markdown_table(
    experiments: dict[str, Any], precision: int
) -> str:
    missing_cases = [name for name in CASE_ORDER if name not in experiments]
    if missing_cases:
        raise ValueError(f"Missing embedding cases: {', '.join(missing_cases)}")

    run_counts = {len(case_runs(experiments, name)) for name in CASE_ORDER}
    if len(run_counts) != 1:
        raise ValueError("All embedding cases must have the same number of runs")
    run_count = run_counts.pop()
    lines = [
        "### Mean ± sample SD",
        "",
        f"Values summarize {run_count} random-seed runs. Each run independently "
        "resamples node pairs and contributes its median similarity.",
        "",
        *table_lines(experiments, precision, show_sd=True),
        "",
        "### Mean only",
        "",
        *table_lines(experiments, precision, show_sd=False),
    ]
    has_negative_control = all(
        "negative_similarities" in run and "separation" in run
        for name in CASE_ORDER
        for run in case_runs(experiments, name)
    )
    if has_negative_control:
        lines.extend(
            [
                "",
                "### Different-parent control: Mean ± sample SD",
                "",
                *table_lines(
                    experiments,
                    precision,
                    show_sd=True,
                    section="negative_similarities",
                ),
                "",
                "### Different-parent control: Mean only",
                "",
                *table_lines(
                    experiments,
                    precision,
                    show_sd=False,
                    section="negative_similarities",
                ),
                "",
                "### Same-parent minus different-parent: Mean ± sample SD",
                "",
                *table_lines(
                    experiments,
                    precision,
                    show_sd=True,
                    section="separation",
                ),
            ]
        )
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    experiments = load_experiments(args.input)
    table = build_markdown_table(experiments, args.precision)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(table, encoding="utf-8")
    print(f"Wrote: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
