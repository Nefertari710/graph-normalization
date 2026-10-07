#!/usr/bin/env python3
"""Export the Provider and Service embedding experiments as Markdown tables."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import fmean, stdev
from typing import Any, Sequence

EMBEDDING_DIR = Path(__file__).resolve().parents[1]
ANALYSIS_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (
        EMBEDDING_DIR
        / "results"
        / "providerandservicecsv"
        / "embedding_providerandservice.json"
)
DEFAULT_OUTPUT = ANALYSIS_DIR / "result.md"

CASES = (
    ("provider", "Provider (same NPI)"),
    ("service", "Service (same HCPCS)"),
)
CASE_ORDER = tuple(case_name for case_name, _ in CASES)
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
            "Write normalized and denormalized median similarities for the "
            "Provider and Service embedding experiments."
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
    for case_name in CASE_ORDER:
        if not isinstance(experiments.get(case_name), list):
            raise ValueError(f"experiments.{case_name} must be a list")
    return experiments


def seed_summary(
        runs: list[dict[str, Any]],
        method_name: str,
        representation: str,
) -> tuple[float, float]:
    if not runs:
        raise ValueError("No random-seed runs found")
    values = []
    for run in runs:
        try:
            value = run["similarities"][method_name][representation]["median"]
        except (KeyError, TypeError) as error:
            raise ValueError(
                f"Missing median for {method_name}/{representation}"
            ) from error
        if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
        ):
            raise ValueError(f"Invalid median for {method_name}/{representation}")
        values.append(float(value))
    return fmean(values), stdev(values) if len(values) > 1 else 0.0


def table_lines(
        experiments: dict[str, Any], precision: int, show_sd: bool
) -> list[str]:
    headers = ["Case"]
    for _, label in METHODS:
        headers.extend((f"{label} Denorm", f"{label} Norm"))

    lines = [
        "| " + " | ".join(headers) + " |",
        "| :--- | " + " | ".join("---:" for _ in headers[1:]) + " |",
    ]
    for case_name, case_label in CASES:
        cells = [case_label]
        for method_name, _ in METHODS:
            for representation in ("denormalized", "normalized"):
                center, spread = seed_summary(
                    experiments[case_name], method_name, representation
                )
                value = f"{center:.{precision}f}"
                if show_sd:
                    value += f" ± {spread:.{precision}f}"
                cells.append(value)
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def validate_runs(experiments: dict[str, Any]) -> int:
    run_counts = {
        case_name: len(experiments[case_name]) for case_name in CASE_ORDER
    }
    if any(count == 0 for count in run_counts.values()):
        raise ValueError("No random-seed runs found")
    if len(set(run_counts.values())) != 1:
        raise ValueError(
            "Provider and Service cases must have the same number of runs"
        )

    seeds: dict[str, list[int]] = {}
    for case_name in CASE_ORDER:
        case_seeds = []
        for run in experiments[case_name]:
            seed = run.get("random_seed") if isinstance(run, dict) else None
            if isinstance(seed, bool) or not isinstance(seed, int):
                raise ValueError(
                    f"Invalid or missing random_seed in {case_name} case"
                )
            case_seeds.append(seed)
        seeds[case_name] = case_seeds

    if seeds["provider"] != seeds["service"]:
        raise ValueError(
            "Provider and Service cases must use the same random seeds "
            "in the same order"
        )
    return run_counts["provider"]


def build_markdown_table(
        experiments: dict[str, Any], precision: int
) -> str:
    missing_cases = [name for name in CASE_ORDER if name not in experiments]
    if missing_cases:
        raise ValueError(f"Missing embedding cases: {', '.join(missing_cases)}")

    run_count = validate_runs(experiments)

    lines = [
        "- Provider (same NPI): N0 Denormalized → N1 Provider-normalized",
        "- Service (same HCPCS): N0 Denormalized → N2 Service-normalized",
        "- N0 contains only Record nodes; all projections keep one "
        "non-semantic self-loop per Record for topology-only methods.",
        "",
        "### Mean ± sample SD",
        "",
        f"Values are mean ± sample SD across {run_count} random-seed runs; "
        "each run independently resamples record pairs and contributes its median.",
        "",
        *table_lines(experiments, precision, show_sd=True),
        "",
        "### Mean only",
        "",
        *table_lines(experiments, precision, show_sd=False),
    ]
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
