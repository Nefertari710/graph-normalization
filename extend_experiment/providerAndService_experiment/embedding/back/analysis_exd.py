#!/usr/bin/env python3
"""Summarize the two sequential normalization steps as Markdown tables."""

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
        / "embedding_providerandservice_exd.json"
)
DEFAULT_OUTPUT = ANALYSIS_DIR / "result_exd.md"

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
            "Compare Denormalized -> Service-normalized and then "
            "Service-normalized -> Service+Provider-normalized embeddings."
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


def load_runs(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    experiments = payload.get("experiments")
    if not isinstance(experiments, dict):
        raise ValueError(f"Missing object 'experiments' in {path}")
    runs = experiments.get("sequential")
    if not isinstance(runs, list):
        raise ValueError(f"experiments.sequential must be a list in {path}")
    if not runs:
        raise ValueError("No sequential random-seed runs found")
    return runs


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
        steps: list[dict[str, Any]], precision: int, show_sd: bool
) -> list[str]:
    headers = ["Step", "Comparison"]
    for _, label in METHODS:
        headers.extend((f"{label} Denormal", f"{label} Normal"))

    lines = [
        "| " + " | ".join(headers) + " |",
        "| :--- | :--- | "
        + " | ".join("---:" for _ in headers[2:])
        + " |",
    ]
    for step in steps:
        cells = [step["label"], step["comparison"]]
        for method_name, _ in METHODS:
            for representation in (step["before"], step["after"]):
                center, spread = seed_summary(
                    step["runs"], method_name, representation
                )
                value = f"{center:.{precision}f}"
                if show_sd:
                    value += f" ± {spread:.{precision}f}"
                cells.append(value)
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def build_markdown_table(
        runs: list[dict[str, Any]],
        precision: int,
) -> str:
    steps = [
        {
            "label": "`FD1 Service`",
            "comparison": "N0 Denormalized → N1 Service-normalized",
            "runs": runs,
            "before": "denormalized",
            "after": "service_normalized",
        },
        {
            "label": "`FD2 Provider`",
            "comparison": (
                "N1 Service-normalized → "
                "N2 Service+Provider-normalized"
            ),
            "runs": runs,
            "before": "service_normalized",
            "after": "service_provider_normalized",
        },
    ]

    lines = [
        "## Sequential normalization",
        "",
        "`N0 Denormalized --FD1--> N1 Service-normalized --FD2--> "
        "N2 Service+Provider-normalized`",
        "",
        "- `FD1`: `hcpcs_cd → (hcpcs_desc, hcpcs_drug_ind)`",
        "- `FD2`: `rndrng_npi → Provider attributes`",
        "",
        "### Mean ± sample SD",
        "",
        f"Values are summarized across {len(runs)} random-seed runs; each "
        "run contributes its median over the fixed same-NPI record pairs.",
        "",
        "Both rows use exactly the same record pairs. N1 is computed once "
        "per seed and is shared by the two comparisons.",
        "Within each row, Denormal is the less-normalized baseline and "
        "Normal is the next normalized state.",
        "",
        *table_lines(steps, precision, show_sd=True),
        "",
        "### Mean only",
        "",
        *table_lines(steps, precision, show_sd=False),
    ]
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    runs = load_runs(args.input)
    table = build_markdown_table(runs, args.precision)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(table, encoding="utf-8")
    print(f"Wrote: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
