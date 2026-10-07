#!/usr/bin/env python3
"""Export Offshore embedding summaries and Address case-study data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import fmean, stdev
from typing import Any, Sequence


EMBEDDING_DIR = Path(__file__).resolve().parents[1]
ANALYSIS_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (
    EMBEDDING_DIR / "results" / "offshorecsv" / "embedding_offshore.json"
)
DEFAULT_OUTPUT = ANALYSIS_DIR / "result.md"

CASES = (
    ("address_country", "Address", "countries → country_codes"),
    (
        "service_provider",
        "Entity",
        "service_provider → (sourceID, valid_until)",
    ),
    ("country", "Entity", "country_codes → countries"),
    ("entity_country_reverse", "Entity", "countries → country_codes"),
    ("intermediary_country", "Intermediary", "country_codes → countries"),
    (
        "intermediary_country_reverse",
        "Intermediary",
        "countries → country_codes",
    ),
    ("intermediary_valid_until", "Intermediary", "valid_until → sourceID"),
    ("intermediary_source", "Intermediary", "sourceID → valid_until"),
    (
        "other_jurisdiction",
        "Other",
        "jurisdiction → jurisdiction_description",
    ),
    (
        "other_jurisdiction_reverse",
        "Other",
        "jurisdiction_description → jurisdiction",
    ),
    ("other_country", "Other", "country_codes → countries"),
    ("other_country_reverse", "Other", "countries → country_codes"),
)
CASE_ORDER = tuple(case_name for case_name, _, _ in CASES)

METHODS = (
    ("property_hash", "Property Hash"),
    ("fast_rp", "FastRP"),
    ("node2vec", "Node2Vec"),
    ("hash_gnn", "HashGNN"),
    ("graph_sage", "GraphSAGE"),
)

PAIR_TYPES = (
    ("Pairs with the Same LHS Value", "similarities"),
    ("Pairs with Different LHS Values", "different_lhs_similarities"),
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write normalized and denormalized median similarities for all "
            "selected Offshore FD embedding cases to a Markdown table. "
            "Multi-seed results are reported as the mean and sample SD of "
            "per-seed medians."
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


def seed_summary(
    runs: list[dict[str, Any]],
    section: str,
    method_name: str,
    representation: str,
) -> tuple[float, float]:
    values = [
        float(run[section][method_name][representation]["median"])
        for run in runs
    ]
    if not values:
        raise ValueError("No seed runs found")
    return fmean(values), stdev(values) if len(values) > 1 else 0.0


def table_lines(
    experiments: dict[str, Any],
    section: str,
    precision: int,
    show_sd: bool,
) -> list[str]:
    headers = ["Entity", "Functional Dependency"]
    for _, label in METHODS:
        headers.extend((f"{label} Norm", f"{label} Denorm"))

    lines = [
        "| " + " | ".join(headers) + " |",
        "| :--- | :--- | "
        + " | ".join("---:" for _ in headers[2:])
        + " |",
    ]
    for case_name, entity, dependency in CASES:
        cells = [entity, f"`{dependency}`"]
        for method_name, _ in METHODS:
            for representation in ("normalized", "denormalized"):
                center, spread = seed_summary(
                    experiments[case_name],
                    section,
                    method_name,
                    representation,
                )
                value = f"{center:.{precision}f}"
                if show_sd:
                    value += f" ± {spread:.{precision}f}"
                cells.append(value)
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def pair_tables(
    experiments: dict[str, Any],
    pair_types: tuple[tuple[str, str], ...],
    precision: int,
    show_sd: bool,
) -> list[str]:
    lines: list[str] = []
    for pair_type, section in pair_types:
        if lines:
            lines.append("")
        lines.extend(
            [
                f"#### {pair_type}",
                "",
                *table_lines(experiments, section, precision, show_sd),
            ]
        )
    return lines


def markdown_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).replace("|", "\\|").replace("\n", " ")


def address_case_study_lines(
    experiments: dict[str, Any], precision: int
) -> list[str]:
    runs = experiments.get("address_country", [])
    candidates = []
    for run in runs:
        nodes = {
            int(node["node_id"]): node
            for node in run.get("nodes", [])
        }
        for pair in run.get("pairs", []):
            node_ids = [int(node_id) for node_id in pair["node_ids"]]
            if any(node_id not in nodes for node_id in node_ids):
                continue
            scores = pair.get("similarities", {}).get("fast_rp")
            if not scores:
                continue
            denormalized = float(scores["denormalized"])
            normalized = float(scores["normalized"])
            candidates.append((
                normalized - denormalized,
                run,
                pair,
                [nodes[node_id] for node_id in node_ids],
                denormalized,
                normalized,
            ))
    if not candidates:
        return []

    delta, run, pair, nodes, denormalized, normalized = max(
        candidates, key=lambda candidate: candidate[0]
    )
    same_denormalized = seed_summary(
        runs, "similarities", "fast_rp", "denormalized"
    )[0]
    same_normalized = seed_summary(
        runs, "similarities", "fast_rp", "normalized"
    )[0]
    different_denormalized = seed_summary(
        runs, "different_lhs_similarities", "fast_rp", "denormalized"
    )[0]
    different_normalized = seed_summary(
        runs, "different_lhs_similarities", "fast_rp", "normalized"
    )[0]
    denormalized_separation = same_denormalized - different_denormalized
    normalized_separation = same_normalized - different_normalized

    number = lambda value: f"{value:.{precision}f}"
    lines = [
        "### Address FastRP Case Study Data",
        "",
        ("The selected same-determinant pair has the largest "
         "normalized-minus-denormalized FastRP similarity."),
        "",
        "| Seed | Determinant | Node IDs | FastRP Denorm | FastRP Norm | N − D |",
        "| ---: | :--- | :--- | ---: | ---: | ---: |",
        "| " + " | ".join((
            str(run["random_seed"]),
            str(pair["determinant"]).replace("|", "\\|"),
            ", ".join(str(node_id) for node_id in pair["node_ids"]),
            number(denormalized),
            number(normalized),
            number(delta),
        )) + " |",
        "",
        "#### Selected Node Information",
        "",
        "| Node ID | Labels | Original degree | Relationship counts | Properties |",
        "| ---: | :--- | ---: | :--- | :--- |",
    ]
    for node in nodes:
        lines.append("| " + " | ".join((
            str(node["node_id"]),
            ", ".join(node.get("labels", [])),
            str(node.get("original_degree", 0)),
            markdown_json(node.get("original_relationship_type_counts", {})),
            markdown_json(node.get("properties", {})),
        )) + " |")

    lines.extend([
        "",
        "#### Aggregate FastRP Comparison",
        "",
        ("Values are means across the per-seed medians used in the main "
         "tables."),
        "",
        "| Pair set | Denormalized | Normalized | N − D |",
        "| :--- | ---: | ---: | ---: |",
        (f"| Same determinant | {number(same_denormalized)} | "
         f"{number(same_normalized)} | "
         f"{number(same_normalized - same_denormalized)} |"),
        (f"| Different determinants | {number(different_denormalized)} | "
         f"{number(different_normalized)} | "
         f"{number(different_normalized - different_denormalized)} |"),
        (f"| Same−different separation | "
         f"{number(denormalized_separation)} | "
         f"{number(normalized_separation)} | "
         f"{number(normalized_separation - denormalized_separation)} |"),
    ])
    return lines


def build_markdown_table(
    experiments: dict[str, Any], precision: int
) -> str:
    missing_cases = [name for name in CASE_ORDER if name not in experiments]
    if missing_cases:
        raise ValueError(f"Missing embedding cases: {', '.join(missing_cases)}")

    run_counts = {len(experiments[name]) for name in CASE_ORDER}
    if len(run_counts) != 1:
        raise ValueError("Embedding cases have different numbers of seed runs")
    run_count = next(iter(run_counts))
    if run_count == 0:
        raise ValueError("No seed runs found")

    different_lhs_flags = [
        "different_lhs_similarities" in run
        for name in CASE_ORDER
        for run in experiments[name]
    ]
    if any(different_lhs_flags) and not all(different_lhs_flags):
        raise ValueError("Different-LHS results are incomplete")
    pair_types = PAIR_TYPES if all(different_lhs_flags) else PAIR_TYPES[:1]

    lines = [
        "### Mean ± sample SD",
        "",
        f"Values are mean ± sample SD across {run_count} random-seed "
        "runs; each run contributes its median over sampled node pairs.",
        "Same- and different-LHS-value pairs are reported separately.",
        "",
        *pair_tables(experiments, pair_types, precision, show_sd=True),
        "",
        "### Mean only",
        "",
        *pair_tables(experiments, pair_types, precision, show_sd=False),
    ]
    case_study = address_case_study_lines(experiments, precision)
    if case_study:
        lines.extend(("", *case_study))
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
