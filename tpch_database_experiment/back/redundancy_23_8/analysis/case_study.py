#!/usr/bin/env python3
"""Create a one-sample case-study table for L -> PS -> S -> N -> R."""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (
    SCRIPT_DIR.parent
    / "results"
    / "new"
    / "tpch-sf-001"
    / "new_redundancy_results.json"
)

# mask = sum of the edge bits defined in scripts/new_redundancy.py.
STRATEGIES = (
    ("NR", 128),
    ("SNR", 192),
    ("PSSNR", 200),
    ("LPSSNR", 201),
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
    normalized_copies = int(baseline_sample["touched_nodes"])
    rows: list[dict[str, Any]] = []

    for name, mask in STRATEGIES:
        row: dict[str, Any] = {
            "name": name,
            "normalized_ms": normalized_ms,
            "normalized_copies": normalized_copies,
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
    return rows


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

    lines = [
        "# R Redundancy Update Case Study",
        "",
        "- Chain: `L → PS → S → N → R`",
        f"- Database: `{report.get('database', 'N/A')}`",
        (
            f"- Selected sample: group `{group}` of `{len(eligible_groups)}` "
            f"eligible paired R samples ({selection})"
        ),
        f"- Update key: `{key}`; property: `r_comment`",
        f"- Completed target strategies: `{completed}/{len(STRATEGIES)}`",
        "",
        (
            "Every row uses this single paired sample; no minimum, maximum, or median "
            "is calculated. `Δ Update Time = De-normalized − Normalized`, so a "
            "positive value is time saved by normalization."
        ),
        "",
        (
            "`R Copies Updated` is `touched_nodes`. `ΔN`, `ΔE`, and `ΔC` are "
            "De-normalized − Normalized."
        ),
        "",
        (
            "| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | "
            "Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | "
            "Normalization Time (ms) | R Copies Updated (Normalized → De-normalized) | "
            "ΔN | ΔE | ΔC |"
        ),
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for number, row in enumerate(rows, start=1):
        denormalized_copies: int | None = row.get("denormalized_copies")
        normalized_copies = int(row["normalized_copies"])
        copies = f"{normalized_copies:,} → N/A"
        if denormalized_copies is not None:
            copies = f"{normalized_copies:,} → {denormalized_copies:,}"
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
    except (CaseStudyError, KeyError, TypeError, ValueError, json.JSONDecodeError, OSError) as exc:
        print(f"Case study failed: {exc}", file=sys.stderr)
        return 1

    print(f"Selected sample group {group}")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
