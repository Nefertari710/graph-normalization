#!/usr/bin/env python3
"""Analyze Offshore update results with the paper's aggregation rules.

Run this file directly, or pass --input /path/to/updates_offshore.json.
The report is printed to stdout; this script does not write any files.

Per FD, table medians are medians of the sampled groups' median times.
For the speedup analysis, compute D/N for each group before taking the
median within each copy-count interval. The largest group is the largest
sampled group, rather than the largest group in the full graph.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import median

DEFAULT_INPUT = (
    Path(__file__).resolve().parent.parent / "results" / "updates_offshore.json"
)
COPY_BINS = (
    ("<100", 0, 100),
    ("100–999", 100, 1000),
    ("1,000–9,999", 1000, 10000),
    ("≥10,000", 10000, None),
)


def load_results(path: Path) -> dict:
    """Read the measured per-group medians, without rerunning Neo4j."""
    with path.open(encoding="utf-8") as handle:
        report = json.load(handle)
    if not isinstance(report, dict) or not isinstance(report.get("fds"), dict):
        raise ValueError("The input JSON must contain an 'fds' object.")
    if not report["fds"]:
        raise ValueError("The input JSON contains no FD results.")

    for name, fd in report["fds"].items():
        required = (
            "node_type", "fd", "valid_groups", "redundant_values",
            "normalization_seconds", "cases",
        )
        if not isinstance(fd, dict) or any(field not in fd for field in required):
            raise ValueError(f"{name}: Required FD result fields are missing.")
        if not isinstance(fd["cases"], list) or not fd["cases"]:
            raise ValueError(f"{name}: No sampled groups are available for analysis.")
        cost = fd["normalization_seconds"]
        if not isinstance(cost, (int, float)) or not math.isfinite(cost) or cost < 0:
            raise ValueError(f"{name}: normalization_seconds must be a finite nonnegative number.")
        for case in fd["cases"]:
            if not isinstance(case, dict) or any(
                field not in case
                for field in ("key", "copies", "d_median_ms", "n_median_ms")
            ):
                raise ValueError(f"{name}: Required sampled-group fields are missing.")
            if type(case["copies"]) is not int or case["copies"] < 1:
                raise ValueError(f"{name}: copies must be a positive integer.")
            for field in ("d_median_ms", "n_median_ms"):
                value = case[field]
                if (
                    not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    or value <= 0
                ):
                    raise ValueError(f"{name}: {field} must be a finite positive number.")
    return report


def compact_count(value: int) -> str:
    """Use the paper's one-decimal K notation for counts >= 1,000."""
    return f"{value / 1000:.1f}K" if value >= 1000 else str(value)


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def render_analysis(report: dict, input_path: Path) -> str:
    fds = report["fds"]
    settings = report.get("settings", {})
    lines = [
        "# Offshore Update Experiment Analysis",
        "",
        f"Input: {input_path}",
        f"Experiment timestamp: {report.get('created_at', 'not recorded')}",
        (
            f"FDs: {len(fds)}; maximum sampled groups per FD: "
            f"{settings.get('groups', 'not recorded')}; repetitions: "
            f"{settings.get('repeats', 'not recorded')}; "
            f"random seed: {settings.get('seed', 'not recorded')}."
        ),
        "",
        "## Paper Table: FD Updates on the Full Graph",
        "",
        "D denotes the denormalized representation; N denotes the normalized representation. B_N is in seconds; U_D/U_N are in milliseconds.",
        "Groups and Redund. cover all valid groups; timing statistics cover sampled groups.",
        "The Median columns take the medians of the groups' d_median_ms and n_median_ms, respectively.",
        "The Largest group columns use the sampled group with the most copies. K = 1,000; values use the paper's display precision.",
        "",
    ]
    rows = []
    samples = []
    for name, fd in fds.items():
        cases = fd["cases"]
        largest = max(cases, key=lambda case: case["copies"])
        determinant = fd["fd"].split("->", 1)[0].strip()
        rows.append([
            str(fd["node_type"]),
            determinant,
            str(fd["valid_groups"]),
            compact_count(fd["redundant_values"]),
            f"{fd['normalization_seconds']:.1f}",
            f"{median(case['d_median_ms'] for case in cases):.1f}",
            f"{median(case['n_median_ms'] for case in cases):.1f}",
            compact_count(largest["copies"]),
            f"{largest['d_median_ms']:.1f}",
            f"{largest['n_median_ms']:.1f}",
        ])
        samples.extend(
            {
                **case,
                "fd_name": name,
                "node_type": fd["node_type"],
                "speedup": case["d_median_ms"] / case["n_median_ms"],
            }
            for case in cases
        )

    lines.append(markdown_table(
        [
            "Type", "Determinant X", "Groups", "Redund.", "B_N (s)",
            "Median U_D", "Median U_N", "Largest copies",
            "Largest U_D", "Largest U_N",
        ],
        rows,
    ))
    n_min = min(case["n_median_ms"] for case in samples)
    n_max = max(case["n_median_ms"] for case in samples)
    fastest = max(samples, key=lambda case: case["speedup"])
    most_costly_name, most_costly_fd = max(
        fds.items(), key=lambda item: item[1]["normalization_seconds"]
    )
    lines.extend([
        "",
        "## Speedup by Copy Count",
        "",
        "Compute S = d_median_ms / n_median_ms for each group, then take the median S within each interval. S > 1 favors normalization.",
        "Intervals: copies < 100; 100 ≤ copies < 1,000; 1,000 ≤ copies < 10,000; copies ≥ 10,000.",
        "",
    ])
    bin_rows = []
    bin_summaries = []
    for label, lower, upper in COPY_BINS:
        values = [
            case["speedup"] for case in samples
            if case["copies"] >= lower
            and (upper is None or case["copies"] < upper)
        ]
        speedup = median(values) if values else None
        bin_rows.append([
            label, str(len(values)),
            f"{speedup:.1f}×" if speedup is not None else "No samples",
            f"{speedup:.6f}" if speedup is not None else "—",
        ])
        if speedup is not None:
            bin_summaries.append(f"{label}: {speedup:.1f}× ({len(values)} groups)")
        else:
            bin_summaries.append(f"{label} No samples")
    lines.append(markdown_table(
        ["Copies", "Sampled groups", "Median speedup (paper precision)", "Median speedup (6 decimals)"],
        bin_rows,
    ))
    lines.extend([
        "",
        "## Analysis Corresponding to the Paper",
        "",
        (
            f"Across {len(fds)} FDs, {len(samples)} sampled groups were analyzed. "
            f"The per-group median normalized update time ranges from {n_min:.1f} to {n_max:.1f} ms "
            f"(before display rounding: {n_min:.6f}–{n_max:.6f} ms)."
        ),
        "",
        "Median group speedups by copy-count interval: " + "; ".join(bin_summaries) + ".",
        "",
        (
            f"The maximum observed speedup is {fastest['speedup']:.0f}× "
            f"(before display rounding: {fastest['speedup']:.6f}×), "
            f"from group {fastest['key']} of FD {fastest['fd_name']} "
            f"({fastest['node_type']}, {fastest['copies']:,} copies). "
            f"Its D/N median update times are {fastest['d_median_ms']:.3f}/"
            f"{fastest['n_median_ms']:.3f} ms, respectively."
        ),
        "",
        (
            f"The maximum one-time normalization cost per FD is "
            f"{most_costly_fd['normalization_seconds']:.1f} s "
            f"(before display rounding: {most_costly_fd['normalization_seconds']:.6f} s; "
            f"FD: {most_costly_name})."
        ),
        "",
        "All values are computed from the input JSON using the paper's aggregation rules. "
        "The reported range covers per-group median times; raw repeated timings are not stored in this JSON.",
    ])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_INPUT,
        help="Input JSON; defaults to ../results/updates_offshore.json.",
    )
    args = parser.parse_args()
    try:
        report = load_results(args.input)
        output = render_analysis(report, args.input.resolve())
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"Analysis failed: {exc}\n")
    print(output)


if __name__ == "__main__":
    main()
