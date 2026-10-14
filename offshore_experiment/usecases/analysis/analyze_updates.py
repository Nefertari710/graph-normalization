#!/usr/bin/env python3
"""Summarize reference-FD update results in one Markdown table.

Input: ../results/updates_reference_fd.json
Output: ../results/report/updates_reference_fd.md
No Neo4j connection is needed. Override paths with --input and --output.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
from statistics import median

HERE = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = HERE / "results" / "updates_reference_fd.json"
DEFAULT_OUTPUT = HERE / "results" / "report" / "updates_reference_fd.md"


def load_results(path: Path) -> dict:
    report = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(report, dict) or not isinstance(report.get("types"), dict) or not report["types"]:
        raise ValueError("The input JSON must contain a nonempty 'types' object.")
    for label, result in report["types"].items():
        required = ("fd", "valid_groups", "redundant_values", "normalization_seconds", "cases")
        if not isinstance(result, dict) or any(field not in result for field in required):
            raise ValueError(f"{label}: Required result fields are missing.")
        for field in ("valid_groups", "redundant_values"):
            if type(result[field]) is not int or result[field] < 0:
                raise ValueError(f"{label}: {field} must be a nonnegative integer.")
        cost = result["normalization_seconds"]
        if type(cost) not in (int, float) or not math.isfinite(cost) or cost < 0:
            raise ValueError(f"{label}: normalization_seconds must be finite and nonnegative.")
        if not isinstance(result["cases"], list):
            raise ValueError(f"{label}: cases must be a list.")
        for case in result["cases"]:
            if not isinstance(case, dict) or "copies" not in case:
                raise ValueError(f"{label}: Sampled-group copies are missing.")
            if type(case["copies"]) is not int or case["copies"] < 1:
                raise ValueError(f"{label}: copies must be a positive integer.")
            for field in ("d_median_ms", "n_median_ms"):
                value = case.get(field) if isinstance(case, dict) else None
                if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                    raise ValueError(f"{label}: {field} must be finite and positive.")
    return report


def render_report(report: dict, input_path: Path, output_path: Path) -> str:
    types = report["types"]
    source = Path(os.path.relpath(input_path.resolve(), output_path.resolve().parent)).as_posix()
    fds = list(dict.fromkeys(result["fd"] for result in types.values()))
    samples = "; ".join(f"{label}: {len(result['cases'])}" for label, result in types.items())
    lines = [
        "# Offshore Reference-FD Updates", "",
        f"Source: [{input_path.name}](<{source}>).", "",
        "FD: " + "; ".join(f"`{fd}`" for fd in fds) + ".", "",
        f"Sampled groups — {samples}. Repetitions per group: "
        f"{report.get('settings', {}).get('repeats', 'not recorded')}.", "",
        "| Type | FD (determinant) | Groups | Redund. | B_N (s) | Median U_D (ms) | Median U_N (ms) | Largest copies | Largest U_D (ms) | Largest U_N (ms) |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for label, result in types.items():
        cases = result["cases"]
        d = f"{median(case['d_median_ms'] for case in cases):.1f}" if cases else "—"
        n = f"{median(case['n_median_ms'] for case in cases):.1f}" if cases else "—"
        largest = max(cases, key=lambda case: case["copies"]) if cases else None
        copies = f"{largest['copies']:,}" if largest else "—"
        largest_d = f"{largest['d_median_ms']:.1f}" if largest else "—"
        largest_n = f"{largest['n_median_ms']:.1f}" if largest else "—"
        determinant = result["fd"].split("->", 1)[0].strip("{} ")
        lines.append(f"| {label} | `{determinant}` | {result['valid_groups']:,} | "
                     f"{result['redundant_values'] / 1000:.1f}K | "
                     f"{result['normalization_seconds']:.1f} | {d} | {n} | "
                     f"{copies} | {largest_d} | {largest_n} |")
    lines.extend([
        "", "B_N = normalization cost, including index setup. D = denormalized; N = normalized. "
        "Median U_D and U_N summarize the sampled groups' median times. "
        "Largest means the sampled group with the most copies, not the largest group in the full graph. "
        "K = 1,000; display precision matches the experiment's LaTeX rows.", "",
        "Counts refer to eligible groups and their logical redundant copies. "
        "The D update query does not repeat the eligibility filters. Empty samples appear as —.",
    ])
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    if args.input.resolve() == args.output.resolve():
        parser.error("Input JSON and output Markdown must have different paths.")
    try:
        report = render_report(load_results(args.input), args.input, args.output)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"Analysis failed: {exc}\n")
    print(report, end="")
    print(f"\nMarkdown report saved to: {args.output.resolve()}")


if __name__ == "__main__":
    main()
