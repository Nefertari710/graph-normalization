#!/usr/bin/env python3
"""Generate the two headline tables for the current TPC-H fold experiment."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (
    SCRIPT_DIR.parent
    / "results"
    / "extend"
    / "tpch-sf-001"
    / "tpch_fd_experiment_extend_results.json"
)
DIRECT_FAMILY = "tpch_direct_pair_fd"
RECURSIVE_FAMILY = "tpch_recursive_fold_fd"
TABLE_ORDER = (
    "REGION",
    "NATION",
    "SUPPLIER",
    "CUSTOMER",
    "PART",
    "PARTSUPP",
    "ORDERS",
    "LINEITEM",
)
ABBREVIATIONS = {
    "REGION": "R",
    "NATION": "N",
    "SUPPLIER": "S",
    "CUSTOMER": "C",
    "PART": "P",
    "PARTSUPP": "PS",
    "ORDERS": "O",
    "LINEITEM": "L",
}


class AnalysisError(RuntimeError):
    """Raised when the experiment report is missing or inconsistent."""


@dataclass(frozen=True)
class CaseResult:
    strategy: str
    mean_normalized_ms: float
    mean_replacement_ms: float
    mean_delta_ms: float
    mean_time_ratio: float
    kappa: int | None
    mean_conversion_ms: float
    delta_nodes: int
    delta_relationships: int
    delta_cells: int


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AnalysisError(message)


def field(value: Any, *path: str) -> Any:
    current = value
    for name in path:
        if not isinstance(current, dict) or name not in current:
            raise AnalysisError(f"Missing required field: {'.'.join(path)}")
        current = current[name]
    return current


def text(value: Any, name: str) -> str:
    require(isinstance(value, str) and bool(value), f"{name} must be non-empty text")
    return value


def integer(value: Any, name: str, *, non_negative: bool = False) -> int:
    require(isinstance(value, int) and not isinstance(value, bool), f"{name} must be an integer")
    if non_negative:
        require(value >= 0, f"{name} must be non-negative")
    return value


def number(value: Any, name: str, *, positive: bool = False) -> float:
    require(isinstance(value, (int, float)) and not isinstance(value, bool), f"{name} must be numeric")
    result = float(value)
    require(math.isfinite(result), f"{name} must be finite")
    if positive:
        require(result > 0, f"{name} must be positive")
    return result


def normalize_cases(report: dict[str, Any]) -> list[dict[str, Any]]:
    if all(name in report for name in ("case_id", "experiment_family", "full_8table_replacement_update")):
        cases = [report]
    else:
        raw_cases = field(report, "cases")
        require(isinstance(raw_cases, list) and bool(raw_cases), "cases must be a non-empty list")
        cases = raw_cases
        if "case_count" in report:
            require(integer(report["case_count"], "case_count") == len(cases), "case_count differs from cases")
        if "case_order" in report:
            order = report["case_order"]
            require(isinstance(order, list), "case_order must be a list")
            by_id = {text(field(case, "case_id"), "case_id"): case for case in cases}
            require(len(by_id) == len(cases) and set(order) == set(by_id), "case_order does not match cases")
            cases = [by_id[text(case_id, "case_order[]")] for case_id in order]
    require(all(isinstance(case, dict) for case in cases), "Every case must be an object")
    return cases


def parse_workload(case: dict[str, Any]) -> tuple[tuple[str, str, int], ...]:
    case_id = text(field(case, "case_id"), "case_id")
    full = field(case, "full_8table_replacement_update")
    workload = field(full, "workload")
    tables = field(workload, "tables")
    require(isinstance(tables, dict) and set(tables) == set(TABLE_ORDER), f"{case_id}: workload must contain all eight tables")
    rows: list[tuple[str, str, int]] = []
    total = 0
    for table in TABLE_ORDER:
        entry = field(tables, table)
        property_name = text(field(entry, "property"), f"{case_id}.{table}.property")
        facts = integer(field(entry, "logical_facts"), f"{case_id}.{table}.logical_facts", non_negative=True)
        require(facts > 0, f"{case_id}.{table}.logical_facts must be positive")
        rows.append((table, property_name, facts))
        total += facts
    require(total == integer(field(workload, "logical_fact_count"), f"{case_id}.logical_fact_count"), f"{case_id}: workload facts do not sum to total")
    return tuple(rows)


def strategy_label(case: dict[str, Any]) -> str:
    case_id = text(field(case, "case_id"), "case_id")
    family = text(field(case, "experiment_family"), f"{case_id}.experiment_family")
    schema = integer(field(case, "report_schema_version"), f"{case_id}.report_schema_version")
    if family == DIRECT_FAMILY:
        require(schema == 12, f"{case_id}: direct case must use schema v12")
        left = text(field(case, "pair", "referencing_table", "label"), f"{case_id}.referencing_table")
        right = text(field(case, "pair", "referenced_table", "label"), f"{case_id}.referenced_table")
        require(left in ABBREVIATIONS and right in ABBREVIATIONS, f"{case_id}: unknown TPC-H table")
        return f"{ABBREVIATIONS[left]}-{ABBREVIATIONS[right]}"
    require(family == RECURSIVE_FAMILY and schema == 6, f"{case_id}: unsupported case family/schema")
    require(integer(field(case, "recursive_fold_schema_version"), f"{case_id}.recursive_fold_schema_version") == 10, f"{case_id}: recursive fold must use schema v10")
    roles = field(case, "fold_chain", "role_order")
    require(isinstance(roles, list) and len(roles) >= 3, f"{case_id}: invalid role_order")
    return "-".join(text(role, f"{case_id}.role_order[]") for role in roles)


def timed_samples(full: dict[str, Any], phase: str, case_id: str) -> dict[int, float]:
    samples = field(full, "timings", phase, "samples")
    require(isinstance(samples, list) and bool(samples), f"{case_id}.{phase}.samples must be non-empty")
    parsed: dict[int, float] = {}
    for index, sample in enumerate(samples):
        run = integer(field(sample, "run"), f"{case_id}.{phase}.samples[{index}].run")
        require(run > 0 and run not in parsed, f"{case_id}.{phase} has an invalid/duplicate run")
        parsed[run] = number(field(sample, "client_wall_ms"), f"{case_id}.{phase}.samples[{index}].client_wall_ms", positive=True)
    return parsed


def conversion_samples(full: dict[str, Any], case_id: str) -> list[float]:
    conversion = field(full, "replacement_to_normalized_conversion")
    require(
        field(conversion, "status") == "measured_once"
        and field(conversion, "destructive") is True
        and field(conversion, "source_read_during_timed_conversion") is False
        and field(conversion, "input_layout_removed_inside_timer") is True
        and field(conversion, "output_layout") == "normalized",
        f"{case_id}: unsupported conversion semantics",
    )
    samples = field(full, "timings", "replacement_to_normalized_conversion", "samples")
    require(isinstance(samples, list) and bool(samples), f"{case_id}: conversion samples must be non-empty")
    values = [
        number(field(sample, "client_wall_ms"), f"{case_id}.conversion.samples[{index}]", positive=True)
        for index, sample in enumerate(samples)
    ]
    require(integer(field(conversion, "measured_runs"), f"{case_id}.conversion.measured_runs") == len(values), f"{case_id}: conversion run count differs")
    return values


def parse_case(case: dict[str, Any]) -> CaseResult:
    case_id = text(field(case, "case_id"), "case_id")
    full = field(case, "full_8table_replacement_update")
    require(
        integer(field(full, "schema_version"), f"{case_id}.full.schema_version") == 3
        and field(full, "semantics_id") == "lossless_graph_replacement_v2"
        and field(full, "status") == "complete",
        f"{case_id}: unsupported/incomplete full-layout result",
    )
    normalized = timed_samples(full, "normalized", case_id)
    replacement = timed_samples(full, "replacement", case_id)
    paired_ids = field(full, "timing_analysis", "paired_run_ids")
    require(isinstance(paired_ids, list), f"{case_id}.paired_run_ids must be a list")
    paired = [integer(run, f"{case_id}.paired_run_ids[]") for run in paired_ids]
    require(len(paired) == len(set(paired)) and set(paired) == set(normalized) == set(replacement), f"{case_id}: normalized/replacement runs are not paired")
    n_times = [normalized[run] for run in paired]
    r_times = [replacement[run] for run in paired]
    mean_n = statistics.fmean(n_times)
    mean_r = statistics.fmean(r_times)
    mean_delta = statistics.fmean(n - r for n, r in zip(n_times, r_times, strict=True))
    mean_ratio = statistics.fmean(r / n for n, r in zip(n_times, r_times, strict=True))
    mean_conversion = statistics.fmean(conversion_samples(full, case_id))
    saving = -mean_delta
    kappa = None
    if saving > 0:
        quotient = Decimal(str(mean_conversion)) / Decimal(str(saving))
        kappa = int(quotient.to_integral_value(rounding=ROUND_CEILING))

    footprint = field(full, "actual_lossless_graph_layout_footprint")
    require(
        field(footprint, "scope") == "complete_eight_table_lossless_graph_replacement"
        and field(footprint, "measurement_basis") == "actually_materialized_run_scoped_layouts",
        f"{case_id}: unsupported footprint scope",
    )
    normalized_fp = field(footprint, "normalized")
    replacement_fp = field(footprint, "replacement")
    names = ("nodes", "explicit_relationships", "logical_business_property_cells")
    normalized_counts = [integer(field(normalized_fp, name), f"{case_id}.normalized.{name}", non_negative=True) for name in names]
    replacement_counts = [integer(field(replacement_fp, name), f"{case_id}.replacement.{name}", non_negative=True) for name in names]
    changes = tuple(r - n for n, r in zip(normalized_counts, replacement_counts, strict=True))
    stored_changes = field(footprint, "change_replacement_minus_normalized")
    require(
        changes == tuple(integer(field(stored_changes, name), f"{case_id}.change.{name}") for name in names),
        f"{case_id}: footprint changes are inconsistent",
    )
    return CaseResult(
        strategy=strategy_label(case),
        mean_normalized_ms=mean_n,
        mean_replacement_ms=mean_r,
        mean_delta_ms=mean_delta,
        mean_time_ratio=mean_ratio,
        kappa=kappa,
        mean_conversion_ms=mean_conversion,
        delta_nodes=changes[0],
        delta_relationships=changes[1],
        delta_cells=changes[2],
    )


def compact_change(value: int) -> str:
    sign = "+" if value > 0 else "-" if value < 0 else ""
    absolute = abs(value)
    if absolute < 1_000:
        return f"{value:+d}" if value else "0"
    divisor, suffix = (1_000_000, "M") if absolute >= 1_000_000 else (1_000, "K")
    rounded = (Decimal(absolute) / Decimal(divisor)).quantize(
        Decimal("1"),
        rounding=ROUND_HALF_UP,
    )
    return f"{sign}{rounded}{suffix}"


def render(workload: tuple[tuple[str, str, int], ...], cases: list[CaseResult]) -> str:
    lines = [
        "## Shared eight-table workload",
        "",
        "| Physical table | Updated property | Logical facts |",
        "|---|---|---:|",
    ]
    lines.extend(f"| {table} | `{property_name}` | {facts:,} |" for table, property_name, facts in workload)
    lines.extend(
        [
            "",
            "## Simplified comparison",
            "",
            (
                "`t_N=mean(t_N,i)`, `t_R=mean(t_R,i)`, `Δ_U=mean(t_N,i−t_R,i)`, "
                "and `A_T=mean(t_R,i/t_N,i)` use paired-run arithmetic means. Positive `Δ_U` "
                "favors replacement; `A_T>1` favors normalization. "
                "`κ=ceil(mean conversion / mean(t_R−t_N))` when normalization saves "
                "update time; otherwise it is N/A."
            ),
            "",
            (
                "| No. | Fold strategy | t_N (ms) | t_R (ms) | Δ_U (ms) | A_T | "
                "Normalization (ms) | κ | ΔV | ΔE | ΔC |"
            ),
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for index, case in enumerate(cases, start=1):
        lines.append(
            "| "
            + " | ".join(
                (
                    str(index),
                    case.strategy,
                    f"{case.mean_normalized_ms:,.3f}",
                    f"{case.mean_replacement_ms:,.3f}",
                    f"{case.mean_delta_ms:+,.3f}",
                    f"{case.mean_time_ratio:.3f}",
                    f"{case.mean_conversion_ms:,.3f}",
                    f"{case.kappa:,}" if case.kappa is not None else "N/A",
                    compact_change(case.delta_nodes),
                    compact_change(case.delta_relationships),
                    compact_change(case.delta_cells),
                )
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate two Markdown tables from the current full-layout TPC-H experiment.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="input experiment JSON")
    parser.add_argument("--output", type=Path, default=None, help="output Markdown path (default: input with .md suffix)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = args.input.expanduser().resolve()
    output_path = args.output.expanduser().resolve() if args.output else input_path.with_suffix(".md")
    try:
        require(input_path != output_path, "Input and output paths must differ")
        report = json.loads(input_path.read_text(encoding="utf-8"))
        require(isinstance(report, dict), "JSON root must be an object")
        raw_cases = normalize_cases(report)
        workload = parse_workload(raw_cases[0])
        for case in raw_cases[1:]:
            require(parse_workload(case) == workload, "Cases do not share the same eight-table workload")
        results = [parse_case(case) for case in raw_cases]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render(workload, results), encoding="utf-8")
    except (AnalysisError, OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Markdown report written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
