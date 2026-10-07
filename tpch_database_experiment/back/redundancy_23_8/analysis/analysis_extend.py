#!/usr/bin/env python3
"""Create final-Fold-layer comparison tables from the dangerous report.

The report produced by ``redundancy_experiment_extend_dangerous.py`` times
single-key updates in a fixed trial schedule.  This analysis gives every
trial slot equal weight and compares the Fold state with the state obtained
by peeling only the final Fold layer.  Earlier recursive Fold layers remain
folded and therefore cancel from the structural deltas.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (
    SCRIPT_DIR.parent
    / "results"
    / "extend"
    / "tpch-sf-01"
    / "tpch_fd_experiment_extend_results.json"
)
DIRECT_FAMILY = "tpch_direct_pair_fd"
RECURSIVE_FAMILY = "tpch_recursive_fold_fd"
DIRECT_SCHEMA = 13
RECURSIVE_SCHEMA = 7
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
    """Raised when the input report is missing or internally inconsistent."""


@dataclass(frozen=True)
class TrialSlot:
    ordinal: int
    key_ordinal: int
    occurrence_ordinal: int
    key_values: tuple[Any, ...]
    key_tokens: tuple[str, ...]
    fanout: int


@dataclass(frozen=True)
class CaseRow:
    fold_chain: str
    normalized_update_median_ms: float
    normalized_update_max_ms: float
    normalized_update_min_ms: float
    folded_update_median_ms: float
    paired_update_delta_median_ms: float
    paired_time_amplification_median: float
    normalization_median_ms: float
    break_even_updates: int | None
    delta_nodes: int
    delta_relationships: int
    delta_property_cells: int


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AnalysisError(message)


def field(value: Any, *path: str) -> Any:
    current = value
    traversed: list[str] = []
    for part in path:
        traversed.append(part)
        if not isinstance(current, dict) or part not in current:
            raise AnalysisError(f"Missing required field: {'.'.join(traversed)}")
        current = current[part]
    return current


def non_empty_text(value: Any, label: str) -> str:
    require(isinstance(value, str) and bool(value.strip()), f"{label} must be a non-empty string")
    return value


def integer(value: Any, label: str, *, minimum: int = 0) -> int:
    require(isinstance(value, int) and not isinstance(value, bool), f"{label} must be an integer")
    require(value >= minimum, f"{label} must be at least {minimum}")
    return value


def number(value: Any, label: str, *, positive: bool = False) -> float:
    require(isinstance(value, (int, float)) and not isinstance(value, bool), f"{label} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise AnalysisError(f"{label} is not a finite number") from exc
    require(math.isfinite(result), f"{label} must be finite")
    if positive:
        require(result > 0.0, f"{label} must be positive")
    return result


def normalize_cases(report: dict[str, Any]) -> list[dict[str, Any]]:
    if "cases" not in report:
        require("case_id" in report, "Input is neither a suite nor a single case")
        return [report]

    raw_cases = field(report, "cases")
    require(isinstance(raw_cases, list) and raw_cases, "cases must be a non-empty list")
    cases: list[dict[str, Any]] = []
    by_id: dict[str, dict[str, Any]] = {}
    for index, case in enumerate(raw_cases, start=1):
        require(isinstance(case, dict), f"cases[{index}] must be an object")
        case_id = non_empty_text(field(case, "case_id"), f"cases[{index}].case_id")
        require(case_id not in by_id, f"Duplicate case_id: {case_id}")
        cases.append(case)
        by_id[case_id] = case

    if "case_order" not in report:
        return cases
    order = field(report, "case_order")
    require(isinstance(order, list), "case_order must be a list")
    require(
        len(order) == len(cases)
        and all(isinstance(case_id, str) and case_id for case_id in order)
        and len(set(order)) == len(order)
        and set(order) == set(by_id),
        "case_order must contain every unique case_id exactly once",
    )
    return [by_id[case_id] for case_id in order]


def validate_case_scope(case: dict[str, Any]) -> str:
    case_id = non_empty_text(field(case, "case_id"), "case_id")
    family = field(case, "experiment_family")
    schema = integer(field(case, "report_schema_version"), f"{case_id}.report_schema_version")
    if family == DIRECT_FAMILY:
        require(schema == DIRECT_SCHEMA, f"{case_id} must use direct schema {DIRECT_SCHEMA}")
    elif family == RECURSIVE_FAMILY:
        require(schema == RECURSIVE_SCHEMA, f"{case_id} must use recursive schema {RECURSIVE_SCHEMA}")
        require(
            field(case, "fold_chain", "normalization", "scope")
            == "peel only the final fold layer",
            f"{case_id} does not normalize only the final Fold layer",
        )
    else:
        raise AnalysisError(f"Unsupported experiment_family in {case_id}: {family!r}")

    before = non_empty_text(
        field(case, "dataset", "source_graph_sha256_before"),
        f"{case_id}.source_graph_sha256_before",
    )
    after = non_empty_text(
        field(case, "dataset", "source_graph_sha256_after"),
        f"{case_id}.source_graph_sha256_after",
    )
    require(before == after, f"{case_id} changed the source graph")
    return before


def fold_chain_label(case: dict[str, Any]) -> str:
    if field(case, "experiment_family") == DIRECT_FAMILY:
        pair = field(case, "pair")
        labels = (
            field(pair, "referencing_table", "label"),
            field(pair, "referenced_table", "label"),
        )
        return " → ".join(ABBREVIATIONS.get(label, str(label)) for label in labels)

    roles = field(case, "fold_chain", "role_order")
    require(isinstance(roles, list) and len(roles) >= 2, "fold_chain.role_order is invalid")
    return " → ".join(non_empty_text(role, "fold role") for role in roles)


def parse_trial_slots(case: dict[str, Any], trial_count: int) -> dict[int, TrialSlot]:
    case_id = field(case, "case_id")
    raw_slots = field(case, "configuration", "update_trial_slots")
    require(
        isinstance(raw_slots, list) and len(raw_slots) == trial_count,
        f"{case_id} must contain exactly {trial_count} update_trial_slots",
    )
    slots: dict[int, TrialSlot] = {}
    for expected_ordinal, raw in enumerate(raw_slots, start=1):
        require(isinstance(raw, dict), f"{case_id}.update_trial_slots[{expected_ordinal}] must be an object")
        ordinal = integer(field(raw, "trial_ordinal"), "trial_ordinal", minimum=1)
        require(ordinal == expected_ordinal, f"{case_id} trial slots are not in execution order")
        key_values = field(raw, "key_values")
        key_tokens = field(raw, "key_value_tokens")
        require(isinstance(key_values, list) and key_values, "trial key_values must be a non-empty list")
        require(
            isinstance(key_tokens, list)
            and len(key_tokens) == len(key_values)
            and all(isinstance(token, str) and token for token in key_tokens),
            "trial key_value_tokens must match key_values",
        )
        fanout = integer(field(raw, "fanout"), "trial fanout", minimum=1)
        require(
            integer(field(raw, "folded_physical_writes"), "folded_physical_writes", minimum=1)
            == fanout
            and integer(field(raw, "normalized_physical_writes"), "normalized_physical_writes", minimum=1)
            == 1,
            f"{case_id} trial {ordinal} has inconsistent physical-write counters",
        )
        slots[ordinal] = TrialSlot(
            ordinal=ordinal,
            key_ordinal=integer(field(raw, "unique_key_ordinal"), "unique_key_ordinal", minimum=1),
            occurrence_ordinal=integer(
                field(raw, "key_occurrence_ordinal"),
                "key_occurrence_ordinal",
                minimum=1,
            ),
            key_values=tuple(key_values),
            key_tokens=tuple(key_tokens),
            fanout=fanout,
        )
    return slots


def parse_update_samples(
    case: dict[str, Any],
    phase: str,
    slots: dict[int, TrialSlot],
    measured_runs: int,
    *,
    folded: bool,
) -> dict[tuple[int, int], float]:
    case_id = field(case, "case_id")
    raw_samples = field(case, "timings", phase, "samples")
    expected_count = len(slots) * measured_runs
    require(
        isinstance(raw_samples, list) and len(raw_samples) == expected_count,
        f"{case_id}.{phase} must contain {expected_count} samples",
    )
    expected_order = [
        (trial_ordinal, run)
        for run in range(1, measured_runs + 1)
        for trial_ordinal in range(1, len(slots) + 1)
    ]
    parsed: dict[tuple[int, int], float] = {}
    for expected_identity, raw in zip(expected_order, raw_samples, strict=True):
        require(isinstance(raw, dict), f"{case_id}.{phase} sample must be an object")
        identity = (
            integer(field(raw, "update_trial_ordinal"), "update_trial_ordinal", minimum=1),
            integer(field(raw, "run"), "run", minimum=1),
        )
        require(identity == expected_identity, f"{case_id}.{phase} samples are not in schedule order")
        slot = slots[identity[0]]
        require(
            integer(field(raw, "update_key_ordinal"), "update_key_ordinal", minimum=1)
            == slot.key_ordinal
            and integer(
                field(raw, "update_key_occurrence_ordinal"),
                "update_key_occurrence_ordinal",
                minimum=1,
            )
            == slot.occurrence_ordinal
            and tuple(field(raw, "update_key_values")) == slot.key_values
            and tuple(field(raw, "update_key_value_tokens")) == slot.key_tokens
            and integer(field(raw, "update_key_fanout"), "update_key_fanout", minimum=1)
            == slot.fanout,
            f"{case_id}.{phase} sample {identity} does not match its trial slot",
        )
        expected_writes = slot.fanout if folded else 1
        require(
            integer(field(raw, "affected_nodes"), "affected_nodes") == expected_writes
            and integer(field(raw, "properties_set"), "properties_set") == expected_writes,
            f"{case_id}.{phase} sample {identity} has incorrect write counters",
        )
        parsed[identity] = number(
            field(raw, "client_wall_ms"),
            f"{case_id}.{phase}.client_wall_ms",
            positive=True,
        )
    return parsed


def parse_normalization_median(case: dict[str, Any]) -> float:
    case_id = field(case, "case_id")
    run_count = integer(
        field(case, "configuration", "normalization_runs"),
        f"{case_id}.normalization_runs",
        minimum=1,
    )
    samples = field(case, "timings", "normalization", "samples")
    require(
        isinstance(samples, list) and len(samples) == run_count,
        f"{case_id}.normalization must contain {run_count} samples",
    )
    values: list[float] = []
    for expected_run, sample in enumerate(samples, start=1):
        require(isinstance(sample, dict), f"{case_id}.normalization sample must be an object")
        require(
            integer(field(sample, "run"), "normalization run", minimum=1) == expected_run,
            f"{case_id}.normalization samples are not in run order",
        )
        values.append(
            number(
                field(sample, "client_wall_ms"),
                f"{case_id}.normalization.client_wall_ms",
                positive=True,
            )
        )
    return statistics.median(values)


def shadow_counts(state: dict[str, Any]) -> tuple[int, int]:
    if "all_case_nodes" in state and "all_case_relationships" in state:
        return (
            integer(field(state, "all_case_nodes"), "all_case_nodes"),
            integer(field(state, "all_case_relationships"), "all_case_relationships"),
        )

    node_fields = (
        "folded_join_nodes",
        "normalized_referencing_nodes",
        "normalized_referenced_nodes",
    )
    nodes = sum(integer(field(state, name), name) for name in node_fields)
    relationships = integer(
        field(state, "normalized_join_relationships"),
        "normalized_join_relationships",
    )
    boundaries = field(state, "boundary_relationships")
    require(isinstance(boundaries, dict), "boundary_relationships must be an object")
    relationships += sum(integer(value, "boundary relationship count") for value in boundaries.values())
    return nodes, relationships


def final_layer_property_cell_delta(case: dict[str, Any]) -> int:
    """Return C_Fold - C_Normalized for only the final Fold layer.

    The recursive prefix is present in both states.  For the final referenced
    role, Fold stores every non-key property on each of the J prefix rows;
    normalization stores all referenced columns once on each of the D factor
    rows.  This also accounts for the referenced key cells that exist only on
    the normalized factors.
    """

    redundancy = field(case, "redundancy")
    join_rows = integer(field(redundancy, "join_rows_J"), "join_rows_J", minimum=1)
    distinct_rows = integer(
        field(redundancy, "participating_referenced_rows_D"),
        "participating_referenced_rows_D",
        minimum=1,
    )
    column_count = integer(
        field(redundancy, "referenced_column_count"),
        "referenced_column_count",
        minimum=1,
    )
    key_length = integer(
        field(redundancy, "referenced_key_length"),
        "referenced_key_length",
        minimum=1,
    )
    dependent_count = integer(
        field(redundancy, "dependent_property_count"),
        "dependent_property_count",
        minimum=1,
    )
    require(
        join_rows >= distinct_rows and dependent_count == column_count - key_length,
        f"{field(case, 'case_id')} has inconsistent final-layer redundancy counts",
    )
    return join_rows * dependent_count - distinct_rows * column_count


def parse_case(case: dict[str, Any]) -> CaseRow:
    case_id = field(case, "case_id")
    configuration = field(case, "configuration")
    trial_count = integer(
        field(configuration, "actual_update_trial_count"),
        f"{case_id}.actual_update_trial_count",
        minimum=1,
    )
    require(
        integer(
            field(configuration, "requested_update_trial_count"),
            f"{case_id}.requested_update_trial_count",
            minimum=1,
        )
        == trial_count
        and integer(
            field(configuration, "update_trial_count_shortfall"),
            f"{case_id}.update_trial_count_shortfall",
        )
        == 0,
        f"{case_id} did not execute its complete requested trial schedule",
    )
    measured_runs = integer(
        field(configuration, "measured_update_runs"),
        f"{case_id}.measured_update_runs",
        minimum=1,
    )
    slots = parse_trial_slots(case, trial_count)
    folded = parse_update_samples(
        case,
        "folded_update",
        slots,
        measured_runs,
        folded=True,
    )
    normalized = parse_update_samples(
        case,
        "normalized_update",
        slots,
        measured_runs,
        folded=False,
    )
    require(folded.keys() == normalized.keys(), f"{case_id} update phases are not paired")

    identities = sorted(folded, key=lambda identity: (identity[1], identity[0]))
    folded_values = [folded[identity] for identity in identities]
    normalized_values = [normalized[identity] for identity in identities]
    folded_median = statistics.median(folded_values)
    normalized_median = statistics.median(normalized_values)
    paired_update_delta_median = statistics.median(
        folded[identity] - normalized[identity] for identity in identities
    )
    paired_time_amplification_median = statistics.median(
        folded[identity] / normalized[identity] for identity in identities
    )
    normalization_median = parse_normalization_median(case)
    break_even = (
        math.ceil(normalization_median / paired_update_delta_median)
        if paired_update_delta_median > 0.0
        else None
    )

    folded_nodes, folded_relationships = shadow_counts(field(case, "folded_shadow_state"))
    normalized_nodes, normalized_relationships = shadow_counts(
        field(case, "normalized_shadow_state")
    )
    return CaseRow(
        fold_chain=fold_chain_label(case),
        normalized_update_median_ms=normalized_median,
        normalized_update_max_ms=max(normalized_values),
        normalized_update_min_ms=min(normalized_values),
        folded_update_median_ms=folded_median,
        paired_update_delta_median_ms=paired_update_delta_median,
        paired_time_amplification_median=paired_time_amplification_median,
        normalization_median_ms=normalization_median,
        break_even_updates=break_even,
        delta_nodes=folded_nodes - normalized_nodes,
        delta_relationships=folded_relationships - normalized_relationships,
        delta_property_cells=final_layer_property_cell_delta(case),
    )


def signed_integer(value: int) -> str:
    return "0" if value == 0 else f"{value:+,}"


def render(rows: list[CaseRow]) -> str:
    lines = [
        "### Calculation notes",
        "",
        (
            "- **Scope:** every metric evaluates only the **final Fold layer**. "
            "`Fold chain` shows the complete path, and its rightmost role is the "
            "layer being unfolded/normalized; any earlier Fold prefix remains folded."
        ),
        (
            "- **`U_N` and `U_D`:** median client wall time of all "
            "`T × R` single-key trial samples in the Normalized and Fold states, "
            "respectively. Every trial slot has equal weight."
        ),
        (
            "- The second table additionally reports the maximum, minimum, and "
            "median `U_N` sample values. Its remaining columns use exactly the "
            "same values and formulas as the first table."
        ),
        (
            "- **`Δ_U = median_i(U_D,i − U_N,i)`:** median paired update time "
            "saved by normalizing the final layer. A positive value means the "
            "Normalized state typically updates faster."
        ),
        (
            "- **`A_T = median_i(U_D,i / U_N,i)`:** median paired update-time "
            "amplification in the Fold state. `A_T > 1` means Fold is typically slower."
        ),
        (
            "- **`Norm.`:** median wall time of the one-time operation "
            "that peels/normalizes the final Fold layer; update, restore, and "
            "validation time is excluded."
        ),
        (
            "- **`κ = ceil(median(Norm.) / median_i(U_D,i − U_N,i))`:** robust "
            "estimate of the number of future single-key updates needed to recover "
            "the normalization cost. It is `N/A` when `Δ_U ≤ 0`."
        ),
        (
            "- **`ΔN`, `ΔE`, and `ΔC`:** `Fold − Normalized` differences in "
            "physical nodes, relationships, and property cells for the final "
            "layer. Thus a negative `ΔN` or `ΔE` means Fold uses fewer of them; "
            "a positive `ΔC` means Fold stores more property cells."
        ),
        "",
        (
            "| No. | Fold chain (final layer evaluated) | $U_N$ median (ms) | "
            "$U_D$ median (ms) | Paired $\\Delta_U$ median (ms) | Paired "
            "$A_T$ median | Final-layer Norm. median (ms) | $\\kappa$ | "
            "Final-layer $\\Delta N$ | "
            "Final-layer $\\Delta E$ | Final-layer $\\Delta C$ |"
        ),
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for index, row in enumerate(rows, start=1):
        lines.append(
            "| "
            + " | ".join(
                (
                    str(index),
                    row.fold_chain,
                    f"{row.normalized_update_median_ms:,.3f}",
                    f"{row.folded_update_median_ms:,.3f}",
                    f"{row.paired_update_delta_median_ms:+,.3f}",
                    f"{row.paired_time_amplification_median:,.3f}",
                    f"{row.normalization_median_ms:,.3f}",
                    f"{row.break_even_updates:,}" if row.break_even_updates is not None else "N/A",
                    signed_integer(row.delta_nodes),
                    signed_integer(row.delta_relationships),
                    signed_integer(row.delta_property_cells),
                )
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "### $U_N$ maximum, minimum, and median",
            "",
            (
                "| No. | Fold chain (final layer evaluated) | $U_N$ max (ms) | "
                "$U_N$ min (ms) | $U_N$ median (ms) | $U_D$ median (ms) | "
                "Paired $\\Delta_U$ median (ms) | Paired $A_T$ median | "
                "Final-layer Norm. median (ms) | $\\kappa$ | Final-layer "
                "$\\Delta N$ | Final-layer "
                "$\\Delta E$ | Final-layer $\\Delta C$ |"
            ),
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for index, row in enumerate(rows, start=1):
        lines.append(
            "| "
            + " | ".join(
                (
                    str(index),
                    row.fold_chain,
                    f"{row.normalized_update_max_ms:,.3f}",
                    f"{row.normalized_update_min_ms:,.3f}",
                    f"{row.normalized_update_median_ms:,.3f}",
                    f"{row.folded_update_median_ms:,.3f}",
                    f"{row.paired_update_delta_median_ms:+,.3f}",
                    f"{row.paired_time_amplification_median:,.3f}",
                    f"{row.normalization_median_ms:,.3f}",
                    f"{row.break_even_updates:,}" if row.break_even_updates is not None else "N/A",
                    signed_integer(row.delta_nodes),
                    signed_integer(row.delta_relationships),
                    signed_integer(row.delta_property_cells),
                )
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate final-Fold-layer Markdown comparison tables."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="input dangerous-report JSON")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="output Markdown path (default: input path with .md suffix)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = args.input.expanduser().resolve()
    output_path = (
        args.output.expanduser().resolve()
        if args.output is not None
        else input_path.with_suffix(".md")
    )
    try:
        require(input_path != output_path, "Input and output paths must differ")
        report = json.loads(input_path.read_text(encoding="utf-8"))
        require(isinstance(report, dict), "JSON root must be an object")
        cases = normalize_cases(report)
        fingerprints = {validate_case_scope(case) for case in cases}
        require(
            len(fingerprints) == 1,
            "All cases must use the same unchanged source-graph fingerprint",
        )
        markdown = render([parse_case(case) for case in cases])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown, encoding="utf-8")
    except (AnalysisError, json.JSONDecodeError, OSError) as exc:
        print(f"Analysis failed: {exc}", file=sys.stderr)
        return 1

    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
