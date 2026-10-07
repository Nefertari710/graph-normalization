#!/usr/bin/env python3
"""Generate a Markdown summary of the TPC-H FD-normalization experiment.

The source JSON is produced by ``redundancy_experiment.py``.  The report maps
the stored measurements directly to the four experimental steps:

1. functional-dependency redundancy removed by normalization;
2. update cost in the folded/denormalized representation;
3. one-off normalization cost; and
4. update cost in the normalized representation.

Run without arguments to read ``tpch_fd_experiment_results.json`` from
``../results/simple`` and write ``tpch_fd_experiment_results.md`` beside the
JSON file::

    python analysis.py

Alternative input and output paths can be supplied with ``--input`` and
``--output``.
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
    / "simple"
    / "tpch_fd_experiment_results.json"
)
SUPPORTED_REPORT_SCHEMA = 8


class AnalysisError(RuntimeError):
    """Raised when the source report is missing or internally inconsistent."""


@dataclass(frozen=True)
class TimingResult:
    """The primary timing value and its measured sample count."""

    median_ms: float
    n: int


@dataclass(frozen=True)
class CaseResult:
    """Step 1--4 headline measurements for one direct FD pair."""

    case_id: str
    referencing_table: str
    referenced_table: str
    update_property: str
    referencing_rows_t1: int
    referencing_columns_t1: int
    referenced_rows_t2: int
    referenced_columns_t2: int
    folded_join_columns: int
    join_rows_j: int
    participating_referenced_rows_d: int
    redundant_tuple_copies: int
    redundant_tuple_percentage: float
    redundant_property_slots: int
    redundant_non_null_values: int
    folded_property_writes: int
    normalized_property_writes: int
    folded_update: TimingResult
    normalization: TimingResult
    normalized_update: TimingResult
    update_time_saved_ms: float
    folded_to_normalized_ratio: float
    write_amplification: float
    break_even_batches: float | None

    @property
    def pair_label(self) -> str:
        return f"{self.referencing_table} → {self.referenced_table}"

    @property
    def referencing_cells_t1(self) -> int:
        return self.referencing_rows_t1 * self.referencing_columns_t1

    @property
    def referenced_cells_t2(self) -> int:
        return self.referenced_rows_t2 * self.referenced_columns_t2

    @property
    def folded_join_cells(self) -> int:
        return self.join_rows_j * self.folded_join_columns

    @property
    def join_to_original_cells_ratio(self) -> float:
        return self.folded_join_cells / (
            self.referencing_cells_t1 + self.referenced_cells_t2
        )


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AnalysisError(message)


def get_path(record: Any, *keys: str) -> Any:
    """Return a required nested value and name the missing path on failure."""

    value = record
    walked: list[str] = []
    for key in keys:
        walked.append(key)
        if not isinstance(value, dict) or key not in value:
            raise AnalysisError(f"Missing required field: {'.'.join(walked)}")
        value = value[key]
    return value


def as_string(value: Any, field_name: str) -> str:
    require(isinstance(value, str) and bool(value), f"{field_name} must be a string")
    return value


def as_int(value: Any, field_name: str) -> int:
    require(
        isinstance(value, int) and not isinstance(value, bool),
        f"{field_name} must be an integer",
    )
    return value


def as_float(value: Any, field_name: str) -> float:
    require(
        isinstance(value, (int, float)) and not isinstance(value, bool),
        f"{field_name} must be numeric",
    )
    result = float(value)
    require(math.isfinite(result), f"{field_name} must be finite")
    return result


def close_enough(actual: float, expected: float) -> bool:
    return math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9)


def parse_timing(case: dict[str, Any], phase: str) -> TimingResult:
    summary = get_path(case, "timings", phase, "summary", "client_wall_ms")
    samples = get_path(case, "timings", phase, "samples")
    require(isinstance(samples, list) and samples, f"{phase}.samples must be non-empty")

    n = as_int(get_path(summary, "n"), f"{phase}.summary.client_wall_ms.n")
    median_ms = as_float(
        get_path(summary, "median"),
        f"{phase}.summary.client_wall_ms.median",
    )
    sample_values = [
        as_float(
            get_path(sample, "client_wall_ms"),
            f"{phase}.samples[{index}].client_wall_ms",
        )
        for index, sample in enumerate(samples)
    ]
    require(n == len(sample_values), f"{phase}: summary n does not match samples")
    require(
        close_enough(median_ms, float(statistics.median(sample_values))),
        f"{phase}: stored median does not match raw samples",
    )
    return TimingResult(median_ms=median_ms, n=n)


def parse_case(record: Any) -> CaseResult:
    require(isinstance(record, dict), "Every cases entry must be an object")
    case_id = as_string(get_path(record, "case_id"), "case_id")
    pair = get_path(record, "pair")
    referencing_table = as_string(
        get_path(pair, "referencing_table", "label"),
        f"{case_id}.pair.referencing_table.label",
    )
    referenced_table = as_string(
        get_path(pair, "referenced_table", "label"),
        f"{case_id}.pair.referenced_table.label",
    )
    referencing_columns = get_path(pair, "referencing_table", "columns")
    referenced_columns = get_path(pair, "referenced_table", "columns")
    referenced_key = get_path(pair, "referenced_table", "key")
    folded_bindings = get_path(pair, "folded_property_namespace", "bindings")
    folded_referencing_bindings = get_path(folded_bindings, "referencing")
    folded_referenced_non_key_bindings = get_path(
        folded_bindings,
        "referenced_non_key",
    )
    require(
        isinstance(referencing_columns, list) and bool(referencing_columns),
        f"{case_id}: referencing columns must be a non-empty list",
    )
    require(
        isinstance(referenced_columns, list) and bool(referenced_columns),
        f"{case_id}: referenced columns must be a non-empty list",
    )
    require(
        isinstance(referenced_key, list) and bool(referenced_key),
        f"{case_id}: referenced key must be a non-empty list",
    )
    require(
        isinstance(folded_referencing_bindings, list)
        and isinstance(folded_referenced_non_key_bindings, list),
        f"{case_id}: folded property bindings must be lists",
    )
    referencing_columns_t1 = len(referencing_columns)
    referenced_columns_t2 = len(referenced_columns)
    folded_join_columns = len(folded_referencing_bindings) + len(
        folded_referenced_non_key_bindings
    )
    require(
        len(folded_referencing_bindings) == referencing_columns_t1,
        f"{case_id}: folded referencing bindings do not cover all t1 columns",
    )
    require(
        len(folded_referenced_non_key_bindings)
        == referenced_columns_t2 - len(referenced_key),
        f"{case_id}: folded t2 bindings do not match its non-key columns",
    )
    require(
        get_path(pair, "folded_property_namespace", "referenced_key_copied")
        is False,
        f"{case_id}: this analysis expects the referenced key not to be copied",
    )
    update_property = as_string(
        get_path(record, "configuration", "update_property"),
        f"{case_id}.configuration.update_property",
    )

    dataset = get_path(record, "dataset")
    referencing_rows_t1 = as_int(
        get_path(dataset, "referencing_rows"),
        f"{case_id}.dataset.referencing_rows",
    )
    referenced_rows_t2 = as_int(
        get_path(dataset, "full_referenced_rows"),
        f"{case_id}.dataset.full_referenced_rows",
    )
    dataset_join_rows = as_int(
        get_path(dataset, "join_rows"),
        f"{case_id}.dataset.join_rows",
    )

    redundancy = get_path(record, "redundancy")
    join_rows_j = as_int(
        get_path(redundancy, "join_rows_J"),
        f"{case_id}.redundancy.join_rows_J",
    )
    participating_d = as_int(
        get_path(redundancy, "participating_referenced_rows_D"),
        f"{case_id}.redundancy.participating_referenced_rows_D",
    )
    redundant_tuples = as_int(
        get_path(redundancy, "redundant_referenced_tuple_copies"),
        f"{case_id}.redundancy.redundant_referenced_tuple_copies",
    )
    redundant_percentage = as_float(
        get_path(
            redundancy,
            "redundant_referenced_tuple_percentage_of_join",
        ),
        f"{case_id}.redundancy.redundant_referenced_tuple_percentage_of_join",
    )
    redundant_slots = as_int(
        get_path(
            redundancy,
            "theoretical_redundant_dependent_property_slots",
        ),
        f"{case_id}.redundancy.theoretical_redundant_dependent_property_slots",
    )
    redundant_non_null_values = as_int(
        get_path(redundancy, "redundant_non_null_dependent_values"),
        f"{case_id}.redundancy.redundant_non_null_dependent_values",
    )
    require(join_rows_j > 0, f"{case_id}: J must be positive")
    require(
        referencing_rows_t1 > 0 and referenced_rows_t2 > 0,
        f"{case_id}: original table row counts must be positive",
    )
    require(
        dataset_join_rows == join_rows_j,
        f"{case_id}: dataset join_rows differs from redundancy J",
    )
    require(0 <= participating_d <= join_rows_j, f"{case_id}: D must be within [0, J]")
    require(
        redundant_tuples == join_rows_j - participating_d,
        f"{case_id}: redundant tuple copies must equal J-D",
    )
    expected_percentage = 100.0 * redundant_tuples / join_rows_j
    require(
        close_enough(redundant_percentage, expected_percentage),
        f"{case_id}: redundant tuple percentage is inconsistent with J-D",
    )
    require(
        min(redundant_slots, redundant_non_null_values) >= 0,
        f"{case_id}: redundancy measurements cannot be negative",
    )

    folded_update = parse_timing(record, "folded_update")
    normalization = parse_timing(record, "normalization")
    normalized_update = parse_timing(record, "normalized_update")
    require(
        get_path(record, "timings", "denormalized_update")
        == get_path(record, "timings", "folded_update"),
        f"{case_id}: denormalized_update alias differs from folded_update",
    )
    require(
        folded_update.median_ms > 0
        and normalization.median_ms > 0
        and normalized_update.median_ms > 0,
        f"{case_id}: timing medians must be positive",
    )

    folded_writes = as_int(
        get_path(
            record,
            "update_payload",
            "folded_join_fanout_weighted",
            "property_writes",
        ),
        f"{case_id}.update_payload.folded_join_fanout_weighted.property_writes",
    )
    normalized_writes = as_int(
        get_path(
            record,
            "update_payload",
            "normalized_logical",
            "property_writes",
        ),
        f"{case_id}.update_payload.normalized_logical.property_writes",
    )
    require(
        folded_writes > 0 and normalized_writes > 0,
        f"{case_id}: property-write counts must be positive",
    )

    derived = get_path(record, "derived")
    stored_ratio = as_float(
        get_path(derived, "folded_to_normalized_update_median_ratio"),
        f"{case_id}.derived.folded_to_normalized_update_median_ratio",
    )
    stored_saved_ms = as_float(
        get_path(derived, "median_client_ms_saved_per_logical_update_batch"),
        f"{case_id}.derived.median_client_ms_saved_per_logical_update_batch",
    )
    stored_write_amplification = as_float(
        get_path(derived, "write_amplification_nodes"),
        f"{case_id}.derived.write_amplification_nodes",
    )
    expected_ratio = folded_update.median_ms / normalized_update.median_ms
    expected_saved_ms = folded_update.median_ms - normalized_update.median_ms
    expected_write_amplification = folded_writes / normalized_writes
    require(
        close_enough(stored_ratio, expected_ratio),
        f"{case_id}: stored update-time ratio is inconsistent with medians",
    )
    require(
        close_enough(stored_saved_ms, expected_saved_ms),
        f"{case_id}: stored update-time saving is inconsistent with medians",
    )
    require(
        close_enough(stored_write_amplification, expected_write_amplification),
        f"{case_id}: stored write amplification is inconsistent with writes",
    )

    raw_break_even = get_path(
        derived,
        "data_rewrite_normalization_break_even_update_batches",
    )
    if raw_break_even is None:
        break_even_batches = None
    else:
        break_even_batches = as_float(
            raw_break_even,
            f"{case_id}.derived.data_rewrite_normalization_break_even_update_batches",
        )

    if redundant_tuples > 0 and expected_saved_ms > 0:
        expected_break_even = normalization.median_ms / expected_saved_ms
        require(
            break_even_batches is not None
            and close_enough(break_even_batches, expected_break_even),
            f"{case_id}: stored break-even is inconsistent with phase medians",
        )
    else:
        require(
            break_even_batches is None,
            f"{case_id}: break-even must be null without a positive update saving",
        )

    require(
        get_path(record, "cleanup", "original_graph_verified_unchanged") is True,
        f"{case_id}: source graph was not verified unchanged",
    )
    require(
        get_path(record, "final_state", "original_graph") == "unchanged",
        f"{case_id}: final original-graph state is not unchanged",
    )
    require(
        get_path(record, "final_state", "shadow_graph") == "removed",
        f"{case_id}: final shadow graph was not removed",
    )

    return CaseResult(
        case_id=case_id,
        referencing_table=referencing_table,
        referenced_table=referenced_table,
        update_property=update_property,
        referencing_rows_t1=referencing_rows_t1,
        referencing_columns_t1=referencing_columns_t1,
        referenced_rows_t2=referenced_rows_t2,
        referenced_columns_t2=referenced_columns_t2,
        folded_join_columns=folded_join_columns,
        join_rows_j=join_rows_j,
        participating_referenced_rows_d=participating_d,
        redundant_tuple_copies=redundant_tuples,
        redundant_tuple_percentage=redundant_percentage,
        redundant_property_slots=redundant_slots,
        redundant_non_null_values=redundant_non_null_values,
        folded_property_writes=folded_writes,
        normalized_property_writes=normalized_writes,
        folded_update=folded_update,
        normalization=normalization,
        normalized_update=normalized_update,
        update_time_saved_ms=stored_saved_ms,
        folded_to_normalized_ratio=stored_ratio,
        write_amplification=stored_write_amplification,
        break_even_batches=break_even_batches,
    )


def load_report(path: Path) -> tuple[dict[str, Any], list[CaseResult]]:
    try:
        raw_report = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise AnalysisError(f"Cannot read input JSON {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise AnalysisError(f"Invalid JSON in {path}: {exc}") from exc

    require(isinstance(raw_report, dict), "The JSON report root must be an object")
    report: dict[str, Any] = raw_report
    schema_version = as_int(
        get_path(report, "report_schema_version"),
        "report_schema_version",
    )
    require(
        schema_version == SUPPORTED_REPORT_SCHEMA,
        f"Unsupported report schema {schema_version}; expected {SUPPORTED_REPORT_SCHEMA}",
    )

    raw_cases = get_path(report, "cases")
    require(isinstance(raw_cases, list) and raw_cases, "cases must be a non-empty list")
    expected_case_count = as_int(get_path(report, "case_count"), "case_count")
    require(
        expected_case_count == len(raw_cases),
        "case_count does not match the number of cases entries",
    )

    cases_by_id: dict[str, dict[str, Any]] = {}
    for raw_case in raw_cases:
        require(isinstance(raw_case, dict), "Every cases entry must be an object")
        case_id = as_string(get_path(raw_case, "case_id"), "cases[].case_id")
        require(case_id not in cases_by_id, f"Duplicate case_id: {case_id}")
        cases_by_id[case_id] = raw_case

    raw_order = get_path(report, "case_order")
    require(isinstance(raw_order, list), "case_order must be a list")
    case_order = [as_string(value, "case_order[]") for value in raw_order]
    require(
        len(case_order) == expected_case_count and set(case_order) == set(cases_by_id),
        "case_order does not identify every case exactly once",
    )

    source_before = as_string(
        get_path(report, "source_graph_sha256_before"),
        "source_graph_sha256_before",
    )
    source_after = as_string(
        get_path(report, "source_graph_sha256_after"),
        "source_graph_sha256_after",
    )
    require(source_before == source_after, "Source graph fingerprint changed")

    return report, [parse_case(cases_by_id[case_id]) for case_id in case_order]


def escape_markdown(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def format_comparison(case: CaseResult) -> str:
    ratio = case.folded_to_normalized_ratio
    percentage_difference = (ratio - 1.0) * 100.0
    if percentage_difference > 1e-9:
        ratio_text = f"{ratio:.3f}× time ({percentage_difference:.1f}% longer)"
    elif percentage_difference < -1e-9:
        ratio_text = f"{ratio:.3f}× time ({abs(percentage_difference):.1f}% shorter)"
    else:
        ratio_text = "1.000× time (same median)"

    break_even_text = (
        "break-even: N/A"
        if case.break_even_batches is None
        else f"break-even: {case.break_even_batches:,.2f} update batches"
    )
    return "<br>".join(
        (
            f"folded − normalized: {case.update_time_saved_ms:+,.3f} ms/batch",
            ratio_text,
            f"write amplification: {case.write_amplification:,.3f}×",
            break_even_text,
        )
    )


def render_markdown(
    report: dict[str, Any],
    cases: list[CaseResult],
    input_path: Path,
) -> str:
    experiment = escape_markdown(
        as_string(get_path(report, "experiment"), "experiment")
    )
    database = escape_markdown(as_string(get_path(report, "database"), "database"))
    topology_mode = escape_markdown(
        as_string(get_path(report, "topology_mode"), "topology_mode")
    )

    lines = [
        "# TPC-H Functional-Dependency Normalization Experiment",
        "",
        f"- Source report: `{escape_markdown(input_path.name)}`",
        f"- Experiment: `{experiment}`",
        f"- Database: `{database}`",
        f"- Topology mode: `{topology_mode}`",
        f"- FD strategies: {len(cases)}",
        "- Primary timing metric: median client wall-clock time in milliseconds.",
        "",
        "## Step 1–4 results",
        "",
        (
            "| FD strategy | Updated property | Step 1 — Redundancy saved | "
            "Step 2 — Folded update | Step 3 — Normalization | "
            "Step 4 — Normalized update | Step 2 vs Step 4 |"
        ),
        "|---|---|---:|---:|---:|---:|---:|",
    ]

    for case in cases:
        redundancy_cell = "<br>".join(
            (
                (
                    f"t1 ({case.referencing_table}): {case.referencing_rows_t1:,} "
                    f"rows × {case.referencing_columns_t1} columns = "
                    f"{case.referencing_cells_t1:,} cells"
                ),
                (
                    f"t2 ({case.referenced_table}): {case.referenced_rows_t2:,} "
                    f"rows × {case.referenced_columns_t2} columns = "
                    f"{case.referenced_cells_t2:,} cells"
                ),
                (
                    f"folded join: {case.join_rows_j:,} rows × "
                    f"{case.folded_join_columns} columns = "
                    f"{case.folded_join_cells:,} cells"
                ),
                (
                    f"join cells/(t1+t2 cells)="
                    f"{case.join_to_original_cells_ratio:.4f} "
                    f"({case.join_to_original_cells_ratio * 100:.2f}%)"
                ),
                f"J={case.join_rows_j:,}; D={case.participating_referenced_rows_d:,}",
                (
                    f"{case.redundant_tuple_copies:,} redundant tuple copies "
                    f"({case.redundant_tuple_percentage:.2f}%)"
                ),
                f"{case.redundant_property_slots:,} dependent-property slots",
                f"{case.redundant_non_null_values:,} redundant non-NULL values",
            )
        )
        folded_cell = "<br>".join(
            (
                f"{case.folded_update.median_ms:,.3f} ms (n={case.folded_update.n})",
                f"{case.folded_property_writes:,} property writes",
            )
        )
        normalization_cell = (
            f"{case.normalization.median_ms:,.3f} ms (n={case.normalization.n})"
        )
        normalized_cell = "<br>".join(
            (
                (
                    f"{case.normalized_update.median_ms:,.3f} ms "
                    f"(n={case.normalized_update.n})"
                ),
                f"{case.normalized_property_writes:,} property writes",
            )
        )
        updated_property = escape_markdown(
            f"{case.referenced_table}.{case.update_property}"
        )
        lines.append(
            "| "
            + " | ".join(
                (
                    escape_markdown(case.pair_label),
                    f"`{updated_property}`",
                    redundancy_cell,
                    folded_cell,
                    normalization_cell,
                    normalized_cell,
                    format_comparison(case),
                )
            )
            + " |"
        )

    measured_counts = sorted(
        {
            timing.n
            for case in cases
            for timing in (
                case.folded_update,
                case.normalization,
                case.normalized_update,
            )
        }
    )
    count_text = ", ".join(str(value) for value in measured_counts)
    lines.extend(
        [
            "",
            "## Measurement notes",
            "",
            (
                "- `J` is the folded join-tuple count and `D` is the number of "
                "distinct referenced keys participating in that join. The tuple "
                "redundancy removed by normalization is `J − D`."
            ),
            (
                "- Table size is approximated as logical cells (`rows × columns`). "
                "The folded join contains every t1 column plus the non-key t2 "
                "columns; the t2 key is not copied because the t1 foreign key "
                "already represents it. `join cells/(t1+t2 cells)` is still a "
                "logical-size ratio, not Neo4j physical storage."
            ),
            (
                "- Step 2 measures the folded/denormalized **shadow** projection "
                '(the "original" layout). The actual source graph remained read-only, '
                "was fingerprint-verified unchanged, and all shadow data was removed."
            ),
            (
                "- Step 2 and Step 4 exclude restoration and validation. Step 3 "
                "includes the batched data decomposition and commits, but excludes "
                "DDL, validation, refolding, and cleanup."
            ),
            (
                "- Break-even batches are `normalization median / "
                "(folded-update median − normalized-update median)`."
            ),
            f"- Measured sample count(s) present in this report: n={count_text}.",
        ]
    )
    if measured_counts == [1]:
        lines.append(
            "- **Caution:** every median is currently based on one measured run; "
            "the report therefore provides no evidence about timing variability "
            "or statistical stability."
        )

    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read a TPC-H FD experiment JSON report and write a Markdown table "
            "covering redundancy, normalization cost, and update cost."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="input experiment JSON",
    )
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
        report, cases = load_report(input_path)
        markdown = render_markdown(report, cases, input_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown, encoding="utf-8")
    except AnalysisError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"ERROR: cannot write {output_path}: {exc}", file=sys.stderr)
        return 1

    print(f"Markdown report written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
