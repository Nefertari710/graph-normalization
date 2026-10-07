#!/usr/bin/env python3
"""Generate a Markdown summary of direct and recursive TPC-H FD experiments.

The source JSON is produced by ``redundancy_experiment_extend.py``.  It mixes
direct-pair cases with recursive fold chains.  This report maps both case
families to the same four experimental steps:

1. functional-dependency redundancy removed by normalization;
2. update cost in the folded/denormalized representation;
3. one-off normalization cost; and
4. update cost in the normalized representation.

Run without arguments to read ``tpch_fd_experiment_extend_results.json`` from
``../results/extend`` and write ``tpch_fd_experiment_extend_results.md`` beside
the JSON file::

    python analysis_extend.py

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
    / "extend"
    / "tpch_fd_experiment_extend_results.json"
)
SUPPORTED_REPORT_SCHEMA = 8
SUPPORTED_RECURSIVE_REPORT_SCHEMA = 3
SUPPORTED_RECURSIVE_FOLD_SCHEMA = 4
DIRECT_FAMILY = "tpch_direct_pair_fd"
RECURSIVE_FAMILY = "tpch_recursive_fold_fd"
TABLE_ABBREVIATIONS = {
    "LINEITEM": "L",
    "ORDERS": "O",
    "CUSTOMER": "C",
    "PARTSUPP": "PS",
    "PART": "P",
    "SUPPLIER": "S",
    "NATION": "N",
    "REGION": "R",
}


class AnalysisError(RuntimeError):
    """Raised when the source report is missing or internally inconsistent."""


@dataclass(frozen=True)
class TimingResult:
    """The primary timing value and its measured sample count."""

    median_ms: float
    n: int


@dataclass(frozen=True)
class CaseShape:
    """Family-specific relation shape mapped to a final two-way fold."""

    experiment_family: str
    strategy_label: str
    t1_label: str
    t2_label: str
    update_target_label: str
    fold_depth: int
    t1_rows: int
    t1_columns: int
    t2_rows: int
    t2_columns: int
    folded_join_columns: int
    dataset_join_rows: int
    dataset_participating_rows: int


@dataclass(frozen=True)
class CaseResult:
    """Step 1--4 headline measurements for one direct or recursive FD case."""

    case_id: str
    experiment_family: str
    strategy_label: str
    t1_label: str
    t2_label: str
    update_target_label: str
    fold_depth: int
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
    def family_label(self) -> str:
        if self.experiment_family == DIRECT_FAMILY:
            return "Direct pair"
        return f"Recursive ({self.fold_depth} folds)"

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


def parse_direct_shape(record: dict[str, Any], case_id: str) -> CaseShape:
    case_schema = as_int(
        get_path(record, "report_schema_version"),
        f"{case_id}.report_schema_version",
    )
    require(
        case_schema == SUPPORTED_REPORT_SCHEMA,
        f"{case_id}: direct case schema must be {SUPPORTED_REPORT_SCHEMA}",
    )

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
    t1_columns = len(referencing_columns)
    t2_columns = len(referenced_columns)
    folded_join_columns = len(folded_referencing_bindings) + len(
        folded_referenced_non_key_bindings
    )
    require(
        len(folded_referencing_bindings) == t1_columns,
        f"{case_id}: folded referencing bindings do not cover all t1 columns",
    )
    require(
        len(folded_referenced_non_key_bindings)
        == t2_columns - len(referenced_key),
        f"{case_id}: folded t2 bindings do not match its non-key columns",
    )
    require(
        get_path(pair, "folded_property_namespace", "referenced_key_copied")
        is False,
        f"{case_id}: this analysis expects the referenced key not to be copied",
    )

    dataset = get_path(record, "dataset")
    return CaseShape(
        experiment_family=DIRECT_FAMILY,
        strategy_label=f"{referencing_table} → {referenced_table}",
        t1_label=referencing_table,
        t2_label=referenced_table,
        update_target_label=referenced_table,
        fold_depth=1,
        t1_rows=as_int(
            get_path(dataset, "referencing_rows"),
            f"{case_id}.dataset.referencing_rows",
        ),
        t1_columns=t1_columns,
        t2_rows=as_int(
            get_path(dataset, "full_referenced_rows"),
            f"{case_id}.dataset.full_referenced_rows",
        ),
        t2_columns=t2_columns,
        folded_join_columns=folded_join_columns,
        dataset_join_rows=as_int(
            get_path(dataset, "join_rows"),
            f"{case_id}.dataset.join_rows",
        ),
        dataset_participating_rows=as_int(
            get_path(dataset, "participating_referenced_rows"),
            f"{case_id}.dataset.participating_referenced_rows",
        ),
    )


def parse_recursive_shape(record: dict[str, Any], case_id: str) -> CaseShape:
    case_schema = as_int(
        get_path(record, "report_schema_version"),
        f"{case_id}.report_schema_version",
    )
    fold_schema = as_int(
        get_path(record, "recursive_fold_schema_version"),
        f"{case_id}.recursive_fold_schema_version",
    )
    require(
        case_schema == SUPPORTED_RECURSIVE_REPORT_SCHEMA,
        (
            f"{case_id}: recursive case schema must be "
            f"{SUPPORTED_RECURSIVE_REPORT_SCHEMA}"
        ),
    )
    require(
        fold_schema == SUPPORTED_RECURSIVE_FOLD_SCHEMA,
        (
            f"{case_id}: recursive fold schema must be "
            f"{SUPPORTED_RECURSIVE_FOLD_SCHEMA}"
        ),
    )

    fold_chain = get_path(record, "fold_chain")
    require(
        as_int(
            get_path(fold_chain, "recursive_fold_schema_version"),
            f"{case_id}.fold_chain.recursive_fold_schema_version",
        )
        == SUPPORTED_RECURSIVE_FOLD_SCHEMA,
        f"{case_id}: fold_chain schema differs from the case schema",
    )
    roles = get_path(fold_chain, "roles")
    role_order = get_path(fold_chain, "role_order")
    physical_table_order = get_path(fold_chain, "physical_table_order")
    require(
        isinstance(roles, list) and len(roles) >= 3,
        f"{case_id}: recursive roles must contain at least three entries",
    )
    require(
        isinstance(role_order, list) and len(role_order) == len(roles),
        f"{case_id}: role_order does not match roles",
    )
    require(
        isinstance(physical_table_order, list)
        and len(physical_table_order) == len(roles),
        f"{case_id}: physical_table_order does not match roles",
    )

    role_labels: list[str] = []
    folded_names_by_role: list[set[str]] = []
    role_columns: list[list[Any]] = []
    role_keys: list[list[Any]] = []
    role_mappings: list[dict[str, Any]] = []
    role_symbols: list[str] = []
    role_tables: list[str] = []
    for index, role in enumerate(roles):
        require(isinstance(role, dict), f"{case_id}: roles[{index}] must be an object")
        symbol = as_string(
            get_path(role, "role_symbol"),
            f"{case_id}.fold_chain.roles[{index}].role_symbol",
        )
        table = as_string(
            get_path(role, "table"),
            f"{case_id}.fold_chain.roles[{index}].table",
        )
        columns = get_path(role, "columns")
        key = get_path(role, "key")
        mapping = get_path(role, "logical_to_folded_property")
        require(
            isinstance(columns, list) and bool(columns),
            f"{case_id}: roles[{index}].columns must be non-empty",
        )
        require(
            isinstance(key, list) and bool(key),
            f"{case_id}: roles[{index}].key must be non-empty",
        )
        require(
            isinstance(mapping, dict) and set(mapping) == set(columns),
            f"{case_id}: roles[{index}] mapping must cover every logical column",
        )
        folded_names = {
            as_string(
                value,
                f"{case_id}.fold_chain.roles[{index}].logical_to_folded_property",
            )
            for value in mapping.values()
        }
        require(
            len(folded_names) == len(columns),
            f"{case_id}: roles[{index}] maps two columns to one local property",
        )
        role_symbols.append(symbol)
        role_tables.append(table)
        role_labels.append(f"{symbol}:{table}")
        role_columns.append(columns)
        role_keys.append(key)
        role_mappings.append(mapping)
        folded_names_by_role.append(folded_names)

    require(role_symbols == role_order, f"{case_id}: role_order is inconsistent")
    require(
        role_tables == physical_table_order,
        f"{case_id}: physical_table_order is inconsistent",
    )
    final_role_symbol = as_string(
        get_path(fold_chain, "normalization", "final_role"),
        f"{case_id}.fold_chain.normalization.final_role",
    )
    final_table = as_string(
        get_path(fold_chain, "normalization", "final_table"),
        f"{case_id}.fold_chain.normalization.final_table",
    )
    require(
        role_symbols[-1] == final_role_symbol and role_tables[-1] == final_table,
        f"{case_id}: normalization target is not the final declared role",
    )

    prefix_folded_names = set().union(*folded_names_by_role[:-1])
    all_folded_names = prefix_folded_names | folded_names_by_role[-1]
    final_key_folded_names = {
        as_string(
            role_mappings[-1][logical_key],
            f"{case_id}.final_role.key_mapping",
        )
        for logical_key in role_keys[-1]
    }
    require(
        final_key_folded_names <= prefix_folded_names,
        f"{case_id}: final key is not represented by the folded prefix FK",
    )
    require(
        len(all_folded_names)
        == len(prefix_folded_names) + len(role_columns[-1]) - len(role_keys[-1]),
        f"{case_id}: final folded column count is inconsistent",
    )

    declared_stages = get_path(fold_chain, "stages")
    stage_builds = get_path(record, "stage_builds")
    expected_stages = len(roles) - 1
    require(
        isinstance(declared_stages, list) and len(declared_stages) == expected_stages,
        f"{case_id}: declared stage count does not match role depth",
    )
    require(
        isinstance(stage_builds, list) and len(stage_builds) == expected_stages,
        f"{case_id}: stage_build count does not match role depth",
    )

    dataset = get_path(record, "dataset")
    grain_rows = as_int(
        get_path(dataset, "grain_rows"),
        f"{case_id}.dataset.grain_rows",
    )
    join_rows = as_int(
        get_path(dataset, "join_rows"),
        f"{case_id}.dataset.join_rows",
    )
    require(grain_rows == join_rows, f"{case_id}: grain rows differ from join rows")
    for index, stage_build in enumerate(stage_builds):
        require(
            as_int(
                get_path(stage_build, "created_nodes"),
                f"{case_id}.stage_builds[{index}].created_nodes",
            )
            == join_rows,
            f"{case_id}: stage {index + 1} cardinality differs from final J",
        )

    return CaseShape(
        experiment_family=RECURSIVE_FAMILY,
        strategy_label=" → ".join(role_labels),
        t1_label="folded prefix (" + " → ".join(role_labels[:-1]) + ")",
        t2_label=role_labels[-1],
        update_target_label=role_labels[-1],
        fold_depth=expected_stages,
        t1_rows=grain_rows,
        t1_columns=len(prefix_folded_names),
        t2_rows=as_int(
            get_path(dataset, "full_final_relation_rows"),
            f"{case_id}.dataset.full_final_relation_rows",
        ),
        t2_columns=len(role_columns[-1]),
        folded_join_columns=len(all_folded_names),
        dataset_join_rows=join_rows,
        dataset_participating_rows=as_int(
            get_path(dataset, "participating_final_rows"),
            f"{case_id}.dataset.participating_final_rows",
        ),
    )


def parse_case_shape(record: dict[str, Any], case_id: str) -> CaseShape:
    family = as_string(
        get_path(record, "experiment_family"),
        f"{case_id}.experiment_family",
    )
    if family == DIRECT_FAMILY:
        return parse_direct_shape(record, case_id)
    if family == RECURSIVE_FAMILY:
        return parse_recursive_shape(record, case_id)
    raise AnalysisError(f"{case_id}: unsupported experiment_family {family!r}")


def parse_case(record: Any) -> CaseResult:
    require(isinstance(record, dict), "Every cases entry must be an object")
    case_id = as_string(get_path(record, "case_id"), "case_id")
    shape = parse_case_shape(record, case_id)
    update_property = as_string(
        get_path(record, "configuration", "update_property"),
        f"{case_id}.configuration.update_property",
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
        shape.t1_rows > 0 and shape.t2_rows > 0,
        f"{case_id}: original table row counts must be positive",
    )
    require(
        shape.dataset_join_rows == join_rows_j,
        f"{case_id}: dataset join_rows differs from redundancy J",
    )
    require(
        shape.dataset_participating_rows == participating_d,
        f"{case_id}: dataset participating rows differ from redundancy D",
    )
    require(
        as_int(
            get_path(redundancy, "full_referenced_rows"),
            f"{case_id}.redundancy.full_referenced_rows",
        )
        == shape.t2_rows,
        f"{case_id}: redundancy full referenced rows differ from t2",
    )
    require(
        as_int(
            get_path(redundancy, "referenced_column_count"),
            f"{case_id}.redundancy.referenced_column_count",
        )
        == shape.t2_columns,
        f"{case_id}: redundancy referenced column count differs from t2",
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
        experiment_family=shape.experiment_family,
        strategy_label=shape.strategy_label,
        t1_label=shape.t1_label,
        t2_label=shape.t2_label,
        update_target_label=shape.update_target_label,
        fold_depth=shape.fold_depth,
        update_property=update_property,
        referencing_rows_t1=shape.t1_rows,
        referencing_columns_t1=shape.t1_columns,
        referenced_rows_t2=shape.t2_rows,
        referenced_columns_t2=shape.t2_columns,
        folded_join_columns=shape.folded_join_columns,
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
    family_counts = {DIRECT_FAMILY: 0, RECURSIVE_FAMILY: 0}
    for raw_case in raw_cases:
        require(isinstance(raw_case, dict), "Every cases entry must be an object")
        case_id = as_string(get_path(raw_case, "case_id"), "cases[].case_id")
        require(case_id not in cases_by_id, f"Duplicate case_id: {case_id}")
        family = as_string(
            get_path(raw_case, "experiment_family"),
            f"{case_id}.experiment_family",
        )
        require(family in family_counts, f"{case_id}: unsupported case family")
        family_counts[family] += 1
        cases_by_id[case_id] = raw_case

    # Mixed-suite reports declare both family counts; direct-only suites omit
    # them, so infer the counts from the already validated case entries.
    expected_direct_count = (
        as_int(report["direct_pair_case_count"], "direct_pair_case_count")
        if "direct_pair_case_count" in report
        else family_counts[DIRECT_FAMILY]
    )
    expected_recursive_count = (
        as_int(report["recursive_case_count"], "recursive_case_count")
        if "recursive_case_count" in report
        else family_counts[RECURSIVE_FAMILY]
    )
    require(
        expected_direct_count + expected_recursive_count == expected_case_count,
        "direct and recursive case counts do not sum to case_count",
    )
    require(
        family_counts[DIRECT_FAMILY] == expected_direct_count,
        "direct_pair_case_count does not match actual cases",
    )
    require(
        family_counts[RECURSIVE_FAMILY] == expected_recursive_count,
        "recursive_case_count does not match actual cases",
    )
    if expected_recursive_count:
        require(
            as_int(
                get_path(report, "recursive_report_schema_version"),
                "recursive_report_schema_version",
            )
            == SUPPORTED_RECURSIVE_REPORT_SCHEMA,
            "Unsupported recursive report schema",
        )
        require(
            as_int(
                get_path(report, "recursive_fold_schema_version"),
                "recursive_fold_schema_version",
            )
            == SUPPORTED_RECURSIVE_FOLD_SCHEMA,
            "Unsupported recursive fold schema",
        )

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


def abbreviate_fd_strategy(case: CaseResult) -> str:
    """Return the compact role path used by the simplified table."""

    if case.experiment_family == DIRECT_FAMILY:
        labels = (case.t1_label, case.t2_label)
        try:
            return "-".join(TABLE_ABBREVIATIONS[label] for label in labels)
        except KeyError as exc:
            raise AnalysisError(
                f"{case.case_id}: no table abbreviation is defined for "
                f"{exc.args[0]!r}"
            ) from exc

    role_symbols: list[str] = []
    for role_label in case.strategy_label.split(" → "):
        role_symbol, separator, _table = role_label.partition(":")
        require(
            bool(separator) and bool(role_symbol),
            f"{case.case_id}: recursive strategy role has no abbreviation",
        )
        role_symbols.append(role_symbol)
    return "-".join(role_symbols)


def format_topology_mode(value: Any) -> str:
    if isinstance(value, str) and value:
        return escape_markdown(value)
    require(isinstance(value, dict) and bool(value), "topology_mode must be an object")
    parts: list[str] = []
    for name, mode in value.items():
        parts.append(
            f"{escape_markdown(as_string(name, 'topology_mode key'))}="
            f"{escape_markdown(as_string(mode, f'topology_mode.{name}'))}"
        )
    return "; ".join(parts)


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
    topology_mode = format_topology_mode(get_path(report, "topology_mode"))
    direct_count = sum(case.experiment_family == DIRECT_FAMILY for case in cases)
    recursive_count = sum(case.experiment_family == RECURSIVE_FAMILY for case in cases)

    lines = [
        "# TPC-H Direct and Recursive FD-Normalization Experiment",
        "",
        f"- Source report: `{escape_markdown(input_path.name)}`",
        f"- Experiment: `{experiment}`",
        f"- Database: `{database}`",
        f"- Topology mode: `{topology_mode}`",
        (
            f"- FD strategies: {len(cases)} "
            f"({direct_count} direct pairs; {recursive_count} recursive folds)"
        ),
        "- Primary timing metric: median client wall-clock time in milliseconds.",
        "",
        "## Step 1–4 results",
        "",
        (
            "| Case type | FD strategy | Updated property | "
            "Step 1 — Redundancy saved | "
            "Step 2 — Folded update | Step 3 — Normalization | "
            "Step 4 — Normalized update | Step 2 vs Step 4 |"
        ),
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]

    for case in cases:
        redundancy_cell = "<br>".join(
            (
                (
                    f"t1 ({case.t1_label}): {case.referencing_rows_t1:,} "
                    f"rows × {case.referencing_columns_t1} columns = "
                    f"{case.referencing_cells_t1:,} cells"
                ),
                (
                    f"t2 ({case.t2_label}): {case.referenced_rows_t2:,} "
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
            f"{case.update_target_label}.{case.update_property}"
        )
        lines.append(
            "| "
            + " | ".join(
                (
                    case.family_label,
                    escape_markdown(case.strategy_label),
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
                "For direct cases, t1 is the referencing table. For recursive "
                "cases, t1 is the already-folded prefix before the final role is "
                "added. The folded join contains every physical t1 property plus "
                "the non-key t2 properties; the t2 key is already represented by "
                "the t1 foreign key. The cell ratio is not Neo4j physical storage."
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
                "- In recursive cases, Step 3 peels only the final fold layer; "
                "the earlier roles remain together as the normalized prefix."
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

    lines.extend(
        [
            "",
            "## Simplified comparison",
            "",
            (
                "| FD strategy | Ratio | "
                "Folded update time | Normalization time | Normalized update time | "
                "Folded update time / Normalized update time | Break-even |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for case in cases:
        break_even_text = (
            "N/A"
            if case.break_even_batches is None
            else f"{case.break_even_batches:,.1f} batches"
        )
        lines.append(
            "| "
            + " | ".join(
                (
                    abbreviate_fd_strategy(case),
                    f"{case.join_to_original_cells_ratio:.2f}",
                    f"{case.folded_update.median_ms:,.1f} ms",
                    f"{case.normalization.median_ms:,.1f} ms",
                    f"{case.normalized_update.median_ms:,.1f} ms",
                    f"{case.folded_to_normalized_ratio:.2f}×",
                    break_even_text,
                )
            )
            + " |"
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
