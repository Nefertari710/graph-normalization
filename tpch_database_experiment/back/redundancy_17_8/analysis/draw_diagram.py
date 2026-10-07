#!/usr/bin/env python3
"""Draw a replacement-relative cell difference against fold-chain length.

The plot is derived only from ``tpch_fd_experiment_extend_results.json``:

* x = folded cells minus cells in the distinct physical source relations;
* y = the number of operational fold occurrences in the chain;
* right y = folded-update time / predecessor-update time;
* folded cells = final join rows * distinct folded property names;
* original cells = the sum of rows * columns for each distinct physical table
  used by the chain; and
* net cell difference = folded cells - original cells.

The table cardinalities needed by recursive cases are learned from the direct
pair cases in the same JSON report.  No Markdown report or analysis module is
read.  By default the figure is written beside the source JSON file::

    python draw_diagram.py

Use ``--input`` and ``--output`` to override either path.  The output suffix
may be ``.png``, ``.svg``, or ``.pdf``.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_cache_root = Path(tempfile.gettempdir()) / "tpch_redundancy_diagram_cache"
_cache_root.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_cache_root / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(_cache_root))

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
except ImportError as exc:  # pragma: no cover - depends on the Python environment
    raise SystemExit(
        "Matplotlib is required to draw the figure. Run this script in the "
        "graph_database_denormalization Conda environment."
    ) from exc


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (
    SCRIPT_DIR.parent
    / "results"
    / "extend"
    / "tpch_fd_experiment_extend_results.json"
)
DEFAULT_OUTPUT_NAME = "folds_times_vs_redundant_values.png"
DIRECT_FAMILY = "tpch_direct_pair_fd"
RECURSIVE_FAMILY = "tpch_recursive_fold_fd"
SUPPORTED_OUTPUT_SUFFIXES = {".png", ".svg", ".pdf"}


class DiagramError(RuntimeError):
    """Raised when the JSON cannot support the requested diagram."""


@dataclass(frozen=True)
class TableShape:
    rows: int
    columns: int

    @property
    def cells(self) -> int:
        return self.rows * self.columns


@dataclass(frozen=True)
class PlotPoint:
    case_id: str
    fold_chain_length: int
    folded_join_cells: int
    original_table_cells: int
    folded_to_predecessor_update_ratio: float

    @property
    def net_cell_difference(self) -> int:
        return self.folded_join_cells - self.original_table_cells


def require(condition: bool, message: str) -> None:
    if not condition:
        raise DiagramError(message)


def as_object(value: Any, context: str) -> dict[str, Any]:
    require(isinstance(value, dict), f"{context} must be an object")
    return value


def as_list(value: Any, context: str) -> list[Any]:
    require(isinstance(value, list), f"{context} must be a list")
    return value


def as_string(value: Any, context: str) -> str:
    require(isinstance(value, str) and bool(value), f"{context} must be a string")
    return value


def as_nonnegative_int(value: Any, context: str) -> int:
    require(
        isinstance(value, int) and not isinstance(value, bool) and value >= 0,
        f"{context} must be a non-negative integer",
    )
    return value


def as_positive_float(value: Any, context: str) -> float:
    require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0,
        f"{context} must be a finite positive number",
    )
    return float(value)


def required(record: dict[str, Any], key: str, context: str) -> Any:
    require(key in record, f"{context}.{key} is missing")
    return record[key]


def ordered_cases(report: dict[str, Any]) -> list[dict[str, Any]]:
    raw_cases = required(report, "cases", "report")
    cases_by_id: dict[str, dict[str, Any]] = {}

    if isinstance(raw_cases, list):
        case_values = raw_cases
    elif isinstance(raw_cases, dict):
        case_values = list(raw_cases.values())
    else:
        raise DiagramError("report.cases must be a list or object")

    for index, raw_case in enumerate(case_values):
        case = as_object(raw_case, f"report.cases[{index}]")
        case_id = as_string(
            required(case, "case_id", f"report.cases[{index}]"),
            f"report.cases[{index}].case_id",
        )
        require(case_id not in cases_by_id, f"duplicate case_id: {case_id}")
        cases_by_id[case_id] = case

    raw_order = required(report, "case_order", "report")
    order = [
        as_string(value, f"report.case_order[{index}]")
        for index, value in enumerate(as_list(raw_order, "report.case_order"))
    ]
    require(len(order) == len(set(order)), "report.case_order contains duplicates")
    require(
        set(order) == set(cases_by_id),
        "report.case_order must identify every case exactly once",
    )

    declared_count = report.get("case_count")
    if declared_count is not None:
        require(
            as_nonnegative_int(declared_count, "report.case_count") == len(order),
            "report.case_count does not match report.cases",
        )
    return [cases_by_id[case_id] for case_id in order]


def read_cases(input_path: Path) -> list[dict[str, Any]]:
    try:
        report = json.loads(input_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise DiagramError(f"input JSON does not exist: {input_path}") from exc
    except json.JSONDecodeError as exc:
        raise DiagramError(f"input is not valid JSON: {exc}") from exc
    except OSError as exc:
        raise DiagramError(f"cannot read {input_path}: {exc}") from exc

    return ordered_cases(as_object(report, "report"))


def register_table(
    table_shapes: dict[str, TableShape],
    *,
    label: str,
    rows: int,
    columns: int,
    context: str,
) -> None:
    shape = TableShape(rows=rows, columns=columns)
    previous = table_shapes.get(label)
    require(
        previous is None or previous == shape,
        f"{context}: inconsistent row/column counts for {label}",
    )
    table_shapes[label] = shape


def build_table_shapes(cases: list[dict[str, Any]]) -> dict[str, TableShape]:
    """Learn base-table sizes from direct cases in this JSON report."""

    table_shapes: dict[str, TableShape] = {}
    for case in cases:
        family = as_string(
            required(case, "experiment_family", "case"),
            "case.experiment_family",
        )
        if family != DIRECT_FAMILY:
            continue

        case_id = as_string(required(case, "case_id", "case"), "case.case_id")
        pair = as_object(required(case, "pair", case_id), f"{case_id}.pair")
        dataset = as_object(
            required(case, "dataset", case_id), f"{case_id}.dataset"
        )
        for table_key, row_key in (
            ("referencing_table", "referencing_rows"),
            ("referenced_table", "full_referenced_rows"),
        ):
            table = as_object(
                required(pair, table_key, f"{case_id}.pair"),
                f"{case_id}.pair.{table_key}",
            )
            columns = as_list(
                required(table, "columns", f"{case_id}.pair.{table_key}"),
                f"{case_id}.pair.{table_key}.columns",
            )
            require(bool(columns), f"{case_id}.{table_key}.columns is empty")
            register_table(
                table_shapes,
                label=as_string(
                    required(table, "label", f"{case_id}.pair.{table_key}"),
                    f"{case_id}.pair.{table_key}.label",
                ),
                rows=as_nonnegative_int(
                    required(dataset, row_key, f"{case_id}.dataset"),
                    f"{case_id}.dataset.{row_key}",
                ),
                columns=len(columns),
                context=case_id,
            )

    require(bool(table_shapes), "the JSON has no direct cases for table sizes")
    return table_shapes


def folded_names_from_direct(pair: dict[str, Any], case_id: str) -> set[str]:
    namespace = as_object(
        required(pair, "folded_property_namespace", f"{case_id}.pair"),
        f"{case_id}.pair.folded_property_namespace",
    )
    bindings = as_object(
        required(namespace, "bindings", f"{case_id}.pair.folded_property_namespace"),
        f"{case_id}.pair.folded_property_namespace.bindings",
    )
    names: list[str] = []
    for group_name in ("referencing", "referenced_non_key"):
        group = as_list(
            required(bindings, group_name, f"{case_id}.bindings"),
            f"{case_id}.bindings.{group_name}",
        )
        for index, raw_binding in enumerate(group):
            binding = as_object(
                raw_binding, f"{case_id}.bindings.{group_name}[{index}]"
            )
            names.append(
                as_string(
                    required(
                        binding,
                        "folded_name",
                        f"{case_id}.bindings.{group_name}[{index}]",
                    ),
                    f"{case_id}.bindings.{group_name}[{index}].folded_name",
                )
            )
    require(bool(names), f"{case_id}: no folded property bindings")
    require(len(names) == len(set(names)), f"{case_id}: duplicate folded property")
    return set(names)


def update_time_ratio(case: dict[str, Any], case_id: str) -> float:
    """Read folded/predecessor client-time ratio from its legacy JSON field."""

    derived = as_object(required(case, "derived", case_id), f"{case_id}.derived")
    field = "folded_to_normalized_update_median_ratio"
    return as_positive_float(
        required(derived, field, f"{case_id}.derived"),
        f"{case_id}.derived.{field}",
    )


def direct_point(
    case: dict[str, Any],
    table_shapes: dict[str, TableShape],
    folded_to_predecessor_update_ratio: float,
) -> PlotPoint:
    case_id = as_string(required(case, "case_id", "case"), "case.case_id")
    pair = as_object(required(case, "pair", case_id), f"{case_id}.pair")
    dataset = as_object(required(case, "dataset", case_id), f"{case_id}.dataset")
    referencing = as_object(
        required(pair, "referencing_table", f"{case_id}.pair"),
        f"{case_id}.pair.referencing_table",
    )
    referenced = as_object(
        required(pair, "referenced_table", f"{case_id}.pair"),
        f"{case_id}.pair.referenced_table",
    )
    table_labels = [
        as_string(required(table, "label", case_id), f"{case_id}.table.label")
        for table in (referencing, referenced)
    ]
    for label in table_labels:
        require(label in table_shapes, f"{case_id}: no size found for {label}")

    join_rows = as_nonnegative_int(
        required(dataset, "join_rows", f"{case_id}.dataset"),
        f"{case_id}.dataset.join_rows",
    )
    folded_columns = len(folded_names_from_direct(pair, case_id))
    original_cells = sum(
        table_shapes[label].cells for label in dict.fromkeys(table_labels)
    )
    return PlotPoint(
        case_id=case_id,
        fold_chain_length=1,
        folded_join_cells=join_rows * folded_columns,
        original_table_cells=original_cells,
        folded_to_predecessor_update_ratio=(
            folded_to_predecessor_update_ratio
        ),
    )


def recursive_point(
    case: dict[str, Any],
    table_shapes: dict[str, TableShape],
    folded_to_predecessor_update_ratio: float,
) -> PlotPoint:
    case_id = as_string(required(case, "case_id", "case"), "case.case_id")
    fold_chain = as_object(
        required(case, "fold_chain", case_id), f"{case_id}.fold_chain"
    )
    roles = as_list(
        required(fold_chain, "roles", f"{case_id}.fold_chain"),
        f"{case_id}.fold_chain.roles",
    )
    stages = as_list(
        required(fold_chain, "stages", f"{case_id}.fold_chain"),
        f"{case_id}.fold_chain.stages",
    )
    role_order = as_list(
        required(fold_chain, "role_order", f"{case_id}.fold_chain"),
        f"{case_id}.fold_chain.role_order",
    )
    physical_order = as_list(
        required(fold_chain, "physical_table_order", f"{case_id}.fold_chain"),
        f"{case_id}.fold_chain.physical_table_order",
    )
    require(len(roles) >= 3, f"{case_id}: recursive chain has fewer than 3 roles")
    require(
        len(stages) == len(roles) - 1,
        f"{case_id}: join count differs from role count - 1",
    )

    stage_builds = as_list(
        required(case, "stage_builds", case_id), f"{case_id}.stage_builds"
    )
    require(
        len(stage_builds) == len(stages),
        f"{case_id}: stage_build count differs from join count",
    )

    role_symbols: list[str] = []
    role_tables: list[str] = []
    folded_names: set[str] = set()
    for index, raw_role in enumerate(roles):
        role = as_object(raw_role, f"{case_id}.roles[{index}]")
        symbol = as_string(
            required(role, "role_symbol", f"{case_id}.roles[{index}]"),
            f"{case_id}.roles[{index}].role_symbol",
        )
        table = as_string(
            required(role, "table", f"{case_id}.roles[{index}]"),
            f"{case_id}.roles[{index}].table",
        )
        columns = as_list(
            required(role, "columns", f"{case_id}.roles[{index}]"),
            f"{case_id}.roles[{index}].columns",
        )
        mapping = as_object(
            required(
                role,
                "logical_to_folded_property",
                f"{case_id}.roles[{index}]",
            ),
            f"{case_id}.roles[{index}].logical_to_folded_property",
        )
        require(
            set(mapping) == set(columns),
            f"{case_id}: role {symbol} mapping does not cover its columns",
        )
        require(table in table_shapes, f"{case_id}: no size found for {table}")
        require(
            len(columns) == table_shapes[table].columns,
            f"{case_id}: role {symbol} column count differs from {table}",
        )
        folded_names.update(
            as_string(value, f"{case_id}.roles[{index}].mapping value")
            for value in mapping.values()
        )
        role_symbols.append(symbol)
        role_tables.append(table)

    require(role_order == role_symbols, f"{case_id}: role_order is inconsistent")
    require(
        physical_order == role_tables,
        f"{case_id}: physical_table_order is inconsistent",
    )

    dataset = as_object(required(case, "dataset", case_id), f"{case_id}.dataset")
    join_rows = as_nonnegative_int(
        required(dataset, "join_rows", f"{case_id}.dataset"),
        f"{case_id}.dataset.join_rows",
    )
    original_cells = sum(
        table_shapes[table].cells for table in dict.fromkeys(role_tables)
    )
    return PlotPoint(
        case_id=case_id,
        fold_chain_length=len(stages),
        folded_join_cells=join_rows * len(folded_names),
        original_table_cells=original_cells,
        folded_to_predecessor_update_ratio=(
            folded_to_predecessor_update_ratio
        ),
    )


def build_points(cases: list[dict[str, Any]]) -> list[PlotPoint]:
    table_shapes = build_table_shapes(cases)
    points: list[PlotPoint] = []
    for case in cases:
        case_id = as_string(required(case, "case_id", "case"), "case.case_id")
        family = as_string(
            required(case, "experiment_family", case_id),
            f"{case_id}.experiment_family",
        )
        ratio = update_time_ratio(case, case_id)
        if family == DIRECT_FAMILY:
            point = direct_point(case, table_shapes, ratio)
        elif family == RECURSIVE_FAMILY:
            point = recursive_point(case, table_shapes, ratio)
        else:
            raise DiagramError(f"{case_id}: unsupported experiment family {family!r}")
        points.append(point)

    require(bool(points), "the JSON report contains no experiment cases")
    return points


def format_compact_count(value: float, _position: float | None = None) -> str:
    """Format axis counts compactly, for example 500K and 1.5M."""

    absolute_value = abs(value)
    if absolute_value >= 1_000_000:
        text = f"{value / 1_000_000:.1f}".rstrip("0").rstrip(".")
        return f"{text}M"
    if absolute_value >= 1_000:
        text = f"{value / 1_000:.1f}".rstrip("0").rstrip(".")
        return f"{text}K"
    return f"{value:,.0f}"


def format_ratio(value: float, _position: float | None = None) -> str:
    """Format a dimensionless update-time ratio as a multiplier."""

    return f"{value:g}×" if value >= 0 else ""


def draw_scatter(points: list[PlotPoint], output_path: Path) -> None:
    suffix = output_path.suffix.lower()
    require(
        suffix in SUPPORTED_OUTPUT_SUFFIXES,
        "output must end in .png, .svg, or .pdf",
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    x_values = [point.net_cell_difference for point in points]
    y_values = [point.fold_chain_length for point in points]
    ratio_values = [
        point.folded_to_predecessor_update_ratio for point in points
    ]
    figure, axis = plt.subplots(figsize=(9.5, 6.2), constrained_layout=True)
    fold_scatter = axis.scatter(
        x_values,
        y_values,
        s=62,
        color="#0072B2",
        alpha=0.72,
        edgecolors="white",
        linewidths=0.7,
        zorder=3,
    )
    ratio_axis = axis.twinx()
    ratio_scatter = ratio_axis.scatter(
        x_values,
        ratio_values,
        s=68,
        marker="^",
        facecolors="none",
        edgecolors="#D55E00",
        linewidths=1.4,
        alpha=0.8,
        zorder=4,
    )

    x_min = min(x_values)
    x_max = max(x_values)
    x_span = max(x_max - min(0, x_min), 1)
    axis.set_xlim(min(0, x_min - x_span * 0.05), x_max + x_span * 0.08)
    axis.xaxis.set_major_formatter(FuncFormatter(format_compact_count))

    minimum_join_count = min(y_values)
    maximum_join_count = max(y_values)
    axis.set_yticks(range(minimum_join_count, maximum_join_count + 1))
    axis.set_ylim(minimum_join_count - 0.5, maximum_join_count + 0.5)

    ratio_min = min(ratio_values)
    ratio_max = max(ratio_values)
    ratio_span = max(ratio_max - min(0, ratio_min), 1)
    ratio_axis.set_ylim(
        min(0, ratio_min - ratio_span * 0.05),
        ratio_max + ratio_span * 0.08,
    )
    ratio_axis.yaxis.set_major_formatter(FuncFormatter(format_ratio))

    axis.set_xlabel("Net logical-cell difference", fontsize=19)
    axis.set_ylabel("Fold-chain length", fontsize=20, color="#0072B2")
    ratio_axis.set_ylabel(
        "Folded / predecessor update-time ratio",
        fontsize=17,
        color="#D55E00",
    )
    axis.tick_params(axis="y", colors="#0072B2")
    ratio_axis.tick_params(axis="y", colors="#D55E00")
    axis.spines["left"].set_color("#0072B2")
    ratio_axis.spines["right"].set_color("#D55E00")
    # axis.set_title("Data Redundancy by Number of Folds", loc="left", pad=14)
    axis.grid(axis="x", color="#D9E1E8", linewidth=0.8)
    axis.set_axisbelow(True)
    axis.spines["top"].set_visible(False)
    ratio_axis.spines["top"].set_visible(False)
    axis.legend(
        (fold_scatter, ratio_scatter),
        ("Fold-chain length", "Folded / predecessor update-time ratio"),
        loc="lower center",
        bbox_to_anchor=(0.5, 1.01),
        ncol=2,
        frameon=False,
        fontsize=11,
    )

    figure.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(figure)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Draw the net replacement-relative cell difference against fold-"
            "chain length and the folded-to-predecessor update-time ratio."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="TPC-H extended experiment JSON report",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "output figure (.png, .svg, or .pdf); by default, write "
            f"{DEFAULT_OUTPUT_NAME} beside the input JSON"
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = args.input.resolve()
    output_path = (
        args.output.resolve()
        if args.output is not None
        else input_path.with_name(DEFAULT_OUTPUT_NAME)
    )
    try:
        points = build_points(read_cases(input_path))
        draw_scatter(points, output_path)
    except (DiagramError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    chain_lengths = [point.fold_chain_length for point in points]
    print(
        f"Scatter plot written to {output_path} "
        f"({len(points)} cases; {min(chain_lengths)}-{max(chain_lengths)} folds)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
