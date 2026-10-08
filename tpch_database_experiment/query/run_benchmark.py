#!/usr/bin/env python3
"""Benchmark the 74 schema templates against their Neo4j baselines.

By default the runner keeps only the MV(s) needed by the current template group:

1. removes stale generated MV artifacts left by an earlier interrupted run;
2. executes each Query baseline once before materializing its MVs;
3. materializes one exact strategy (or one query group when requested);
4. executes every corresponding template with the baseline parameters;
5. compares every baseline/template result pair;
6. deletes that group in ``finally`` before advancing in Query-ID order; and
7. writes a structured JSON report after the final summary.

Use ``--reuse-existing-mvs`` to retain the former read-only behavior.
The rotating runner skips relationship-fanout Phase 2 by default; use
``--inheritance-preflight`` to restore the exact directional fanout checks.
Each template run is capped at twice its matching baseline client time by
default; use ``--template-timeout-multiplier 0`` to disable that policy.

Rows are compared as a multiset, so a different order among tied rows is
accepted while duplicate rows are still significant.  Floating-point values
use configurable tolerances; other values are compared exactly.
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import os
import re
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from getpass import getpass
from pathlib import Path
from statistics import fmean
from time import perf_counter_ns
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

if __package__:
    from . import generate_params_neo4j
    from .benchmark_query_neo4j import BENCHMARK_QUERIES
else:
    import generate_params_neo4j
    from benchmark_query_neo4j import BENCHMARK_QUERIES

from tpch_database_experiment.join import (  # noqa: E402
    auto_join_mv_schema_neo4j as auto_join_mv,
)

enumerate_strategies = auto_join_mv.enumerate_strategies


RUNS_PER_TEMPLATE = 50
EXPECTED_TEMPLATE_COUNT = 74
DEFAULT_SEED = 20260724
DEFAULT_SCALE_FACTOR = 0.01
DEFAULT_REL_TOLERANCE = 1e-10
DEFAULT_ABS_TOLERANCE = 1e-7
DEFAULT_TEMPLATE_TIMEOUT_MULTIPLIER = 2.0
MIN_NEO4J_QUERY_TIMEOUT_SECONDS = 0.001
CLIENT_CONFIGURED_TIMEOUT_CODE = (
    "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration"
)
DATABASE_NAME = "tpch-sf-01"
EXPERIMENT_NAME = auto_join_mv.EXPERIMENT_NAME
MATERIALIZATION_SCHEMA_VERSION = auto_join_mv.MATERIALIZATION_SCHEMA_VERSION
TEMPLATE_DIRECTORY = Path(__file__).resolve().parents[1] / "templates_manual"
DEFAULT_OUTPUT_JSON = (
    Path(__file__).resolve().parents[0]
    / "results"
    / "results_benchmark"
    / DATABASE_NAME
    / "run_benchmark_results.json"
)
# TEMPLATE_DIRECTORY = Path(__file__).resolve().parents[1] / "templates_manual_foreign"
# TEMPLATE_DIRECTORY = Path(__file__).resolve().parents[1] / "templates_manual_test"


# Local debugging only.  NEO4J_PASSWORD takes precedence when it is set.
# Keep this as None in committed code; it can be temporarily filled locally.
DEBUG_NEO4J_PASSWORD: str | None = ""

PARAMETER_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
TEMPLATE_FILE_PATTERN = re.compile(r"template_(\d+)_([A-Za-z0-9_]+)\.py")
STRATEGY_NAME_PATTERN = re.compile(
    r"AUTO_JOIN_STRATEGY\s*\{\s*name\s*:\s*'([A-Za-z0-9_]+)'\s*\}"
)
MV_LABEL_PATTERN = re.compile(
    r"AUTO_JOIN_MV\s*:\s*`([A-Za-z0-9_]+)`"
)
READY_GUARD_PATTERN = re.compile(r"\.status\s*=\s*'ready'")

STRATEGY_METADATA_QUERY = """
UNWIND $names AS requested_name
OPTIONAL MATCH (strategy:AUTO_JOIN_STRATEGY)
WHERE strategy[$name_property] = requested_name
WITH requested_name, collect(properties(strategy)) AS matches
RETURN requested_name, matches
ORDER BY requested_name
"""


@dataclass(frozen=True)
class TemplateCase:
    query_id: int
    symbols: tuple[str, ...]
    strategy_name: str
    path: Path
    query: str

    @property
    def display_name(self) -> str:
        return self.path.name


@dataclass(frozen=True)
class QueryExecution:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]
    client_wall_ms: float
    available_after_ms: int | None
    consumed_after_ms: int | None
    neo4j_total_ms: int | None
    row_count: int | None = None


@dataclass(frozen=True)
class ExecutionAttempt:
    execution: QueryExecution | None
    error: str | None
    relative_limit_ms: float | None = None
    relative_limit_multiplier: float | None = None
    timed_out: bool = False

    @property
    def succeeded(self) -> bool:
        return self.execution is not None

    @property
    def exceeded_relative_limit(self) -> bool:
        if self.timed_out:
            return True
        return (
            self.execution is not None
            and self.relative_limit_ms is not None
            and self.execution.client_wall_ms > self.relative_limit_ms
        )


@dataclass(frozen=True)
class ResultComparison:
    equal: bool
    reason: str
    censored: bool = False


@dataclass(frozen=True)
class StrategyPreflight:
    ready: bool
    status: str
    detail: str


@dataclass(frozen=True)
class CaseOutcome:
    case: TemplateCase
    comparisons: tuple[ResultComparison, ...]
    baseline_attempts: tuple[ExecutionAttempt, ...]
    template_attempts: tuple[ExecutionAttempt, ...]
    setup_error: str | None = None
    mv_creation_wall_ms: float | None = None

    @property
    def passed(self) -> bool:
        return (
            self.setup_error is None
            and bool(self.comparisons)
            and all(comparison.equal for comparison in self.comparisons)
        )

    @property
    def censored(self) -> bool:
        return any(comparison.censored for comparison in self.comparisons)

    @property
    def failed(self) -> bool:
        if self.setup_error is not None or not self.comparisons:
            return True
        return any(
            not comparison.equal and not comparison.censored
            for comparison in self.comparisons
        )


@dataclass(frozen=True)
class BenchmarkPlan:
    all_cases: tuple[TemplateCase, ...]
    cases: tuple[TemplateCase, ...]
    query_ids: tuple[int, ...]
    parameters_by_query: dict[int, list[dict[str, Any]]]


@dataclass(frozen=True)
class OrderField:
    column: str
    descending: bool = False


ORDER_FIELDS_BY_QUERY: dict[int, tuple[OrderField, ...]] = {
    2: (
        OrderField("s_acctbal", descending=True),
        OrderField("n_name"),
        OrderField("s_name"),
        OrderField("p_partkey"),
    ),
    3: (
        OrderField("revenue", descending=True),
        OrderField("o_orderdate"),
        OrderField("l_orderkey"),
    ),
    4: (OrderField("o_orderpriority"),),
    5: (OrderField("revenue", descending=True),),
    7: (
        OrderField("supp_nation"),
        OrderField("cust_nation"),
        OrderField("l_year"),
    ),
    8: (OrderField("o_year"),),
    9: (
        OrderField("nation"),
        OrderField("o_year", descending=True),
    ),
    10: (
        OrderField("revenue", descending=True),
        OrderField("c_custkey"),
    ),
    11: (OrderField("value", descending=True),),
    12: (OrderField("l_shipmode"),),
    13: (
        OrderField("custdist", descending=True),
        OrderField("c_count", descending=True),
    ),
    16: (
        OrderField("supplier_cnt", descending=True),
        OrderField("p_brand"),
        OrderField("p_type"),
        OrderField("p_size"),
    ),
    18: (
        OrderField("o_totalprice", descending=True),
        OrderField("o_orderdate"),
        OrderField("o_orderkey"),
    ),
    20: (OrderField("s_name"),),
    21: (
        OrderField("numwait", descending=True),
        OrderField("s_name"),
    ),
    22: (OrderField("cntrycode"),),
}

# These LIMIT queries do not have a unique final ordering key in the original
# TPC-H text.  Adding the same returned key to both variants prevents a valid
# tie at the LIMIT boundary from selecting different rows.
LIMIT_TIEBREAKER_BY_QUERY = {
    3: "l_orderkey",
    10: "c_custkey",
    18: "o_orderkey",
}


def positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def positive_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return value


def non_negative_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError("must be a finite non-negative number")
    return value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run each Query baseline once, then run every loaded Cypher "
            "template with the same generated parameters and compare results."
        )
    )
    parser.add_argument(
        "--uri",
        default=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        help="Neo4j URI (default: NEO4J_URI or bolt://localhost:7687)",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("NEO4J_USER", "neo4j"),
        help="Neo4j user (default: NEO4J_USER or neo4j)",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("NEO4J_DATABASE") or DATABASE_NAME,
        help=f"Neo4j database (default: NEO4J_DATABASE or {DATABASE_NAME})",
    )
    parser.add_argument(
        "--templates-dir",
        type=Path,
        default=TEMPLATE_DIRECTORY,
        help=f"template directory (default: {TEMPLATE_DIRECTORY})",
    )
    parser.add_argument(
        "--reuse-existing-mvs",
        action="store_true",
        help=(
            "do not create or delete MVs; benchmark strategies that are already "
            "materialized (the former runner behavior)"
        ),
    )
    parser.add_argument(
        "--rotation-scope",
        choices=("strategy", "query"),
        default="strategy",
        help=(
            "rotating MV group: one strategy/template for the lowest storage "
            "peak, or all selected strategies for one Query (default: strategy)"
        ),
    )
    parser.add_argument(
        "--initial-cleanup-scope",
        choices=("all", "selected"),
        default="all",
        help=(
            "before rotating, remove all generated AUTO_JOIN artifacts for the "
            "lowest and cleanest storage state, or only selected strategies "
            "(default: all; source TPC-H nodes are never removed)"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=positive_int,
        default=auto_join_mv.DEFAULT_BATCH_SIZE,
        help=(
            "MV rows committed per inner transaction "
            f"(default: {auto_join_mv.DEFAULT_BATCH_SIZE})"
        ),
    )
    parser.add_argument(
        "--max-estimated-rows",
        type=positive_int,
        default=auto_join_mv.DEFAULT_MAX_ESTIMATED_ROWS,
        help=(
            "skip an MV when an EXPLAIN operator reaches this estimate "
            f"(default: {auto_join_mv.DEFAULT_MAX_ESTIMATED_ROWS})"
        ),
    )
    parser.add_argument(
        "--max-candidate-rows",
        type=positive_int,
        default=auto_join_mv.DEFAULT_MAX_CANDIDATE_ROWS,
        help=(
            "skip an MV when its bounded count exceeds this many rows "
            f"(default: {auto_join_mv.DEFAULT_MAX_CANDIDATE_ROWS})"
        ),
    )
    parser.add_argument(
        "--query-timeout-seconds",
        type=non_negative_float,
        default=auto_join_mv.DEFAULT_QUERY_TIMEOUT_SECONDS,
        help=(
            "server-side timeout for each MV count, preflight, and build; 0 "
            "disables it "
            f"(default: {auto_join_mv.DEFAULT_QUERY_TIMEOUT_SECONDS:g})"
        ),
    )
    parser.add_argument(
        "--allow-unestimated",
        action="store_true",
        help="build an MV even when EXPLAIN has no cardinality estimate",
    )
    parser.add_argument(
        "--inheritance-preflight",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "measure directional relationship fanout before each MV build; "
            "by default Phase 2 is skipped and validated boundary types are "
            "used after structural hard blocks"
        ),
    )
    parser.add_argument(
        "--max-incoming-fanout",
        type=positive_int,
        default=auto_join_mv.DEFAULT_MAX_INCOMING_FANOUT,
        help=(
            "maximum incoming inherited fanout when --inheritance-preflight "
            f"is enabled (default: {auto_join_mv.DEFAULT_MAX_INCOMING_FANOUT})"
        ),
    )
    parser.add_argument(
        "--max-outgoing-degree",
        type=positive_int,
        default=auto_join_mv.DEFAULT_MAX_OUTGOING_DEGREE,
        help=(
            "maximum outgoing inherited degree when --inheritance-preflight "
            f"is enabled (default: {auto_join_mv.DEFAULT_MAX_OUTGOING_DEGREE})"
        ),
    )
    parser.add_argument(
        "--create-indexes",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="create candidate-specific indexes while each MV is active",
    )
    parser.add_argument(
        "--index-wait-seconds",
        type=positive_int,
        default=auto_join_mv.DEFAULT_INDEX_WAIT_SECONDS,
        help=(
            "maximum wait for candidate indexes to become ONLINE "
            f"(default: {auto_join_mv.DEFAULT_INDEX_WAIT_SECONDS})"
        ),
    )
    parser.add_argument(
        "--require-complete-template-set",
        action="store_true",
        help=(
            "require all 74 schema templates; by default a validated subset "
            "is allowed for one-at-a-time debugging"
        ),
    )
    parser.add_argument(
        "--runs",
        type=positive_int,
        default=RUNS_PER_TEMPLATE,
        help=f"runs for each baseline and template (default: {RUNS_PER_TEMPLATE})",
    )
    parser.add_argument(
        "--template-timeout-multiplier",
        type=non_negative_float,
        default=DEFAULT_TEMPLATE_TIMEOUT_MULTIPLIER,
        help=(
            "stop each template run after this multiple of its matching "
            "baseline client time; 0 disables the limit "
            f"(default: {DEFAULT_TEMPLATE_TIMEOUT_MULTIPLIER:g})"
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"parameter-generator seed (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--sf",
        type=positive_float,
        default=DEFAULT_SCALE_FACTOR,
        help=f"TPC-H scale factor used by Q11 (default: {DEFAULT_SCALE_FACTOR})",
    )
    parser.add_argument(
        "--rel-tol",
        type=non_negative_float,
        default=DEFAULT_REL_TOLERANCE,
        help=(
            "relative tolerance for finite floating-point values "
            f"(default: {DEFAULT_REL_TOLERANCE:g})"
        ),
    )
    parser.add_argument(
        "--abs-tol",
        type=non_negative_float,
        default=DEFAULT_ABS_TOLERANCE,
        help=(
            "absolute tolerance for finite floating-point values "
            f"(default: {DEFAULT_ABS_TOLERANCE:g})"
        ),
    )
    parser.add_argument(
        "--query-id",
        type=positive_int,
        action="append",
        default=[],
        help="run only this TPC-H query ID; may be supplied more than once",
    )
    parser.add_argument(
        "--strategy",
        action="append",
        default=[],
        help="run only this exact AUTO_JOIN_STRATEGY name; may be repeated",
    )
    parser.add_argument(
        "--show-query",
        action="store_true",
        help="print the first baseline and template query with parameters inlined",
    )
    parser.add_argument(
        "--require-all-ready",
        action="store_true",
        help=(
            "existing-MV mode: abort before all timing if any selection is not "
            "ready; rotating mode: abort before timing the first non-ready group"
        ),
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="stop immediately after a query execution error",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=DEFAULT_OUTPUT_JSON,
        help=(
            "path for the JSON report; defaults to "
            "tpch_database_experiment/query/results/results_benchmark/"
            f"{DATABASE_NAME}/"
            "run_benchmark_results.json"
        ),
    )
    return parser.parse_args(argv)


def _literal_template_query(path: Path, query_id: int) -> str:
    """Read one generated module without importing or executing it."""

    try:
        source = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"cannot read template file {path}: {exc}") from exc

    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        raise RuntimeError(f"invalid Python syntax in {path.name}: {exc}") from exc

    expected_name = f"TEMPLATE_Q{query_id}"
    assignment: ast.Assign | None = None
    for node in tree.body:
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            # The generated module's documentation string.
            continue
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == expected_name
        ):
            if assignment is not None:
                raise RuntimeError(
                    f"{path.name} assigns {expected_name} more than once"
                )
            assignment = node
            continue
        raise RuntimeError(
            f"{path.name} must contain only a docstring and {expected_name}"
        )

    if assignment is None:
        raise RuntimeError(f"{path.name} does not define {expected_name}")
    try:
        value = ast.literal_eval(assignment.value)
    except (ValueError, TypeError) as exc:
        raise RuntimeError(
            f"{expected_name} in {path.name} must be a string literal"
        ) from exc
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(
            f"{expected_name} in {path.name} must be a non-empty string"
        )
    return value


def query_parameter_names(query: str) -> frozenset[str]:
    return frozenset(PARAMETER_PATTERN.findall(query))


def load_template_cases(
    template_dir: Path,
    *,
    require_complete: bool = False,
) -> list[TemplateCase]:
    template_dir = template_dir.expanduser().resolve()
    if not template_dir.is_dir():
        raise RuntimeError(f"template directory does not exist: {template_dir}")

    paths = sorted(template_dir.glob("template_*.py"))
    expected_by_filename = {
        (
            f"template_{strategy.query_id}_"
            f"{'_'.join(strategy.symbols)}.py"
        ): strategy
        for strategy in enumerate_strategies()
    }
    if len(expected_by_filename) != EXPECTED_TEMPLATE_COUNT:
        raise RuntimeError(
            "auto_join_mv_schema_neo4j produced "
            f"{len(expected_by_filename)} strategies; "
            f"expected {EXPECTED_TEMPLATE_COUNT}"
        )

    if not paths:
        raise RuntimeError(f"{template_dir} contains no template_*.py files")

    actual_filenames = {path.name for path in paths}
    expected_filenames = set(expected_by_filename)
    extra = sorted(actual_filenames - expected_filenames)
    if extra:
        raise RuntimeError(
            f"{template_dir} contains files that are not schema strategies: "
            f"{extra!r}"
        )
    if require_complete and actual_filenames != expected_filenames:
        missing = sorted(expected_filenames - actual_filenames)
        raise RuntimeError(
            f"{template_dir} is missing {len(missing)} of the 74 schema "
            f"templates: {missing!r}"
        )

    cases: list[TemplateCase] = []
    seen_strategies: set[str] = set()
    for path in paths:
        file_match = TEMPLATE_FILE_PATTERN.fullmatch(path.name)
        if file_match is None:
            raise RuntimeError(f"unexpected template filename: {path.name}")
        query_id = int(file_match.group(1))
        symbols = tuple(file_match.group(2).split("_"))
        expected_strategy = expected_by_filename[path.name]
        if (
            query_id != expected_strategy.query_id
            or symbols != expected_strategy.symbols
        ):
            raise RuntimeError(
                f"{path.name} does not match its enumerated schema strategy"
            )
        if query_id not in BENCHMARK_QUERIES:
            raise RuntimeError(
                f"{path.name} refers to missing baseline Q{query_id}"
            )

        query = _literal_template_query(path, query_id)
        strategy_names = set(STRATEGY_NAME_PATTERN.findall(query))
        if strategy_names and strategy_names != {expected_strategy.name}:
            raise RuntimeError(
                f"{path.name} metadata strategies "
                f"{sorted(strategy_names)!r} do not match "
                f"{expected_strategy.name!r}"
            )
        strategy_name = expected_strategy.name
        if not strategy_name.startswith(f"q{query_id}_mv_"):
            raise RuntimeError(
                f"{path.name} references strategy {strategy_name!r} "
                f"for the wrong query ID"
            )
        mv_labels = set(MV_LABEL_PATTERN.findall(query))
        if mv_labels != {strategy_name}:
            raise RuntimeError(
                f"{path.name} MV labels {sorted(mv_labels)!r} do not match "
                f"strategy {strategy_name!r}"
            )
        strategy_reference_count = len(STRATEGY_NAME_PATTERN.findall(query))
        ready_guard_count = len(READY_GUARD_PATTERN.findall(query))
        if ready_guard_count != strategy_reference_count:
            raise RuntimeError(
                f"{path.name} contains {strategy_reference_count} strategy "
                f"matches but {ready_guard_count} ready-status guards"
            )
        if strategy_name in seen_strategies:
            raise RuntimeError(
                f"strategy {strategy_name!r} appears in more than one template"
            )
        seen_strategies.add(strategy_name)

        baseline_parameters = query_parameter_names(BENCHMARK_QUERIES[query_id])
        template_parameters = query_parameter_names(query)
        if template_parameters != baseline_parameters:
            raise RuntimeError(
                f"{path.name} parameters {sorted(template_parameters)!r} do not "
                f"match Q{query_id} baseline {sorted(baseline_parameters)!r}"
            )

        cases.append(
            TemplateCase(
                query_id=query_id,
                symbols=symbols,
                strategy_name=strategy_name,
                path=path,
                query=query,
            )
        )

    cases.sort(key=lambda case: (case.query_id, case.display_name))
    return cases


def select_template_cases(
    cases: Sequence[TemplateCase],
    query_ids: Sequence[int],
    strategy_names: Sequence[str],
) -> list[TemplateCase]:
    selected_query_ids = set(query_ids)
    selected_strategy_names = set(strategy_names)

    known_query_ids = {case.query_id for case in cases}
    unknown_query_ids = selected_query_ids - known_query_ids
    if unknown_query_ids:
        raise RuntimeError(
            "no loaded template candidates exist for query IDs: "
            + ", ".join(f"Q{query_id}" for query_id in sorted(unknown_query_ids))
        )

    known_strategy_names = {case.strategy_name for case in cases}
    unknown_strategies = selected_strategy_names - known_strategy_names
    if unknown_strategies:
        raise RuntimeError(
            "unknown template strategies: " + ", ".join(sorted(unknown_strategies))
        )

    selected = [
        case
        for case in cases
        if (not selected_query_ids or case.query_id in selected_query_ids)
        and (
            not selected_strategy_names
            or case.strategy_name in selected_strategy_names
        )
    ]
    if not selected:
        raise RuntimeError("the query/strategy filters selected no templates")
    return selected


def generate_parameter_sets(
    query_id: int,
    seed: int,
    scale_factor: float,
    runs: int,
) -> list[dict[str, Any]]:
    generator_name = f"make_params_q{query_id}"
    try:
        generator = getattr(generate_params_neo4j, generator_name)
    except AttributeError as exc:
        raise RuntimeError(f"missing parameter generator: {generator_name}") from exc

    if query_id == 11:
        generated = generator(seed, runs, scale_factor)
    else:
        generated = generator(seed, runs)
    if len(generated) != runs:
        raise RuntimeError(
            f"{generator_name} returned {len(generated)} parameter sets; "
            f"expected {runs}"
        )

    expected_names = query_parameter_names(BENCHMARK_QUERIES[query_id])
    parameter_sets: list[dict[str, Any]] = []
    for run_number, parameters in enumerate(generated, start=1):
        if not isinstance(parameters, dict):
            raise RuntimeError(
                f"{generator_name} run {run_number} did not return a dictionary"
            )
        actual_names = set(parameters)
        if actual_names != expected_names:
            raise RuntimeError(
                f"{generator_name} run {run_number} returned parameters "
                f"{sorted(actual_names)!r}; expected {sorted(expected_names)!r}"
            )
        parameter_sets.append(dict(parameters))
    return parameter_sets


def format_cypher_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        escaped = (
            value.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\b", "\\b")
            .replace("\f", "\\f")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
        )
        return f"'{escaped}'"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Cypher display values must be finite")
        return repr(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(format_cypher_literal(item) for item in value) + "]"
    raise TypeError(
        f"unsupported Cypher display value type: {type(value).__name__}"
    )


def inline_query_parameters(query: str, parameters: Mapping[str, Any]) -> str:
    def replace_parameter(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in parameters:
            raise KeyError(f"missing value for Cypher parameter ${name}")
        return format_cypher_literal(parameters[name])

    return PARAMETER_PATTERN.sub(replace_parameter, query)


def add_limit_tiebreaker(query: str, query_id: int) -> str:
    tiebreaker = LIMIT_TIEBREAKER_BY_QUERY.get(query_id)
    if tiebreaker is None:
        return query

    limit_index = query.rfind("\nLIMIT ")
    if limit_index < 0:
        raise RuntimeError(
            f"Q{query_id} requires a deterministic LIMIT but has no LIMIT clause"
        )
    order_index = query.rfind("\nORDER BY", 0, limit_index)
    if order_index < 0:
        raise RuntimeError(
            f"Q{query_id} requires a deterministic LIMIT but has no ORDER BY"
        )
    returned_columns = {
        field.column for field in ORDER_FIELDS_BY_QUERY[query_id]
    }
    if tiebreaker not in returned_columns:
        raise RuntimeError(
            f"Q{query_id} tiebreaker {tiebreaker!r} is not a returned column"
        )

    prefix = query[:limit_index].rstrip()
    suffix = query[limit_index + 1 :]
    return f"{prefix},\n    {tiebreaker}\n{suffix}"


def quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def fetch_strategy_metadata(
    session: Any,
    cases: Sequence[TemplateCase],
) -> dict[str, list[dict[str, Any]]]:
    names = [case.strategy_name for case in cases]
    if (
        auto_join_mv.STRATEGY_LABEL
        not in auto_join_mv.database_node_labels(session)
    ):
        return {name: [] for name in names}

    result = session.run(
        STRATEGY_METADATA_QUERY,
        names=names,
        name_property="name",
    )
    records = list(result)
    result.consume()
    metadata: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        requested_name = str(record["requested_name"])
        matches = [dict(item) for item in record["matches"]]
        metadata[requested_name] = matches
    return metadata


def count_strategy_nodes(session: Any, strategy_name: str) -> int:
    query = (
        f"MATCH (mv:{quote_identifier(strategy_name)}) "
        "RETURN count(mv) AS node_count"
    )
    result = session.run(query)
    records = list(result)
    result.consume()
    if len(records) != 1:
        raise RuntimeError(
            f"could not count nodes for strategy {strategy_name!r}"
        )
    return int(records[0]["node_count"])


def expected_inheritance_modes(args: argparse.Namespace) -> frozenset[str]:
    """Return metadata modes compatible with the selected MV lifecycle."""

    known_modes = frozenset(
        {
            auto_join_mv.CARDINALITY_AWARE_INHERITANCE_MODE,
            auto_join_mv.UNCHECKED_INHERITANCE_MODE,
        }
    )
    if getattr(args, "reuse_existing_mvs", False):
        return known_modes
    if getattr(args, "inheritance_preflight", False):
        return frozenset({auto_join_mv.CARDINALITY_AWARE_INHERITANCE_MODE})
    return frozenset({auto_join_mv.UNCHECKED_INHERITANCE_MODE})


def preflight_template_cases(
    session: Any,
    cases: Sequence[TemplateCase],
    accepted_inheritance_modes: frozenset[str] | None = None,
) -> dict[str, StrategyPreflight]:
    if accepted_inheritance_modes is None:
        accepted_inheritance_modes = frozenset(
            {
                auto_join_mv.CARDINALITY_AWARE_INHERITANCE_MODE,
                auto_join_mv.UNCHECKED_INHERITANCE_MODE,
            }
        )
    metadata_by_name = fetch_strategy_metadata(session, cases)
    preflights: dict[str, StrategyPreflight] = {}

    for case in cases:
        matches = metadata_by_name.get(case.strategy_name, [])
        if not matches:
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status="missing",
                detail="AUTO_JOIN_STRATEGY metadata does not exist",
            )
            continue
        if len(matches) != 1:
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status="duplicate",
                detail=f"found {len(matches)} metadata nodes",
            )
            continue

        metadata = matches[0]
        status = str(metadata.get("status") or "unknown")
        if status != "ready":
            message = metadata.get("message")
            estimated_rows = metadata.get("estimated_rows")
            details = [f"strategy status is {status!r}"]
            if estimated_rows is not None:
                details.append(f"estimated_rows={estimated_rows:,}")
            if message:
                details.append(str(message))
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status=status,
                detail="; ".join(details),
            )
            continue

        metadata_query_id = metadata.get("query_id")
        if metadata_query_id != case.query_id:
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status=status,
                detail=(
                    f"metadata query_id={metadata_query_id!r}; "
                    f"expected {case.query_id}"
                ),
            )
            continue

        metadata_symbols = tuple(metadata.get("symbols") or ())
        if metadata_symbols != case.symbols:
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status=status,
                detail=(
                    f"metadata symbols={metadata_symbols!r}; "
                    f"expected {case.symbols!r}"
                ),
            )
            continue

        experiment = metadata.get("experiment")
        if experiment != EXPERIMENT_NAME:
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status=status,
                detail=(
                    f"metadata experiment={experiment!r}; "
                    f"expected {EXPERIMENT_NAME!r}"
                ),
            )
            continue

        materialization_schema_version = metadata.get(
            "materialization_schema_version"
        )
        if materialization_schema_version != MATERIALIZATION_SCHEMA_VERSION:
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status=status,
                detail=(
                    "metadata materialization_schema_version="
                    f"{materialization_schema_version!r}; expected "
                    f"{MATERIALIZATION_SCHEMA_VERSION!r}; rebuild this MV"
                ),
            )
            continue

        inheritance_mode = metadata.get("inheritance_mode")
        if inheritance_mode not in accepted_inheritance_modes:
            expected_modes = ", ".join(
                repr(mode) for mode in sorted(accepted_inheritance_modes)
            )
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status=status,
                detail=(
                    f"metadata inheritance_mode={inheritance_mode!r}; expected "
                    f"one of {expected_modes}"
                ),
            )
            continue

        invalid_allowlists = [
            property_name
            for property_name in (
                "allowed_incoming_types",
                "allowed_outgoing_types",
            )
            if not isinstance(metadata.get(property_name), list)
        ]
        if invalid_allowlists:
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status=status,
                detail=(
                    "metadata properties must be lists: "
                    + ", ".join(invalid_allowlists)
                ),
            )
            continue

        row_count = metadata.get("row_count")
        if not isinstance(row_count, int) or isinstance(row_count, bool) or row_count < 0:
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status=status,
                detail=f"invalid metadata row_count={row_count!r}",
            )
            continue

        actual_node_count = count_strategy_nodes(session, case.strategy_name)
        if actual_node_count != row_count:
            preflights[case.strategy_name] = StrategyPreflight(
                ready=False,
                status=status,
                detail=(
                    f"metadata row_count={row_count:,}, but label "
                    f"{case.strategy_name!r} contains {actual_node_count:,} nodes"
                ),
            )
            continue

        preflights[case.strategy_name] = StrategyPreflight(
            ready=True,
            status=status,
            detail=f"{row_count:,} materialized nodes",
        )

    return preflights


def execute_query(
    session: Any,
    query: str,
    parameters: Mapping[str, Any],
    *,
    timeout_seconds: float | None = None,
) -> QueryExecution:
    executable_query: Any = query
    if timeout_seconds is not None:
        from neo4j import Query

        executable_query = Query(query, timeout=timeout_seconds)

    started_ns = perf_counter_ns()
    result = session.run(executable_query, dict(parameters))
    columns = tuple(result.keys())
    rows = tuple(
        tuple(record[column] for column in columns)
        for record in result
    )
    summary = result.consume()
    client_wall_ms = (perf_counter_ns() - started_ns) / 1_000_000

    available_after_ms = summary.result_available_after
    consumed_after_ms = summary.result_consumed_after
    if available_after_ms is None or consumed_after_ms is None:
        neo4j_total_ms = None
    else:
        neo4j_total_ms = available_after_ms + consumed_after_ms
    return QueryExecution(
        columns=columns,
        rows=rows,
        client_wall_ms=client_wall_ms,
        available_after_ms=available_after_ms,
        consumed_after_ms=consumed_after_ms,
        neo4j_total_ms=neo4j_total_ms,
        row_count=len(rows),
    )


def _temporal_value(value: Any) -> tuple[str, str] | None:
    if isinstance(value, datetime):
        return "datetime", value.isoformat()
    if isinstance(value, date):
        return "date", value.isoformat()
    if isinstance(value, time):
        return "time", value.isoformat()

    module_name = type(value).__module__
    if module_name.startswith("neo4j.time"):
        iso_method = getattr(value, "iso_format", None)
        if callable(iso_method):
            return type(value).__name__.lower(), str(iso_method())
        iso_method = getattr(value, "isoformat", None)
        if callable(iso_method):
            return type(value).__name__.lower(), str(iso_method())
    return None


def values_equal(
    left: Any,
    right: Any,
    *,
    rel_tol: float,
    abs_tol: float,
) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right

    if isinstance(left, float) or isinstance(right, float):
        if not isinstance(left, float) or not isinstance(right, float):
            return False
        if not math.isfinite(left) or not math.isfinite(right):
            return False
        return math.isclose(left, right, rel_tol=rel_tol, abs_tol=abs_tol)

    left_temporal = _temporal_value(left)
    right_temporal = _temporal_value(right)
    if left_temporal is not None or right_temporal is not None:
        return left_temporal == right_temporal

    if isinstance(left, Mapping) or isinstance(right, Mapping):
        if not isinstance(left, Mapping) or not isinstance(right, Mapping):
            return False
        if set(left) != set(right):
            return False
        return all(
            values_equal(
                left[key],
                right[key],
                rel_tol=rel_tol,
                abs_tol=abs_tol,
            )
            for key in left
        )

    sequence_types = (list, tuple)
    if isinstance(left, sequence_types) or isinstance(right, sequence_types):
        if type(left) is not type(right):
            return False
        if len(left) != len(right):
            return False
        return all(
            values_equal(
                left_item,
                right_item,
                rel_tol=rel_tol,
                abs_tol=abs_tol,
            )
            for left_item, right_item in zip(left, right)
        )

    return type(left) is type(right) and left == right


def _compare_order_values(
    left: Any,
    right: Any,
) -> int:
    if left is None:
        return 0 if right is None else 1
    if right is None:
        return -1

    left_temporal = _temporal_value(left)
    right_temporal = _temporal_value(right)
    if left_temporal is not None or right_temporal is not None:
        if left_temporal is None or right_temporal is None:
            raise TypeError(
                f"cannot order temporal {left!r} and non-temporal {right!r}"
            )
        left_value: Any = left_temporal
        right_value: Any = right_temporal
    else:
        left_value = left
        right_value = right

    try:
        if left_value == right_value:
            return 0
        if left_value < right_value:
            return -1
        if left_value > right_value:
            return 1
    except TypeError as exc:
        raise TypeError(
            f"cannot compare ordering values {left!r} and {right!r}"
        ) from exc
    raise TypeError(
        f"ordering values are neither equal nor ordered: {left!r}, {right!r}"
    )


def validate_result_order(
    execution: QueryExecution,
    query_id: int,
) -> str | None:
    try:
        order_fields = ORDER_FIELDS_BY_QUERY[query_id]
    except KeyError as exc:
        raise RuntimeError(f"missing ORDER BY specification for Q{query_id}") from exc

    missing_columns = [
        field.column
        for field in order_fields
        if field.column not in execution.columns
    ]
    if missing_columns:
        return f"ORDER BY columns missing from result: {missing_columns!r}"
    indexes = [
        (execution.columns.index(field.column), field)
        for field in order_fields
    ]

    for row_index in range(1, len(execution.rows)):
        previous = execution.rows[row_index - 1]
        current = execution.rows[row_index]
        comparison = 0
        for column_index, field in indexes:
            comparison = _compare_order_values(
                previous[column_index],
                current[column_index],
            )
            if field.descending:
                comparison = -comparison
            if comparison:
                break
        if comparison > 0:
            return (
                f"ORDER BY violation between rows {row_index} and "
                f"{row_index + 1}: {_short_repr(previous)} then "
                f"{_short_repr(current)}"
            )
    return None


def _comparison_bucket(value: Any) -> Any:
    """Build an exact-value bucket while treating every float as a wildcard."""

    if isinstance(value, float):
        return ("float",)
    temporal = _temporal_value(value)
    if temporal is not None:
        return ("temporal", temporal)
    if isinstance(value, Mapping):
        return (
            "mapping",
            tuple(
                sorted(
                    (
                        _comparison_bucket(key),
                        _comparison_bucket(item),
                    )
                    for key, item in value.items()
                )
            ),
        )
    if isinstance(value, (list, tuple)):
        return (
            type(value).__name__,
            tuple(_comparison_bucket(item) for item in value),
        )
    try:
        hash(value)
    except TypeError:
        return (type(value).__qualname__, repr(value))
    return (type(value).__qualname__, value)


def _short_repr(value: Any, limit: int = 260) -> str:
    text = repr(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def compare_results(
    baseline: QueryExecution,
    template: QueryExecution,
    *,
    rel_tol: float,
    abs_tol: float,
) -> ResultComparison:
    if baseline.columns != template.columns:
        return ResultComparison(
            equal=False,
            reason=(
                f"column mismatch: baseline={baseline.columns!r}, "
                f"template={template.columns!r}"
            ),
        )
    if len(baseline.rows) != len(template.rows):
        return ResultComparison(
            equal=False,
            reason=(
                f"row-count mismatch: baseline={len(baseline.rows):,}, "
                f"template={len(template.rows):,}"
            ),
        )

    baseline_buckets: dict[Any, list[tuple[Any, ...]]] = defaultdict(list)
    template_buckets: dict[Any, list[tuple[Any, ...]]] = defaultdict(list)
    for row in baseline.rows:
        baseline_buckets[_comparison_bucket(row)].append(row)
    for row in template.rows:
        template_buckets[_comparison_bucket(row)].append(row)

    if set(baseline_buckets) != set(template_buckets):
        missing_bucket = next(
            (
                bucket
                for bucket in baseline_buckets
                if bucket not in template_buckets
            ),
            None,
        )
        if missing_bucket is not None:
            example = baseline_buckets[missing_bucket][0]
            return ResultComparison(
                equal=False,
                reason=f"template is missing baseline row {_short_repr(example)}",
            )
        extra_bucket = next(
            bucket
            for bucket in template_buckets
            if bucket not in baseline_buckets
        )
        example = template_buckets[extra_bucket][0]
        return ResultComparison(
            equal=False,
            reason=f"template has extra row {_short_repr(example)}",
        )

    for bucket, baseline_rows in baseline_buckets.items():
        template_rows = template_buckets[bucket]
        if len(baseline_rows) != len(template_rows):
            return ResultComparison(
                equal=False,
                reason=(
                    "duplicate-count mismatch for row group "
                    f"{_short_repr(baseline_rows[0])}: "
                    f"baseline={len(baseline_rows)}, "
                    f"template={len(template_rows)}"
                ),
            )

        # Maximum bipartite matching avoids a false mismatch when several
        # near-equal floating-point rows share the same exact-value bucket.
        matched_baseline_by_template = [-1] * len(template_rows)

        def find_match(
            baseline_index: int,
            visited_template_indexes: set[int],
        ) -> bool:
            baseline_row = baseline_rows[baseline_index]
            for template_index, template_row in enumerate(template_rows):
                if template_index in visited_template_indexes:
                    continue
                if not values_equal(
                    baseline_row,
                    template_row,
                    rel_tol=rel_tol,
                    abs_tol=abs_tol,
                ):
                    continue
                visited_template_indexes.add(template_index)
                previous_baseline = matched_baseline_by_template[template_index]
                if previous_baseline == -1 or find_match(
                    previous_baseline,
                    visited_template_indexes,
                ):
                    matched_baseline_by_template[template_index] = baseline_index
                    return True
            return False

        for baseline_index in range(len(baseline_rows)):
            if not find_match(baseline_index, set()):
                return ResultComparison(
                    equal=False,
                    reason=(
                        "numeric/value mismatch near baseline row "
                        f"{_short_repr(baseline_rows[baseline_index])}"
                    ),
                )

    return ResultComparison(
        equal=True,
        reason=f"same {len(baseline.rows):,} rows",
    )


def _format_ms(value: float | int | None) -> str:
    return "N/A" if value is None else f"{value:.3f}"


def _is_client_configured_timeout(exc: BaseException) -> bool:
    """Recognize only the timeout attached to this query by the runner."""

    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if getattr(current, "code", None) == CLIENT_CONFIGURED_TIMEOUT_CODE:
            return True
        current = current.__cause__
    return False


def _attempt_query(
    session: Any,
    query: str,
    parameters: Mapping[str, Any],
    *,
    fail_fast: bool,
    relative_limit_ms: float | None = None,
    relative_limit_multiplier: float | None = None,
) -> ExecutionAttempt:
    timeout_seconds = (
        None
        if relative_limit_ms is None
        else max(
            MIN_NEO4J_QUERY_TIMEOUT_SECONDS,
            relative_limit_ms / 1_000,
        )
    )
    try:
        return ExecutionAttempt(
            execution=execute_query(
                session,
                query,
                parameters,
                timeout_seconds=timeout_seconds,
            ),
            error=None,
            relative_limit_ms=relative_limit_ms,
            relative_limit_multiplier=relative_limit_multiplier,
        )
    except Exception as exc:
        if relative_limit_ms is not None and _is_client_configured_timeout(exc):
            return ExecutionAttempt(
                execution=None,
                error=None,
                relative_limit_ms=relative_limit_ms,
                relative_limit_multiplier=relative_limit_multiplier,
                timed_out=True,
            )
        if fail_fast:
            raise
        return ExecutionAttempt(
            execution=None,
            error=f"{type(exc).__name__}: {exc}",
            relative_limit_ms=relative_limit_ms,
            relative_limit_multiplier=relative_limit_multiplier,
        )


def _timing_only_attempt(attempt: ExecutionAttempt) -> ExecutionAttempt:
    """Discard retained result rows after comparison while preserving metrics."""

    execution = attempt.execution
    if execution is None:
        return attempt
    return ExecutionAttempt(
        execution=QueryExecution(
            columns=execution.columns,
            rows=(),
            client_wall_ms=execution.client_wall_ms,
            available_after_ms=execution.available_after_ms,
            consumed_after_ms=execution.consumed_after_ms,
            neo4j_total_ms=execution.neo4j_total_ms,
            row_count=(
                execution.row_count
                if execution.row_count is not None
                else len(execution.rows)
            ),
        ),
        error=None,
        relative_limit_ms=attempt.relative_limit_ms,
        relative_limit_multiplier=attempt.relative_limit_multiplier,
        timed_out=attempt.timed_out,
    )


def run_query_baseline(
    session: Any,
    query_id: int,
    parameter_sets: Sequence[dict[str, Any]],
    *,
    show_query: bool,
    fail_fast: bool,
) -> tuple[ExecutionAttempt, ...]:
    """Run one shared baseline for every strategy of a Query."""

    total_runs = len(parameter_sets)
    baseline_query = add_limit_tiebreaker(
        BENCHMARK_QUERIES[query_id],
        query_id,
    )
    if show_query:
        print("  first shared baseline query:")
        print(inline_query_parameters(baseline_query, parameter_sets[0]).strip())

    print(f"  shared Q{query_id:02d} baseline phase ({total_runs} runs)")
    attempts: list[ExecutionAttempt] = []
    for run_number, parameters in enumerate(parameter_sets, start=1):
        attempt = _attempt_query(
            session,
            baseline_query,
            parameters,
            fail_fast=fail_fast,
        )
        attempts.append(attempt)
        if attempt.execution is None:
            print(
                f"    baseline {run_number}/{total_runs}: "
                f"{attempt.error}"
            )
            continue
        execution = attempt.execution
        print(
            f"    baseline {run_number}/{total_runs}: "
            f"client={execution.client_wall_ms:.3f} ms | "
            f"neo4j={_format_ms(execution.neo4j_total_ms)} ms | "
            f"rows={len(execution.rows):,}"
        )
    return tuple(attempts)


def run_case(
    session: Any,
    case: TemplateCase,
    parameter_sets: Sequence[dict[str, Any]],
    *,
    baseline_attempts: Sequence[ExecutionAttempt],
    rel_tol: float,
    abs_tol: float,
    show_query: bool,
    fail_fast: bool,
    template_timeout_multiplier: float = DEFAULT_TEMPLATE_TIMEOUT_MULTIPLIER,
    mv_creation_wall_ms: float | None = None,
) -> CaseOutcome:
    total_runs = len(parameter_sets)
    if len(baseline_attempts) != total_runs:
        raise RuntimeError(
            f"Q{case.query_id} shared baseline has {len(baseline_attempts)} "
            f"runs; expected {total_runs}"
        )
    template_query = add_limit_tiebreaker(case.query, case.query_id)

    if show_query:
        print("  first template query:")
        print(inline_query_parameters(template_query, parameter_sets[0]).strip())

    print(f"  template phase ({total_runs} runs)")
    template_attempts: list[ExecutionAttempt] = []
    comparisons: list[ResultComparison] = []
    for run_number, parameters in enumerate(parameter_sets, start=1):
        baseline_attempt = baseline_attempts[run_number - 1]
        if baseline_attempt.execution is None:
            attempt = ExecutionAttempt(
                execution=None,
                error="not run because the matching baseline failed",
            )
        else:
            relative_limit_ms = (
                max(
                    MIN_NEO4J_QUERY_TIMEOUT_SECONDS * 1_000,
                    template_timeout_multiplier
                    * baseline_attempt.execution.client_wall_ms,
                )
                if template_timeout_multiplier > 0
                else None
            )
            attempt = _attempt_query(
                session,
                template_query,
                parameters,
                fail_fast=fail_fast,
                relative_limit_ms=relative_limit_ms,
                relative_limit_multiplier=(
                    template_timeout_multiplier
                    if relative_limit_ms is not None
                    else None
                ),
            )
        template_attempts.append(attempt)

        if baseline_attempt.execution is None:
            comparison = ResultComparison(
                equal=False,
                reason=f"baseline execution failed: {baseline_attempt.error}",
            )
        elif attempt.timed_out:
            multiplier = attempt.relative_limit_multiplier
            comparison = ResultComparison(
                equal=False,
                reason=(
                    "time limit reached: template runtime > "
                    f"{multiplier:g}* baseline; result not compared"
                ),
                censored=True,
            )
        elif attempt.execution is None:
            comparison = ResultComparison(
                equal=False,
                reason=f"template execution failed: {attempt.error}",
            )
        else:
            baseline_order_error = validate_result_order(
                baseline_attempt.execution,
                case.query_id,
            )
            template_order_error = validate_result_order(
                attempt.execution,
                case.query_id,
            )
            if baseline_order_error is not None:
                comparison = ResultComparison(
                    equal=False,
                    reason=f"baseline {baseline_order_error}",
                )
            elif template_order_error is not None:
                comparison = ResultComparison(
                    equal=False,
                    reason=f"template {template_order_error}",
                )
            else:
                comparison = compare_results(
                    baseline_attempt.execution,
                    attempt.execution,
                    rel_tol=rel_tol,
                    abs_tol=abs_tol,
                )
        comparisons.append(comparison)
        mark = "⏱️" if comparison.censored else (
            "✅" if comparison.equal else "❌"
        )

        if attempt.timed_out:
            timing = (
                f"client=>{attempt.relative_limit_ms:.3f} ms "
                f"(> {attempt.relative_limit_multiplier:g}* baseline) | "
                "neo4j=N/A | rows=N/A"
            )
        elif attempt.execution is None:
            timing = "client=N/A | neo4j=N/A | rows=N/A"
        else:
            execution = attempt.execution
            if attempt.exceeded_relative_limit:
                timing = (
                    f"client={execution.client_wall_ms:.3f} ms "
                    f"(> {attempt.relative_limit_multiplier:g}* baseline; "
                    f"limit={attempt.relative_limit_ms:.3f} ms) | "
                    f"neo4j={_format_ms(execution.neo4j_total_ms)} ms | "
                    f"rows={len(execution.rows):,}"
                )
            else:
                timing = (
                    f"client={execution.client_wall_ms:.3f} ms | "
                    f"neo4j={_format_ms(execution.neo4j_total_ms)} ms | "
                    f"rows={len(execution.rows):,}"
                )
        print(
            f"    template {run_number}/{total_runs}: {timing} | "
            f"{comparison.reason} {mark}"
        )

    (
        baseline_average,
        template_average_or_lower_bound,
        censored_timings,
        timing_pairs,
    ) = _paired_mean_times(baseline_attempts, template_attempts)
    if (
        baseline_average is not None
        and template_average_or_lower_bound is not None
    ):
        if censored_timings:
            averages = (
                f"baseline avg={baseline_average:.3f} ms | "
                f"template avg >{template_average_or_lower_bound:.3f} ms "
                f"({censored_timings}/{timing_pairs} capped at "
                f"{template_timeout_multiplier:g}* matching baseline)"
            )
        else:
            averages = (
                f"baseline avg={baseline_average:.3f} ms | "
                f"template avg={template_average_or_lower_bound:.3f} ms"
            )
    else:
        averages = "average timing unavailable"
    has_failure = any(
        not comparison.equal and not comparison.censored
        for comparison in comparisons
    )
    result_mark = "❌" if has_failure else (
        "⏱️" if censored_timings else "✅"
    )
    print(f"  result: {averages} {result_mark}")

    return CaseOutcome(
        case=case,
        comparisons=tuple(comparisons),
        baseline_attempts=tuple(
            _timing_only_attempt(attempt) for attempt in baseline_attempts
        ),
        template_attempts=tuple(
            _timing_only_attempt(attempt) for attempt in template_attempts
        ),
        mv_creation_wall_ms=mv_creation_wall_ms,
    )


def _paired_mean_times(
    baseline_attempts: Sequence[ExecutionAttempt],
    template_attempts: Sequence[ExecutionAttempt],
) -> tuple[float | None, float | None, int, int]:
    """Return exact baseline mean and an exact/censored template mean."""

    baseline_times: list[float] = []
    template_times_or_lower_bounds: list[float] = []
    censored_count = 0
    for baseline, template in zip(baseline_attempts, template_attempts):
        if baseline.execution is None:
            continue
        if template.execution is not None:
            template_time = template.execution.client_wall_ms
        elif template.timed_out and template.relative_limit_ms is not None:
            template_time = template.relative_limit_ms
            censored_count += 1
        else:
            continue
        baseline_times.append(baseline.execution.client_wall_ms)
        template_times_or_lower_bounds.append(template_time)

    if not baseline_times:
        return None, None, censored_count, 0
    return (
        fmean(baseline_times),
        fmean(template_times_or_lower_bounds),
        censored_count,
        len(baseline_times),
    )


def print_summary(outcomes: Sequence[CaseOutcome], runs: int) -> bool:
    print("\n" + "=" * 132)
    print(
        f"Summary ({runs} shared baseline runs per Query + "
        f"{runs} template runs per ready case)"
    )
    header = (
        f"{'Query':<7}"
        f"{'Template':<43}"
        f"{'Baseline avg (ms)':>19}"
        f"{'Template avg (ms)':>19}"
        f"{'Speedup':>11}"
        f"{'Pairs':>12}"
        f"{'Result':>10}"
    )
    print(header)
    print("-" * len(header))

    total_pairs = 0
    passed_pairs = 0
    total_censored_pairs = 0
    for outcome in outcomes:
        (
            baseline_average,
            template_average_or_lower_bound,
            censored_timings,
            _,
        ) = _paired_mean_times(
            outcome.baseline_attempts,
            outcome.template_attempts,
        )
        if (
            baseline_average is not None
            and template_average_or_lower_bound is not None
            and template_average_or_lower_bound > 0
        ):
            speedup_value = (
                baseline_average / template_average_or_lower_bound
            )
            speedup = (
                f"<{speedup_value:.3f}x"
                if censored_timings
                else f"{speedup_value:.3f}x"
            )
            template_timing = (
                f">{template_average_or_lower_bound:.3f}"
                if censored_timings
                else f"{template_average_or_lower_bound:.3f}"
            )
        else:
            speedup = "N/A"
            template_timing = "N/A"

        completed_comparisons = [
            comparison
            for comparison in outcome.comparisons
            if not comparison.censored
        ]
        pair_count = len(completed_comparisons)
        pair_passes = sum(
            comparison.equal for comparison in completed_comparisons
        )
        censored_pair_count = sum(
            comparison.censored for comparison in outcome.comparisons
        )
        total_pairs += pair_count
        passed_pairs += pair_passes
        total_censored_pairs += censored_pair_count
        if censored_pair_count:
            pair_text = (
                f"{pair_passes}/{pair_count} + {censored_pair_count} cap"
            )
        elif pair_count:
            pair_text = f"{pair_passes}/{pair_count}"
        else:
            pair_text = "not run"
        result_mark = "❌" if outcome.failed else (
            "⏱️" if outcome.censored else "✅"
        )
        print(
            f"{f'Q{outcome.case.query_id:02d}':<7}"
            f"{outcome.case.display_name:<43}"
            f"{_format_ms(baseline_average):>19}"
            f"{template_timing:>19}"
            f"{speedup:>11}"
            f"{pair_text:>12}"
            f"{result_mark:>10}"
        )
        if outcome.setup_error is not None:
            print(f"       setup: {outcome.setup_error}")

    failed_pairs = total_pairs - passed_pairs
    failed_cases = sum(outcome.failed for outcome in outcomes)
    censored_cases = sum(
        outcome.censored and not outcome.failed for outcome in outcomes
    )
    print("-" * len(header))
    print(
        f"Compared pairs: {total_pairs}; passed: {passed_pairs}; "
        f"failed: {failed_pairs}; capped/not-compared: "
        f"{total_censored_pairs}; failed/not-run templates: "
        f"{failed_cases}/{len(outcomes)}; capped templates: "
        f"{censored_cases}/{len(outcomes)} "
        f"{'✅' if failed_cases == 0 else '❌'}"
    )
    return failed_cases == 0


def _attempt_json(attempt: ExecutionAttempt | None) -> dict[str, Any]:
    if attempt is None:
        return {
            "status": "not_run",
            "client_time_ms": None,
            "neo4j_time_ms": None,
            "result_rows": None,
        }

    execution = attempt.execution
    if attempt.timed_out:
        status = "capped"
    elif execution is not None and attempt.exceeded_relative_limit:
        status = "completed_over_limit"
    elif execution is not None:
        status = "completed"
    elif attempt.error is not None and attempt.error.startswith(
        "not run because "
    ):
        status = "not_run"
    elif attempt.error is not None:
        status = "error"
    else:
        status = "not_run"

    result: dict[str, Any] = {
        "status": status,
        "client_time_ms": (
            execution.client_wall_ms if execution is not None else None
        ),
        "neo4j_time_ms": (
            execution.neo4j_total_ms if execution is not None else None
        ),
        "result_rows": (
            execution.row_count if execution is not None else None
        ),
    }
    if attempt.timed_out:
        result["client_time_lower_bound_ms"] = attempt.relative_limit_ms
        result["limit_multiplier"] = attempt.relative_limit_multiplier
    if attempt.error is not None:
        result["error"] = attempt.error
    return result


def case_result_rows(
    outcome: CaseOutcome,
    parameter_sets: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    expected_runs = len(parameter_sets)
    run_lengths = {
        "baseline attempts": len(outcome.baseline_attempts),
        "template attempts": len(outcome.template_attempts),
        "comparisons": len(outcome.comparisons),
    }
    invalid_lengths = {
        name: length
        for name, length in run_lengths.items()
        if length not in (0, expected_runs)
    }
    if invalid_lengths:
        raise RuntimeError(
            f"{outcome.case.strategy_name} JSON run counts do not match "
            f"its {expected_runs} parameter sets: {invalid_lengths}"
        )

    rows: list[dict[str, Any]] = []
    for index, parameters in enumerate(parameter_sets):
        baseline_attempt = (
            outcome.baseline_attempts[index]
            if outcome.baseline_attempts
            else None
        )
        template_attempt = (
            outcome.template_attempts[index]
            if outcome.template_attempts
            else None
        )
        rows.append(
            {
                "query_id": outcome.case.query_id,
                "template": outcome.case.display_name,
                "strategy": outcome.case.strategy_name,
                "run_number": index + 1,
                "parameters": dict(parameters),
                "mv_create_time_ms": outcome.mv_creation_wall_ms,
                "baseline": _attempt_json(baseline_attempt),
                "template_result": _attempt_json(template_attempt),
            }
        )
    return rows


def build_json_report(
    plan: BenchmarkPlan,
    outcomes: Sequence[CaseOutcome],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        rows.extend(
            case_result_rows(
                outcome,
                plan.parameters_by_query[outcome.case.query_id],
            )
        )
    return rows


def write_json_report(
    path: Path,
    report: Sequence[Mapping[str, Any]],
) -> Path:
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    lines = ["["]
    for index, row in enumerate(report):
        suffix = "," if index + 1 < len(report) else ""
        lines.append(
            "  "
            + json.dumps(
                row,
                ensure_ascii=False,
                allow_nan=False,
            )
            + suffix
        )
    lines.append("]")
    resolved.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
    return resolved


def finish_benchmark(
    args: argparse.Namespace,
    plan: BenchmarkPlan,
    outcomes: Sequence[CaseOutcome],
) -> bool:
    all_passed = print_summary(outcomes, args.runs)
    output_json = getattr(args, "output_json", None)
    if output_json is not None:
        output = write_json_report(
            output_json,
            build_json_report(plan, outcomes),
        )
        print(f"JSON report: {output}")
    return all_passed


def prepare_benchmark_plan(args: argparse.Namespace) -> BenchmarkPlan:
    all_cases = load_template_cases(
        args.templates_dir,
        require_complete=getattr(
            args,
            "require_complete_template_set",
            False,
        ),
    )
    cases = select_template_cases(
        all_cases,
        args.query_id,
        args.strategy,
    )
    query_ids = sorted({case.query_id for case in cases})
    parameters_by_query = {
        query_id: generate_parameter_sets(
            query_id,
            args.seed,
            args.sf,
            args.runs,
        )
        for query_id in query_ids
    }

    return BenchmarkPlan(
        all_cases=tuple(all_cases),
        cases=tuple(cases),
        query_ids=tuple(query_ids),
        parameters_by_query=parameters_by_query,
    )


def print_benchmark_plan(args: argparse.Namespace, plan: BenchmarkPlan) -> None:
    lifecycle = (
        "reuse existing MVs"
        if getattr(args, "reuse_existing_mvs", True)
        else (
            f"rotate by {args.rotation_scope} (create -> benchmark -> delete); "
            f"initial cleanup={args.initial_cleanup_scope}"
        )
    )

    print(
        f"Connecting to {args.uri} as {args.user!r}; "
        f"database={args.database!r}"
    )
    print(
        f"Loaded {len(plan.all_cases)} template modules from "
        f"{args.templates_dir}; selected {len(plan.cases)}; "
        f"runs={args.runs}; seed={args.seed}; sf={args.sf}"
    )
    print(f"MV lifecycle: {lifecycle}")
    if getattr(args, "reuse_existing_mvs", True):
        print("Relationship inheritance: use the policy stored on each existing MV")
    elif getattr(args, "inheritance_preflight", False):
        print("Relationship inheritance: Phase 2 fanout preflight enabled")
    else:
        print(
            "Relationship inheritance: Phase 2 fanout preflight skipped "
            "(validated boundary types plus structural hard blocks)"
        )
    print(
        "All templates for one Query share the same baseline executions and "
        "generated parameters. Rows are compared as a multiset."
    )
    print(
        f"Float comparison: rel_tol={args.rel_tol:g}, "
        f"abs_tol={args.abs_tol:g}"
    )
    template_timeout_multiplier = getattr(
        args,
        "template_timeout_multiplier",
        DEFAULT_TEMPLATE_TIMEOUT_MULTIPLIER,
    )
    if template_timeout_multiplier > 0:
        print(
            "Template time limit: each run is capped at "
            f"{template_timeout_multiplier:g}* its matching baseline client "
            "time (server-side transaction timeout; enforcement is approximate)."
        )
    else:
        print("Template time limit: disabled")
    if any(
        query_id in LIMIT_TIEBREAKER_BY_QUERY
        for query_id in plan.query_ids
    ):
        print(
            "Q3/Q10/Q18 use the same stable primary-key tiebreaker in the "
            "baseline and template to make LIMIT deterministic."
        )


def _not_run_outcome(
    case: TemplateCase,
    preflight: StrategyPreflight,
    mv_creation_wall_ms: float | None = None,
) -> CaseOutcome:
    setup_error = f"{preflight.status}: {preflight.detail}"
    print(f"  not run: {setup_error} ❌")
    return CaseOutcome(
        case=case,
        comparisons=(),
        baseline_attempts=(),
        template_attempts=(),
        setup_error=setup_error,
        mv_creation_wall_ms=mv_creation_wall_ms,
    )


def run_selected_cases(
    session: Any,
    args: argparse.Namespace,
    plan: BenchmarkPlan,
    cases: Sequence[TemplateCase],
    outcomes: list[CaseOutcome],
    position_by_strategy: Mapping[str, int],
    *,
    baseline_attempts: Sequence[ExecutionAttempt],
    mv_creation_wall_ms_by_strategy: Mapping[str, float] | None = None,
) -> None:
    """Preflight and benchmark one already-materialized case group."""

    print("Checking strategy metadata and materialized node counts...")
    preflights = preflight_template_cases(
        session,
        cases,
        expected_inheritance_modes(args),
    )
    invalid_cases = [
        case
        for case in cases
        if not preflights[case.strategy_name].ready
    ]
    if args.require_all_ready and invalid_cases:
        details = "; ".join(
            f"{case.strategy_name}: {preflights[case.strategy_name].detail}"
            for case in invalid_cases
        )
        raise RuntimeError(
            f"{len(invalid_cases)} selected strategies are not ready: {details}"
        )

    for case in cases:
        mv_creation_wall_ms = (
            mv_creation_wall_ms_by_strategy.get(case.strategy_name)
            if mv_creation_wall_ms_by_strategy is not None
            else None
        )
        index = position_by_strategy[case.strategy_name]
        print("\n" + "=" * 132)
        print(
            f"[{index}/{len(plan.cases)}] Q{case.query_id:02d} "
            f"{case.display_name} | strategy={case.strategy_name}"
        )
        preflight = preflights[case.strategy_name]
        if not preflight.ready:
            outcomes.append(
                _not_run_outcome(
                    case,
                    preflight,
                    mv_creation_wall_ms,
                )
            )
            continue

        print(f"  preflight: {preflight.detail}")
        try:
            outcome = run_case(
                session,
                case,
                plan.parameters_by_query[case.query_id],
                baseline_attempts=baseline_attempts,
                rel_tol=args.rel_tol,
                abs_tol=args.abs_tol,
                show_query=args.show_query,
                fail_fast=args.fail_fast,
                template_timeout_multiplier=getattr(
                    args,
                    "template_timeout_multiplier",
                    DEFAULT_TEMPLATE_TIMEOUT_MULTIPLIER,
                ),
                mv_creation_wall_ms=mv_creation_wall_ms,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Q{case.query_id} {case.display_name} failed"
            ) from exc
        outcomes.append(outcome)


def run_existing_benchmark(
    args: argparse.Namespace,
    password: str,
    plan: BenchmarkPlan,
) -> bool:
    try:
        from neo4j import READ_ACCESS, GraphDatabase
    except ImportError as exc:
        raise RuntimeError(
            "The Neo4j Python driver is not installed. "
            "Install it with: pip install neo4j"
        ) from exc

    outcomes: list[CaseOutcome] = []
    position_by_strategy = {
        case.strategy_name: index
        for index, case in enumerate(plan.cases, start=1)
    }
    with GraphDatabase.driver(args.uri, auth=(args.user, password)) as driver:
        driver.verify_connectivity()
        with driver.session(
            database=args.database,
            default_access_mode=READ_ACCESS,
        ) as session:
            for query_cases in rotation_groups(plan.cases, "query"):
                query_id = query_cases[0].query_id
                baseline_attempts = run_query_baseline(
                    session,
                    query_id,
                    plan.parameters_by_query[query_id],
                    show_query=args.show_query,
                    fail_fast=args.fail_fast,
                )
                run_selected_cases(
                    session,
                    args,
                    plan,
                    query_cases,
                    outcomes,
                    position_by_strategy,
                    baseline_attempts=baseline_attempts,
                )

    return finish_benchmark(args, plan, outcomes)


def rotation_groups(
    cases: Sequence[TemplateCase],
    scope: str,
) -> list[tuple[TemplateCase, ...]]:
    if scope == "strategy":
        return [(case,) for case in cases]
    if scope != "query":
        raise RuntimeError(f"unknown MV rotation scope: {scope!r}")

    groups: list[tuple[TemplateCase, ...]] = []
    current: list[TemplateCase] = []
    current_query_id: int | None = None
    for case in cases:
        if current and case.query_id != current_query_id:
            groups.append(tuple(current))
            current = []
        current.append(case)
        current_query_id = case.query_id
    if current:
        groups.append(tuple(current))
    return groups


def _format_cleanup_counts(counts: tuple[int, int, int]) -> str:
    deleted_rows, deleted_strategies, dropped_indexes = counts
    return (
        f"{deleted_rows:,} materialized nodes, "
        f"{deleted_strategies:,} strategy nodes, "
        f"{dropped_indexes:,} indexes"
    )


def run_rotating_benchmark(
    args: argparse.Namespace,
    password: str,
    plan: BenchmarkPlan,
) -> bool:
    try:
        from neo4j import GraphDatabase, WRITE_ACCESS
    except ImportError as exc:
        raise RuntimeError(
            "The Neo4j Python driver is not installed. "
            "Install it with: pip install neo4j"
        ) from exc

    strategy_names = {case.strategy_name for case in plan.cases}
    selected_strategies = auto_join_mv.select_strategies(
        selected_strategy_names=strategy_names,
    )
    strategy_by_name = {
        strategy.name: strategy for strategy in selected_strategies
    }
    groups = rotation_groups(plan.cases, args.rotation_scope)
    position_by_strategy = {
        case.strategy_name: index
        for index, case in enumerate(plan.cases, start=1)
    }
    outcomes: list[CaseOutcome] = []
    baseline_query_id: int | None = None
    baseline_attempts: tuple[ExecutionAttempt, ...] = ()

    with GraphDatabase.driver(args.uri, auth=(args.user, password)) as driver:
        driver.verify_connectivity()
        with driver.session(
            database=args.database,
            default_access_mode=WRITE_ACCESS,
        ) as session:
            source_counts, relationship_counts = (
                auto_join_mv.validate_source_graph(session)
            )
            print(
                "Source nodes: "
                + ", ".join(
                    f"{label}={count:,}" for label, count in source_counts.items()
                )
            )
            print(
                "Source relationships: "
                + ", ".join(
                    f"{relationship_type}={count:,}"
                    for relationship_type, count in relationship_counts.items()
                )
            )
            initial_strategies = (
                selected_strategies
                if args.initial_cleanup_scope == "selected"
                else None
            )
            print(
                "Checking for existing generated AUTO_JOIN artifacts "
                f"({args.initial_cleanup_scope}; source TPC-H nodes are "
                "never touched)..."
            )
            initial_presence = auto_join_mv.inspect_generated_artifacts(
                session,
                None,
                initial_strategies,
            )
            if not initial_presence.any:
                print("Initial cleanup skipped: no generated artifacts found.")
            else:
                found_kinds = []
                if initial_presence.materialized_nodes:
                    found_kinds.append("materialized nodes")
                if initial_presence.strategy_nodes:
                    found_kinds.append("strategy metadata")
                if initial_presence.indexes:
                    found_kinds.append("indexes")
                print(
                    "Existing generated artifacts found: "
                    + ", ".join(found_kinds)
                    + "; cleaning..."
                )
                initial_cleanup = auto_join_mv.cleanup_generated_artifacts(
                    session,
                    args.batch_size,
                    None,
                    initial_strategies,
                    known_presence=initial_presence,
                )
                print(
                    "Initial cleanup: "
                    f"{_format_cleanup_counts(initial_cleanup)}"
                )

            for group_number, group_cases in enumerate(groups, start=1):
                group_strategies = tuple(
                    strategy_by_name[case.strategy_name]
                    for case in group_cases
                )
                query_ids = {case.query_id for case in group_cases}
                if len(query_ids) != 1:
                    raise RuntimeError(
                        "an MV rotation group must contain exactly one Query"
                    )
                query_id = next(iter(query_ids))
                query_text = f"Q{query_id}"
                print("\n" + "#" * 132)
                print(
                    f"MV rotation group {group_number}/{len(groups)}: "
                    f"{query_text}; strategies={len(group_strategies)}"
                )

                if query_id != baseline_query_id:
                    baseline_attempts = run_query_baseline(
                        session,
                        query_id,
                        plan.parameters_by_query[query_id],
                        show_query=args.show_query,
                        fail_fast=args.fail_fast,
                    )
                    baseline_query_id = query_id

                active_error: BaseException | None = None
                try:
                    summary = auto_join_mv.materialize_strategies_in_session(
                        session,
                        args,
                        group_strategies,
                        relationship_counts,
                    )
                    print(
                        "Materialization result: "
                        f"ready={summary.ready}, skipped={summary.skipped}, "
                        f"nodes={summary.materialized_nodes:,}, "
                        "inherited_relationships="
                        f"{summary.inherited_relationships:,}, "
                        f"indexes={summary.indexes:,}, "
                        "creation_time="
                        f"{summary.creation_wall_ms / 1_000:.3f} s"
                    )
                    run_selected_cases(
                        session,
                        args,
                        plan,
                        group_cases,
                        outcomes,
                        position_by_strategy,
                        baseline_attempts=baseline_attempts,
                        mv_creation_wall_ms_by_strategy=dict(
                            summary.creation_wall_ms_by_strategy
                        ),
                    )
                except BaseException as exc:
                    active_error = exc
                    raise
                finally:
                    try:
                        cleanup = auto_join_mv.cleanup_generated_artifacts(
                            session,
                            args.batch_size,
                            None,
                            group_strategies,
                        )
                    except Exception as cleanup_exc:
                        if active_error is None:
                            raise
                        note = (
                            "cleanup also failed for "
                            f"{', '.join(strategy.name for strategy in group_strategies)}: "
                            f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                        )
                        active_error.add_note(note)
                        print(f"WARNING: {note}", file=sys.stderr)
                    else:
                        print(
                            "Post-benchmark cleanup: "
                            f"{_format_cleanup_counts(cleanup)}"
                        )

    return finish_benchmark(args, plan, outcomes)


def run_benchmark(args: argparse.Namespace, password: str) -> bool:
    plan = prepare_benchmark_plan(args)
    print_benchmark_plan(args, plan)
    if getattr(args, "reuse_existing_mvs", True):
        return run_existing_benchmark(args, password, plan)
    return run_rotating_benchmark(args, password, plan)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        password = os.getenv("NEO4J_PASSWORD") or DEBUG_NEO4J_PASSWORD
        if password is None:
            password = getpass(f"Neo4j password for {args.user}: ")
        all_passed = run_benchmark(args, password)
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        cause = exc.__cause__
        if cause is not None:
            print(f"CAUSE: {cause}", file=sys.stderr)
        return 1
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
