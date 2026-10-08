#!/usr/bin/env python3
"""Analyse the Northwind FD-normalisation benchmark.

The input is the v6 JSON report written by
``supplier_product_fd_experiment.py``.  This program validates the report,
recomputes its headline metrics, creates reusable CSV/JSON summaries, draws
publication-ready figures, and writes English Markdown reports.

Default usage (inside the ``graph_database_denormalization`` Conda env)::

    python extend_experiment/northwind/analysis/analysis.py

Use ``--help`` to select another input, output directory, or focus case.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent
from typing import Any, Sequence

_cache_root = Path(tempfile.gettempdir()) / "northwind_fd_analysis_cache"
_cache_root.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_cache_root / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(_cache_root))

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.lines import Line2D
    from matplotlib.patches import FancyBboxPatch
except ImportError as exc:  # pragma: no cover - only used for a bad environment
    raise SystemExit(
        "Matplotlib and NumPy are required. Install them with:\n"
        "  conda install -n graph_database_denormalization matplotlib numpy"
    ) from exc


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / "northwind_fd_experiment_results.json"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "output"
SUPPORTED_REPORT_SCHEMA = 6
DEFAULT_BOOTSTRAP_SEED = 20260729
DEFAULT_BOOTSTRAP_RESAMPLES = 10_000

COLORS = {
    "folded": "#D55E00",
    "normalized": "#0072B2",
    "normalization": "#7B61A8",
    "positive": "#009E73",
    "negative": "#C43C39",
    "focus": "#E69F00",
    "neutral": "#7D8790",
    "grid": "#D9E1E8",
    "ink": "#23384D",
    "muted": "#66788A",
    "panel": "#F5F8FA",
}

CASE_CODES = {
    "order_customer": "OC",
    "order_detail_order": "ODO",
    "order_detail_product": "ODP",
    "product_category": "PC",
    "product_supplier": "PS",
    "order_employee": "OE",
    "order_shipper": "OS",
    "employee_territory_employee": "ETE",
    "employee_territory_territory": "ETT",
    "territory_region": "TR",
}

PHASES = ("folded_update", "normalized_update", "normalization")


class AnalysisError(RuntimeError):
    """The source report is missing data or violates an experiment invariant."""


@dataclass(frozen=True)
class Timing:
    n: int
    mean_ms: float
    median_ms: float
    stdev_ms: float
    min_ms: float
    q1_ms: float
    q3_ms: float
    p95_ms: float
    max_ms: float
    samples_ms: tuple[float, ...]
    bootstrap_median_low_ms: float = field(default=math.nan)
    bootstrap_median_high_ms: float = field(default=math.nan)


@dataclass(frozen=True)
class CaseResult:
    case_id: str
    code: str
    referencing_table: str
    referenced_table: str
    pair_label: str
    update_property: str
    update_value_type: str
    logical_updates: int
    active_domain_size: int
    join_rows_j: int
    participating_referenced_rows_d: int
    full_referenced_rows: int
    unused_referenced_rows: int
    redundant_tuples: int
    redundant_tuple_pct: float
    dependent_property_count: int
    redundant_property_slots: int
    total_non_null_values: int
    redundant_non_null_values: int
    redundant_non_null_pct: float
    total_payload_bytes: int
    redundant_payload_bytes: int
    redundant_payload_pct: float
    topology_folded_relationships: int
    topology_normalized_relationships: int
    topology_redundant_relationships: int
    folded_writes: int
    normalized_writes: int
    write_amplification: float
    folded_update: Timing
    normalized_update: Timing
    normalization: Timing
    update_time_ratio: float
    update_time_saved_ms: float
    normalized_time_reduction_pct: float
    break_even_batches: float | None
    fanout_histogram: tuple[tuple[int, int], ...]
    folded_nodes: int
    normalized_referencing_nodes: int
    normalized_referenced_nodes: int
    normalized_join_relationships: int
    run_id: str
    join_sha256: str
    source_graph_sha256: str


@dataclass(frozen=True)
class SuiteSummary:
    case_count: int
    cases_with_tuple_redundancy: int
    cases_with_lower_normalized_median: int
    total_join_rows: int
    total_participating_referenced_rows: int
    total_redundant_tuples: int
    weighted_redundancy_pct: float
    total_redundant_property_slots: int
    total_redundant_non_null_values: int
    total_redundant_payload_bytes: int
    total_topology_redundant_relationships: int
    median_case_update_ratio: float
    median_case_normalization_ms: float


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AnalysisError(message)


def get_path(record: dict[str, Any], *keys: str) -> Any:
    value: Any = record
    walked: list[str] = []
    for key in keys:
        walked.append(key)
        if not isinstance(value, dict) or key not in value:
            raise AnalysisError(f"Missing required field: {'.'.join(walked)}")
        value = value[key]
    return value


def as_finite_float(value: Any, field_name: str) -> float:
    require(
        isinstance(value, (int, float)) and not isinstance(value, bool),
        f"{field_name} must be numeric",
    )
    result = float(value)
    require(math.isfinite(result), f"{field_name} must be finite")
    return result


def nearest_rank(values: Sequence[float], probability: float) -> float:
    require(bool(values), "Cannot calculate a percentile from no samples")
    ordered = sorted(values)
    rank = max(1, math.ceil(probability * len(ordered)))
    return ordered[rank - 1]


def bootstrap_median_interval(
    samples: Sequence[float],
    *,
    seed: int,
    resamples: int,
) -> tuple[float, float]:
    """Return a deterministic percentile bootstrap interval for the median.

    This is a descriptive uncertainty interval for repeated timings.  It is not
    interpreted as population inference over independent database deployments.
    """

    require(resamples > 0, "bootstrap resamples must be positive")
    values = np.asarray(samples, dtype=float)
    rng = np.random.default_rng(seed)
    draws = rng.choice(values, size=(resamples, len(values)), replace=True)
    medians = np.median(draws, axis=1)
    low, high = np.percentile(medians, [2.5, 97.5], method="linear")
    return float(low), float(high)


def parse_timing(
    case_record: dict[str, Any],
    phase: str,
    *,
    bootstrap_seed: int,
    bootstrap_resamples: int,
) -> Timing:
    samples = get_path(case_record, "timings", phase, "samples")
    summary = get_path(
        case_record, "timings", phase, "summary", "client_wall_ms"
    )
    require(isinstance(samples, list) and samples, f"{phase}.samples must be non-empty")
    values = tuple(
        as_finite_float(
            get_path(sample, "client_wall_ms"),
            f"{phase}.samples[{index}].client_wall_ms",
        )
        for index, sample in enumerate(samples)
    )
    n = int(get_path(summary, "n"))
    mean = as_finite_float(get_path(summary, "mean"), f"{phase}.mean")
    median = as_finite_float(get_path(summary, "median"), f"{phase}.median")
    stdev = as_finite_float(get_path(summary, "stdev"), f"{phase}.stdev")
    minimum = as_finite_float(get_path(summary, "min"), f"{phase}.min")
    p95 = as_finite_float(get_path(summary, "p95"), f"{phase}.p95")
    maximum = as_finite_float(get_path(summary, "max"), f"{phase}.max")

    require(n == len(values), f"{phase}: summary n does not match samples")
    require(
        math.isclose(mean, statistics.fmean(values), rel_tol=1e-10, abs_tol=1e-10),
        f"{phase}: stored mean does not match raw samples",
    )
    require(
        math.isclose(median, statistics.median(values), rel_tol=1e-10, abs_tol=1e-10),
        f"{phase}: stored median does not match raw samples",
    )
    require(
        math.isclose(stdev, statistics.stdev(values), rel_tol=1e-10, abs_tol=1e-10),
        f"{phase}: stored sample stdev does not match raw samples",
    )
    require(math.isclose(minimum, min(values)), f"{phase}: stored min mismatch")
    require(math.isclose(maximum, max(values)), f"{phase}: stored max mismatch")
    require(
        math.isclose(p95, nearest_rank(values, 0.95)),
        f"{phase}: stored p95 does not use the expected nearest-rank result",
    )
    low, high = bootstrap_median_interval(
        values,
        seed=bootstrap_seed,
        resamples=bootstrap_resamples,
    )
    return Timing(
        n=n,
        mean_ms=mean,
        median_ms=median,
        stdev_ms=stdev,
        min_ms=minimum,
        q1_ms=float(np.percentile(values, 25, method="linear")),
        q3_ms=float(np.percentile(values, 75, method="linear")),
        p95_ms=p95,
        max_ms=maximum,
        samples_ms=values,
        bootstrap_median_low_ms=low,
        bootstrap_median_high_ms=high,
    )


def validate_summary_mutation_count(
    case_record: dict[str, Any],
    phase: str,
    field_name: str,
    expected: int,
) -> None:
    values = get_path(case_record, "timings", phase, "summary", field_name)
    require(
        values == [expected],
        f"{case_record.get('case_id')}.{phase}.{field_name}: "
        f"expected [{expected}], found {values!r}",
    )


def parse_case(
    record: dict[str, Any],
    *,
    case_index: int,
    bootstrap_seed: int,
    bootstrap_resamples: int,
) -> CaseResult:
    case_id = str(get_path(record, "case_id"))
    pair = get_path(record, "pair")
    referencing = str(get_path(pair, "referencing_table", "label"))
    referenced = str(get_path(pair, "referenced_table", "label"))
    redundancy = get_path(record, "redundancy")
    topology = get_path(redundancy, "topology")
    configuration = get_path(record, "configuration")
    update_payload = get_path(record, "update_payload")
    derived = get_path(record, "derived")

    phase_seed_base = bootstrap_seed + case_index * 101
    folded = parse_timing(
        record,
        "folded_update",
        bootstrap_seed=phase_seed_base,
        bootstrap_resamples=bootstrap_resamples,
    )
    normalized = parse_timing(
        record,
        "normalized_update",
        bootstrap_seed=phase_seed_base + 1,
        bootstrap_resamples=bootstrap_resamples,
    )
    normalization = parse_timing(
        record,
        "normalization",
        bootstrap_seed=phase_seed_base + 2,
        bootstrap_resamples=bootstrap_resamples,
    )

    # ``denormalized_update`` is a backwards-compatible alias, not another
    # experiment phase.  Assert equality so it can never be double counted.
    require(
        get_path(record, "timings", "denormalized_update")
        == get_path(record, "timings", "folded_update"),
        f"{case_id}: denormalized_update alias differs from folded_update",
    )

    join_rows = int(get_path(redundancy, "join_rows_J"))
    participating = int(
        get_path(redundancy, "participating_referenced_rows_D")
    )
    full_referenced = int(get_path(redundancy, "full_referenced_rows"))
    redundant = int(get_path(redundancy, "redundant_referenced_tuple_copies"))
    dependent_count = int(get_path(redundancy, "dependent_property_count"))
    slots = int(
        get_path(
            redundancy, "theoretical_redundant_dependent_property_slots"
        )
    )
    folded_writes = int(
        get_path(update_payload, "folded_join_fanout_weighted", "property_writes")
    )
    normalized_writes = int(
        get_path(update_payload, "normalized_logical", "property_writes")
    )
    ratio = as_finite_float(
        get_path(derived, "folded_to_normalized_update_median_ratio"),
        f"{case_id}.update ratio",
    )
    saved_ms = as_finite_float(
        get_path(derived, "median_client_ms_saved_per_logical_update_batch"),
        f"{case_id}.saved time",
    )
    raw_break_even = derived.get(
        "data_rewrite_normalization_break_even_update_batches"
    )
    break_even = (
        None
        if raw_break_even is None
        else as_finite_float(raw_break_even, f"{case_id}.break-even")
    )

    histogram_raw = get_path(redundancy, "referenced_fanout_histogram")
    require(isinstance(histogram_raw, dict), f"{case_id}: fanout must be an object")
    histogram = tuple(
        sorted((int(fanout), int(count)) for fanout, count in histogram_raw.items())
    )

    source_before = str(get_path(record, "dataset", "source_graph_sha256_before"))
    source_during = str(get_path(record, "dataset", "source_graph_sha256_during"))
    source_after = str(get_path(record, "dataset", "source_graph_sha256_after"))

    result = CaseResult(
        case_id=case_id,
        code=CASE_CODES.get(case_id, case_id.upper()),
        referencing_table=referencing,
        referenced_table=referenced,
        pair_label=f"{referencing} \N{RIGHTWARDS ARROW} {referenced}",
        update_property=str(get_path(configuration, "update_property")),
        update_value_type=str(get_path(configuration, "update_value_type")),
        logical_updates=int(get_path(configuration, "logical_update_tuple_count")),
        active_domain_size=int(get_path(configuration, "active_domain_size")),
        join_rows_j=join_rows,
        participating_referenced_rows_d=participating,
        full_referenced_rows=full_referenced,
        unused_referenced_rows=int(get_path(redundancy, "unused_referenced_rows")),
        redundant_tuples=redundant,
        redundant_tuple_pct=as_finite_float(
            get_path(
                redundancy,
                "redundant_referenced_tuple_percentage_of_join",
            ),
            f"{case_id}.redundant tuple percentage",
        ),
        dependent_property_count=dependent_count,
        redundant_property_slots=slots,
        total_non_null_values=int(
            get_path(
                redundancy,
                "total_non_null_dependent_values_in_folded_join",
            )
        ),
        redundant_non_null_values=int(
            get_path(redundancy, "redundant_non_null_dependent_values")
        ),
        redundant_non_null_pct=as_finite_float(
            get_path(redundancy, "redundant_non_null_value_percentage"),
            f"{case_id}.non-null redundancy percentage",
        ),
        total_payload_bytes=int(
            get_path(redundancy, "total_value_payload_bytes_in_folded_join")
        ),
        redundant_payload_bytes=int(
            get_path(redundancy, "redundant_value_payload_bytes")
        ),
        redundant_payload_pct=as_finite_float(
            get_path(redundancy, "redundant_value_payload_percentage"),
            f"{case_id}.payload redundancy percentage",
        ),
        topology_folded_relationships=int(
            get_path(topology, "folded_boundary_relationships")
        ),
        topology_normalized_relationships=int(
            get_path(topology, "normalized_boundary_relationships")
        ),
        topology_redundant_relationships=int(
            get_path(
                topology, "redundant_folded_boundary_relationship_copies"
            )
        ),
        folded_writes=folded_writes,
        normalized_writes=normalized_writes,
        write_amplification=as_finite_float(
            get_path(derived, "write_amplification_nodes"),
            f"{case_id}.write amplification",
        ),
        folded_update=folded,
        normalized_update=normalized,
        normalization=normalization,
        update_time_ratio=ratio,
        update_time_saved_ms=saved_ms,
        normalized_time_reduction_pct=(
            1.0 - normalized.median_ms / folded.median_ms
        )
        * 100.0,
        break_even_batches=break_even,
        fanout_histogram=histogram,
        folded_nodes=int(
            get_path(record, "folded_shadow_state", "folded_join_nodes")
        ),
        normalized_referencing_nodes=int(
            get_path(
                record,
                "normalized_shadow_state",
                "normalized_referencing_nodes",
            )
        ),
        normalized_referenced_nodes=int(
            get_path(
                record,
                "normalized_shadow_state",
                "normalized_referenced_nodes",
            )
        ),
        normalized_join_relationships=int(
            get_path(
                record,
                "normalized_shadow_state",
                "normalized_join_relationships",
            )
        ),
        run_id=str(get_path(record, "run_id")),
        join_sha256=str(get_path(record, "dataset", "join_sha256")),
        source_graph_sha256=source_before,
    )

    require(join_rows >= participating > 0, f"{case_id}: require J >= D > 0")
    require(
        full_referenced >= participating,
        f"{case_id}: full referenced rows must be at least D",
    )
    require(
        redundant == join_rows - participating,
        f"{case_id}: tuple redundancy must equal J-D",
    )
    require(
        slots == redundant * dependent_count,
        f"{case_id}: slots must equal (J-D)*dependent_property_count",
    )
    expected_pct = redundant / join_rows * 100.0
    require(
        math.isclose(
            result.redundant_tuple_pct,
            expected_pct,
            rel_tol=1e-10,
            abs_tol=1e-10,
        ),
        f"{case_id}: tuple redundancy percentage mismatch",
    )
    require(
        sum(count for _, count in histogram) == participating,
        f"{case_id}: fanout frequencies must sum to D",
    )
    require(
        sum(fanout * count for fanout, count in histogram) == join_rows,
        f"{case_id}: weighted fanout frequencies must sum to J",
    )
    require(
        folded_writes == join_rows and normalized_writes == participating,
        f"{case_id}: update property writes must be J folded and D normalized",
    )
    require(
        math.isclose(
            result.write_amplification,
            join_rows / participating,
            rel_tol=1e-10,
        ),
        f"{case_id}: write amplification must equal J/D",
    )
    require(
        math.isclose(
            ratio,
            folded.median_ms / normalized.median_ms,
            rel_tol=1e-10,
        ),
        f"{case_id}: update median ratio mismatch",
    )
    require(
        math.isclose(
            saved_ms,
            folded.median_ms - normalized.median_ms,
            rel_tol=1e-10,
            abs_tol=1e-10,
        ),
        f"{case_id}: median time saving mismatch",
    )
    expected_break_even = (
        normalization.median_ms / saved_ms
        if redundant > 0 and saved_ms > 0
        else None
    )
    if expected_break_even is None:
        require(break_even is None, f"{case_id}: break-even must be null")
    else:
        require(
            break_even is not None
            and math.isclose(
                break_even, expected_break_even, rel_tol=1e-10
            ),
            f"{case_id}: break-even mismatch",
        )
    require(
        source_before == source_during == source_after,
        f"{case_id}: original graph fingerprint changed",
    )
    require(
        bool(get_path(record, "cleanup", "original_graph_verified_unchanged")),
        f"{case_id}: cleanup did not verify the original graph",
    )
    require(
        get_path(record, "cleanup", "final_shadow_state") == "clean",
        f"{case_id}: shadow data was not cleaned",
    )
    require(
        bool(get_path(record, "cleanup", "experiment_owned_schema_dropped")),
        f"{case_id}: temporary schema was not dropped",
    )
    require(
        get_path(record, "final_state", "original_graph") == "unchanged"
        and get_path(record, "final_state", "shadow_graph") == "removed",
        f"{case_id}: unexpected final state",
    )
    validate_summary_mutation_count(
        record, "folded_update", "affected_nodes", join_rows
    )
    validate_summary_mutation_count(
        record, "folded_update", "properties_set", join_rows
    )
    validate_summary_mutation_count(
        record, "normalized_update", "affected_nodes", participating
    )
    validate_summary_mutation_count(
        record, "normalized_update", "properties_set", participating
    )
    require(
        result.topology_folded_relationships
        - result.topology_normalized_relationships
        == result.topology_redundant_relationships,
        f"{case_id}: topology redundancy mismatch",
    )
    require(
        sum(
            int(boundary["folded_count"])
            for boundary in get_path(topology, "by_boundary")
        )
        == result.topology_folded_relationships,
        f"{case_id}: folded topology boundary sum mismatch",
    )
    require(
        sum(
            int(boundary["normalized_count"])
            for boundary in get_path(topology, "by_boundary")
        )
        == result.topology_normalized_relationships,
        f"{case_id}: normalized topology boundary sum mismatch",
    )
    return result


def load_report(
    path: Path,
    *,
    bootstrap_seed: int,
    bootstrap_resamples: int,
) -> tuple[dict[str, Any], list[CaseResult], str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise AnalysisError(f"Cannot read {path}: {exc}") from exc
    try:
        report = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AnalysisError(f"Invalid JSON in {path}: {exc}") from exc
    require(isinstance(report, dict), "Top-level JSON value must be an object")
    schema = get_path(report, "report_schema_version")
    require(
        schema == SUPPORTED_REPORT_SCHEMA,
        f"Unsupported report_schema_version={schema!r}; "
        f"expected {SUPPORTED_REPORT_SCHEMA}",
    )
    case_records = get_path(report, "cases")
    case_order = get_path(report, "case_order")
    require(isinstance(case_records, list), "cases must be a list")
    require(
        int(get_path(report, "case_count")) == len(case_records),
        "case_count does not match the cases array",
    )
    ids = [record.get("case_id") for record in case_records]
    require(len(ids) == len(set(ids)), "case_id values must be unique")
    require(case_order == ids, "case_order does not match cases array order")
    require(
        all(record.get("report_schema_version") == schema for record in case_records),
        "Every case must use the top-level report schema version",
    )
    require(
        all(
            get_path(record, "dataset", "join_fingerprint_schema_version") == 2
            for record in case_records
        ),
        "Every case must use join fingerprint schema v2",
    )
    cases = [
        parse_case(
            record,
            case_index=index,
            bootstrap_seed=bootstrap_seed,
            bootstrap_resamples=bootstrap_resamples,
        )
        for index, record in enumerate(case_records)
    ]
    return report, cases, hashlib.sha256(raw).hexdigest()


def summarise_suite(cases: Sequence[CaseResult]) -> SuiteSummary:
    total_j = sum(case.join_rows_j for case in cases)
    total_d = sum(case.participating_referenced_rows_d for case in cases)
    total_redundant = sum(case.redundant_tuples for case in cases)
    return SuiteSummary(
        case_count=len(cases),
        cases_with_tuple_redundancy=sum(case.redundant_tuples > 0 for case in cases),
        cases_with_lower_normalized_median=sum(
            case.update_time_ratio > 1.0 for case in cases
        ),
        total_join_rows=total_j,
        total_participating_referenced_rows=total_d,
        total_redundant_tuples=total_redundant,
        weighted_redundancy_pct=total_redundant / total_j * 100.0,
        total_redundant_property_slots=sum(
            case.redundant_property_slots for case in cases
        ),
        total_redundant_non_null_values=sum(
            case.redundant_non_null_values for case in cases
        ),
        total_redundant_payload_bytes=sum(
            case.redundant_payload_bytes for case in cases
        ),
        total_topology_redundant_relationships=sum(
            case.topology_redundant_relationships for case in cases
        ),
        median_case_update_ratio=statistics.median(
            case.update_time_ratio for case in cases
        ),
        median_case_normalization_ms=statistics.median(
            case.normalization.median_ms for case in cases
        ),
    )


def configure_plotting() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": COLORS["ink"],
            "axes.labelcolor": COLORS["ink"],
            "axes.titlecolor": COLORS["ink"],
            "axes.titleweight": "bold",
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": COLORS["grid"],
            "grid.linewidth": 0.8,
            "grid.alpha": 0.8,
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "xtick.color": COLORS["muted"],
            "ytick.color": COLORS["ink"],
            "legend.frameon": False,
            "savefig.facecolor": "white",
        }
    )


def save_figure(
    figure: plt.Figure,
    base_path: Path,
    formats: Sequence[str],
) -> list[Path]:
    written: list[Path] = []
    for output_format in formats:
        path = base_path.with_suffix(f".{output_format}")
        options: dict[str, Any] = {"bbox_inches": "tight"}
        if output_format == "png":
            options["dpi"] = 300
        figure.savefig(path, **options)
        written.append(path)
    plt.close(figure)
    return written


def style_axis(axis: plt.Axes, *, grid_axis: str = "x") -> None:
    axis.grid(False)
    axis.grid(True, axis=grid_axis)
    axis.spines["top"].set_visible(False)
    axis.spines["right"].set_visible(False)


def focus_color(case: CaseResult, focus_case_id: str, default: str) -> str:
    return COLORS["focus"] if case.case_id == focus_case_id else default


def draw_focus_fanout(
    focus: CaseResult,
    figures_dir: Path,
    formats: Sequence[str],
) -> list[Path]:
    fanouts = [fanout for fanout, _ in focus.fanout_histogram]
    counts = [count for _, count in focus.fanout_histogram]
    product_contributions = [
        fanout * count for fanout, count in focus.fanout_histogram
    ]
    figure, axis = plt.subplots(figsize=(9.2, 5.6), constrained_layout=True)
    bars = axis.bar(
        fanouts,
        counts,
        width=0.68,
        color=COLORS["normalized"],
        edgecolor="white",
        linewidth=1.2,
    )
    for bar, count, contribution in zip(
        bars, counts, product_contributions, strict=True
    ):
        axis.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.25,
            f"{count} {focus.referenced_table}s\n({contribution} rows)",
            ha="center",
            va="bottom",
            fontsize=9,
            color=COLORS["ink"],
        )
    axis.set_title(
        f"{focus.referenced_table} fanout in the folded "
        f"{focus.referencing_table}–{focus.referenced_table} join",
        loc="left",
        pad=14,
    )
    axis.text(
        0,
        1.01,
        "Fanout is the number of referencing rows that repeat one referenced tuple",
        transform=axis.transAxes,
        color=COLORS["muted"],
        fontsize=9,
    )
    axis.set_xlabel(
        f"Number of {focus.referencing_table} rows per "
        f"{focus.referenced_table}"
    )
    axis.set_ylabel(f"Number of {focus.referenced_table} keys")
    axis.set_xticks(fanouts)
    axis.set_ylim(0, max(counts) * 1.35)
    axis.text(
        0.985,
        0.93,
        f"J = {focus.join_rows_j} rows\n"
        f"D = {focus.participating_referenced_rows_d} distinct keys\n"
        f"J − D = {focus.redundant_tuples} redundant copies",
        transform=axis.transAxes,
        ha="right",
        va="top",
        fontsize=10,
        color=COLORS["ink"],
        bbox={
            "boxstyle": "round,pad=0.5",
            "facecolor": COLORS["panel"],
            "edgecolor": COLORS["grid"],
        },
    )
    style_axis(axis, grid_axis="y")
    return save_figure(figure, figures_dir / "01_focus_fanout", formats)


def draw_redundancy_by_pair(
    cases: Sequence[CaseResult],
    focus_case_id: str,
    figures_dir: Path,
    formats: Sequence[str],
) -> list[Path]:
    ordered = sorted(cases, key=lambda case: case.redundant_tuples)
    labels = [case.pair_label for case in ordered]
    positions = np.arange(len(ordered))
    figure, axes = plt.subplots(
        1,
        3,
        figsize=(17.5, 7.2),
        sharey=True,
        constrained_layout=True,
        gridspec_kw={"width_ratios": [1.1, 1.0, 1.15]},
    )
    figure.suptitle(
        "Step 1 — Functional-dependency redundancy removed by normalization",
        x=0.02,
        ha="left",
        fontsize=16,
        fontweight="bold",
        color=COLORS["ink"],
    )
    colors = [
        focus_color(case, focus_case_id, COLORS["normalized"])
        if case.redundant_tuples
        else COLORS["neutral"]
        for case in ordered
    ]

    tuple_bars = axes[0].barh(
        positions,
        [case.redundant_tuples for case in ordered],
        color=colors,
        edgecolor="white",
    )
    axes[0].set_yticks(positions, labels)
    axes[0].set_title("A. Redundant tuple copies (J − D)", loc="left")
    axes[0].set_xlabel("Tuple copies")
    for bar, case in zip(tuple_bars, ordered, strict=True):
        axes[0].text(
            bar.get_width() + max(ordered[-1].redundant_tuples, 1) * 0.015,
            bar.get_y() + bar.get_height() / 2,
            f"{case.redundant_tuples:,}",
            va="center",
            fontsize=8.5,
        )
    axes[0].set_xlim(
        0, max(case.redundant_tuples for case in ordered) * 1.22
    )

    percentage_bars = axes[1].barh(
        positions,
        [case.redundant_tuple_pct for case in ordered],
        color=colors,
        edgecolor="white",
    )
    axes[1].set_title("B. Redundant share of join", loc="left")
    axes[1].set_xlabel("(J − D) / J")
    axes[1].set_xlim(0, 112)
    axes[1].set_xticks([0, 20, 40, 60, 80, 100])
    axes[1].set_xticklabels(["0%", "20%", "40%", "60%", "80%", "100%"])
    for bar, case in zip(percentage_bars, ordered, strict=True):
        axes[1].text(
            bar.get_width() + 1.2,
            bar.get_y() + bar.get_height() / 2,
            f"{case.redundant_tuple_pct:.1f}%",
            va="center",
            fontsize=8.5,
        )

    slot_bars = axes[2].barh(
        positions,
        [case.redundant_property_slots for case in ordered],
        color=colors,
        edgecolor="white",
    )
    axes[2].set_title(
        "C. Theoretical dependent-property slots", loc="left"
    )
    axes[2].set_xlabel("(J − D) × dependent-property count")
    max_slots = max(case.redundant_property_slots for case in ordered)
    axes[2].set_xlim(0, max_slots * 1.24)
    for bar, case in zip(slot_bars, ordered, strict=True):
        axes[2].text(
            bar.get_width() + max_slots * 0.012,
            bar.get_y() + bar.get_height() / 2,
            f"{case.redundant_property_slots:,}",
            va="center",
            fontsize=8.5,
        )
    for axis in axes:
        style_axis(axis, grid_axis="x")
    return save_figure(
        figure, figures_dir / "02_redundancy_by_pair", formats
    )


def draw_update_comparison(
    cases: Sequence[CaseResult],
    focus_case_id: str,
    figures_dir: Path,
    formats: Sequence[str],
) -> list[Path]:
    ordered = sorted(cases, key=lambda case: case.update_time_ratio)
    positions = np.arange(len(ordered))
    figure, axis = plt.subplots(figsize=(12.8, 7.4), constrained_layout=True)
    axis.set_title(
        "Steps 2 and 4 — Folded versus normalized update latency",
        loc="left",
        pad=18,
    )
    axis.text(
        0,
        1.01,
        "Dots are client wall-clock medians; whiskers are descriptive 95% "
        "bootstrap intervals for the median (10,000 resamples)",
        transform=axis.transAxes,
        color=COLORS["muted"],
        fontsize=9,
    )
    for y, case in zip(positions, ordered, strict=True):
        if case.case_id == focus_case_id:
            axis.axhspan(
                y - 0.43,
                y + 0.43,
                color=COLORS["focus"],
                alpha=0.10,
                zorder=0,
            )
        line_color = (
            COLORS["positive"]
            if case.update_time_ratio > 1.0
            else COLORS["negative"]
        )
        axis.plot(
            [
                case.normalized_update.median_ms,
                case.folded_update.median_ms,
            ],
            [y, y],
            color=line_color,
            linewidth=2.5,
            alpha=0.8,
            zorder=2,
        )
        folded_error = np.array(
            [
                [
                    case.folded_update.median_ms
                    - case.folded_update.bootstrap_median_low_ms
                ],
                [
                    case.folded_update.bootstrap_median_high_ms
                    - case.folded_update.median_ms
                ],
            ]
        )
        normalized_error = np.array(
            [
                [
                    case.normalized_update.median_ms
                    - case.normalized_update.bootstrap_median_low_ms
                ],
                [
                    case.normalized_update.bootstrap_median_high_ms
                    - case.normalized_update.median_ms
                ],
            ]
        )
        axis.errorbar(
            case.folded_update.median_ms,
            y,
            xerr=folded_error,
            fmt="o",
            markersize=7,
            color=COLORS["folded"],
            ecolor=COLORS["folded"],
            elinewidth=1.2,
            capsize=3,
            zorder=4,
        )
        axis.errorbar(
            case.normalized_update.median_ms,
            y,
            xerr=normalized_error,
            fmt="s",
            markersize=6.5,
            color=COLORS["normalized"],
            ecolor=COLORS["normalized"],
            elinewidth=1.2,
            capsize=3,
            zorder=4,
        )
    axis.set_yticks(positions, [case.pair_label for case in ordered])
    axis.set_xlabel("Client wall-clock time (ms)")
    maximum = max(case.folded_update.median_ms for case in ordered)
    axis.set_xlim(0, maximum * 1.34)
    for y, case in zip(positions, ordered, strict=True):
        status = (
            f"{case.update_time_ratio:.3f}×"
            if case.update_time_ratio >= 1
            else f"{case.update_time_ratio:.3f}× (slower normalized)"
        )
        axis.text(
            maximum * 1.03,
            y,
            status,
            va="center",
            fontsize=8.5,
            color=(
                COLORS["positive"]
                if case.update_time_ratio > 1
                else COLORS["negative"]
            ),
            fontweight="bold",
        )
    axis.legend(
        handles=[
            Line2D(
                [],
                [],
                marker="o",
                linestyle="none",
                color=COLORS["folded"],
                label="Folded / denormalized",
            ),
            Line2D(
                [],
                [],
                marker="s",
                linestyle="none",
                color=COLORS["normalized"],
                label="Normalized",
            ),
        ],
        loc="upper right",
    )
    style_axis(axis, grid_axis="x")
    return save_figure(
        figure, figures_dir / "03_update_comparison", formats
    )


def draw_focus_update_samples(
    focus: CaseResult,
    figures_dir: Path,
    formats: Sequence[str],
    *,
    seed: int,
) -> list[Path]:
    figure, axis = plt.subplots(figsize=(9.5, 6.4), constrained_layout=True)
    values = [
        np.asarray(focus.folded_update.samples_ms),
        np.asarray(focus.normalized_update.samples_ms),
    ]
    labels = ["Folded", "Normalized"]
    colors = [COLORS["folded"], COLORS["normalized"]]
    boxplot = axis.boxplot(
        values,
        tick_labels=labels,
        widths=0.46,
        patch_artist=True,
        showfliers=False,
        whis=(0, 100),
        medianprops={"color": COLORS["ink"], "linewidth": 2.2},
        whiskerprops={"color": COLORS["muted"]},
        capprops={"color": COLORS["muted"]},
    )
    for patch, color in zip(boxplot["boxes"], colors, strict=True):
        patch.set_facecolor(color)
        patch.set_alpha(0.28)
        patch.set_edgecolor(color)
        patch.set_linewidth(1.7)
    rng = np.random.default_rng(seed)
    for position, (sample_values, color) in enumerate(
        zip(values, colors, strict=True), start=1
    ):
        jitter = rng.normal(0, 0.055, len(sample_values))
        axis.scatter(
            position + jitter,
            sample_values,
            s=32,
            color=color,
            alpha=0.78,
            edgecolor="white",
            linewidth=0.5,
            zorder=3,
        )
    axis.set_yscale("log")
    axis.set_ylabel("Client wall-clock time (ms, log scale)")
    axis.set_title(
        f"{focus.pair_label}: raw update samples", loc="left", pad=17
    )
    axis.text(
        0,
        1.01,
        f"n={focus.folded_update.n} per representation; restore and validation "
        "are outside the timer",
        transform=axis.transAxes,
        fontsize=9,
        color=COLORS["muted"],
    )
    axis.annotate(
        f"median {focus.folded_update.median_ms:.3f} ms\n"
        f"{focus.folded_writes} property writes",
        (1, focus.folded_update.median_ms),
        xytext=(18, 11),
        textcoords="offset points",
        va="center",
        fontsize=9,
        color=COLORS["folded"],
        fontweight="bold",
    )
    axis.annotate(
        f"median {focus.normalized_update.median_ms:.3f} ms\n"
        f"{focus.normalized_writes} property writes",
        (2, focus.normalized_update.median_ms),
        xytext=(18, -10),
        textcoords="offset points",
        va="center",
        fontsize=9,
        color=COLORS["normalized"],
        fontweight="bold",
    )
    style_axis(axis, grid_axis="y")
    return save_figure(
        figure, figures_dir / "04_focus_update_samples", formats
    )


def draw_normalization_effort(
    cases: Sequence[CaseResult],
    focus_case_id: str,
    figures_dir: Path,
    formats: Sequence[str],
    *,
    seed: int,
) -> list[Path]:
    ordered = sorted(cases, key=lambda case: case.normalization.median_ms)
    positions = np.arange(1, len(ordered) + 1)
    data = [case.normalization.samples_ms for case in ordered]
    figure, axis = plt.subplots(figsize=(12.2, 7.4), constrained_layout=True)
    boxplot = axis.boxplot(
        data,
        orientation="horizontal",
        tick_labels=[case.pair_label for case in ordered],
        widths=0.55,
        patch_artist=True,
        showfliers=False,
        whis=(0, 100),
        medianprops={"color": COLORS["ink"], "linewidth": 1.8},
        whiskerprops={"color": COLORS["muted"]},
        capprops={"color": COLORS["muted"]},
    )
    for patch, case in zip(boxplot["boxes"], ordered, strict=True):
        color = focus_color(
            case, focus_case_id, COLORS["normalization"]
        )
        patch.set_facecolor(color)
        patch.set_edgecolor(color)
        patch.set_alpha(0.28)
        patch.set_linewidth(1.5)
    rng = np.random.default_rng(seed)
    for y, case in zip(positions, ordered, strict=True):
        jitter = rng.normal(0, 0.055, case.normalization.n)
        color = focus_color(
            case, focus_case_id, COLORS["normalization"]
        )
        axis.scatter(
            case.normalization.samples_ms,
            y + jitter,
            s=16,
            color=color,
            alpha=0.55,
            edgecolor="none",
        )
        axis.text(
            case.normalization.max_ms * 1.08,
            y,
            f"median {case.normalization.median_ms:.2f} ms",
            va="center",
            fontsize=8,
            color=COLORS["muted"],
        )
    axis.set_xscale("log")
    axis.set_xlabel("Client wall-clock time (ms, log scale)")
    axis.set_title(
        "Step 3 — Normalization data-rewrite effort", loc="left", pad=18
    )
    axis.text(
        0,
        1.01,
        "Raw samples and median/IQR; excludes DDL, validation, refolding, "
        "and cleanup",
        transform=axis.transAxes,
        fontsize=9,
        color=COLORS["muted"],
    )
    style_axis(axis, grid_axis="x")
    return save_figure(
        figure, figures_dir / "05_normalization_effort", formats
    )


def draw_write_amplification_vs_speedup(
    cases: Sequence[CaseResult],
    focus_case_id: str,
    figures_dir: Path,
    formats: Sequence[str],
) -> list[Path]:
    figure, axis = plt.subplots(figsize=(10.5, 7.2), constrained_layout=True)
    sizes = [
        55 + 90 * math.sqrt(case.join_rows_j / max(c.join_rows_j for c in cases))
        for case in cases
    ]
    for case, size in zip(cases, sizes, strict=True):
        color = (
            COLORS["focus"]
            if case.case_id == focus_case_id
            else (
                COLORS["positive"]
                if case.update_time_ratio > 1
                else COLORS["negative"]
            )
        )
        marker = "*" if case.case_id == focus_case_id else "o"
        axis.scatter(
            case.write_amplification,
            case.update_time_ratio,
            s=size * (1.35 if marker == "*" else 1),
            marker=marker,
            color=color,
            alpha=0.88,
            edgecolor="white",
            linewidth=0.9,
            zorder=3,
        )
        offset = (5, 6)
        if case.code in {"OE", "OS"}:
            offset = (-20, 6)
        elif case.code in {"PC", "ETE", "ETT"}:
            offset = (5, -14)
        axis.annotate(
            case.code,
            (case.write_amplification, case.update_time_ratio),
            xytext=offset,
            textcoords="offset points",
            fontsize=9,
            fontweight="bold",
            color=color,
        )
    axis.axhline(
        1,
        color=COLORS["negative"],
        linestyle="--",
        linewidth=1.2,
        label="Equal median update time",
    )
    axis.axvline(
        1,
        color=COLORS["neutral"],
        linestyle=":",
        linewidth=1.0,
    )
    axis.set_xscale("log")
    axis.set_xlabel("Physical property-write amplification J/D (log scale)")
    axis.set_ylabel("Folded / normalized median update time")
    axis.set_title(
        "Write amplification does not translate proportionally into latency",
        loc="left",
        pad=18,
    )
    axis.text(
        0,
        1.01,
        "Above 1 on the y-axis means the normalized representation was faster",
        transform=axis.transAxes,
        fontsize=9,
        color=COLORS["muted"],
    )
    code_text = "\n".join(
        f"{case.code}: {case.pair_label}" for case in cases
    )
    axis.text(
        1.015,
        0.98,
        code_text,
        transform=axis.transAxes,
        va="top",
        fontsize=8,
        color=COLORS["muted"],
        linespacing=1.45,
        bbox={
            "boxstyle": "round,pad=0.5",
            "facecolor": COLORS["panel"],
            "edgecolor": COLORS["grid"],
        },
    )
    axis.set_ylim(
        min(0.7, min(case.update_time_ratio for case in cases) - 0.1),
        max(case.update_time_ratio for case in cases) * 1.08,
    )
    style_axis(axis, grid_axis="both")
    return save_figure(
        figure,
        figures_dir / "06_write_amplification_vs_speedup",
        formats,
    )


def draw_break_even(
    cases: Sequence[CaseResult],
    focus_case_id: str,
    figures_dir: Path,
    formats: Sequence[str],
) -> list[Path]:
    ordered = sorted(
        cases,
        key=lambda case: (
            case.break_even_batches is None,
            case.break_even_batches
            if case.break_even_batches is not None
            else math.inf,
        ),
        reverse=True,
    )
    positions = np.arange(len(ordered))
    figure, axis = plt.subplots(figsize=(11.8, 7.2), constrained_layout=True)
    axis.axvline(1, color=COLORS["neutral"], linestyle=":", linewidth=1)
    for y, case in zip(positions, ordered, strict=True):
        if case.case_id == focus_case_id:
            axis.axhspan(
                y - 0.42,
                y + 0.42,
                color=COLORS["focus"],
                alpha=0.10,
                zorder=0,
            )
        color = focus_color(case, focus_case_id, COLORS["normalization"])
        if case.break_even_batches is None:
            axis.scatter(
                0.62,
                y,
                marker="x",
                s=60,
                color=COLORS["negative"],
                linewidth=2,
                zorder=3,
            )
            reason = (
                "N/A: no tuple redundancy"
                if case.redundant_tuples == 0
                else "N/A: normalized median not lower"
            )
            axis.text(
                0.72,
                y,
                reason,
                va="center",
                fontsize=8.5,
                color=COLORS["negative"],
            )
        else:
            axis.hlines(
                y,
                1,
                case.break_even_batches,
                color=color,
                linewidth=2.3,
                alpha=0.8,
            )
            axis.scatter(
                case.break_even_batches,
                y,
                color=color,
                s=55,
                edgecolor="white",
                linewidth=0.8,
                zorder=3,
            )
            axis.text(
                case.break_even_batches * 1.12,
                y,
                f"{case.break_even_batches:.2f}",
                va="center",
                fontsize=8.5,
                color=color,
                fontweight="bold",
            )
    axis.set_xscale("log")
    axis.set_xlim(0.5, 4000)
    axis.set_yticks(positions, [case.pair_label for case in ordered])
    axis.set_xlabel("Logical update batches (log scale)")
    axis.set_title(
        "Median-based normalization break-even", loc="left", pad=18
    )
    axis.text(
        0,
        1.01,
        "Normalization median ÷ positive folded-minus-normalized median "
        "saving; N/A is not zero or infinity",
        transform=axis.transAxes,
        fontsize=9,
        color=COLORS["muted"],
    )
    style_axis(axis, grid_axis="x")
    return save_figure(
        figure, figures_dir / "07_normalization_break_even", formats
    )


def draw_focus_step_summary(
    focus: CaseResult,
    figures_dir: Path,
    formats: Sequence[str],
) -> list[Path]:
    figure, axis = plt.subplots(figsize=(15.5, 5.6), constrained_layout=True)
    axis.set_xlim(0, 1)
    axis.set_ylim(0, 1)
    axis.axis("off")
    axis.text(
        0.01,
        0.96,
        f"{focus.pair_label}: Step 1–4 result summary",
        fontsize=17,
        fontweight="bold",
        color=COLORS["ink"],
        va="top",
    )
    axis.text(
        0.01,
        0.89,
        "All work uses a run-scoped shadow projection; the original "
        "Northwind graph remains unchanged",
        fontsize=9,
        color=COLORS["muted"],
        va="top",
    )
    card_xs = [0.015, 0.26, 0.505, 0.75]
    card_width = 0.225
    card_y = 0.27
    card_height = 0.52
    cards = [
        (
            "STEP 1",
            "Measure FD redundancy",
            COLORS["normalized"],
            [
                f"J={focus.join_rows_j}, D={focus.participating_referenced_rows_d}",
                f"J−D={focus.redundant_tuples} copies "
                f"({focus.redundant_tuple_pct:.1f}%)",
                f"{focus.redundant_property_slots:,} property slots",
                f"{focus.redundant_payload_bytes:,} payload-proxy bytes",
            ],
        ),
        (
            "STEP 2",
            "Update folded copies",
            COLORS["folded"],
            [
                f"{focus.logical_updates} logical updates",
                f"{focus.folded_writes} property writes",
                f"Median {focus.folded_update.median_ms:.3f} ms",
                "Restore excluded",
            ],
        ),
        (
            "STEP 3",
            "Normalize shadow MV",
            COLORS["normalization"],
            [
                f"{focus.normalized_referenced_nodes} referenced nodes",
                f"{focus.normalized_join_relationships} join edges",
                f"Median {focus.normalization.median_ms:.3f} ms",
                "DDL/validation excluded",
            ],
        ),
        (
            "STEP 4",
            "Update normalized nodes",
            COLORS["positive"],
            [
                f"{focus.logical_updates} logical updates",
                f"{focus.normalized_writes} property writes",
                f"Median {focus.normalized_update.median_ms:.3f} ms",
                f"Folded/normalized {focus.update_time_ratio:.3f}×",
            ],
        ),
    ]
    for index, (step, heading, color, lines) in enumerate(cards):
        x = card_xs[index]
        patch = FancyBboxPatch(
            (x, card_y),
            card_width,
            card_height,
            boxstyle="round,pad=0.012,rounding_size=0.018",
            transform=axis.transAxes,
            facecolor=COLORS["panel"],
            edgecolor=color,
            linewidth=1.8,
        )
        axis.add_patch(patch)
        axis.add_patch(
            FancyBboxPatch(
                (x, card_y + card_height - 0.10),
                card_width,
                0.10,
                boxstyle="round,pad=0.012,rounding_size=0.018",
                transform=axis.transAxes,
                facecolor=color,
                edgecolor=color,
            )
        )
        axis.text(
            x + 0.015,
            card_y + card_height - 0.048,
            step,
            transform=axis.transAxes,
            color="white",
            fontsize=10,
            fontweight="bold",
            va="center",
        )
        axis.text(
            x + 0.015,
            card_y + card_height - 0.15,
            heading,
            transform=axis.transAxes,
            color=COLORS["ink"],
            fontsize=12,
            fontweight="bold",
            va="center",
        )
        for line_index, line in enumerate(lines):
            axis.text(
                x + 0.02,
                card_y + card_height - 0.23 - line_index * 0.068,
                "• " + line,
                transform=axis.transAxes,
                color=COLORS["ink"],
                fontsize=9.5,
                va="center",
            )
        if index < 3:
            axis.annotate(
                "",
                xy=(card_xs[index + 1] - 0.007, card_y + card_height / 2),
                xytext=(x + card_width + 0.007, card_y + card_height / 2),
                xycoords=axis.transAxes,
                arrowprops={
                    "arrowstyle": "-|>",
                    "color": COLORS["muted"],
                    "linewidth": 1.4,
                },
            )
    break_even = (
        "not reached"
        if focus.break_even_batches is None
        else f"{focus.break_even_batches:.2f} batches "
        f"(about {math.ceil(focus.break_even_batches)})"
    )
    axis.text(
        0.5,
        0.11,
        f"Write amplification: {focus.write_amplification:.3f}×     |     "
        f"Median time ratio: {focus.update_time_ratio:.3f}×     |     "
        f"Estimated break-even: {break_even}",
        transform=axis.transAxes,
        ha="center",
        va="center",
        color="white",
        fontsize=11.5,
        fontweight="bold",
        bbox={
            "boxstyle": "round,pad=0.75",
            "facecolor": COLORS["ink"],
            "edgecolor": COLORS["ink"],
        },
    )
    return save_figure(
        figure, figures_dir / "08_focus_step_summary", formats
    )


def draw_all_figures(
    cases: Sequence[CaseResult],
    focus: CaseResult,
    output_dir: Path,
    formats: Sequence[str],
    *,
    seed: int,
) -> list[Path]:
    figures_dir = output_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    configure_plotting()
    outputs: list[Path] = []
    outputs += draw_focus_fanout(focus, figures_dir, formats)
    outputs += draw_redundancy_by_pair(
        cases, focus.case_id, figures_dir, formats
    )
    outputs += draw_update_comparison(
        cases, focus.case_id, figures_dir, formats
    )
    outputs += draw_focus_update_samples(
        focus, figures_dir, formats, seed=seed + 9001
    )
    outputs += draw_normalization_effort(
        cases,
        focus.case_id,
        figures_dir,
        formats,
        seed=seed + 9002,
    )
    outputs += draw_write_amplification_vs_speedup(
        cases, focus.case_id, figures_dir, formats
    )
    outputs += draw_break_even(
        cases, focus.case_id, figures_dir, formats
    )
    outputs += draw_focus_step_summary(focus, figures_dir, formats)
    return outputs


def write_case_summary_csv(
    path: Path, cases: Sequence[CaseResult]
) -> None:
    columns = [
        "case_id",
        "referencing_table",
        "referenced_table",
        "update_property",
        "update_value_type",
        "logical_updates",
        "active_domain_size",
        "join_rows_J",
        "participating_referenced_rows_D",
        "full_referenced_rows",
        "unused_referenced_rows",
        "redundant_tuples_J_minus_D",
        "redundant_tuple_pct",
        "dependent_property_count",
        "redundant_property_slots",
        "redundant_non_null_values",
        "redundant_non_null_pct",
        "redundant_payload_proxy_bytes",
        "redundant_payload_pct",
        "topology_redundant_relationships",
        "folded_property_writes",
        "normalized_property_writes",
        "write_amplification_J_over_D",
        "folded_update_median_client_ms",
        "folded_update_bootstrap_median_low_ms",
        "folded_update_bootstrap_median_high_ms",
        "folded_update_p95_client_ms",
        "normalized_update_median_client_ms",
        "normalized_update_bootstrap_median_low_ms",
        "normalized_update_bootstrap_median_high_ms",
        "normalized_update_p95_client_ms",
        "folded_over_normalized_median_ratio",
        "median_client_ms_saved_per_batch",
        "normalized_time_reduction_pct",
        "normalization_median_client_ms",
        "normalization_bootstrap_median_low_ms",
        "normalization_bootstrap_median_high_ms",
        "normalization_p95_client_ms",
        "break_even_logical_update_batches",
        "run_id",
        "join_sha256",
        "source_graph_sha256",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for case in cases:
            writer.writerow(
                {
                    "case_id": case.case_id,
                    "referencing_table": case.referencing_table,
                    "referenced_table": case.referenced_table,
                    "update_property": case.update_property,
                    "update_value_type": case.update_value_type,
                    "logical_updates": case.logical_updates,
                    "active_domain_size": case.active_domain_size,
                    "join_rows_J": case.join_rows_j,
                    "participating_referenced_rows_D": (
                        case.participating_referenced_rows_d
                    ),
                    "full_referenced_rows": case.full_referenced_rows,
                    "unused_referenced_rows": case.unused_referenced_rows,
                    "redundant_tuples_J_minus_D": case.redundant_tuples,
                    "redundant_tuple_pct": case.redundant_tuple_pct,
                    "dependent_property_count": (
                        case.dependent_property_count
                    ),
                    "redundant_property_slots": (
                        case.redundant_property_slots
                    ),
                    "redundant_non_null_values": (
                        case.redundant_non_null_values
                    ),
                    "redundant_non_null_pct": case.redundant_non_null_pct,
                    "redundant_payload_proxy_bytes": (
                        case.redundant_payload_bytes
                    ),
                    "redundant_payload_pct": case.redundant_payload_pct,
                    "topology_redundant_relationships": (
                        case.topology_redundant_relationships
                    ),
                    "folded_property_writes": case.folded_writes,
                    "normalized_property_writes": case.normalized_writes,
                    "write_amplification_J_over_D": (
                        case.write_amplification
                    ),
                    "folded_update_median_client_ms": (
                        case.folded_update.median_ms
                    ),
                    "folded_update_bootstrap_median_low_ms": (
                        case.folded_update.bootstrap_median_low_ms
                    ),
                    "folded_update_bootstrap_median_high_ms": (
                        case.folded_update.bootstrap_median_high_ms
                    ),
                    "folded_update_p95_client_ms": (
                        case.folded_update.p95_ms
                    ),
                    "normalized_update_median_client_ms": (
                        case.normalized_update.median_ms
                    ),
                    "normalized_update_bootstrap_median_low_ms": (
                        case.normalized_update.bootstrap_median_low_ms
                    ),
                    "normalized_update_bootstrap_median_high_ms": (
                        case.normalized_update.bootstrap_median_high_ms
                    ),
                    "normalized_update_p95_client_ms": (
                        case.normalized_update.p95_ms
                    ),
                    "folded_over_normalized_median_ratio": (
                        case.update_time_ratio
                    ),
                    "median_client_ms_saved_per_batch": (
                        case.update_time_saved_ms
                    ),
                    "normalized_time_reduction_pct": (
                        case.normalized_time_reduction_pct
                    ),
                    "normalization_median_client_ms": (
                        case.normalization.median_ms
                    ),
                    "normalization_bootstrap_median_low_ms": (
                        case.normalization.bootstrap_median_low_ms
                    ),
                    "normalization_bootstrap_median_high_ms": (
                        case.normalization.bootstrap_median_high_ms
                    ),
                    "normalization_p95_client_ms": (
                        case.normalization.p95_ms
                    ),
                    "break_even_logical_update_batches": (
                        ""
                        if case.break_even_batches is None
                        else case.break_even_batches
                    ),
                    "run_id": case.run_id,
                    "join_sha256": case.join_sha256,
                    "source_graph_sha256": case.source_graph_sha256,
                }
            )


def write_timing_samples_csv(
    path: Path,
    report: dict[str, Any],
) -> None:
    columns = [
        "case_id",
        "phase",
        "run",
        "client_wall_ms",
        "neo4j_reported_total_ms",
        "affected_nodes",
        "properties_set",
        "nodes_created",
        "nodes_deleted",
        "relationships_created",
        "relationships_deleted",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for case_record in get_path(report, "cases"):
            for phase in PHASES:
                for sample in get_path(
                    case_record, "timings", phase, "samples"
                ):
                    writer.writerow(
                        {
                            "case_id": case_record["case_id"],
                            "phase": phase,
                            **{
                                column: sample[column]
                                for column in columns[2:]
                            },
                        }
                    )


def case_to_json(case: CaseResult) -> dict[str, Any]:
    payload = asdict(case)
    payload["fanout_histogram"] = {
        str(fanout): count
        for fanout, count in case.fanout_histogram
    }
    return payload


def write_analysis_summary_json(
    path: Path,
    *,
    report: dict[str, Any],
    cases: Sequence[CaseResult],
    suite: SuiteSummary,
    focus: CaseResult,
    source_sha256: str,
    bootstrap_seed: int,
    bootstrap_resamples: int,
) -> None:
    payload = {
        "analysis_schema_version": 1,
        "source": {
            "experiment": get_path(report, "experiment"),
            "database": get_path(report, "database"),
            "topology_mode": get_path(report, "topology_mode"),
            "report_schema_version": get_path(
                report, "report_schema_version"
            ),
            "join_fingerprint_schema_version": 2,
            "sha256": source_sha256,
        },
        "analysis": {
            "focus_case_id": focus.case_id,
            "primary_timing_metric": "client wall-clock median (ms)",
            "bootstrap_seed": bootstrap_seed,
            "bootstrap_resamples": bootstrap_resamples,
            "bootstrap_interval": (
                "2.5th and 97.5th percentiles of resampled medians; "
                "descriptive, not independent-deployment inference"
            ),
            "p95_definition": "nearest rank: sorted[ceil(0.95*n)-1]",
        },
        "suite_summary": asdict(suite),
        "cases": [case_to_json(case) for case in cases],
        "interpretation_notes": {
            "payload": (
                "UTF-8 value-content proxy; not Neo4j on-disk store bytes"
            ),
            "break_even": (
                "normalization median divided by positive folded-minus-"
                "normalized update median; null when not defined"
            ),
            "suite_aggregate": (
                "sum across independent pair-projection workloads, not a "
                "simultaneous whole-graph saving"
            ),
            "source_graph": (
                "read-only and fingerprint-identical before, during, and "
                "after every experiment"
            ),
        },
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def format_break_even(case: CaseResult) -> str:
    return (
        "N/A"
        if case.break_even_batches is None
        else f"{case.break_even_batches:.2f}"
    )


def main_results_table(cases: Sequence[CaseResult]) -> str:
    rows = []
    for case in cases:
        rows.append(
            f"| {case.pair_label} | {case.join_rows_j:,} | "
            f"{case.participating_referenced_rows_d:,} | "
            f"{case.redundant_tuples:,} | "
            f"{case.redundant_tuple_pct:.2f}% | "
            f"{case.folded_update.median_ms:.3f} | "
            f"{case.normalized_update.median_ms:.3f} | "
            f"{case.update_time_ratio:.3f}× | "
            f"{case.normalization.median_ms:.3f} | "
            f"{format_break_even(case)} |"
        )
    return "\n        ".join(rows)


def redundancy_results_table(cases: Sequence[CaseResult]) -> str:
    rows = []
    for case in cases:
        rows.append(
            f"| {case.pair_label} | "
            f"{case.redundant_property_slots:,} | "
            f"{case.redundant_non_null_values:,} "
            f"({case.redundant_non_null_pct:.2f}%) | "
            f"{case.redundant_payload_bytes:,} "
            f"({case.redundant_payload_pct:.2f}%) | "
            f"{case.topology_redundant_relationships:,} |"
        )
    return "\n        ".join(rows)


def environment_text(report: dict[str, Any]) -> str:
    environment = get_path(report["cases"][0], "environment")
    return (
        f"{environment['server_agent']}; Python "
        f"{environment['python_version']}; Neo4j driver "
        f"{environment['neo4j_driver_version']}"
    )


def build_english_report(
    *,
    report: dict[str, Any],
    cases: Sequence[CaseResult],
    suite: SuiteSummary,
    focus: CaseResult,
    source_path: Path,
    source_sha256: str,
    bootstrap_resamples: int,
) -> str:
    strongest = max(cases, key=lambda case: case.update_time_ratio)
    maximum_write = max(cases, key=lambda case: case.write_amplification)
    whole_break_even = (
        "not reached"
        if focus.break_even_batches is None
        else (
            f"{focus.break_even_batches:.2f} batches; the first whole-batch "
            f"threshold is approximately batch "
            f"{math.ceil(focus.break_even_batches)}"
        )
    )
    return _build_english_report_body(
        report=report,
        cases=cases,
        suite=suite,
        focus=focus,
        source_path=source_path,
        source_sha256=source_sha256,
        bootstrap_resamples=bootstrap_resamples,
        strongest=strongest,
        maximum_write=maximum_write,
        whole_break_even=whole_break_even,
    )


def build_chinese_report(
    *,
    report: dict[str, Any],
    cases: Sequence[CaseResult],
    suite: SuiteSummary,
    focus: CaseResult,
    source_path: Path,
    source_sha256: str,
) -> str:
    strongest = max(cases, key=lambda case: case.update_time_ratio)
    break_even_whole = (
        "not reached"
        if focus.break_even_batches is None
        else (
            f"{focus.break_even_batches:.2f} batches; the first whole-batch "
            f"threshold is approximately batch {math.ceil(focus.break_even_batches)}"
        )
    )
    break_even_calculation = (
        "Break-even is undefined for this case because the median update saving is not positive."
        if focus.break_even_batches is None
        else (
            f"For {focus.pair_label}, the calculation is "
            f"{focus.normalization.median_ms:.3f}/"
            f"({focus.folded_update.median_ms:.3f}-"
            f"{focus.normalized_update.median_ms:.3f})"
            f"={focus.break_even_batches:.3f}; the first whole batch that recovers "
            f"the cost is approximately batch {math.ceil(focus.break_even_batches)}."
        )
    )
    return dedent(
        rf"""
        # Northwind Functional Dependency Normalization Experiment Report

        ## Abstract

        This report analyzes {suite.case_count} directly related table pairs in
        Northwind, using **{focus.pair_label}** as the focus case for Steps 1–4:
        {focus.referenced_table} information is folded into each
        {focus.referencing_table} tuple. All experiments run on shadow
        projections tagged with `run_id`; the original Northwind graph remains
        read-only. Its SHA-256 fingerprint is identical before, during, and
        after cleanup of every case.

        The {focus.pair_label} folded join contains \(J={focus.join_rows_j}\)
        {focus.referencing_table}
        tuples and \(D={focus.participating_referenced_rows_d}\) distinct
        {focus.referenced_table} keys participating in the join. Normalization
        therefore removes **\(J-D={focus.redundant_tuples}\)
        {focus.referenced_table} tuple copies, representing
        {focus.redundant_tuple_pct:.3f}% of J**. Further calculations give
        **{focus.redundant_property_slots:,} theoretical redundant
        dependent-property slots**, **{focus.redundant_non_null_values:,}
        redundant non-NULL values**, and **{focus.redundant_payload_bytes:,}
        bytes of redundant value-payload
        proxy**.

        When updating `{focus.referenced_table}.{focus.update_property}`, the
        folded representation writes {focus.folded_writes} property copies,
        while the normalized representation writes only
        {focus.normalized_writes} distinct {focus.referenced_table} nodes. Write
        amplification is **{focus.write_amplification:.3f}×**. The client
        wall-clock medians are **{focus.folded_update.median_ms:.3f} ms** and
        **{focus.normalized_update.median_ms:.3f} ms**, respectively, giving a
        time ratio of **{focus.update_time_ratio:.3f}×**. The Step 3 normalization
        median is **{focus.normalization.median_ms:.3f} ms**, and the estimated
        break-even based on medians is **{break_even_whole}**.

        ![Step 1–4 summary](figures/08_focus_step_summary.png)

        ## 1. Research Questions

        - **RQ1:** How much redundancy is caused by functional dependencies,
          and how much can normalization eliminate?
        - **RQ2:** What is the one-time cost of decomposing a folded MV into
          normalized MVs?
        - **RQ3:** How many physical writes and how much time does the same set
          of logical updates require in folded and normalized representations?
        - **RQ4:** How many comparable update batches are needed to recover the
          normalization cost through update savings?

        ## 2. Experimental Method

        ### Step 0: Create the Folded Baseline (Not Timed)

        The program creates one run-scoped MV node for each join tuple, copies
        the referencing table properties and the referenced table's non-key
        properties into that node, and copies the necessary boundary
        relationships according to the complete topology. This baseline is the
        report's "denormalized/original representation"; the actual source
        graph is never deleted or modified.

        ### Step 1: Calculate Redundancy

        Let \(J\) be the number of folded join tuples and \(D\) the number of
        distinct referenced keys participating in the join:

        \[
        R_{{tuple}}=J-D
        \]

        For a referenced table with \(c_B\) columns and key length \(k_B\):

        \[
        R_{{slot}}=(J-D)(c_B-k_B)
        \]

        Theoretical slots include NULL positions; the non-NULL metric counts
        only duplicated property values that actually exist. The payload metric
        counts UTF-8 bytes for strings and UTF-8 bytes of `str(value)` for other
        values. It is only a **proxy for value-content size, not actual Neo4j
        disk usage**. Topology relationship redundancy is a separate
        relationship-level metric and cannot be added directly to property
        counts.

        ### Steps 2 and 4: Identical Logical Updates

        Each case selects a non-key property from the referenced table and
        applies a deterministic value rotation over the active domain of the
        complete referenced relation. Both representations use exactly the
        same key-to-value mapping:

        - Step 2 updates all \(J\) duplicated positions in the folded MV;
        - Step 4 updates \(D\) distinct referenced nodes in the normalized MV.

        One measured run is one autocommit batch, timed through result
        consumption and commit completion; restoration and validation are not
        timed. Each phase has {focus.folded_update.n} measured runs, with the
        client wall-clock median as the primary reported statistic.

        ### Step 3: Normalization

        The program recreates normalized referencing nodes from the folded
        nodes, creates referenced nodes after applying DISTINCT to referenced
        keys, adds explicit join relationships, and migrates and deduplicates
        boundary relationships. Timing includes the explicit transaction,
        data decomposition, and commit, but excludes DDL, validation,
        refolding, and cleanup.

        ## 3. Experimental Results

        ### 3.1 RQ1: Redundancy Savings

        In the focus case, \(J-D={focus.join_rows_j}-
        {focus.participating_referenced_rows_d}={focus.redundant_tuples}\).
        {focus.referenced_table} has {focus.dependent_property_count}
        dependent properties, so the theoretical redundant slot count is
        \({focus.redundant_tuples}\times
        {focus.dependent_property_count}={focus.redundant_property_slots}\).

        The {focus.referenced_table} fanout histogram is
        `{dict(focus.fanout_histogram)}`. A {focus.referenced_table} row with
        fanout \(f\) contributes \(f-1\) redundant tuple copies.

        ![{focus.referenced_table} fanout](figures/01_focus_fanout.png)

        ![Redundancy across all table pairs](figures/02_redundancy_by_pair.png)

        | Table pair | Theoretical redundant property slots | Redundant non-NULL values | Redundant payload proxy (bytes) | Redundant topology relationships |
        |---|---:|---:|---:|---:|
        {redundancy_results_table(cases)}

        For {focus.pair_label}, topology relationship redundancy is
        {focus.topology_redundant_relationships}; both folded and normalized
        representations have {focus.topology_folded_relationships:,} boundary
        relationships. Zero relationship redundancy does not imply zero
        property redundancy: the two metrics measure different resources.

        ### 3.2 RQ3: Update Cost

        One batch in the focus case contains {focus.logical_updates} logical
        {focus.referenced_table} updates.
        The folded structure updates {focus.folded_writes} property positions,
        while the normalized structure updates {focus.normalized_writes}
        nodes, so:

        \[
        A_{{write}}=J/D={focus.write_amplification:.3f}
        \]

        The folded median is {focus.folded_update.median_ms:.3f} ms and the
        normalized median is {focus.normalized_update.median_ms:.3f} ms.
        The folded median is {(focus.update_time_ratio - 1) * 100:.3f}% higher;
        relative to the folded baseline, normalization reduces the median by
        {focus.normalized_time_reduction_pct:.3f}%.

        ![Folded and normalized updates](figures/03_update_comparison.png)

        ![{focus.pair_label} update raw samples](figures/04_focus_update_samples.png)

        The folded focus samples have mean={focus.folded_update.mean_ms:.3f} ms,
        median={focus.folded_update.median_ms:.3f} ms, and max=
        {focus.folded_update.max_ms:.3f} ms. Slow outliers raise the mean, so
        this report emphasizes the median, IQR, and raw sample points rather
        than using mean bars as the primary evidence.

        ### 3.3 RQ2: Normalization Cost

        The {focus.folded_nodes} folded nodes are decomposed into
        {focus.normalized_referencing_nodes} normalized
        {focus.referencing_table} nodes and
        {focus.normalized_referenced_nodes} distinct {focus.referenced_table}
        nodes, with {focus.normalized_join_relationships} join relationships
        created. The normalization median is
        {focus.normalization.median_ms:.3f} ms.

        Thus, \(J-D\) counts the **referenced property copies** removed; it
        cannot be interpreted as a net reduction of \(J-D\) physical graph
        nodes. Normalization adds distinct referenced nodes and explicit join
        edges.

        ![Normalization effort](figures/05_normalization_effort.png)

        ### 3.4 Write Amplification and Time Ratio

        \(J/D\) determines the number of physical writes, but elapsed time also
        includes fixed or variable costs such as transaction startup, query
        execution, index/cache behavior, result consumption, and commit.
        Write amplification therefore does not translate proportionally into
        time amplification.

        ![Write amplification and time ratio](figures/06_write_amplification_vs_speedup.png)

        ### 3.5 RQ4: Break-even

        Break-even is defined only when \(J-D>0\) and the folded median is
        strictly greater than the normalized median:

        \[
        N_{{break-even}}=
        \frac{{T_{{normalization,median}}}}
        {{T_{{folded-update,median}}-
          T_{{normalized-update,median}}}}
        \]

        {break_even_calculation}

        ![Break-even](figures/07_normalization_break_even.png)

        EmployeeTerritory → Employee has redundancy, but its normalized median
        is slightly slower. EmployeeTerritory → Territory has \(J=D=49\) and
        no tuple redundancy. Break-even is undefined for both cases, rather
        than zero or infinity. The estimate for Product → Category is about
        1,940.6, but the median difference is only 0.004 ms, within the range
        of microbenchmark noise, and should not be treated as a precise
        prediction.

        ## 4. Complete Results

        | Table pair | J | D | J−D | Tuple redundancy | Folded update median (ms) | Normalized update median (ms) | Time ratio | Normalization median (ms) | Break-even batches |
        |---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
        {main_results_table(cases)}

        Of the {suite.case_count} cases,
        {suite.cases_with_tuple_redundancy} have tuple redundancy, and
        {suite.cases_with_lower_normalized_median} have a lower normalized
        median. The strongest runtime result is {strongest.pair_label}, with
        folded/normalized median ratio={strongest.update_time_ratio:.3f}× and
        estimated break-even=
        {strongest.break_even_batches:.2f} batches.

        Descriptive totals across the ten separate workloads are
        J={suite.total_join_rows:,},
        D={suite.total_participating_referenced_rows:,}, J−D=
        {suite.total_redundant_tuples:,}, and weighted tuple redundancy=
        {suite.weighted_redundancy_pct:.3f}%. These cases share or overlap source
        tables, so the total **does not equal** the net database savings from
        materializing all ten MVs simultaneously.

        ## 5. Discussion

        1. When J>D, normalization eliminates value duplication caused by
           functional dependencies; tuple, slot, non-NULL, and payload metrics
           describe this redundancy at different levels of granularity.
        2. Fewer physical writes do not guarantee proportional reductions in
           wall-clock time. The normalized median is lower in 8/10 cases, but
           fixed overhead can dominate small workloads.
        3. {strongest.pair_label} provides the strongest update-performance
           evidence: a {strongest.update_time_ratio:.3f}× time ratio, with
           median-based cost recovery after approximately
           {math.ceil(strongest.break_even_batches or 0)} batches.
        4. "Original graph update" should be described precisely as an update
           to the folded/denormalized shadow representation; the actual source
           graph remains unchanged throughout the experiment.

        ## 6. Validity Limitations

        - The data come from one local warm-cache suite
          (`{environment_text(report)}`). The server page/query cache was not
          cleared, and different phases were not randomly interleaved.
        - The 20 timings are repeated transactions on the same database, not
          20 independent database deployments. Bootstrap intervals describe
          only resampling variability in the current sample sequence and are
          not used to infer population-level significance.
        - Cases differ in the updated property, type, active-domain size,
          batch size, and topology. Absolute milliseconds across cases provide
          descriptive context only; folded-versus-normalized comparisons
          within a case are controlled comparisons.
        - Update restoration/validation are not timed; normalization excludes
          DDL, validation, refolding, and cleanup. Break-even inherits these
          timing boundaries and does not predict the full cost of a production
          migration.
        - The payload proxy excludes Neo4j record headers, dynamic stores,
          indexes, labels, relationships, transaction logs, compression, and
          page layout. Actual disk usage requires a separate store-level
          experiment.
        - Read queries, concurrency, locking, and long-term operation were not
          measured.

        ## 7. Conclusion

        The {focus.pair_label} case directly answers the three original tasks:

        - **Redundancy eliminated by normalization:** {focus.redundant_tuples}
          {focus.referenced_table} tuple copies,
          {focus.redundant_property_slots} theoretical dependent-property slots,
          {focus.redundant_non_null_values} redundant non-NULL values, and
          {focus.redundant_payload_bytes:,} bytes value-payload proxy;
        - **Normalization time:** median=
          {focus.normalization.median_ms:.3f} ms for the defined data-rewrite
          scope;
        - **Update difference:** {focus.folded_writes} versus
          {focus.normalized_writes} property writes, folded median=
          {focus.folded_update.median_ms:.3f} ms, normalized median=
          {focus.normalized_update.median_ms:.3f} ms, and folded/normalized=
          {focus.update_time_ratio:.3f}×.

        Based on the measured medians, the estimated break-even is
        {break_even_whole}. The full suite also shows that high FD/write
        redundancy can produce clear update savings, while small workloads
        require careful interpretation using raw samples, outliers, and fixed
        overhead.

        ## Appendix: Reproducibility

        - Input file: `{source_path.name}`
        - Input SHA-256: `{source_sha256}`
        - Report schema: v{get_path(report, "report_schema_version")}
        - Join fingerprint schema: v2
        - Database: `{get_path(report, "database")}`
        - Topology mode: `{get_path(report, "topology_mode")}`
        - Source graph fingerprints match before, during, and after every case
        - All shadow graphs and temporary schema objects are cleaned up after
          each case

        `case_summary.csv`, `timing_samples.csv`, and `analysis_summary.json`
        contain the underlying data used in the report and figures. Each
        figure is exported as both a 300 dpi PNG and a scalable SVG.
        """
    ).lstrip()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def git_metadata(repo_root: Path) -> dict[str, Any]:
    def run_git(*arguments: str) -> str | None:
        try:
            completed = subprocess.run(
                ["git", *arguments],
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError):
            return None
        return completed.stdout.strip()

    commit = run_git("rev-parse", "HEAD")
    status = run_git("status", "--short")
    return {
        "commit": commit,
        "worktree_dirty": None if status is None else bool(status),
        "status_short": None if status is None else status.splitlines(),
    }


def write_manifest(
    path: Path,
    *,
    report: dict[str, Any],
    cases: Sequence[CaseResult],
    source_path: Path,
    source_sha256: str,
    output_paths: Sequence[Path],
    focus_case_id: str,
    bootstrap_seed: int,
    bootstrap_resamples: int,
    formats: Sequence[str],
) -> None:
    repo_root = SCRIPT_DIR.parent.parent
    first_configuration = get_path(report["cases"][0], "configuration")
    payload = {
        "manifest_schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "generator": str(Path(__file__).resolve()),
        "command_options": {
            "source": str(source_path),
            "focus_case_id": focus_case_id,
            "bootstrap_seed": bootstrap_seed,
            "bootstrap_resamples": bootstrap_resamples,
            "figure_formats": list(formats),
        },
        "runtime": {
            "python": sys.version,
            "numpy": np.__version__,
            "matplotlib": matplotlib.__version__,
        },
        "git": git_metadata(repo_root),
        "source_report": {
            "path": str(source_path),
            "sha256": source_sha256,
            "report_schema_version": get_path(
                report, "report_schema_version"
            ),
            "join_fingerprint_schema_version": 2,
            "experiment": get_path(report, "experiment"),
            "database": get_path(report, "database"),
            "topology_mode": get_path(report, "topology_mode"),
        },
        "statistical_definitions": {
            "primary_time": "client wall-clock median in milliseconds",
            "quartiles": "NumPy linear percentile",
            "p95": "nearest rank: sorted[ceil(0.95*n)-1]",
            "bootstrap": (
                "percentile interval over independently resampled observed "
                "timings; descriptive only"
            ),
        },
        "experiment_scope": {
            "update_warmups": first_configuration["warmup_runs"],
            "update_measured_runs": first_configuration[
                "measured_update_runs"
            ],
            "normalization_warmups": first_configuration[
                "normalization_warmup_runs"
            ],
            "normalization_measured_runs": first_configuration[
                "normalization_runs"
            ],
            "update_timing_scope": first_configuration[
                "update_timing_scope"
            ],
            "normalization_timing_scope": first_configuration[
                "normalization_timing_scope"
            ],
            "cache_policy": first_configuration["cache_policy"],
        },
        "cases": [
            {
                "case_id": case.case_id,
                "run_id": case.run_id,
                "source_graph_sha256": case.source_graph_sha256,
                "join_sha256": case.join_sha256,
            }
            for case in cases
        ],
        "artifacts": [
            {
                "path": str(output.relative_to(path.parent)),
                "sha256": file_sha256(output),
                "bytes": output.stat().st_size,
            }
            for output in output_paths
        ],
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def generate_outputs(
    *,
    report: dict[str, Any],
    cases: Sequence[CaseResult],
    source_path: Path,
    source_sha256: str,
    output_dir: Path,
    focus_case_id: str,
    bootstrap_seed: int,
    bootstrap_resamples: int,
    formats: Sequence[str],
) -> list[Path]:
    by_id = {case.case_id: case for case in cases}
    require(
        focus_case_id in by_id,
        f"Unknown focus case {focus_case_id!r}; choose one of "
        f"{', '.join(by_id)}",
    )
    focus = by_id[focus_case_id]
    suite = summarise_suite(cases)
    output_dir.mkdir(parents=True, exist_ok=True)

    outputs = draw_all_figures(
        cases,
        focus,
        output_dir,
        formats,
        seed=bootstrap_seed,
    )
    case_csv = output_dir / "case_summary.csv"
    samples_csv = output_dir / "timing_samples.csv"
    summary_json = output_dir / "analysis_summary.json"
    report_en = output_dir / "northwind_fd_report.md"
    report_zh = output_dir / "northwind_fd_report_zh.md"
    write_case_summary_csv(case_csv, cases)
    write_timing_samples_csv(samples_csv, report)
    write_analysis_summary_json(
        summary_json,
        report=report,
        cases=cases,
        suite=suite,
        focus=focus,
        source_sha256=source_sha256,
        bootstrap_seed=bootstrap_seed,
        bootstrap_resamples=bootstrap_resamples,
    )
    report_en.write_text(
        build_english_report(
            report=report,
            cases=cases,
            suite=suite,
            focus=focus,
            source_path=source_path,
            source_sha256=source_sha256,
            bootstrap_resamples=bootstrap_resamples,
        ),
        encoding="utf-8",
    )
    report_zh.write_text(
        build_chinese_report(
            report=report,
            cases=cases,
            suite=suite,
            focus=focus,
            source_path=source_path,
            source_sha256=source_sha256,
        ),
        encoding="utf-8",
    )
    outputs += [case_csv, samples_csv, summary_json, report_en, report_zh]
    manifest = output_dir / "analysis_manifest.json"
    write_manifest(
        manifest,
        report=report,
        cases=cases,
        source_path=source_path,
        source_sha256=source_sha256,
        output_paths=outputs,
        focus_case_id=focus_case_id,
        bootstrap_seed=bootstrap_seed,
        bootstrap_resamples=bootstrap_resamples,
        formats=formats,
    )
    outputs.append(manifest)
    return outputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and analyse the Northwind FD benchmark, then write "
            "tables, reports, and figures."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"v6 JSON report (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"artifact directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--focus-case",
        default="product_supplier",
        help="case for detailed Step 1–4 analysis (default: product_supplier)",
    )
    parser.add_argument(
        "--bootstrap-seed",
        type=int,
        default=DEFAULT_BOOTSTRAP_SEED,
        help=f"deterministic bootstrap seed (default: {DEFAULT_BOOTSTRAP_SEED})",
    )
    parser.add_argument(
        "--bootstrap-resamples",
        type=int,
        default=DEFAULT_BOOTSTRAP_RESAMPLES,
        help=(
            "median bootstrap resamples "
            f"(default: {DEFAULT_BOOTSTRAP_RESAMPLES})"
        ),
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=("png", "svg", "pdf"),
        default=("png", "svg"),
        help="figure formats (default: png svg)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="validate and print headline results without writing files",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        require(
            args.bootstrap_resamples > 0,
            "--bootstrap-resamples must be positive",
        )
        source_path = args.input.resolve()
        report, cases, source_sha256 = load_report(
            source_path,
            bootstrap_seed=args.bootstrap_seed,
            bootstrap_resamples=args.bootstrap_resamples,
        )
        by_id = {case.case_id: case for case in cases}
        require(
            args.focus_case in by_id,
            f"Unknown focus case {args.focus_case!r}; choose one of "
            f"{', '.join(by_id)}",
        )
        focus = by_id[args.focus_case]
        suite = summarise_suite(cases)
        print(
            f"Validated report schema v"
            f"{get_path(report, 'report_schema_version')}: "
            f"{len(cases)} cases; SHA-256 {source_sha256[:12]}…"
        )
        print(
            f"Suite: {suite.cases_with_tuple_redundancy}/"
            f"{suite.case_count} cases have tuple redundancy; "
            f"{suite.cases_with_lower_normalized_median}/"
            f"{suite.case_count} have a lower normalized median"
        )
        print(
            f"Focus {focus.pair_label}: J={focus.join_rows_j}, "
            f"D={focus.participating_referenced_rows_d}, "
            f"J-D={focus.redundant_tuples}; update ratio="
            f"{focus.update_time_ratio:.3f}×; normalization median="
            f"{focus.normalization.median_ms:.3f} ms"
        )
        if args.validate_only:
            print("Validation-only mode: no artifacts written")
            return 0
        output_dir = args.output_dir.resolve()
        outputs = generate_outputs(
            report=report,
            cases=cases,
            source_path=source_path,
            source_sha256=source_sha256,
            output_dir=output_dir,
            focus_case_id=args.focus_case,
            bootstrap_seed=args.bootstrap_seed,
            bootstrap_resamples=args.bootstrap_resamples,
            formats=tuple(dict.fromkeys(args.formats)),
        )
        print(f"Generated {len(outputs)} artifacts in {output_dir}")
        for output in outputs:
            print(f"  {output.relative_to(output_dir)}")
        return 0
    except (AnalysisError, OSError, ValueError, TypeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


def _build_english_report_body(
    *,
    report: dict[str, Any],
    cases: Sequence[CaseResult],
    suite: SuiteSummary,
    focus: CaseResult,
    source_path: Path,
    source_sha256: str,
    bootstrap_resamples: int,
    strongest: CaseResult,
    maximum_write: CaseResult,
    whole_break_even: str,
) -> str:
    update_warmups = get_path(
        report["cases"][0], "configuration", "warmup_runs"
    )
    normalization_warmups = get_path(
        report["cases"][0],
        "configuration",
        "normalization_warmup_runs",
    )
    break_even_calculation = (
        "The focus case has no defined break-even because its median update "
        "saving is not positive."
        if focus.break_even_batches is None
        else (
            f"For the focus case this is "
            f"{focus.normalization.median_ms:.3f}/"
            f"({focus.folded_update.median_ms:.3f}-"
            f"{focus.normalized_update.median_ms:.3f})"
            f"={focus.break_even_batches:.3f} batches, or approximately "
            f"whole batch {math.ceil(focus.break_even_batches)}."
        )
    )
    return dedent(
        rf"""
        # Northwind Functional-Dependency Normalization Experiment

        ## Abstract

        This report evaluates the storage redundancy, update cost, and
        normalization effort of direct related-table pairs in the Northwind
        graph. The primary case is **{focus.pair_label}**, corresponding to
        folding {focus.referenced_table} information into
        {focus.referencing_table} rows. All experiments use
        run-scoped shadow projections; the source Northwind graph is read-only
        and was fingerprint-verified unchanged.

        In the focus case, the folded join contains \(J={focus.join_rows_j}\)
        {focus.referencing_table} tuples and
        \(D={focus.participating_referenced_rows_d}\) distinct participating
        {focus.referenced_table} keys. Normalization therefore removes
        **{focus.redundant_tuples} redundant {focus.referenced_table} tuple copies
        ({focus.redundant_tuple_pct:.3f}% of J)**. These copies account for
        **{focus.redundant_property_slots:,} theoretical dependent-property
        slots**, **{focus.redundant_non_null_values:,} duplicate non-NULL
        values**, and **{focus.redundant_payload_bytes:,} bytes of duplicate
        value-payload proxy**.

        Updating `{focus.referenced_table}.{focus.update_property}` required
        {focus.folded_writes} property writes in the folded projection and
        {focus.normalized_writes} in the normalized projection, a
        **{focus.write_amplification:.3f}× write amplification**. Median client
        time was **{focus.folded_update.median_ms:.3f} ms folded** and
        **{focus.normalized_update.median_ms:.3f} ms normalized**, a
        {focus.update_time_ratio:.3f}× ratio. The normalization rewrite took a
        median **{focus.normalization.median_ms:.3f} ms**, yielding a
        median-based break-even of **{whole_break_even}**.

        Across all {suite.case_count} table pairs,
        {suite.cases_with_tuple_redundancy}/{suite.case_count} contain tuple
        redundancy and {suite.cases_with_lower_normalized_median}/{suite.case_count}
        have a lower normalized update median. The results
        support the expected reduction in physical writes, but also show that
        fixed transaction/query overhead prevents write amplification from
        translating proportionally into elapsed time.

        ![Step 1–4 focus-case summary](figures/08_focus_step_summary.png)

        ## 1. Research questions

        - **RQ1 — Redundancy:** How much referenced-side FD redundancy does
          normalization remove?
        - **RQ2 — Normalization effort:** How long does the one-off data
          rewrite take?
        - **RQ3 — Update cost:** How much longer does the same logical update
          take on the folded representation than on the normalized one?
        - **RQ4 — Amortization:** After how many comparable update batches does
          cumulative median update saving recover the normalization cost?

        ## 2. Experimental design

        ### Step 0 — Folded baseline (not timed)

        The experiment builds a complete-topology, run-scoped folded
        materialized-view projection. Referencing-table and non-key
        referenced-table properties are copied into one MV node per join
        tuple, using table-specific prefixes. Necessary boundary relationships
        are copied. Source nodes and relationships are never deleted or
        modified.

        ### Step 1 — Redundancy

        Let \(J\) be the number of folded join tuples and \(D\) the number of
        distinct referenced keys that participate in the join. Tuple
        redundancy is:

        \[
        R_{{tuple}} = J-D
        \]

        If the referenced table has \(c_B\) columns and key length \(k_B\),
        the number of dependent properties is \(c_B-k_B\), and theoretical
        redundant property slots are:

        \[
        R_{{slot}}=(J-D)(c_B-k_B)
        \]

        The non-NULL metric counts only populated duplicate dependent values.
        The payload metric sums UTF-8 bytes for strings and UTF-8 bytes of
        `str(value)` for other types. It is a **value-content proxy, not Neo4j
        physical store size**. Complete-topology relationship redundancy is a
        separate graph-specific metric and is never added to property counts.

        ### Steps 2 and 4 — Controlled update

        Each case chooses one eligible non-key referenced-table property. A
        deterministic rotation over its full referenced-relation active domain
        maps every participating referenced key to a different value. The same
        key-to-value mapping is applied in both representations:

        - Step 2 updates all \(J\) folded copies.
        - Step 4 updates the \(D\) distinct normalized referenced nodes.

        Both phases execute one autocommit batch through result consumption and
        commit. Restore and validation are excluded from timing. There are
        {update_warmups} warm-ups and {focus.folded_update.n} measured runs per
        representation.

        ### Step 3 — Normalization

        The data rewrite recreates normalized referencing nodes, creates one
        referenced node per distinct key, creates the join relationship, and
        deduplicates/migrates copied referenced-side boundary relationships.
        The timer includes explicit transaction begin, data decomposition, and
        commit. It excludes schema DDL, validation, refolding, and cleanup.
        There are {normalization_warmups} warm-up and
        {focus.normalization.n} measured runs.

        Client wall-clock median is the primary time metric. Neo4j-reported
        time has integer-millisecond resolution and is frequently zero for
        these small operations. Raw samples, IQR, and descriptive
        {bootstrap_resamples:,}-resample bootstrap intervals are retained; no
        significance test is claimed.

        ## 3. Results

        ### 3.1 RQ1 — Redundancy saved

        For {focus.pair_label},
        \(R_{{tuple}}={focus.join_rows_j}-{focus.participating_referenced_rows_d}
        ={focus.redundant_tuples}\). {focus.referenced_table} has
        {focus.dependent_property_count} dependent properties, so
        \(R_{{slot}}={focus.redundant_tuples}\times
        {focus.dependent_property_count}={focus.redundant_property_slots}\).
        The actual non-NULL and payload-proxy measurements are
        {focus.redundant_non_null_values:,} values
        ({focus.redundant_non_null_pct:.3f}%) and
        {focus.redundant_payload_bytes:,} bytes
        ({focus.redundant_payload_pct:.3f}%).

        The {focus.referenced_table} fanout histogram is
        `{dict(focus.fanout_histogram)}`. A {focus.referenced_table} tuple with
        fanout \(f\) contributes \(f-1\) redundant tuple copies.

        ![{focus.referenced_table} fanout](figures/01_focus_fanout.png)

        ![Redundancy by pair](figures/02_redundancy_by_pair.png)

        | Pair | Redundant property slots | Redundant non-NULL values | Redundant payload proxy (bytes) | Redundant topology relationships |
        |---|---:|---:|---:|---:|
        {redundancy_results_table(cases)}

        The focus case has
        {focus.topology_redundant_relationships:,} redundant boundary
        relationships: both folded and normalized states contain
        {focus.topology_folded_relationships:,}. This zero topology value does
        not contradict the positive attribute redundancy; the two metrics
        measure different resources.

        ### 3.2 RQ3 — Folded versus normalized updates

        The focus update contains {focus.logical_updates} logical
        {focus.referenced_table} updates. It writes {focus.folded_writes}
        folded nodes versus
        {focus.normalized_writes} normalized nodes, so physical write
        amplification is exactly:

        \[
        A_{{write}}=J/D={focus.join_rows_j}/
        {focus.participating_referenced_rows_d}
        ={focus.write_amplification:.3f}
        \]

        The folded update median is {focus.folded_update.median_ms:.3f} ms
        (descriptive bootstrap interval
        {focus.folded_update.bootstrap_median_low_ms:.3f}–
        {focus.folded_update.bootstrap_median_high_ms:.3f} ms), and the
        normalized median is {focus.normalized_update.median_ms:.3f} ms
        ({focus.normalized_update.bootstrap_median_low_ms:.3f}–
        {focus.normalized_update.bootstrap_median_high_ms:.3f} ms). The folded
        median is {(focus.update_time_ratio - 1) * 100:.3f}% higher;
        equivalently, normalization reduces the median by
        {focus.normalized_time_reduction_pct:.3f}% relative to the folded
        baseline.

        ![Update comparison](figures/03_update_comparison.png)

        ![Focus update samples](figures/04_focus_update_samples.png)

        The folded focus samples contain visible slow runs: the mean is
        {focus.folded_update.mean_ms:.3f} ms, median
        {focus.folded_update.median_ms:.3f} ms, and maximum
        {focus.folded_update.max_ms:.3f} ms. This skew is why medians and raw
        samples, rather than mean bars, are the primary evidence.

        ### 3.3 RQ2 — Normalization effort

        The focus rewrite converts {focus.folded_nodes} folded nodes into
        {focus.normalized_referencing_nodes} normalized
        {focus.referencing_table} nodes and
        {focus.normalized_referenced_nodes} distinct
        {focus.referenced_table} nodes, then
        creates {focus.normalized_join_relationships} join relationships. Its
        median is {focus.normalization.median_ms:.3f} ms, with a descriptive
        bootstrap interval of
        {focus.normalization.bootstrap_median_low_ms:.3f}–
        {focus.normalization.bootstrap_median_high_ms:.3f} ms.

        Consequently, \(J-D\) measures removed **referenced attribute copies**,
        not a net reduction of \(J-D\) graph nodes. Normalization adds distinct
        referenced nodes and explicit join relationships while removing
        dependent values from their repeated folded locations.

        ![Normalization effort](figures/05_normalization_effort.png)

        ### 3.4 Write amplification and elapsed time

        Physical write reduction is deterministic from \(J/D\), whereas client
        time contains fixed transaction, planning/execution, cache, result
        consumption, index, and commit costs. The relationship is therefore
        not proportional. For example, {maximum_write.pair_label} has
        {maximum_write.write_amplification:.1f}× physical write amplification
        but only a {maximum_write.update_time_ratio:.3f}× median time ratio.

        ![Write amplification versus speedup](figures/06_write_amplification_vs_speedup.png)

        ### 3.5 RQ4 — Break-even

        When folded median update time is strictly greater than normalized
        median time and \(J-D>0\), the data-rewrite break-even is:

        \[
        N_{{break-even}}=
        \frac{{T_{{normalization,median}}}}
        {{T_{{folded-update,median}}-
          T_{{normalized-update,median}}}}
        \]

        {break_even_calculation} A fractional estimate is an
        amortization projection, not a guarantee for an individual run.

        ![Normalization break-even](figures/07_normalization_break_even.png)

        Two cases have no break-even value. EmployeeTerritory → Employee has
        positive redundancy but a slightly slower normalized median;
        EmployeeTerritory → Territory has \(J=D=49\) and therefore no tuple
        redundancy. Product → Category has a calculated break-even near
        1,940.6 batches, but its median saving is only 0.004 ms—well within the
        practical noise of such a small workload—so this number must not be
        treated as a precise forecast.

        ## 4. Complete case results

        | Pair | J | D | J−D | Tuple redundancy | Folded update median (ms) | Normalized update median (ms) | Time ratio | Normalization median (ms) | Break-even batches |
        |---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
        {main_results_table(cases)}

        Nine of ten pairs have tuple redundancy; eight have a lower normalized
        median. The strongest observed runtime result is
        {strongest.pair_label}: folded median is
        {strongest.update_time_ratio:.3f}× normalized, with an estimated
        {strongest.break_even_batches:.2f}-batch break-even.

        Descriptively summing the ten independent pair workloads gives
        J={suite.total_join_rows:,}, D=
        {suite.total_participating_referenced_rows:,}, and J−D=
        {suite.total_redundant_tuples:,}
        ({suite.weighted_redundancy_pct:.3f}% weighted redundancy), together
        with {suite.total_redundant_property_slots:,} theoretical slots,
        {suite.total_redundant_non_null_values:,} duplicate non-NULL values,
        {suite.total_redundant_payload_bytes:,} payload-proxy bytes, and
        {suite.total_topology_redundant_relationships:,} duplicate boundary
        relationships. Because these projections overlap and are evaluated
        separately, the sums are **not** the net saving from materializing all
        ten pairs simultaneously.

        ## 5. Discussion

        1. **Normalization removes FD redundancy whenever J>D.** The tuple
           formula and the property-level counts quantify different levels of
           the same repeated referenced data.
        2. **Fewer physical writes do not guarantee proportionally faster
           transactions.** Eight within-case comparisons favor normalization,
           but fixed overhead dominates several small workloads.
        3. **{strongest.pair_label} is the strongest runtime evidence.** Its
           {strongest.update_time_ratio:.3f}× ratio and approximately
           {math.ceil(strongest.break_even_batches or 0)}-batch break-even are
           materially clearer than near-tied microbenchmarks.
        4. **Property redundancy and topology redundancy are distinct.**
           Normalization may save repeated attributes without saving boundary
           edges, or may additionally deduplicate referenced-owned boundary
           edges in complete-topology mode.
        5. **The “original graph” performance baseline is the folded shadow
           graph.** The actual source Northwind graph remains unchanged.

        ## 6. Threats to validity

        - The suite is one local warm-cache execution in
          `{environment_text(report)}`. Server page/query caches were not
          cleared, and phases were not interleaved or randomized.
        - Twenty timings are repeated transactions on the same database, not
          twenty independent database deployments. Bootstrap intervals only
          describe resampling variability in this observed sequence.
        - Different cases update different attributes, types, active-domain
          sizes, logical batch sizes, and graph topologies. Cross-case
          milliseconds are descriptive context; the controlled comparison is
          folded versus normalized within each case.
        - Update restore and validation are excluded. Normalization excludes
          DDL, validation, refolding, and cleanup. Break-even inherits those
          exact timing scopes and is not a full production-migration forecast.
        - Value-payload proxy excludes Neo4j record headers, dynamic stores,
          indexes, labels, relationships, transaction logs, compression, and
          page layout. Actual disk savings require store-level measurement.
        - Read-query latency, concurrent workloads, locking, and long-running
          operational effects are outside this experiment.

        ## 7. Conclusion

        The focus experiment answers the three requested outcomes directly:

        - **Redundancy saved:** {focus.redundant_tuples}
          {focus.referenced_table} tuple copies,
          {focus.redundant_property_slots} theoretical dependent-property
          slots, {focus.redundant_non_null_values} duplicate non-NULL values,
          and {focus.redundant_payload_bytes:,} value-payload-proxy bytes.
        - **Time to normalize:** {focus.normalization.median_ms:.3f} ms median
          for the defined shadow data-rewrite scope.
        - **Update overhead:** {focus.folded_writes} versus
          {focus.normalized_writes} property writes and
          {focus.folded_update.median_ms:.3f} versus
          {focus.normalized_update.median_ms:.3f} ms median client time, giving
          a {focus.update_time_ratio:.3f}× folded/normalized ratio.

        On this workload, the median-based break-even is
        {whole_break_even}.
        The wider suite confirms that large FD/write redundancy can produce
        meaningful update gains, but also demonstrates that tiny workloads
        must be interpreted with raw timing variability and fixed overhead in
        view.

        ## Appendix A — Reproducibility

        - Source file: `{source_path.name}`
        - Source SHA-256: `{source_sha256}`
        - Report schema: v{get_path(report, "report_schema_version")}
        - Join fingerprint schema: v2
        - Database: `{get_path(report, "database")}`
        - Topology mode: `{get_path(report, "topology_mode")}`
        - Original graph: SHA-256 identical before, during, and after every case
        - Shadow graph and temporary schema: cleaned after every case

        `case_summary.csv`, `timing_samples.csv`, and
        `analysis_summary.json` contain the data used by the figures and
        report. SVG copies of every figure are provided for publication.
        """
    ).lstrip()


if __name__ == "__main__":
    raise SystemExit(main())
