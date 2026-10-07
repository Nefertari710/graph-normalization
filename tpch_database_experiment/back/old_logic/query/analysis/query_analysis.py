#!/usr/bin/env python3
"""Generate Markdown tables from the fast TPC-H query benchmark JSON.

By default, the script reads
``../results/results_benchmark_fast/run_benchmark_fast_results.json``
relative to this file and writes ``run_benchmark_fast_results.md`` beside the
JSON report::

    python query_analysis.py

Alternative paths can be supplied with ``--input`` and ``--output``. Records
with the same strategy are grouped into one row, and every timing column in the
detailed table is the arithmetic mean across that strategy's runs. A compact,
one-row-per-query table suitable for a paper is appended after it.
"""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (
    SCRIPT_DIR.parents[0]
    / "results"
    / "results_benchmark_fast"
    / "run_benchmark_fast_results.json"
)

TABLE_COLUMNS = (
    "query_id",
    "strategy",
    "mv_create_time_ms",
    "baseline.client_time_ms",
    "baseline.neo4j_time_ms",
    "template_result.client_time_ms",
    "template_result.neo4j_time_ms",
)


class AnalysisError(RuntimeError):
    """Raised when the benchmark report is missing or malformed."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AnalysisError(message)


def required(record: dict[str, Any], key: str, context: str) -> Any:
    require(key in record, f"{context}.{key} is missing")
    return record[key]


def as_object(value: Any, context: str) -> dict[str, Any]:
    require(isinstance(value, dict), f"{context} must be an object")
    return value


def as_nonempty_string(value: Any, context: str) -> str:
    require(
        isinstance(value, str) and bool(value),
        f"{context} must be a non-empty string",
    )
    return value


def as_positive_int(value: Any, context: str) -> int:
    require(
        isinstance(value, int) and not isinstance(value, bool) and value > 0,
        f"{context} must be a positive integer",
    )
    return value


def as_time(
    value: Any,
    context: str,
    *,
    nullable: bool = False,
) -> int | Decimal | None:
    if value is None:
        require(nullable, f"{context} must not be null")
        return None
    require(
        isinstance(value, (int, Decimal))
        and not isinstance(value, bool)
        and value >= 0,
        f"{context} must be a non-negative number",
    )
    return value


def validate_record(raw_record: Any, index: int) -> dict[str, Any]:
    context = f"records[{index}]"
    record = as_object(raw_record, context)
    as_positive_int(required(record, "query_id", context), f"{context}.query_id")
    as_nonempty_string(required(record, "template", context), f"{context}.template")
    as_nonempty_string(required(record, "strategy", context), f"{context}.strategy")
    as_time(
        required(record, "mv_create_time_ms", context),
        f"{context}.mv_create_time_ms",
        nullable=True,
    )

    baseline = as_object(required(record, "baseline", context), f"{context}.baseline")
    as_time(
        required(baseline, "client_time_ms", f"{context}.baseline"),
        f"{context}.baseline.client_time_ms",
        nullable=True,
    )
    as_time(
        required(baseline, "neo4j_time_ms", f"{context}.baseline"),
        f"{context}.baseline.neo4j_time_ms",
        nullable=True,
    )

    template_result = as_object(
        required(record, "template_result", context),
        f"{context}.template_result",
    )
    as_time(
        required(
            template_result,
            "client_time_ms",
            f"{context}.template_result",
        ),
        f"{context}.template_result.client_time_ms",
        nullable=True,
    )
    as_time(
        required(
            template_result,
            "neo4j_time_ms",
            f"{context}.template_result",
        ),
        f"{context}.template_result.neo4j_time_ms",
        nullable=True,
    )
    return record


def load_records(input_path: Path) -> list[dict[str, Any]]:
    try:
        raw_report = json.loads(
            input_path.read_text(encoding="utf-8"),
            parse_float=Decimal,
        )
    except FileNotFoundError as exc:
        raise AnalysisError(f"input JSON does not exist: {input_path}") from exc
    except json.JSONDecodeError as exc:
        raise AnalysisError(f"input is not valid JSON: {exc}") from exc
    except OSError as exc:
        raise AnalysisError(f"cannot read {input_path}: {exc}") from exc

    require(isinstance(raw_report, list), "the JSON root must be a list")
    require(bool(raw_report), "the JSON report contains no benchmark records")
    return [validate_record(record, index) for index, record in enumerate(raw_report)]


def escape_markdown(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def format_cell(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, str):
        return escape_markdown(value)
    return str(value)


def row_values(record: dict[str, Any]) -> tuple[Any, ...]:
    baseline = as_object(record["baseline"], "record.baseline")
    template_result = as_object(record["template_result"], "record.template_result")
    return (
        record["query_id"],
        record["strategy"],
        record["mv_create_time_ms"],
        baseline["client_time_ms"],
        baseline["neo4j_time_ms"],
        template_result["client_time_ms"],
        template_result["neo4j_time_ms"],
    )


def arithmetic_mean(values: list[int | Decimal | None]) -> Decimal | None:
    """Return the complete-group arithmetic mean, or None if a value is absent."""

    if not values or any(value is None for value in values):
        return None
    total = sum((Decimal(value) for value in values if value is not None), Decimal(0))
    return total / Decimal(len(values))


def decimal_mean(values: list[int | Decimal]) -> Decimal:
    """Return the arithmetic mean without converting JSON values to floats."""

    require(bool(values), "cannot calculate the mean of an empty list")
    total = sum((Decimal(value) for value in values), Decimal(0))
    return total / Decimal(len(values))


def build_paper_summaries(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build a no-selection arithmetic-mean summary for each TPC-H query.

    Each valid strategy is first averaged across all parameterized runs. The
    resulting strategy means then receive equal weight in the query-level
    macro-average. No fastest run or fastest strategy is selected.
    """

    groups_by_query: dict[int, dict[str, list[dict[str, Any]]]] = {}
    for record in records:
        groups_by_query.setdefault(record["query_id"], {}).setdefault(
            record["strategy"], []
        ).append(record)

    paper_summaries: list[dict[str, Any]] = []
    for query_id in sorted(groups_by_query):
        strategy_groups = groups_by_query[query_id]
        run_counts = {len(group) for group in strategy_groups.values()}
        require(
            len(run_counts) == 1,
            f"query {query_id} has inconsistent run counts across strategies",
        )
        runs_per_strategy = next(iter(run_counts))
        require(runs_per_strategy > 0, f"query {query_id} has no benchmark runs")

        eligible: list[dict[str, Any]] = []
        for strategy, group in strategy_groups.items():
            complete = all(
                record["mv_create_time_ms"] is not None
                and record["baseline"]["client_time_ms"] is not None
                and record["template_result"]["client_time_ms"] is not None
                for record in group
            )
            if not complete:
                continue

            baseline_times = [
                record["baseline"]["client_time_ms"] for record in group
            ]
            rewritten_times = [
                record["template_result"]["client_time_ms"] for record in group
            ]
            mv_create_times = [record["mv_create_time_ms"] for record in group]
            require(
                all(Decimal(value) > 0 for value in baseline_times),
                f"strategy {strategy!r} contains a non-positive baseline time",
            )
            require(
                all(Decimal(value) > 0 for value in rewritten_times),
                f"strategy {strategy!r} contains a non-positive rewritten time",
            )

            eligible.append(
                {
                    "baseline_client_time_ms": decimal_mean(baseline_times),
                    "rewritten_client_time_ms": decimal_mean(rewritten_times),
                    "mv_create_time_ms": decimal_mean(mv_create_times),
                }
            )

        require(
            bool(eligible),
            f"query {query_id} has no strategy with {runs_per_strategy} complete runs",
        )

        baseline_mean = decimal_mean(
            [item["baseline_client_time_ms"] for item in eligible]
        )
        rewritten_mean = decimal_mean(
            [item["rewritten_client_time_ms"] for item in eligible]
        )
        mv_create_mean = decimal_mean(
            [item["mv_create_time_ms"] for item in eligible]
        )
        baseline_saving = rewritten_mean - baseline_mean
        paper_summaries.append(
            {
                "query_id": query_id,
                "valid_strategies": len(eligible),
                "tested_strategies": len(strategy_groups),
                "baseline_client_time_ms": baseline_mean,
                "rewritten_client_time_ms": rewritten_mean,
                "baseline_saving_ms": (
                    baseline_saving if baseline_saving > 0 else None
                ),
                "baseline_speedup": rewritten_mean / baseline_mean,
                "mv_create_time_s": mv_create_mean / Decimal(1000),
            }
        )
    return paper_summaries


def format_paper_row(summary: dict[str, Any]) -> str:
    """Render one row of the compact paper-oriented table."""

    baseline_saving = summary["baseline_saving_ms"]
    saving_text = "—" if baseline_saving is None else f"{baseline_saving:.1f}"
    client_times = (
        f'{summary["rewritten_client_time_ms"]:.1f} → '
        f'{summary["baseline_client_time_ms"]:.1f}'
    )
    cells = (
        f'Q{summary["query_id"]}',
        f'{summary["valid_strategies"]}/{summary["tested_strategies"]}',
        client_times,
        saving_text,
        f'{summary["baseline_speedup"]:.2f}×',
        f'{summary["mv_create_time_s"]:.2f}',
    )
    return "| " + " | ".join(cells) + " |"


def aggregate_by_strategy(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse benchmark runs to one arithmetic-mean row per strategy."""

    groups: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        groups.setdefault(record["strategy"], []).append(record)

    summaries: list[dict[str, Any]] = []
    for strategy, group in groups.items():
        query_ids = {record["query_id"] for record in group}
        templates = {record["template"] for record in group}
        require(
            len(query_ids) == 1,
            f"strategy {strategy!r} is associated with multiple query_id values",
        )
        require(
            len(templates) == 1,
            f"strategy {strategy!r} is associated with multiple templates",
        )

        summaries.append(
            {
                "query_id": group[0]["query_id"],
                "template": group[0]["template"],
                "strategy": strategy,
                "mv_create_time_ms": arithmetic_mean(
                    [record["mv_create_time_ms"] for record in group]
                ),
                "baseline": {
                    "client_time_ms": arithmetic_mean(
                        [record["baseline"]["client_time_ms"] for record in group]
                    ),
                    "neo4j_time_ms": arithmetic_mean(
                        [record["baseline"]["neo4j_time_ms"] for record in group]
                    ),
                },
                "template_result": {
                    "client_time_ms": arithmetic_mean(
                        [
                            record["template_result"]["client_time_ms"]
                            for record in group
                        ]
                    ),
                    "neo4j_time_ms": arithmetic_mean(
                        [
                            record["template_result"]["neo4j_time_ms"]
                            for record in group
                        ]
                    ),
                },
            }
        )
    return summaries


def render_markdown(
    records: list[dict[str, Any]],
    summaries: list[dict[str, Any]],
    input_path: Path,
) -> str:
    query_count = len({summary["query_id"] for summary in summaries})
    incomplete_rewritten_means = sum(
        summary["template_result"]["client_time_ms"] is None
        or summary["template_result"]["neo4j_time_ms"] is None
        for summary in summaries
    )
    run_counts_by_strategy = {
        strategy: sum(record["strategy"] == strategy for record in records)
        for strategy in {record["strategy"] for record in records}
    }
    distinct_run_counts = sorted(set(run_counts_by_strategy.values()))
    run_count_text = ", ".join(str(value) for value in distinct_run_counts)

    lines = [
        "# TPC-H Fast Benchmark Results",
        "",
        f"- Source: `{escape_markdown(input_path.name)}`",
        f"- Benchmark runs analyzed: {len(records)}",
        f"- TPC-H queries: {query_count}",
        f"- Strategies: {len(summaries)}",
        f"- Runs per strategy: {run_count_text}",
        "- Aggregation: arithmetic mean of each timing field by strategy.",
        (
            f"- Strategies with incomplete rewritten timings: "
            f"{incomplete_rewritten_means}; if any run is `null`/capped, the "
            "complete-group rewritten mean is shown as `N/A`."
        ),
        "",
        "| " + " | ".join(TABLE_COLUMNS) + " |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for summary in summaries:
        lines.append(
            "| "
            + " | ".join(format_cell(value) for value in row_values(summary))
            + " |"
        )

    paper_summaries = build_paper_summaries(records)
    lines.extend(
        [
            "",
            "## Paper-ready Query Summary",
            "",
            (
                "- `Valid/tested` counts strategies whose MV creation, baseline "
                "client time, and rewritten client time are present in every run; "
                "incomplete/capped strategies are excluded from the averages."
            ),
            (
                "- No fastest run or strategy is selected. Each valid strategy is "
                "first summarized by the arithmetic mean across all parameterized "
                "runs; those strategy means are then averaged with equal weight "
                "for the query (a macro-average)."
            ),
            (
                "- Times are displayed as `rewritten → baseline`. Baseline saving "
                "is `mean rewritten client time - mean baseline client time` and "
                "is shown only when positive; `—` means baseline does not save "
                "time for that query."
            ),
            (
                "- Baseline speedup is `mean rewritten client time / mean baseline "
                "client time`; values above `1×` mean baseline is faster."
            ),
            "",
            (
                "| Query | Valid/tested | Mean client time: rewritten → baseline "
                "(ms) | Baseline saving (ms) | Baseline speedup | Mean MV build "
                "(s) |"
            ),
            "|---:|---:|---:|---:|---:|---:|",
        ]
    )
    lines.extend(format_paper_row(summary) for summary in paper_summaries)
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a Markdown table from the fast TPC-H benchmark JSON.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="fast benchmark JSON report",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="output Markdown path; defaults to the input path with a .md suffix",
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
        require(
            input_path != output_path,
            "input and output paths must be different",
        )
        records = load_records(input_path)
        summaries = aggregate_by_strategy(records)
        markdown = render_markdown(records, summaries, input_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown, encoding="utf-8")
    except (AnalysisError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        f"Markdown report written to {output_path} "
        f"({len(summaries)} strategy rows from {len(records)} runs)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
