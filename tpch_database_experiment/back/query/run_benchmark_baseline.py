#!/usr/bin/env python3
"""Run the Neo4j TPC-H baseline queries repeatedly and print timing information.

Each query receives reproducible parameter sets generated from the same seed.
Parameter generation and connection setup are outside the timed region. The
client wall timer covers submitting the Cypher query and fully consuming its
result. Neo4j's result summary additionally reports the time until the first
result is available and the time needed to consume the remaining result.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import sys
from dataclasses import dataclass
from getpass import getpass
from statistics import fmean
from time import perf_counter_ns
from typing import Any, Sequence


if __package__:
    from . import generate_params_neo4j
    from .benchmark_query_neo4j import BENCHMARK_QUERIES
else:
    import generate_params_neo4j
    from benchmark_query_neo4j import BENCHMARK_QUERIES


RUNS_PER_QUERY = 1
DEFAULT_SEED = 20260724
DEFAULT_SCALE_FACTOR = 0.01         # This will have an impact on the parameter generator.

# Edit this value to select the Neo4j database used by the benchmark.
DATABASE_NAME = "tpch-sf-001"

# Local debugging only. NEO4J_PASSWORD overrides this value when it is set.
# Do not commit or share this file while it contains a real password.
DEFAULT_NEO4J_PASSWORD: str | None = ""

PARAMETER_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


# The `frozen` option is used only to prevent data from being accidentally modified
@dataclass(frozen=True)
class QueryTiming:
    query_id: int
    client_wall_ms: tuple[float, ...]
    available_after_ms: tuple[int | None, ...]
    consumed_after_ms: tuple[int | None, ...]
    neo4j_reported_total_ms: tuple[int | None, ...]
    row_counts: tuple[int, ...]


def positive_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Neo4j TPC-H Q1-Q22 repeatedly with generated parameters "
            "and print client and Neo4j-reported query times."
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
        help=(
            "Neo4j database "
            f"(default: NEO4J_DATABASE or {DATABASE_NAME})"
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"random seed shared by all parameter generators (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--sf",
        type=positive_float,
        default=DEFAULT_SCALE_FACTOR,
        help=(
            "TPC-H scale factor required by Q11 "
            f"(default: {DEFAULT_SCALE_FACTOR})"
        ),
    )
    return parser.parse_args(argv)


def generate_parameter_sets(
    query_id: int,
    seed: int,
    sf: float,
) -> list[dict[str, Any]]:
    generator_name = f"make_params_q{query_id}"
    try:
        generator = getattr(generate_params_neo4j, generator_name)
    except AttributeError as exc:
        raise RuntimeError(f"missing parameter generator: {generator_name}") from exc

    if query_id == 11:  # Because Query11 depend on SF
        parameter_sets = generator(seed, RUNS_PER_QUERY, sf)
    else:
        parameter_sets = generator(seed, RUNS_PER_QUERY)

    if len(parameter_sets) != RUNS_PER_QUERY:
        raise RuntimeError(
            f"{generator_name} returned {len(parameter_sets)} parameter sets; "
            f"expected {RUNS_PER_QUERY}"
        )
    return parameter_sets


def format_ms(value: float | int | None, width: int = 10) -> str:
    if value is None:
        return f"{'N/A':>{width}}"
    return f"{value:>{width}.3f}"


def complete_mean(values: Sequence[int | None]) -> float | None:
    if any(value is None for value in values):
        return None
    return fmean(value for value in values if value is not None)


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
            raise ValueError("Cypher display values must be finite numbers")
        return repr(value)
    if isinstance(value, (list, tuple)):
        values = ", ".join(format_cypher_literal(item) for item in value)
        return f"[{values}]"
    raise TypeError(
        f"unsupported Cypher display value type: {type(value).__name__}"
    )


def inline_query_parameters(
    query: str,
    parameters: dict[str, Any],
) -> str:
    def replace_parameter(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in parameters:
            raise KeyError(f"missing value for Cypher parameter ${name}")
        return format_cypher_literal(parameters[name])

    return PARAMETER_PATTERN.sub(replace_parameter, query)


def run_query_iterations(
    session: Any,
    query_id: int,
    query: str,
    parameter_sets: list[dict[str, Any]],
) -> QueryTiming:
    client_wall_times: list[float] = []
    available_after_times: list[int | None] = []
    consumed_after_times: list[int | None] = []
    neo4j_reported_total_times: list[int | None] = []
    row_counts: list[int] = []
    total_runs = len(parameter_sets)
    run_number_width = len(str(total_runs))
    run_label_width = len(f"run {total_runs}/{total_runs}")

    print("========================================================================================================================================")
    print(f"Q{query_id:02d}", flush=True)
    for run_number, parameters in enumerate(parameter_sets, start=1):
        if run_number == 1:
            print("----------------------------------------------------------------------------------------------------------------------------------------")
            displayed_query = inline_query_parameters(query, parameters)
            print(displayed_query.strip(), flush=True)
            print("----------------------------------------------------------------------------------------------------------------------------------------")
        started_ns = perf_counter_ns()
        result = session.run(query, parameters)
        records = list(result)
        summary = result.consume()
        client_wall_ms = (perf_counter_ns() - started_ns) / 1_000_000

        available_after_ms = summary.result_available_after
        consumed_after_ms = summary.result_consumed_after
        if available_after_ms is None or consumed_after_ms is None:
            neo4j_reported_total_ms = None
        else:
            neo4j_reported_total_ms = (
                available_after_ms + consumed_after_ms
            )

        client_wall_times.append(client_wall_ms)
        available_after_times.append(available_after_ms)
        consumed_after_times.append(consumed_after_ms)
        neo4j_reported_total_times.append(neo4j_reported_total_ms)
        row_counts.append(len(records))
        run_label = (
            f"run {run_number:>{run_number_width}}/{total_runs}"
        )
        print(
            f"  {run_label}: "
            f"client={client_wall_ms:>10.3f} ms | "
            f"available={format_ms(available_after_ms)} ms | "
            f"consumed={format_ms(consumed_after_ms)} ms | "
            f"neo4j total={format_ms(neo4j_reported_total_ms)} ms | "
            f"{len(records):>6,} rows",
            flush=True,
        )

    print(
        f"  {'average':>{run_label_width}}: "
        f"client={fmean(client_wall_times):>10.3f} ms | "
        f"available={format_ms(complete_mean(available_after_times))} ms | "
        f"consumed={format_ms(complete_mean(consumed_after_times))} ms | "
        f"neo4j total="
        f"{format_ms(complete_mean(neo4j_reported_total_times))} ms",
        flush=True,
    )
    return QueryTiming(
        query_id=query_id,
        client_wall_ms=tuple(client_wall_times),
        available_after_ms=tuple(available_after_times),
        consumed_after_ms=tuple(consumed_after_times),
        neo4j_reported_total_ms=tuple(neo4j_reported_total_times),
        row_counts=tuple(row_counts),
    )


def print_summary(timings: list[QueryTiming]) -> None:
    print(f"\nSummary (average of {RUNS_PER_QUERY} runs)")
    header = (
        f"{'Query':<7}"
        f"{'Client wall (ms)':>18}"
        f"{'Available (ms)':>17}"
        f"{'Consumed (ms)':>17}"
        f"{'Neo4j total (ms)':>20}"
        f"{'Rows':>16}"
    )
    print(header)
    print("-" * len(header))

    for timing in timings:
        query_label = f"Q{timing.query_id:02d}"
        rows = "/".join(f"{count:,}" for count in timing.row_counts)
        print(
            f"{query_label:<7}"
            f"{format_ms(fmean(timing.client_wall_ms), 18)}"
            f"{format_ms(complete_mean(timing.available_after_ms), 17)}"
            f"{format_ms(complete_mean(timing.consumed_after_ms), 17)}"
            f"{format_ms(complete_mean(timing.neo4j_reported_total_ms), 20)}"
            f"{rows:>16}"
        )

    client_total_ms = sum(sum(timing.client_wall_ms) for timing in timings)
    neo4j_totals = [
        value
        for timing in timings
        for value in timing.neo4j_reported_total_ms
    ]
    if any(value is None for value in neo4j_totals):
        neo4j_total_text = "N/A"
    else:
        neo4j_total_text = (
            f"{sum(value for value in neo4j_totals if value is not None):.3f} ms"
        )

    print("-" * len(header))
    print(
        f"Timed executions: {len(timings) * RUNS_PER_QUERY}; "
        f"total client wall time: {client_total_ms:.3f} ms; "
        f"total Neo4j-reported time: {neo4j_total_text}"
    )


def run_benchmark(args: argparse.Namespace, password: str) -> None:
    try:
        from neo4j import READ_ACCESS, GraphDatabase
    except ImportError as exc:
        raise RuntimeError(
            "The Neo4j Python driver is not installed. "
            "Install it with: pip install neo4j"
        ) from exc

    expected_query_ids = tuple(range(1, 23))
    if tuple(BENCHMARK_QUERIES) != expected_query_ids:
        raise RuntimeError("BENCHMARK_QUERIES must contain Q1 through Q22 in order")

    all_parameters = {
        query_id: generate_parameter_sets(query_id, args.seed, args.sf)
        for query_id in expected_query_ids
    }

    print(
        f"Connecting to {args.uri} as {args.user!r}; "
        f"database={args.database!r}"
    )
    print(
        f"Running Q1-Q22; {RUNS_PER_QUERY} executions per query; "
        f"seed={args.seed}; sf={args.sf}"
    )
    print(
        "Client wall timing includes query submission and full result "
        "consumption."
    )
    print(
        "Neo4j-reported total = available-after + consumed-after; "
        "the consumed phase can include result streaming.\n"
    )

    timings: list[QueryTiming] = []
    with GraphDatabase.driver(args.uri, auth=(args.user, password)) as driver:
        driver.verify_connectivity()
        with driver.session(
            database=args.database,
            default_access_mode=READ_ACCESS,
        ) as session:
            for query_id, query in BENCHMARK_QUERIES.items():
                try:
                    timing = run_query_iterations(
                        session,
                        query_id,
                        query,
                        all_parameters[query_id],
                    )
                except Exception as exc:
                    raise RuntimeError(
                        f"Q{query_id} failed while executing the baseline"
                    ) from exc
                timings.append(timing)

    print_summary(timings)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    password = os.getenv("NEO4J_PASSWORD") or DEFAULT_NEO4J_PASSWORD
    if password is None:
        password = getpass(f"Neo4j password for {args.user}: ")

    try:
        run_benchmark(args, password)
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        cause = exc.__cause__
        if cause is not None:
            print(f"CAUSE: {cause}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
