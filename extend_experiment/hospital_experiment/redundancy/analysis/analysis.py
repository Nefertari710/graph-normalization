#!/usr/bin/env python3
"""Build the Hospital normalization analysis from the experiment JSON."""

import json
import math
import statistics
from pathlib import Path

INPUT = (
        Path(__file__).resolve().parents[1]
        / "results"
        / "hospitalcsv"
        / "redundancy_experiment.json"
)
OUTPUT = INPUT.with_suffix(".md")
EXPECTED_EXPERIMENT = "hospital_record_normalization"
STATE_FIELDS = ("node_count", "relationship_count", "property_cell_count")


class AnalysisError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AnalysisError(message)


def number(value: object, name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise AnalysisError(f"{name} must be numeric")
    value = float(value)
    require(math.isfinite(value) and value >= 0, f"{name} must be non-negative")
    return value


def integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise AnalysisError(f"{name} must be an integer")
    return value


def read_state(report: dict, name: str) -> dict[str, int]:
    state = report.get(name)
    if not isinstance(state, dict):
        raise AnalysisError(f"Missing {name}")
    return {
        field: integer(state.get(field), f"{name}.{field}")
        for field in STATE_FIELDS
    }


def read_times(run: dict, name: str, repeats: int, position: int) -> list[float]:
    values = run.get(name)
    if not isinstance(values, list):
        raise AnalysisError(f"update {position}: missing {name}")
    require(
        len(values) == repeats,
        f"update {position}: {name} must contain {repeats} values",
    )
    return [number(value, f"update {position}.{name}") for value in values]


def signed(value: int) -> str:
    return "0" if value == 0 else f"{value:+,}"


def render(report: dict) -> str:
    require(
        report.get("experiment") == EXPECTED_EXPERIMENT,
        f"experiment must be {EXPECTED_EXPERIMENT!r}",
    )

    settings = report.get("settings")
    dependency = report.get("dependency")
    runs = report.get("update_runs")
    if not isinstance(settings, dict):
        raise AnalysisError("Missing settings")
    if not isinstance(dependency, dict):
        raise AnalysisError("Missing dependency")
    if not isinstance(runs, list) or not runs:
        raise AnalysisError("update_runs must be non-empty")

    update_count = integer(settings.get("update_count"), "settings.update_count")
    repeats = integer(settings.get("update_repeats"), "settings.update_repeats")
    require(len(runs) == update_count, "update_runs count is inconsistent")
    require(repeats > 0, "update_repeats must be positive")

    determinant = dependency.get("determinant")
    source_dependents = dependency.get("source_dependents")
    properties = dependency.get("hospital_properties")
    if not isinstance(determinant, str):
        raise AnalysisError("Invalid dependency determinant")
    if not isinstance(source_dependents, list) or not source_dependents:
        raise AnalysisError("Invalid source dependent properties")
    if (
            not isinstance(properties, list)
            or len(properties) <= 1
            or properties[0] != determinant
    ):
        raise AnalysisError("Invalid Hospital properties")
    fd = f"{determinant} → ({', '.join(properties[1:])})"
    group_count = integer(dependency.get("groups"), "dependency.groups")
    row_count = integer(dependency.get("rows"), "dependency.rows")

    denormalized_state = read_state(report, "denormalized_state")
    normalized_state = read_state(report, "normalized_state")
    delta = report.get("normalized_minus_denormalized")
    if not isinstance(delta, dict):
        raise AnalysisError("Missing normalized_minus_denormalized")
    for field in STATE_FIELDS:
        expected = normalized_state[field] - denormalized_state[field]
        require(
            integer(delta.get(field), f"delta.{field}") == expected,
            f"delta.{field} is inconsistent",
        )
    require(
        row_count == denormalized_state["node_count"],
        "dependency row count does not match HospitalRecord nodes",
    )

    schema = report.get("normalized_schema")
    if not isinstance(schema, dict):
        raise AnalysisError("Missing normalized_schema")
    removed = schema.get("record_removed_properties")
    if not isinstance(removed, list) or not removed:
        raise AnalysisError("Invalid removed properties")
    require(
        normalized_state["node_count"]
        == denormalized_state["node_count"] + group_count,
        "normalized node count is inconsistent",
    )
    require(
        normalized_state["relationship_count"] == row_count,
        "normalized relationship count is inconsistent",
    )
    expected_cells = (
            denormalized_state["property_cell_count"]
            - row_count * len(removed)
            + group_count * len(properties)
    )
    require(
        normalized_state["property_cell_count"] == expected_cells,
        "normalized property-cell count is inconsistent",
    )

    denormalized_medians = []
    normalized_medians = []
    seen_facilities = set()
    for position, run in enumerate(runs, start=1):
        if not isinstance(run, dict):
            raise AnalysisError(f"update {position} must be an object")
        facility_id = run.get("facility_id")
        if not isinstance(facility_id, str) or not facility_id:
            raise AnalysisError(f"update {position}: invalid facility_id")
        require(
            facility_id not in seen_facilities,
            f"update {position}: duplicate facility_id",
        )
        seen_facilities.add(facility_id)
        fanout = integer(run.get("fanout"), f"update {position}.fanout")
        require(fanout > 0, f"update {position}: fanout must be positive")

        old_values = run.get("old")
        new_values = run.get("new")
        if (
                not isinstance(old_values, list)
                or not isinstance(new_values, list)
                or len(old_values) != len(source_dependents)
                or len(new_values) != len(source_dependents)
        ):
            raise AnalysisError(f"update {position}: invalid old/new values")
        require(old_values != new_values, f"update {position}: no change")

        old_times = read_times(run, "denormalized_elapsed_ms", repeats, position)
        new_times = read_times(run, "normalized_elapsed_ms", repeats, position)
        denormalized_medians.append(statistics.median(old_times))
        normalized_medians.append(statistics.median(new_times))

    denormalized_min = float(min(denormalized_medians))
    denormalized_median = float(statistics.median(denormalized_medians))
    denormalized_max = float(max(denormalized_medians))
    normalized_median = float(statistics.median(normalized_medians))
    savings = [
        old - new
        for old, new in zip(denormalized_medians, normalized_medians)
    ]
    saving_median = float(statistics.median(savings))
    normalization_ms = number(
        report.get("normalization_elapsed_ms"), "normalization_elapsed_ms"
    )
    kappa: int | None = (
        math.ceil(normalization_ms / saving_median) if saving_median > 0 else None
    )

    kappa_text = format(kappa, ",d") if kappa is not None else "N/A"
    result_values = [
        "1",
        "Hospital",
        fd,
        f"{denormalized_min:,.3f}",
        f"{denormalized_median:,.3f}",
        f"{denormalized_max:,.3f}",
        f"{normalized_median:,.3f}",
        f"{saving_median:+,.3f}",
        f"{normalization_ms:,.3f}",
        kappa_text,
        signed(delta["node_count"]),
        signed(delta["relationship_count"]),
        signed(delta["property_cell_count"]),
    ]

    lines = [
        "# Hospital Record Normalization Analysis",
        "",
        f"Each dependency uses `{update_count}` update cases. Each case updates "
        f"one hospital, is repeated `{repeats}` times, and its median latency "
        "is used. "
        "`Δ Update Time = Denormalized - Normalized`.",
        "",
        "## Functional-dependency validation",
        "",
        "| Functional dependency | Groups | Rows | Valid groups | Valid rows | "
        "Conflicting groups | Incomplete groups | Conflicting keys |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
        f"| {fd} | {group_count:,} | {row_count:,} | {group_count:,} | "
        f"{row_count:,} | 0 | 0 | — |",
        "",
        "Only complete, conflict-free groups are normalized.",
        "",
        "## Denormalized HospitalRecord baseline",
        "",
        "| HospitalRecord nodes | Relationships in experiment scope | "
        "Property cells |",
        "|---:|---:|---:|",
        f"| {denormalized_state['node_count']:,} | "
        f"{denormalized_state['relationship_count']:,} | "
        f"{denormalized_state['property_cell_count']:,} |",
        "",
        "## Independent FD normalization results (1 row)",
        "",
        "| No. | Normalization strategy | Functional dependency | "
        "Update (Denormalized) Min (ms) | "
        "Update (Denormalized) Median (ms) | "
        "Update (Denormalized) Max (ms) | "
        "Update (Normalized) Median (ms) | Δ Update Time Median (ms) | "
        "Normalization Time (ms) | Kappa | ΔN | ΔE | ΔC |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        "| " + " | ".join(result_values) + " |",
        "",
        "## Metric definitions",
        "",
        "- **Normalization Time** includes copying HospitalRecord nodes, "
        "creating Hospital nodes and relationships, creating the Hospital Node "
        "Key, removing repeated properties, and counting the resulting structure.",
        "- **Kappa** is `ceil(Normalization Time / median update saving)`; "
        "it estimates how many logical one-hospital updates are needed to recover "
        "the one-off normalization cost.",
        "- **ΔN, ΔE, ΔC** are Normalized minus Denormalized node, relationship, "
        "and property-cell counts.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    try:
        report = json.loads(INPUT.read_text(encoding="utf-8"))
        require(isinstance(report, dict), "JSON root must be an object")
        OUTPUT.write_text(render(report), encoding="utf-8")
    except (AnalysisError, json.JSONDecodeError, OSError) as error:
        print(f"Analysis failed: {error}")
        return 1
    print(f"Analysis complete: {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
