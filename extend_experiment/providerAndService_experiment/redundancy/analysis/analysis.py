#!/usr/bin/env python3
"""Build the Provider normalization analysis from the experiment JSON."""

import json
import math
import statistics
from pathlib import Path

INPUT = (
        Path(__file__).resolve().parents[1]
        / "results"
        / "providerandservicecsv"
        / "redundancy_experiment.json"
)
OUTPUT = INPUT.with_suffix(".md")
EXPECTED_EXPERIMENT = "provider_service_record_normalization"
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
    providers_per_update = integer(
        settings.get("providers_per_update"),
        "settings.providers_per_update",
    )
    require(len(runs) == update_count, "update_runs count is inconsistent")
    require(update_count > 0, "update_count must be positive")
    require(repeats > 0, "update_repeats must be positive")
    require(providers_per_update == 1, "providers_per_update must be 1")

    determinant = dependency.get("determinant")
    source_dependents = dependency.get("source_dependents")
    properties = dependency.get("provider_properties")
    if not isinstance(determinant, str) or not determinant:
        raise AnalysisError("Invalid dependency determinant")
    if (
            not isinstance(source_dependents, list)
            or not source_dependents
            or not all(isinstance(field, str) for field in source_dependents)
            or len(set(source_dependents)) != len(source_dependents)
    ):
        raise AnalysisError("Invalid source dependent properties")
    if not isinstance(properties, list):
        raise AnalysisError("Invalid Provider properties")
    require(
        properties == [determinant, *source_dependents],
        "Invalid Provider properties",
    )
    fd = f"{determinant} → ({', '.join(source_dependents)})"

    group_count = integer(dependency.get("groups"), "dependency.groups")
    row_count = integer(dependency.get("rows"), "dependency.rows")
    valid_group_count = integer(
        dependency.get("valid_groups"), "dependency.valid_groups"
    )
    valid_row_count = integer(
        dependency.get("valid_rows"), "dependency.valid_rows"
    )
    conflicting_group_count = integer(
        dependency.get("conflicting_groups"), "dependency.conflicting_groups"
    )
    incomplete_group_count = integer(
        dependency.get("incomplete_groups"), "dependency.incomplete_groups"
    )
    require(0 < valid_group_count <= group_count, "Invalid valid group count")
    require(0 < valid_row_count <= row_count, "Invalid valid row count")
    require(
        0 <= conflicting_group_count <= group_count,
        "Invalid conflicting group count",
    )
    require(
        0 <= incomplete_group_count <= group_count,
        "Invalid incomplete group count",
    )

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
        row_count <= denormalized_state["node_count"],
        "dependency row count exceeds ProviderServiceRecord nodes",
    )

    schema = report.get("normalized_schema")
    if not isinstance(schema, dict):
        raise AnalysisError("Missing normalized_schema")
    require(
        schema.get("pattern")
        == "(ProviderServiceRecord)-[:FOR_PROVIDER]->(Provider)",
        "Invalid normalized schema pattern",
    )
    removed = schema.get("record_removed_properties")
    if not isinstance(removed, list):
        raise AnalysisError("Invalid removed properties")
    require(removed == properties, "Invalid removed properties")
    require(
        normalized_state["node_count"]
        == denormalized_state["node_count"] + valid_group_count,
        "normalized node count is inconsistent",
    )
    require(
        normalized_state["relationship_count"] == valid_row_count,
        "normalized relationship count is inconsistent",
    )
    expected_cells = (
            denormalized_state["property_cell_count"]
            - valid_row_count * len(removed)
            + valid_group_count * len(properties)
    )
    require(
        normalized_state["property_cell_count"] == expected_cells,
        "normalized property-cell count is inconsistent",
    )

    denormalized_medians = []
    normalized_medians = []
    seen_npis = set()
    for position, run in enumerate(runs, start=1):
        if not isinstance(run, dict):
            raise AnalysisError(f"update {position} must be an object")
        npi = run.get("rndrng_npi")
        if not isinstance(npi, str) or not npi:
            raise AnalysisError(f"update {position}: invalid rndrng_npi")
        if valid_group_count >= update_count:
            require(
                npi not in seen_npis,
                f"update {position}: duplicate rndrng_npi",
            )
        seen_npis.add(npi)

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

        denormalized_times = read_times(
            run, "denormalized_elapsed_ms", repeats, position
        )
        normalized_times = read_times(
            run, "normalized_elapsed_ms", repeats, position
        )
        denormalized_medians.append(statistics.median(denormalized_times))
        normalized_medians.append(statistics.median(normalized_times))

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
        "Provider",
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
        "# Provider Service Record Normalization Analysis",
        "",
        f"The dependency uses `{update_count}` update cases. Each case updates "
        f"`{providers_per_update}` Provider, is repeated `{repeats}` times, and "
        "its median latency is used. "
        "`Δ Update Time = Denormalized - Normalized`.",
        "",
        "## Functional-dependency validation",
        "",
        "| Functional dependency | Groups | Rows | Valid groups | Valid rows | "
        "Conflicting groups | Incomplete groups |",
        "|---|---:|---:|---:|---:|---:|---:|",
        f"| {fd} | {group_count:,} | {row_count:,} | "
        f"{valid_group_count:,} | {valid_row_count:,} | "
        f"{conflicting_group_count:,} | {incomplete_group_count:,} |",
        "",
        "Only complete, conflict-free NPI groups are normalized. Empty strings "
        "are treated as valid Provider values.",
        "",
        "## Denormalized ProviderServiceRecord baseline",
        "",
        "| ProviderServiceRecord nodes | Relationships in experiment scope | "
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
        "- **Normalization Time** includes copying ProviderServiceRecord nodes, "
        "creating Provider nodes and relationships, creating the Provider Node "
        "Key, removing repeated properties, and counting the resulting structure.",
        "- **Kappa** is `ceil(Normalization Time / median update saving)`; "
        "it estimates how many logical one-Provider updates are needed to recover "
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
