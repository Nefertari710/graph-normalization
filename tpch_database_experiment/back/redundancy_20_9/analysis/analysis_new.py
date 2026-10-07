#!/usr/bin/env python3
"""Build the final strategy/terminal comparison table from the experiment JSON."""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (
    SCRIPT_DIR.parent / "results" / "new" / "tpch-sf-001"
    / "new_redundancy_results.json"
)
DEFAULT_OUTPUT: Path | None = None
EXPECTED_EXPERIMENT = "tpch_256_fold_terminal_update"
EDGE_ENDPOINTS = {
    "L_PS": ("L", "PS"), "L_O": ("L", "O"),
    "PS_P": ("PS", "P"), "PS_S": ("PS", "S"),
    "O_C": ("O", "C"), "C_N": ("C", "N"),
    "S_N": ("S", "N"), "N_R": ("N", "R"),
}
EDGE_ORDER = {edge: position for position, edge in enumerate(EDGE_ENDPOINTS)}


class AnalysisError(RuntimeError):
    pass


@dataclass(frozen=True)
class Row:
    mask: int
    edges: tuple[str, ...]
    strategy: str
    terminal: str
    effective_paths: tuple[tuple[str, ...], ...]
    denorm_min_ms: float
    denorm_median_ms: float
    denorm_max_ms: float
    normalized_median_ms: float
    save_median_ms: float
    normalization_ms: float
    kappa: int | None
    delta_n: int
    delta_e: int
    delta_c: int


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AnalysisError(message)


def field(value: Any, *path: str) -> Any:
    current = value
    for part in path:
        if not isinstance(current, dict) or part not in current:
            raise AnalysisError(f"Missing required field: {'.'.join(path)}")
        current = current[part]
    return current


def number(value: Any, label: str, *, nonnegative: bool = False) -> float:
    require(isinstance(value, (int, float)) and not isinstance(value, bool),
            f"{label} must be numeric")
    result = float(value)
    require(math.isfinite(result), f"{label} must be finite")
    require(not nonnegative or result >= 0, f"{label} must be non-negative")
    return result


def integer(value: Any, label: str, *, nonnegative: bool = False) -> int:
    require(isinstance(value, int) and not isinstance(value, bool),
            f"{label} must be an integer")
    require(not nonnegative or value >= 0, f"{label} must be non-negative")
    return value


def strings(value: Any, label: str) -> list[str]:
    require(isinstance(value, list), f"{label} must be a list")
    require(all(isinstance(item, str) and item for item in value),
            f"{label} must contain non-empty strings")
    return value


def paths_from_fold(fold: str, terminal: str) -> set[tuple[str, ...]]:
    codes = fold.split("__")
    require(all(code in EDGE_ENDPOINTS for code in codes),
            f"unknown edge in fold {fold!r}")
    targets = {EDGE_ENDPOINTS[code][1] for code in codes}
    roots = [EDGE_ENDPOINTS[code][0] for code in codes
             if EDGE_ENDPOINTS[code][0] not in targets]
    roots = list(dict.fromkeys(roots))
    require(len(roots) == 1, f"fold {fold!r} must have one root")
    outgoing: dict[str, list[str]] = {}
    for code in codes:
        outgoing.setdefault(EDGE_ENDPOINTS[code][0], []).append(code)
    paths: set[tuple[str, ...]] = set()

    def visit(node: str, path: tuple[str, ...]) -> None:
        if node == terminal:
            paths.add(path)
            return
        for code in outgoing.get(node, []):
            visit(EDGE_ENDPOINTS[code][1], (*path, code))

    visit(roots[0], ())
    return {path for path in paths if path}


def effective_paths(update: dict[str, Any], terminal: str,
                    label: str) -> tuple[tuple[str, ...], ...]:
    lookups = field(update, "indexed_lookup")
    require(isinstance(lookups, list) and lookups,
            f"{label}.indexed_lookup must be a non-empty list")
    folds: set[str] = set()
    for position, lookup in enumerate(lookups, start=1):
        require(isinstance(lookup, dict),
                f"{label}.indexed_lookup[{position}] must be an object")
        fold = field(lookup, "fold")
        require(isinstance(fold, str) and fold,
                f"{label}.indexed_lookup[{position}].fold must be a string")
        folds.add(fold)
    paths = {path for fold in folds for path in paths_from_fold(fold, terminal)}
    require(paths, f"{label} has no fold path ending at {terminal}")
    return tuple(sorted(paths, key=lambda path: tuple(EDGE_ORDER[x] for x in path)))


def parse_rows(report: dict[str, Any]) -> list[Row]:
    require(field(report, "experiment") == EXPECTED_EXPERIMENT,
            f"experiment must be {EXPECTED_EXPERIMENT!r}")
    configurations = field(report, "configurations")
    require(isinstance(configurations, list), "configurations must be a list")
    rows: list[Row] = []
    seen_masks: set[int] = set()
    for position, config in enumerate(configurations, start=1):
        require(isinstance(config, dict), f"configuration {position} must be an object")
        mask = integer(field(config, "mask"), f"configuration {position}.mask",
                       nonnegative=True)
        require(mask not in seen_masks, f"duplicate mask {mask}")
        seen_masks.add(mask)
        edges = strings(field(config, "folded_edges"), f"mask {mask}.folded_edges")
        terminals = strings(field(config, "terminal_relations"),
                            f"mask {mask}.terminal_relations")
        strategy = " + ".join(edges) if edges else "Normalized"
        normalization_ms = number(field(config, "normalization", "elapsed_ms"),
                                  f"mask {mask}.normalization.elapsed_ms",
                                  nonnegative=True)
        delta = field(config, "structural_delta")
        delta_n = integer(field(delta, "delta_nodes"), f"mask {mask}.delta_nodes")
        delta_e = integer(field(delta, "delta_relationships"),
                          f"mask {mask}.delta_relationships")
        delta_c = integer(field(delta, "delta_property_cells"),
                          f"mask {mask}.delta_property_cells")
        updates = field(config, "updates")
        require(isinstance(updates, dict), f"mask {mask}.updates must be an object")
        require(set(updates) == set(terminals),
                f"mask {mask}.updates must match terminal_relations")
        for terminal in terminals:
            update = field(updates, terminal)
            label = f"mask {mask}.{terminal}"
            denorm_min = number(field(update, "min_ms"),
                                f"{label}.min_ms", nonnegative=True)
            denorm_median = number(field(update, "median_ms"),
                                   f"{label}.median_ms", nonnegative=True)
            denorm_max = number(field(update, "max_ms"),
                                f"{label}.max_ms", nonnegative=True)
            normalized = number(field(update, "normalized_update", "median_ms"),
                                f"{label}.normalized median",
                                nonnegative=True)
            saving = number(field(update, "normalization_save", "median_ms"),
                            f"{label}.normalization save")
            rows.append(Row(
                mask, tuple(edges), strategy, terminal,
                effective_paths(update, terminal, label),
                denorm_min, denorm_median, denorm_max,
                normalized, saving, normalization_ms,
                math.ceil(normalization_ms / saving) if saving > 0 else None,
                delta_n, delta_e, delta_c,
            ))
    return rows


def signed(value: int) -> str:
    return "0" if value == 0 else f"{value:+,}"


def filtered_rows(rows: list[Row]) -> list[Row]:
    representatives: dict[tuple[str, tuple[tuple[str, ...], ...]], Row] = {}
    for row in rows:
        key = (row.terminal, row.effective_paths)
        current = representatives.get(key)
        if current is None or (len(row.edges), row.mask) < (len(current.edges), current.mask):
            representatives[key] = row
    return sorted(representatives.values(), key=lambda row: (row.mask, row.terminal))


def render_table(rows: list[Row]) -> str:
    headers = [
        "No.", "De-normalization strategy", "Terminal",
        "Update (De-normalization strategy) Min (ms)",
        "Update (De-normalization strategy) Median (ms)",
        "Update (De-normalization strategy) Max (ms)",
        "Update (Normalization strategy) Median (ms)",
        "Δ Update Time (Normalization Save) Median (ms)",
        "Normalization Time (ms)", "Kappa", "ΔN", "ΔE", "ΔC",
    ]
    alignments = ["---:", "---", "---", "---:", "---:", "---:", "---:",
                  "---:", "---:", "---:", "---:", "---:", "---:"]
    lines = ["| " + " | ".join(headers) + " |",
             "|" + "|".join(alignments) + "|"]
    for position, row in enumerate(rows, start=1):
        kappa = f"{row.kappa:,}" if row.kappa is not None else "N/A"
        cells = [str(position), row.strategy, row.terminal,
                 f"{row.denorm_min_ms:,.3f}", f"{row.denorm_median_ms:,.3f}",
                 f"{row.denorm_max_ms:,.3f}", f"{row.normalized_median_ms:,.3f}",
                 f"{row.save_median_ms:+,.3f}", f"{row.normalization_ms:,.3f}",
                 kappa, signed(row.delta_n), signed(row.delta_e), signed(row.delta_c)]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def render(rows: list[Row], repeat_count: int, case_count: int) -> str:
    filtered = filtered_rows(rows)
    measurement = (
        f"Each of the `{case_count}` update cases is measured once."
        if repeat_count == 1 else
        f"Each of the `{case_count}` update cases is repeated `{repeat_count}` "
        "times, and its median latency is used."
    )
    return "\n".join([
        "# Normalization Redundancy Analysis", "",
        measurement + " The reported minimum, median, and maximum are calculated "
        f"across the `{case_count}` per-case latencies. `Δ Update Time` is the "
        "median of the paired De-normalized minus Normalized per-case latencies.", "",
        f"## Complete fold configurations ({len(rows)} rows)", "",
        render_table(rows), "",
        f"## Filtered effective redundancy cases ({len(filtered)} rows)", "",
        ("Rows are grouped by Terminal and effective redundancy-inducing paths. "
         "For each group, the configuration with the fewest folded edges, then "
         "the lowest mask, is retained."), "",
        render_table(filtered), "",
    ])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the final comparison table.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="Markdown output (default: input path with .md suffix)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = args.input.expanduser().resolve()
    output_path = (args.output.expanduser().resolve() if args.output is not None
                   else input_path.with_suffix(".md"))
    try:
        require(input_path != output_path, "Input and output paths must differ")
        report = json.loads(input_path.read_text(encoding="utf-8"))
        require(isinstance(report, dict), "JSON root must be an object")
        repeat_count = integer(report.get("update_repeat_count", 1),
                               "update_repeat_count", nonnegative=True)
        require(repeat_count > 0, "update_repeat_count must be positive")
        case_count = integer(field(report, "update_group_count"),
                             "update_group_count", nonnegative=True)
        markdown = render(parse_rows(report), repeat_count, case_count)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown, encoding="utf-8")
    except (AnalysisError, json.JSONDecodeError, OSError) as exc:
        print(f"Analysis failed: {exc}", file=sys.stderr)
        return 1
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
