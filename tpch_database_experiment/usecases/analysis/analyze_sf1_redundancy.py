#!/usr/bin/env python3
"""Summarise the TPC-H SF 1 runs of new_redundancy.py (both result files)."""

import json
from pathlib import Path
from statistics import median

HERE = Path(__file__).resolve().parents[2]
FILES = [HERE / "usecases/results/sf1/redundancy/new_redundancy_results.json",
         HERE / "usecases/results/sf1/redundancy/new_redundancy_results_part2.json"]
REPORT = HERE / "usecases/results/sf1/report/sf1_redundancy.md"
EDGE = {"L_PS": "e_1", "L_O": "e_2", "PS_P": "e_3", "PS_S": "e_4",
        "O_C": "e_5", "C_N": "e_6", "S_N": "e_7", "N_R": "e_8"}


def fmt(n: float) -> str:
    return (f"+{n / 1e6:.2f}M" if abs(n) >= 1e6 else
            f"+{n / 1e3:.1f}K" if abs(n) >= 1e3 else f"+{n:.0f}")


rows = []
sources = []
for path in FILES:
    if not path.exists():
        continue
    result = json.loads(path.read_text(encoding="utf-8"))
    configurations = result["configurations"]
    sources.append(
        f"- `{path.name}`: {len(configurations)} configurations; "
        f"update groups per target table: {result.get('update_group_count', 'unknown')}; "
        f"repeats per group: {result.get('update_repeat_count', 'unknown')}."
    )
    for c in configurations:
        edges = ",".join(EDGE[e] for e in c["folded_edges"])
        for target, u in c["updates"].items():
            rows.append((target, edges, u["min_ms"], u["median_ms"], u["max_ms"],
                         u["normalized_update"]["median_ms"],
                         u["normalization_save"]["median_ms"],
                         c["normalization"]["elapsed_ms"] / 1000,
                         c["structural_delta"]["delta_property_cells"],
                         median(s["touched_nodes"] for s in u["samples"])))
print(f"{'target':6s} {'edges':18s} {'UDmin':>7s} {'UDmed':>7s} {'UDmax':>7s} "
      f"{'UNmed':>6s} {'dU':>7s} {'B_N s':>7s} {'dP':>8s} {'copies':>7s}")
for r in rows:
    print(f"{r[0]:6s} {r[1]:18s} {r[2]:7.1f} {r[3]:7.1f} {r[4]:7.1f} {r[5]:6.1f} "
          f"{r[6]:+7.1f} {r[7]:7.1f} {fmt(r[8]):>8s} {r[9]:7.0f}")
print("\nLaTeX rows:")
for r in rows:
    print(f"${r[0]}$ & $\\{{{r[1]}\\}}$ & {r[3]:.1f} & {r[5]:.1f} & "
          f"{r[6]:+.1f} & {r[7]:.1f} & {fmt(r[8])} \\\\")

report = [
    "# TPC-H SF1 Redundancy and Update Experiment Report",
    "",
    "Sources:",
    "",
    *(sources or ["No input result files found."]),
    "",
    "UD is the denormalized graph update time and UN is the normalized source graph update time, in milliseconds (ms). "
    "Each update group uses the median time of repeated runs before computing the table's minimum, median and maximum; restoring original values is excluded.",
    "",
    "dU is the median of paired-group UD - UN differences, in ms; a positive value means updating the normalized source graph is faster. "
    "B_N is the time to rebuild normalized nodes, relationships and indexes from the denormalized graph, in seconds (s), excluding cleanup.",
    "",
    "dP is the increase in non-null TPC-H property values relative to the normalized source graph (excluding `nr_*` properties). "
    "K = 1,000, M = 1,000,000. copies is the median number of nodes touched per update group.",
    "",
    "Edge mapping: " + ", ".join(f"`{number}` = `{edge}`" for edge, number in EDGE.items()) + ".",
    "",
    "| target | edges | UD min (ms) | UD median (ms) | UD max (ms) | UN median (ms) | dU (ms) | B_N (s) | dP | copies |",
    "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
]
for r in rows:
    report.append(
        f"| {r[0]} | {r[1]} | {r[2]:.1f} | {r[3]:.1f} | {r[4]:.1f} | {r[5]:.1f} | "
        f"{r[6]:+.1f} | {r[7]:.1f} | {fmt(r[8])} | {r[9]:.0f} |"
    )
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text("\n".join(report) + "\n", encoding="utf-8")
print(f"\nMarkdown report: {REPORT}")
