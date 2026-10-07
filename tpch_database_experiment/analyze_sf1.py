#!/usr/bin/env python3
"""Summarise the TPC-H SF 1 runs of new_redundancy.py (both result files)."""

import json
from pathlib import Path
from statistics import median

HERE = Path(__file__).resolve().parent
FILES = [HERE / "back/results_sf1/new_redundancy_results.json",
         HERE / "back/results_sf1/new_redundancy_results_part2.json"]
EDGE = {"L_PS": "e_1", "L_O": "e_2", "PS_P": "e_3", "PS_S": "e_4",
        "O_C": "e_5", "C_N": "e_6", "S_N": "e_7", "N_R": "e_8"}


def fmt(n: float) -> str:
    return (f"+{n / 1e6:.2f}M" if abs(n) >= 1e6 else
            f"+{n / 1e3:.1f}K" if abs(n) >= 1e3 else f"+{n:.0f}")


rows = []
for path in FILES:
    if not path.exists():
        continue
    for c in json.loads(path.read_text())["configurations"]:
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
