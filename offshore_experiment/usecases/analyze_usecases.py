#!/usr/bin/env python3
"""Aggregate results/usecases_offshore.json into the numbers of Tables 6-8.

Prints a reproduction check against Table 5, the U1/U2/U3 summaries, and
writes the U3 figure to results/usecase_partial_updates.png.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from statistics import fmean

import numpy as np
from scipy.stats import wilcoxon

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results" / "usecases_offshore.json"
FIGURE = HERE / "results" / "usecase_partial_updates.png"
METHODS = ("property_hash", "fast_rp", "node2vec", "hash_gnn", "graph_sage")
NAMES = {"property_hash": "Property Hash", "fast_rp": "FastRP",
         "node2vec": "Node2Vec", "hash_gnn": "HashGNN",
         "graph_sage": "GraphSAGE"}
UPDATE_METHODS = ("property_hash", "fast_rp", "hash_gnn", "graph_sage")
FRACTIONS = ("0.0", "0.25", "0.5", "0.75", "1.0")
NOISE_LIMIT = 0.01

# Table 5 of the paper (same-determinant medians, N / D), for the check.
TABLE5 = {
    "address_country": {"property_hash": (.478, .559), "fast_rp": (.723, .294), "node2vec": (.998, .511), "hash_gnn": (.639, .593), "graph_sage": (.975, .959)},
    "service_provider": {"property_hash": (.204, .419), "fast_rp": (.488, .388), "node2vec": (.553, .551), "hash_gnn": (.558, .552), "graph_sage": (.975, .990)},
    "other_jurisdiction": {"property_hash": (.689, .744), "fast_rp": (.543, .421), "node2vec": (.998, .397), "hash_gnn": (.691, .652), "graph_sage": (.967, .939)},
}


def holm(p_values: list[float]) -> list[float]:
    order = np.argsort(p_values)
    adjusted = np.empty(len(p_values))
    running = 0.0
    for rank, index in enumerate(order):
        running = max(running, (len(p_values) - rank) * p_values[index])
        adjusted[index] = min(1.0, running)
    return adjusted.tolist()


def main() -> None:
    cases = json.loads(RESULTS.read_text())["cases"]
    by_fd: dict[str, list[dict]] = defaultdict(list)
    for case in cases:
        by_fd[case["fd"]].append(case)
    fds = list(by_fd)
    print(f"{len(cases)} cases, {len(fds)} FDs, seeds per FD: "
          f"{sorted({len(v) for v in by_fd.values()})}\n")

    print("Reproduction check (mean over seeds of same-LHS medians, N/D):")
    for fd, expected in TABLE5.items():
        if fd not in by_fd:
            continue
        for method in METHODS:
            got = tuple(fmean(c["median_same"][method][r] for c in by_fd[fd])
                        for r in ("N", "D"))
            print(f"  {fd:20s} {method:14s} ours {got[0]:.3f}/{got[1]:.3f}"
                  f"  table {expected[method][0]:.3f}/{expected[method][1]:.3f}")

    print("\nU1: ROC-AUC (mean over FDs and seeds); wins = FDs with Holm-"
          "corrected Wilcoxon p < 0.05 over seeds")
    print(f"{'Method':14s} AUC_N  AUC_D  AUC_R  WinsN WinsD")
    for method in METHODS:
        means = {r: fmean(c["auc"][method][r] for c in cases)
                 for r in ("N", "D", "R")}
        p_values, signs = [], []
        for fd in fds:
            n = [c["auc"][method]["N"] for c in by_fd[fd]]
            d = [c["auc"][method]["D"] for c in by_fd[fd]]
            differences = np.subtract(n, d)
            if np.allclose(differences, 0):
                p_values.append(1.0)
            else:
                p_values.append(float(wilcoxon(n, d).pvalue))
            signs.append(np.median(differences))
        adjusted = holm(p_values)
        wins_n = sum(1 for p, s in zip(adjusted, signs) if p < .05 and s > 0)
        wins_d = sum(1 for p, s in zip(adjusted, signs) if p < .05 and s < 0)
        print(f"{NAMES[method]:14s} {means['N']:.3f}  {means['D']:.3f}  "
              f"{means['R']:.3f}  {wins_n:5d} {wins_d:5d}")

    print("\nU2: retrieval among sampled nodes (mean over FDs and seeds)")
    print(f"{'Method':14s} RPrecN RPrecD MRR_N  MRR_D  MAP_N  MAP_D  LiftN  LiftD")
    for method in METHODS:
        value = {(m, r): fmean(c["retrieval"][method][r][m] for c in cases)
                 for m in ("r_precision", "mrr", "map", "lift")
                 for r in ("N", "D")}
        print(f"{NAMES[method]:14s} "
              + "  ".join(f"{value[(m, r)]:.3f}" if m != "lift"
                          else f"{value[(m, r)]:.1f}"
                          for m in ("r_precision", "mrr", "map", "lift")
                          for r in ("N", "D")))

    print("\nU3: updates (mean over FDs and seeds)")
    written = {r: fmean(c["update"]["fast_rp"]["written_values"][r]
                        for c in cases) for r in ("N", "D")}
    print(f"  written values per update: N {written['N']:.1f}, "
          f"D {written['D']:.1f}")
    print(f"{'Method':14s} RF_N   RF_D   DriftInN DriftInD DriftOutN "
          f"DriftOutD  WGS(q=.5)/WGS(q=1) D")
    curves = {}
    for method in UPDATE_METHODS:
        # Exclude cases in which re-embedding without any change is not
        # reproducible (null-run drift above NOISE_LIMIT).
        u = [c["update"][method] for c in cases
             if max(abs(c["update"][method]["drift_noise"][r][w])
                    for r in ("N", "D") for w in ("inside", "outside"))
             <= NOISE_LIMIT]
        print(f"{'':14s} {len(u)} of {len(cases)} cases with reproducible "
              f"null re-embedding")
        ratio_field = (fmean(x["receptive_field"]["N"] / x["receptive_field"]["D"]
                             for x in u) if u[0]["receptive_field"] else None)
        if ratio_field is not None:
            print(f"{'':14s} mean receptive-field ratio N/D: {ratio_field:.3f}")
        field = ([fmean(x["receptive_field"][r] for x in u)
                  for r in ("N", "D")] if u[0]["receptive_field"] else
                 [float("nan")] * 2)
        drift = {(r, w): fmean(x["drift"][r][w] for x in u)
                 for r in ("N", "D") for w in ("inside", "outside")}
        ratio = fmean(x["within_group_similarity"]["D_q=0.5"]
                      / x["within_group_similarity"]["D_q=1.0"] for x in u)
        curves[method] = [fmean(x["within_group_similarity"][f"D_q={q}"]
                                / x["within_group_similarity"]["D_q=1.0"]
                                for x in u) for q in FRACTIONS]
        print(f"{NAMES[method]:14s} {field[0]:6.0f} {field[1]:6.0f} "
              f"{drift[('N', 'inside')]:8.4f} {drift[('D', 'inside')]:8.4f} "
              f"{drift[('N', 'outside')]:9.4f} {drift[('D', 'outside')]:9.4f}"
              f"  {ratio:.4f}")
        noise = max(abs(x["drift_noise"][r][w]) for x in u
                    for r in ("N", "D") for w in ("inside", "outside"))
        print(f"{'':14s} max drift of null re-embedding: {noise:.2e}")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    figure, axis = plt.subplots(figsize=(3.4, 2.2))
    xs = [float(q) for q in FRACTIONS]
    for method, curve in curves.items():
        axis.plot(xs, curve, marker="o", markersize=3, label=NAMES[method])
    axis.axhline(1.0, color="grey", linewidth=0.8, linestyle="--")
    axis.set_xlabel("fraction $q$ of updated copies (D)")
    axis.set_ylabel("within-group sim.\nrelative to $q=1$")
    axis.legend(fontsize=6, frameon=False)
    axis.tick_params(labelsize=7)
    figure.tight_layout()
    figure.savefig(FIGURE, dpi=300)
    print(f"\nFigure written to {FIGURE}")


if __name__ == "__main__":
    main()
