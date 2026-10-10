#!/usr/bin/env python3
"""Aggregate results/er_offshore.json (use case U4) for the paper table."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from statistics import fmean

import numpy as np
from scipy.stats import wilcoxon

HERE = Path(__file__).resolve().parents[1]
RESULTS = HERE / "results" / "er_offshore.json"
METHODS = ("property_hash", "fast_rp", "node2vec", "hash_gnn", "graph_sage")
NAMES = {"property_hash": "Property Hash", "fast_rp": "FastRP",
         "node2vec": "Node2Vec", "hash_gnn": "HashGNN",
         "graph_sage": "GraphSAGE"}
METRICS = ("auc_random", "auc_hard", "mrr", "auc_hard_same_det",
           "auc_hard_diff_det")


def mean(values: list) -> float:
    values = [v for v in values if v is not None]
    return fmean(values) if values else float("nan")


def main() -> None:
    cases = json.loads(RESULTS.read_text())["cases"]
    by_fd: dict[str, list[dict]] = defaultdict(list)
    for case in cases:
        by_fd[case["fd"]].append(case)

    print("Samples per FD (mean over seeds):")
    for fd, rows in by_fd.items():
        print(f"  {fd:24s} seeds {len(rows):2d}  usable duplicates "
              f"{rows[0]['usable_duplicate_pairs']:6d}  positives "
              f"{mean([r['positives'] for r in rows]):.0f}  sharing det. "
              f"{mean([r['positives_sharing_determinant'] for r in rows]):.0f}"
              f"  entities {mean([r['entities'] for r in rows]):.0f}")

    for scope, selected in (("all FDs", cases), *by_fd.items()):
        print(f"\nU4 entity resolution, {scope} (mean over seeds"
              f"{' and FDs' if scope == 'all FDs' else ''}); N/D")
        print(f"{'Method':14s} " + "  ".join(f"{m:>17s}" for m in METRICS))
        for method in METHODS:
            cells = []
            for metric in METRICS:
                n = mean([c["results"][method]["N"].get(metric)
                          for c in selected])
                d = mean([c["results"][method]["D"].get(metric)
                          for c in selected])
                cells.append(f"{n:.3f}/{d:.3f}".rjust(17))
            print(f"{NAMES[method]:14s} " + "  ".join(cells))

    print("\nPaired Wilcoxon over seeds (N vs D), auc_hard, per FD:")
    for fd, rows in by_fd.items():
        for method in METHODS:
            n = [r["results"][method]["N"]["auc_hard"] for r in rows]
            d = [r["results"][method]["D"]["auc_hard"] for r in rows]
            if len(rows) > 1 and not np.allclose(np.subtract(n, d), 0):
                p = wilcoxon(n, d).pvalue
                print(f"  {fd:24s} {method:14s} median diff "
                      f"{np.median(np.subtract(n, d)):+.3f}  p = {p:.4f}")


if __name__ == "__main__":
    main()
