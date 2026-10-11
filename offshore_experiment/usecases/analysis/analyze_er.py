#!/usr/bin/env python3
"""Aggregate U4 results for terminal tables and results/report/er_offshore.md."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from statistics import fmean

import numpy as np
from scipy.stats import wilcoxon


HERE = Path(__file__).resolve().parents[1]
RESULTS = HERE / "results" / "er_offshore.json"
REPORT = HERE / "results" / "report" / "er_offshore.md"
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

    report = [
        "# Offshore Entity Resolution (U4)",
        "",
        "Source: [er_offshore.json](../er_offshore.json).",
        "",
        "N = normalized representation; D = denormalized representation. "
        "Metric cells show N/D. Unavailable metrics appear as `nan`.",
        "",
        "## Samples per FD",
        "",
        "Sample counts are means over seeds, except usable duplicate pairs, "
        "which are taken from the first case for each FD.",
        "",
        "| FD | Seeds | Usable duplicate pairs | Positive pairs | "
        "Positives sharing determinant | Entities |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    print("Samples per FD (mean over seeds):")
    for fd, rows in by_fd.items():
        positives = mean([r["positives"] for r in rows])
        sharing = mean([r["positives_sharing_determinant"] for r in rows])
        entities = mean([r["entities"] for r in rows])
        print(f"  {fd:24s} seeds {len(rows):2d}  usable duplicates "
              f"{rows[0]['usable_duplicate_pairs']:6d}  positives "
              f"{positives:.0f}  sharing det. {sharing:.0f}"
              f"  entities {entities:.0f}")
        report.append(f"| {fd} | {len(rows)} | "
                      f"{rows[0]['usable_duplicate_pairs']} | {positives:.0f} | "
                      f"{sharing:.0f} | {entities:.0f} |")

    for scope, selected in (("all FDs", cases), *by_fd.items()):
        print(f"\nU4 entity resolution, {scope} (mean over seeds"
              f"{' and FDs' if scope == 'all FDs' else ''}); N/D")
        print(f"{'Method':14s} " + "  ".join(f"{m:>17s}" for m in METRICS))
        report.extend([
            "",
            f"## Metrics: {scope}",
            "",
            "Mean over seeds" + (" and FDs." if scope == "all FDs" else "."),
            "",
            "| Method | " + " | ".join(METRICS) + " |",
            "| --- | " + " | ".join("---:" for _ in METRICS) + " |",
        ])
        for method in METHODS:
            cells = []
            for metric in METRICS:
                n = mean([c["results"][method]["N"].get(metric)
                          for c in selected])
                d = mean([c["results"][method]["D"].get(metric)
                          for c in selected])
                cells.append(f"{n:.3f}/{d:.3f}".rjust(17))
            print(f"{NAMES[method]:14s} " + "  ".join(cells))
            report.append(f"| {NAMES[method]} | "
                          + " | ".join(cell.strip() for cell in cells) + " |")

    print("\nPaired Wilcoxon over seeds (N vs D), auc_hard, per FD:")
    report.extend([
        "",
        "## Paired Wilcoxon tests",
        "",
        "N versus D for `auc_hard`, paired over seeds within each FD. "
        "Only comparisons with more than one seed and differences that are "
        "not all close to zero are tested. Median differences are N minus D.",
        "",
        "| FD | Method | Median difference (N - D) | p-value |",
        "| --- | --- | ---: | ---: |",
    ])
    tested = 0
    for fd, rows in by_fd.items():
        for method in METHODS:
            n = [r["results"][method]["N"]["auc_hard"] for r in rows]
            d = [r["results"][method]["D"]["auc_hard"] for r in rows]
            if len(rows) > 1 and not np.allclose(np.subtract(n, d), 0):
                p = wilcoxon(n, d).pvalue
                median_difference = np.median(np.subtract(n, d))
                print(f"  {fd:24s} {method:14s} median diff "
                      f"{median_difference:+.3f}  p = {p:.4f}")
                report.append(f"| {fd} | {NAMES[method]} | "
                              f"{median_difference:+.3f} | {p:.4f} |")
                tested += 1
    if not tested:
        report.extend(["", "No comparisons met the testing rule above."])

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text("\n".join(report) + "\n", encoding="utf-8")
    print(f"\nSaved report: {REPORT}")


if __name__ == "__main__":
    main()
