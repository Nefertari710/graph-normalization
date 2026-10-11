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



def build_paper_er_table(cases: list[dict]) -> tuple[list[str], list[str]]:
    """Render the U4 paper table with grouped headers and eight AUC columns."""
    selected = (
        ("service_provider", "service_provider"),
        ("country", "country_codes"),
    )
    grouped = {name: [case for case in cases if case["fd"] == name]
               for name, _ in selected}
    missing = [name for name, rows in grouped.items() if not rows]
    if missing:
        raise ValueError("Missing FDs for the paper table: " + ", ".join(missing))

    headings = []
    for name, determinant in selected:
        rows = grouped[name]
        positives = sum(row["positives"] for row in rows)
        sharing = sum(row["positives_sharing_determinant"] for row in rows)
        agreement = 100 * sharing / positives if positives else float("nan")
        headings.append(f"{determinant} ({agreement:.0f}% agree)")
    seed_counts = {len(rows) for rows in grouped.values()}
    averaging = (f"mean over {next(iter(seed_counts))} seeds"
                 if len(seed_counts) == 1 else "mean over seeds")
    caption = (
        f"U4: Entity resolution, AUC against hard negatives, {averaging}. "
        "Overall includes all duplicate pairs; Disagree includes duplicate "
        "pairs with different determinant values. N = normalized; "
        "D = denormalized. Bold marks the better overall AUC when the "
        "paired Wilcoxon test over seeds has p < 0.05."
    )
    report_lines = [
        "", "## Paper Table: U4 Entity Resolution", "", caption, "",
        "<table>", "<thead>",
        '<tr><th rowspan="3">Method</th>'
        + "".join(f'<th colspan="4">{heading}</th>' for heading in headings)
        + "</tr>",
        "<tr>" + ('<th colspan="2">Overall</th>'
                  '<th colspan="2">Disagree</th>') * 2 + "</tr>",
        "<tr>" + "<th>N</th><th>D</th>" * 4 + "</tr>",
        "</thead>", "<tbody>",
    ]
    terminal_lines = [
        "", caption,
        f"{'':14s}" + "".join(f"{heading:^36s}" for heading in headings),
        f"{'':14s}" + "".join(f"{label:^18s}"
                             for label in ("Overall", "Disagree") * 2),
        f"{'Method':14s}" + "".join(f"{rep:^9s}" for rep in ("N", "D") * 4),
    ]
    for method in METHODS:
        html_cells, text_cells = [], []
        for name, _ in selected:
            rows = grouped[name]
            n = [row["results"][method]["N"]["auc_hard"] for row in rows]
            d = [row["results"][method]["D"]["auc_hard"] for row in rows]
            significant = (
                len(rows) > 1
                and not np.allclose(np.subtract(n, d), 0)
                and wilcoxon(n, d).pvalue < 0.05
            )
            for metric in ("auc_hard", "auc_hard_diff_det"):
                values = {
                    rep: mean([row["results"][method][rep].get(metric)
                               for row in rows])
                    for rep in ("N", "D")
                }
                for rep, other in (("N", "D"), ("D", "N")):
                    value = f"{values[rep]:.3f}"
                    bold = (metric == "auc_hard" and significant
                            and values[rep] > values[other])
                    html_cells.append(
                        f"<td><strong>{value}</strong></td>"
                        if bold else f"<td>{value}</td>"
                    )
                    text_cells.append(f"**{value}**" if bold else value)
        report_lines.append(
            f"<tr><td>{NAMES[method]}</td>" + "".join(html_cells) + "</tr>"
        )
        terminal_lines.append(
            f"{NAMES[method]:14s}"
            + "".join(f"{cell:^9s}" for cell in text_cells)
        )
    report_lines.extend(["</tbody>", "</table>"])
    return report_lines, terminal_lines


def append_paper_er_table() -> None:
    """Append the paper-layout table after the existing report and output."""
    cases = json.loads(RESULTS.read_text(encoding="utf-8"))["cases"]
    report_lines, terminal_lines = build_paper_er_table(cases)
    with REPORT.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(report_lines) + "\n")
    print("\n".join(terminal_lines))
    print(f"\nAppended paper table to: {REPORT}")


if __name__ == "__main__":
    append_paper_er_table()
