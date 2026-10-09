#!/usr/bin/env python3
"""Summarise the TPC-H SF 1 query runs and write a Markdown report."""
import json
from pathlib import Path
from statistics import mean, median
RESULTS = Path(__file__).resolve().parents[2] / "usecases/results/sf1"
D = RESULTS / "query"
REPORT = RESULTS / "report" / "sf1_queries.md"
files = sorted(D.glob("q*_mv_*.json"))
report = [
    "# TPC-H SF1 Query Performance Report",
    "",
    f"Data source: `../query/`, containing {len(files)} strategy result files.",
    "",
    "- Query times are client wall-clock timings in milliseconds (ms); MV build times are in seconds (s).",
    "- TN: baseline query times on the original graph. TD: measured runtimes of completed template queries, including those completed over the limit.",
    "- capped: runs stopped at the time limit; over: runs that completed beyond the time-limit threshold.",
    "- TDlb: the mean of completed runtimes and lower bounds for capped runs, using runs with recorded timings. When capped runs are present, this is only a lower bound on the overall mean runtime.",
    "- Errors: runs in other execution states; — indicates missing timing data.",
    "",
    "| Strategy | Runs | TN mean (ms) | TN median (ms) | TD mean (ms) | TD median (ms) | capped | over | TDlb mean (ms) | MV build (s) | Errors |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
]
for f in files:
    r = json.loads(f.read_text())
    b = [x["baseline"]["client_time_ms"] for x in r]
    t = [x["template_result"] for x in r]
    done = [x["client_time_ms"] for x in t if x["client_time_ms"] is not None]
    capped = sum(x["status"] == "capped" for x in t)
    over = sum(x["status"] == "completed_over_limit" for x in t)
    lb = [x.get("client_time_lower_bound_ms") or x["client_time_ms"] for x in t]
    lb = [value for value in lb if value is not None]
    td_mean = mean(done) if done else float("nan")
    td_median = median(done) if done else float("nan")
    lb_mean = mean(lb) if lb else float("nan")
    build_seconds = r[0]['mv_create_time_ms'] / 1000
    errors = sum(x['status'] not in ('completed', 'capped', 'completed_over_limit') for x in t)
    print(f"{f.stem:42s} n={len(r):2d} TN={mean(b):8.1f} (med {median(b):8.1f}) "
          f"TD={td_mean:8.1f} (med {td_median:8.1f}) "
          f"capped={capped:2d} over={over:2d} TDlb={lb_mean:8.1f} build={build_seconds:6.1f}s "
          f"errs={errors}")
    td_mean_text = f"{td_mean:.1f}" if done else "—"
    td_median_text = f"{td_median:.1f}" if done else "—"
    lb_mean_text = f"{lb_mean:.1f}" if lb else "—"
    report.append(
        f"| [{f.stem}](../query/{f.name}) | {len(r)} | {mean(b):.1f} | {median(b):.1f} "
        f"| {td_mean_text} | {td_median_text} | {capped} | {over} | {lb_mean_text} "
        f"| {build_seconds:.1f} | {errors} |"
    )

REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text("\n".join(report) + "\n", encoding="utf-8")
print(f"\nMarkdown report: {REPORT}")
