#!/usr/bin/env python3
"""Summarise the TPC-H SF 1 query runs (one JSON per variant)."""
import json
from pathlib import Path
from statistics import mean, median
D = Path(__file__).resolve().parents[2] / "usecases/results/sf1/query"
for f in sorted(D.glob("q*_mv_*.json")):
    r = json.loads(f.read_text())
    b = [x["baseline"]["client_time_ms"] for x in r]
    t = [x["template_result"] for x in r]
    done = [x["client_time_ms"] for x in t if x["client_time_ms"] is not None]
    capped = sum(x["status"] == "capped" for x in t)
    over = sum(x["status"] == "completed_over_limit" for x in t)
    lb = [x.get("client_time_lower_bound_ms") or x["client_time_ms"] for x in t]
    print(f"{f.stem:42s} n={len(r):2d} TN={mean(b):8.1f} (med {median(b):8.1f}) "
          f"TD={mean(done) if done else float('nan'):8.1f} (med {median(done) if done else float('nan'):8.1f}) "
          f"capped={capped:2d} over={over:2d} TDlb={mean(lb):8.1f} build={r[0]['mv_create_time_ms']/1000:6.1f}s "
          f"errs={sum(x['template_result']['status'] not in ('completed','capped','completed_over_limit') for x in r)}")
