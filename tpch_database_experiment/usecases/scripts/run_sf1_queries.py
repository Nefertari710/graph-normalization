#!/usr/bin/env python3
"""Run the 20 TPC-H SF 1 query variants against an already imported graph."""

import json
import os
import subprocess
import sys
from datetime import datetime
from getpass import getpass
from pathlib import Path

# Connection and use-case settings.
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "")  # Empty: prompt when running.
DATABASE = "tpch-sf-1-usecase"
SCALE_FACTOR = 1
RUNS = 50
BATCH_SIZE = 1000
HERE = Path(__file__).resolve().parents[2]
RUNNER = HERE / "query" / "scripts" / "run_benchmark.py"
OUTPUT_DIR = HERE / "usecases" / "results" / "sf1" / "query"

# Cheap builds first, LINEITEM folds last; same order as the Shell version.
VARIANTS = (
    (21, "q21_mv_supplier_nation"),
    (9, "q9_mv_supplier_nation"),
    (7, "q7_mv_supplier_nation1"),
    (10, "q10_mv_customer_nation"),
    (13, "q13_mv_orders_customer"),
    (22, "q22_mv_orders_customer"),
    (3, "q3_mv_orders_customer"),
    (5, "q5_mv_orders_customer"),
    (8, "q8_mv_orders_customer"),
    (16, "q16_mv_partsupp_supplier_part"),
    (20, "q20_mv_partsupp_supplier_part"),
    (11, "q11_mv_partsupp_supplier_nation"),
    (2, "q2_mv_partsupp_supplier_nation_region"),
    (5, "q5_mv_lineitem_orders"),
    (8, "q8_mv_lineitem_orders"),
    (12, "q12_mv_lineitem_orders"),
    (18, "q18_mv_lineitem_orders"),
    (7, "q7_mv_lineitem_orders_customer"),
    (9, "q9_mv_lineitem_orders_partsupp"),
    (20, "q20_mv_lineitem_partsupp_supplier_part"),
)


def is_complete(path: Path) -> bool:
    """Use the original script's basic report-structure check."""
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(report, list) and bool(report) and all(
        isinstance(item, dict) and "template_result" in item for item in report
    )


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    password = PASSWORD
    failed = []
    for number, (query_id, strategy) in enumerate(VARIANTS, start=1):
        report_path = OUTPUT_DIR / f"{strategy}.json"
        log_path = OUTPUT_DIR / f"{strategy}.log"
        if not password:
            password = getpass("Neo4j password: ")
        environment["NEO4J_PASSWORD"] = password
        print(f"[{number}/{len(VARIANTS)}] {strategy} (batch size {BATCH_SIZE}) "
              f"{datetime.now():%H:%M:%S}", flush=True)
        command = [
            sys.executable, str(RUNNER), "--uri", URI, "--user", USER,
            "--database", DATABASE, "--sf", str(SCALE_FACTOR),
            "--runs", str(RUNS), "--query-id", str(query_id),
            "--strategy", strategy, "--batch-size", str(BATCH_SIZE),
            "--output-json", str(report_path),
        ]
        with log_path.open("w", encoding="utf-8") as log:
            result = subprocess.run(
                command, cwd=HERE, env=environment,
                stdout=log, stderr=subprocess.STDOUT,
            )
        if result.returncode in (130, -2):
            print("Query experiment interrupted.", file=sys.stderr)
            return 130
        if result.returncode == 0 and is_complete(report_path):
            lines = log_path.read_text(encoding="utf-8").splitlines()
            if lines:
                print(lines[-3:][0], flush=True)
        else:
            print(f"  failed (see {log_path})", flush=True)
            report_path.unlink(missing_ok=True)
            failed.append(strategy)
    print(f"All variants finished {datetime.now():%H:%M:%S}", flush=True)
    if failed:
        print(f"Unfinished variants: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nQuery experiment interrupted.", file=sys.stderr)
        raise SystemExit(130)
