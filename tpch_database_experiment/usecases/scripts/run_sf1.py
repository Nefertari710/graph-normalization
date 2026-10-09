#!/usr/bin/env python3
"""Run the SF 1 import/update experiments, then the query experiments."""

import os
import subprocess
import sys
from getpass import getpass
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


def main() -> int:
    environment = os.environ.copy()
    if not environment.get("NEO4J_PASSWORD"):
        environment["NEO4J_PASSWORD"] = getpass("Neo4j password: ")

    for step, script_name in enumerate(
        (
            "components/run_sf1_redundancy.py",
            "components/run_sf1_queries.py",
        ),
        start=1,
    ):
        print(f"[{step}/2] Running {script_name} ...", flush=True)
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / script_name)],
            cwd=SCRIPT_DIR, env=environment,
        )
        if result.returncode != 0:
            print(f"{script_name} failed; stopping.", file=sys.stderr)
            return 130 if result.returncode == -2 else result.returncode

    print("All SF 1 experiments finished.", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nSF 1 experiments interrupted.", file=sys.stderr)
        raise SystemExit(130)
