#!/usr/bin/env python3
"""Re-run ``embedding/scripts/embedding_offshore.py`` unchanged on Community.

Only the connection settings, the output path, and optionally the selection
of FDs and seeds are overridden, to check that the setup reproduces Table 5.

Usage (after ``source ~/neo4j/env.sh``)::

    python rerun_original.py --fds address_country --seeds 1
"""

import argparse
import importlib.util
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ORIGINAL = HERE.parent / "embedding" / "scripts" / "embedding_offshore.py"


def load_original():
    spec = importlib.util.spec_from_file_location("embedding_offshore",
                                                  ORIGINAL)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fds", nargs="*", default=None,
                        help="FD names to run (default: all 12)")
    parser.add_argument("--seeds", type=int, default=10,
                        help="number of seeds, starting at 20260917")
    args = parser.parse_args()

    original = load_original()
    original.DATABASE = "neo4j"
    original.NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
    original.RANDOM_SEEDS = range(20260917, 20260917 + args.seeds)
    if args.fds:
        original.FDS = tuple(fd for fd in original.FDS if fd.name in args.fds)
    original.OUTPUT = HERE / "results" / "rerun_original.json"
    original.main()


if __name__ == "__main__":
    main()
