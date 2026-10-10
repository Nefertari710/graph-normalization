#!/usr/bin/env python3
"""Update cost of redundancy elimination on the full Offshore graph (RQ1).

For each of the 12 Offshore FDs X -> Y of the embedding experiment:

1. Normalization (timed, B_N): create one dimension node per valid
   determinant value (groups without missing or conflicting values, as in the
   embedding experiment) carrying X and Y, link every source node of the
   group to it, and index the dimension nodes on X.  The source nodes keep
   their copies, so the de-normalized and the normalized representation
   coexist and are measured on the same data.
2. Updates: for up to 50 sampled groups, correct the first dependent value
   - de-normalized (D): on every source node of the group;
   - normalized (N): once, on the dimension node.
   Each update is followed by an (also timed) update back to the original
   value; the median over all repetitions is reported per group.
3. Cleanup: dimension nodes, relationships and the dimension index are
   removed; source-node indexes created for the experiment are kept unless
   --drop-source-indexes is given.

Set ``DEBUG_NEO4J_PASSWORD`` below or use ``NEO4J_PASSWORD`` in the environment;
otherwise the script prompts for the password when running.

Usage::

    python updates_offshore.py                 # all 12 FDs
    python updates_offshore.py --fds address_country --groups 5 --repeats 2
"""

from __future__ import annotations

import argparse
import getpass
import importlib.util
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

HERE = Path(__file__).resolve().parent
ORIGINAL = HERE.parent / "embedding" / "scripts" / "embedding_offshore.py"
OUTPUT = HERE / "results" / "updates_offshore.json"
DIMENSION = "OFFSHORE_UPD_DIM"
RELATIONSHIP = "HAS_UPD_DIM"
SEED = 20261006
SUFFIX = " [corrected]"
DEBUG_NEO4J_PASSWORD: str | None = ""  # Empty: use the environment or prompt.


def load_original():
    spec = importlib.util.spec_from_file_location("embedding_offshore",
                                                  ORIGINAL)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


O = load_original()


def run(session: Any, query: str, **parameters: Any) -> Any:
    return session.run(query, parameters).consume()


def timed(session: Any, query: str, **parameters: Any) -> tuple[float, Any]:
    start = time.perf_counter()
    summary = session.run(query, parameters).consume()
    return (time.perf_counter() - start) * 1000.0, summary


def source_index(session: Any, fd: Any) -> None:
    name = f"upd_src_{fd.source_label}_{fd.determinant}".lower()
    run(session, f"CREATE INDEX {name} IF NOT EXISTS "
                 f"FOR (n:{fd.source_label}) ON (n.{fd.determinant})")
    run(session, "CALL db.awaitIndexes(3000)")


def cleanup(session: Any) -> None:
    while True:
        summary = run(session, f"""
MATCH (d:{DIMENSION}) WITH d LIMIT 2000 DETACH DELETE d""")
        if summary.counters.nodes_deleted == 0:
            break
    run(session, "DROP INDEX upd_dim_key IF EXISTS")


def normalize(session: Any, fd: Any, groups: list[dict]) -> dict[str, Any]:
    """Create dimension nodes and links for all valid groups (timed)."""
    rows = [{"key": g["key"], "props": dict(zip(fd.dependents, g["rhs"]))}
            for g in groups]
    start = time.perf_counter()
    for begin in range(0, len(rows), 500):
        run(session, f"""
UNWIND $rows AS row
CREATE (d:{DIMENSION})
SET d.key = row.key, d += row.props
""", rows=rows[begin:begin + 500])
    run(session, f"CREATE INDEX upd_dim_key IF NOT EXISTS "
                 f"FOR (d:{DIMENSION}) ON (d.key)")
    run(session, "CALL db.awaitIndexes(3000)")
    links = 0
    for group in groups:
        summary = run(session, f"""
MATCH (d:{DIMENSION} {{key: $key}})
MATCH (e:{fd.source_label}) WHERE e.{fd.determinant} = $key
CREATE (e)-[:{RELATIONSHIP}]->(d)
""", key=group["key"])
        links += summary.counters.relationships_created
    seconds = time.perf_counter() - start
    return {"normalization_seconds": seconds, "dimension_nodes": len(groups),
            "links": links}


def measure(session: Any, fd: Any, group: dict, repeats: int) -> dict:
    dependent = fd.dependents[0]
    original = group["rhs"][0]
    corrected = str(original) + SUFFIX
    d_times, n_times = [], []
    for _ in range(repeats):
        for value in (corrected, original):
            ms, summary = timed(session, f"""
MATCH (e:{fd.source_label}) WHERE e.{fd.determinant} = $key
SET e.{dependent} = $value
""", key=group["key"], value=value)
            d_times.append(ms)
            ms, _ = timed(session, f"""
MATCH (d:{DIMENSION} {{key: $key}})
SET d.{dependent} = $value
""", key=group["key"], value=value)
            n_times.append(ms)
    return {"key": str(group["key"]), "copies": group["fanout"],
            "d_median_ms": median(d_times), "n_median_ms": median(n_times),
            "paired_diff_ms": median(d - n for d, n in zip(d_times, n_times))}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fds", nargs="*", default=None)
    parser.add_argument("--groups", type=int, default=50)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    fds = [fd for fd in O.FDS if not args.fds or fd.name in args.fds]

    password = DEBUG_NEO4J_PASSWORD or os.environ.get("NEO4J_PASSWORD")
    if not password:
        password = getpass.getpass(f"Neo4j password for {O.NEO4J_USER}: ")

    from neo4j import GraphDatabase
    with GraphDatabase.driver(O.NEO4J_URI, auth=(
            O.NEO4J_USER, password)) as driver:
        with driver.session(database="offshorecsv") as session:
            report: dict[str, Any] = {
                "experiment": "offshore_updates_full_graph",
                "created_at": datetime.now().astimezone().isoformat(),
                "settings": {"groups": args.groups, "repeats": args.repeats,
                             "seed": SEED},
                "fds": {}}
            if args.resume and OUTPUT.exists():
                report["fds"] = json.loads(OUTPUT.read_text())["fds"]
            cleanup(session)
            for number, fd in enumerate(fds, start=1):
                if fd.name in report["fds"]:
                    continue
                start = time.perf_counter()
                source_index(session, fd)
                groups = O.read_valid_groups(session, fd)
                try:
                    built = normalize(session, fd, groups)
                    rng = random.Random(SEED + number)
                    sample = rng.sample(groups, min(args.groups, len(groups)))
                    cases = [measure(session, fd, g, args.repeats)
                             for g in sample]
                finally:
                    cleanup(session)
                report["fds"][fd.name] = {
                    "node_type": fd.source_label,
                    "fd": f"{fd.determinant} -> {', '.join(fd.dependents)}",
                    "valid_groups": len(groups),
                    "valid_nodes": sum(g["fanout"] for g in groups),
                    "redundant_values": sum(g["fanout"] - 1 for g in groups)
                    * len(fd.dependents),
                    **built, "cases": cases}
                OUTPUT.parent.mkdir(parents=True, exist_ok=True)
                OUTPUT.write_text(json.dumps(report, indent=1) + "\n")
                print(f"[{number}/{len(fds)} {fd.name}] "
                      f"{time.perf_counter() - start:.0f}s", flush=True)
    print(f"Done: {OUTPUT}")


if __name__ == "__main__":
    main()
