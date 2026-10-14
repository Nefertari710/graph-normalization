#!/usr/bin/env python3
"""Update cost of the reference-FD decompositions on the full Offshore graph (RQ2).

Companion of updates_offshore.py for the FDs of Section 5.1 whose determinant is
the reference registered_address (theory/reference_fd_experiment.py):

    Officer:      {registered_address} -> {sourceID}
    Intermediary: {registered_address} -> {sourceID}   (also countries, country_codes, valid_until)
    Entity:       {registered_address} -> {sourceID}

A group is an Address node a with at least two source nodes of the type that have
exactly one registered_address edge, which points to a, and agree on the dependent
(the nodes carrying all features involved, as in the paper).  The separated node
type T_Z is keyed by the reference, hence in one-to-one correspondence with the
addresses and merged with Address (Section 5.1): normalization (timed, B_N) copies
the dependent once onto each group's Address node, under the property name
<dependent>_<SourceLabel>, and leaves the source nodes' copies in place, so both
representations coexist on the same data and are measured on the same groups.

Updates, for up to --groups sampled groups and --repeats repetitions: correct the
dependent value
  - de-normalized (D): on every source node of the group (MATCH via the edge);
  - normalized (N):    once, on the Address node;
each followed by a timed update back to the original value; medians per group.

Cleanup removes the <dependent>_<SourceLabel> properties and the index.

Set ``DEBUG_NEO4J_PASSWORD`` below or use ``NEO4J_PASSWORD`` in the environment;
otherwise the script prompts for the password when running.

Usage (with Neo4j running and Offshore data imported into offshorecsv)::

    python updates_reference_fd.py                       # all three
    python updates_reference_fd.py --types Officer --groups 20 --repeats 3

Output: ../results/updates_reference_fd.json plus a LaTeX-ready summary on stdout
(valid groups, redundant values, B_N, median U_D/U_N, largest group).
"""
from __future__ import annotations
import argparse, getpass, json, os, random, time
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

HERE = Path(__file__).resolve().parent
OUTPUT = HERE.parent / "results" / "updates_reference_fd.json"
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_DATABASE = "offshorecsv"
DEBUG_NEO4J_PASSWORD: str | None = ""  # Empty: use the environment or prompt.
SEED = 20261014
SUFFIX = " [corrected]"
CASES = {  # source label -> dependent
    "Officer": "sourceID",
    "Intermediary": "sourceID",
    "Entity": "sourceID",
}


def run(session, query, **p):
    return session.run(query, p).consume()


def timed(session, query, **p):
    start = time.perf_counter()
    summary = session.run(query, p).consume()
    return (time.perf_counter() - start) * 1000.0, summary


def read_groups(session, label, dep):
    """addresses with >= 2 single-address source nodes that agree on dep"""
    records = session.run(f"""
MATCH (e:{label})-[:registered_address]->(a:Address)
WITH e, a, size([(e)-[:registered_address]->() | 1]) AS deg
WHERE deg = 1 AND e.{dep} IS NOT NULL AND e.{dep} <> ''
WITH a.node_id AS key, count(*) AS fanout, collect(DISTINCT e.{dep}) AS vals
WHERE fanout >= 2 AND size(vals) = 1
RETURN key, fanout, vals[0] AS value ORDER BY key""")
    return [{"key": r["key"], "fanout": int(r["fanout"]), "value": r["value"]} for r in records]


def normalize(session, label, dep, groups):
    prop = f"{dep}_{label}"
    rows = [{"key": g["key"], "value": g["value"]} for g in groups]
    start = time.perf_counter()
    run(session, "CREATE INDEX upd_ref_address IF NOT EXISTS FOR (a:Address) ON (a.node_id)")
    run(session, "CALL db.awaitIndexes(3000)")
    for b in range(0, len(rows), 1000):
        run(session, f"""
UNWIND $rows AS row
MATCH (a:Address {{node_id: row.key}})
SET a.{prop} = row.value""", rows=rows[b:b + 1000])
    return {"normalization_seconds": time.perf_counter() - start, "address_nodes": len(groups)}


def measure(session, label, dep, group, repeats):
    prop = f"{dep}_{label}"
    original, corrected = group["value"], str(group["value"]) + SUFFIX
    d_times, n_times = [], []
    for _ in range(repeats):
        for value in (corrected, original):
            ms, _ = timed(session, f"""
MATCH (e:{label})-[:registered_address]->(a:Address {{node_id: $key}})
SET e.{dep} = $value""", key=group["key"], value=value)
            d_times.append(ms)
            ms, _ = timed(session, f"""
MATCH (a:Address {{node_id: $key}})
SET a.{prop} = $value""", key=group["key"], value=value)
            n_times.append(ms)
    return {"key": str(group["key"]), "copies": group["fanout"],
            "d_median_ms": median(d_times), "n_median_ms": median(n_times),
            "paired_diff_ms": median(d - n for d, n in zip(d_times, n_times))}


def cleanup(session, label, dep):
    prop = f"{dep}_{label}"
    while True:
        s = run(session, f"MATCH (a:Address) WHERE a.{prop} IS NOT NULL WITH a LIMIT 5000 REMOVE a.{prop}")
        if s.counters.properties_set == 0:
            break


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--types", nargs="*", default=None)
    ap.add_argument("--groups", type=int, default=50)
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()
    password = DEBUG_NEO4J_PASSWORD or os.environ.get("NEO4J_PASSWORD")
    if not password:
        password = getpass.getpass(f"Neo4j password for {NEO4J_USER}: ")
    from neo4j import GraphDatabase
    report = {"experiment": "offshore_reference_fd_updates", "created_at": datetime.now().astimezone().isoformat(),
              "settings": vars(args) | {"seed": SEED}, "types": {}}
    if args.resume and OUTPUT.exists():
        report["types"] = json.loads(OUTPUT.read_text())["types"]
    with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, password)) as driver:
        with driver.session(database=NEO4J_DATABASE) as session:
            for n, (label, dep) in enumerate(CASES.items(), start=1):
                if args.types and label not in args.types or label in report["types"]:
                    continue
                t0 = time.perf_counter()
                groups = read_groups(session, label, dep)
                try:
                    built = normalize(session, label, dep, groups)
                    rng = random.Random(SEED + n)
                    sample = rng.sample(groups, min(args.groups, len(groups)))
                    sample.sort(key=lambda g: -g["fanout"])
                    cases = [measure(session, label, dep, g, args.repeats) for g in sample]
                finally:
                    cleanup(session, label, dep)
                report["types"][label] = {
                    "fd": f"{{registered_address}} -> {{{dep}}}", "valid_groups": len(groups),
                    "valid_nodes": sum(g["fanout"] for g in groups),
                    "redundant_values": sum(g["fanout"] - 1 for g in groups), **built, "cases": cases}
                OUTPUT.parent.mkdir(parents=True, exist_ok=True)
                OUTPUT.write_text(json.dumps(report, indent=1) + "\n")
                print(f"[{n}/{len(CASES)} {label}] {time.perf_counter() - t0:.0f}s", flush=True)
    print("\nLaTeX rows (Type & FD & groups & redundant & B_N & med U_D & med U_N & largest: copies & U_D & U_N):")
    for label, r in report["types"].items():
        c = r["cases"]; big = max(c, key=lambda x: x["copies"])
        print(f"\\textsc{{{label}}} & \\textit{{registered\\_address}} & {r['valid_groups']:,} & {r['redundant_values']/1000:.1f}K & "
              f"{r['normalization_seconds']:.1f} & {median(x['d_median_ms'] for x in c):.1f} & {median(x['n_median_ms'] for x in c):.1f} & "
              f"{big['copies']:,} & {big['d_median_ms']:.1f} & {big['n_median_ms']:.1f} \\\\")
    print(f"Done: {OUTPUT.relative_to(HERE.parents[2]).as_posix()}")


if __name__ == "__main__":
    main()
