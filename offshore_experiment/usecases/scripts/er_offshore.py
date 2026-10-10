#!/usr/bin/env python3
"""Use case U4: entity resolution on Offshore Entity nodes.

Known duplicates (Entity-Entity relationships ``same_company_as``,
``same_as``, ``same_name_as``, ``similar_company_as``) are positives.  For
each positive pair (a, b) we add negatives (a, c): *hard* negatives c that
share a's determinant value, and *random* negatives c drawn from all entities
with a valid determinant value.  The sampled entities, their one-hop
non-Entity neighbours and, for the normalized graph, one dimension node per
determinant value are embedded with the configurations of the original
experiment.  Duplicate relationships are never projected, so the embeddings
cannot see the links they are evaluated on.

Uses its own labels, property and graph names, so it can run while
``usecases_offshore.py`` is running.

Usage (after ``source ~/neo4j/env.sh``)::

    python er_offshore.py                       # 3 Entity FDs, 10 seeds
    python er_offshore.py --fds service_provider --seeds 1
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from statistics import fmean
from typing import Any

HERE = Path(__file__).resolve().parent
OUTPUT = HERE.parent / "results" / "er_offshore.json"


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


U = load("usecases_offshore", HERE / "usecases_offshore.py")
O = U.O

# Separate names, so that this script can run next to usecases_offshore.py.
# The OFFSHORE_EMB_ / offshore_emb_ / HAS_EMB_ prefixes keep these artifacts
# out of the features and projections of the other script, and vice versa.
O.GRAPH_NODE = "OFFSHORE_EMB_ER_NODE"
O.TARGET_NODE = "OFFSHORE_EMB_ER_TARGET"
O.ARTIFACT_NODE = "OFFSHORE_EMB_ER_ARTIFACT"
O.FEATURE = "offshore_emb_er_features"
O.DENORMALIZED_GRAPH = "offshore_er_denormalized"
O.NORMALIZED_GRAPH = "offshore_er_normalized"
O.DENORMALIZED_MODEL = "offshore_er_denormalized_sage"
O.NORMALIZED_MODEL = "offshore_er_normalized_sage"
U.RANDOM_GRAPH = "offshore_er_random"
U.UPDATED_GRAPH = "offshore_er_updated"
U.RANDOM_MODEL = "offshore_er_random_sage"
U.UPDATED_MODEL = "offshore_er_updated_sage"

DUPLICATE_TYPES = ("same_company_as", "same_as", "same_name_as",
                   "similar_company_as")
# All duplicate-style relationship types are excluded from the projection,
# so that duplicates cannot be linked indirectly, e.g. via similar officers.
EXCLUDED_TYPES = (*DUPLICATE_TYPES, "similar", "same_id_as",
                  "probably_same_officer_as", "same_address_as",
                  "same_intermediary_as")
ER_FDS = ("service_provider", "country", "entity_country_reverse")
POSITIVES = 500
HARD_NEGATIVES = 3
RANDOM_NEGATIVES = 3
METHODS = ("property_hash", "fast_rp", "node2vec", "hash_gnn", "graph_sage")


def valid_entities(session: Any, fd: Any, groups: list[dict]) -> dict:
    keys = {group["key"]: index for index, group in enumerate(groups)}
    records = session.run(f"""
MATCH (e:Entity) WHERE e.{fd.determinant} IN $keys
RETURN e.{fd.determinant} AS key, e.node_id AS node_id,
       elementId(e) AS element_id
ORDER BY node_id
""", keys=list(keys))
    return {int(r["node_id"]): {"group": keys[r["key"]],
                                "element_id": r["element_id"]}
            for r in records}


def duplicate_pairs(session: Any, valid: dict) -> tuple[list, set]:
    records = session.run("""
MATCH (a:Entity)-[r]-(b:Entity)
WHERE type(r) IN $types AND a.node_id < b.node_id
RETURN DISTINCT a.node_id AS a, b.node_id AS b
""", types=list(DUPLICATE_TYPES))
    every = {(int(r["a"]), int(r["b"])) for r in records}
    usable = sorted(pair for pair in every
                    if pair[0] in valid and pair[1] in valid)
    return usable, every


def sample(valid: dict, usable: list, every: set, seed: int) -> dict:
    rng = random.Random(seed)
    positives = rng.sample(usable, min(POSITIVES, len(usable)))
    by_group: dict[int, list[int]] = {}
    for node, info in valid.items():
        by_group.setdefault(info["group"], []).append(node)
    all_nodes = sorted(valid)

    def duplicate(a: int, b: int) -> bool:
        return (min(a, b), max(a, b)) in every

    hard, random_negatives = [], []
    for a, b in positives:
        anchor = a if rng.random() < 0.5 else b
        members = by_group[valid[anchor]["group"]]
        chosen = set()
        for _ in range(50 * HARD_NEGATIVES):
            if len(chosen) == HARD_NEGATIVES or len(members) < 2:
                break
            c = rng.choice(members)
            if c != anchor and not duplicate(anchor, c):
                chosen.add(c)
        hard.extend((anchor, c) for c in sorted(chosen))
        chosen = set()
        while len(chosen) < RANDOM_NEGATIVES:
            c = rng.choice(all_nodes)
            if c != anchor and not duplicate(anchor, c):
                chosen.add(c)
        random_negatives.extend((anchor, c) for c in sorted(chosen))
    return {"positive": positives, "hard": hard, "random": random_negatives}


def as_pairs(pairs: list[tuple[int, int]], valid: dict) -> list[dict]:
    """Original pair format, so that prepare_features can be reused."""
    return [{"group": valid[a]["group"],
             "members": tuple({"node_id": n,
                               "element_id": valid[n]["element_id"]}
                              for n in (a, b))}
            for a, b in pairs]


def mark(session: Any, element_ids: list[str]) -> list[str]:
    O.run(session, f"""
UNWIND $element_ids AS element_id
MATCH (e:Entity) WHERE elementId(e) = element_id
SET e:{O.GRAPH_NODE}:{O.TARGET_NODE}
""", element_ids=element_ids)
    O.run(session, f"""
MATCH (e:{O.TARGET_NODE})--(neighbor)
WHERE NOT neighbor:Entity AND NOT neighbor:{O.ARTIFACT_NODE}
SET neighbor:{O.GRAPH_NODE}
""")
    record = session.run(f"""
MATCH (a:{O.GRAPH_NODE})-[r]->(b:{O.GRAPH_NODE})
WHERE NOT type(r) STARTS WITH 'HAS_EMB_' AND NOT type(r) IN $excluded
RETURN collect(DISTINCT type(r)) AS types, count(r) AS relationships
""", excluded=list(EXCLUDED_TYPES)).single(strict=True)
    return list(record["types"])


def materialize(session: Any, fd: Any, groups: list[dict], valid: dict,
                nodes: list[int]) -> str:
    relationship = "HAS_EMB_ER_" + fd.relationship.removeprefix("HAS_EMB_")
    members: dict[int, list[str]] = {}
    for node in nodes:
        members.setdefault(valid[node]["group"], []).append(
            valid[node]["element_id"])
    rows = []
    for index, element_ids in members.items():
        group = groups[index]
        properties = {fd.determinant: group["key"]}
        properties.update(zip(fd.dependents, group["rhs"]))
        rows.append({"properties": properties, "element_ids": element_ids,
                     "feature": O.hashed_feature((
                         (fd.name, properties,
                          (fd.determinant, *fd.dependents)),))})
    O.run(session, f"""
UNWIND $rows AS row
CREATE (d:{O.ARTIFACT_NODE}:{O.GRAPH_NODE}:{fd.node_label})
SET d += row.properties, d.{O.FEATURE} = row.feature
WITH d, row
UNWIND row.element_ids AS element_id
MATCH (t:{O.TARGET_NODE}) WHERE elementId(t) = element_id
CREATE (t)-[:{relationship}]->(d)
""", rows=rows)
    return relationship


def scores(vectors: dict, pairs: list[tuple[int, int]]) -> list[float]:
    return [O.cosine(vectors[a], vectors[b]) for a, b in pairs]


def evaluate(vectors: dict, sampled: dict, valid: dict) -> dict:
    positive = scores(vectors, sampled["positive"])
    hard = scores(vectors, sampled["hard"])
    rand = scores(vectors, sampled["random"])
    same = [s for s, (a, b) in zip(positive, sampled["positive"])
            if valid[a]["group"] == valid[b]["group"]]
    different = [s for s, (a, b) in zip(positive, sampled["positive"])
                 if valid[a]["group"] != valid[b]["group"]]
    # MRR: rank the partner among itself and the anchor's negatives.
    negatives: dict[int, list[float]] = {}
    for s, (anchor, _) in zip(hard + rand,
                              sampled["hard"] + sampled["random"]):
        negatives.setdefault(anchor, []).append(s)
    reciprocal = []
    for s, (a, b) in zip(positive, sampled["positive"]):
        anchor_scores = negatives.get(a, []) + negatives.get(b, [])
        reciprocal.append(1.0 / (1 + sum(1 for x in anchor_scores if x > s)))
    result = {"auc_random": U.auc(positive, rand),
              "auc_hard": U.auc(positive, hard) if hard else None,
              "mrr": fmean(reciprocal)}
    if same and hard:
        result["auc_hard_same_det"] = U.auc(same, hard)
    if different and hard:
        result["auc_hard_diff_det"] = U.auc(different, hard)
    return result


def run_case(session: Any, fd: Any, seed: int) -> dict[str, Any]:
    groups = O.read_valid_groups(session, fd)
    valid = valid_entities(session, fd, groups)
    usable, every = duplicate_pairs(session, valid)
    sampled = sample(valid, usable, every, seed)
    nodes = sorted({n for kind in sampled.values() for pair in kind
                    for n in pair})
    pairs = as_pairs([p for kind in sampled.values() for p in kind], valid)
    types = mark(session, [valid[n]["element_id"] for n in nodes])

    denorm_ph, norm_ph, normalized_rows = O.prepare_features(session, fd,
                                                             pairs)
    O.project(session, O.DENORMALIZED_GRAPH, types)
    D, _ = U.embed_all(session, O.DENORMALIZED_GRAPH, O.DENORMALIZED_MODEL,
                       nodes, seed, METHODS[1:])
    D["property_hash"] = denorm_ph

    O.write_features(session, normalized_rows)
    relationship = materialize(session, fd, groups, valid, nodes)
    O.project(session, O.NORMALIZED_GRAPH, [*types, relationship])
    N, _ = U.embed_all(session, O.NORMALIZED_GRAPH, O.NORMALIZED_MODEL,
                       nodes, seed, METHODS[1:])
    N["property_hash"] = norm_ph

    shared = sum(1 for a, b in sampled["positive"]
                 if valid[a]["group"] == valid[b]["group"])
    return {
        "fd": fd.name, "seed": seed,
        "usable_duplicate_pairs": len(usable),
        "positives": len(sampled["positive"]),
        "positives_sharing_determinant": shared,
        "hard_negatives": len(sampled["hard"]),
        "random_negatives": len(sampled["random"]),
        "entities": len(nodes), "projected_types": types,
        "results": {method: {"N": evaluate(N[method], sampled, valid),
                             "D": evaluate(D[method], sampled, valid)}
                    for method in METHODS},
    }


def cleanup(session: Any) -> None:
    O.cleanup(session)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fds", nargs="*", default=list(ER_FDS))
    parser.add_argument("--seeds", type=int, default=10)
    args = parser.parse_args()
    fds = [fd for fd in O.FDS if fd.name in args.fds]
    seeds = list(range(20260917, 20260917 + args.seeds))

    from neo4j import GraphDatabase
    with GraphDatabase.driver(O.NEO4J_URI, auth=(
            O.NEO4J_USER, os.environ["NEO4J_PASSWORD"])) as driver:
        with driver.session(database="offshorecsv") as session:
            report = {"experiment": "offshore_entity_resolution",
                      "created_at": datetime.now().astimezone().isoformat(),
                      "settings": {"duplicate_types": DUPLICATE_TYPES,
                                   "excluded_types": EXCLUDED_TYPES,
                                   "positives": POSITIVES,
                                   "hard_negatives": HARD_NEGATIVES,
                                   "random_negatives": RANDOM_NEGATIVES,
                                   "seeds": seeds},
                      "cases": []}
            cleanup(session)
            for number, fd in enumerate(fds, start=1):
                for repeat, seed in enumerate(seeds, start=1):
                    start = time.perf_counter()
                    try:
                        report["cases"].append(run_case(session, fd, seed))
                    finally:
                        cleanup(session)
                    print(f"[{number}/{len(fds)} {fd.name} seed "
                          f"{repeat}/{len(seeds)}] "
                          f"{time.perf_counter() - start:.1f}s", flush=True)
                    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
                    OUTPUT.write_text(json.dumps(report, indent=1) + "\n")
    print(f"Done: {OUTPUT}")


if __name__ == "__main__":
    main()
