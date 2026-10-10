#!/usr/bin/env python3
"""Downstream use cases U1-U3 for the Offshore FD embedding experiment.

Reuses sampling, feature construction, normalization, projection and the
embedding configurations of ``embedding/scripts/embedding_offshore.py``
unchanged, and adds per (FD, seed):

* U1  determinant classification: ROC-AUC of cosine similarity separating
      same-LHS pairs (positives) from different-LHS pairs (negatives), for the
      normalized (N), de-normalized (D) and random-hub (R) representations;
* U2  similarity search among the sampled source nodes: R-precision, average
      precision, MRR, P@5 and lift over random retrieval;
* U3  updates of one dependent value for a set of groups: receptive fields,
      embedding drift inside/outside the updated groups, full re-embedding
      time, and within-group similarity after complete and incomplete
      updates of the de-normalized copies.

The medians of the original experiment are recorded as well, so that the run
also reproduces Table 5 of the paper.

Set ``DEBUG_NEO4J_PASSWORD`` below or use ``NEO4J_PASSWORD`` in the environment;
otherwise the script prompts for the password when running.

Usage::

    python usecases_offshore.py                 # all 12 FDs, 10 seeds
    python usecases_offshore.py --fds address_country --seeds 1
"""

from __future__ import annotations

import argparse
import getpass
import importlib.util
import json
import math
import os
import random
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from statistics import fmean, median
from typing import Any

import numpy as np
from scipy.stats import mannwhitneyu

HERE = Path(__file__).resolve().parent
ORIGINAL = HERE.parent.parent / "embedding" / "scripts" / "embedding_offshore.py"
OUTPUT = HERE.parent / "results" / "usecases_offshore.json"
DEBUG_NEO4J_PASSWORD: str | None = ""  # Empty: use the environment or prompt.

RANDOM_GRAPH = "offshore_embedding_random"
UPDATED_GRAPH = "offshore_embedding_updated"
RANDOM_MODEL = "offshore_embedding_random_sage"
UPDATED_MODEL = "offshore_embedding_updated_sage"
RANDOM_REL = "HAS_EMB_RANDOM_HUB"
RANDOM_SEED_OFFSET = 7_919
UPDATE_SEED_OFFSET = 104_729
MAX_UPDATED_GROUPS = 50
PARTIAL_FRACTIONS = (0.25, 0.5, 0.75)
UPDATE_SUFFIX = " [corrected]"
RECEPTIVE_HOPS = {"hash_gnn": 1, "graph_sage": 2, "fast_rp": 3}
FEATURE_METHODS = ("fast_rp", "hash_gnn", "graph_sage")


def load_original():
    spec = importlib.util.spec_from_file_location("embedding_offshore",
                                                  ORIGINAL)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


O = load_original()


# ---------------------------------------------------------------- embeddings

def configurations(seed: int) -> tuple[tuple[str, str, dict[str, Any]], ...]:
    """The exact GDS configurations of the original experiment."""
    return (
        ("fast_rp", "gds.fastRP.stream", {
            "embeddingDimension": O.DIMENSION,
            "iterationWeights": [0.0, 1.0, 1.0],
            "featureProperties": [O.FEATURE], "propertyRatio": 0.5,
            "randomSeed": seed, "concurrency": 1,
        }),
        ("node2vec", "gds.node2vec.stream", {
            "embeddingDimension": O.DIMENSION, "walkLength": 20,
            "walksPerNode": 5, "windowSize": 5, "iterations": 2,
            "randomSeed": seed, "concurrency": 1,
        }),
        ("hash_gnn", "gds.hashgnn.stream", {
            "featureProperties": [O.FEATURE], "iterations": 1,
            "embeddingDensity": O.DIMENSION, "neighborInfluence": 1.0,
            "binarizeFeatures": {"dimension": O.DIMENSION * 4,
                                 "threshold": 0.0},
            "outputDimension": O.DIMENSION, "randomSeed": seed,
            "concurrency": 1,
        }),
    )


def embed_all(session: Any, graph: str, model: str, node_ids: list[int],
              seed: int, methods: tuple[str, ...]) -> tuple[
                  dict[str, dict[int, list[float]]], dict[str, float]]:
    vectors, seconds = {}, {}
    for name, procedure, configuration in configurations(seed):
        if name in methods:
            start = time.perf_counter()
            vectors[name] = O.embeddings(session, procedure, graph, node_ids,
                                         configuration)
            seconds[name] = time.perf_counter() - start
    if "graph_sage" in methods:
        start = time.perf_counter()
        vectors["graph_sage"] = O.graph_sage(session, graph, model,
                                             node_ids, seed)
        seconds["graph_sage"] = time.perf_counter() - start
    return vectors, seconds


def reproject(session: Any, name: str, types: list[str]) -> dict[str, int]:
    O.drop_graph(session, name)
    return O.project(session, name, types)


# ------------------------------------------------------------------ helpers

def cosine_scores(vectors: dict[int, list[float]],
                  pairs: list[dict[str, Any]]) -> list[float]:
    return [O.cosine(vectors[int(a["node_id"])], vectors[int(b["node_id"])])
            for a, b in (pair["members"] for pair in pairs)]


def auc(positive: list[float], negative: list[float]) -> float:
    statistic = mannwhitneyu(positive, negative,
                             alternative="two-sided").statistic
    return float(statistic) / (len(positive) * len(negative))


def pool_groups(pairs: list[dict[str, Any]]) -> dict[int, list[dict]]:
    members: dict[int, dict[str, dict]] = {}
    for pair in pairs:
        for member in pair["members"]:
            members.setdefault(pair["group"], {})[member["element_id"]] = member
    return {group: sorted(found.values(), key=lambda m: m["node_id"])
            for group, found in members.items()}


def retrieval(vectors: dict[int, list[float]], group_of: dict[int, int],
              seed: int) -> dict[str, float]:
    """k-NN retrieval among the sampled source nodes."""
    ids = sorted(group_of)
    matrix = np.array([vectors[i] for i in ids], dtype=float)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    matrix /= norms
    similarity = matrix @ matrix.T
    labels = np.array([group_of[i] for i in ids])
    n = len(ids)
    rng = np.random.default_rng(seed)
    tie_break = rng.random(n)
    r_precision, average_precision, reciprocal, p_at_5, lift = (
        [], [], [], [], [])
    for query in range(n):
        relevant_total = int((labels == labels[query]).sum()) - 1
        if relevant_total < 1:
            continue
        scores = similarity[query].copy()
        scores[query] = -np.inf
        order = np.lexsort((tie_break, -scores))[: n - 1]
        hits = labels[order] == labels[query]
        ranks = np.flatnonzero(hits) + 1
        r_prec = hits[:relevant_total].mean()
        r_precision.append(r_prec)
        average_precision.append(
            float(np.mean(np.arange(1, len(ranks) + 1) / ranks)))
        reciprocal.append(1.0 / ranks[0])
        p_at_5.append(hits[:5].mean())
        lift.append(r_prec / (relevant_total / (n - 1)))
    return {"r_precision": fmean(r_precision),
            "map": fmean(average_precision), "mrr": fmean(reciprocal),
            "p_at_5": fmean(p_at_5), "lift": fmean(lift),
            "queries": len(r_precision), "pool": n}


def drift(before: dict[int, list[float]], after: dict[int, list[float]],
          inside: set[int]) -> dict[str, float]:
    values = {node: 1.0 - O.cosine(before[node], after[node])
              for node in before}
    outside = set(values) - inside
    return {"inside": fmean(values[n] for n in inside) if inside else 0.0,
            "outside": fmean(values[n] for n in outside) if outside else 0.0}


def within_group_similarity(vectors: dict[int, list[float]],
                            groups: list[list[int]]) -> float:
    values = []
    for members in groups:
        pairs = [(a, b) for i, a in enumerate(members) for b in members[i + 1:]]
        if pairs:
            values.append(fmean(O.cosine(vectors[a], vectors[b])
                                for a, b in pairs))
    return fmean(values)


def receptive_field(session: Any, types: list[str],
                    changed: set[str]) -> dict[int, int]:
    records = session.run(f"""
MATCH (a:{O.GRAPH_NODE})-[r]->(b:{O.GRAPH_NODE})
WHERE type(r) IN $types
RETURN elementId(a) AS a, elementId(b) AS b
""", types=types)
    adjacency: dict[str, set[str]] = {}
    for record in records:
        adjacency.setdefault(record["a"], set()).add(record["b"])
        adjacency.setdefault(record["b"], set()).add(record["a"])
    distance = {node: 0 for node in changed}
    queue = deque(changed)
    while queue:
        node = queue.popleft()
        if distance[node] == max(RECEPTIVE_HOPS.values()):
            continue
        for neighbor in adjacency.get(node, ()):
            if neighbor not in distance:
                distance[neighbor] = distance[node] + 1
                queue.append(neighbor)
    return {hops: sum(1 for d in distance.values() if d <= hops)
            for hops in sorted(set(RECEPTIVE_HOPS.values()))}


# ---------------------------------------------------------- feature updates

def target_records(session: Any) -> list[dict[str, Any]]:
    return [{"element_id": r["element_id"], "labels": r["labels"],
             "properties": dict(r["properties"])}
            for r in session.run(f"""
MATCH (n:{O.TARGET_NODE})
RETURN elementId(n) AS element_id, labels(n) AS labels,
       properties(n) AS properties
""")]


def denormalized_rows(fd: Any, records: list[dict[str, Any]],
                      updated: set[str]) -> tuple[list[dict], dict]:
    """Recompute the original de-normalized target features, with the first
    dependent value corrected on the nodes in ``updated``."""
    rows, by_node = [], {}
    dimension_fields = {fd.determinant, *fd.dependents}
    for record in records:
        labels = sorted(label for label in record["labels"]
                        if not label.startswith("OFFSHORE_EMB_"))
        namespace = "+".join(labels) or "node"
        properties = {str(k): v for k, v in record["properties"].items()}
        fields = {f for f in properties
                  if f not in {"node_id", "internal_id", O.FEATURE}
                  and not f.startswith("offshore_emb_")}
        moved = dict(properties)
        if record["element_id"] in updated:
            moved[fd.dependents[0]] = (str(moved[fd.dependents[0]])
                                       + UPDATE_SUFFIX)
        feature = O.hashed_feature((
            (namespace, properties, fields - dimension_fields),
            (fd.name, moved, fields & dimension_fields),
        ))
        rows.append({"element_id": record["element_id"], "feature": feature})
        by_node[int(properties["node_id"])] = feature
    return rows, by_node


def update_dimensions(session: Any, fd: Any, groups: list[dict[str, Any]],
                      updated_groups: list[int]) -> set[str]:
    rows = []
    for index in updated_groups:
        group = groups[index]
        properties = {fd.determinant: group["key"]}
        properties.update(zip(fd.dependents, group["rhs"]))
        properties[fd.dependents[0]] = (str(properties[fd.dependents[0]])
                                        + UPDATE_SUFFIX)
        rows.append({"key": group["key"], "feature": O.hashed_feature((
            (fd.name, properties, (fd.determinant, *fd.dependents)),))})
    records = session.run(f"""
UNWIND $rows AS row
MATCH (d:{O.ARTIFACT_NODE}:{fd.node_label})
WHERE d.{fd.determinant} = row.key
SET d.{O.FEATURE} = row.feature
RETURN elementId(d) AS element_id
""", rows=rows)
    return {record["element_id"] for record in records}


def materialize_random(session: Any, fd: Any, groups: list[dict[str, Any]],
                       pool: dict[int, list[dict]], seed: int) -> None:
    """Same dimension nodes and group sizes as N, random membership."""
    rng = random.Random(seed + RANDOM_SEED_OFFSET)
    members = [m["element_id"] for group in sorted(pool)
               for m in pool[group]]
    rng.shuffle(members)
    rows, position = [], 0
    for group_index in sorted(pool):
        size = len(pool[group_index])
        group = groups[group_index]
        properties = {fd.determinant: group["key"]}
        properties.update(zip(fd.dependents, group["rhs"]))
        rows.append({
            "properties": properties,
            "feature": O.hashed_feature((
                (fd.name, properties, (fd.determinant, *fd.dependents)),)),
            "element_ids": members[position:position + size],
        })
        position += size
    O.run(session, f"""
UNWIND $rows AS row
CREATE (dimension:{O.ARTIFACT_NODE}:{O.GRAPH_NODE}:{fd.node_label})
SET dimension += row.properties, dimension.{O.FEATURE} = row.feature
WITH dimension, row
UNWIND row.element_ids AS element_id
MATCH (target:{O.TARGET_NODE}) WHERE elementId(target) = element_id
CREATE (target)-[:{RANDOM_REL}]->(dimension)
""", rows=rows)


# --------------------------------------------------------------------- case

def run_case(session: Any, fd: Any, seed: int) -> dict[str, Any]:
    groups = O.read_valid_groups(session, fd)
    pairs, _ = O.sample_pairs(session, fd, groups, seed)
    different, _ = O.sample_different_lhs_pairs(pairs, seed)
    pool = pool_groups(pairs)
    group_of = {int(m["node_id"]): g for g, ms in pool.items() for m in ms}
    node_ids = sorted(group_of)

    source_graph = O.mark_subgraph(session, fd, pairs)
    types = source_graph["types"]
    denorm_ph, norm_ph, normalized_rows = O.prepare_features(session, fd,
                                                             pairs)
    records = target_records(session)

    # De-normalized representation (D).
    O.project(session, O.DENORMALIZED_GRAPH, types)
    D, d_seconds = embed_all(session, O.DENORMALIZED_GRAPH,
                             O.DENORMALIZED_MODEL, node_ids, seed,
                             ("fast_rp", "node2vec", "hash_gnn",
                              "graph_sage"))
    D["property_hash"] = denorm_ph

    # Normalized representation (N).
    O.write_features(session, normalized_rows)
    O.materialize_normalized(session, fd, groups, pairs)
    n_types = [*types, fd.relationship]
    O.project(session, O.NORMALIZED_GRAPH, n_types)
    N, n_seconds = embed_all(session, O.NORMALIZED_GRAPH,
                             O.NORMALIZED_MODEL, node_ids, seed,
                             ("fast_rp", "node2vec", "hash_gnn",
                              "graph_sage"))
    N["property_hash"] = norm_ph

    # U3 setup: groups whose first dependent value is corrected.
    rng = random.Random(seed + UPDATE_SEED_OFFSET)
    pool_group_ids = sorted(pool)
    updated_groups = rng.sample(
        pool_group_ids,
        min(MAX_UPDATED_GROUPS, max(1, len(pool_group_ids) // 2)))
    updated_members = {g: [m["element_id"] for m in pool[g]]
                       for g in updated_groups}
    inside_ids = {int(m["node_id"]) for g in updated_groups for m in pool[g]}
    updated_node_lists = [[int(m["node_id"]) for m in pool[g]]
                          for g in updated_groups]

    # U3 on N: null run (re-projection without change) as noise floor, then
    # one feature change per updated group (its dimension node).
    reproject(session, UPDATED_GRAPH, n_types)
    N_null, _ = embed_all(session, UPDATED_GRAPH, UPDATED_MODEL, node_ids,
                          seed, FEATURE_METHODS)
    N_null["property_hash"] = norm_ph
    O.drop_graph(session, UPDATED_GRAPH)
    changed_dimensions = update_dimensions(session, fd, groups,
                                           updated_groups)
    reproject(session, UPDATED_GRAPH, n_types)
    N_upd, _ = embed_all(session, UPDATED_GRAPH, UPDATED_MODEL, node_ids,
                         seed, FEATURE_METHODS)
    N_upd["property_hash"] = norm_ph
    n_field = receptive_field(session, n_types, changed_dimensions)
    O.drop_graph(session, UPDATED_GRAPH)

    # Random-hub control (R): replace N's dimensions by randomized ones.
    O.run(session, f"MATCH (d:{O.ARTIFACT_NODE}) DETACH DELETE d")
    materialize_random(session, fd, groups, pool, seed)
    r_types = [*types, RANDOM_REL]
    O.project(session, RANDOM_GRAPH, r_types)
    R, _ = embed_all(session, RANDOM_GRAPH, RANDOM_MODEL, node_ids, seed,
                     ("fast_rp", "node2vec", "hash_gnn", "graph_sage"))
    R["property_hash"] = norm_ph
    O.drop_graph(session, RANDOM_GRAPH)
    O.run(session, f"MATCH (d:{O.ARTIFACT_NODE}) DETACH DELETE d")

    # U3 on D: complete (q = 1) and incomplete updates of the copies.
    d_field = receptive_field(
        session, types, {e for es in updated_members.values() for e in es})
    D_q: dict[str, dict[str, dict[int, list[float]]]] = {}
    for fraction in (0.0, *PARTIAL_FRACTIONS, 1.0):
        chosen = set()
        for group, members in sorted(updated_members.items()):
            shuffled = members[:]
            random.Random(seed + UPDATE_SEED_OFFSET + group).shuffle(
                shuffled)
            chosen.update(shuffled[:math.ceil(fraction * len(shuffled))])
        rows, ph = denormalized_rows(fd, records, chosen)
        O.write_features(session, rows)
        reproject(session, UPDATED_GRAPH, types)
        vectors, _ = embed_all(session, UPDATED_GRAPH, UPDATED_MODEL,
                               node_ids, seed, FEATURE_METHODS)
        vectors["property_hash"] = ph
        D_q[str(fraction)] = vectors
        O.drop_graph(session, UPDATED_GRAPH)

    # ------------------------------------------------------------- scoring
    methods = ("property_hash", "fast_rp", "node2vec", "hash_gnn",
               "graph_sage")
    result: dict[str, Any] = {
        "seed": seed, "fd": fd.name, "pool": len(node_ids),
        "groups_in_pool": len(pool), "pairs": len(pairs),
        "updated_groups": len(updated_groups),
        "updated_nodes": len(inside_ids),
        "median_same": {}, "median_different": {}, "auc": {},
        "retrieval": {}, "update": {},
    }
    for method in methods:
        result["median_same"][method] = {}
        result["median_different"][method] = {}
        result["auc"][method] = {}
        result["retrieval"][method] = {}
        for label, vectors in (("N", N), ("D", D), ("R", R)):
            same = cosine_scores(vectors[method], pairs)
            diff = cosine_scores(vectors[method], different)
            result["median_same"][method][label] = float(median(same))
            result["median_different"][method][label] = float(median(diff))
            result["auc"][method][label] = auc(same, diff)
            if label != "R":
                result["retrieval"][method][label] = retrieval(
                    vectors[method], group_of, seed)

    for method in ("property_hash", *FEATURE_METHODS):
        complete = D_q["1.0"][method]
        d_null = D_q["0.0"][method]
        hops = RECEPTIVE_HOPS.get(method, 0)
        result["update"][method] = {
            "written_values": {"N": len(updated_groups),
                               "D": len(inside_ids)},
            "receptive_field": ({"N": n_field[hops], "D": d_field[hops]}
                                if hops else None),
            "embedding_seconds": ({"N": n_seconds[method],
                                   "D": d_seconds[method]}
                                  if method in n_seconds else None),
            "drift": {"N": drift(N_null[method], N_upd[method], inside_ids),
                      "D": drift(d_null, complete, inside_ids)},
            "drift_noise": {"N": drift(N[method], N_null[method], inside_ids),
                            "D": drift(D[method], d_null, inside_ids)},
            "within_group_similarity": {
                "N_null": within_group_similarity(N_null[method],
                                                    updated_node_lists),
                "N_after": within_group_similarity(N_upd[method],
                                                   updated_node_lists),
                "D_original": within_group_similarity(D[method],
                                                    updated_node_lists),
                **{f"D_q={q}": within_group_similarity(
                    D_q[q][method], updated_node_lists) for q in D_q},
            },
        }
    return result


def cleanup(session: Any) -> None:
    for name in (RANDOM_GRAPH, UPDATED_GRAPH):
        O.drop_graph(session, name)
    for name in (RANDOM_MODEL, UPDATED_MODEL):
        O.drop_model(session, name)
    O.cleanup(session)


def write_report(report: dict[str, Any]) -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, indent=1) + "\n",
                         encoding="utf-8")
    temporary.replace(OUTPUT)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fds", nargs="*", default=None)
    parser.add_argument("--seeds", type=int, default=10)
    parser.add_argument("--database", default="offshorecsv")
    parser.add_argument("--resume", action="store_true",
                        help="keep finished (FD, seed) cases in the output "
                             "file and run only the missing ones")
    parser.add_argument("--max-cases", type=int, default=None,
                        help="stop after this many new cases")
    args = parser.parse_args()
    fds = [fd for fd in O.FDS if not args.fds or fd.name in args.fds]
    seeds = list(range(20260917, 20260917 + args.seeds))

    password = DEBUG_NEO4J_PASSWORD or os.environ.get("NEO4J_PASSWORD")
    if not password:
        password = getpass.getpass(f"Neo4j password for {O.NEO4J_USER}: ")

    from neo4j import GraphDatabase
    with GraphDatabase.driver(O.NEO4J_URI, auth=(
            O.NEO4J_USER, password)) as driver:
        with driver.session(database=args.database) as session:
            report = {
                "experiment": "offshore_fd_embedding_usecases",
                "created_at": datetime.now().astimezone().isoformat(),
                "gds_version": session.run("RETURN gds.version() AS v")
                .single()["v"],
                "settings": {"pair_count": O.PAIR_COUNT, "seeds": seeds,
                             "max_updated_groups": MAX_UPDATED_GROUPS,
                             "partial_fractions": PARTIAL_FRACTIONS,
                             "receptive_hops": RECEPTIVE_HOPS},
                "cases": [],
            }
            done = set()
            if args.resume and OUTPUT.exists():
                previous = json.loads(OUTPUT.read_text())
                report["cases"] = previous["cases"]
                done = {(c["fd"], c["seed"]) for c in previous["cases"]}
            cleanup(session)
            new_cases = 0
            for number, fd in enumerate(fds, start=1):
                for repeat, seed in enumerate(seeds, start=1):
                    if (fd.name, seed) in done:
                        continue
                    if args.max_cases is not None and new_cases >= args.max_cases:
                        break
                    new_cases += 1
                    start = time.perf_counter()
                    try:
                        report["cases"].append(run_case(session, fd, seed))
                    finally:
                        cleanup(session)
                    print(f"[{number}/{len(fds)} {fd.name} seed "
                          f"{repeat}/{len(seeds)}] "
                          f"{time.perf_counter() - start:.1f}s", flush=True)
                    write_report(report)
    print(f"Done: {OUTPUT}")


if __name__ == "__main__":
    main()
