#!/usr/bin/env python3
"""Compare denormalized and normalized embeddings for selected Offshore FDs.

Every FD in the following table runs through the same sampling, normalization,
embedding, and scoring workflow.

Offshore-wide within-table FD experiments:

| Node table  | Functional dependency                              | Complete rows (coverage) | Repeated LHS groups / extra occurrences | Conflict groups / rows | Type          |
|-------------|----------------------------------------------------|--------------------------|-----------------------------------------|------------------------|---------------|
| Address     | countries -> country_codes                         | 277,109 (68.89%)         | 323 / 276,743                           | 0 / 0                  | Strict        |
| Entity      | service_provider -> (sourceID, valid_until)         | 344,086 (42.25%)         | 4 / 344,082                             | 0 / 0                  | Strict        |
| Entity      | country_codes -> countries                         | 504,991 (62.01%)         | 685 / 503,890                           | 3 / 4,713              | Approximate   |
| Entity      | countries -> country_codes                         | 504,991 (62.01%)         | 688 / 503,889                           | 2 / 7                  | Approximate   |
| Intermediary| country_codes -> countries                         | 23,152 (86.49%)          | 166 / 22,867                            | 0 / 0                  | Strict        |
| Intermediary| countries -> country_codes                         | 23,152 (86.49%)          | 166 / 22,867                            | 0 / 0                  | Strict        |
| Intermediary| valid_until -> sourceID                            | 26,768 (100%)            | 10 / 26,757                             | 0 / 0                  | Strict        |
| Intermediary| sourceID -> valid_until                            | 26,768 (100%)            | 9 / 26,759                              | 1 / 1,023              | Approximate   |
| Other       | jurisdiction -> jurisdiction_description           | 958 (32.05%)             | 5 / 952                                 | 0 / 0                  | Strict        |
| Other       | jurisdiction_description -> jurisdiction           | 958 (32.05%)             | 5 / 952                                 | 0 / 0                  | Strict        |
| Other       | country_codes -> countries                         | 386 (12.91%)             | 23 / 323                                | 0 / 0                  | Strict        |
| Other       | countries -> country_codes                         | 386 (12.91%)             | 23 / 323                                | 0 / 0                  | Strict        |

Known failures, Officer candidates, selection-induced pseudo-FDs, and the two
source-scoped Entity composite FDs are excluded.  An extra LHS occurrence is
one row beyond the first occurrence of each distinct non-empty LHS value.  For
approximate FDs, the table reports full complete-case counts before conflicts
are removed.  The Entity country experiment retains 682 conflict-free repeated
groups and 499,180 extra occurrences after filtering.

For each dependency, the script moves the determinant and dependent properties
from the sampled source nodes to a shared dimension node, then compares
embeddings from the original and normalized graph representations.  It scores
both node pairs with the same LHS value and control pairs with different LHS
values, using the same sampled nodes and embeddings.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import re
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import fmean, median
from typing import Any, Iterable

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""
DATABASE = "offshorecsv"

PAIR_COUNT = 1_000  # 4_000
DIMENSION = 128
RANDOM_SEEDS = range(20260917, 20260927)
DIFFERENT_LHS_SEED_OFFSET = 1_000_003
WRITE_BATCH = 500
OUTPUT = (Path(__file__).resolve().parents[1] / "results" / DATABASE
          / "embedding_offshore.json")

GRAPH_NODE = "OFFSHORE_EMB_NODE"
TARGET_NODE = "OFFSHORE_EMB_TARGET"
ARTIFACT_NODE = "OFFSHORE_EMB_ARTIFACT"
FEATURE = "offshore_emb_features"
DENORMALIZED_GRAPH = "offshore_embedding_denormalized"
NORMALIZED_GRAPH = "offshore_embedding_normalized"
DENORMALIZED_MODEL = "offshore_embedding_denormalized_sage"
NORMALIZED_MODEL = "offshore_embedding_normalized_sage"
WORD_RE = re.compile(r"\w+", re.UNICODE)


@dataclass(frozen=True)
class FD:
    name: str
    source_label: str
    determinant: str
    dependents: tuple[str, ...]
    node_label: str
    relationship: str


FDS = (
    FD("address_country", "Address", "countries", ("country_codes",),
       "OFFSHORE_EMB_COUNTRY", "HAS_EMB_COUNTRY"),
    FD("service_provider", "Entity", "service_provider",
       ("sourceID", "valid_until"),
       "OFFSHORE_EMB_SERVICE_PROVIDER", "HAS_EMB_SERVICE_PROVIDER"),
    FD("country", "Entity", "country_codes", ("countries",),
       "OFFSHORE_EMB_COUNTRY", "HAS_EMB_COUNTRY"),
    FD("entity_country_reverse", "Entity", "countries", ("country_codes",),
       "OFFSHORE_EMB_COUNTRY", "HAS_EMB_COUNTRY"),
    FD("intermediary_country", "Intermediary", "country_codes",
       ("countries",), "OFFSHORE_EMB_COUNTRY", "HAS_EMB_COUNTRY"),
    FD("intermediary_country_reverse", "Intermediary", "countries",
       ("country_codes",), "OFFSHORE_EMB_COUNTRY", "HAS_EMB_COUNTRY"),
    FD("intermediary_valid_until", "Intermediary", "valid_until",
       ("sourceID",), "OFFSHORE_EMB_SOURCE_SNAPSHOT",
       "HAS_EMB_SOURCE_SNAPSHOT"),
    FD("intermediary_source", "Intermediary", "sourceID",
       ("valid_until",), "OFFSHORE_EMB_SOURCE_SNAPSHOT",
       "HAS_EMB_SOURCE_SNAPSHOT"),
    FD("other_jurisdiction", "Other", "jurisdiction",
       ("jurisdiction_description",), "OFFSHORE_EMB_JURISDICTION",
       "HAS_EMB_JURISDICTION"),
    FD("other_jurisdiction_reverse", "Other", "jurisdiction_description",
       ("jurisdiction",), "OFFSHORE_EMB_JURISDICTION",
       "HAS_EMB_JURISDICTION"),
    FD("other_country", "Other", "country_codes", ("countries",),
       "OFFSHORE_EMB_COUNTRY", "HAS_EMB_COUNTRY"),
    FD("other_country_reverse", "Other", "countries", ("country_codes",),
       "OFFSHORE_EMB_COUNTRY", "HAS_EMB_COUNTRY"),
)


def run(session: Any, query: str, **parameters: Any) -> Any:
    return session.run(query, parameters).consume()


def drop_graph(session: Any, name: str) -> None:
    record = session.run(
        "CALL gds.graph.exists($name) YIELD exists RETURN exists", name=name
    ).single(strict=True)
    if record["exists"]:
        run(
            session,
            "CALL gds.graph.drop($name) YIELD graphName RETURN graphName",
            name=name,
        )


def drop_model(session: Any, name: str) -> None:
    run(session, "CALL gds.model.drop($name, false)", name=name)


def cleanup(session: Any) -> None:
    errors = []
    for name in (DENORMALIZED_GRAPH, NORMALIZED_GRAPH):
        try:
            drop_graph(session, name)
        except Exception as error:
            errors.append(error)
    for name in (DENORMALIZED_MODEL, NORMALIZED_MODEL):
        try:
            drop_model(session, name)
        except Exception as error:
            errors.append(error)
    for query in (
            f"MATCH (n:{ARTIFACT_NODE}) DETACH DELETE n",
            f"""
MATCH (n:{GRAPH_NODE})
REMOVE n.{FEATURE}, n:{GRAPH_NODE}, n:{TARGET_NODE}
""",
    ):
        try:
            run(session, query)
        except Exception as error:
            errors.append(error)
    if errors:
        raise errors[0]


def read_valid_groups(session: Any, fd: FD) -> list[dict[str, Any]]:
    present = " AND ".join(
        f"e.{prop} IS NOT NULL AND e.{prop} <> ''"
        for prop in fd.dependents
    )
    rhs = ", ".join(f"e.{prop}" for prop in fd.dependents)
    records = session.run(f"""
MATCH (e:{fd.source_label})
WHERE e.{fd.determinant} IS NOT NULL AND e.{fd.determinant} <> ''
WITH e.{fd.determinant} AS key,
     count(*) AS fanout,
     sum(CASE WHEN {present} THEN 1 ELSE 0 END) AS complete,
     collect(DISTINCT CASE WHEN {present} THEN [{rhs}] END) AS rhs
WHERE fanout >= 2 AND complete = fanout AND size(rhs) = 1
RETURN key, fanout, rhs[0] AS rhs
ORDER BY key
""")
    return [
        {"key": record["key"], "rhs": list(record["rhs"]),
         "fanout": int(record["fanout"])}
        for record in records
    ]


def sample_pairs(session: Any, fd: FD, groups: list[dict[str, Any]], seed: int
                 ) -> tuple[list[dict[str, Any]], int]:
    """Make balanced, disjoint pairs without retaining every node in RAM."""
    if not groups:
        return [], 0
    rng = random.Random(seed)
    pairs_per_group = math.ceil(PAIR_COUNT / len(groups))
    reservoir_size = pairs_per_group * 2
    group_index = {group["key"]: index for index, group in enumerate(groups)}
    reservoirs = [[] for _ in groups]
    seen = [0] * len(groups)
    records = session.run(f"""
MATCH (e:{fd.source_label})
WHERE e.{fd.determinant} IN $keys
RETURN e.{fd.determinant} AS key, e.node_id AS node_id,
       elementId(e) AS element_id
ORDER BY key, node_id, element_id
""", keys=list(group_index))
    for record in records:
        index = group_index[record["key"]]
        seen[index] += 1
        member = {"node_id": record["node_id"],
                  "element_id": record["element_id"]}
        if len(reservoirs[index]) < reservoir_size:
            reservoirs[index].append(member)
        else:
            position = rng.randrange(seen[index])
            if position < reservoir_size:
                reservoirs[index][position] = member

    pairs = []
    for index, members in enumerate(reservoirs):
        rng.shuffle(members)
        pairs.extend(
            {"group": index, "members": (members[position],
                                         members[position + 1])}
            for position in range(0, len(members) - 1, 2)
        )
    rng.shuffle(pairs)
    theoretical = sum(count * (count - 1) // 2 for count in seen)
    return pairs[:PAIR_COUNT], theoretical


def sample_different_lhs_pairs(
    pairs: list[dict[str, Any]], seed: int
) -> tuple[list[dict[str, Any]], int]:
    """Re-pair the sampled nodes so every pair has different LHS values."""
    members_by_group: dict[int, list[dict[str, Any]]] = {}
    for pair in pairs:
        members_by_group.setdefault(pair["group"], []).extend(pair["members"])

    for members in members_by_group.values():
        members.sort(key=lambda member: (
            member["node_id"], member["element_id"]
        ))

    blocks: list[tuple[int, int, int]] = []
    theoretical = 0
    group_ids = sorted(members_by_group)
    for position, first_group in enumerate(group_ids):
        for second_group in group_ids[position + 1:]:
            theoretical += (
                len(members_by_group[first_group])
                * len(members_by_group[second_group])
            )
            blocks.append((theoretical, first_group, second_group))

    if theoretical < len(pairs):
        raise RuntimeError(
            "not enough unique different-LHS pairs to match same-LHS pairs"
        )

    rng = random.Random(seed + DIFFERENT_LHS_SEED_OFFSET)
    different_pairs = []
    block_ends = [end for end, _, _ in blocks]
    for pair_index in rng.sample(range(theoretical), len(pairs)):
        block_index = bisect_right(block_ends, pair_index)
        block_start = 0 if block_index == 0 else block_ends[block_index - 1]
        _, first_group, second_group = blocks[block_index]
        first_members = members_by_group[first_group]
        second_members = members_by_group[second_group]
        first_index, second_index = divmod(
            pair_index - block_start, len(second_members)
        )
        different_pairs.append({
            "groups": (first_group, second_group),
            "members": (
                first_members[first_index],
                second_members[second_index],
            ),
        })
    return different_pairs, theoretical


def mark_subgraph(session: Any, fd: FD,
                  pairs: list[dict[str, Any]]) -> dict[str, Any]:
    element_ids = sorted({member["element_id"] for pair in pairs
                          for member in pair["members"]})
    run(session, f"""
UNWIND $element_ids AS element_id
MATCH (e:{fd.source_label}) WHERE elementId(e) = element_id
SET e:{GRAPH_NODE}:{TARGET_NODE}
""", element_ids=element_ids)
    run(session, f"""
MATCH (e:{TARGET_NODE})--(neighbor)
WHERE NOT neighbor:{fd.source_label} AND NOT neighbor:{ARTIFACT_NODE}
  AND NOT neighbor:OFFSHORE_NR_ARTIFACT
SET neighbor:{GRAPH_NODE}
""")
    record = session.run(f"""
CALL () {{ MATCH (n:{GRAPH_NODE}) RETURN count(n) AS nodes }}
CALL () {{
  MATCH (a:{GRAPH_NODE})-[r]->(b:{GRAPH_NODE})
  WHERE NOT type(r) STARTS WITH 'HAS_EMB_'
    AND NOT type(r) STARTS WITH 'HAS_NR_'
  RETURN count(r) AS relationships, collect(DISTINCT type(r)) AS types
}}
RETURN nodes, relationships, types
""").single(strict=True)
    return {"nodes": int(record["nodes"]),
            "relationships": int(record["relationships"]),
            "types": list(record["types"])}


def read_node_details(session: Any, fd: FD,
                      pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Read original properties and degrees for the case-study nodes."""
    element_ids = sorted({member["element_id"] for pair in pairs
                          for member in pair["members"]})
    records = session.run(f"""
UNWIND $element_ids AS element_id
MATCH (e:{fd.source_label})
WHERE elementId(e) = element_id
OPTIONAL MATCH (e)-[r]-()
WHERE r IS NULL OR (
  NOT type(r) STARTS WITH 'HAS_EMB_'
  AND NOT type(r) STARTS WITH 'HAS_NR_'
)
WITH e, type(r) AS relationship_type, count(r) AS relationship_count
ORDER BY relationship_type
WITH e, sum(relationship_count) AS original_degree,
     collect(CASE WHEN relationship_type IS NULL THEN NULL ELSE
       {{type: relationship_type, count: relationship_count}} END) AS counts
RETURN e.node_id AS node_id, labels(e) AS labels,
       properties(e) AS properties, original_degree, counts
ORDER BY node_id
""", element_ids=element_ids)
    details = []
    for record in records:
        properties = {
            str(key): value
            for key, value in dict(record["properties"]).items()
            if key not in {"node_id", "internal_id", FEATURE}
            and not str(key).startswith(("offshore_emb_", "offshore_nr_"))
        }
        details.append({
            "node_id": int(record["node_id"]),
            "labels": [
                label for label in record["labels"]
                if not label.startswith(("OFFSHORE_EMB_", "OFFSHORE_NR_"))
            ],
            "properties": properties,
            "original_degree": int(record["original_degree"]),
            "original_relationship_type_counts": {
                item["type"]: int(item["count"])
                for item in record["counts"]
            },
        })
    return details


def hashed_feature(parts: Iterable[
    tuple[str, dict[str, Any], Iterable[str]]]) -> list[float]:
    vector = [0.0] * DIMENSION
    for namespace, properties, fields in parts:
        for field in sorted(fields):
            text = str(properties[field]).strip().lower()
            tokens = [f"{namespace}.{field}={text}"]
            tokens.extend(f"{namespace}.{field}:word={word}"
                          for word in WORD_RE.findall(text))
            for token in tokens:
                digest = hashlib.blake2b(
                    token.encode("utf-8"), digest_size=16).digest()
                index = int.from_bytes(digest[:8], "big") % DIMENSION
                vector[index] += 1.0 if digest[8] & 1 else -1.0
    length = math.sqrt(sum(value * value for value in vector))
    return vector if length == 0 else [value / length for value in vector]


def write_features(session: Any, rows: list[dict[str, Any]]) -> None:
    for start in range(0, len(rows), WRITE_BATCH):
        run(session, f"""
UNWIND $rows AS row
MATCH (n) WHERE elementId(n) = row.element_id
SET n.{FEATURE} = row.feature
""", rows=rows[start:start + WRITE_BATCH])


def prepare_features(session: Any, fd: FD,
                     pairs: list[dict[str, Any]]) -> tuple[
    dict[int, list[float]], dict[int, list[float]],
    list[dict[str, Any]]]:
    target_ids = {member["element_id"] for pair in pairs
                  for member in pair["members"]}
    records = list(session.run(f"""
MATCH (n:{GRAPH_NODE})
RETURN elementId(n) AS element_id, labels(n) AS labels,
       properties(n) AS properties
"""))
    denormalized, normalized, normalized_rows = {}, {}, []
    batch = []
    for record in records:
        element_id = record["element_id"]
        labels = sorted(label for label in record["labels"]
                        if not label.startswith("OFFSHORE_EMB_"))
        namespace = "+".join(labels) or "node"
        properties = {str(key): value
                      for key, value in dict(record["properties"]).items()}
        fields = {field for field in properties
                  if field not in {"node_id", "internal_id", FEATURE}
                  and not field.startswith("offshore_emb_")}
        dimension_fields = {fd.determinant, *fd.dependents}
        target_fields = fields - dimension_fields
        moved_fields = fields & dimension_fields
        if element_id in target_ids:
            # Keep token namespaces stable across the two schemas: the same
            # determinant and dependent features move to a dimension node.
            denormalized_feature = hashed_feature((
                (namespace, properties, target_fields),
                (fd.name, properties, moved_fields),
            ))
        else:
            denormalized_feature = hashed_feature((
                (namespace, properties, fields),
            ))
        batch.append({"element_id": element_id,
                      "feature": denormalized_feature})
        if element_id in target_ids:
            node_id = int(properties["node_id"])
            normalized_feature = hashed_feature((
                (namespace, properties, target_fields),
            ))
            denormalized[node_id] = denormalized_feature
            normalized[node_id] = normalized_feature
            normalized_rows.append({"element_id": element_id,
                                    "feature": normalized_feature})
        if len(batch) == WRITE_BATCH:
            write_features(session, batch)
            batch.clear()
    write_features(session, batch)
    return denormalized, normalized, normalized_rows


def materialize_normalized(session: Any, fd: FD,
                           groups: list[dict[str, Any]],
                           pairs: list[dict[str, Any]]) -> dict[str, int]:
    selected: dict[int, set[str]] = {}
    for pair in pairs:
        selected.setdefault(pair["group"], set()).update(
            member["element_id"] for member in pair["members"])

    rows = []
    for group_index, element_ids in selected.items():
        group = groups[group_index]
        properties = {fd.determinant: group["key"]}
        properties.update(zip(fd.dependents, group["rhs"]))
        rows.append({
            "properties": properties,
            "feature": hashed_feature((
                (fd.name, properties, (fd.determinant, *fd.dependents)),
            )),
            "element_ids": sorted(element_ids),
        })
    summary = run(session, f"""
UNWIND $rows AS row
CREATE (dimension:{ARTIFACT_NODE}:{GRAPH_NODE}:{fd.node_label})
SET dimension += row.properties, dimension.{FEATURE} = row.feature
WITH dimension, row
UNWIND row.element_ids AS element_id
MATCH (target:{TARGET_NODE}) WHERE elementId(target) = element_id
CREATE (target)-[:{fd.relationship}]->(dimension)
""", rows=rows)
    return {"dimension_nodes": summary.counters.nodes_created,
            "dimension_relationships": summary.counters.relationships_created}


def project(session: Any, name: str,
            relationship_types: list[str]) -> dict[str, int]:
    relationships = {
        relationship: {"orientation": "UNDIRECTED"}
        for relationship in relationship_types
    }
    record = session.run("""
CALL gds.graph.project($name, $nodes, $relationships)
YIELD nodeCount, relationshipCount
RETURN nodeCount, relationshipCount
""", name=name,
                         nodes={GRAPH_NODE: {"properties": [FEATURE]}},
                         relationships=relationships).single(strict=True)
    return {"nodes": int(record["nodeCount"]),
            "relationships": int(record["relationshipCount"])}


def embeddings(session: Any, procedure: str, graph: str,
               node_ids: list[int], configuration: dict[str, Any]
               ) -> dict[int, list[float]]:
    allowed = {"gds.fastRP.stream", "gds.node2vec.stream",
               "gds.hashgnn.stream", "gds.beta.graphSage.stream"}
    if procedure not in allowed:
        raise ValueError(f"unsupported GDS procedure: {procedure}")
    records = session.run(f"""
CALL {procedure}($graph, $configuration)
YIELD nodeId, embedding
WITH gds.util.asNode(nodeId) AS node, embedding
WHERE node:{TARGET_NODE} AND node.node_id IN $node_ids
RETURN node.node_id AS node_id, embedding
""", graph=graph, configuration=configuration, node_ids=node_ids)
    result = {int(record["node_id"]): [float(value) for value
                                       in record["embedding"]]
              for record in records}
    missing = set(node_ids) - result.keys()
    if missing:
        raise RuntimeError(f"{procedure} omitted {len(missing)} target nodes")
    if any(not math.isfinite(value) for vector in result.values()
           for value in vector):
        raise RuntimeError(f"{procedure} returned a non-finite embedding")
    return result


def graph_sage(session: Any, graph: str, model: str,
               node_ids: list[int], seed: int) -> dict[int, list[float]]:
    drop_model(session, model)
    try:
        run(session, "CALL gds.beta.graphSage.train($graph, $configuration)",
            graph=graph, configuration={
                "modelName": model, "featureProperties": [FEATURE],
                "embeddingDimension": DIMENSION, "aggregator": "mean",
                "activationFunction": "sigmoid", "maxIterations": 10,
                "randomSeed": seed, "concurrency": 1,
            })
        return embeddings(session, "gds.beta.graphSage.stream", graph,
                          node_ids, {"modelName": model, "concurrency": 1})
    finally:
        drop_model(session, model)


def cosine(first: list[float], second: list[float]) -> float:
    first_length = math.sqrt(sum(value * value for value in first))
    second_length = math.sqrt(sum(value * value for value in second))
    if first_length == 0 or second_length == 0:
        return 0.0
    return sum(a * b for a, b in zip(first, second)) / (
            first_length * second_length)


def score_pairs(denormalized: dict[int, list[float]],
                normalized: dict[int, list[float]],
                pairs: list[dict[str, Any]]) -> list[dict[str, float]]:
    scores = []
    for pair in pairs:
        first, second = (int(member["node_id"])
                         for member in pair["members"])
        denormalized_score = cosine(
            denormalized[first], denormalized[second])
        normalized_score = cosine(normalized[first], normalized[second])
        scores.append({
            "denormalized": denormalized_score,
            "normalized": normalized_score,
        })
    return scores


def summarize(scores: list[dict[str, float]]) -> dict[str, Any]:
    return {
        field: {"mean": fmean(score[field] for score in scores),
                "median": float(median(score[field] for score in scores))}
        for field in scores[0]
    }


def run_case(session: Any, fd: FD, seed: int) -> dict[str, Any]:
    groups = read_valid_groups(session, fd)
    pairs, theoretical = sample_pairs(session, fd, groups, seed)
    if not pairs:
        raise RuntimeError(
            f"no valid {fd.source_label} pairs for {fd.name}")
    different_lhs_pairs, different_lhs_theoretical = (
        sample_different_lhs_pairs(pairs, seed)
    )

    source_graph = mark_subgraph(session, fd, pairs)
    node_details = (
        read_node_details(session, fd, pairs)
        if fd.name == "address_country" else None
    )
    denormalized_properties, normalized_properties, normalized_rows = (
        prepare_features(session, fd, pairs))
    denormalized_projection = project(
        session, DENORMALIZED_GRAPH, source_graph["types"])

    write_features(session, normalized_rows)
    materialized = materialize_normalized(session, fd, groups, pairs)
    normalized_projection = project(
        session, NORMALIZED_GRAPH, [*source_graph["types"], fd.relationship])

    node_ids = sorted({int(member["node_id"]) for pair in pairs
                       for member in pair["members"]})
    method_scores = {
        "property_hash": score_pairs(
            denormalized_properties, normalized_properties, pairs)
    }
    different_lhs_method_scores = {
        "property_hash": score_pairs(
            denormalized_properties,
            normalized_properties,
            different_lhs_pairs,
        )
    }
    configurations = (
        ("fast_rp", "gds.fastRP.stream", {
            "embeddingDimension": DIMENSION,
            "iterationWeights": [0.0, 1.0, 1.0],
            "featureProperties": [FEATURE], "propertyRatio": 0.5,
            "randomSeed": seed, "concurrency": 1,
        }),
        ("node2vec", "gds.node2vec.stream", {
            "embeddingDimension": DIMENSION, "walkLength": 20,
            "walksPerNode": 5, "windowSize": 5, "iterations": 2,
            "randomSeed": seed, "concurrency": 1,
        }),
        ("hash_gnn", "gds.hashgnn.stream", {
            "featureProperties": [FEATURE], "iterations": 1,
            "embeddingDensity": DIMENSION, "neighborInfluence": 1.0,
            "binarizeFeatures": {"dimension": DIMENSION * 4,
                                 "threshold": 0.0},
            "outputDimension": DIMENSION, "randomSeed": seed,
            "concurrency": 1,
        }),
    )
    for name, procedure, configuration in configurations:
        # print(f"  {name}", flush=True)
        denormalized = embeddings(session, procedure, DENORMALIZED_GRAPH,
                                  node_ids, configuration)
        normalized = embeddings(session, procedure, NORMALIZED_GRAPH,
                                node_ids, configuration)
        method_scores[name] = score_pairs(denormalized, normalized, pairs)
        different_lhs_method_scores[name] = score_pairs(
            denormalized, normalized, different_lhs_pairs
        )

    # print("  graph_sage", flush=True)
    denormalized_sage = graph_sage(
        session, DENORMALIZED_GRAPH, DENORMALIZED_MODEL, node_ids, seed)
    normalized_sage = graph_sage(
        session, NORMALIZED_GRAPH, NORMALIZED_MODEL, node_ids, seed)
    method_scores["graph_sage"] = score_pairs(
        denormalized_sage, normalized_sage, pairs)
    different_lhs_method_scores["graph_sage"] = score_pairs(
        denormalized_sage, normalized_sage, different_lhs_pairs)

    result = {
        "random_seed": seed,
        "node_table": fd.source_label,
        "functional_dependency": (
            f"{fd.determinant} -> ({', '.join(fd.dependents)})"),
        "valid_group_count": len(groups),
        "valid_node_count": sum(group["fanout"] for group in groups),
        "theoretical_pair_count": theoretical,
        "pair_count": len(pairs),
        "selection_strategy": (
            "balanced per-group reservoir sampling; pairs within a group "
            "do not reuse a source node"),
        "different_lhs_pair_sample_seed": (
            seed + DIFFERENT_LHS_SEED_OFFSET),
        "different_lhs_theoretical_pair_count": different_lhs_theoretical,
        "different_lhs_pair_count": len(different_lhs_pairs),
        "different_lhs_selection_strategy": (
            "unique pairs with different determinant values, sampled from "
            "the same source-node pool as the same-LHS pairs; a source node "
            "may occur in more than one control pair"),
        "property_hash_scope": (
            "target source node; the normalized vector excludes the "
            "determinant and dependent properties moved to the dimension node"),
        "source_sample_graph": {key: value for key, value in source_graph.items()
                                if key != "types"},
        "materialized_normalization": materialized,
        "projected_graphs": {
            "denormalized": denormalized_projection,
            "normalized": normalized_projection,
        },
        "similarities": {name: summarize(scores)
                         for name, scores in method_scores.items()},
        "different_lhs_similarities": {
            name: summarize(scores)
            for name, scores in different_lhs_method_scores.items()
        },
    }
    if node_details is not None:
        result["nodes"] = node_details
        result["pairs"] = [
            {"determinant": groups[pair["group"]]["key"],
             "node_ids": [int(member["node_id"])
                          for member in pair["members"]],
             "similarities": {name: scores[index]
                              for name, scores in method_scores.items()}}
            for index, pair in enumerate(pairs)
        ]
        result["different_lhs_pairs"] = [
            {
                "determinants": [
                    groups[group]["key"] for group in pair["groups"]
                ],
                "node_ids": [int(member["node_id"])
                             for member in pair["members"]],
                "similarities": {
                    name: scores[index]
                    for name, scores in different_lhs_method_scores.items()
                },
            }
            for index, pair in enumerate(different_lhs_pairs)
        ]
    return result


def write_report(report: dict[str, Any]) -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n",
                         encoding="utf-8")
    temporary.replace(OUTPUT)


def main() -> None:
    from neo4j import GraphDatabase, __version__ as driver_version

    print(f"Connecting to {DATABASE} at {NEO4J_URI}")
    with GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
        driver.verify_connectivity()
        with driver.session(database=DATABASE) as session:
            gds_version = session.run(
                "RETURN gds.version() AS version").single(strict=True)["version"]
            cleanup(session)
            report: dict[str, Any] = {
                "experiment": "offshore_table_fd_normalization_embeddings",
                "created_at": datetime.now().astimezone().isoformat(),
                "database": DATABASE,
                "neo4j_driver": driver_version,
                "gds_version": gds_version,
                "graph_scope": (
                    "sampled source-table nodes, their non-source-table one-hop "
                    "neighbors, and dimensions connected only to sampled nodes"),
                "relationship_orientation": "UNDIRECTED",
                "settings": {"pair_count": PAIR_COUNT,
                             "embedding_dimension": DIMENSION,
                             "random_seeds": list(RANDOM_SEEDS)},
                "experiments": {fd.name: [] for fd in FDS},
            }
            write_report(report)
            for number, fd in enumerate(FDS, start=1):
                for repeat, seed in enumerate(RANDOM_SEEDS, start=1):
                    print(
                        f"[case {number}/{len(FDS)}, "
                        f"seed {repeat}/{len(RANDOM_SEEDS)}] "
                        f"{fd.source_label}: {fd.determinant} -> "
                        f"({', '.join(fd.dependents)}), seed={seed}",
                        flush=True,
                    )
                    try:
                        result = run_case(session, fd, seed)
                    finally:
                        cleanup(session)
                    result["repeat"] = repeat
                    report["experiments"][fd.name].append(result)
                    write_report(report)

    print(f"\nExperiment complete: {OUTPUT}")


if __name__ == "__main__":
    main()
