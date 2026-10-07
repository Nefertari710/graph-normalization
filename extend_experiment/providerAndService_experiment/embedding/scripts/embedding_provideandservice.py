#!/usr/bin/env python3
"""Compare one-step Provider and Service normalizations.

Provider case: same-NPI Record pairs, N0 -> Provider-normalized N1.
Service case:  same-HCPCS Record pairs, N0 -> Service-normalized N2.

N0 contains only denormalized Record nodes. Every projected graph keeps one
non-semantic self-loop per Record so topology-only GDS methods can run without
connecting different Records.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import re
from datetime import datetime
from pathlib import Path
from statistics import fmean, median
from typing import Any, Iterable

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
DATABASE = "providerandservicecsv"

PAIR_COUNT = 1_000
RANDOM_SEEDS = range(20260917, 20260927)
DIMENSION = 128
WRITE_BATCH = 500
RELATIONSHIP_ORIENTATION = "UNDIRECTED"  # or "NATURAL"
OUTPUT = (
        Path(__file__).resolve().parents[1]
        / "results"
        / DATABASE
        / "embedding_providerandservice.json"
)

PROVIDER_DETERMINANT = "rndrng_npi"
PROVIDER_DEPENDENTS = (
    "rndrng_prvdr_last_org_name",
    "rndrng_prvdr_first_name",
    "rndrng_prvdr_mi",
    "rndrng_prvdr_ent_cd",
    "rndrng_prvdr_st1",
    "rndrng_prvdr_st2",
    "rndrng_prvdr_city",
    "rndrng_prvdr_state_abrvtn",
    "rndrng_prvdr_zip5",
    "rndrng_prvdr_cntry",
)
SERVICE_DETERMINANT = "hcpcs_cd"
SERVICE_DEPENDENTS = ("hcpcs_desc", "hcpcs_drug_ind")

GRAPH_NODE = "PROVIDER_SERVICE_EMB_NODE"
TARGET_NODE = "PROVIDER_SERVICE_EMB_TARGET"
ARTIFACT_NODE = "PROVIDER_SERVICE_EMB_ARTIFACT"
PROVIDER_NODE = "PROVIDER_SERVICE_EMB_PROVIDER"
SERVICE_NODE = "PROVIDER_SERVICE_EMB_SERVICE"
SELF_RELATIONSHIP = "HAS_EMB_SELF"
PROVIDER_RELATIONSHIP = "HAS_EMB_PROVIDER"
SERVICE_RELATIONSHIP = "HAS_EMB_SERVICE"
FEATURE = "provider_service_emb_features"
SOURCE_INDEXES = {
    "provider_service_emb_source_npi": PROVIDER_DETERMINANT,
    "provider_service_emb_source_hcpcs": SERVICE_DETERMINANT,
}
DENORMALIZED_GRAPH = "provider_service_embedding_denormalized"
NORMALIZED_GRAPH = "provider_service_embedding_normalized"
DENORMALIZED_MODEL = "provider_service_embedding_denormalized_sage"
NORMALIZED_MODEL = "provider_service_embedding_normalized_sage"
WORD_RE = re.compile(r"\w+", re.UNICODE)

CASES = {
    "provider": {
        "determinant": PROVIDER_DETERMINANT,
        "dependents": PROVIDER_DEPENDENTS,
        "namespace": "provider",
        "node_label": PROVIDER_NODE,
        "relationship": PROVIDER_RELATIONSHIP,
        "group_label": "NPI",
        "comparison": "N0 Denormalized -> N1 Provider-normalized",
    },
    "service": {
        "determinant": SERVICE_DETERMINANT,
        "dependents": SERVICE_DEPENDENTS,
        "namespace": "service",
        "node_label": SERVICE_NODE,
        "relationship": SERVICE_RELATIONSHIP,
        "group_label": "HCPCS service",
        "comparison": "N0 Denormalized -> N2 Service-normalized",
    },
}


def run(session: Any, query: str, **parameters: Any) -> Any:
    return session.run(query, parameters).consume()


def drop_graph(session: Any, name: str) -> None:
    exists = session.run(
        "CALL gds.graph.exists($name) YIELD exists RETURN exists", name=name
    ).single(strict=True)["exists"]
    if exists:
        run(
            session,
            "CALL gds.graph.drop($name) YIELD graphName RETURN graphName",
            name=name,
        )


def drop_model(session: Any, name: str) -> None:
    run(
        session,
        "CALL gds.model.drop($name, false) YIELD modelName RETURN modelName",
        name=name,
    )


def cleanup(session: Any) -> None:
    errors = []
    for dropper, names in (
            (drop_graph, (DENORMALIZED_GRAPH, NORMALIZED_GRAPH)),
            (drop_model, (DENORMALIZED_MODEL, NORMALIZED_MODEL)),
    ):
        for name in names:
            try:
                dropper(session, name)
            except Exception as error:
                errors.append(error)
    for query in (
            f"MATCH ()-[relationship:{SELF_RELATIONSHIP}]->() "
            "DELETE relationship",
            f"MATCH (node:{ARTIFACT_NODE}) DETACH DELETE node",
            f"MATCH (node:{GRAPH_NODE}) "
            f"REMOVE node.{FEATURE}, node:{GRAPH_NODE}, node:{TARGET_NODE}",
    ):
        try:
            run(session, query)
        except Exception as error:
            errors.append(error)
    if errors:
        raise errors[0]


def create_source_index(session: Any) -> None:
    for name, field in SOURCE_INDEXES.items():
        run(
            session,
            f"CREATE INDEX {name} IF NOT EXISTS "
            f"FOR (record:ProviderServiceRecord) ON (record.{field})",
        )
    run(session, "CALL db.awaitIndexes(1800)")


def drop_source_index(session: Any) -> None:
    for name in SOURCE_INDEXES:
        run(session, f"DROP INDEX {name} IF EXISTS")


def valid_group_query(determinant: str, dependents: tuple[str, ...]) -> str:
    present = " AND ".join(
        f"record.{field} IS NOT NULL" for field in dependents
    )
    values = ", ".join(f"record.{field}" for field in dependents)
    return f"""
MATCH (record:ProviderServiceRecord)
WHERE record.{determinant} IS NOT NULL
  AND record.{determinant} <> ''
WITH record.{determinant} AS key,
     count(*) AS fanout,
     sum(CASE WHEN {present} THEN 1 ELSE 0 END) AS complete,
     collect(DISTINCT CASE WHEN {present} THEN [{values}] END) AS variants
WHERE fanout >= 2 AND complete = fanout AND size(variants) = 1
RETURN key, fanout
ORDER BY key
"""


def sample_workload(
        session: Any, case: dict[str, Any], seed: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Sample determinant groups and one Record pair for one seed."""

    determinant = case["determinant"]
    dependents = case["dependents"]
    group_label = case["group_label"]

    rng = random.Random(seed)
    groups: list[dict[str, Any]] = []
    group_count = record_count = theoretical_pairs = 0

    # Streaming reservoir sampling keeps only PAIR_COUNT of all valid groups.
    for record in session.run(valid_group_query(determinant, dependents)):
        fanout = int(record["fanout"])
        group = {"key": record["key"], "fanout": fanout}
        group_count += 1
        record_count += fanout
        theoretical_pairs += fanout * (fanout - 1) // 2
        if len(groups) < PAIR_COUNT:
            groups.append(group)
        else:
            position = rng.randrange(group_count)
            if position < PAIR_COUNT:
                groups[position] = group

    if not groups:
        raise RuntimeError(
            f"No complete, conflict-free {group_label} has two records"
        )
    groups.sort(key=lambda item: str(item["key"]))

    group_index = {group["key"]: index for index, group in enumerate(groups)}
    reservoirs: list[list[dict[str, Any]]] = [[] for _ in groups]
    seen = [0] * len(groups)
    records = session.run(
        f"""
MATCH (record:ProviderServiceRecord)
WHERE record.{determinant} IN $keys
RETURN record.{determinant} AS key,
       record.{PROVIDER_DETERMINANT} AS rndrng_npi,
       record.hcpcs_cd AS hcpcs_cd,
       record.place_of_srvc AS place_of_srvc,
       elementId(record) AS element_id
ORDER BY key, rndrng_npi, hcpcs_cd, place_of_srvc, element_id
""",
        keys=list(group_index),
    )
    for record in records:
        index = group_index[record["key"]]
        seen[index] += 1
        member = {
            "element_id": record["element_id"],
            "record_key": {
                PROVIDER_DETERMINANT: record["rndrng_npi"],
                "hcpcs_cd": record["hcpcs_cd"],
                "place_of_srvc": record["place_of_srvc"],
            },
        }
        if len(reservoirs[index]) < 2:
            reservoirs[index].append(member)
        else:
            position = rng.randrange(seen[index])
            if position < 2:
                reservoirs[index][position] = member

    pairs = []
    for index, members in enumerate(reservoirs):
        if len(members) != 2 or seen[index] != groups[index]["fanout"]:
            raise RuntimeError(
                f"A sampled {group_label} did not yield its expected records"
            )
        pairs.append(
            {"determinant": groups[index]["key"], "members": tuple(members)}
        )

    population = {
        "pairable_group_count": group_count,
        "pairable_record_count": record_count,
        "theoretical_pair_count": theoretical_pairs,
        "sampled_group_count": len(groups),
        "sampled_group_record_count": sum(group["fanout"] for group in groups),
    }
    return pairs, population


def hashed_feature(
        parts: Iterable[tuple[str, dict[str, Any], Iterable[str]]],
) -> list[float]:
    vector = [0.0] * DIMENSION
    for namespace, properties, fields in parts:
        for field in sorted(fields):
            value = properties.get(field, "<MISSING>")
            text = "<MISSING>" if value is None else str(value).strip().lower()
            text = text or "<MISSING>"
            tokens = [f"{namespace}.{field}={text}"] + [
                f"{namespace}.{field}:word={word}"
                for word in WORD_RE.findall(text)
            ]
            for token in tokens:
                digest = hashlib.blake2b(
                    token.encode("utf-8"), digest_size=16
                ).digest()
                index = int.from_bytes(digest[:8], "big") % DIMENSION
                vector[index] += 1.0 if digest[8] & 1 else -1.0
    length = vector_length(vector)
    return vector if length == 0 else [value / length for value in vector]


def vector_length(vector: Iterable[float]) -> float:
    return math.sqrt(sum(value * value for value in vector))


def write_features(session: Any, rows: list[dict[str, Any]]) -> None:
    for start in range(0, len(rows), WRITE_BATCH):
        run(
            session,
            f"""
UNWIND $rows AS row
MATCH (node) WHERE elementId(node) = row.element_id
SET node.{FEATURE} = row.feature
""",
            rows=rows[start: start + WRITE_BATCH],
        )


def prepare_features(
        session: Any,
        pairs: list[dict[str, Any]],
        case: dict[str, Any],
) -> tuple[
    dict[str, list[float]],
    dict[str, list[float]],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    determinant = case["determinant"]
    dependents = case["dependents"]
    namespace = case["namespace"]
    element_ids = sorted(
        member["element_id"] for pair in pairs for member in pair["members"]
    )
    if len(set(element_ids)) != len(pairs) * 2:
        raise RuntimeError("Sampled pairs unexpectedly reuse a record")
    run(
        session,
        f"""
UNWIND $element_ids AS element_id
MATCH (record:ProviderServiceRecord) WHERE elementId(record) = element_id
SET record:{GRAPH_NODE}:{TARGET_NODE}
""",
        element_ids=element_ids,
    )

    normalization_fields = {determinant, *dependents}
    denormalized: dict[str, list[float]] = {}
    normalized: dict[str, list[float]] = {}
    denormalized_rows = []
    normalized_rows = []
    entities: dict[str, dict[str, Any]] = {}
    records = session.run(
        f"MATCH (record:{TARGET_NODE}) "
        "RETURN elementId(record) AS element_id, properties(record) AS properties"
    )
    for record in records:
        element_id = record["element_id"]
        properties = dict(record["properties"])
        missing = normalization_fields - properties.keys()
        if missing:
            raise RuntimeError(
                f"Missing {namespace.title()} fields: {sorted(missing)}"
            )

        fields = set(properties) - {FEATURE}
        record_fields = fields - normalization_fields
        denormalized[element_id] = hashed_feature(
            (
                ("record", properties, record_fields),
                (namespace, properties, normalization_fields),
            )
        )
        normalized[element_id] = hashed_feature(
            (("record", properties, record_fields),)
        )
        denormalized_rows.append(
            {"element_id": element_id, "feature": denormalized[element_id]}
        )
        normalized_rows.append(
            {"element_id": element_id, "feature": normalized[element_id]}
        )

        entity_properties = {
            field: properties[field] for field in normalization_fields
        }
        entity = entities.setdefault(
            properties[determinant],
            {"properties": entity_properties, "element_ids": []},
        )
        if entity["properties"] != entity_properties:
            raise RuntimeError(
                f"The sampled data violates the {namespace.title()} dependency"
            )
        entity["element_ids"].append(element_id)

    if len(denormalized) != len(element_ids):
        raise RuntimeError("Some sampled records could not be found")
    write_features(session, denormalized_rows)
    return denormalized, normalized, normalized_rows, entities


def materialize_self_loops(
        session: Any, pairs: list[dict[str, Any]]
) -> dict[str, int]:
    """Give every Record one non-semantic edge to itself."""

    element_ids = sorted(
        member["element_id"] for pair in pairs for member in pair["members"]
    )
    summary = run(
        session,
        f"""
UNWIND $element_ids AS element_id
MATCH (record:{TARGET_NODE}) WHERE elementId(record) = element_id
CREATE (record)-[:{SELF_RELATIONSHIP}]->(record)
""",
        element_ids=element_ids,
    )
    return {
        "self_relationships": summary.counters.relationships_created,
    }


def materialize_normalized(
        session: Any,
        pairs: list[dict[str, Any]],
        entities: dict[str, dict[str, Any]],
        case: dict[str, Any],
) -> dict[str, int]:
    determinant = case["determinant"]
    dependents = case["dependents"]
    namespace = case["namespace"]
    node_label = case["node_label"]
    relationship = case["relationship"]
    entity_fields = (determinant, *dependents)
    rows = []
    for pair in pairs:
        entity = entities[pair["determinant"]]
        properties = entity["properties"]
        rows.append(
            {
                "properties": properties,
                "feature": hashed_feature(
                    ((namespace, properties, entity_fields),)
                ),
                "element_ids": [
                    member["element_id"] for member in pair["members"]
                ],
            }
        )
    summary = run(
        session,
        f"""
UNWIND $rows AS row
CREATE (entity:{ARTIFACT_NODE}:{GRAPH_NODE}:{node_label})
SET entity = row.properties, entity.{FEATURE} = row.feature
WITH entity, row
UNWIND row.element_ids AS element_id
MATCH (record:{TARGET_NODE}) WHERE elementId(record) = element_id
CREATE (record)-[:{relationship}]->(entity)
""",
        rows=rows,
    )
    return {
        "normalization_nodes": summary.counters.nodes_created,
        "normalization_relationships": summary.counters.relationships_created,
    }


def project(
        session: Any, name: str, relationship_types: list[str]
) -> dict[str, int]:
    if RELATIONSHIP_ORIENTATION not in {"NATURAL", "UNDIRECTED"}:
        raise ValueError("RELATIONSHIP_ORIENTATION must be NATURAL or UNDIRECTED")
    relationships = {
        relationship: {"orientation": RELATIONSHIP_ORIENTATION}
        for relationship in relationship_types
    }
    record = session.run(
        """
CALL gds.graph.project($name, $nodes, $relationships)
YIELD nodeCount, relationshipCount
RETURN nodeCount AS nodes, relationshipCount AS relationships
""",
        name=name,
        nodes={GRAPH_NODE: {"properties": [FEATURE]}},
        relationships=relationships,
    ).single(strict=True)
    return {"nodes": int(record["nodes"]), "relationships": int(record["relationships"])}


def embeddings(
        session: Any,
        procedure: str,
        graph: str,
        element_ids: list[str],
        configuration: dict[str, Any],
) -> dict[str, list[float]]:
    if procedure not in {
        "gds.fastRP.stream",
        "gds.node2vec.stream",
        "gds.hashgnn.stream",
        "gds.beta.graphSage.stream",
    }:
        raise ValueError(f"Unsupported GDS procedure: {procedure}")
    records = session.run(
        f"""
CALL {procedure}($graph, $configuration)
YIELD nodeId, embedding
WITH gds.util.asNode(nodeId) AS node, embedding
WHERE node:{TARGET_NODE} AND elementId(node) IN $element_ids
RETURN elementId(node) AS element_id, embedding
""",
        graph=graph,
        configuration=configuration,
        element_ids=element_ids,
    )
    result = {
        record["element_id"]: [float(value) for value in record["embedding"]]
        for record in records
    }
    if set(element_ids) != result.keys():
        raise RuntimeError(f"{procedure} did not return every target record")
    if any(not math.isfinite(x) for vector in result.values() for x in vector):
        raise RuntimeError(f"{procedure} returned a non-finite embedding")
    return result


def graph_sage(
        session: Any,
        graph: str,
        model: str,
        element_ids: list[str],
        seed: int,
) -> dict[str, list[float]]:
    drop_model(session, model)
    try:
        run(
            session,
            """
CALL gds.beta.graphSage.train($graph, $configuration)
YIELD modelInfo
RETURN modelInfo
""",
            graph=graph,
            configuration={
                "modelName": model,
                "featureProperties": [FEATURE],
                "embeddingDimension": DIMENSION,
                "aggregator": "mean",
                "activationFunction": "sigmoid",
                "maxIterations": 10,
                "randomSeed": seed,
            },
        )
        return embeddings(
            session,
            "gds.beta.graphSage.stream",
            graph,
            element_ids,
            {"modelName": model, "concurrency": 1},
        )
    finally:
        drop_model(session, model)


def cosine(first: list[float], second: list[float]) -> float:
    first_length = vector_length(first)
    second_length = vector_length(second)
    if first_length == 0 or second_length == 0:
        return 0.0
    return sum(a * b for a, b in zip(first, second)) / (
            first_length * second_length
    )


def score_pairs(
        denormalized: dict[str, list[float]],
        normalized: dict[str, list[float]],
        pairs: list[dict[str, Any]],
) -> list[dict[str, float]]:
    scores = []
    for pair in pairs:
        first, second = [
            member["element_id"] for member in pair["members"]
        ]
        denorm = cosine(denormalized[first], denormalized[second])
        norm = cosine(normalized[first], normalized[second])
        scores.append(
            {
                "denormalized": denorm,
                "normalized": norm,
                "delta_normalized_minus_denormalized": norm - denorm,
            }
        )
    return scores


def run_embedding_method(
        session: Any,
        procedure: str,
        element_ids: list[str],
        configuration: dict[str, Any],
        pairs: list[dict[str, Any]],
) -> list[dict[str, float]]:
    denormalized = embeddings(
        session, procedure, DENORMALIZED_GRAPH, element_ids, configuration
    )
    normalized = embeddings(
        session, procedure, NORMALIZED_GRAPH, element_ids, configuration
    )
    return score_pairs(denormalized, normalized, pairs)


def summarize(scores: list[dict[str, float]]) -> dict[str, Any]:
    return {
        field: {
            "mean": fmean(score[field] for score in scores),
            "median": float(median(score[field] for score in scores)),
        }
        for field in scores[0]
    }


def run_case(
        session: Any,
        pairs: list[dict[str, Any]],
        population: dict[str, int],
        seed: int,
        case: dict[str, Any],
) -> dict[str, Any]:
    determinant = case["determinant"]
    dependents = case["dependents"]
    group_label = case["group_label"]
    denorm_hash, norm_hash, normalized_rows, entities = prepare_features(
        session, pairs, case
    )
    self_loops = materialize_self_loops(session, pairs)
    denormalized_projection = project(
        session, DENORMALIZED_GRAPH, [SELF_RELATIONSHIP]
    )

    write_features(session, normalized_rows)
    materialized = materialize_normalized(session, pairs, entities, case)
    normalized_projection = project(
        session,
        NORMALIZED_GRAPH,
        [SELF_RELATIONSHIP, case["relationship"]],
    )

    target_count = len(pairs) * 2
    if self_loops != {"self_relationships": target_count}:
        raise RuntimeError("Unexpected Record self-loop structure")
    if materialized != {
        "normalization_nodes": len(entities),
        "normalization_relationships": target_count,
    } or len(entities) != len(pairs):
        raise RuntimeError(
            f"Unexpected {case['namespace'].title()} normalization structure"
        )
    relationship_multiplier = 2 if RELATIONSHIP_ORIENTATION == "UNDIRECTED" else 1
    if (
            denormalized_projection["nodes"] != target_count
            or denormalized_projection["relationships"]
            not in {target_count, target_count * 2}
    ):
        raise RuntimeError(f"Unexpected denormalized graph: {denormalized_projection}")
    expected_normalized = {
        "nodes": target_count + materialized["normalization_nodes"],
        "relationships": (
                denormalized_projection["relationships"]
                + materialized["normalization_relationships"]
                * relationship_multiplier
        ),
    }
    if normalized_projection != expected_normalized:
        raise RuntimeError(f"Unexpected normalized graph: {normalized_projection}")

    element_ids = sorted(
        member["element_id"] for pair in pairs for member in pair["members"]
    )
    method_scores = {"property_hash": score_pairs(denorm_hash, norm_hash, pairs)}
    configurations = (
        (
            "fast_rp",
            "gds.fastRP.stream",
            {
                "embeddingDimension": DIMENSION,
                "iterationWeights": [0.0, 1.0, 1.0],
                "featureProperties": [FEATURE],
                "propertyRatio": 0.5,
                "randomSeed": seed,
            },
        ),
        (
            "node2vec",
            "gds.node2vec.stream",
            {
                "embeddingDimension": DIMENSION,
                "randomSeed": seed,
            },
        ),
        (
            "hash_gnn",
            "gds.hashgnn.stream",
            {
                "featureProperties": [FEATURE],
                "iterations": 1,
                "embeddingDensity": DIMENSION,
                "neighborInfluence": 1.0,
                "binarizeFeatures": {
                    "dimension": DIMENSION * 4,
                    "threshold": 0.0,
                },
                "outputDimension": DIMENSION,
                "randomSeed": seed,
            },
        ),
    )
    for name, procedure, configuration in configurations:
        method_scores[name] = run_embedding_method(
            session, procedure, element_ids, configuration, pairs
        )

    denormalized_sage = graph_sage(
        session, DENORMALIZED_GRAPH, DENORMALIZED_MODEL, element_ids, seed
    )
    normalized_sage = graph_sage(
        session, NORMALIZED_GRAPH, NORMALIZED_MODEL, element_ids, seed
    )
    method_scores["graph_sage"] = score_pairs(
        denormalized_sage, normalized_sage, pairs
    )

    return {
        "random_seed": seed,
        "pair_sample_seed": seed,
        "comparison": case["comparison"],
        "functional_dependency": (
            f"{determinant} -> ({', '.join(dependents)})"
        ),
        **population,
        "pair_count": len(pairs),
        "selection_strategy": (
            f"streaming reservoir sample of pairable {group_label} groups; "
            f"one same-{group_label} pair per selected group; groups and "
            "pairs are resampled for every embedding seed"
        ),
        "projected_graphs": {
            "denormalized": denormalized_projection,
            "normalized": normalized_projection,
        },
        "materialized_self_loops": self_loops,
        "materialized_normalization": materialized,
        "similarities": {
            name: summarize(scores) for name, scores in method_scores.items()
        },
        "pairs": [
            {
                "determinant": pair["determinant"],
                "record_keys": [
                    member["record_key"] for member in pair["members"]
                ],
                "similarities": {
                    name: scores[index]
                    for name, scores in method_scores.items()
                },
            }
            for index, pair in enumerate(pairs)
        ],
    }


def write_report(report: dict[str, Any]) -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    temporary.replace(OUTPUT)


def main() -> None:
    from neo4j import GraphDatabase, __version__ as driver_version

    print(f"Connecting to {DATABASE} at {NEO4J_URI}")
    with GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
    ) as driver:
        driver.verify_connectivity()
        with driver.session(database=DATABASE) as session:
            gds_version = session.run(
                "RETURN gds.version() AS version"
            ).single(strict=True)["version"]
            cleanup(session)
            drop_source_index(session)
            try:
                create_source_index(session)
                report: dict[str, Any] = {
                    "experiment": (
                        "provider_and_service_fd_normalization_embeddings"
                    ),
                    "created_at": datetime.now().astimezone().isoformat(),
                    "database": DATABASE,
                    "neo4j_driver": driver_version,
                    "gds_version": gds_version,
                    "relationship_orientation": RELATIONSHIP_ORIENTATION,
                    "settings": {
                        "pair_count": PAIR_COUNT,
                        "pair_sampling": "resampled for each random seed",
                        "embedding_dimension": DIMENSION,
                        "random_seeds": list(RANDOM_SEEDS),
                        "methods": [
                            "property_hash",
                            "fast_rp",
                            "node2vec",
                            "hash_gnn",
                            "graph_sage",
                        ],
                    },
                    "method_scope": (
                        "N0 contains only denormalized Record nodes; every "
                        "projection keeps one non-semantic self-loop per "
                        "Record; N1 adds only Provider normalization and N2 "
                        "adds only Service normalization"
                    ),
                    "experiments": {
                        case_name: [] for case_name in CASES
                    },
                }
                write_report(report)

                for repeat, seed in enumerate(RANDOM_SEEDS, start=1):
                    for case_name, case in CASES.items():
                        print(
                            f"[seed {repeat}/{len(RANDOM_SEEDS)}] "
                            f"case={case_name}, seed={seed}; sampling pairs",
                            flush=True,
                        )
                        pairs, population = sample_workload(
                            session, case, seed
                        )
                        try:
                            result = run_case(
                                session, pairs, population, seed, case
                            )
                        finally:
                            cleanup(session)
                        result["repeat"] = repeat
                        report["experiments"][case_name].append(result)
                        write_report(report)
            finally:
                try:
                    cleanup(session)
                finally:
                    drop_source_index(session)

    print(f"\nExperiment complete: {OUTPUT}")


if __name__ == "__main__":
    main()
