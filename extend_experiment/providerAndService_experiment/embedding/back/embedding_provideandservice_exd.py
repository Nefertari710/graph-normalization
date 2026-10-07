#!/usr/bin/env python3
"""Run two sequential normalizations on one fixed same-NPI workload.

N0 Denormalized -> N1 Service-normalized -> N2 Service+Provider-normalized
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
PAIR_SAMPLE_SEED = 20260920
RANDOM_SEEDS = range(20260917, 20260927)
DIMENSION = 128
WRITE_BATCH = 500
RELATIONSHIP_ORIENTATION = "UNDIRECTED"  # or "NATURAL"
OUTPUT = (
        Path(__file__).resolve().parents[1]
        / "results"
        / DATABASE
        / "embedding_providerandservice_exd.json"
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

GRAPH_NODE = "PROVIDER_SERVICE_EXD_EMB_NODE"
TARGET_NODE = "PROVIDER_SERVICE_EXD_EMB_TARGET"
ARTIFACT_NODE = "PROVIDER_SERVICE_EXD_EMB_ARTIFACT"
CONTEXT_NODE = "PROVIDER_SERVICE_EXD_EMB_CONTEXT"
PROVIDER_NODE = "PROVIDER_SERVICE_EXD_EMB_PROVIDER"
SERVICE_NODE = "PROVIDER_SERVICE_EXD_EMB_SERVICE"
CONTEXT_RELATIONSHIP = "HAS_EXD_CONTEXT"
PROVIDER_RELATIONSHIP = "HAS_EXD_PROVIDER"
SERVICE_RELATIONSHIP = "HAS_EXD_SERVICE"
FEATURE = "provider_service_exd_emb_features"
SOURCE_INDEX = "provider_service_exd_emb_source_npi"
DENORMALIZED_GRAPH = "provider_service_exd_denormalized"
SERVICE_NORMALIZED_GRAPH = "provider_service_exd_service_normalized"
SERVICE_PROVIDER_NORMALIZED_GRAPH = (
    "provider_service_exd_service_provider_normalized"
)
DENORMALIZED_MODEL = "provider_service_exd_denormalized_sage"
SERVICE_NORMALIZED_MODEL = "provider_service_exd_service_normalized_sage"
SERVICE_PROVIDER_NORMALIZED_MODEL = (
    "provider_service_exd_service_provider_normalized_sage"
)
WORD_RE = re.compile(r"\w+", re.UNICODE)


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
            (
                    drop_graph,
                    (
                            DENORMALIZED_GRAPH,
                            SERVICE_NORMALIZED_GRAPH,
                            SERVICE_PROVIDER_NORMALIZED_GRAPH,
                    ),
            ),
            (
                    drop_model,
                    (
                            DENORMALIZED_MODEL,
                            SERVICE_NORMALIZED_MODEL,
                            SERVICE_PROVIDER_NORMALIZED_MODEL,
                    ),
            ),
    ):
        for name in names:
            try:
                dropper(session, name)
            except Exception as error:
                errors.append(error)
    for query in (
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
    run(
        session,
        f"CREATE INDEX {SOURCE_INDEX} IF NOT EXISTS "
        f"FOR (record:ProviderServiceRecord) ON "
        f"(record.{PROVIDER_DETERMINANT})",
    )
    run(session, "CALL db.awaitIndexes(1800)")


def drop_source_index(session: Any) -> None:
    run(session, f"DROP INDEX {SOURCE_INDEX} IF EXISTS")


def valid_group_query() -> str:
    required_fields = (
        *PROVIDER_DEPENDENTS,
        SERVICE_DETERMINANT,
        *SERVICE_DEPENDENTS,
    )
    present = " AND ".join(
        [f"record.{field} IS NOT NULL" for field in required_fields]
        + [f"record.{SERVICE_DETERMINANT} <> ''"]
    )
    values = ", ".join(
        f"record.{field}" for field in PROVIDER_DEPENDENTS
    )
    return f"""
MATCH (record:ProviderServiceRecord)
WHERE record.{PROVIDER_DETERMINANT} IS NOT NULL
  AND record.{PROVIDER_DETERMINANT} <> ''
WITH record.{PROVIDER_DETERMINANT} AS key,
     count(*) AS fanout,
     sum(CASE WHEN {present} THEN 1 ELSE 0 END) AS complete,
     collect(DISTINCT CASE WHEN {present} THEN [{values}] END) AS variants
WHERE fanout >= 2 AND complete = fanout AND size(variants) = 1
RETURN key, fanout
ORDER BY key
"""


def sample_workload(
        session: Any,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Sample NPI groups and one fixed record pair from each group."""

    rng = random.Random(PAIR_SAMPLE_SEED)
    groups: list[dict[str, Any]] = []
    group_count = record_count = theoretical_pairs = 0

    # Streaming reservoir sampling keeps only PAIR_COUNT of all valid groups.
    for record in session.run(valid_group_query()):
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
        raise RuntimeError("No complete, conflict-free NPI has two records")
    groups.sort(key=lambda item: str(item["key"]))

    group_index = {group["key"]: index for index, group in enumerate(groups)}
    reservoirs: list[list[dict[str, Any]]] = [[] for _ in groups]
    seen = [0] * len(groups)
    pair_rng = random.Random(PAIR_SAMPLE_SEED)
    records = session.run(
        f"""
MATCH (record:ProviderServiceRecord)
WHERE record.{PROVIDER_DETERMINANT} IN $keys
RETURN record.{PROVIDER_DETERMINANT} AS key,
       record.{SERVICE_DETERMINANT} AS hcpcs_cd,
       record.place_of_srvc AS place_of_srvc,
       elementId(record) AS element_id
ORDER BY key, hcpcs_cd, place_of_srvc, element_id
""",
        keys=list(group_index),
    )
    for record in records:
        index = group_index[record["key"]]
        seen[index] += 1
        member = {
            "element_id": record["element_id"],
            "record_key": {
                PROVIDER_DETERMINANT: record["key"],
                SERVICE_DETERMINANT: record["hcpcs_cd"],
                "place_of_srvc": record["place_of_srvc"],
            },
        }
        if len(reservoirs[index]) < 2:
            reservoirs[index].append(member)
        else:
            position = pair_rng.randrange(seen[index])
            if position < 2:
                reservoirs[index][position] = member

    pairs = []
    for index, members in enumerate(reservoirs):
        if len(members) != 2 or seen[index] != groups[index]["fanout"]:
            raise RuntimeError("A sampled NPI did not yield its expected records")
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
            text = str(properties[field]).strip().lower()
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
        session: Any, pairs: list[dict[str, Any]]
) -> tuple[
    dict[str, list[float]],
    dict[str, list[float]],
    dict[str, list[float]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
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

    provider_fields = {PROVIDER_DETERMINANT, *PROVIDER_DEPENDENTS}
    service_fields = {SERVICE_DETERMINANT, *SERVICE_DEPENDENTS}
    denormalized: dict[str, list[float]] = {}
    service_normalized: dict[str, list[float]] = {}
    service_provider_normalized: dict[str, list[float]] = {}
    denormalized_rows = []
    service_normalized_rows = []
    service_provider_normalized_rows = []
    providers: dict[str, dict[str, Any]] = {}
    services: dict[str, dict[str, Any]] = {}
    records = session.run(
        f"MATCH (record:{TARGET_NODE}) "
        "RETURN elementId(record) AS element_id, properties(record) AS properties"
    )
    for record in records:
        element_id = record["element_id"]
        properties = dict(record["properties"])
        missing = (provider_fields | service_fields) - properties.keys()
        if missing:
            raise RuntimeError(f"Missing normalization fields: {sorted(missing)}")

        fields = set(properties) - {FEATURE}
        record_fields = fields - provider_fields - service_fields
        denormalized[element_id] = hashed_feature(
            (
                ("record", properties, record_fields),
                ("service", properties, service_fields),
                ("provider", properties, provider_fields),
            )
        )
        service_normalized[element_id] = hashed_feature(
            (
                ("record", properties, record_fields),
                ("provider", properties, provider_fields),
            )
        )
        service_provider_normalized[element_id] = hashed_feature(
            (("record", properties, record_fields),)
        )
        denormalized_rows.append(
            {
                "element_id": element_id,
                "feature": denormalized[element_id],
            }
        )
        service_normalized_rows.append(
            {
                "element_id": element_id,
                "feature": service_normalized[element_id],
            }
        )
        service_provider_normalized_rows.append(
            {
                "element_id": element_id,
                "feature": service_provider_normalized[element_id],
            }
        )

        provider_properties = {
            field: properties[field] for field in provider_fields
        }
        provider = providers.setdefault(
            properties[PROVIDER_DETERMINANT],
            {"properties": provider_properties, "element_ids": []},
        )
        if provider["properties"] != provider_properties:
            raise RuntimeError("The sampled data violates the Provider dependency")
        provider["element_ids"].append(element_id)

        service_properties = {
            field: properties[field] for field in service_fields
        }
        service = services.setdefault(
            properties[SERVICE_DETERMINANT],
            {"properties": service_properties, "element_ids": []},
        )
        if service["properties"] != service_properties:
            raise RuntimeError("The sampled data violates the Service dependency")
        service["element_ids"].append(element_id)

    if len(denormalized) != len(element_ids):
        raise RuntimeError("Some sampled records could not be found")
    write_features(session, denormalized_rows)
    return (
        denormalized,
        service_normalized,
        service_provider_normalized,
        service_normalized_rows,
        service_provider_normalized_rows,
        providers,
        services,
    )


def materialize_record_context(
        session: Any, pairs: list[dict[str, Any]]
) -> dict[str, int]:
    """Attach one private anchor to each record for topology-only methods."""

    rows = []
    for pair in pairs:
        for member in pair["members"]:
            properties = {"anchor": 1}
            rows.append(
                {
                    "properties": properties,
                    "feature": hashed_feature(
                        (("context", properties, properties.keys()),)
                    ),
                    "element_id": member["element_id"],
                }
            )
    summary = run(
        session,
        f"""
UNWIND $rows AS row
CREATE (context:{ARTIFACT_NODE}:{GRAPH_NODE}:{CONTEXT_NODE})
SET context = row.properties, context.{FEATURE} = row.feature
WITH context, row
MATCH (record:{TARGET_NODE}) WHERE elementId(record) = row.element_id
CREATE (record)-[:{CONTEXT_RELATIONSHIP}]->(context)
""",
        rows=rows,
    )
    return {
        "context_nodes": summary.counters.nodes_created,
        "context_relationships": summary.counters.relationships_created,
    }


def materialize_service_normalization(
        session: Any, services: dict[str, dict[str, Any]]
) -> dict[str, int]:
    rows = []
    service_fields = (SERVICE_DETERMINANT, *SERVICE_DEPENDENTS)
    for key in sorted(services):
        service = services[key]
        properties = service["properties"]
        rows.append(
            {
                "properties": properties,
                "feature": hashed_feature(
                    (("service", properties, service_fields),)
                ),
                "element_ids": service["element_ids"],
            }
        )
    summary = run(
        session,
        f"""
UNWIND $rows AS row
CREATE (service:{ARTIFACT_NODE}:{GRAPH_NODE}:{SERVICE_NODE})
SET service = row.properties, service.{FEATURE} = row.feature
WITH service, row
UNWIND row.element_ids AS element_id
MATCH (record:{TARGET_NODE}) WHERE elementId(record) = element_id
CREATE (record)-[:{SERVICE_RELATIONSHIP}]->(service)
""",
        rows=rows,
    )
    return {
        "service_nodes": summary.counters.nodes_created,
        "service_relationships": summary.counters.relationships_created,
    }


def materialize_provider_normalization(
        session: Any, providers: dict[str, dict[str, Any]]
) -> dict[str, int]:
    rows = []
    provider_fields = (PROVIDER_DETERMINANT, *PROVIDER_DEPENDENTS)
    for key in sorted(providers):
        provider = providers[key]
        properties = provider["properties"]
        rows.append(
            {
                "properties": properties,
                "feature": hashed_feature(
                    (("provider", properties, provider_fields),)
                ),
                "element_ids": provider["element_ids"],
            }
        )
    summary = run(
        session,
        f"""
UNWIND $rows AS row
CREATE (provider:{ARTIFACT_NODE}:{GRAPH_NODE}:{PROVIDER_NODE})
SET provider = row.properties, provider.{FEATURE} = row.feature
WITH provider, row
UNWIND row.element_ids AS element_id
MATCH (record:{TARGET_NODE}) WHERE elementId(record) = element_id
CREATE (record)-[:{PROVIDER_RELATIONSHIP}]->(provider)
""",
        rows=rows,
    )
    return {
        "provider_nodes": summary.counters.nodes_created,
        "provider_relationships": summary.counters.relationships_created,
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
                "concurrency": 1,
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
        stages: dict[str, dict[str, list[float]]],
        pairs: list[dict[str, Any]],
) -> list[dict[str, float]]:
    scores = []
    for pair in pairs:
        first, second = [
            member["element_id"] for member in pair["members"]
        ]
        stage_scores = {
            stage: cosine(vectors[first], vectors[second])
            for stage, vectors in stages.items()
        }
        scores.append(
            {
                **stage_scores,
                "delta_service_minus_denormalized": (
                        stage_scores["service_normalized"]
                        - stage_scores["denormalized"]
                ),
                "delta_provider_minus_service": (
                        stage_scores["service_provider_normalized"]
                        - stage_scores["service_normalized"]
                ),
            }
        )
    return scores


def run_embedding_method(
        session: Any,
        procedure: str,
        element_ids: list[str],
        configuration: dict[str, Any],
) -> dict[str, dict[str, list[float]]]:
    return {
        "denormalized": embeddings(
            session,
            procedure,
            DENORMALIZED_GRAPH,
            element_ids,
            configuration,
        ),
        "service_normalized": embeddings(
            session,
            procedure,
            SERVICE_NORMALIZED_GRAPH,
            element_ids,
            configuration,
        ),
        "service_provider_normalized": embeddings(
            session,
            procedure,
            SERVICE_PROVIDER_NORMALIZED_GRAPH,
            element_ids,
            configuration,
        ),
    }


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
) -> dict[str, Any]:
    (
        denormalized_hash,
        service_hash,
        service_provider_hash,
        service_rows,
        service_provider_rows,
        providers,
        services,
    ) = prepare_features(session, pairs)

    common_context = materialize_record_context(session, pairs)
    denormalized_projection = project(
        session, DENORMALIZED_GRAPH, [CONTEXT_RELATIONSHIP]
    )

    write_features(session, service_rows)
    service_normalization = materialize_service_normalization(
        session, services
    )
    service_projection = project(
        session,
        SERVICE_NORMALIZED_GRAPH,
        [CONTEXT_RELATIONSHIP, SERVICE_RELATIONSHIP],
    )

    write_features(session, service_provider_rows)
    provider_normalization = materialize_provider_normalization(
        session, providers
    )
    provider_projection = project(
        session,
        SERVICE_PROVIDER_NORMALIZED_GRAPH,
        [
            CONTEXT_RELATIONSHIP,
            SERVICE_RELATIONSHIP,
            PROVIDER_RELATIONSHIP,
        ],
    )

    target_count = len(pairs) * 2
    if common_context != {
        "context_nodes": target_count,
        "context_relationships": target_count,
    }:
        raise RuntimeError("Unexpected common record-anchor structure")
    if service_normalization != {
        "service_nodes": len(services),
        "service_relationships": target_count,
    }:
        raise RuntimeError("Unexpected Service normalization structure")
    if provider_normalization != {
        "provider_nodes": len(providers),
        "provider_relationships": target_count,
    } or len(providers) != len(pairs):
        raise RuntimeError("Unexpected Provider normalization structure")

    relationship_multiplier = (
        2 if RELATIONSHIP_ORIENTATION == "UNDIRECTED" else 1
    )
    expected_denormalized = {
        "nodes": target_count + common_context["context_nodes"],
        "relationships": (
                common_context["context_relationships"]
                * relationship_multiplier
        ),
    }
    expected_service = {
        "nodes": (
                expected_denormalized["nodes"]
                + service_normalization["service_nodes"]
        ),
        "relationships": (
                                 common_context["context_relationships"]
                                 + service_normalization["service_relationships"]
                         ) * relationship_multiplier,
    }
    expected_provider = {
        "nodes": (
                expected_service["nodes"]
                + provider_normalization["provider_nodes"]
        ),
        "relationships": (
                                 common_context["context_relationships"]
                                 + service_normalization["service_relationships"]
                                 + provider_normalization["provider_relationships"]
                         ) * relationship_multiplier,
    }
    for name, actual, expected in (
            ("denormalized", denormalized_projection, expected_denormalized),
            ("service-normalized", service_projection, expected_service),
            ("service+provider-normalized", provider_projection, expected_provider),
    ):
        if actual != expected:
            raise RuntimeError(f"Unexpected {name} graph: {actual}")

    element_ids = sorted(
        member["element_id"] for pair in pairs for member in pair["members"]
    )
    hash_stages = {
        "denormalized": denormalized_hash,
        "service_normalized": service_hash,
        "service_provider_normalized": service_provider_hash,
    }
    method_scores = {
        "property_hash": score_pairs(hash_stages, pairs)
    }
    configurations = (
        (
            "fast_rp",
            "gds.fastRP.stream",
            {
                "embeddingDimension": DIMENSION,
                "iterationWeights": [0.0, 1.0, 1.0],
                "nodeSelfInfluence": 1.0,
                "featureProperties": [FEATURE],
                "propertyRatio": 0.5,
                "randomSeed": seed,
                "concurrency": 1,
            },
        ),
        (
            "node2vec",
            "gds.node2vec.stream",
            {
                "embeddingDimension": DIMENSION,
                "walkLength": 20,
                "walksPerNode": 5,
                "windowSize": 5,
                "iterations": 2,
                "randomSeed": seed,
                "concurrency": 1,
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
                "concurrency": 1,
            },
        ),
    )
    for name, procedure, configuration in configurations:
        stages = run_embedding_method(
            session, procedure, element_ids, configuration
        )
        method_scores[name] = score_pairs(stages, pairs)

    sage_stages = {
        "denormalized": graph_sage(
            session,
            DENORMALIZED_GRAPH,
            DENORMALIZED_MODEL,
            element_ids,
            seed,
        ),
        "service_normalized": graph_sage(
            session,
            SERVICE_NORMALIZED_GRAPH,
            SERVICE_NORMALIZED_MODEL,
            element_ids,
            seed,
        ),
        "service_provider_normalized": graph_sage(
            session,
            SERVICE_PROVIDER_NORMALIZED_GRAPH,
            SERVICE_PROVIDER_NORMALIZED_MODEL,
            element_ids,
            seed,
        ),
    }
    method_scores["graph_sage"] = score_pairs(sage_stages, pairs)

    return {
        "random_seed": seed,
        **population,
        "pair_count": len(pairs),
        "selection_strategy": (
            "one fixed pair of distinct records per sampled NPI; the same "
            "pairs are reused across all three schemas and embedding seeds"
        ),
        "projected_graphs": {
            "denormalized": denormalized_projection,
            "service_normalized": service_projection,
            "service_provider_normalized": provider_projection,
        },
        "materialized_common_record_anchors": common_context,
        "materialized_service_normalization": service_normalization,
        "materialized_provider_normalization": provider_normalization,
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
                print(
                    "[sampling] Selecting pairs of records with the same NPI",
                    flush=True,
                )
                pairs, population = sample_workload(session)
                report: dict[str, Any] = {
                    "experiment": (
                        "provider_service_sequential_normalization_embeddings"
                    ),
                    "created_at": datetime.now().astimezone().isoformat(),
                    "database": DATABASE,
                    "neo4j_driver": driver_version,
                    "gds_version": gds_version,
                    "relationship_orientation": RELATIONSHIP_ORIENTATION,
                    "settings": {
                        "pair_count": PAIR_COUNT,
                        "pair_sample_seed": PAIR_SAMPLE_SEED,
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
                        "all three projections use the same fixed same-NPI "
                        "record pairs and one private anchor per record; N1 "
                        "extracts Service and N2 additionally extracts Provider"
                    ),
                    "normalization_sequence": [
                        {
                            "step": 1,
                            "functional_dependency": (
                                f"{SERVICE_DETERMINANT} -> "
                                f"({', '.join(SERVICE_DEPENDENTS)})"
                            ),
                            "comparison": (
                                "denormalized -> service_normalized"
                            ),
                        },
                        {
                            "step": 2,
                            "functional_dependency": (
                                f"{PROVIDER_DETERMINANT} -> "
                                f"({', '.join(PROVIDER_DEPENDENTS)})"
                            ),
                            "comparison": (
                                "service_normalized -> "
                                "service_provider_normalized"
                            ),
                        },
                    ],
                    "experiments": {"sequential": []},
                }
                write_report(report)

                for repeat, seed in enumerate(RANDOM_SEEDS, start=1):
                    print(
                        f"[seed {repeat}/{len(RANDOM_SEEDS)}] seed={seed}",
                        flush=True,
                    )
                    try:
                        result = run_case(session, pairs, population, seed)
                    finally:
                        cleanup(session)
                    result["repeat"] = repeat
                    report["experiments"]["sequential"].append(result)
                    write_report(report)
            finally:
                try:
                    cleanup(session)
                finally:
                    drop_source_index(session)

    print(f"\nExperiment complete: {OUTPUT}")


if __name__ == "__main__":
    main()
