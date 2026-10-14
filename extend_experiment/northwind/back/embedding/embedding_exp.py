#!/usr/bin/env python3
"""Compare same-customer Order embeddings in normalized and folded schemas.

The script is intentionally self-contained.  It reads the imported Northwind
graph, samples several Customer-specific Order pairs, creates temporary
Customer+Order nodes, runs five small embedding experiments, writes
``embedding_results.json`` beside this file, and removes the temporary graph
unless ``--keep-artifacts`` is supplied.

Embedding methods:

* property_hash: deterministic feature hashing over node properties;
* FastRP: node-property and topology embedding from Neo4j Graph Data Science;
* Node2Vec: random-walk topology embedding from Neo4j Graph Data Science.
* HashGNN: hashed node features propagated over typed relationships.
* GraphSAGE: trained aggregation of node features and directed neighbors.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean, median
from typing import Any, Iterable, Sequence

from neo4j import GraphDatabase


DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = ""
DEFAULT_DATABASE = "northwind"
DEFAULT_SEED = 20260827
DEFAULT_DIMENSION = 64
DEFAULT_PAIR_COUNT = 4000
OUTPUT_FILE = Path(__file__).with_name("embedding_results.json")

CO_LABEL = "EmbeddingCustomerOrder"
NORMALIZED_GRAPH = "northwind_embedding_normalized"
DENORMALIZED_GRAPH = "northwind_embedding_denormalized"
NORMALIZED_SAGE_MODEL = "northwind_embedding_normalized_sage"
DENORMALIZED_SAGE_MODEL = "northwind_embedding_denormalized_sage"
FEATURE_PROPERTY = "exp_embedding_features"
GRAPH_LABELS = ("Order", "Customer", "OrderDetail", "Employee", "Shipper")

ORDER_FIELDS = (
    "OrderID",
    "CustomerID",
    "EmployeeID",
    "OrderDate",
    "RequiredDate",
    "ShippedDate",
    "ShipVia",
    "Freight",
    "ShipName",
    "ShipAddress",
    "ShipCity",
    "ShipRegion",
    "ShipPostalCode",
    "ShipCountry",
)

CUSTOMER_FIELDS = (
    "CustomerID",
    "CompanyName",
    "ContactName",
    "ContactTitle",
    "Address",
    "City",
    "Region",
    "PostalCode",
    "Country",
    "Phone",
    "Fax",
)

# OrderID and CustomerID identify the answer and are not embedding features.
ORDER_FEATURE_FIELDS = tuple(
    field for field in ORDER_FIELDS if field not in {"OrderID", "CustomerID"}
)
CUSTOMER_FEATURE_FIELDS = tuple(
    field for field in CUSTOMER_FIELDS if field != "CustomerID"
)
WORD_RE = re.compile(r"[\w]+", re.UNICODE)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare same-customer Order pairs in normalized and "
            "Customer+Order schemas."
        )
    )
    parser.add_argument("--uri", default=DEFAULT_URI)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--dimension", type=int, default=DEFAULT_DIMENSION)
    parser.add_argument(
        "--pair-count",
        type=int,
        default=DEFAULT_PAIR_COUNT,
        help="number of distinct Customers to sample (one Order pair each)",
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help=f"keep :{CO_LABEL} nodes after the experiment",
    )
    args = parser.parse_args(argv)
    if args.dimension < 8:
        parser.error("--dimension must be at least 8")
    if args.pair_count < 1:
        parser.error("--pair-count must be at least 1")
    return args


def quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def consume(session: Any, query: str, **parameters: Any) -> Any:
    return session.run(query, **parameters).consume()


def check_gds(session: Any) -> str:
    try:
        record = session.run("RETURN gds.version() AS version").single(strict=True)
    except Exception as exc:
        raise RuntimeError(
            "Neo4j GDS is unavailable. Install/enable the bundled Graph Data "
            "Science plugin and restart Neo4j."
        ) from exc
    return str(record["version"])


def choose_order_pairs(
    session: Any,
    seed: int,
    pair_count: int,
) -> list[dict[str, Any]]:
    records = list(
        session.run(
            """
            MATCH (customer:Customer)<-[:PLACED_BY]-(order:`Order`)
            WITH customer.CustomerID AS customer_id,
                 collect(order.OrderID) AS order_ids
            WHERE size(order_ids) >= 2
            RETURN customer_id, order_ids
            ORDER BY customer_id
            """
        )
    )
    if not records:
        raise RuntimeError("No Customer with at least two Orders was found")
    rng = random.Random(seed)
    selected = rng.sample(records, min(pair_count, len(records)))
    pairs = [
        {
            "customer_id": record["customer_id"],
            "order_ids": sorted(rng.sample(list(record["order_ids"]), 2)),
        }
        for record in selected
    ]
    return sorted(pairs, key=lambda pair: str(pair["customer_id"]))


def folded_property_map() -> str:
    entries = [
        f"{quote('exp_OrderID')}: order.{quote('OrderID')}",
        f"{quote('exp_CustomerID')}: customer.{quote('CustomerID')}",
    ]
    entries.extend(
        f"{quote('ord_' + field)}: order.{quote(field)}" for field in ORDER_FIELDS
    )
    entries.extend(
        f"{quote('cust_' + field)}: customer.{quote(field)}"
        for field in CUSTOMER_FEATURE_FIELDS
    )
    return "{" + ", ".join(entries) + "}"


def cleanup_artifacts(session: Any) -> None:
    consume(session, f"MATCH (node:{quote(CO_LABEL)}) DETACH DELETE node")


def cleanup_feature_properties(session: Any) -> None:
    consume(
        session,
        f"""
        MATCH (node)
        WHERE any(label IN labels(node) WHERE label IN $labels)
        REMOVE node.{quote(FEATURE_PROPERTY)}
        """,
        labels=[*GRAPH_LABELS, CO_LABEL],
    )


def materialize_denormalized(session: Any) -> dict[str, int]:
    cleanup_artifacts(session)
    node_record = session.run(
        f"""
        MATCH (order:`Order`)-[:PLACED_BY]->(customer:Customer)
        CREATE (folded:{quote(CO_LABEL)})
        SET folded = {folded_property_map()}
        RETURN count(folded) AS count
        """
    ).single(strict=True)

    detail_record = session.run(
        f"""
        MATCH (detail:OrderDetail)-[:OF_ORDER]->(order:`Order`)
        MATCH (folded:{quote(CO_LABEL)}
               {{{quote('exp_OrderID')}: order.OrderID}})
        CREATE (detail)-[:EMB_OF_ORDER]->(folded)
        RETURN count(*) AS count
        """
    ).single(strict=True)

    employee_record = session.run(
        f"""
        MATCH (order:`Order`)-[:HANDLED_BY]->(employee:Employee)
        MATCH (folded:{quote(CO_LABEL)}
               {{{quote('exp_OrderID')}: order.OrderID}})
        CREATE (folded)-[:EMB_HANDLED_BY]->(employee)
        RETURN count(*) AS count
        """
    ).single(strict=True)

    shipper_record = session.run(
        f"""
        MATCH (order:`Order`)-[:SHIPPED_BY]->(shipper:Shipper)
        MATCH (folded:{quote(CO_LABEL)}
               {{{quote('exp_OrderID')}: order.OrderID}})
        CREATE (folded)-[:EMB_SHIPPED_BY]->(shipper)
        RETURN count(*) AS count
        """
    ).single(strict=True)

    return {
        "nodes": int(node_record["count"]),
        "detail_edges": int(detail_record["count"]),
        "employee_edges": int(employee_record["count"]),
        "shipper_edges": int(shipper_record["count"]),
    }


def drop_gds_graph(session: Any, graph_name: str) -> None:
    record = session.run(
        "CALL gds.graph.exists($name) YIELD exists RETURN exists",
        name=graph_name,
    ).single(strict=True)
    if record["exists"]:
        consume(
            session,
            "CALL gds.graph.drop($name) YIELD graphName RETURN graphName",
            name=graph_name,
        )


def drop_gds_model(session: Any, model_name: str) -> None:
    consume(session, "CALL gds.model.drop($name, false)", name=model_name)


def project_graphs(session: Any) -> dict[str, dict[str, int]]:
    for graph_name in (NORMALIZED_GRAPH, DENORMALIZED_GRAPH):
        drop_gds_graph(session, graph_name)

    normalized_relationships = {
        "PLACED_BY": {"orientation": "NATURAL"},
        "OF_ORDER": {"orientation": "NATURAL"},
        "HANDLED_BY": {"orientation": "NATURAL"},
        "SHIPPED_BY": {"orientation": "NATURAL"},
    }
    denormalized_relationships = {
        "EMB_OF_ORDER": {"orientation": "NATURAL"},
        "EMB_HANDLED_BY": {"orientation": "NATURAL"},
        "EMB_SHIPPED_BY": {"orientation": "NATURAL"},
    }

    normalized = session.run(
        """
        CALL gds.graph.project($name, $labels, $relationships)
        YIELD nodeCount, relationshipCount
        RETURN nodeCount, relationshipCount
        """,
        name=NORMALIZED_GRAPH,
        labels={
            label: {"properties": [FEATURE_PROPERTY]} for label in GRAPH_LABELS
        },
        relationships=normalized_relationships,
    ).single(strict=True)

    denormalized = session.run(
        """
        CALL gds.graph.project($name, $labels, $relationships)
        YIELD nodeCount, relationshipCount
        RETURN nodeCount, relationshipCount
        """,
        name=DENORMALIZED_GRAPH,
        labels={
            label: {"properties": [FEATURE_PROPERTY]}
            for label in (CO_LABEL, "OrderDetail", "Employee", "Shipper")
        },
        relationships=denormalized_relationships,
    ).single(strict=True)

    return {
        "normalized": {
            "nodes": int(normalized["nodeCount"]),
            "relationships": int(normalized["relationshipCount"]),
        },
        "denormalized": {
            "nodes": int(denormalized["nodeCount"]),
            "relationships": int(denormalized["relationshipCount"]),
        },
    }


def feature_tokens(namespace: str, fields: Iterable[str], values: dict[str, Any]):
    for field in fields:
        value = values.get(field, "<MISSING>")
        text = "<MISSING>" if value is None else str(value).strip().lower()
        text = text or "<MISSING>"
        yield f"{namespace}.{field}={text}"
        for word in WORD_RE.findall(text):
            yield f"{namespace}.{field}:word={word}"


def feature_hash(tokens: Iterable[str], dimension: int) -> list[float]:
    vector = [0.0] * dimension
    for token in tokens:
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=16).digest()
        index = int.from_bytes(digest[:8], "big") % dimension
        sign = 1.0 if digest[8] & 1 else -1.0
        vector[index] += sign
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [value / norm for value in vector]


def hashgnn_tokens(labels: list[str], props: dict[str, Any]) -> list[str]:
    if CO_LABEL in labels:
        order_values = {
            field: props.get("ord_" + field) for field in ORDER_FEATURE_FIELDS
        }
        customer_values = {
            field: props.get("cust_" + field) for field in CUSTOMER_FEATURE_FIELDS
        }
        tokens = list(feature_tokens("order", ORDER_FEATURE_FIELDS, order_values))
        tokens.extend(
            feature_tokens("customer", CUSTOMER_FEATURE_FIELDS, customer_values)
        )
        return tokens
    if "Order" in labels:
        return list(feature_tokens("order", ORDER_FEATURE_FIELDS, props))
    if "Customer" in labels:
        return list(feature_tokens("customer", CUSTOMER_FEATURE_FIELDS, props))

    label = next(name for name in GRAPH_LABELS if name in labels)
    fields = sorted(
        field
        for field in props
        if field != FEATURE_PROPERTY and not field.lower().endswith("id")
    )
    return list(feature_tokens(label.lower(), fields, props))


def prepare_hashgnn_features(session: Any, dimension: int) -> None:
    rows = []
    for record in session.run(
        """
        MATCH (node)
        WHERE any(label IN labels(node) WHERE label IN $labels)
        RETURN elementId(node) AS element_id,
               labels(node) AS node_labels,
               properties(node) AS node_properties
        """,
        labels=[*GRAPH_LABELS, CO_LABEL],
    ):
        props = dict(record["node_properties"])
        rows.append(
            {
                "element_id": record["element_id"],
                "features": feature_hash(
                    hashgnn_tokens(list(record["node_labels"]), props), dimension
                ),
            }
        )

    consume(
        session,
        f"""
        UNWIND $rows AS row
        MATCH (node) WHERE elementId(node) = row.element_id
        SET node.{quote(FEATURE_PROPERTY)} = row.features
        """,
        rows=rows,
    )


def property_embeddings(
    session: Any,
    order_ids: list[int],
    dimension: int,
) -> tuple[dict[int, list[float]], dict[int, list[float]]]:
    normalized: dict[int, list[float]] = {}
    for record in session.run(
        """
        MATCH (order:`Order`)-[:PLACED_BY]->(customer:Customer)
        WHERE order.OrderID IN $order_ids
        RETURN order.OrderID AS order_id,
               properties(order) AS order_properties
        """,
        order_ids=order_ids,
    ):
        props = dict(record["order_properties"])
        tokens = feature_tokens("order", ORDER_FEATURE_FIELDS, props)
        normalized[int(record["order_id"])] = feature_hash(tokens, dimension)

    denormalized: dict[int, list[float]] = {}
    for record in session.run(
        f"""
        MATCH (folded:{quote(CO_LABEL)})
        WHERE folded.{quote('exp_OrderID')} IN $order_ids
        RETURN folded.{quote('exp_OrderID')} AS order_id,
               properties(folded) AS folded_properties
        """,
        order_ids=order_ids,
    ):
        props = dict(record["folded_properties"])
        order_values = {
            field: props.get("ord_" + field) for field in ORDER_FEATURE_FIELDS
        }
        customer_values = {
            field: props.get("cust_" + field) for field in CUSTOMER_FEATURE_FIELDS
        }
        tokens = list(feature_tokens("order", ORDER_FEATURE_FIELDS, order_values))
        tokens.extend(
            feature_tokens("customer", CUSTOMER_FEATURE_FIELDS, customer_values)
        )
        denormalized[int(record["order_id"])] = feature_hash(tokens, dimension)

    return normalized, denormalized


def gds_embeddings(
    session: Any,
    procedure: str,
    graph_name: str,
    target_label: str,
    key_property: str,
    order_ids: list[int],
    configuration: dict[str, Any],
) -> dict[int, list[float]]:
    if procedure not in {
        "gds.fastRP.stream",
        "gds.node2vec.stream",
        "gds.hashgnn.stream",
        "gds.beta.graphSage.stream",
    }:
        raise ValueError(f"Unsupported GDS procedure: {procedure}")
    query = f"""
        CALL {procedure}($graph_name, $configuration)
        YIELD nodeId, embedding
        WITH gds.util.asNode(nodeId) AS node, embedding
        WHERE node:{quote(target_label)}
          AND node.{quote(key_property)} IN $order_ids
        RETURN node.{quote(key_property)} AS order_id, embedding
    """
    return {
        int(record["order_id"]): [float(value) for value in record["embedding"]]
        for record in session.run(
            query,
            graph_name=graph_name,
            configuration=configuration,
            order_ids=order_ids,
        )
    }


def graph_sage_embeddings(
    session: Any,
    graph_name: str,
    model_name: str,
    target_label: str,
    key_property: str,
    order_ids: list[int],
    dimension: int,
    seed: int,
) -> dict[int, list[float]]:
    drop_gds_model(session, model_name)
    configuration = {
        "modelName": model_name,
        "featureProperties": [FEATURE_PROPERTY],
        "embeddingDimension": dimension,
        "aggregator": "mean",
        "activationFunction": "ReLu",
        "sampleSizes": [5],
        "epochs": 2,
        "maxIterations": 10,
        "randomSeed": seed,
        "concurrency": 1,
    }
    try:
        consume(
            session,
            "CALL gds.beta.graphSage.train($graph_name, $configuration)",
            graph_name=graph_name,
            configuration=configuration,
        )
        return gds_embeddings(
            session,
            "gds.beta.graphSage.stream",
            graph_name,
            target_label,
            key_property,
            order_ids,
            {"modelName": model_name, "concurrency": 1},
        )
    finally:
        drop_gds_model(session, model_name)


def cosine(left: list[float], right: list[float]) -> float:
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return sum(a * b for a, b in zip(left, right, strict=True)) / (
        left_norm * right_norm
    )


def similarity_result(
    normalized: dict[int, list[float]],
    denormalized: dict[int, list[float]],
    order_ids: list[int],
) -> dict[str, float]:
    first, second = order_ids
    try:
        normalized_score = cosine(normalized[first], normalized[second])
        denormalized_score = cosine(denormalized[first], denormalized[second])
    except KeyError as exc:
        raise RuntimeError(f"Missing embedding for OrderID {exc.args[0]}") from exc
    return {
        "normalized": normalized_score,
        "denormalized": denormalized_score,
        "delta_denormalized_minus_normalized": (
            denormalized_score - normalized_score
        ),
    }


def score_pairs(
    normalized: dict[int, list[float]],
    denormalized: dict[int, list[float]],
    pairs: list[dict[str, Any]],
) -> list[dict[str, float]]:
    return [
        similarity_result(normalized, denormalized, pair["order_ids"])
        for pair in pairs
    ]


def summarize_scores(
    scores: list[dict[str, float]],
) -> dict[str, dict[str, float]]:
    fields = (
        "normalized",
        "denormalized",
        "delta_denormalized_minus_normalized",
    )
    return {
        field: {
            "mean": fmean(score[field] for score in scores),
            "median": float(median(score[field] for score in scores)),
        }
        for field in fields
    }


def run_experiment(session: Any, args: argparse.Namespace) -> dict[str, Any]:
    gds_version = check_gds(session)
    pairs = choose_order_pairs(session, args.seed, args.pair_count)
    materialized = materialize_denormalized(session)
    prepare_hashgnn_features(session, args.dimension)
    graph_counts = project_graphs(session)
    order_ids = sorted(
        {order_id for pair in pairs for order_id in pair["order_ids"]}
    )

    normalized_properties, denormalized_properties = property_embeddings(
        session,
        order_ids,
        args.dimension,
    )

    fast_rp_config = {
        "embeddingDimension": args.dimension,
        "iterationWeights": [1.0, 1.0, 1.0],
        "normalizationStrength": -0.5,
        "featureProperties": [FEATURE_PROPERTY],
        "propertyRatio": 0.5,
        "nodeSelfInfluence": 1.0,
        "randomSeed": args.seed,
        "concurrency": 1,
    }
    node2vec_config = {
        "embeddingDimension": args.dimension,
        "walkLength": 20,
        "walksPerNode": 5,
        "windowSize": 5,
        "iterations": 2,
        "randomSeed": args.seed,
        "concurrency": 1,
    }
    hash_gnn_config = {
        "featureProperties": [FEATURE_PROPERTY],
        "iterations": 2,
        "embeddingDensity": args.dimension,
        "heterogeneous": True,
        "neighborInfluence": 1.0,
        "binarizeFeatures": {"dimension": args.dimension * 4, "threshold": 0.0},
        "outputDimension": args.dimension,
        "randomSeed": args.seed,
        "concurrency": 1,
    }

    method_scores = {
        "property_hash": score_pairs(
            normalized_properties,
            denormalized_properties,
            pairs,
        )
    }
    for method, procedure, configuration in (
        ("fast_rp", "gds.fastRP.stream", fast_rp_config),
        ("node2vec", "gds.node2vec.stream", node2vec_config),
        ("hash_gnn", "gds.hashgnn.stream", hash_gnn_config),
    ):
        normalized = gds_embeddings(
            session,
            procedure,
            NORMALIZED_GRAPH,
            "Order",
            "OrderID",
            order_ids,
            configuration,
        )
        denormalized = gds_embeddings(
            session,
            procedure,
            DENORMALIZED_GRAPH,
            CO_LABEL,
            "exp_OrderID",
            order_ids,
            configuration,
        )
        method_scores[method] = score_pairs(
            normalized,
            denormalized,
            pairs,
        )

    normalized_sage = graph_sage_embeddings(
        session,
        NORMALIZED_GRAPH,
        NORMALIZED_SAGE_MODEL,
        "Order",
        "OrderID",
        order_ids,
        args.dimension,
        args.seed,
    )
    denormalized_sage = graph_sage_embeddings(
        session,
        DENORMALIZED_GRAPH,
        DENORMALIZED_SAGE_MODEL,
        CO_LABEL,
        "exp_OrderID",
        order_ids,
        args.dimension,
        args.seed,
    )
    method_scores["graph_sage"] = score_pairs(
        normalized_sage,
        denormalized_sage,
        pairs,
    )

    pair_results = [
        {
            **pair,
            "similarities": {
                method: scores[index] for method, scores in method_scores.items()
            },
        }
        for index, pair in enumerate(pairs)
    ]

    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "database": args.database,
        "gds_version": gds_version,
        "seed": args.seed,
        "embedding_dimension": args.dimension,
        "requested_pair_count": args.pair_count,
        "pair_count": len(pairs),
        "selection_strategy": "distinct Customers; one random Order pair each",
        "pair_results": pair_results,
        "materialized_denormalized": materialized,
        "projected_graphs": graph_counts,
        "similarities": {
            method: summarize_scores(scores)
            for method, scores in method_scores.items()
        },
    }


def print_summary(result: dict[str, Any]) -> None:
    print(f"Pairs={result['pair_count']} (one pair per distinct Customer)")
    print(f"GDS={result['gds_version']}  dimension={result['embedding_dimension']}")
    print()
    print(
        f"{'method':<16} {'norm med':>10} {'denorm med':>11} "
        f"{'delta med':>10} {'norm mean':>10} {'denorm mean':>11} "
        f"{'delta mean':>10}"
    )
    for method, scores in result["similarities"].items():
        print(
            f"{method:<16} "
            f"{scores['normalized']['median']:>10.6f} "
            f"{scores['denormalized']['median']:>11.6f} "
            f"{scores['delta_denormalized_minus_normalized']['median']:>10.6f} "
            f"{scores['normalized']['mean']:>10.6f} "
            f"{scores['denormalized']['mean']:>11.6f} "
            f"{scores['delta_denormalized_minus_normalized']['mean']:>10.6f}"
        )
    print(f"\nSaved: {OUTPUT_FILE}")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    driver = GraphDatabase.driver(
        args.uri,
        auth=(args.user, args.password),
    )
    result: dict[str, Any] | None = None
    try:
        driver.verify_connectivity()
        with driver.session(database=args.database) as session:
            try:
                result = run_experiment(session, args)
                OUTPUT_FILE.write_text(
                    json.dumps(result, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            finally:
                for graph_name in (NORMALIZED_GRAPH, DENORMALIZED_GRAPH):
                    try:
                        drop_gds_graph(session, graph_name)
                    except Exception:
                        pass
                cleanup_feature_properties(session)
                if not args.keep_artifacts:
                    cleanup_artifacts(session)
    finally:
        driver.close()

    if result is None:
        raise RuntimeError("Experiment did not produce a result")
    print_summary(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
