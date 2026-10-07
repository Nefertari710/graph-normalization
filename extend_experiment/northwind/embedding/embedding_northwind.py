#!/usr/bin/env python3
"""Compare nine denormalizations on the full declared Northwind graph."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from statistics import fmean, median, stdev
from typing import Any, Iterable, Sequence

from neo4j import GraphDatabase


DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = ""
DEFAULT_DATABASE = "northwind"
DEFAULT_SEED = 20260827
DEFAULT_RUNS = 10
DEFAULT_DIMENSION = 128
DEFAULT_PAIR_COUNT = 100000
DEFAULT_HASH_GNN_SEEDS = 1
DEFAULT_MAX_HOPS = 10
RELATIONSHIP_ORIENTATION = "UNDIRECTED"  # Change to "UNDIRECTED" when needed.

if RELATIONSHIP_ORIENTATION not in {"NATURAL", "UNDIRECTED"}:
    raise ValueError(
        "RELATIONSHIP_ORIENTATION must be 'NATURAL' or 'UNDIRECTED'"
    )

OUTPUT_NAME_BY_ORIENTATION = {
    "NATURAL": "embedding_results_natrual.json",
    "UNDIRECTED": "embedding_results_undirection.json",
}
OUTPUT_FILE = Path(__file__).with_name(
    OUTPUT_NAME_BY_ORIENTATION[RELATIONSHIP_ORIENTATION]
)

NORMALIZED_GRAPH = "northwind_embedding_normalized"
DENORMALIZED_GRAPH = "northwind_embedding_denormalized"
NORMALIZED_SAGE_MODEL = "northwind_embedding_normalized_sage"
DENORMALIZED_SAGE_MODEL = "northwind_embedding_denormalized_sage"
FEATURE_PROPERTY = "exp_embedding_features"
WORD_RE = re.compile(r"[\w]+", re.UNICODE)


@dataclass(frozen=True)
class Table:
    fields: tuple[str, ...]
    keys: tuple[str, ...]


@dataclass(frozen=True)
class Relationship:
    source: str
    rel_type: str
    target: str


@dataclass(frozen=True)
class ExperimentCase:
    name: str
    child: str
    parent: str
    rel_type: str
    child_parent_fields: tuple[str, ...]
    folded_label: str


def table(fields: str, keys: str) -> Table:
    return Table(tuple(fields.split()), tuple(keys.split()))


TABLES = {
    "Category": table("CategoryID CategoryName Description Picture", "CategoryID"),
    "Customer": table(
        "CustomerID CompanyName ContactName ContactTitle Address City Region "
        "PostalCode Country Phone Fax",
        "CustomerID",
    ),
    "Employee": table(
        "EmployeeID LastName FirstName Title TitleOfCourtesy BirthDate HireDate "
        "Address City Region PostalCode Country HomePhone Extension Photo Notes "
        "ReportsTo PhotoPath",
        "EmployeeID",
    ),
    "EmployeeTerritory": table(
        "EmployeeID TerritoryID", "EmployeeID TerritoryID"
    ),
    "OrderDetail": table(
        "OrderID ProductID UnitPrice Quantity Discount", "OrderID ProductID"
    ),
    "Order": table(
        "OrderID CustomerID EmployeeID OrderDate RequiredDate ShippedDate "
        "ShipVia Freight ShipName ShipAddress ShipCity ShipRegion "
        "ShipPostalCode ShipCountry",
        "OrderID",
    ),
    "Product": table(
        "ProductID ProductName SupplierID CategoryID QuantityPerUnit UnitPrice "
        "UnitsInStock UnitsOnOrder ReorderLevel Discontinued",
        "ProductID",
    ),
    "Region": table("RegionID RegionDescription", "RegionID"),
    "Shipper": table("ShipperID CompanyName Phone", "ShipperID"),
    "Supplier": table(
        "SupplierID CompanyName ContactName ContactTitle Address City Region "
        "PostalCode Country Phone Fax HomePage",
        "SupplierID",
    ),
    "Territory": table(
        "TerritoryID TerritoryDescription RegionID", "TerritoryID"
    ),
}

RELATIONSHIPS = (
    Relationship("Order", "PLACED_BY", "Customer"),
    Relationship("OrderDetail", "OF_ORDER", "Order"),
    Relationship("OrderDetail", "OF_PRODUCT", "Product"),
    Relationship("Product", "IN_CATEGORY", "Category"),
    Relationship("Product", "SUPPLIED_BY", "Supplier"),
    Relationship("Order", "HANDLED_BY", "Employee"),
    Relationship("Order", "SHIPPED_BY", "Shipper"),
    Relationship("EmployeeTerritory", "HAS_TERRITORY", "Employee"),
    Relationship("EmployeeTerritory", "IN_TERRITORY", "Territory"),
    Relationship("Territory", "IN_REGION", "Region"),
)

CASES = (
    ExperimentCase("order_customer", "Order", "Customer", "PLACED_BY", ("CustomerID",), "EmbeddingCustomerOrder"),
    ExperimentCase("order_detail_product", "OrderDetail", "Product", "OF_PRODUCT", ("ProductID",), "EmbeddingOrderDetailProduct"),
    ExperimentCase("order_detail_order", "OrderDetail", "Order", "OF_ORDER", ("OrderID",), "EmbeddingOrderDetailOrder"),
    ExperimentCase("product_supplier", "Product", "Supplier", "SUPPLIED_BY", ("SupplierID",), "EmbeddingProductSupplier"),
    ExperimentCase("order_employee", "Order", "Employee", "HANDLED_BY", ("EmployeeID",), "EmbeddingOrderEmployee"),
    ExperimentCase("order_shipper", "Order", "Shipper", "SHIPPED_BY", ("ShipVia",), "EmbeddingOrderShipper"),
    ExperimentCase("product_category", "Product", "Category", "IN_CATEGORY", ("CategoryID",), "EmbeddingProductCategory"),
    ExperimentCase("employee_territory_employee", "EmployeeTerritory", "Employee", "HAS_TERRITORY", ("EmployeeID",), "EmbeddingEmployeeTerritoryEmployee"),
    ExperimentCase("territory_region", "Territory", "Region", "IN_REGION", ("RegionID",), "EmbeddingTerritoryRegion"),
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare nine independent Northwind denormalization cases."
    )
    parser.add_argument("--uri", default=DEFAULT_URI)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    parser.add_argument("--dimension", type=int, default=DEFAULT_DIMENSION)
    parser.add_argument("--pair-count", type=int, default=DEFAULT_PAIR_COUNT, help="maximum child pairs sampled per case")
    parser.add_argument("--hash-gnn-seeds", type=int, default=DEFAULT_HASH_GNN_SEEDS, help="number of HashGNN random seeds")
    parser.add_argument("--keep-artifacts", action="store_true", help="keep temporary folded nodes")
    args = parser.parse_args(argv)
    if args.dimension < 8:
        parser.error("--dimension must be at least 8")
    if args.pair_count < 1:
        parser.error("--pair-count must be at least 1")
    if args.runs < 1:
        parser.error("--runs must be at least 1")
    if args.hash_gnn_seeds < 1: parser.error("--hash-gnn-seeds must be at least 1")
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
            "Neo4j GDS is unavailable. Install/enable Graph Data Science and "
            "restart Neo4j."
        ) from exc
    return str(record["version"])


def key_expression(alias: str, fields: Iterable[str]) -> str:
    parts = [
        f"coalesce(toString({alias}.{quote(field)}), '<NULL>')"
        for field in fields
    ]
    return " + '|' + ".join(parts)


def key_map_expression(alias: str, fields: Iterable[str]) -> str:
    entries = [f"{quote(field)}: {alias}.{quote(field)}" for field in fields]
    return "{" + ", ".join(entries) + "}"


def child_feature_fields(case: ExperimentCase) -> tuple[str, ...]:
    table_spec = TABLES[case.child]
    return tuple(
        field
        for field in table_spec.fields
        if field not in case.child_parent_fields
    )


def parent_feature_fields(case: ExperimentCase) -> tuple[str, ...]:
    return TABLES[case.parent].fields


def parent_key_sources(case: ExperimentCase) -> dict[str, str]:
    """Map each parent key to the equivalent foreign-key field on the child."""
    parent_keys = TABLES[case.parent].keys
    if len(parent_keys) != len(case.child_parent_fields):
        raise ValueError(f"Key mapping mismatch for {case.name}")
    return dict(zip(parent_keys, case.child_parent_fields, strict=True))


def folded_parent_property(case: ExperimentCase, field: str) -> str:
    """Return the stored property name for a non-key parent field."""
    if field in TABLES[case.child].fields:
        return "parent_" + field
    return field


def incident_relationships(case: ExperimentCase) -> tuple[Relationship, ...]:
    """Select relationships to rewire, without restricting the projected graph."""
    folded_tables = {case.child, case.parent}
    return tuple(
        relationship
        for relationship in RELATIONSHIPS
        if relationship.source in folded_tables
        or relationship.target in folded_tables
    )


def is_folded_relationship(
    case: ExperimentCase, relationship: Relationship
) -> bool:
    return (
        relationship.source == case.child
        and relationship.target == case.parent
        and relationship.rel_type == case.rel_type
    )


def choose_pairs(
    session: Any,
    case: ExperimentCase,
    seed: int,
    pair_count: int,
) -> tuple[list[dict[str, Any]], int, int]:
    child_table = TABLES[case.child]
    parent_table = TABLES[case.parent]
    child_key = key_expression("child", child_table.keys)
    parent_key = key_expression("parent", parent_table.keys)
    records = list(
        session.run(
            f"""
            MATCH (child:{quote(case.child)})
                  -[:{quote(case.rel_type)}]->
                  (parent:{quote(case.parent)})
            WITH {parent_key} AS parent_node_key,
                 {key_map_expression('parent', parent_table.keys)} AS parent_key,
                 {key_map_expression('parent', parent_table.fields)} AS parent_properties,
                 collect({{
                     node_key: {child_key},
                     key: {key_map_expression('child', child_table.keys)},
                     properties: {key_map_expression('child', child_table.fields)}
                 }}) AS children
            WHERE size(children) >= 2
            RETURN parent_node_key, parent_key, parent_properties, children
            ORDER BY parent_node_key
            """
        )
    )
    if not records:
        raise RuntimeError(f"No eligible parent group for {case.name}")

    all_pairs = []
    for record in records:
        parent_properties = dict(record["parent_properties"])
        children = sorted(
            (dict(child) for child in record["children"]),
            key=lambda child: child["node_key"],
        )
        for child_pair in combinations(children, 2):
            all_pairs.append(
                {
                    "parent_key": dict(record["parent_key"]),
                    "parent_properties": parent_properties,
                    "children": list(child_pair),
                    "node_keys": [child["node_key"] for child in child_pair],
                }
            )

    rng = random.Random(seed)
    pairs = rng.sample(all_pairs, min(pair_count, len(all_pairs)))
    pairs.sort(key=lambda pair: (
        json.dumps(pair["parent_key"], sort_keys=True), pair["node_keys"]
    ))
    return pairs, len(records), len(all_pairs)

def folded_property_map(case: ExperimentCase) -> str:
    child_table = TABLES[case.child]
    parent_table = TABLES[case.parent]
    parent_keys = parent_key_sources(case)
    entries = [
        f"{quote(field)}: child.{quote(field)}"
        for field in child_table.fields
    ]
    entries.extend(
        f"{quote(folded_parent_property(case, field))}: parent.{quote(field)}"
        for field in parent_table.fields
        if field not in parent_keys
    )
    return "{" + ", ".join(entries) + "}"


def cleanup_artifacts(session: Any) -> None:
    consume(
        session,
        """
        MATCH (node)
        WHERE any(label IN labels(node) WHERE label IN $labels)
        DETACH DELETE node
        """,
        labels=[case.folded_label for case in CASES],
    )


def cleanup_case_artifacts(session: Any, case: ExperimentCase) -> None:
    consume(
        session,
        f"MATCH (node:{quote(case.folded_label)}) DETACH DELETE node",
    )


def cleanup_feature_properties(session: Any) -> None:
    consume(
        session,
        f"""
        MATCH (node)
        WHERE any(label IN labels(node) WHERE label IN $labels)
        REMOVE node.{quote(FEATURE_PROPERTY)}
        """,
        labels=[*TABLES, *(case.folded_label for case in CASES)],
    )


def endpoint_pattern(
    case: ExperimentCase, label: str, external_alias: str
) -> tuple[str, str]:
    if label == case.child:
        return "(child)", "child"
    if label == case.parent:
        return "(parent)", "parent"
    return f"({external_alias}:{quote(label)})", external_alias


def materialize_denormalized(
    session: Any, case: ExperimentCase
) -> dict[str, Any]:
    node_record = session.run(
        f"""
        MATCH (child:{quote(case.child)})
              -[:{quote(case.rel_type)}]->
              (parent:{quote(case.parent)})
        CREATE (folded:{quote(case.folded_label)})
        SET folded = {folded_property_map(case)}
        RETURN count(folded) AS count
        """
    ).single(strict=True)

    inherited_edges: dict[str, int] = {}
    child_keys = TABLES[case.child].keys
    child_key = key_expression("child", child_keys)
    folded_key = key_expression("folded", child_keys)
    folded_tables = {case.child, case.parent}
    for relationship in incident_relationships(case):
        if is_folded_relationship(case, relationship):
            continue
        source_pattern, source_alias = endpoint_pattern(
            case, relationship.source, "source"
        )
        target_pattern, target_alias = endpoint_pattern(
            case, relationship.target, "target"
        )
        new_source = (
            "folded" if relationship.source in folded_tables else source_alias
        )
        new_target = (
            "folded" if relationship.target in folded_tables else target_alias
        )
        new_type = f"EMB_{relationship.rel_type}"
        record = session.run(
            f"""
            MATCH (child:{quote(case.child)})
                  -[:{quote(case.rel_type)}]->
                  (parent:{quote(case.parent)})
            MATCH {source_pattern}
                  -[:{quote(relationship.rel_type)}]->
                  {target_pattern}
            MATCH (folded:{quote(case.folded_label)})
            WHERE {folded_key} = {child_key}
            CREATE ({new_source})-[:{quote(new_type)}]->({new_target})
            RETURN count(*) AS count
            """
        ).single(strict=True)
        inherited_edges[relationship.rel_type] = int(record["count"])
    return {
        "nodes": int(node_record["count"]),
        "inherited_edges": inherited_edges,
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


def projected_labels(case: ExperimentCase) -> tuple[list[str], list[str]]:
    normalized = list(TABLES)
    denormalized = [case.folded_label] + [
        label for label in normalized if label not in {case.child, case.parent}
    ]
    return normalized, denormalized


def project_graphs(
    session: Any, case: ExperimentCase
) -> dict[str, dict[str, Any]]:
    for graph_name in (NORMALIZED_GRAPH, DENORMALIZED_GRAPH):
        drop_gds_graph(session, graph_name)

    relationships = RELATIONSHIPS
    incident_types = {
        relationship.rel_type for relationship in incident_relationships(case)
    }
    normalized_relationships = {
        relationship.rel_type: {"orientation": RELATIONSHIP_ORIENTATION}
        for relationship in relationships
    }
    denormalized_relationships = {
        (
            f"EMB_{relationship.rel_type}"
            if relationship.rel_type in incident_types
            else relationship.rel_type
        ): {"orientation": RELATIONSHIP_ORIENTATION}
        for relationship in relationships
        if not is_folded_relationship(case, relationship)
    }
    normalized_labels, denormalized_labels = projected_labels(case)
    normalized = session.run(
        """
        CALL gds.graph.project($name, $labels, $relationships)
        YIELD nodeCount, relationshipCount
        RETURN nodeCount, relationshipCount
        """,
        name=NORMALIZED_GRAPH,
        labels={
            label: {"properties": [FEATURE_PROPERTY]}
            for label in normalized_labels
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
            for label in denormalized_labels
        },
        relationships=denormalized_relationships,
    ).single(strict=True)
    return {
        "normalized": {
            "nodes": int(normalized["nodeCount"]),
            "relationships": int(normalized["relationshipCount"]),
            "node_labels": normalized_labels,
            "relationship_types": list(normalized_relationships),
        },
        "denormalized": {
            "nodes": int(denormalized["nodeCount"]),
            "relationships": int(denormalized["relationshipCount"]),
            "node_labels": denormalized_labels,
            "relationship_types": list(denormalized_relationships),
        },
    }


def feature_tokens(
    namespace: str, fields: Iterable[str], values: dict[str, Any]
) -> Iterable[str]:
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
        vector[index] += 1.0 if digest[8] & 1 else -1.0
    norm = math.sqrt(sum(value * value for value in vector))
    return vector if norm == 0 else [value / norm for value in vector]


def node_tokens(
    case: ExperimentCase, labels: list[str], props: dict[str, Any]
) -> list[str]:
    if case.folded_label in labels:
        child_values = {
            field: props.get(field)
            for field in child_feature_fields(case)
        }
        parent_keys = parent_key_sources(case)
        parent_values = {
            field: props.get(
                parent_keys.get(field, folded_parent_property(case, field))
            )
            for field in parent_feature_fields(case)
        }
        tokens = list(
            feature_tokens(
                case.child.lower(), child_feature_fields(case), child_values
            )
        )
        tokens.extend(
            feature_tokens(
                case.parent.lower(), parent_feature_fields(case), parent_values
            )
        )
        return tokens

    normalized_labels, _ = projected_labels(case)
    label = next(label for label in normalized_labels if label in labels)
    if label == case.child:
        fields = child_feature_fields(case)
    elif label == case.parent:
        fields = parent_feature_fields(case)
    else:
        fields = sorted(
            field
            for field in props
            if field != FEATURE_PROPERTY and not field.lower().endswith("id")
        )
    return list(feature_tokens(label.lower(), fields, props))


def prepare_features(
    session: Any, case: ExperimentCase, dimension: int
) -> None:
    normalized_labels, denormalized_labels = projected_labels(case)
    selected_labels = sorted(set(normalized_labels) | set(denormalized_labels))
    rows = []
    for record in session.run(
        """
        MATCH (node)
        WHERE any(label IN labels(node) WHERE label IN $labels)
        RETURN elementId(node) AS element_id,
               labels(node) AS node_labels,
               properties(node) AS node_properties
        """,
        labels=selected_labels,
    ):
        labels = list(record["node_labels"])
        props = dict(record["node_properties"])
        rows.append(
            {
                "element_id": record["element_id"],
                "features": feature_hash(
                    node_tokens(case, labels, props), dimension
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
    case: ExperimentCase,
    node_keys: list[str],
    dimension: int,
) -> tuple[dict[str, list[float]], dict[str, list[float]]]:
    child_keys = TABLES[case.child].keys
    child_key = key_expression("child", child_keys)
    folded_key = key_expression("folded", child_keys)
    normalized = {}
    for record in session.run(
        f"""
        MATCH (child:{quote(case.child)})
        WITH child, {child_key} AS node_key
        WHERE node_key IN $node_keys
        RETURN node_key, properties(child) AS node_properties
        """,
        node_keys=node_keys,
    ):
        normalized[record["node_key"]] = feature_hash(
            feature_tokens(
                case.child.lower(),
                child_feature_fields(case),
                dict(record["node_properties"]),
            ),
            dimension,
        )

    denormalized = {}
    for record in session.run(
        f"""
        MATCH (folded:{quote(case.folded_label)})
        WITH folded, {folded_key} AS node_key
        WHERE node_key IN $node_keys
        RETURN node_key, properties(folded) AS node_properties
        """,
        node_keys=node_keys,
    ):
        props = dict(record["node_properties"])
        denormalized[record["node_key"]] = feature_hash(
            node_tokens(case, [case.folded_label], props), dimension
        )
    return normalized, denormalized


def gds_embeddings(
    session: Any,
    procedure: str,
    graph_name: str,
    target_label: str,
    target_key_expression: str,
    node_keys: list[str],
    configuration: dict[str, Any],
) -> dict[str, list[float]]:
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
        WITH node, embedding, {target_key_expression} AS node_key
        WHERE node:{quote(target_label)} AND node_key IN $node_keys
        RETURN node_key, embedding
    """
    return {
        record["node_key"]: [float(value) for value in record["embedding"]]
        for record in session.run(
            query,
            graph_name=graph_name,
            configuration=configuration,
            node_keys=node_keys,
        )
    }


def graph_sage_embeddings(
    session: Any,
    graph_name: str,
    model_name: str,
    target_label: str,
    target_key_expression: str,
    node_keys: list[str],
    dimension: int,
    seed: int,
) -> dict[str, list[float]]:
    drop_gds_model(session, model_name)
    configuration = {
        "modelName": model_name,
        "featureProperties": [FEATURE_PROPERTY],
        "embeddingDimension": dimension,
        "aggregator": "mean",
        "activationFunction": "sigmoid",   # "ReLu"
        # "sampleSizes": [10],      # Default: [25,10]
        # "epochs": 3,
        "maxIterations": 10,
        "randomSeed": seed,
        # "concurrency": 1,
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
            target_key_expression,
            node_keys,
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


def score_pairs(
    normalized: dict[str, list[float]],
    denormalized: dict[str, list[float]],
    pairs: list[dict[str, Any]],
) -> list[dict[str, float]]:
    scores = []
    for pair in pairs:
        first, second = pair["node_keys"]
        try:
            normalized_score = cosine(normalized[first], normalized[second])
            denormalized_score = cosine(
                denormalized[first], denormalized[second]
            )
        except KeyError as exc:
            raise RuntimeError(f"Missing embedding for key {exc.args[0]}") from exc
        scores.append(
            {
                "normalized": normalized_score,
                "denormalized": denormalized_score,
                "delta_normalized_minus_denormalized": (
                    normalized_score - denormalized_score
                ),
            }
        )
    return scores


def summarize_scores(
    scores: list[dict[str, float]],
) -> dict[str, dict[str, float]]:
    fields = (
        "normalized",
        "denormalized",
        "delta_normalized_minus_denormalized",
    )
    return {
        field: {
            "mean": fmean(score[field] for score in scores),
            "median": float(median(score[field] for score in scores)),
        }
        for field in fields
    }


def run_case(
    session: Any,
    case: ExperimentCase,
    args: argparse.Namespace,
    seed: int,
    run_sweeps: bool,
) -> dict[str, Any]:
    pairs, eligible_parent_count, theoretical_pair_count = choose_pairs(
        session, case, seed, args.pair_count
    )
    materialized = materialize_denormalized(session, case)
    prepare_features(session, case, args.dimension)
    graph_counts = project_graphs(session, case)
    node_keys = sorted(
        {node_key for pair in pairs for node_key in pair["node_keys"]}
    )
    normalized_properties, denormalized_properties = property_embeddings(
        session, case, node_keys, args.dimension
    )
    fast_rp_config = {
        "embeddingDimension": args.dimension,
        "iterationWeights": [0.0, 1.0, 1.0],  # fixed at two hops for ratio sweep
        # "normalizationStrength": -0.5,
        "featureProperties": [FEATURE_PROPERTY],
        "propertyRatio": 0.5,       # property-dimension ratio
        # "nodeSelfInfluence": 1.0,
        "randomSeed": seed,
        # "concurrency": 1,
    }
    node2vec_config = {
        "embeddingDimension": args.dimension,
        # "walkLength": 20,
        # "walksPerNode": 5,
        # "windowSize": 5,
        # "iterations": 2,
        "randomSeed": seed,
        # "concurrency": 1,
    }
    hash_gnn_config = {
        "featureProperties": [FEATURE_PROPERTY],
        "iterations": 1,
        "embeddingDensity": args.dimension,
        # "heterogeneous": True,    # different relationship types should be treated differently
        "neighborInfluence": 1.0,   # neighbor sampling bias
        "binarizeFeatures": {
            "dimension": args.dimension * 4,
            "threshold": 0.0,
        },
        "outputDimension": args.dimension,
        "randomSeed": seed,
        # "concurrency": 1,
    }
    normalized_key = key_expression("node", TABLES[case.child].keys)
    denormalized_key = key_expression("node", TABLES[case.child].keys)

    method_scores = {"property_hash": score_pairs(normalized_properties, denormalized_properties, pairs)}
    parameter_sweeps, hop_sweeps = {}, {}
    sweep_specs = {
        "fast_rp": (("propertyRatio", [index / 10 for index in range(11)], parameter_sweeps), ("maxHops", range(1, DEFAULT_MAX_HOPS + 1), hop_sweeps)),
        "hash_gnn": (("neighborInfluence", [index / 5 for index in range(11)], parameter_sweeps), ("maxHops", range(1, DEFAULT_MAX_HOPS + 1), hop_sweeps)),
    } if run_sweeps else {}
    hash_gnn_seeds = [seed + offset for offset in range(args.hash_gnn_seeds)]
    algorithms = (("fast_rp", "gds.fastRP.stream", fast_rp_config), ("node2vec", "gds.node2vec.stream", node2vec_config), ("hash_gnn", "gds.hashgnn.stream", hash_gnn_config))
    for method, procedure, configuration in algorithms:
        specs = sweep_specs.get(method, ((None, [None], None),))
        for parameter, values, output in specs:
            for value in values:
                seeds = hash_gnn_seeds if method == "hash_gnn" else [seed]
                score_runs, seed_results = [], []
                for algorithm_seed in seeds:
                    run_config = dict(configuration)
                    if parameter == "maxHops":
                        run_config["iterationWeights" if method == "fast_rp" else "iterations"] = [1.0] * value if method == "fast_rp" else value
                    elif parameter:
                        run_config[parameter] = value
                    run_config["randomSeed"] = algorithm_seed
                    normalized = gds_embeddings(session, procedure, NORMALIZED_GRAPH, case.child, normalized_key, node_keys, run_config)
                    denormalized = gds_embeddings(session, procedure, DENORMALIZED_GRAPH, case.folded_label, denormalized_key, node_keys, run_config)
                    run_scores = score_pairs(normalized, denormalized, pairs)
                    score_runs.append(run_scores)
                    if method == "hash_gnn": seed_results.append({"seed": algorithm_seed, "similarities": summarize_scores(run_scores)})
                scores = [
                    {field: float(median(run[index][field] for run in score_runs)) for field in score_runs[0][index]}
                    for index in range(len(pairs))
                ]
                if output is not None:
                    sweep = output.setdefault(method, {"parameter": parameter, "points": []})
                    point = {"value": value, "similarities": summarize_scores(scores)}
                    if seed_results: point["seed_results"] = seed_results
                    sweep["points"].append(point)
                if not parameter or value == configuration.get(parameter): method_scores[method] = scores

    normalized_sage = graph_sage_embeddings(
        session,
        NORMALIZED_GRAPH,
        NORMALIZED_SAGE_MODEL,
        case.child,
        normalized_key,
        node_keys,
        args.dimension,
        seed,
    )
    denormalized_sage = graph_sage_embeddings(
        session,
        DENORMALIZED_GRAPH,
        DENORMALIZED_SAGE_MODEL,
        case.folded_label,
        denormalized_key,
        node_keys,
        args.dimension,
        seed,
    )
    method_scores["graph_sage"] = score_pairs(
        normalized_sage, denormalized_sage, pairs
    )

    pair_results = [
        {
            "parent_key": pair["parent_key"],
            "parent_properties": pair["parent_properties"],
            "children": pair["children"],
            "similarities": {
                method: scores[index]
                for method, scores in method_scores.items()
            },
        }
        for index, pair in enumerate(pairs)
    ]
    return {
        "random_seed": seed,
        "pair_sample_seed": seed,
        "child_label": case.child,
        "parent_label": case.parent,
        "relationship_type": case.rel_type,
        "eligible_parent_count": eligible_parent_count,
        "theoretical_pair_count": theoretical_pair_count,
        "requested_pair_count": args.pair_count,
        "pair_count": len(pairs),
        "selection_strategy": (
            "random sample from all child pairs sharing a parent, "
            "resampled for this seed"
        ),
        "hash_gnn_pair_extremes": hash_gnn_extremes(pair_results),
        "parameter_sweeps": parameter_sweeps,
        "hop_sweeps": hop_sweeps,
        "materialized_denormalized": materialized,
        "projected_graphs": graph_counts,
        "similarities": {
            method: summarize_scores(scores)
            for method, scores in method_scores.items()
        },
    }

def run_experiment(session: Any, args: argparse.Namespace) -> dict[str, Any]:
    gds_version = check_gds(session)
    cleanup_artifacts(session)
    random_seeds = [args.seed + offset for offset in range(args.runs)]
    experiments = {}
    for case in CASES:
        case_result = {
            "child_label": case.child,
            "parent_label": case.parent,
            "relationship_type": case.rel_type,
            "parameter_sweeps": {},
            "hop_sweeps": {},
            "runs": [],
        }
        for repeat, seed in enumerate(random_seeds, start=1):
            print(
                f"Running {case.name}, seed {repeat}/{args.runs}: "
                f"{case.child} -[:{case.rel_type}]-> {case.parent}",
                flush=True,
            )
            try:
                run_result = run_case(
                    session,
                    case,
                    args,
                    seed,
                    run_sweeps=repeat == 1,
                )
                if repeat == 1:
                    case_result["parameter_sweeps"] = run_result.pop(
                        "parameter_sweeps"
                    )
                    case_result["hop_sweeps"] = run_result.pop("hop_sweeps")
                else:
                    run_result.pop("parameter_sweeps")
                    run_result.pop("hop_sweeps")
                run_result["repeat"] = repeat
                case_result["runs"].append(run_result)
            finally:
                for graph_name in (NORMALIZED_GRAPH, DENORMALIZED_GRAPH):
                    drop_gds_graph(session, graph_name)
                if repeat < args.runs or not args.keep_artifacts:
                    cleanup_case_artifacts(session, case)
        experiments[case.name] = case_result
    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "database": args.database,
        "gds_version": gds_version,
        "graph_scope": "full_northwind",
        "relationship_orientation": RELATIONSHIP_ORIENTATION,
        "seed": args.seed,
        "random_seeds": random_seeds,
        "runs_per_case": args.runs,
        "parameter_sweep_seed": random_seeds[0],
        "embedding_dimension": args.dimension,
        "experiment_count": len(experiments),
        "experiments": experiments,
    }

def hash_gnn_extremes(rows: list[dict[str, Any]]) -> dict[str, Any]:
    score = lambda row: row["similarities"]["hash_gnn"]["normalized"] - row["similarities"]["hash_gnn"]["denormalized"]
    extremes = {}
    for label, pick in (("closest", min), ("largest_gap", max)):
        pair = pick(rows, key=lambda row: abs(score(row)))
        similarities = pair["similarities"]["hash_gnn"]
        extremes[label] = {"parent_key": pair["parent_key"], "child_keys": [child["key"] for child in pair["children"]], "child_tuples": [child["properties"] for child in pair["children"]], "normalized": similarities["normalized"], "denormalized": similarities["denormalized"], "norm_minus_denorm": score(pair), "absolute_difference": abs(score(pair)), "pair": pair}
    return extremes

def print_table(headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> None:
    widths = [max(len(header), *(len(row[index]) for row in rows)) for index, header in enumerate(headers)]
    print(" | ".join(f"{header:<{widths[index]}}" for index, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for row in rows: print(" | ".join(f"{value:<{widths[index]}}" for index, value in enumerate(row)))
def print_summary(result: dict[str, Any]) -> None:
    print(f"\nGDS={result['gds_version']}  dimension={result['embedding_dimension']}")
    for name, experiment in result["experiments"].items():
        runs = experiment["runs"]
        reference = runs[0]
        print(f"\n[{name}] {experiment['child_label']} -[:{experiment['relationship_type']}]-> {experiment['parent_label']}")
        pair_counts = [run["pair_count"] for run in runs]
        print(
            f"Runs={len(runs)}, pairs/run={min(pair_counts)}-{max(pair_counts)} "
            f"(theoretical pairs={reference['theoretical_pair_count']}, "
            f"eligible parents={reference['eligible_parent_count']})"
        )
        print(
            f"{'method':<16} {'norm med mean±sd':>22} "
            f"{'denorm med mean±sd':>22}"
        )
        for method in reference["similarities"]:
            values = {}
            for representation in ("normalized", "denormalized"):
                medians = [
                    run["similarities"][method][representation]["median"]
                    for run in runs
                ]
                values[representation] = (
                    fmean(medians),
                    stdev(medians) if len(medians) > 1 else 0.0,
                )
            print(
                f"{method:<16} "
                f"{values['normalized'][0]:>10.6f} ± "
                f"{values['normalized'][1]:<9.6f} "
                f"{values['denormalized'][0]:>10.6f} ± "
                f"{values['denormalized'][1]:<9.6f}"
            )
        summary_rows, tuple_rows = [], []
        for label, row in reference["hash_gnn_pair_extremes"].items():
            cells = [json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str) for value in [row["parent_key"], *row["child_keys"], *row["child_tuples"]]]
            summary_rows.append((label, *cells[:3], f"{row['normalized']:.6f}", f"{row['denormalized']:.6f}", f"{row['norm_minus_denorm']:.6f}", f"{row['absolute_difference']:.6f}"))
            tuple_rows.extend(((label, "child 1", cells[3]), (label, "child 2", cells[4])))
        print("HashGNN extremes from the first seed")
        print_table(("kind", "parent key", "child 1 key", "child 2 key", "norm", "denorm", "norm-denorm", "abs-diff"), summary_rows)
        print("HashGNN tuple details")
        print_table(("kind", "child", "tuple"), tuple_rows)
    print(f"\nSaved: {OUTPUT_FILE}")

def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    result: dict[str, Any] | None = None
    try:
        driver.verify_connectivity()
        with driver.session(database=args.database) as session:
            try:
                result = run_experiment(session, args)
                OUTPUT_FILE.write_text(
                    json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
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
