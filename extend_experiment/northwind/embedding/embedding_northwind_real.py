#!/usr/bin/env python3
"""Compare same-parent and different-parent pairs before and after folding."""

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
DEFAULT_RUNS = 1
DEFAULT_DIMENSION = 128
DEFAULT_PAIR_COUNT = 100000
DEFAULT_HASH_GNN_SEEDS = 1
DEFAULT_MAX_HOPS = 10
DIFFERENT_PARENT_SEED_OFFSET = 1_000_003
RELATIONSHIP_ORIENTATION = "UNDIRECTED"  # Change to "UNDIRECTED" when needed.
FEATURE_PROPERTY = "exp_embedding_features"
WORD_RE = re.compile(r"[\w]+", re.UNICODE)

if RELATIONSHIP_ORIENTATION not in {"NATURAL", "UNDIRECTED"}:
    raise ValueError(
        "RELATIONSHIP_ORIENTATION must be 'NATURAL' or 'UNDIRECTED'"
    )


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
    ExperimentCase(
        "order_customer",
        "Order",
        "Customer",
        "PLACED_BY",
        ("CustomerID",),
        "EmbeddingCustomerOrder",
    ),
    ExperimentCase(
        "order_detail_product",
        "OrderDetail",
        "Product",
        "OF_PRODUCT",
        ("ProductID",),
        "EmbeddingOrderDetailProduct",
    ),
    ExperimentCase(
        "order_detail_order",
        "OrderDetail",
        "Order",
        "OF_ORDER",
        ("OrderID",),
        "EmbeddingOrderDetailOrder",
    ),
    ExperimentCase(
        "product_supplier",
        "Product",
        "Supplier",
        "SUPPLIED_BY",
        ("SupplierID",),
        "EmbeddingProductSupplier",
    ),
    ExperimentCase(
        "order_employee",
        "Order",
        "Employee",
        "HANDLED_BY",
        ("EmployeeID",),
        "EmbeddingOrderEmployee",
    ),
    ExperimentCase(
        "order_shipper",
        "Order",
        "Shipper",
        "SHIPPED_BY",
        ("ShipVia",),
        "EmbeddingOrderShipper",
    ),
    ExperimentCase(
        "product_category",
        "Product",
        "Category",
        "IN_CATEGORY",
        ("CategoryID",),
        "EmbeddingProductCategory",
    ),
    ExperimentCase(
        "employee_territory_employee",
        "EmployeeTerritory",
        "Employee",
        "HAS_TERRITORY",
        ("EmployeeID",),
        "EmbeddingEmployeeTerritoryEmployee",
    ),
    ExperimentCase(
        "territory_region",
        "Territory",
        "Region",
        "IN_REGION",
        ("RegionID",),
        "EmbeddingTerritoryRegion",
    ),
)


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
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    int,
    int,
    int,
]:
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
            RETURN parent_node_key, parent_key, parent_properties, children
            ORDER BY parent_node_key
            """
        )
    )
    eligible_records = [record for record in records if len(record["children"]) >= 2]
    if not eligible_records:
        raise RuntimeError(f"No eligible parent group for {case.name}")

    all_pairs = []
    all_children = []
    for record in eligible_records:
        parent_properties = dict(record["parent_properties"])
        children = sorted(
            (dict(child) for child in record["children"]),
            key=lambda child: child["node_key"],
        )
        all_children.extend(
            {
                "parent_node_key": record["parent_node_key"],
                "parent_key": dict(record["parent_key"]),
                "node_key": child["node_key"],
            }
            for child in children
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

    theoretical_different_parent_pairs = (
        len(all_children) * (len(all_children) - 1) // 2 - len(all_pairs)
    )
    if theoretical_different_parent_pairs < len(pairs):
        raise RuntimeError(
            f"{case.name}: not enough different-parent pairs to match "
            f"{len(pairs)} same-parent pairs"
        )

    negative_rng = random.Random(seed + DIFFERENT_PARENT_SEED_OFFSET)
    selected_indexes: set[tuple[int, int]] = set()
    different_parent_pairs = []
    while len(different_parent_pairs) < len(pairs):
        first_index, second_index = sorted(
            negative_rng.sample(range(len(all_children)), 2)
        )
        index_pair = (first_index, second_index)
        if index_pair in selected_indexes:
            continue
        first = all_children[first_index]
        second = all_children[second_index]
        if first["parent_node_key"] == second["parent_node_key"]:
            continue
        selected_indexes.add(index_pair)
        different_parent_pairs.append(
            {
                "parent_keys": [first["parent_key"], second["parent_key"]],
                "node_keys": [first["node_key"], second["node_key"]],
            }
        )
    different_parent_pairs.sort(
        key=lambda pair: (
            json.dumps(pair["parent_keys"], sort_keys=True),
            pair["node_keys"],
        )
    )
    return (
        pairs,
        different_parent_pairs,
        len(eligible_records),
        len(all_pairs),
        theoretical_different_parent_pairs,
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
        "activationFunction": "ReLu",
        "sampleSizes": [25, 10],
        "epochs": 3,
        "maxIterations": 20,
        "searchDepth": 2,
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


def median_score_runs(
        score_runs: list[list[dict[str, float]]],
) -> list[dict[str, float]]:
    return [
        {
            field: float(median(run[index][field] for run in score_runs))
            for field in score_runs[0][index]
        }
        for index in range(len(score_runs[0]))
    ]


def summarize_separation(
        same_parent_scores: list[dict[str, float]],
        different_parent_scores: list[dict[str, float]],
) -> dict[str, dict[str, float]]:
    same_parent = summarize_scores(same_parent_scores)
    different_parent = summarize_scores(different_parent_scores)
    return {
        field: {
            statistic: (
                    same_parent[field][statistic]
                    - different_parent[field][statistic]
            )
            for statistic in ("mean", "median")
        }
        for field in (
            "normalized",
            "denormalized",
            "delta_normalized_minus_denormalized",
        )
    }


def hash_gnn_extremes(rows: list[dict[str, Any]]) -> dict[str, Any]:
    score = lambda row: row["similarities"]["hash_gnn"]["normalized"] - row["similarities"]["hash_gnn"]["denormalized"]
    extremes = {}
    for label, pick in (("closest", min), ("largest_gap", max)):
        pair = pick(rows, key=lambda row: abs(score(row)))
        similarities = pair["similarities"]["hash_gnn"]
        extremes[label] = {"parent_key": pair["parent_key"], "child_keys": [child["key"] for child in pair["children"]],
                           "child_tuples": [child["properties"] for child in pair["children"]],
                           "normalized": similarities["normalized"], "denormalized": similarities["denormalized"],
                           "norm_minus_denorm": score(pair), "absolute_difference": abs(score(pair)), "pair": pair}
    return extremes


DEFAULT_WORK_DATABASE = "northwind-embedding-real"
WORK_DATABASE_PREFIX = "northwind-embedding-real"
COPY_ID_PROPERTY = "__embeddingRealSourceId"
COPY_BATCH_SIZE = 1_000

NORMALIZED_GRAPH = "northwind_real_normalized"
DENORMALIZED_GRAPH = "northwind_real_denormalized"
NORMALIZED_SAGE_MODEL = "northwind_real_normalized_sage"
DENORMALIZED_SAGE_MODEL = "northwind_real_denormalized_sage"

OUTPUT_NAME_BY_ORIENTATION = {
    "NATURAL": "embedding_results_real_natrual.json",
    "UNDIRECTED": "embedding_results_real_undirection.json",
}
OUTPUT_FILE = Path(__file__).with_name(
    OUTPUT_NAME_BY_ORIENTATION[RELATIONSHIP_ORIENTATION]
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare the normalized Northwind database with a physically "
            "denormalized copy in a dedicated work database."
        )
    )
    parser.add_argument("--uri", default=DEFAULT_URI)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--work-database", default=DEFAULT_WORK_DATABASE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    parser.add_argument("--dimension", type=int, default=DEFAULT_DIMENSION)
    parser.add_argument(
        "--pair-count",
        type=int,
        default=DEFAULT_PAIR_COUNT,
        help="maximum child pairs sampled per case",
    )
    parser.add_argument(
        "--hash-gnn-seeds",
        type=int,
        default=DEFAULT_HASH_GNN_SEEDS,
    )
    parser.add_argument(
        "--keep-work-database",
        action="store_true",
        help="keep the final physically denormalized work database",
    )
    args = parser.parse_args(argv)
    if args.dimension < 8:
        parser.error("--dimension must be at least 8")
    if args.pair_count < 1:
        parser.error("--pair-count must be at least 1")
    if args.runs < 1:
        parser.error("--runs must be at least 1")
    if args.hash_gnn_seeds < 1:
        parser.error("--hash-gnn-seeds must be at least 1")
    if args.work_database in {args.database, "system"}:
        parser.error("--work-database must be separate from the source database")
    if not args.work_database.startswith(WORK_DATABASE_PREFIX):
        parser.error(
            f"--work-database must start with {WORK_DATABASE_PREFIX!r}; "
            "the work database is cleared and deleted by this script"
        )
    return args


def batches(rows: list[dict[str, Any]]) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(rows), COPY_BATCH_SIZE):
        yield rows[start: start + COPY_BATCH_SIZE]


def ensure_work_database(driver: Any, database: str) -> None:
    with driver.session(database="system") as session:
        record = session.run(
            """
            SHOW DATABASES
            YIELD name, currentStatus
            WHERE name = $name
            RETURN currentStatus
            """,
            name=database,
        ).single()
        if record is None:
            session.run(
                "CREATE DATABASE $name IF NOT EXISTS WAIT 60 SECONDS",
                name=database,
            ).consume()
        elif str(record["currentStatus"]).lower() != "online":
            raise RuntimeError(f"Work database {database!r} is not online")


def drop_catalog_objects(session: Any) -> None:
    for graph_name in (NORMALIZED_GRAPH, DENORMALIZED_GRAPH):
        try:
            drop_gds_graph(session, graph_name)
        except Exception:
            pass
    for model_name in (NORMALIZED_SAGE_MODEL, DENORMALIZED_SAGE_MODEL):
        try:
            drop_gds_model(session, model_name)
        except Exception:
            pass


def copy_source_database(driver: Any, source: str, target: str) -> dict[str, int]:
    """Replace the dedicated work database with a clean logical copy."""
    ensure_work_database(driver, target)
    with driver.session(database=target) as target_session:
        drop_catalog_objects(target_session)
        consume(target_session, "MATCH (node) DETACH DELETE node")

    node_counts: dict[str, int] = {}
    with (
        driver.session(database=source) as source_session,
        driver.session(database=target) as target_session,
    ):
        labels = list(TABLES)
        relationship_types = [rel.rel_type for rel in RELATIONSHIPS]
        invalid_nodes = source_session.run(
            """
            MATCH (node)
            WITH labels(node) AS node_labels
            WHERE size(node_labels) <> 1 OR NOT node_labels[0] IN $labels
            RETURN count(*) AS count
            """,
            labels=labels,
        ).single(strict=True)["count"]
        if invalid_nodes:
            raise RuntimeError(
                f"Source database has {invalid_nodes} nodes outside the "
                "declared single-label Northwind schema"
            )
        invalid_relationships = source_session.run(
            """
            MATCH ()-[relationship]->()
            WHERE NOT type(relationship) IN $types
            RETURN count(relationship) AS count
            """,
            types=relationship_types,
        ).single(strict=True)["count"]
        if invalid_relationships:
            raise RuntimeError(
                f"Source database has {invalid_relationships} undeclared "
                "relationships"
            )
        source_relationship_count = int(
            source_session.run(
                "MATCH ()-[relationship]->() RETURN count(relationship) AS count"
            ).single(strict=True)["count"]
        )

        for label in TABLES:
            rows = []
            for record in source_session.run(
                    f"MATCH (node:{quote(label)}) "
                    "RETURN elementId(node) AS source_id, properties(node) AS props"
            ):
                props = dict(record["props"])
                props.pop(FEATURE_PROPERTY, None)
                props.pop(COPY_ID_PROPERTY, None)
                rows.append({"source_id": record["source_id"], "props": props})
            node_counts[label] = len(rows)
            for batch in batches(rows):
                consume(
                    target_session,
                    f"""
                    UNWIND $rows AS row
                    CREATE (node:{quote(label)})
                    SET node = row.props,
                        node.{quote(COPY_ID_PROPERTY)} = row.source_id
                    """,
                    rows=batch,
                )

        relationship_count = 0
        for relationship in RELATIONSHIPS:
            rows = [
                {
                    "source_id": record["source_id"],
                    "target_id": record["target_id"],
                    "props": dict(record["props"]),
                }
                for record in source_session.run(
                    f"""
                    MATCH (source:{quote(relationship.source)})
                          -[rel:{quote(relationship.rel_type)}]->
                          (target:{quote(relationship.target)})
                    RETURN elementId(source) AS source_id,
                           elementId(target) AS target_id,
                           properties(rel) AS props
                    """
                )
            ]
            relationship_count += len(rows)
            for batch in batches(rows):
                consume(
                    target_session,
                    f"""
                    UNWIND $rows AS row
                    MATCH (source:{quote(relationship.source)})
                    WHERE source.{quote(COPY_ID_PROPERTY)} = row.source_id
                    MATCH (target:{quote(relationship.target)})
                    WHERE target.{quote(COPY_ID_PROPERTY)} = row.target_id
                    CREATE (source)-[rel:{quote(relationship.rel_type)}]->(target)
                    SET rel = row.props
                    """,
                    rows=batch,
                )

        if relationship_count != source_relationship_count:
            raise RuntimeError(
                "Declared relationship patterns cover "
                f"{relationship_count} of {source_relationship_count} "
                "source relationships; check endpoints and directions"
            )

        consume(
            target_session,
            f"MATCH (node) REMOVE node.{quote(COPY_ID_PROPERTY)}",
        )
        for label, expected in node_counts.items():
            actual = target_session.run(
                f"MATCH (node:{quote(label)}) RETURN count(node) AS count"
            ).single(strict=True)["count"]
            if int(actual) != expected:
                raise RuntimeError(
                    f"Copy failed for {label}: expected {expected}, got {actual}"
                )
        actual_relationships = target_session.run(
            "MATCH ()-[relationship]->() RETURN count(relationship) AS count"
        ).single(strict=True)["count"]
        if int(actual_relationships) != relationship_count:
            raise RuntimeError(
                "Relationship copy failed: expected "
                f"{relationship_count}, got {actual_relationships}"
            )
    return {
        "nodes": sum(node_counts.values()),
        "relationships": relationship_count,
    }


def validate_case_dependency(session: Any, case: ExperimentCase) -> int:
    child_key = key_expression("child", TABLES[case.child].keys)
    record = session.run(
        f"""
        MATCH (child:{quote(case.child)})
        OPTIONAL MATCH (child)-[:{quote(case.rel_type)}]->
                       (parent:{quote(case.parent)})
        WITH child, count(parent) AS parent_count
        RETURN count(child) AS child_count,
               sum(CASE WHEN parent_count = 1 THEN 0 ELSE 1 END) AS invalid_count
        """
    ).single(strict=True)
    child_count = int(record["child_count"])
    if int(record["invalid_count"]):
        raise RuntimeError(
            f"{case.name}: every {case.child} must have exactly one "
            f"{case.parent}"
        )
    unique_count = session.run(
        f"""
        MATCH (child:{quote(case.child)})
        RETURN count(DISTINCT {child_key}) AS count
        """
    ).single(strict=True)["count"]
    if int(unique_count) != child_count:
        raise RuntimeError(f"{case.name}: child key is not unique")
    return child_count


def count_declared_relationship(
        session: Any, relationship: Relationship
) -> int:
    return int(
        session.run(
            f"""
            MATCH (source:{quote(relationship.source)})
                  -[rel:{quote(relationship.rel_type)}]->
                  (target:{quote(relationship.target)})
            RETURN count(rel) AS count
            """
        ).single(strict=True)["count"]
    )


def copy_parent_relationships(
        session: Any, case: ExperimentCase
) -> dict[str, int]:
    copied: dict[str, int] = {}
    for relationship in incident_relationships(case):
        if is_folded_relationship(case, relationship):
            continue
        if relationship.source == case.parent:
            record = session.run(
                f"""
                MATCH (child:{quote(case.child)})
                      -[:{quote(case.rel_type)}]->
                      (parent:{quote(case.parent)})
                MATCH (parent)-[original:{quote(relationship.rel_type)}]->
                      (target:{quote(relationship.target)})
                CREATE (child)-[copy:{quote(relationship.rel_type)}]->(target)
                SET copy = properties(original)
                RETURN count(copy) AS count
                """
            ).single(strict=True)
        elif relationship.target == case.parent:
            record = session.run(
                f"""
                MATCH (child:{quote(case.child)})
                      -[:{quote(case.rel_type)}]->
                      (parent:{quote(case.parent)})
                MATCH (source:{quote(relationship.source)})
                      -[original:{quote(relationship.rel_type)}]->(parent)
                CREATE (source)-[copy:{quote(relationship.rel_type)}]->(child)
                SET copy = properties(original)
                RETURN count(copy) AS count
                """
            ).single(strict=True)
        else:
            continue
        copied[relationship.rel_type] = int(record["count"])
    return copied


def physically_denormalize(
        session: Any, case: ExperimentCase
) -> dict[str, Any]:
    """Persist the fold by reusing child nodes and deleting parent nodes."""
    child_count = validate_case_dependency(session, case)
    parent_count = int(
        session.run(
            f"MATCH (parent:{quote(case.parent)}) "
            "RETURN count(parent) AS count"
        ).single(strict=True)["count"]
    )
    retained_child_relationships = {
        relationship.rel_type: count_declared_relationship(session, relationship)
        for relationship in incident_relationships(case)
        if not is_folded_relationship(case, relationship)
           and (
                   relationship.source == case.child
                   or relationship.target == case.child
           )
    }
    copied_parent_relationships = copy_parent_relationships(session, case)

    parent_keys = parent_key_sources(case)
    assignments = [
        f"child.{quote(folded_parent_property(case, field))} = "
        f"parent.{quote(field)}"
        for field in TABLES[case.parent].fields
        if field not in parent_keys
    ]
    session.run(
        f"""
        MATCH (child:{quote(case.child)})
              -[:{quote(case.rel_type)}]->
              (parent:{quote(case.parent)})
        SET {', '.join(assignments)}
        SET child:{quote(case.folded_label)}
        REMOVE child:{quote(case.child)}
        """
    ).consume()
    consume(
        session,
        f"""
        MATCH (folded:{quote(case.folded_label)})
              -[relationship:{quote(case.rel_type)}]->
              (parent:{quote(case.parent)})
        DELETE relationship
        """,
    )
    consume(
        session,
        f"MATCH (parent:{quote(case.parent)}) DETACH DELETE parent",
    )

    remaining = session.run(
        f"""
        MATCH (node)
        WHERE node:{quote(case.child)}
           OR node:{quote(case.parent)}
        RETURN count(node) AS count
        """
    ).single(strict=True)["count"]
    folded_count = int(
        session.run(
            f"MATCH (node:{quote(case.folded_label)}) "
            "RETURN count(node) AS count"
        ).single(strict=True)["count"]
    )
    if remaining or folded_count != child_count:
        raise RuntimeError(
            f"{case.name}: physical fold validation failed "
            f"(remaining={remaining}, folded={folded_count})"
        )
    storage = session.run(
        """
        MATCH (node)
        WITH count(node) AS nodes
        MATCH ()-[relationship]->()
        RETURN nodes, count(relationship) AS relationships
        """
    ).single(strict=True)
    return {
        "nodes": folded_count,
        "reused_child_nodes": child_count,
        "deleted_parent_nodes": parent_count,
        "remaining_source_nodes": 0,
        "retained_child_relationships": retained_child_relationships,
        "copied_parent_relationships": copied_parent_relationships,
        "physical_nodes": int(storage["nodes"]),
        "physical_relationships": int(storage["relationships"]),
    }


def project_one_graph(
        session: Any,
        case: ExperimentCase,
        representation: str,
) -> dict[str, Any]:
    if representation == "normalized":
        graph_name = NORMALIZED_GRAPH
        labels = list(TABLES)
        declared_relationships = RELATIONSHIPS
    elif representation == "denormalized":
        graph_name = DENORMALIZED_GRAPH
        _, labels = projected_labels(case)
        declared_relationships = tuple(
            relationship
            for relationship in RELATIONSHIPS
            if not is_folded_relationship(case, relationship)
        )
    else:
        raise ValueError(f"Unknown representation: {representation}")
    relationships = {
        relationship.rel_type: {"orientation": RELATIONSHIP_ORIENTATION}
        for relationship in declared_relationships
    }
    drop_gds_graph(session, graph_name)
    record = session.run(
        """
        CALL gds.graph.project($name, $labels, $relationships)
        YIELD nodeCount, relationshipCount
        RETURN nodeCount, relationshipCount
        """,
        name=graph_name,
        labels={
            label: {"properties": [FEATURE_PROPERTY]}
            for label in labels
        },
        relationships=relationships,
    ).single(strict=True)
    return {
        "nodes": int(record["nodeCount"]),
        "relationships": int(record["relationshipCount"]),
        "node_labels": labels,
        "relationship_types": list(relationships),
    }


def property_embeddings(
        normalized_session: Any,
        denormalized_session: Any,
        case: ExperimentCase,
        node_keys: list[str],
        dimension: int,
) -> tuple[dict[str, list[float]], dict[str, list[float]]]:
    child_key = key_expression("child", TABLES[case.child].keys)
    normalized = {}
    for record in normalized_session.run(
            f"""
        MATCH (child:{quote(case.child)})
        WITH child, {child_key} AS node_key
        WHERE node_key IN $node_keys
        RETURN node_key, properties(child) AS props
        """,
            node_keys=node_keys,
    ):
        normalized[record["node_key"]] = feature_hash(
            feature_tokens(
                case.child.lower(),
                child_feature_fields(case),
                dict(record["props"]),
            ),
            dimension,
        )

    folded_key = key_expression("folded", TABLES[case.child].keys)
    denormalized = {}
    for record in denormalized_session.run(
            f"""
        MATCH (folded:{quote(case.folded_label)})
        WITH folded, {folded_key} AS node_key
        WHERE node_key IN $node_keys
        RETURN node_key, properties(folded) AS props
        """,
            node_keys=node_keys,
    ):
        props = dict(record["props"])
        denormalized[record["node_key"]] = feature_hash(
            node_tokens(case, [case.folded_label], props), dimension
        )
    return normalized, denormalized


def run_seed(
        source_session: Any,
        work_session: Any,
        case: ExperimentCase,
        args: argparse.Namespace,
        seed: int,
        run_sweeps: bool,
        graph_counts: dict[str, dict[str, Any]],
        physical_denormalization: dict[str, Any],
) -> dict[str, Any]:
    (
        pairs,
        different_parent_pairs,
        eligible_parent_count,
        theoretical_pair_count,
        theoretical_different_parent_pair_count,
    ) = choose_pairs(source_session, case, seed, args.pair_count)
    node_keys = sorted(
        {
            node_key
            for pair in (*pairs, *different_parent_pairs)
            for node_key in pair["node_keys"]
        }
    )
    normalized_properties, denormalized_properties = property_embeddings(
        source_session,
        work_session,
        case,
        node_keys,
        args.dimension,
    )
    fast_rp_config = {
        "embeddingDimension": args.dimension,
        "iterationWeights": [0.0, 1.0, 1.0],
        "featureProperties": [FEATURE_PROPERTY],
        "propertyRatio": 0.0,
        "randomSeed": seed,
    }
    node2vec_config = {
        "embeddingDimension": args.dimension,
        "walkLength": 20,
        "walksPerNode": 20,
        "windowSize": 5,
        "returnFactor": 2.0,
        "inOutFactor": 0.5,
        "iterations": 3,
        "randomSeed": seed,
    }
    hash_gnn_config = {
        "featureProperties": [FEATURE_PROPERTY],
        "iterations": 1,
        "embeddingDensity": args.dimension,
        "neighborInfluence": 1.0,
        "binarizeFeatures": {
            "dimension": args.dimension * 4,
            "threshold": 0.0,
        },
        "outputDimension": args.dimension,
        "randomSeed": seed,
    }
    normalized_key = key_expression("node", TABLES[case.child].keys)
    denormalized_key = normalized_key
    method_scores = {
        "property_hash": {
            "same_parent": score_pairs(
                normalized_properties, denormalized_properties, pairs
            ),
            "different_parent": score_pairs(
                normalized_properties,
                denormalized_properties,
                different_parent_pairs,
            ),
        }
    }
    parameter_sweeps: dict[str, Any] = {}
    hop_sweeps: dict[str, Any] = {}
    sweep_specs = (
        {
            "fast_rp": (
                (
                    "propertyRatio",
                    [index / 10 for index in range(11)],
                    parameter_sweeps,
                ),
                (
                    "maxHops",
                    range(1, DEFAULT_MAX_HOPS + 1),
                    hop_sweeps,
                ),
            ),
            "hash_gnn": (
                (
                    "neighborInfluence",
                    [index / 5 for index in range(11)],
                    parameter_sweeps,
                ),
                (
                    "maxHops",
                    range(1, DEFAULT_MAX_HOPS + 1),
                    hop_sweeps,
                ),
            ),
        }
        if run_sweeps
        else {}
    )
    algorithms = (
        ("fast_rp", "gds.fastRP.stream", fast_rp_config),
        ("node2vec", "gds.node2vec.stream", node2vec_config),
        ("hash_gnn", "gds.hashgnn.stream", hash_gnn_config),
    )
    hash_gnn_seeds = [seed + offset for offset in range(args.hash_gnn_seeds)]
    for method, procedure, configuration in algorithms:
        specs = sweep_specs.get(method, ((None, [None], None),))
        for parameter, values, output in specs:
            for value in values:
                seeds = hash_gnn_seeds if method == "hash_gnn" else [seed]
                same_parent_runs, different_parent_runs = [], []
                seed_results = []
                for algorithm_seed in seeds:
                    run_config = dict(configuration)
                    if parameter == "maxHops":
                        if method == "fast_rp":
                            run_config["iterationWeights"] = [1.0] * value
                        else:
                            run_config["iterations"] = value
                    elif parameter:
                        run_config[parameter] = value
                    run_config["randomSeed"] = algorithm_seed
                    normalized = gds_embeddings(
                        work_session,
                        procedure,
                        NORMALIZED_GRAPH,
                        case.folded_label,
                        normalized_key,
                        node_keys,
                        run_config,
                    )
                    denormalized = gds_embeddings(
                        work_session,
                        procedure,
                        DENORMALIZED_GRAPH,
                        case.folded_label,
                        denormalized_key,
                        node_keys,
                        run_config,
                    )
                    same_parent_scores = score_pairs(
                        normalized, denormalized, pairs
                    )
                    different_parent_scores = score_pairs(
                        normalized, denormalized, different_parent_pairs
                    )
                    same_parent_runs.append(same_parent_scores)
                    different_parent_runs.append(different_parent_scores)
                    if method == "hash_gnn":
                        seed_results.append(
                            {
                                "seed": algorithm_seed,
                                "similarities": summarize_scores(
                                    same_parent_scores
                                ),
                                "negative_similarities": summarize_scores(
                                    different_parent_scores
                                ),
                                "separation": summarize_separation(
                                    same_parent_scores,
                                    different_parent_scores,
                                ),
                            }
                        )
                same_parent_scores = median_score_runs(same_parent_runs)
                different_parent_scores = median_score_runs(
                    different_parent_runs
                )
                if output is not None:
                    sweep = output.setdefault(
                        method, {"parameter": parameter, "points": []}
                    )
                    point = {
                        "value": value,
                        "similarities": summarize_scores(same_parent_scores),
                        "negative_similarities": summarize_scores(
                            different_parent_scores
                        ),
                        "separation": summarize_separation(
                            same_parent_scores, different_parent_scores
                        ),
                    }
                    if seed_results:
                        point["seed_results"] = seed_results
                    sweep["points"].append(point)
                if not parameter or value == configuration.get(parameter):
                    method_scores[method] = {
                        "same_parent": same_parent_scores,
                        "different_parent": different_parent_scores,
                    }

    normalized_sage = graph_sage_embeddings(
        work_session,
        NORMALIZED_GRAPH,
        NORMALIZED_SAGE_MODEL,
        case.folded_label,
        normalized_key,
        node_keys,
        args.dimension,
        seed,
    )
    denormalized_sage = graph_sage_embeddings(
        work_session,
        DENORMALIZED_GRAPH,
        DENORMALIZED_SAGE_MODEL,
        case.folded_label,
        denormalized_key,
        node_keys,
        args.dimension,
        seed,
    )
    method_scores["graph_sage"] = {
        "same_parent": score_pairs(
            normalized_sage, denormalized_sage, pairs
        ),
        "different_parent": score_pairs(
            normalized_sage, denormalized_sage, different_parent_pairs
        ),
    }
    pair_results = [
        {
            "parent_key": pair["parent_key"],
            "parent_properties": pair["parent_properties"],
            "children": pair["children"],
            "similarities": {
                method: groups["same_parent"][index]
                for method, groups in method_scores.items()
            },
        }
        for index, pair in enumerate(pairs)
    ]
    sampled = len(pairs) < theoretical_pair_count
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
            "random sample from all child pairs sharing a parent"
            if sampled
            else "all child pairs sharing a parent"
        ),
        "negative_pair_sample_seed": seed + DIFFERENT_PARENT_SEED_OFFSET,
        "negative_theoretical_pair_count": (
            theoretical_different_parent_pair_count
        ),
        "negative_pair_count": len(different_parent_pairs),
        "negative_selection_strategy": (
            "random sample of unique child pairs with different parents, "
            "matched to the same-parent pair count and drawn from the same "
            "eligible parent groups"
        ),
        "separation_definition": "same_parent_minus_different_parent",
        "negative_pair_examples": different_parent_pairs[:3],
        "hash_gnn_pair_extremes": hash_gnn_extremes(pair_results),
        "parameter_sweeps": parameter_sweeps,
        "hop_sweeps": hop_sweeps,
        "physical_denormalization": physical_denormalization,
        "projected_graphs": graph_counts,
        "similarities": {
            method: summarize_scores(groups["same_parent"])
            for method, groups in method_scores.items()
        },
        "negative_similarities": {
            method: summarize_scores(groups["different_parent"])
            for method, groups in method_scores.items()
        },
        "separation": {
            method: summarize_separation(
                groups["same_parent"], groups["different_parent"]
            )
            for method, groups in method_scores.items()
        },
    }


def run_case(
        driver: Any,
        case: ExperimentCase,
        args: argparse.Namespace,
        random_seeds: list[int],
) -> dict[str, Any]:
    copied = copy_source_database(driver, args.database, args.work_database)
    case_result = {
        "child_label": case.child,
        "parent_label": case.parent,
        "relationship_type": case.rel_type,
        "work_database": args.work_database,
        "source_copy": copied,
        "parameter_sweeps": {},
        "hop_sweeps": {},
        "runs": [],
    }
    with (
        driver.session(database=args.database) as source_session,
        driver.session(database=args.work_database) as work_session,
    ):
        try:
            cleanup_feature_properties(work_session)
            prepare_features(work_session, case, args.dimension)
            normalized_counts = project_one_graph(
                work_session, case, "normalized"
            )

            physical = physically_denormalize(work_session, case)
            prepare_features(work_session, case, args.dimension)
            denormalized_counts = project_one_graph(
                work_session, case, "denormalized"
            )
            graph_counts = {
                "normalized": normalized_counts,
                "denormalized": denormalized_counts,
            }
            for repeat, seed in enumerate(random_seeds, start=1):
                print(
                    f"Running real {case.name}, seed {repeat}/{len(random_seeds)}",
                    flush=True,
                )
                run_result = run_seed(
                    source_session,
                    work_session,
                    case,
                    args,
                    seed,
                    run_sweeps=repeat == 1,
                    graph_counts=graph_counts,
                    physical_denormalization=physical,
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
            drop_catalog_objects(work_session)
            cleanup_feature_properties(work_session)
    return case_result


def run_experiment(driver: Any, args: argparse.Namespace) -> dict[str, Any]:
    with driver.session(database=args.database) as session:
        gds_version = check_gds(session)
    random_seeds = [args.seed + offset for offset in range(args.runs)]
    experiments = {
        case.name: run_case(driver, case, args, random_seeds)
        for case in CASES
    }
    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "database": args.database,
        "work_database": args.work_database,
        "experiment_mode": "physical_denormalization",
        "comparison_design": "same_work_database_before_and_after_fold",
        "pair_design": (
            "equal-sized same-parent and different-parent child pairs"
        ),
        "separation_definition": "same_parent_minus_different_parent",
        "source_database_access": "read_only",
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


def drop_work_database(driver: Any, database: str) -> None:
    with driver.session(database="system") as session:
        session.run(
            "DROP DATABASE $name IF EXISTS DESTROY DATA WAIT 60 SECONDS",
            name=database,
        ).consume()


def print_summary(result: dict[str, Any]) -> None:
    print(
        f"\nGDS={result['gds_version']}  "
        f"dimension={result['embedding_dimension']}"
    )
    for name, experiment in result["experiments"].items():
        runs = experiment["runs"]
        print(f"\n[{name}] physical denormalization")
        for method in runs[0]["similarities"]:
            values = {}
            for group in (
                "similarities",
                "negative_similarities",
                "separation",
            ):
                values[group] = {}
                for representation in ("normalized", "denormalized"):
                    medians = [
                        run[group][method][representation]["median"]
                        for run in runs
                    ]
                    values[group][representation] = (
                        fmean(medians),
                        stdev(medians) if len(medians) > 1 else 0.0,
                    )
            print(
                f"{method:<16} "
                f"same parent N "
                f"{values['similarities']['normalized'][0]:.6f} ± "
                f"{values['similarities']['normalized'][1]:.6f}, D "
                f"{values['similarities']['denormalized'][0]:.6f} ± "
                f"{values['similarities']['denormalized'][1]:.6f}"
            )
            print(
                f"{'':<16} different N "
                f"{values['negative_similarities']['normalized'][0]:.6f} ± "
                f"{values['negative_similarities']['normalized'][1]:.6f}, D "
                f"{values['negative_similarities']['denormalized'][0]:.6f} ± "
                f"{values['negative_similarities']['denormalized'][1]:.6f}"
            )
            print(
                f"{'':<16} separation N "
                f"{values['separation']['normalized'][0]:.6f} ± "
                f"{values['separation']['normalized'][1]:.6f}, D "
                f"{values['separation']['denormalized'][0]:.6f} ± "
                f"{values['separation']['denormalized'][1]:.6f}"
            )
    print(f"\nSaved: {OUTPUT_FILE}")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    result: dict[str, Any] | None = None
    try:
        driver.verify_connectivity()
        result = run_experiment(driver, args)
        OUTPUT_FILE.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    finally:
        if not args.keep_work_database:
            try:
                drop_work_database(driver, args.work_database)
            except Exception as exc:
                print(
                    f"Warning: could not drop work database "
                    f"{args.work_database!r}: {exc}"
                )
        driver.close()
    if result is None:
        raise RuntimeError("Experiment did not produce a result")
    print_summary(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
