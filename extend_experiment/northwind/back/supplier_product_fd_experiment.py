#!/usr/bin/env python3
"""Run the Northwind Supplier -> Product normalization experiment.

The experiment implements the requested workflow:

Step 0
    Fold the 11 non-key Supplier properties into every Product, add the
    ``Supplier_Products`` label, and remove ``SUPPLIED_BY`` relationships and
    Supplier nodes. This setup is not timed.

Step 1
    Measure logical redundancy caused by the functional dependency
    ``SupplierID -> Supplier attributes``. The report includes redundant
    supplier copies, theoretical property slots, non-null property values, and
    UTF-8 value payload. The byte figure is a logical payload estimate, not
    Neo4j store-file usage.

Step 2
    Apply the same 29 logical Supplier updates to the folded graph and time the
    single write transaction. Because Supplier data is repeated, the query must
    update 77 Product copies. Steps 2 and 4 deliberately use the same
    parameterized batch and RANGE-index hint. Every timed update is restored
    outside the timer.

Step 3
    Normalize the graph in one timed transaction: recreate one Supplier per
    distinct SupplierID, recreate 77 ``SUPPLIED_BY`` relationships, and remove
    the folded properties and label from Product. This measures the data
    rewrite, not schema creation/deletion; Step 0 leaves the original Supplier
    key constraint in place.

Step 4
    Apply the same logical updates to the normalized graph. This query must
    update only 29 Supplier nodes. It is also restored outside the timer.

The script finishes in the normalized state. It can be run again: Step 0 will
fold the restored graph again. If a failure occurs while the graph is folded,
the script makes a best-effort attempt to restore all Supplier values and
normalize it before exiting. An already folded starting state is rejected by
default to avoid silently resuming a stopped experiment. The 77 Product rows,
29 Supplier values, and their relevant data SHA-256 must match the bundled
case-study dataset before measurements begin.

Examples:

    python supplier_product_fd_experiment.py

    python supplier_product_fd_experiment.py \
        --warmup-runs 10 \
        --runs 100 \
        --normalization-runs 10 \
        --output-json results/supplier_product_fd.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from getpass import getpass
from pathlib import Path
from statistics import fmean, median, stdev
from time import perf_counter_ns
from typing import Any, Iterable, Sequence
from uuid import uuid4


DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_DATABASE = "northwind"
DEFAULT_WARMUP_RUNS = 5
DEFAULT_RUNS = 20
DEFAULT_NORMALIZATION_WARMUP_RUNS = 1
DEFAULT_NORMALIZATION_RUNS = 20
DEFAULT_INDEX_WAIT_SECONDS = 300
DEFAULT_UPDATE_PROPERTY = "Phone"
EXPECTED_PRODUCT_COUNT = 77
EXPECTED_SUPPLIER_COUNT = 29
EXPECTED_SUPPLIED_BY_COUNT = 77
EXPECTED_RELEVANT_DATA_SHA256 = (
    "614256e0ab65e7e5b6c1aa45178001d729697a63a9eea2b9bfc2bb7a948c6496"
)

# Local debugging only. Never commit a real password here.
DEBUG_NEO4J_PASSWORD: str | None = None

PRODUCT_LABEL = "Product"
SUPPLIER_LABEL = "Supplier"
FOLDED_LABEL = "Supplier_Products"
SUPPLIER_RELATIONSHIP = "SUPPLIED_BY"
SUPPLIER_ID_PROPERTY = "SupplierID"
FOLDED_SUPPLIER_ID_INDEX_PREFIX = "supplier_products_supplier_id_experiment"

# SupplierID remains on Product as the foreign-key property. Only these
# dependent attributes are copied during Step 0 and removed during Step 3.
SUPPLIER_ATTRIBUTES = (
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
    "HomePage",
)

# These attributes are non-null for all 29 Suppliers in the current dataset,
# so a same-length reversible string update can be used without changing the
# number of stored properties.
UPDATE_PROPERTIES = (
    "CompanyName",
    "ContactName",
    "ContactTitle",
    "Address",
    "City",
    "PostalCode",
    "Country",
    "Phone",
)


class ExperimentStateError(RuntimeError):
    """Raised when the graph is not in a safe normalized/folded state."""


class ExperimentValidationError(RuntimeError):
    """Raised when a transformation or measured update has a wrong result."""


@dataclass(frozen=True)
class GraphState:
    product_nodes: int
    folded_product_nodes: int
    supplier_nodes: int
    supplied_by_relationships: int

    @property
    def phase(self) -> str:
        if (
            self.product_nodes > 0
            and self.folded_product_nodes == 0
            and self.supplier_nodes > 0
            and self.supplied_by_relationships == self.product_nodes
        ):
            return "normalized"
        if (
            self.product_nodes > 0
            and self.folded_product_nodes == self.product_nodes
            and self.supplier_nodes == 0
            and self.supplied_by_relationships == 0
        ):
            return "folded"
        return "inconsistent"


@dataclass(frozen=True)
class TimingSample:
    run: int
    client_wall_ms: float
    available_after_ms: int | None
    consumed_after_ms: int | None
    neo4j_reported_total_ms: int | None
    affected_nodes: int
    properties_set: int
    nodes_created: int
    relationships_created: int
    labels_added: int
    labels_removed: int


@dataclass
class IndexLease:
    """Track a reusable or experiment-owned folded-side RANGE index."""

    name: str | None = None
    created_by_experiment: bool = False


def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def nonnegative_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if value < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure Supplier functional-dependency redundancy, update "
            "amplification, and normalization cost in Northwind."
        )
    )
    parser.add_argument(
        "--uri",
        default=os.getenv("NEO4J_URI", DEFAULT_URI),
        help=f"Neo4j URI (default: NEO4J_URI or {DEFAULT_URI})",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("NEO4J_USER", DEFAULT_USER),
        help=f"Neo4j user (default: NEO4J_USER or {DEFAULT_USER})",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("NEO4J_DATABASE") or DEFAULT_DATABASE,
        help=(
            "existing imported Northwind database "
            f"(default: NEO4J_DATABASE or {DEFAULT_DATABASE})"
        ),
    )
    parser.add_argument(
        "--update-property",
        choices=UPDATE_PROPERTIES,
        default=DEFAULT_UPDATE_PROPERTY,
        help=(
            "Supplier attribute used by Steps 2 and 4 "
            f"(default: {DEFAULT_UPDATE_PROPERTY})"
        ),
    )
    parser.add_argument(
        "--warmup-runs",
        type=nonnegative_int,
        default=DEFAULT_WARMUP_RUNS,
        help=(
            "unmeasured update/restore pairs before each update benchmark "
            f"(default: {DEFAULT_WARMUP_RUNS})"
        ),
    )
    parser.add_argument(
        "--runs",
        type=positive_int,
        default=DEFAULT_RUNS,
        help=f"measured update/restore pairs per graph state (default: {DEFAULT_RUNS})",
    )
    parser.add_argument(
        "--normalization-runs",
        type=positive_int,
        default=DEFAULT_NORMALIZATION_RUNS,
        help=(
            "measured Step 3 repetitions; Step 0 is rebuilt outside the "
            f"timer between runs (default: {DEFAULT_NORMALIZATION_RUNS})"
        ),
    )
    parser.add_argument(
        "--normalization-warmup-runs",
        type=nonnegative_int,
        default=DEFAULT_NORMALIZATION_WARMUP_RUNS,
        help=(
            "unmeasured normalize/refold cycles before Step 3 samples "
            f"(default: {DEFAULT_NORMALIZATION_WARMUP_RUNS})"
        ),
    )
    parser.add_argument(
        "--index-wait-seconds",
        type=positive_int,
        default=DEFAULT_INDEX_WAIT_SECONDS,
        help=(
            "maximum wait for the folded SupplierID index "
            f"(default: {DEFAULT_INDEX_WAIT_SECONDS})"
        ),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="optional path for raw samples and all derived metrics",
    )
    parser.add_argument(
        "--allow-existing-folded",
        action="store_true",
        help=(
            "accept an already folded graph as the baseline; use only when "
            "its Supplier values are known to be trustworthy"
        ),
    )
    return parser.parse_args(argv)


def result_records_and_summary(result: Any) -> tuple[list[Any], Any]:
    records = list(result)
    summary = result.consume()
    return records, summary


def one_record(result: Any, context: str) -> tuple[Any, Any]:
    records, summary = result_records_and_summary(result)
    if len(records) != 1:
        raise RuntimeError(f"{context}: expected one record, found {len(records)}")
    return records[0], summary


def scalar_count(session: Any, query: str, field: str, context: str) -> int:
    record, _ = one_record(session.run(query), context)
    return int(record[field])


def inspect_graph_state(session: Any) -> GraphState:
    product_nodes = scalar_count(
        session,
        f"MATCH (n:{quote_ident(PRODUCT_LABEL)}) RETURN count(n) AS count",
        "count",
        "count Product",
    )
    folded_record, _ = one_record(
        session.run(
            """
            MATCH (n)
            WHERE $folded_label IN labels(n)
            RETURN count(n) AS count
            """,
            folded_label=FOLDED_LABEL,
        ),
        "count folded Product",
    )
    folded_product_nodes = int(folded_record["count"])
    supplier_nodes = scalar_count(
        session,
        f"MATCH (n:{quote_ident(SUPPLIER_LABEL)}) RETURN count(n) AS count",
        "count",
        "count Supplier",
    )
    supplied_by_relationships = scalar_count(
        session,
        (
            f"MATCH (:{quote_ident(PRODUCT_LABEL)})"
            f"-[r:{quote_ident(SUPPLIER_RELATIONSHIP)}]->"
            f"(:{quote_ident(SUPPLIER_LABEL)}) "
            "RETURN count(r) AS count"
        ),
        "count",
        "count SUPPLIED_BY",
    )
    return GraphState(
        product_nodes=product_nodes,
        folded_product_nodes=folded_product_nodes,
        supplier_nodes=supplier_nodes,
        supplied_by_relationships=supplied_by_relationships,
    )


def folded_property_predicate(alias: str) -> str:
    return " OR ".join(
        f"{alias}.{quote_ident(attribute)} IS NOT NULL"
        for attribute in SUPPLIER_ATTRIBUTES
    )


def validate_normalized_state(session: Any) -> GraphState:
    state = inspect_graph_state(session)
    if state.phase != "normalized":
        raise ExperimentStateError(
            f"expected a normalized graph, found {state.phase}: {state}"
        )

    record, _ = one_record(
        session.run(
            f"""
            MATCH (p:{quote_ident(PRODUCT_LABEL)})
            OPTIONAL MATCH
              (p)-[r:{quote_ident(SUPPLIER_RELATIONSHIP)}]->
              (s:{quote_ident(SUPPLIER_LABEL)})
            WITH p, collect(r) AS relationships, collect(s) AS suppliers
            RETURN
              count(p) AS product_count,
              count(DISTINCT p.{quote_ident(SUPPLIER_ID_PROPERTY)})
                AS distinct_supplier_ids,
              sum(
                CASE WHEN size(relationships) = 1 THEN 0 ELSE 1 END
              ) AS invalid_degrees,
              sum(
                CASE
                  WHEN size(suppliers) = 1
                   AND suppliers[0].{quote_ident(SUPPLIER_ID_PROPERTY)}
                       = p.{quote_ident(SUPPLIER_ID_PROPERTY)}
                  THEN 0
                  ELSE 1
                END
              ) AS mismatched_ids,
              sum(
                CASE WHEN {folded_property_predicate("p")} THEN 1 ELSE 0 END
              ) AS products_with_folded_properties
            """
        ),
        "validate normalized Product relationships",
    )

    problems: list[str] = []
    if int(record["invalid_degrees"]) != 0:
        problems.append(
            f"{int(record['invalid_degrees'])} Product nodes do not have "
            "exactly one Supplier"
        )
    if int(record["mismatched_ids"]) != 0:
        problems.append(
            f"{int(record['mismatched_ids'])} Product.SupplierID values "
            "disagree with their Supplier nodes"
        )
    if int(record["products_with_folded_properties"]) != 0:
        problems.append(
            f"{int(record['products_with_folded_properties'])} Product nodes "
            "still contain folded Supplier properties"
        )
    distinct_supplier_ids = int(record["distinct_supplier_ids"])
    if distinct_supplier_ids != state.supplier_nodes:
        problems.append(
            f"distinct Product.SupplierID={distinct_supplier_ids}, "
            f"Supplier nodes={state.supplier_nodes}"
        )

    adjacent_supplier_relationships = scalar_count(
        session,
        (f"MATCH (s:{quote_ident(SUPPLIER_LABEL)})-[r]-() RETURN count(r) AS count"),
        "count",
        "count all Supplier relationships",
    )
    if adjacent_supplier_relationships != state.supplied_by_relationships:
        problems.append(
            "Supplier nodes have relationships outside the expected "
            f"{SUPPLIER_RELATIONSHIP} set"
        )

    supplier_shape, _ = one_record(
        session.run(
            f"""
            MATCH (s:{quote_ident(SUPPLIER_LABEL)})
            WITH
              s,
              [
                label IN labels(s)
                WHERE label <> $supplier_label
              ] AS extra_labels,
              [
                property IN keys(s)
                WHERE NOT (property IN $allowed_properties)
              ] AS extra_properties
            RETURN
              sum(
                CASE WHEN size(extra_labels) = 0 THEN 0 ELSE 1 END
              ) AS suppliers_with_extra_labels,
              sum(
                CASE WHEN size(extra_properties) = 0 THEN 0 ELSE 1 END
              ) AS suppliers_with_extra_properties
            """,
            supplier_label=SUPPLIER_LABEL,
            allowed_properties=[
                SUPPLIER_ID_PROPERTY,
                *SUPPLIER_ATTRIBUTES,
            ],
        ),
        "validate Supplier labels and properties",
    )
    if int(supplier_shape["suppliers_with_extra_labels"]) != 0:
        problems.append(
            f"{int(supplier_shape['suppliers_with_extra_labels'])} Supplier "
            "nodes have extra labels that Step 0 cannot preserve"
        )
    if int(supplier_shape["suppliers_with_extra_properties"]) != 0:
        problems.append(
            f"{int(supplier_shape['suppliers_with_extra_properties'])} "
            "Supplier nodes have extra properties that Step 0 cannot preserve"
        )

    relationship_properties = scalar_count(
        session,
        (
            f"MATCH (:{quote_ident(PRODUCT_LABEL)})"
            f"-[r:{quote_ident(SUPPLIER_RELATIONSHIP)}]->"
            f"(:{quote_ident(SUPPLIER_LABEL)}) "
            "WHERE size(keys(r)) > 0 "
            "RETURN count(r) AS count"
        ),
        "count",
        "count SUPPLIED_BY relationships with properties",
    )
    if relationship_properties:
        problems.append(
            f"{relationship_properties} {SUPPLIER_RELATIONSHIP} "
            "relationships have properties that Step 0 cannot preserve"
        )

    if problems:
        raise ExperimentValidationError(
            "Normalized graph validation failed:\n  - " + "\n  - ".join(problems)
        )
    return state


def validate_folded_state(session: Any) -> GraphState:
    state = inspect_graph_state(session)
    if state.phase != "folded":
        raise ExperimentStateError(
            f"expected a folded graph, found {state.phase}: {state}"
        )

    null_supplier_ids = scalar_count(
        session,
        (
            f"MATCH (p:{quote_ident(FOLDED_LABEL)}) "
            f"WHERE p.{quote_ident(SUPPLIER_ID_PROPERTY)} IS NULL "
            "RETURN count(p) AS count"
        ),
        "count",
        "count folded Product nodes without SupplierID",
    )
    if null_supplier_ids:
        raise ExperimentValidationError(
            f"{null_supplier_ids} folded Product nodes have no SupplierID"
        )

    product_only = scalar_count(
        session,
        (
            f"MATCH (p:{quote_ident(PRODUCT_LABEL)}) "
            f"WHERE NOT (p:{quote_ident(FOLDED_LABEL)}) "
            "RETURN count(p) AS count"
        ),
        "count",
        "count Product nodes without the folded label",
    )
    folded_only = scalar_count(
        session,
        (
            f"MATCH (p:{quote_ident(FOLDED_LABEL)}) "
            f"WHERE NOT (p:{quote_ident(PRODUCT_LABEL)}) "
            "RETURN count(p) AS count"
        ),
        "count",
        "count folded nodes without the Product label",
    )
    if product_only or folded_only:
        raise ExperimentValidationError(
            "folded labels do not identify exactly the Product node set: "
            f"Product-only={product_only}, folded-only={folded_only}"
        )
    return state


def transaction_record(
    transaction: Any,
    query: str,
    context: str,
) -> Any:
    record, _ = one_record(transaction.run(query), context)
    return record


def supplier_copy_set_clause(product_alias: str, supplier_alias: str) -> str:
    assignments = ",\n        ".join(
        (
            f"{product_alias}.{quote_ident(attribute)} = "
            f"{supplier_alias}.{quote_ident(attribute)}"
        )
        for attribute in SUPPLIER_ATTRIBUTES
    )
    return f"SET {product_alias}:{quote_ident(FOLDED_LABEL)},\n        {assignments}"


def fold_supplier_into_products(session: Any) -> tuple[int, int, int]:
    """Perform Step 0 atomically. This function is intentionally not timed."""

    normalized = validate_normalized_state(session)
    transaction = session.begin_transaction()
    try:
        copied = transaction_record(
            transaction,
            f"""
            MATCH
              (p:{quote_ident(PRODUCT_LABEL)})
              -[:{quote_ident(SUPPLIER_RELATIONSHIP)}]->
              (s:{quote_ident(SUPPLIER_LABEL)})
            {supplier_copy_set_clause("p", "s")}
            RETURN
              count(p) AS product_count,
              count(DISTINCT s) AS supplier_count
            """,
            "fold Supplier properties",
        )
        deleted_relationships = transaction_record(
            transaction,
            f"""
            MATCH
              (:{quote_ident(PRODUCT_LABEL)})
              -[r:{quote_ident(SUPPLIER_RELATIONSHIP)}]->
              (:{quote_ident(SUPPLIER_LABEL)})
            WITH collect(r) AS relationships
            FOREACH (relationship IN relationships | DELETE relationship)
            RETURN size(relationships) AS relationship_count
            """,
            "delete SUPPLIED_BY relationships",
        )
        deleted_suppliers = transaction_record(
            transaction,
            f"""
            MATCH (s:{quote_ident(SUPPLIER_LABEL)})
            WITH collect(s) AS suppliers
            FOREACH (supplier IN suppliers | DELETE supplier)
            RETURN size(suppliers) AS supplier_count
            """,
            "delete folded Supplier nodes",
        )
        transaction.commit()
    except BaseException:
        transaction.rollback()
        raise

    product_count = int(copied["product_count"])
    supplier_count = int(copied["supplier_count"])
    relationship_count = int(deleted_relationships["relationship_count"])
    deleted_supplier_count = int(deleted_suppliers["supplier_count"])
    if (
        product_count != normalized.product_nodes
        or supplier_count != normalized.supplier_nodes
        or relationship_count != normalized.supplied_by_relationships
        or deleted_supplier_count != normalized.supplier_nodes
    ):
        raise ExperimentValidationError(
            "Step 0 counters did not match the normalized preflight: "
            f"products={product_count}, suppliers={supplier_count}, "
            f"relationships={relationship_count}, "
            f"deleted_suppliers={deleted_supplier_count}"
        )

    validate_folded_state(session)
    return product_count, supplier_count, relationship_count


def matching_range_indexes(
    session: Any,
    label: str,
    property_name: str,
) -> list[dict[str, str]]:
    result = session.run(
        """
        SHOW INDEXES
        YIELD name, state, type, entityType, labelsOrTypes, properties
        RETURN name, state, type, entityType, labelsOrTypes, properties
        """
    )
    records, _ = result_records_and_summary(result)
    matches: list[dict[str, str]] = []
    for record in records:
        labels = tuple(str(value) for value in record["labelsOrTypes"] or ())
        properties = tuple(str(value) for value in record["properties"] or ())
        if (
            str(record["type"]).upper() == "RANGE"
            and str(record["entityType"]).upper() == "NODE"
            and labels == (label,)
            and properties == (property_name,)
        ):
            matches.append(
                {
                    "name": str(record["name"]),
                    "state": str(record["state"]).upper(),
                }
            )
    return sorted(
        matches,
        key=lambda index: (
            index["state"] != "ONLINE",
            index["name"],
        ),
    )


def await_and_validate_range_index(
    session: Any,
    *,
    index_name: str,
    label: str,
    property_name: str,
    index_wait_seconds: int,
) -> None:
    session.run(
        "CALL db.awaitIndex($index_name, $timeout_seconds)",
        index_name=index_name,
        timeout_seconds=index_wait_seconds,
    ).consume()
    matches = matching_range_indexes(session, label, property_name)
    matching_names = {index["name"]: index["state"] for index in matches}
    state = matching_names.get(index_name)
    if state != "ONLINE":
        raise ExperimentValidationError(
            f"RANGE index {index_name!r} for "
            f"{label}({property_name}) is not ONLINE; state={state!r}"
        )


def require_range_index(
    session: Any,
    *,
    label: str,
    property_name: str,
    index_wait_seconds: int,
) -> str:
    matches = matching_range_indexes(session, label, property_name)
    if not matches:
        raise ExperimentValidationError(
            f"no RANGE index exists for {label}({property_name}); "
            "run case_study/data/import_data.py to create the expected "
            "Supplier key constraint"
        )
    index_name = matches[0]["name"]
    await_and_validate_range_index(
        session,
        index_name=index_name,
        label=label,
        property_name=property_name,
        index_wait_seconds=index_wait_seconds,
    )
    return index_name


def ensure_folded_supplier_index(
    session: Any,
    index_wait_seconds: int,
    lease: IndexLease,
) -> IndexLease:
    existing = matching_range_indexes(
        session,
        FOLDED_LABEL,
        SUPPLIER_ID_PROPERTY,
    )
    if existing:
        lease.name = existing[0]["name"]
        lease.created_by_experiment = False
    else:
        lease.name = f"{FOLDED_SUPPLIER_ID_INDEX_PREFIX}_{uuid4().hex[:12]}"
        # Set ownership before CREATE so failure recovery can safely drop this
        # unique name even if the server committed before the driver errored.
        lease.created_by_experiment = True
        session.run(
            f"""
            CREATE INDEX {quote_ident(lease.name)}
            FOR (p:{quote_ident(FOLDED_LABEL)})
            ON (p.{quote_ident(SUPPLIER_ID_PROPERTY)})
            """
        ).consume()

    await_and_validate_range_index(
        session,
        index_name=lease.name,
        label=FOLDED_LABEL,
        property_name=SUPPLIER_ID_PROPERTY,
        index_wait_seconds=index_wait_seconds,
    )
    return lease


def drop_folded_supplier_index(
    session: Any,
    lease: IndexLease,
) -> None:
    if not lease.created_by_experiment or lease.name is None:
        return
    session.run(f"DROP INDEX {quote_ident(lease.name)} IF EXISTS").consume()


def folded_rows_query() -> str:
    projections = [
        f"p.{quote_ident('ProductID')} AS {quote_ident('ProductID')}",
        (
            f"p.{quote_ident(SUPPLIER_ID_PROPERTY)} "
            f"AS {quote_ident(SUPPLIER_ID_PROPERTY)}"
        ),
    ]
    projections.extend(
        f"p.{quote_ident(attribute)} AS {quote_ident(attribute)}"
        for attribute in SUPPLIER_ATTRIBUTES
    )
    return (
        f"MATCH (p:{quote_ident(FOLDED_LABEL)})\n"
        "RETURN\n  "
        + ",\n  ".join(projections)
        + f"\nORDER BY p.{quote_ident('ProductID')}"
    )


def fetch_folded_rows(session: Any) -> list[dict[str, Any]]:
    result = session.run(folded_rows_query())
    records, _ = result_records_and_summary(result)
    fields = ("ProductID", SUPPLIER_ID_PROPERTY, *SUPPLIER_ATTRIBUTES)
    return [{field: record[field] for field in fields} for record in records]


def fetch_normalized_join_rows(session: Any) -> list[dict[str, Any]]:
    projections = [
        f"p.{quote_ident('ProductID')} AS {quote_ident('ProductID')}",
        (
            f"p.{quote_ident(SUPPLIER_ID_PROPERTY)} "
            f"AS {quote_ident(SUPPLIER_ID_PROPERTY)}"
        ),
    ]
    projections.extend(
        f"s.{quote_ident(attribute)} AS {quote_ident(attribute)}"
        for attribute in SUPPLIER_ATTRIBUTES
    )
    result = session.run(
        f"""
        MATCH
          (p:{quote_ident(PRODUCT_LABEL)})
          -[:{quote_ident(SUPPLIER_RELATIONSHIP)}]->
          (s:{quote_ident(SUPPLIER_LABEL)})
        RETURN
          {", ".join(projections)}
        ORDER BY p.{quote_ident("ProductID")}
        """
    )
    records, _ = result_records_and_summary(result)
    fields = ("ProductID", SUPPLIER_ID_PROPERTY, *SUPPLIER_ATTRIBUTES)
    return [{field: record[field] for field in fields} for record in records]


def relevant_data_sha256(rows: Sequence[dict[str, Any]]) -> str:
    fields = ("ProductID", SUPPLIER_ID_PROPERTY, *SUPPLIER_ATTRIBUTES)
    canonical = [
        {field: row[field] for field in fields}
        for row in sorted(rows, key=lambda row: row["ProductID"])
    ]
    payload = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def validate_case_study_dataset(
    rows: Sequence[dict[str, Any]],
    metrics: dict[str, Any] | None = None,
) -> str:
    if metrics is None:
        metrics = calculate_redundancy(rows)
    fingerprint = relevant_data_sha256(rows)
    problems: list[str] = []
    if metrics["product_occurrences"] != EXPECTED_PRODUCT_COUNT:
        problems.append(
            f"Product count={metrics['product_occurrences']}, "
            f"expected {EXPECTED_PRODUCT_COUNT}"
        )
    if metrics["distinct_suppliers"] != EXPECTED_SUPPLIER_COUNT:
        problems.append(
            f"Supplier count={metrics['distinct_suppliers']}, "
            f"expected {EXPECTED_SUPPLIER_COUNT}"
        )
    if fingerprint != EXPECTED_RELEVANT_DATA_SHA256:
        problems.append(
            f"relevant-data SHA-256={fingerprint}, "
            f"expected {EXPECTED_RELEVANT_DATA_SHA256}"
        )
    if problems:
        raise ExperimentValidationError(
            "database does not match case_study/data/dataset Products and "
            "Suppliers:\n  - " + "\n  - ".join(problems)
        )
    return fingerprint


def canonical_supplier_snapshot(
    rows: Sequence[dict[str, Any]],
    *,
    require_one_row_per_supplier: bool = False,
) -> dict[Any, dict[str, Any]]:
    groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        supplier_id = row[SUPPLIER_ID_PROPERTY]
        if supplier_id is None:
            raise ExperimentValidationError("SupplierID cannot be NULL")
        groups[supplier_id].append(row)

    snapshot: dict[Any, dict[str, Any]] = {}
    for supplier_id, copies in groups.items():
        if require_one_row_per_supplier and len(copies) != 1:
            raise ExperimentValidationError(
                f"SupplierID={supplier_id!r} appears in {len(copies)} "
                "normalized Supplier nodes"
            )
        attributes: dict[str, Any] = {}
        for attribute in SUPPLIER_ATTRIBUTES:
            values = {copy[attribute] for copy in copies}
            if len(values) != 1:
                raise ExperimentValidationError(
                    "functional dependency violation: "
                    f"SupplierID={supplier_id!r} has multiple values for "
                    f"{attribute}: {values!r}"
                )
            attributes[attribute] = next(iter(values))
        snapshot[supplier_id] = attributes
    return snapshot


def fetch_normalized_supplier_snapshot(
    session: Any,
) -> dict[Any, dict[str, Any]]:
    projections = [
        (
            f"s.{quote_ident(SUPPLIER_ID_PROPERTY)} "
            f"AS {quote_ident(SUPPLIER_ID_PROPERTY)}"
        ),
        *(
            f"s.{quote_ident(attribute)} AS {quote_ident(attribute)}"
            for attribute in SUPPLIER_ATTRIBUTES
        ),
    ]
    result = session.run(
        f"MATCH (s:{quote_ident(SUPPLIER_LABEL)})\n"
        "RETURN\n  "
        + ",\n  ".join(projections)
        + f"\nORDER BY s.{quote_ident(SUPPLIER_ID_PROPERTY)}"
    )
    records, _ = result_records_and_summary(result)
    fields = (SUPPLIER_ID_PROPERTY, *SUPPLIER_ATTRIBUTES)
    rows = [{field: record[field] for field in fields} for record in records]
    return canonical_supplier_snapshot(
        rows,
        require_one_row_per_supplier=True,
    )


def verify_supplier_snapshot(
    expected: dict[Any, dict[str, Any]],
    actual: dict[Any, dict[str, Any]],
    context: str,
) -> None:
    if expected == actual:
        return

    expected_ids = set(expected)
    actual_ids = set(actual)
    problems: list[str] = []
    missing = sorted(expected_ids - actual_ids)
    extra = sorted(actual_ids - expected_ids)
    if missing:
        problems.append(f"missing SupplierID values: {missing!r}")
    if extra:
        problems.append(f"extra SupplierID values: {extra!r}")
    for supplier_id in sorted(expected_ids & actual_ids):
        for attribute in SUPPLIER_ATTRIBUTES:
            expected_value = expected[supplier_id][attribute]
            actual_value = actual[supplier_id][attribute]
            if expected_value != actual_value:
                problems.append(
                    f"SupplierID={supplier_id!r} {attribute}: expected "
                    f"{expected_value!r}, found {actual_value!r}"
                )
                if len(problems) >= 10:
                    problems.append("additional differences omitted")
                    break
        if len(problems) >= 10:
            break
    raise ExperimentValidationError(
        f"{context} Supplier snapshot mismatch:\n  - " + "\n  - ".join(problems)
    )


def value_payload_bytes(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    return len(str(value).encode("utf-8"))


def calculate_redundancy(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ExperimentValidationError("the folded graph has no Product rows")

    product_ids = [row["ProductID"] for row in rows]
    if len(product_ids) != len(set(product_ids)):
        raise ExperimentValidationError(
            "folded rows contain duplicate ProductID values"
        )

    groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        supplier_id = row[SUPPLIER_ID_PROPERTY]
        if supplier_id is None:
            raise ExperimentValidationError("folded Product has NULL SupplierID")
        groups[supplier_id].append(row)

    by_attribute: dict[str, dict[str, int]] = {}
    baseline_non_null_values = 0
    normalized_non_null_values = 0
    baseline_payload_bytes = 0
    normalized_payload_bytes = 0

    for attribute in SUPPLIER_ATTRIBUTES:
        attribute_baseline_values = 0
        attribute_normalized_values = 0
        attribute_baseline_bytes = 0
        attribute_normalized_bytes = 0

        for supplier_id, copies in groups.items():
            values = {copy[attribute] for copy in copies}
            if len(values) != 1:
                raise ExperimentValidationError(
                    "functional dependency violation: "
                    f"SupplierID={supplier_id!r} has multiple values for "
                    f"{attribute}: {values!r}"
                )
            value = next(iter(values))
            if value is None:
                continue

            attribute_baseline_values += len(copies)
            attribute_normalized_values += 1
            payload_bytes = value_payload_bytes(value)
            attribute_baseline_bytes += payload_bytes * len(copies)
            attribute_normalized_bytes += payload_bytes

        baseline_non_null_values += attribute_baseline_values
        normalized_non_null_values += attribute_normalized_values
        baseline_payload_bytes += attribute_baseline_bytes
        normalized_payload_bytes += attribute_normalized_bytes
        by_attribute[attribute] = {
            "folded_non_null_values": attribute_baseline_values,
            "normalized_non_null_values": attribute_normalized_values,
            "redundant_non_null_values": (
                attribute_baseline_values - attribute_normalized_values
            ),
            "folded_payload_bytes": attribute_baseline_bytes,
            "normalized_payload_bytes": attribute_normalized_bytes,
            "redundant_payload_bytes": (
                attribute_baseline_bytes - attribute_normalized_bytes
            ),
        }

    product_occurrences = len(rows)
    distinct_suppliers = len(groups)
    redundant_supplier_copies = product_occurrences - distinct_suppliers
    dependent_attribute_count = len(SUPPLIER_ATTRIBUTES)
    theoretical_folded_slots = product_occurrences * dependent_attribute_count
    theoretical_normalized_slots = distinct_suppliers * dependent_attribute_count
    redundant_non_null_values = baseline_non_null_values - normalized_non_null_values
    redundant_payload_bytes = baseline_payload_bytes - normalized_payload_bytes
    fanout_histogram = Counter(len(copies) for copies in groups.values())

    return {
        "functional_dependency": (
            f"{SUPPLIER_ID_PROPERTY} -> " + ", ".join(SUPPLIER_ATTRIBUTES)
        ),
        "dependent_attributes": list(SUPPLIER_ATTRIBUTES),
        "dependent_attribute_count": dependent_attribute_count,
        "product_occurrences": product_occurrences,
        "distinct_suppliers": distinct_suppliers,
        "average_products_per_supplier": (product_occurrences / distinct_suppliers),
        "redundant_supplier_copies": redundant_supplier_copies,
        "redundant_supplier_copy_percent": (
            100.0 * redundant_supplier_copies / product_occurrences
        ),
        "theoretical_folded_property_slots": theoretical_folded_slots,
        "theoretical_normalized_property_slots": theoretical_normalized_slots,
        "theoretical_redundant_property_slots": (
            theoretical_folded_slots - theoretical_normalized_slots
        ),
        "folded_non_null_dependent_values": baseline_non_null_values,
        "normalized_non_null_dependent_values": normalized_non_null_values,
        "redundant_non_null_dependent_values": redundant_non_null_values,
        "redundant_non_null_value_percent": (
            100.0 * redundant_non_null_values / baseline_non_null_values
            if baseline_non_null_values
            else 0.0
        ),
        "folded_value_payload_bytes": baseline_payload_bytes,
        "normalized_value_payload_bytes": normalized_payload_bytes,
        "redundant_value_payload_bytes": redundant_payload_bytes,
        "redundant_value_payload_percent": (
            100.0 * redundant_payload_bytes / baseline_payload_bytes
            if baseline_payload_bytes
            else 0.0
        ),
        "fanout_by_products_per_supplier": {
            str(fanout): supplier_count
            for fanout, supplier_count in sorted(fanout_histogram.items())
        },
        "copy_count_by_supplier": {
            str(supplier_id): len(copies)
            for supplier_id, copies in sorted(groups.items())
        },
        "by_attribute": by_attribute,
        "byte_metric_note": (
            "UTF-8 value payload only; excludes property keys, records, "
            "indexes, nodes, relationships, and Neo4j store overhead."
        ),
    }


def print_redundancy(metrics: dict[str, Any]) -> None:
    print(
        "  Product occurrences: "
        f"{metrics['product_occurrences']:,}; "
        f"distinct suppliers: {metrics['distinct_suppliers']:,}"
    )
    print(
        "  Redundant Supplier copies: "
        f"{metrics['redundant_supplier_copies']:,} "
        f"({metrics['redundant_supplier_copy_percent']:.3f}%)"
    )
    print(
        "  Theoretical redundant dependent-property slots: "
        f"{metrics['theoretical_redundant_property_slots']:,}"
    )
    print(
        "  Redundant non-null dependent values: "
        f"{metrics['redundant_non_null_dependent_values']:,} "
        f"({metrics['redundant_non_null_value_percent']:.3f}%)"
    )
    print(
        "  Redundant UTF-8 value payload: "
        f"{metrics['redundant_value_payload_bytes']:,} bytes "
        f"({metrics['redundant_value_payload_percent']:.3f}%)"
    )
    fanout = ", ".join(
        f"{products} product(s) -> {suppliers} supplier(s)"
        for products, suppliers in metrics["fanout_by_products_per_supplier"].items()
    )
    print(f"  Supplier fanout histogram: {fanout}")


def canonical_folded_values(
    rows: Sequence[dict[str, Any]],
    property_name: str,
) -> dict[Any, str]:
    values_by_supplier: dict[Any, set[Any]] = defaultdict(set)
    for row in rows:
        values_by_supplier[row[SUPPLIER_ID_PROPERTY]].add(row[property_name])

    canonical: dict[Any, str] = {}
    for supplier_id, values in values_by_supplier.items():
        if len(values) != 1:
            raise ExperimentValidationError(
                f"SupplierID={supplier_id!r} has inconsistent "
                f"{property_name}: {values!r}"
            )
        value = next(iter(values))
        if not isinstance(value, str) or len(value) < 2:
            raise ExperimentValidationError(
                f"{property_name} must be a non-null string of at least two "
                f"characters for SupplierID={supplier_id!r}; found {value!r}"
            )
        canonical[supplier_id] = value
    return canonical


def make_same_length_update(value: str) -> str:
    """Create a deterministic correction without changing UTF-8 byte length."""

    characters = list(value)
    for position in range(len(characters) - 1, -1, -1):
        character = characters[position]
        if character in "0123456789":
            characters[position] = str((int(character) + 1) % 10)
            break
    else:
        for position, character in enumerate(characters):
            if character.isascii() and character.isalpha():
                characters[position] = (
                    character.upper() if character.islower() else character.lower()
                )
                break
        else:
            raise ExperimentValidationError(
                f"cannot produce a same-length update for {value!r}"
            )

    updated = "".join(characters)
    if updated == value:
        raise ExperimentValidationError(
            f"cannot produce a different same-length update for {value!r}"
        )
    if len(updated.encode("utf-8")) != len(value.encode("utf-8")):
        raise ExperimentValidationError(
            "generated update changed the UTF-8 payload length"
        )
    return updated


def build_update_rows(
    originals: dict[Any, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    updates = [
        {
            SUPPLIER_ID_PROPERTY: supplier_id,
            "value": make_same_length_update(original),
        }
        for supplier_id, original in sorted(originals.items())
    ]
    restores = [
        {
            SUPPLIER_ID_PROPERTY: supplier_id,
            "value": original,
        }
        for supplier_id, original in sorted(originals.items())
    ]
    return updates, restores


def snapshot_parameter_rows(
    snapshot: dict[Any, dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            SUPPLIER_ID_PROPERTY: supplier_id,
            "attributes": attributes,
        }
        for supplier_id, attributes in sorted(snapshot.items())
    ]


def restore_supplier_snapshot(
    session: Any,
    *,
    label: str,
    snapshot: dict[Any, dict[str, Any]],
    expected_nodes: int,
) -> None:
    record, _ = one_record(
        session.run(
            f"""
            UNWIND $suppliers AS original
            MATCH (
              n:{quote_ident(label)}
              {{{quote_ident(SUPPLIER_ID_PROPERTY)}:
                original.{quote_ident(SUPPLIER_ID_PROPERTY)}}}
            )
            SET n += original.attributes
            RETURN count(n) AS restored_nodes
            """,
            suppliers=snapshot_parameter_rows(snapshot),
        ),
        f"restore complete Supplier snapshot on {label}",
    )
    restored_nodes = int(record["restored_nodes"])
    if restored_nodes != expected_nodes:
        raise ExperimentValidationError(
            f"restoring {label} matched {restored_nodes} nodes; "
            f"expected {expected_nodes}"
        )

    if label == FOLDED_LABEL:
        actual = canonical_supplier_snapshot(fetch_folded_rows(session))
    elif label == SUPPLIER_LABEL:
        actual = fetch_normalized_supplier_snapshot(session)
    else:
        raise ValueError(f"unsupported snapshot restore label: {label}")
    verify_supplier_snapshot(snapshot, actual, f"restore on {label}")


def update_query(label: str, property_name: str) -> str:
    return f"""
    UNWIND $updates AS update
    MATCH (
      n:{quote_ident(label)}
      {{{quote_ident(SUPPLIER_ID_PROPERTY)}:
        update.{quote_ident(SUPPLIER_ID_PROPERTY)}}}
    )
    USING INDEX n:{quote_ident(label)}({quote_ident(SUPPLIER_ID_PROPERTY)})
    SET n.{quote_ident(property_name)} = update.value
    RETURN count(n) AS updated_nodes
    """


def server_total_ms(summary: Any) -> int | None:
    available = summary.result_available_after
    consumed = summary.result_consumed_after
    if available is None or consumed is None:
        return None
    return int(available) + int(consumed)


def make_timing_sample(
    run: int,
    wall_ms: float,
    summary: Any,
    affected_nodes: int,
) -> TimingSample:
    counters = summary.counters
    return TimingSample(
        run=run,
        client_wall_ms=wall_ms,
        available_after_ms=summary.result_available_after,
        consumed_after_ms=summary.result_consumed_after,
        neo4j_reported_total_ms=server_total_ms(summary),
        affected_nodes=affected_nodes,
        properties_set=int(getattr(counters, "properties_set", 0)),
        nodes_created=int(getattr(counters, "nodes_created", 0)),
        relationships_created=int(getattr(counters, "relationships_created", 0)),
        labels_added=int(getattr(counters, "labels_added", 0)),
        labels_removed=int(getattr(counters, "labels_removed", 0)),
    )


def timed_query(
    session: Any,
    query: str,
    parameters: dict[str, Any],
    run: int,
    affected_field: str,
    context: str,
) -> TimingSample:
    started_ns = perf_counter_ns()
    result = session.run(query, parameters)
    records, summary = result_records_and_summary(result)
    wall_ms = (perf_counter_ns() - started_ns) / 1_000_000

    if len(records) != 1:
        raise ExperimentValidationError(
            f"{context}: expected one result record, found {len(records)}"
        )
    affected_nodes = int(records[0][affected_field])
    return make_timing_sample(
        run=run,
        wall_ms=wall_ms,
        summary=summary,
        affected_nodes=affected_nodes,
    )


def verify_current_values(
    session: Any,
    label: str,
    property_name: str,
    originals: dict[Any, str],
    expected_nodes: int,
) -> None:
    result = session.run(
        f"""
        MATCH (n:{quote_ident(label)})
        RETURN
          n.{quote_ident(SUPPLIER_ID_PROPERTY)} AS supplier_id,
          n.{quote_ident(property_name)} AS property_value
        """
    )
    records, _ = result_records_and_summary(result)
    if len(records) != expected_nodes:
        raise ExperimentValidationError(
            f"{label}: expected {expected_nodes} nodes while verifying "
            f"{property_name}, found {len(records)}"
        )
    for record in records:
        supplier_id = record["supplier_id"]
        expected = originals.get(supplier_id)
        if record["property_value"] != expected:
            raise ExperimentValidationError(
                f"{label} SupplierID={supplier_id!r} did not restore "
                f"{property_name}: expected {expected!r}, "
                f"found {record['property_value']!r}"
            )


def benchmark_updates(
    session: Any,
    *,
    phase_name: str,
    label: str,
    property_name: str,
    originals: dict[Any, str],
    expected_nodes: int,
    warmup_runs: int,
    measured_runs: int,
) -> list[TimingSample]:
    updates, restores = build_update_rows(originals)
    query = update_query(label, property_name)
    samples: list[TimingSample] = []
    total_runs = warmup_runs + measured_runs

    for ordinal in range(1, total_runs + 1):
        measured_number = ordinal - warmup_runs
        try:
            sample = timed_query(
                session,
                query,
                {"updates": updates},
                run=max(measured_number, 0),
                affected_field="updated_nodes",
                context=f"{phase_name} update",
            )
            if sample.affected_nodes != expected_nodes:
                raise ExperimentValidationError(
                    f"{phase_name}: expected to update {expected_nodes} nodes, "
                    f"updated {sample.affected_nodes}"
                )
            if sample.properties_set != expected_nodes:
                raise ExperimentValidationError(
                    f"{phase_name}: expected {expected_nodes} property writes, "
                    f"Neo4j reported {sample.properties_set}"
                )
        except BaseException:
            # An autocommit write can have committed even if the driver fails
            # while receiving the result summary. Restoring is therefore safe
            # and necessary after every attempted timed query.
            try:
                restore_result = session.run(query, updates=restores)
                one_record(
                    restore_result,
                    f"{phase_name} best-effort restore",
                )
            except BaseException as restore_error:
                print(
                    f"WARNING: {phase_name} in-session restore failed: "
                    f"{type(restore_error).__name__}: {restore_error}",
                    file=sys.stderr,
                )
            raise
        else:
            restore_result = session.run(query, updates=restores)
            restore_record, _ = one_record(
                restore_result,
                f"{phase_name} restore",
            )
            restored_nodes = int(restore_record["updated_nodes"])
            if restored_nodes != expected_nodes:
                raise ExperimentValidationError(
                    f"{phase_name}: restore matched {restored_nodes} nodes; "
                    f"expected {expected_nodes}"
                )
            if ordinal > warmup_runs:
                samples.append(sample)

    verify_current_values(
        session,
        label,
        property_name,
        originals,
        expected_nodes,
    )
    return samples


def percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def numeric_summary(values: Sequence[float]) -> dict[str, float | int]:
    if not values:
        raise ValueError("timing summary requires at least one value")
    return {
        "n": len(values),
        "mean": fmean(values),
        "median": median(values),
        "stdev": stdev(values) if len(values) > 1 else 0.0,
        "min": min(values),
        "p95": percentile(values, 0.95),
        "max": max(values),
    }


def timing_summary(samples: Sequence[TimingSample]) -> dict[str, Any]:
    client_values = [sample.client_wall_ms for sample in samples]
    server_values = [
        float(sample.neo4j_reported_total_ms)
        for sample in samples
        if sample.neo4j_reported_total_ms is not None
    ]
    return {
        "client_wall_ms": numeric_summary(client_values),
        "neo4j_reported_total_ms": (
            numeric_summary(server_values) if server_values else None
        ),
        "affected_nodes": sorted({sample.affected_nodes for sample in samples}),
        "properties_set": sorted({sample.properties_set for sample in samples}),
    }


def print_timing_summary(
    title: str,
    samples: Sequence[TimingSample],
) -> None:
    summary = timing_summary(samples)
    client = summary["client_wall_ms"]
    server = summary["neo4j_reported_total_ms"]
    server_text = (
        "N/A"
        if server is None
        else (f"median={server['median']:.3f} ms, mean={server['mean']:.3f} ms")
    )
    print(
        f"  {title}: n={client['n']}, "
        f"client median={client['median']:.3f} ms, "
        f"mean={client['mean']:.3f} ms, "
        f"p95={client['p95']:.3f} ms; "
        f"Neo4j total {server_text}"
    )


def supplier_property_map(source_alias: str) -> str:
    assignments = ", ".join(
        f"{quote_ident(attribute)}: {source_alias}.{quote_ident(attribute)}"
        for attribute in SUPPLIER_ATTRIBUTES
    )
    return "{" + assignments + "}"


def normalization_query() -> str:
    remove_items = [f"p.{quote_ident(attribute)}" for attribute in SUPPLIER_ATTRIBUTES]
    remove_items.append(f"p:{quote_ident(FOLDED_LABEL)}")
    return f"""
    MATCH (folded:{quote_ident(FOLDED_LABEL)})
    WITH
      folded.{quote_ident(SUPPLIER_ID_PROPERTY)} AS supplier_id,
      head(collect(folded)) AS representative
    MERGE (
      supplier:{quote_ident(SUPPLIER_LABEL)}
      {{{quote_ident(SUPPLIER_ID_PROPERTY)}: supplier_id}}
    )
    SET supplier += {supplier_property_map("representative")}
    WITH count(supplier) AS supplier_count
    MATCH (p:{quote_ident(FOLDED_LABEL)})
    MATCH (
      s:{quote_ident(SUPPLIER_LABEL)}
      {{{quote_ident(SUPPLIER_ID_PROPERTY)}:
        p.{quote_ident(SUPPLIER_ID_PROPERTY)}}}
    )
    USING INDEX s:{quote_ident(SUPPLIER_LABEL)}(
      {quote_ident(SUPPLIER_ID_PROPERTY)})
    MERGE (p)-[:{quote_ident(SUPPLIER_RELATIONSHIP)}]->(s)
    REMOVE {", ".join(remove_items)}
    RETURN supplier_count, count(p) AS product_count
    """


def normalize_once(
    session: Any,
    run: int,
    expected_products: int,
    expected_suppliers: int,
) -> TimingSample:
    sample = timed_query(
        session,
        normalization_query(),
        {},
        run=run,
        affected_field="product_count",
        context="Step 3 normalization",
    )

    # Verify the Supplier total separately, outside the timed transaction.
    result = session.run(
        f"MATCH (s:{quote_ident(SUPPLIER_LABEL)}) RETURN count(s) AS supplier_count"
    )
    record, _ = one_record(result, "count normalized Supplier nodes")
    supplier_count = int(record["supplier_count"])

    if sample.affected_nodes != expected_products:
        raise ExperimentValidationError(
            f"Step 3 normalized {sample.affected_nodes} Product nodes; "
            f"expected {expected_products}"
        )
    if supplier_count != expected_suppliers:
        raise ExperimentValidationError(
            f"Step 3 created/restored {supplier_count} Supplier nodes; "
            f"expected {expected_suppliers}"
        )
    if sample.nodes_created != expected_suppliers:
        raise ExperimentValidationError(
            f"Step 3 Neo4j counter reported {sample.nodes_created} new "
            f"Supplier nodes; expected {expected_suppliers}"
        )
    if sample.relationships_created != expected_products:
        raise ExperimentValidationError(
            f"Step 3 Neo4j counter reported "
            f"{sample.relationships_created} relationships; "
            f"expected {expected_products}"
        )

    validate_normalized_state(session)
    return sample


def samples_as_dicts(
    samples: Iterable[TimingSample],
) -> list[dict[str, Any]]:
    return [asdict(sample) for sample in samples]


def collect_environment_metadata(
    session: Any,
    driver: Any,
    driver_version: str,
) -> dict[str, Any]:
    server_info = driver.get_server_info()
    metadata: dict[str, Any] = {
        "python_version": platform.python_version(),
        "neo4j_driver_version": driver_version,
        "server_agent": str(server_info.agent),
        "bolt_protocol_version": str(server_info.protocol_version),
        "server_address": str(server_info.address),
        "cypher_runtime": "server default",
    }
    try:
        result = session.run(
            """
            CALL dbms.components()
            YIELD name, versions, edition
            RETURN name, versions, edition
            """
        )
        records, _ = result_records_and_summary(result)
        metadata["dbms_components"] = [
            {
                "name": str(record["name"]),
                "versions": [str(version) for version in record["versions"]],
                "edition": str(record["edition"]),
            }
            for record in records
        ]
    except Exception as exc:
        metadata["dbms_components_unavailable"] = f"{type(exc).__name__}: {exc}"
    return metadata


def derived_comparison(
    redundancy: dict[str, Any],
    denormalized_updates: Sequence[TimingSample],
    normalization_samples: Sequence[TimingSample],
    normalized_updates: Sequence[TimingSample],
) -> dict[str, Any]:
    denormalized_median = median(
        sample.client_wall_ms for sample in denormalized_updates
    )
    normalized_median = median(sample.client_wall_ms for sample in normalized_updates)
    normalization_median = median(
        sample.client_wall_ms for sample in normalization_samples
    )
    saved_per_update = denormalized_median - normalized_median
    return {
        "write_amplification_nodes": (
            redundancy["product_occurrences"] / redundancy["distinct_suppliers"]
        ),
        "denormalized_to_normalized_update_median_ratio": (
            denormalized_median / normalized_median if normalized_median > 0 else None
        ),
        "median_client_ms_saved_per_logical_update_batch": saved_per_update,
        "data_rewrite_normalization_break_even_update_batches": (
            normalization_median / saved_per_update if saved_per_update > 0 else None
        ),
    }


def recover_normalized_graph(
    driver: Any,
    database: str,
    index_lease: IndexLease,
    supplier_snapshot: dict[Any, dict[str, Any]] | None,
) -> None:
    """Best-effort structure and value recovery; timing is discarded."""

    from neo4j import WRITE_ACCESS

    with driver.session(
        database=database,
        default_access_mode=WRITE_ACCESS,
    ) as session:
        try:
            state = inspect_graph_state(session)
            if state.phase == "folded":
                validate_folded_state(session)
                if supplier_snapshot is not None:
                    restore_supplier_snapshot(
                        session,
                        label=FOLDED_LABEL,
                        snapshot=supplier_snapshot,
                        expected_nodes=state.product_nodes,
                    )
                rows = fetch_folded_rows(session)
                metrics = calculate_redundancy(rows)
                normalize_once(
                    session,
                    run=0,
                    expected_products=metrics["product_occurrences"],
                    expected_suppliers=metrics["distinct_suppliers"],
                )
            elif state.phase != "normalized":
                raise ExperimentStateError(
                    f"automatic recovery found an inconsistent graph: {state}"
                )

            normalized_state = validate_normalized_state(session)
            if supplier_snapshot is not None:
                restore_supplier_snapshot(
                    session,
                    label=SUPPLIER_LABEL,
                    snapshot=supplier_snapshot,
                    expected_nodes=normalized_state.supplier_nodes,
                )
        finally:
            drop_folded_supplier_index(session, index_lease)


def run_experiment(
    driver: Any,
    database: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    from neo4j import WRITE_ACCESS, __version__ as neo4j_driver_version

    started_at = datetime.now(timezone.utc)
    index_lease = IndexLease()
    supplier_snapshot: dict[Any, dict[str, Any]] | None = None
    normalized_supplier_index: str | None = None
    dataset_fingerprint: str | None = None
    environment_metadata: dict[str, Any] = {}
    recovery_required = False
    try:
        with driver.session(
            database=database,
            default_access_mode=WRITE_ACCESS,
        ) as session:
            environment_metadata = collect_environment_metadata(
                session,
                driver,
                neo4j_driver_version,
            )
            initial_state = inspect_graph_state(session)
            if initial_state.phase == "normalized":
                validate_normalized_state(session)
                normalized_supplier_index = require_range_index(
                    session,
                    label=SUPPLIER_LABEL,
                    property_name=SUPPLIER_ID_PROPERTY,
                    index_wait_seconds=args.index_wait_seconds,
                )
                if (
                    initial_state.supplied_by_relationships
                    != EXPECTED_SUPPLIED_BY_COUNT
                ):
                    raise ExperimentValidationError(
                        "SUPPLIED_BY count="
                        f"{initial_state.supplied_by_relationships}, expected "
                        f"{EXPECTED_SUPPLIED_BY_COUNT}"
                    )
                normalized_join_rows = fetch_normalized_join_rows(session)
                dataset_fingerprint = validate_case_study_dataset(normalized_join_rows)
                supplier_snapshot = fetch_normalized_supplier_snapshot(session)
                print("\nStep 0: folding Supplier into Product (not timed)")
                recovery_required = True
                product_count, supplier_count, relationship_count = (
                    fold_supplier_into_products(session)
                )
                print(
                    f"  Folded {supplier_count:,} suppliers into "
                    f"{product_count:,} products; removed "
                    f"{relationship_count:,} SUPPLIED_BY relationships"
                )
            elif initial_state.phase == "folded":
                if not args.allow_existing_folded:
                    raise ExperimentStateError(
                        "the database is already folded. Re-import/normalize "
                        "it first, or pass --allow-existing-folded only if "
                        "the current Supplier values are a trusted baseline"
                    )
                validate_folded_state(session)
                normalized_supplier_index = require_range_index(
                    session,
                    label=SUPPLIER_LABEL,
                    property_name=SUPPLIER_ID_PROPERTY,
                    index_wait_seconds=args.index_wait_seconds,
                )
                recovery_required = True
                print(
                    "\nStep 0: WARNING: reusing the explicitly accepted "
                    "folded state (not timed)"
                )
            else:
                raise ExperimentStateError(
                    f"cannot start from a partially transformed graph: {initial_state}"
                )

            ensure_folded_supplier_index(
                session,
                args.index_wait_seconds,
                index_lease,
            )
            folded_state = validate_folded_state(session)

            print("\nStep 1: measuring FD redundancy")
            folded_rows = fetch_folded_rows(session)
            redundancy = calculate_redundancy(folded_rows)
            folded_fingerprint = validate_case_study_dataset(
                folded_rows,
                redundancy,
            )
            if (
                dataset_fingerprint is not None
                and folded_fingerprint != dataset_fingerprint
            ):
                raise ExperimentValidationError(
                    "Step 0 changed the relevant dataset fingerprint"
                )
            dataset_fingerprint = folded_fingerprint
            print(f"  Dataset fingerprint verified: {dataset_fingerprint}")
            print_redundancy(redundancy)
            folded_supplier_snapshot = canonical_supplier_snapshot(folded_rows)
            if supplier_snapshot is None:
                supplier_snapshot = folded_supplier_snapshot
            else:
                verify_supplier_snapshot(
                    supplier_snapshot,
                    folded_supplier_snapshot,
                    "Step 0",
                )

            original_update_values = canonical_folded_values(
                folded_rows,
                args.update_property,
            )

            print(
                "\nStep 2: updating folded Supplier copies "
                f"({args.update_property}; "
                f"{args.warmup_runs} warmups, {args.runs} measured runs)"
            )
            print(
                f"  Batch: {len(original_update_values)} logical Supplier "
                "updates in one transaction; restore is outside the timer"
            )
            denormalized_update_samples = benchmark_updates(
                session,
                phase_name="folded update",
                label=FOLDED_LABEL,
                property_name=args.update_property,
                originals=original_update_values,
                expected_nodes=folded_state.product_nodes,
                warmup_runs=args.warmup_runs,
                measured_runs=args.runs,
            )
            print_timing_summary(
                "folded update",
                denormalized_update_samples,
            )
            verify_supplier_snapshot(
                supplier_snapshot,
                canonical_supplier_snapshot(fetch_folded_rows(session)),
                "Step 2 restore",
            )

            print(
                "\nStep 3: normalizing Supplier_Products "
                f"({args.normalization_warmup_runs} warmups, "
                f"{args.normalization_runs} measured runs)"
            )
            print("  Timing scope: data rewrite only; schema DDL is excluded")
            normalization_samples: list[TimingSample] = []
            normalization_total_runs = (
                args.normalization_warmup_runs + args.normalization_runs
            )
            for ordinal in range(1, normalization_total_runs + 1):
                measured_number = ordinal - args.normalization_warmup_runs
                sample = normalize_once(
                    session,
                    run=max(measured_number, 0),
                    expected_products=redundancy["product_occurrences"],
                    expected_suppliers=redundancy["distinct_suppliers"],
                )
                normalized_snapshot = fetch_normalized_supplier_snapshot(session)
                verify_supplier_snapshot(
                    supplier_snapshot,
                    normalized_snapshot,
                    "Step 3",
                )
                if ordinal > args.normalization_warmup_runs:
                    normalization_samples.append(sample)
                if ordinal < normalization_total_runs:
                    fold_supplier_into_products(session)
                    refolded_snapshot = canonical_supplier_snapshot(
                        fetch_folded_rows(session)
                    )
                    verify_supplier_snapshot(
                        supplier_snapshot,
                        refolded_snapshot,
                        "Step 3 repetition setup",
                    )
            print_timing_summary(
                "normalization",
                normalization_samples,
            )

            # The temporary index has no labeled nodes after normalization and
            # is not part of the normalized graph's schema.
            drop_folded_supplier_index(session, index_lease)
            normalized_state = validate_normalized_state(session)
            final_normalized_snapshot = fetch_normalized_supplier_snapshot(session)
            verify_supplier_snapshot(
                supplier_snapshot,
                final_normalized_snapshot,
                "final Step 3",
            )

            print(
                "\nStep 4: updating normalized Supplier nodes "
                f"({args.update_property}; "
                f"{args.warmup_runs} warmups, {args.runs} measured runs)"
            )
            print(
                f"  Batch: {len(original_update_values)} logical Supplier "
                "updates in one transaction; restore is outside the timer"
            )
            normalized_update_samples = benchmark_updates(
                session,
                phase_name="normalized update",
                label=SUPPLIER_LABEL,
                property_name=args.update_property,
                originals=original_update_values,
                expected_nodes=normalized_state.supplier_nodes,
                warmup_runs=args.warmup_runs,
                measured_runs=args.runs,
            )
            print_timing_summary(
                "normalized update",
                normalized_update_samples,
            )
            final_state = validate_normalized_state(session)
            verify_supplier_snapshot(
                supplier_snapshot,
                fetch_normalized_supplier_snapshot(session),
                "Step 4 restore",
            )
            recovery_required = False

        completed_at = datetime.now(timezone.utc)
        report = {
            "experiment": "northwind_supplier_product_fd",
            "started_at_utc": started_at.isoformat(),
            "completed_at_utc": completed_at.isoformat(),
            "database": database,
            "environment": environment_metadata,
            "dataset": {
                "name": "case_study Northwind",
                "relevant_data_sha256": dataset_fingerprint,
                "expected_product_count": EXPECTED_PRODUCT_COUNT,
                "expected_supplier_count": EXPECTED_SUPPLIER_COUNT,
                "expected_supplied_by_count": EXPECTED_SUPPLIED_BY_COUNT,
                "fingerprint_scope": (
                    "ProductID, Product.SupplierID, and the 11 dependent "
                    "Supplier attributes"
                ),
            },
            "configuration": {
                "update_property": args.update_property,
                "warmup_runs": args.warmup_runs,
                "measured_update_runs": args.runs,
                "normalization_warmup_runs": (args.normalization_warmup_runs),
                "normalization_runs": args.normalization_runs,
                "normalized_supplier_id_range_index": (normalized_supplier_index),
                "folded_supplier_id_range_index": index_lease.name,
                "folded_index_created_by_experiment": (
                    index_lease.created_by_experiment
                ),
                "update_workload": (
                    "one transaction containing one logical update for "
                    "every distinct Supplier"
                ),
                "update_value_strategy": (
                    "deterministic one-character correction with unchanged "
                    "UTF-8 byte length; original values restored outside timer"
                ),
                "update_timing_scope": (
                    "one autocommit batch write through full result "
                    "consumption/commit; restore and validation excluded"
                ),
                "normalization_timing_scope": (
                    "data rewrite only; Step 0, validation, schema DDL, "
                    "restore, and repetition setup are outside the timer"
                ),
                "cache_policy": (
                    "warm application workload; server page/query caches "
                    "are not cleared"
                ),
                "allow_existing_folded": args.allow_existing_folded,
            },
            "initial_state": asdict(initial_state),
            "final_state": asdict(final_state),
            "redundancy": redundancy,
            "timings": {
                "denormalized_update": {
                    "samples": samples_as_dicts(denormalized_update_samples),
                    "summary": timing_summary(denormalized_update_samples),
                },
                "normalization": {
                    "samples": samples_as_dicts(normalization_samples),
                    "summary": timing_summary(normalization_samples),
                },
                "normalized_update": {
                    "samples": samples_as_dicts(normalized_update_samples),
                    "summary": timing_summary(normalized_update_samples),
                },
            },
            "derived": derived_comparison(
                redundancy,
                denormalized_update_samples,
                normalization_samples,
                normalized_update_samples,
            ),
        }
        return report
    except BaseException:
        if recovery_required:
            print(
                "\nWARNING: experiment stopped; attempting to restore the "
                "normalized graph and original Supplier values",
                file=sys.stderr,
            )
            try:
                recover_normalized_graph(
                    driver,
                    database,
                    index_lease,
                    supplier_snapshot,
                )
            except BaseException as recovery_error:
                print(
                    "WARNING: automatic recovery also failed: "
                    f"{type(recovery_error).__name__}: {recovery_error}",
                    file=sys.stderr,
                )
        raise


def print_final_comparison(report: dict[str, Any]) -> None:
    derived = report["derived"]
    break_even = derived["data_rewrite_normalization_break_even_update_batches"]
    break_even_text = (
        "N/A" if break_even is None else f"{break_even:.3f} logical update batches"
    )
    print("\nFinal comparison")
    print(
        "  Write amplification by updated nodes: "
        f"{derived['write_amplification_nodes']:.3f}x"
    )
    ratio = derived["denormalized_to_normalized_update_median_ratio"]
    print(
        "  Folded/normalized median client-time ratio: "
        + ("N/A" if ratio is None else f"{ratio:.3f}x")
    )
    print(f"  Estimated data-rewrite-only normalization break-even: {break_even_text}")
    print("  Final graph state: normalized")


def write_json_report(path: Path, report: dict[str, Any]) -> Path:
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return resolved


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    database = args.database.strip().lower()
    if not database or database == "system":
        print(
            "ERROR: --database must name an existing Northwind data database",
            file=sys.stderr,
        )
        return 2

    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        print(
            'ERROR: install the Neo4j driver with: python -m pip install "neo4j>=5.7"',
            file=sys.stderr,
        )
        print(f"CAUSE: {exc}", file=sys.stderr)
        return 1

    password = os.getenv("NEO4J_PASSWORD") or DEBUG_NEO4J_PASSWORD
    if password is None:
        password = getpass(f"Neo4j password for {args.user}: ")

    try:
        print(f"Connecting to {args.uri} as {args.user!r}; database={database!r}")
        with GraphDatabase.driver(
            args.uri,
            auth=(args.user, password),
        ) as driver:
            if not hasattr(driver, "execute_query"):
                raise RuntimeError("Neo4j Python Driver 5.7 or newer is required")
            driver.verify_connectivity()
            driver.execute_query(
                "RETURN 1 AS ready",
                database_=database,
            )
            report = run_experiment(driver, database, args)

        print_final_comparison(report)
        if args.output_json is not None:
            output_path = write_json_report(args.output_json, report)
            print(f"  JSON report: {output_path}")
    except KeyboardInterrupt:
        print("\nExperiment interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        if exc.__cause__ is not None:
            print(f"CAUSE: {exc.__cause__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
