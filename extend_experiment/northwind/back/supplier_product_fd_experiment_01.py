#!/usr/bin/env python3
"""Run the Northwind Supplier -> Product FD experiment on a shadow subgraph.

The imported Northwind graph is treated as read-only experimental input.  The
script creates a run-scoped materialized-view-style projection and removes it
after the experiment, so Product, Supplier, and the original relationships are
never deleted or updated.

Step 0 (not timed)
    Create one ``mv_product_supplier`` node per Product.  Each node receives all
    Product properties and all Supplier properties.  It also keeps the stable
    ``mv_product`` label in both phases; ``mv_product_supplier`` is the folded
    marker.  Copy the Product-facing topology as:

        (OrderDetail)-[:join_of_product]->(mv_product_supplier)
        (mv_product_supplier)-[:join_in_category]->(Category)

    Relationship property maps are copied as well.  The original ``OF_PRODUCT``,
    ``IN_CATEGORY``, and ``SUPPLIED_BY`` relationships remain unchanged.

Step 1
    Measure logical redundancy caused by
    ``SupplierID -> Supplier attributes`` inside this shadow projection.

Step 2
    Choose a non-key Supplier attribute and construct one deterministic update
    per SupplierID by rotating each value to the next distinct value in that
    attribute's observed active domain.  Apply the same logical updates to all
    matching ``mv_product_supplier`` copies.  Each timed update is restored
    outside the timer.

Step 3
    Normalize only the shadow projection in one timed transaction.  Create one
    ``mv_supplier`` per distinct SupplierID, create ``join_supplied_by``
    relationships, remove copied Supplier attributes from the shadow Product
    nodes, and remove the ``mv_product_supplier`` marker.  The stable
    ``mv_product`` label and copied order/category relationships are retained
    and are not part of the timed rewrite.

Step 4
    Apply the exact same precomputed logical updates to the newly created
    ``mv_supplier`` nodes.  Each timed update is restored outside the timer.

For repeated Step 3 measurements, the normalized shadow projection is folded
back outside the timer while retaining ``join_of_product`` and
``join_in_category``.  On success, and on a best-effort basis after failure,
all nodes belonging to this ``run_id`` and their attached shadow relationships
are removed.  Experiment-owned schema objects are also dropped.  Existing
compatible schema is reused and left in place.

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
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from getpass import getpass
from pathlib import Path
from statistics import fmean, median, stdev
from time import perf_counter_ns
from typing import Any, Iterable, Iterator, Sequence
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
EXPECTED_OF_PRODUCT_COUNT = 2_155
EXPECTED_IN_CATEGORY_COUNT = 77
EXPECTED_RELEVANT_DATA_SHA256 = (
    "614256e0ab65e7e5b6c1aa45178001d729697a63a9eea2b9bfc2bb7a948c6496"
)

# Local debugging only. Never commit a real password here.
DEBUG_NEO4J_PASSWORD: str | None = None

# Original Northwind schema.
PRODUCT_LABEL = "Product"
SUPPLIER_LABEL = "Supplier"
ORDER_DETAIL_LABEL = "OrderDetail"
CATEGORY_LABEL = "Category"
OF_PRODUCT_RELATIONSHIP = "OF_PRODUCT"
IN_CATEGORY_RELATIONSHIP = "IN_CATEGORY"
SUPPLIED_BY_RELATIONSHIP = "SUPPLIED_BY"

# Run-scoped shadow schema.  The lower-case names are deliberate and match the
# names chosen for the materialized-view experiment.
MV_ARTIFACT_LABEL = "mv_experiment"
MV_FOLDED_LABEL = "mv_product_supplier"
MV_PRODUCT_LABEL = "mv_product"
MV_SUPPLIER_LABEL = "mv_supplier"
JOIN_OF_PRODUCT_RELATIONSHIP = "join_of_product"
JOIN_IN_CATEGORY_RELATIONSHIP = "join_in_category"
JOIN_SUPPLIED_BY_RELATIONSHIP = "join_supplied_by"
RUN_ID_PROPERTY = "run_id"

PRODUCT_ID_PROPERTY = "ProductID"
SUPPLIER_ID_PROPERTY = "SupplierID"
SUPPLIER_KEY_PROPERTIES = (SUPPLIER_ID_PROPERTY,)

MV_FOLDED_INDEX_PREFIX = "mv_product_supplier_run_supplier_experiment"
MV_PRODUCT_CONSTRAINT_PREFIX = "mv_product_run_product_experiment"
MV_SUPPLIER_CONSTRAINT_PREFIX = "mv_supplier_run_supplier_experiment"
LEGACY_MV_PRODUCT_ID_INDEX_PREFIX = (
    "mv_product_supplier_run_product_experiment"
)

# SupplierID remains on the shadow Product as the foreign-key property.  Only
# these dependent attributes are removed by Step 3.
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

# Every dependent Supplier attribute is selectable.  The active-domain update
# builder rejects NULL-containing attributes before timing because Neo4j models
# ``SET n.property = null`` as property removal, which is a different workload.
UPDATE_PROPERTIES = tuple(
    attribute
    for attribute in SUPPLIER_ATTRIBUTES
    if attribute not in SUPPLIER_KEY_PROPERTIES
)

UPDATE_STRATEGY_ID = "deterministic_active_domain_rotation_v1"
UPDATE_NULL_POLICY = "reject_attribute_if_any_logical_value_is_null"


class ExperimentStateError(RuntimeError):
    """Raised when the original or shadow graph is in an unsafe state."""


class ExperimentValidationError(RuntimeError):
    """Raised when a transformation or measured update has a wrong result."""


@dataclass(frozen=True)
class OriginalGraphState:
    product_nodes: int
    supplier_nodes: int
    supplied_by_relationships: int

    @property
    def phase(self) -> str:
        if (
            self.product_nodes > 0
            and self.supplier_nodes > 0
            and self.supplied_by_relationships == self.product_nodes
        ):
            return "normalized"
        return "inconsistent"


@dataclass(frozen=True)
class ShadowGraphState:
    folded_product_nodes: int
    normalized_product_nodes: int
    normalized_supplier_nodes: int
    join_of_product_relationships: int
    join_in_category_relationships: int
    join_supplied_by_relationships: int

    @property
    def phase(self) -> str:
        relationship_total = (
            self.join_of_product_relationships
            + self.join_in_category_relationships
            + self.join_supplied_by_relationships
        )
        node_total = (
            self.folded_product_nodes
            + self.normalized_product_nodes
            + self.normalized_supplier_nodes
        )
        if node_total == 0 and relationship_total == 0:
            return "clean"
        if (
            self.folded_product_nodes > 0
            and self.normalized_product_nodes == 0
            and self.normalized_supplier_nodes == 0
            and self.join_supplied_by_relationships == 0
        ):
            return "folded"
        if (
            self.folded_product_nodes == 0
            and self.normalized_product_nodes > 0
            and self.normalized_supplier_nodes > 0
            and self.join_supplied_by_relationships
            == self.normalized_product_nodes
        ):
            return "normalized"
        return "inconsistent"


@dataclass(frozen=True)
class SourceEdgeCounts:
    of_product: int
    in_category: int


@dataclass(frozen=True)
class CleanupResult:
    deleted_nodes: int
    deleted_relationships: int


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


@dataclass(frozen=True)
class UpdateWorkload:
    """One precomputed logical update workload shared by Steps 2 and 4."""

    property_name: str
    originals: dict[Any, Any]
    updates: list[dict[str, Any]]
    restores: list[dict[str, Any]]
    active_domain_size: int
    active_domain_scope: str
    active_domain_source_tuple_count: int
    active_domain_sha256: str
    mapping_sha256: str
    value_type: str
    logical_original_payload_bytes: int
    logical_updated_payload_bytes: int

    @property
    def logical_tuple_count(self) -> int:
        return len(self.originals)

    @property
    def logical_payload_delta_bytes(self) -> int:
        return (
            self.logical_updated_payload_bytes
            - self.logical_original_payload_bytes
        )


@dataclass
class IndexLease:
    """Track an existing or experiment-owned RANGE index."""

    name: str | None = None
    created_by_experiment: bool = False


@dataclass
class ConstraintLease:
    """Track an existing or experiment-owned uniqueness constraint."""

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
            "amplification, and normalization cost in an isolated Northwind "
            "shadow projection."
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
            "non-key Supplier attribute used by Steps 2 and 4; values are "
            "replaced by deterministic active-domain rotation; attributes "
            "containing NULL fail before timing "
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
        help=f"measured update/restore pairs per state (default: {DEFAULT_RUNS})",
    )
    parser.add_argument(
        "--normalization-runs",
        type=positive_int,
        default=DEFAULT_NORMALIZATION_RUNS,
        help=(
            "measured Step 3 repetitions; shadow refolding is outside the "
            f"timer (default: {DEFAULT_NORMALIZATION_RUNS})"
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
            "maximum wait for an experiment index/constraint "
            f"(default: {DEFAULT_INDEX_WAIT_SECONDS})"
        ),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="optional path for raw samples and all derived metrics",
    )
    parser.add_argument(
        "--cleanup-run-id",
        help=(
            "cleanup-only mode for one printed run_id left by a hard-stopped "
            "experiment; no benchmark steps are run"
        ),
    )
    return parser.parse_args(argv)


@contextmanager
def single_process_experiment_lock() -> Iterator[None]:
    """Prevent two local copies of this script from sharing shadow schema."""

    try:
        import fcntl
    except ImportError:
        # Neo4j-side run_id filters still protect data, but formal benchmark
        # runs on platforms without flock must be serialized by the caller.
        yield
        return

    lock_handle = Path(__file__).resolve().open("rb")
    try:
        try:
            fcntl.flock(
                lock_handle.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
        except BlockingIOError as exc:
            raise ExperimentStateError(
                "another local supplier_product_fd_experiment.py process is "
                "already running; concurrent benchmark runs are unsupported"
            ) from exc
        yield
    finally:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            lock_handle.close()


def result_records_and_summary(result: Any) -> tuple[list[Any], Any]:
    records = list(result)
    summary = result.consume()
    return records, summary


def one_record(result: Any, context: str) -> tuple[Any, Any]:
    records, summary = result_records_and_summary(result)
    if len(records) != 1:
        raise RuntimeError(f"{context}: expected one record, found {len(records)}")
    return records[0], summary


def scalar_count(session: Any, query: str, field: str, context: str, **params: Any) -> int:
    record, _ = one_record(session.run(query, **params), context)
    return int(record[field])


def transaction_record(
    transaction: Any,
    query: str,
    context: str,
    **params: Any,
) -> Any:
    record, _ = one_record(transaction.run(query, **params), context)
    return record


def inspect_original_state(session: Any) -> OriginalGraphState:
    product_nodes = scalar_count(
        session,
        f"MATCH (n:{quote_ident(PRODUCT_LABEL)}) RETURN count(n) AS count",
        "count",
        "count original Product",
    )
    supplier_nodes = scalar_count(
        session,
        f"MATCH (n:{quote_ident(SUPPLIER_LABEL)}) RETURN count(n) AS count",
        "count",
        "count original Supplier",
    )
    supplied_by_relationships = scalar_count(
        session,
        (
            f"MATCH (:{quote_ident(PRODUCT_LABEL)})"
            f"-[r:{quote_ident(SUPPLIED_BY_RELATIONSHIP)}]->"
            f"(:{quote_ident(SUPPLIER_LABEL)}) "
            "RETURN count(r) AS count"
        ),
        "count",
        "count original SUPPLIED_BY",
    )
    return OriginalGraphState(
        product_nodes=product_nodes,
        supplier_nodes=supplier_nodes,
        supplied_by_relationships=supplied_by_relationships,
    )


def count_shadow_nodes(session: Any, run_id: str, label: str) -> int:
    return scalar_count(
        session,
        f"""
        MATCH (n)
        WHERE $label IN labels(n)
          AND n.{quote_ident(RUN_ID_PROPERTY)} = $run_id
        RETURN count(n) AS count
        """,
        "count",
        f"count {label} for run",
        label=label,
        run_id=run_id,
    )


def count_normalized_shadow_products(session: Any, run_id: str) -> int:
    """Count stable mv_product nodes that no longer have the folded marker."""

    return scalar_count(
        session,
        f"""
        MATCH (n)
        WHERE $product_label IN labels(n)
          AND NOT ($folded_label IN labels(n))
          AND n.{quote_ident(RUN_ID_PROPERTY)} = $run_id
        RETURN count(n) AS count
        """,
        "count",
        "count normalized mv_product nodes for run",
        product_label=MV_PRODUCT_LABEL,
        folded_label=MV_FOLDED_LABEL,
        run_id=run_id,
    )


def count_shadow_relationships(
    session: Any,
    run_id: str,
    relationship_type: str,
    *,
    shadow_endpoint: str,
) -> int:
    if shadow_endpoint == "target":
        pattern = "()-[r]->(n)"
    elif shadow_endpoint == "source":
        pattern = "(n)-[r]->()"
    else:
        raise ValueError(f"unsupported shadow endpoint: {shadow_endpoint}")
    return scalar_count(
        session,
        f"""
        MATCH {pattern}
        WHERE type(r) = $relationship_type
          AND $artifact_label IN labels(n)
          AND n.{quote_ident(RUN_ID_PROPERTY)} = $run_id
        RETURN count(r) AS count
        """,
        "count",
        f"count {relationship_type} for run",
        relationship_type=relationship_type,
        artifact_label=MV_ARTIFACT_LABEL,
        run_id=run_id,
    )


def inspect_shadow_state(session: Any, run_id: str) -> ShadowGraphState:
    return ShadowGraphState(
        folded_product_nodes=count_shadow_nodes(
            session, run_id, MV_FOLDED_LABEL
        ),
        normalized_product_nodes=count_normalized_shadow_products(
            session, run_id
        ),
        normalized_supplier_nodes=count_shadow_nodes(
            session, run_id, MV_SUPPLIER_LABEL
        ),
        join_of_product_relationships=count_shadow_relationships(
            session,
            run_id,
            JOIN_OF_PRODUCT_RELATIONSHIP,
            shadow_endpoint="target",
        ),
        join_in_category_relationships=count_shadow_relationships(
            session,
            run_id,
            JOIN_IN_CATEGORY_RELATIONSHIP,
            shadow_endpoint="source",
        ),
        join_supplied_by_relationships=count_shadow_relationships(
            session,
            run_id,
            JOIN_SUPPLIED_BY_RELATIONSHIP,
            shadow_endpoint="source",
        ),
    )


def existing_shadow_artifact_counts(session: Any) -> tuple[int, int]:
    record, _ = one_record(
        session.run(
            """
            OPTIONAL MATCH (n)
            WHERE any(
              label IN labels(n)
              WHERE label IN $artifact_labels
            )
            WITH count(n) AS node_count
            OPTIONAL MATCH ()-[r]->()
            WHERE type(r) IN $relationship_types
            RETURN node_count, count(r) AS relationship_count
            """,
            artifact_labels=[
                MV_ARTIFACT_LABEL,
                MV_FOLDED_LABEL,
                MV_PRODUCT_LABEL,
                MV_SUPPLIER_LABEL,
            ],
            relationship_types=[
                JOIN_OF_PRODUCT_RELATIONSHIP,
                JOIN_IN_CATEGORY_RELATIONSHIP,
                JOIN_SUPPLIED_BY_RELATIONSHIP,
            ],
        ),
        "check pre-existing shadow artifacts",
    )
    return int(record["node_count"]), int(record["relationship_count"])


def existing_shadow_run_ids(session: Any) -> list[str]:
    result = session.run(
        f"""
        MATCH (n)
        WHERE any(
          label IN labels(n)
          WHERE label IN $artifact_labels
        )
          AND n.{quote_ident(RUN_ID_PROPERTY)} IS NOT NULL
        RETURN DISTINCT
          toString(n.{quote_ident(RUN_ID_PROPERTY)}) AS run_id
        ORDER BY run_id
        """,
        artifact_labels=[
            MV_ARTIFACT_LABEL,
            MV_FOLDED_LABEL,
            MV_PRODUCT_LABEL,
            MV_SUPPLIER_LABEL,
        ],
    )
    records, _ = result_records_and_summary(result)
    return [str(record["run_id"]) for record in records]


def ensure_no_existing_shadow_artifacts(session: Any) -> None:
    node_count, relationship_count = existing_shadow_artifact_counts(session)
    if node_count or relationship_count:
        run_ids = existing_shadow_run_ids(session)
        recovery_hint = (
            " Recover each run with --cleanup-run-id; discovered run_id "
            f"values: {run_ids!r}."
            if run_ids
            else " No run_id was discoverable; inspect the artifacts manually."
        )
        raise ExperimentStateError(
            "pre-existing shadow experiment artifacts were found: "
            f"{node_count} nodes and {relationship_count} relationships. "
            "Remove or archive the earlier mv_experiment run before starting "
            f"a new complete-query experiment.{recovery_hint}"
        )


def folded_property_predicate(alias: str) -> str:
    return " OR ".join(
        f"{alias}.{quote_ident(attribute)} IS NOT NULL"
        for attribute in SUPPLIER_ATTRIBUTES
    )


def validate_original_state(session: Any) -> OriginalGraphState:
    """Validate the imported Northwind graph without changing it."""

    state = inspect_original_state(session)
    if state.phase != "normalized":
        raise ExperimentStateError(
            f"expected the original graph to be normalized, found {state}"
        )

    record, _ = one_record(
        session.run(
            f"""
            MATCH (p:{quote_ident(PRODUCT_LABEL)})
            OPTIONAL MATCH
              (p)-[r:{quote_ident(SUPPLIED_BY_RELATIONSHIP)}]->
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
              ) AS products_with_supplier_properties
            """
        ),
        "validate original Product-Supplier relationships",
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
    if int(record["products_with_supplier_properties"]) != 0:
        problems.append(
            f"{int(record['products_with_supplier_properties'])} original "
            "Product nodes already contain Supplier dependent properties"
        )
    distinct_supplier_ids = int(record["distinct_supplier_ids"])
    if distinct_supplier_ids != state.supplier_nodes:
        problems.append(
            f"distinct Product.SupplierID={distinct_supplier_ids}, "
            f"Supplier nodes={state.supplier_nodes}"
        )

    adjacent_supplier_relationships = scalar_count(
        session,
        f"MATCH (s:{quote_ident(SUPPLIER_LABEL)})-[r]-() "
        "RETURN count(r) AS count",
        "count",
        "count all original Supplier relationships",
    )
    if adjacent_supplier_relationships != state.supplied_by_relationships:
        problems.append(
            "Supplier nodes have relationships outside the expected "
            f"{SUPPLIED_BY_RELATIONSHIP} set"
        )

    supplier_shape, _ = one_record(
        session.run(
            f"""
            MATCH (s:{quote_ident(SUPPLIER_LABEL)})
            WITH
              s,
              [label IN labels(s) WHERE label <> $supplier_label]
                AS extra_labels,
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
        "validate original Supplier labels and properties",
    )
    if int(supplier_shape["suppliers_with_extra_labels"]) != 0:
        problems.append(
            f"{int(supplier_shape['suppliers_with_extra_labels'])} Supplier "
            "nodes have unexpected labels"
        )
    if int(supplier_shape["suppliers_with_extra_properties"]) != 0:
        problems.append(
            f"{int(supplier_shape['suppliers_with_extra_properties'])} "
            "Supplier nodes have unexpected properties"
        )

    relationship_properties = scalar_count(
        session,
        (
            f"MATCH (:{quote_ident(PRODUCT_LABEL)})"
            f"-[r:{quote_ident(SUPPLIED_BY_RELATIONSHIP)}]->"
            f"(:{quote_ident(SUPPLIER_LABEL)}) "
            "WHERE size(keys(r)) > 0 "
            "RETURN count(r) AS count"
        ),
        "count",
        "count SUPPLIED_BY relationships with properties",
    )
    if relationship_properties:
        problems.append(
            f"{relationship_properties} {SUPPLIED_BY_RELATIONSHIP} "
            "relationships have properties that the normalized shadow edge "
            "would not represent"
        )

    unexpected_product_relationships = scalar_count(
        session,
        f"""
        MATCH (p:{quote_ident(PRODUCT_LABEL)})-[r]-()
        WHERE NOT (type(r) IN $allowed_types)
        RETURN count(r) AS count
        """,
        "count",
        "count unsupported Product relationships",
        allowed_types=[
            OF_PRODUCT_RELATIONSHIP,
            IN_CATEGORY_RELATIONSHIP,
            SUPPLIED_BY_RELATIONSHIP,
        ],
    )
    if unexpected_product_relationships:
        problems.append(
            f"{unexpected_product_relationships} Product-adjacent "
            "relationships have types not copied by this complete-query "
            "projection"
        )

    reserved_property_uses = scalar_count(
        session,
        f"""
        MATCH
          (p:{quote_ident(PRODUCT_LABEL)})
          -[:{quote_ident(SUPPLIED_BY_RELATIONSHIP)}]->
          (s:{quote_ident(SUPPLIER_LABEL)})
        WHERE $run_id_property IN keys(p)
           OR $run_id_property IN keys(s)
        RETURN count(*) AS count
        """,
        "count",
        "count source uses of reserved run_id property",
        run_id_property=RUN_ID_PROPERTY,
    )
    if reserved_property_uses:
        problems.append(
            f"{reserved_property_uses} source Product-Supplier pairs use "
            f"reserved property {RUN_ID_PROPERTY!r}"
        )

    if problems:
        raise ExperimentValidationError(
            "Original graph validation failed:\n  - " + "\n  - ".join(problems)
        )
    return state


def fetch_source_edge_counts(session: Any) -> SourceEdgeCounts:
    of_product = scalar_count(
        session,
        (
            f"MATCH (:{quote_ident(ORDER_DETAIL_LABEL)})"
            f"-[r:{quote_ident(OF_PRODUCT_RELATIONSHIP)}]->"
            f"(:{quote_ident(PRODUCT_LABEL)}) "
            "RETURN count(r) AS count"
        ),
        "count",
        "count source OF_PRODUCT",
    )
    in_category = scalar_count(
        session,
        (
            f"MATCH (:{quote_ident(PRODUCT_LABEL)})"
            f"-[r:{quote_ident(IN_CATEGORY_RELATIONSHIP)}]->"
            f"(:{quote_ident(CATEGORY_LABEL)}) "
            "RETURN count(r) AS count"
        ),
        "count",
        "count source IN_CATEGORY",
    )
    product_edge_shape, _ = one_record(
        session.run(
            f"""
            MATCH (p:{quote_ident(PRODUCT_LABEL)})-[r]-()
            WHERE type(r) IN $product_relationship_types
            RETURN
              sum(
                CASE
                  WHEN type(r) = $of_product_type THEN 1 ELSE 0
                END
              ) AS all_of_product,
              sum(
                CASE
                  WHEN type(r) = $in_category_type THEN 1 ELSE 0
                END
              ) AS all_in_category,
              sum(
                CASE
                  WHEN type(r) = $supplied_by_type THEN 1 ELSE 0
                END
              ) AS all_supplied_by
            """,
            product_relationship_types=[
                OF_PRODUCT_RELATIONSHIP,
                IN_CATEGORY_RELATIONSHIP,
                SUPPLIED_BY_RELATIONSHIP,
            ],
            of_product_type=OF_PRODUCT_RELATIONSHIP,
            in_category_type=IN_CATEGORY_RELATIONSHIP,
            supplied_by_type=SUPPLIED_BY_RELATIONSHIP,
        ),
        "validate source Product relationship directions and endpoints",
    )
    original_state = inspect_original_state(session)
    if (
        int(product_edge_shape["all_of_product"]) != of_product
        or int(product_edge_shape["all_in_category"]) != in_category
        or int(product_edge_shape["all_supplied_by"])
        != original_state.supplied_by_relationships
    ):
        raise ExperimentValidationError(
            "Product relationships with a recognized type have an "
            "unsupported direction or endpoint"
        )

    category_shape, _ = one_record(
        session.run(
            f"""
            MATCH (p:{quote_ident(PRODUCT_LABEL)})
            OPTIONAL MATCH
              (p)-[r:{quote_ident(IN_CATEGORY_RELATIONSHIP)}]->
              (c:{quote_ident(CATEGORY_LABEL)})
            WITH p, collect(r) AS relationships, collect(c) AS categories
            RETURN
              count(p) AS product_count,
              sum(
                CASE WHEN size(relationships) = 1 THEN 0 ELSE 1 END
              ) AS invalid_degrees,
              sum(
                CASE
                  WHEN size(categories) = 1
                   AND categories[0].{quote_ident("CategoryID")}
                       = p.{quote_ident("CategoryID")}
                  THEN 0 ELSE 1
                END
              ) AS mismatched_ids
            """
        ),
        "validate Product-IN_CATEGORY shape",
    )
    if (
        int(category_shape["product_count"]) != EXPECTED_PRODUCT_COUNT
        or int(category_shape["invalid_degrees"]) != 0
        or int(category_shape["mismatched_ids"]) != 0
    ):
        raise ExperimentValidationError(
            "each Product must have exactly one IN_CATEGORY relationship "
            "whose CategoryID agrees with Product.CategoryID"
        )

    order_detail_shape, _ = one_record(
        session.run(
            f"""
            MATCH (od:{quote_ident(ORDER_DETAIL_LABEL)})
            OPTIONAL MATCH
              (od)-[r:{quote_ident(OF_PRODUCT_RELATIONSHIP)}]->
              (p:{quote_ident(PRODUCT_LABEL)})
            WITH od, collect(r) AS relationships, collect(p) AS products
            RETURN
              count(od) AS order_detail_count,
              sum(
                CASE WHEN size(relationships) = 1 THEN 0 ELSE 1 END
              ) AS invalid_degrees,
              sum(
                CASE
                  WHEN size(products) = 1
                   AND products[0].{quote_ident(PRODUCT_ID_PROPERTY)}
                       = od.{quote_ident(PRODUCT_ID_PROPERTY)}
                  THEN 0 ELSE 1
                END
              ) AS mismatched_ids
            """
        ),
        "validate OrderDetail-OF_PRODUCT shape",
    )
    if (
        int(order_detail_shape["order_detail_count"])
        != EXPECTED_OF_PRODUCT_COUNT
        or int(order_detail_shape["invalid_degrees"]) != 0
        or int(order_detail_shape["mismatched_ids"]) != 0
    ):
        raise ExperimentValidationError(
            "each OrderDetail must have exactly one OF_PRODUCT relationship "
            "whose ProductID agrees with OrderDetail.ProductID"
        )

    if (
        of_product != EXPECTED_OF_PRODUCT_COUNT
        or in_category != EXPECTED_IN_CATEGORY_COUNT
    ):
        raise ExperimentValidationError(
            "the Product topology does not match the case-study dataset: "
            f"OF_PRODUCT={of_product} "
            f"(expected {EXPECTED_OF_PRODUCT_COUNT}), "
            f"IN_CATEGORY={in_category} "
            f"(expected {EXPECTED_IN_CATEGORY_COUNT})"
        )
    return SourceEdgeCounts(of_product=of_product, in_category=in_category)


def source_topology_sha256(session: Any) -> str:
    """Hash source relationship endpoints and complete property maps."""

    order_result = session.run(
        f"""
        MATCH
          (od:{quote_ident(ORDER_DETAIL_LABEL)})
          -[r:{quote_ident(OF_PRODUCT_RELATIONSHIP)}]->
          (p:{quote_ident(PRODUCT_LABEL)})
        RETURN
          od.{quote_ident("OrderID")} AS order_id,
          od.{quote_ident(PRODUCT_ID_PROPERTY)} AS order_detail_product_id,
          p.{quote_ident(PRODUCT_ID_PROPERTY)} AS product_id,
          properties(r) AS relationship_properties
        ORDER BY order_id, order_detail_product_id, product_id
        """
    )
    order_records, _ = result_records_and_summary(order_result)
    category_result = session.run(
        f"""
        MATCH
          (p:{quote_ident(PRODUCT_LABEL)})
          -[r:{quote_ident(IN_CATEGORY_RELATIONSHIP)}]->
          (c:{quote_ident(CATEGORY_LABEL)})
        RETURN
          p.{quote_ident(PRODUCT_ID_PROPERTY)} AS product_id,
          c.{quote_ident("CategoryID")} AS category_id,
          properties(r) AS relationship_properties
        ORDER BY product_id, category_id
        """
    )
    category_records, _ = result_records_and_summary(category_result)

    canonical = {
        "OF_PRODUCT": [
            {
                "order_id": record["order_id"],
                "order_detail_product_id": record[
                    "order_detail_product_id"
                ],
                "product_id": record["product_id"],
                "relationship_properties": dict(
                    record["relationship_properties"]
                ),
            }
            for record in order_records
        ],
        "IN_CATEGORY": [
            {
                "product_id": record["product_id"],
                "category_id": record["category_id"],
                "relationship_properties": dict(
                    record["relationship_properties"]
                ),
            }
            for record in category_records
        ],
    }
    payload = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def validate_shadow_node_inheritance(
    session: Any,
    run_id: str,
    shadow_label: str,
    *,
    include_supplier_properties: bool,
    expected_products: int,
) -> None:
    supplier_check = (
        "AND all(key IN keys(s) WHERE copies[0][key] = s[key])"
        if include_supplier_properties
        else ""
    )
    record, _ = one_record(
        session.run(
            f"""
            MATCH
              (p:{quote_ident(PRODUCT_LABEL)})
              -[:{quote_ident(SUPPLIED_BY_RELATIONSHIP)}]->
              (s:{quote_ident(SUPPLIER_LABEL)})
            OPTIONAL MATCH (
              mv:{quote_ident(shadow_label)}
              {{
                {quote_ident(RUN_ID_PROPERTY)}: $run_id,
                {quote_ident(PRODUCT_ID_PROPERTY)}:
                  p.{quote_ident(PRODUCT_ID_PROPERTY)}
              }}
            )
            WITH p, s, [node IN collect(mv) WHERE node IS NOT NULL] AS copies
            RETURN
              count(p) AS source_products,
              sum(
                CASE WHEN size(copies) = 1 THEN 0 ELSE 1 END
              ) AS invalid_copy_counts,
              sum(
                CASE
                  WHEN size(copies) = 1
                   AND all(key IN keys(p) WHERE copies[0][key] = p[key])
                   {supplier_check}
                  THEN 0
                  ELSE 1
                END
              ) AS property_mismatches
            """,
            run_id=run_id,
        ),
        f"validate {shadow_label} property inheritance",
    )
    if int(record["source_products"]) != expected_products:
        raise ExperimentValidationError(
            f"source Product count changed while validating {shadow_label}"
        )
    invalid_copy_counts = int(record["invalid_copy_counts"])
    property_mismatches = int(record["property_mismatches"])
    if invalid_copy_counts or property_mismatches:
        raise ExperimentValidationError(
            f"{shadow_label} inheritance failed: "
            f"invalid Product copy counts={invalid_copy_counts}, "
            f"property mismatches={property_mismatches}"
        )


def validate_join_of_product_copy(
    session: Any,
    run_id: str,
    shadow_product_label: str,
    expected_edges: int,
) -> None:
    record, _ = one_record(
        session.run(
            f"""
            MATCH
              (od:{quote_ident(ORDER_DETAIL_LABEL)})
              -[source:{quote_ident(OF_PRODUCT_RELATIONSHIP)}]->
              (p:{quote_ident(PRODUCT_LABEL)})
            OPTIONAL MATCH
              (od)-[copy:{quote_ident(JOIN_OF_PRODUCT_RELATIONSHIP)}]->
              (
                mv:{quote_ident(shadow_product_label)}
                {{
                  {quote_ident(RUN_ID_PROPERTY)}: $run_id,
                  {quote_ident(PRODUCT_ID_PROPERTY)}:
                    p.{quote_ident(PRODUCT_ID_PROPERTY)}
                }}
              )
            WITH source, [r IN collect(copy) WHERE r IS NOT NULL] AS copies
            RETURN
              count(source) AS source_edges,
              sum(
                CASE WHEN size(copies) = 1 THEN 0 ELSE 1 END
              ) AS invalid_copy_counts,
              sum(
                CASE
                  WHEN size(copies) = 1
                   AND properties(copies[0]) = properties(source)
                  THEN 0
                  ELSE 1
                END
              ) AS property_mismatches
            """,
            run_id=run_id,
        ),
        "validate join_of_product copy",
    )
    if int(record["source_edges"]) != expected_edges:
        raise ExperimentValidationError(
            "source OF_PRODUCT relationship count changed"
        )
    invalid = int(record["invalid_copy_counts"])
    mismatches = int(record["property_mismatches"])
    if invalid or mismatches:
        raise ExperimentValidationError(
            "join_of_product validation failed: "
            f"invalid copies={invalid}, property mismatches={mismatches}"
        )


def validate_join_in_category_copy(
    session: Any,
    run_id: str,
    shadow_product_label: str,
    expected_edges: int,
) -> None:
    record, _ = one_record(
        session.run(
            f"""
            MATCH
              (p:{quote_ident(PRODUCT_LABEL)})
              -[source:{quote_ident(IN_CATEGORY_RELATIONSHIP)}]->
              (c:{quote_ident(CATEGORY_LABEL)})
            OPTIONAL MATCH
              (
                mv:{quote_ident(shadow_product_label)}
                {{
                  {quote_ident(RUN_ID_PROPERTY)}: $run_id,
                  {quote_ident(PRODUCT_ID_PROPERTY)}:
                    p.{quote_ident(PRODUCT_ID_PROPERTY)}
                }}
              )
              -[copy:{quote_ident(JOIN_IN_CATEGORY_RELATIONSHIP)}]->
              (c)
            WITH source, [r IN collect(copy) WHERE r IS NOT NULL] AS copies
            RETURN
              count(source) AS source_edges,
              sum(
                CASE WHEN size(copies) = 1 THEN 0 ELSE 1 END
              ) AS invalid_copy_counts,
              sum(
                CASE
                  WHEN size(copies) = 1
                   AND properties(copies[0]) = properties(source)
                  THEN 0
                  ELSE 1
                END
              ) AS property_mismatches
            """,
            run_id=run_id,
        ),
        "validate join_in_category copy",
    )
    if int(record["source_edges"]) != expected_edges:
        raise ExperimentValidationError(
            "source IN_CATEGORY relationship count changed"
        )
    invalid = int(record["invalid_copy_counts"])
    mismatches = int(record["property_mismatches"])
    if invalid or mismatches:
        raise ExperimentValidationError(
            "join_in_category validation failed: "
            f"invalid copies={invalid}, property mismatches={mismatches}"
        )


def validate_copied_topology(
    session: Any,
    run_id: str,
    shadow_product_label: str,
    source_edges: SourceEdgeCounts,
) -> None:
    validate_join_of_product_copy(
        session,
        run_id,
        shadow_product_label,
        source_edges.of_product,
    )
    validate_join_in_category_copy(
        session,
        run_id,
        shadow_product_label,
        source_edges.in_category,
    )


def validate_folded_shadow_state(
    session: Any,
    run_id: str,
    *,
    expected_products: int,
    expected_suppliers: int,
    source_edges: SourceEdgeCounts,
) -> ShadowGraphState:
    state = inspect_shadow_state(session, run_id)
    problems: list[str] = []
    if state.phase != "folded":
        problems.append(f"phase={state.phase}")
    if state.folded_product_nodes != expected_products:
        problems.append(
            f"mv_product_supplier={state.folded_product_nodes}, "
            f"expected {expected_products}"
        )
    if state.join_of_product_relationships != source_edges.of_product:
        problems.append(
            f"join_of_product={state.join_of_product_relationships}, "
            f"expected {source_edges.of_product}"
        )
    if state.join_in_category_relationships != source_edges.in_category:
        problems.append(
            f"join_in_category={state.join_in_category_relationships}, "
            f"expected {source_edges.in_category}"
        )
    if problems:
        raise ExperimentValidationError(
            "Folded shadow state validation failed:\n  - "
            + "\n  - ".join(problems)
        )

    record, _ = one_record(
        session.run(
            f"""
            MATCH (
              mv:{quote_ident(MV_ARTIFACT_LABEL)}:{quote_ident(MV_PRODUCT_LABEL)}:{quote_ident(MV_FOLDED_LABEL)}
              {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
            )
            RETURN
              count(mv) AS product_count,
              count(DISTINCT mv.{quote_ident(PRODUCT_ID_PROPERTY)})
                AS distinct_products,
              count(DISTINCT mv.{quote_ident(SUPPLIER_ID_PROPERTY)})
                AS distinct_suppliers,
              sum(
                CASE
                  WHEN mv.{quote_ident(PRODUCT_ID_PROPERTY)} IS NULL
                    OR mv.{quote_ident(SUPPLIER_ID_PROPERTY)} IS NULL
                  THEN 1 ELSE 0
                END
              ) AS null_keys,
              sum(
                CASE
                  WHEN $artifact_label IN labels(mv)
                   AND $stable_product_label IN labels(mv)
                  THEN 0 ELSE 1
                END
              ) AS invalid_stable_labels
            """,
            run_id=run_id,
            artifact_label=MV_ARTIFACT_LABEL,
            stable_product_label=MV_PRODUCT_LABEL,
        ),
        "validate folded shadow keys",
    )
    if (
        int(record["product_count"]) != expected_products
        or int(record["distinct_products"]) != expected_products
        or int(record["distinct_suppliers"]) != expected_suppliers
        or int(record["null_keys"]) != 0
        or int(record["invalid_stable_labels"]) != 0
    ):
        raise ExperimentValidationError(
            "Folded shadow key validation failed: "
            f"{dict(record)!r}"
        )

    validate_shadow_node_inheritance(
        session,
        run_id,
        MV_FOLDED_LABEL,
        include_supplier_properties=True,
        expected_products=expected_products,
    )
    validate_copied_topology(
        session,
        run_id,
        MV_FOLDED_LABEL,
        source_edges,
    )
    return state


def validate_normalized_shadow_state(
    session: Any,
    run_id: str,
    *,
    expected_products: int,
    expected_suppliers: int,
    source_edges: SourceEdgeCounts,
) -> ShadowGraphState:
    state = inspect_shadow_state(session, run_id)
    problems: list[str] = []
    if state.phase != "normalized":
        problems.append(f"phase={state.phase}")
    if state.normalized_product_nodes != expected_products:
        problems.append(
            f"mv_product={state.normalized_product_nodes}, "
            f"expected {expected_products}"
        )
    if state.normalized_supplier_nodes != expected_suppliers:
        problems.append(
            f"mv_supplier={state.normalized_supplier_nodes}, "
            f"expected {expected_suppliers}"
        )
    if state.join_of_product_relationships != source_edges.of_product:
        problems.append(
            f"join_of_product={state.join_of_product_relationships}, "
            f"expected {source_edges.of_product}"
        )
    if state.join_in_category_relationships != source_edges.in_category:
        problems.append(
            f"join_in_category={state.join_in_category_relationships}, "
            f"expected {source_edges.in_category}"
        )
    if state.join_supplied_by_relationships != expected_products:
        problems.append(
            f"join_supplied_by={state.join_supplied_by_relationships}, "
            f"expected {expected_products}"
        )
    if problems:
        raise ExperimentValidationError(
            "Normalized shadow state validation failed:\n  - "
            + "\n  - ".join(problems)
        )

    record, _ = one_record(
        session.run(
            f"""
            MATCH (
              p:{quote_ident(MV_PRODUCT_LABEL)}
              {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
            )
            OPTIONAL MATCH
              (p)-[r:{quote_ident(JOIN_SUPPLIED_BY_RELATIONSHIP)}]->
              (
                s:{quote_ident(MV_SUPPLIER_LABEL)}
                {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
              )
            WITH p, collect(r) AS relationships, collect(s) AS suppliers
            RETURN
              count(p) AS product_count,
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
              ) AS products_with_supplier_properties,
              sum(
                CASE
                  WHEN $artifact_label IN labels(p) THEN 0 ELSE 1
                END
              ) AS products_without_artifact_label
            """,
            run_id=run_id,
            artifact_label=MV_ARTIFACT_LABEL,
        ),
        "validate normalized shadow Product-Supplier relationships",
    )
    if (
        int(record["product_count"]) != expected_products
        or int(record["invalid_degrees"]) != 0
        or int(record["mismatched_ids"]) != 0
        or int(record["products_with_supplier_properties"]) != 0
        or int(record["products_without_artifact_label"]) != 0
    ):
        raise ExperimentValidationError(
            "Normalized shadow relationship/property validation failed: "
            f"{dict(record)!r}"
        )

    adjacent_supplier_relationships = scalar_count(
        session,
        f"""
        MATCH (
          s:{quote_ident(MV_SUPPLIER_LABEL)}
          {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
        )-[r]-()
        RETURN count(r) AS count
        """,
        "count",
        "count normalized shadow Supplier relationships",
        run_id=run_id,
    )
    if adjacent_supplier_relationships != expected_products:
        raise ExperimentValidationError(
            "mv_supplier nodes have unexpected adjacent relationships: "
            f"{adjacent_supplier_relationships}, expected {expected_products}"
        )

    validate_shadow_node_inheritance(
        session,
        run_id,
        MV_PRODUCT_LABEL,
        include_supplier_properties=False,
        expected_products=expected_products,
    )
    validate_copied_topology(
        session,
        run_id,
        MV_PRODUCT_LABEL,
        source_edges,
    )
    return state


def create_shadow_projection(
    session: Any,
    run_id: str,
    *,
    original_state: OriginalGraphState,
    source_edges: SourceEdgeCounts,
) -> dict[str, int]:
    """Create Step 0 atomically.  This function is intentionally not timed."""

    current = inspect_shadow_state(session, run_id)
    if current.phase != "clean":
        raise ExperimentStateError(
            f"cannot create Step 0 over a non-clean run: {current}"
        )

    expected = {
        "mv_product_supplier_nodes": original_state.product_nodes,
        "distinct_source_suppliers": original_state.supplier_nodes,
        "join_of_product_relationships": source_edges.of_product,
        "join_in_category_relationships": source_edges.in_category,
    }
    transaction = session.begin_transaction()
    try:
        node_record = transaction_record(
            transaction,
            f"""
            MATCH
              (p:{quote_ident(PRODUCT_LABEL)})
              -[:{quote_ident(SUPPLIED_BY_RELATIONSHIP)}]->
              (s:{quote_ident(SUPPLIER_LABEL)})
            CREATE (
              mv:{quote_ident(MV_ARTIFACT_LABEL)}:{quote_ident(MV_PRODUCT_LABEL)}:{quote_ident(MV_FOLDED_LABEL)}
            )
            SET mv = properties(p)
            SET mv += properties(s)
            SET mv.{quote_ident(RUN_ID_PROPERTY)} = $run_id
            RETURN
              count(mv) AS product_count,
              count(DISTINCT s) AS supplier_count
            """,
            "create mv_product_supplier nodes",
            run_id=run_id,
        )
        order_edge_record = transaction_record(
            transaction,
            f"""
            MATCH
              (od:{quote_ident(ORDER_DETAIL_LABEL)})
              -[source:{quote_ident(OF_PRODUCT_RELATIONSHIP)}]->
              (p:{quote_ident(PRODUCT_LABEL)})
            MATCH (
              mv:{quote_ident(MV_ARTIFACT_LABEL)}:{quote_ident(MV_PRODUCT_LABEL)}:{quote_ident(MV_FOLDED_LABEL)}
              {{
                {quote_ident(RUN_ID_PROPERTY)}: $run_id,
                {quote_ident(PRODUCT_ID_PROPERTY)}:
                  p.{quote_ident(PRODUCT_ID_PROPERTY)}
                }}
            )
            USING INDEX mv:{quote_ident(MV_PRODUCT_LABEL)}(
              {quote_ident(RUN_ID_PROPERTY)},
              {quote_ident(PRODUCT_ID_PROPERTY)})
            CREATE
              (od)-[copy:{quote_ident(JOIN_OF_PRODUCT_RELATIONSHIP)}]->(mv)
            SET copy = properties(source)
            RETURN count(copy) AS relationship_count
            """,
            "copy OF_PRODUCT as join_of_product",
            run_id=run_id,
        )
        category_edge_record = transaction_record(
            transaction,
            f"""
            MATCH
              (p:{quote_ident(PRODUCT_LABEL)})
              -[source:{quote_ident(IN_CATEGORY_RELATIONSHIP)}]->
              (c:{quote_ident(CATEGORY_LABEL)})
            MATCH (
              mv:{quote_ident(MV_ARTIFACT_LABEL)}:{quote_ident(MV_PRODUCT_LABEL)}:{quote_ident(MV_FOLDED_LABEL)}
              {{
                {quote_ident(RUN_ID_PROPERTY)}: $run_id,
                {quote_ident(PRODUCT_ID_PROPERTY)}:
                  p.{quote_ident(PRODUCT_ID_PROPERTY)}
                }}
            )
            USING INDEX mv:{quote_ident(MV_PRODUCT_LABEL)}(
              {quote_ident(RUN_ID_PROPERTY)},
              {quote_ident(PRODUCT_ID_PROPERTY)})
            CREATE
              (mv)-[copy:{quote_ident(JOIN_IN_CATEGORY_RELATIONSHIP)}]->(c)
            SET copy = properties(source)
            RETURN count(copy) AS relationship_count
            """,
            "copy IN_CATEGORY as join_in_category",
            run_id=run_id,
        )
        counts = {
            "mv_product_supplier_nodes": int(node_record["product_count"]),
            "distinct_source_suppliers": int(node_record["supplier_count"]),
            "join_of_product_relationships": int(
                order_edge_record["relationship_count"]
            ),
            "join_in_category_relationships": int(
                category_edge_record["relationship_count"]
            ),
        }
        if counts != expected:
            raise ExperimentValidationError(
                "Step 0 counters did not match source topology before "
                f"commit: actual={counts!r}, expected={expected!r}"
            )
        transaction.commit()
    except BaseException:
        try:
            transaction.rollback()
        except BaseException:
            pass
        raise

    validate_folded_shadow_state(
        session,
        run_id,
        expected_products=original_state.product_nodes,
        expected_suppliers=original_state.supplier_nodes,
        source_edges=source_edges,
    )
    return counts


def matching_range_indexes(
    session: Any,
    label: str,
    property_names: Sequence[str],
) -> list[dict[str, str]]:
    result = session.run(
        """
        SHOW INDEXES
        YIELD name, state, type, entityType, labelsOrTypes, properties
        RETURN name, state, type, entityType, labelsOrTypes, properties
        """
    )
    records, _ = result_records_and_summary(result)
    expected_properties = tuple(property_names)
    matches: list[dict[str, str]] = []
    for record in records:
        labels = tuple(str(value) for value in record["labelsOrTypes"] or ())
        properties = tuple(str(value) for value in record["properties"] or ())
        if (
            str(record["type"]).upper() == "RANGE"
            and str(record["entityType"]).upper() == "NODE"
            and labels == (label,)
            and properties == expected_properties
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
    property_names: Sequence[str],
    index_wait_seconds: int,
) -> None:
    session.run(
        "CALL db.awaitIndex($index_name, $timeout_seconds)",
        index_name=index_name,
        timeout_seconds=index_wait_seconds,
    ).consume()
    matches = matching_range_indexes(session, label, property_names)
    matching_names = {index["name"]: index["state"] for index in matches}
    state = matching_names.get(index_name)
    if state != "ONLINE":
        properties = ", ".join(property_names)
        raise ExperimentValidationError(
            f"RANGE index {index_name!r} for "
            f"{label}({properties}) is not ONLINE; state={state!r}"
        )


def require_range_index(
    session: Any,
    *,
    label: str,
    property_names: Sequence[str],
    index_wait_seconds: int,
) -> str:
    matches = matching_range_indexes(session, label, property_names)
    if not matches:
        properties = ", ".join(property_names)
        raise ExperimentValidationError(
            f"no RANGE index exists for {label}({properties}); "
            "run case_study/data/import_data.py to create the expected "
            "Northwind constraints"
        )
    index_name = matches[0]["name"]
    await_and_validate_range_index(
        session,
        index_name=index_name,
        label=label,
        property_names=property_names,
        index_wait_seconds=index_wait_seconds,
    )
    return index_name


def ensure_experiment_range_index(
    session: Any,
    *,
    label: str,
    property_names: Sequence[str],
    prefix: str,
    index_wait_seconds: int,
    lease: IndexLease,
) -> IndexLease:
    existing = matching_range_indexes(session, label, property_names)
    if existing:
        lease.name = existing[0]["name"]
        lease.created_by_experiment = False
    else:
        lease.name = f"{prefix}_{uuid4().hex[:12]}"
        # Record ownership before CREATE.  If the server commits and the driver
        # subsequently errors, failure cleanup can still target this name.
        lease.created_by_experiment = True
        properties = ", ".join(
            f"n.{quote_ident(property_name)}"
            for property_name in property_names
        )
        session.run(
            f"""
            CREATE INDEX {quote_ident(lease.name)}
            FOR (n:{quote_ident(label)})
            ON ({properties})
            """
        ).consume()

    await_and_validate_range_index(
        session,
        index_name=lease.name,
        label=label,
        property_names=property_names,
        index_wait_seconds=index_wait_seconds,
    )
    return lease


def drop_experiment_index(session: Any, lease: IndexLease) -> None:
    if not lease.created_by_experiment or lease.name is None:
        return
    session.run(f"DROP INDEX {quote_ident(lease.name)} IF EXISTS").consume()


def matching_uniqueness_constraints(
    session: Any,
    label: str,
    property_names: Sequence[str],
) -> list[str]:
    result = session.run(
        """
        SHOW CONSTRAINTS
        YIELD name, type, entityType, labelsOrTypes, properties
        RETURN name, type, entityType, labelsOrTypes, properties
        """
    )
    records, _ = result_records_and_summary(result)
    expected_properties = tuple(property_names)
    names: list[str] = []
    for record in records:
        labels = tuple(str(value) for value in record["labelsOrTypes"] or ())
        properties = tuple(str(value) for value in record["properties"] or ())
        constraint_type = str(record["type"]).upper()
        if (
            str(record["entityType"]).upper() == "NODE"
            and labels == (label,)
            and properties == expected_properties
            and "UNIQUENESS" in constraint_type
        ):
            names.append(str(record["name"]))
    return sorted(names)


def ensure_experiment_uniqueness_constraint(
    session: Any,
    *,
    label: str,
    property_names: Sequence[str],
    prefix: str,
    index_wait_seconds: int,
    lease: ConstraintLease,
) -> ConstraintLease:
    existing = matching_uniqueness_constraints(
        session,
        label,
        property_names,
    )
    if existing:
        lease.name = existing[0]
        lease.created_by_experiment = False
    else:
        lease.name = f"{prefix}_{uuid4().hex[:12]}"
        lease.created_by_experiment = True
        if len(property_names) == 1:
            property_expression = (
                f"n.{quote_ident(property_names[0])}"
            )
        else:
            property_expression = (
                "("
                + ", ".join(
                    f"n.{quote_ident(property_name)}"
                    for property_name in property_names
                )
                + ")"
            )
        session.run(
            f"""
            CREATE CONSTRAINT {quote_ident(lease.name)}
            FOR (n:{quote_ident(label)})
            REQUIRE {property_expression} IS UNIQUE
            """
        ).consume()

    session.run(
        "CALL db.awaitIndexes($timeout_seconds)",
        timeout_seconds=index_wait_seconds,
    ).consume()
    matching_constraints = matching_uniqueness_constraints(
        session,
        label,
        property_names,
    )
    if lease.name not in matching_constraints:
        properties = ", ".join(property_names)
        raise ExperimentValidationError(
            f"uniqueness constraint {lease.name!r} for "
            f"{label}({properties}) is unavailable"
        )
    backing_indexes = matching_range_indexes(
        session,
        label,
        property_names,
    )
    if not backing_indexes or backing_indexes[0]["state"] != "ONLINE":
        raise ExperimentValidationError(
            f"backing RANGE index for uniqueness constraint "
            f"{lease.name!r} is not ONLINE"
        )
    return lease


def drop_experiment_constraint(
    session: Any,
    lease: ConstraintLease,
) -> None:
    if not lease.created_by_experiment or lease.name is None:
        return
    session.run(
        f"DROP CONSTRAINT {quote_ident(lease.name)} IF EXISTS"
    ).consume()


def folded_rows_query() -> str:
    projections = [
        f"mv.{quote_ident(PRODUCT_ID_PROPERTY)} "
        f"AS {quote_ident(PRODUCT_ID_PROPERTY)}",
        f"mv.{quote_ident(SUPPLIER_ID_PROPERTY)} "
        f"AS {quote_ident(SUPPLIER_ID_PROPERTY)}",
    ]
    projections.extend(
        f"mv.{quote_ident(attribute)} AS {quote_ident(attribute)}"
        for attribute in SUPPLIER_ATTRIBUTES
    )
    return (
        f"MATCH (mv:{quote_ident(MV_FOLDED_LABEL)} "
        f"{{{quote_ident(RUN_ID_PROPERTY)}: $run_id}})\n"
        "RETURN\n  "
        + ",\n  ".join(projections)
        + f"\nORDER BY mv.{quote_ident(PRODUCT_ID_PROPERTY)}"
    )


def fetch_folded_rows(session: Any, run_id: str) -> list[dict[str, Any]]:
    result = session.run(folded_rows_query(), run_id=run_id)
    records, _ = result_records_and_summary(result)
    fields = (
        PRODUCT_ID_PROPERTY,
        SUPPLIER_ID_PROPERTY,
        *SUPPLIER_ATTRIBUTES,
    )
    return [{field: record[field] for field in fields} for record in records]


def fetch_original_join_rows(session: Any) -> list[dict[str, Any]]:
    projections = [
        f"p.{quote_ident(PRODUCT_ID_PROPERTY)} "
        f"AS {quote_ident(PRODUCT_ID_PROPERTY)}",
        f"p.{quote_ident(SUPPLIER_ID_PROPERTY)} "
        f"AS {quote_ident(SUPPLIER_ID_PROPERTY)}",
    ]
    projections.extend(
        f"s.{quote_ident(attribute)} AS {quote_ident(attribute)}"
        for attribute in SUPPLIER_ATTRIBUTES
    )
    result = session.run(
        f"""
        MATCH
          (p:{quote_ident(PRODUCT_LABEL)})
          -[:{quote_ident(SUPPLIED_BY_RELATIONSHIP)}]->
          (s:{quote_ident(SUPPLIER_LABEL)})
        RETURN
          {", ".join(projections)}
        ORDER BY p.{quote_ident(PRODUCT_ID_PROPERTY)}
        """
    )
    records, _ = result_records_and_summary(result)
    fields = (
        PRODUCT_ID_PROPERTY,
        SUPPLIER_ID_PROPERTY,
        *SUPPLIER_ATTRIBUTES,
    )
    return [{field: record[field] for field in fields} for record in records]


def relevant_data_sha256(rows: Sequence[dict[str, Any]]) -> str:
    fields = (
        PRODUCT_ID_PROPERTY,
        SUPPLIER_ID_PROPERTY,
        *SUPPLIER_ATTRIBUTES,
    )
    canonical = [
        {field: row[field] for field in fields}
        for row in sorted(rows, key=lambda row: row[PRODUCT_ID_PROPERTY])
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
                "normalized shadow Supplier nodes"
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


def fetch_original_supplier_snapshot(
    session: Any,
) -> dict[Any, dict[str, Any]]:
    projections = [
        f"s.{quote_ident(SUPPLIER_ID_PROPERTY)} "
        f"AS {quote_ident(SUPPLIER_ID_PROPERTY)}",
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


def fetch_shadow_supplier_snapshot(
    session: Any,
    run_id: str,
) -> dict[Any, dict[str, Any]]:
    projections = [
        f"s.{quote_ident(SUPPLIER_ID_PROPERTY)} "
        f"AS {quote_ident(SUPPLIER_ID_PROPERTY)}",
        *(
            f"s.{quote_ident(attribute)} AS {quote_ident(attribute)}"
            for attribute in SUPPLIER_ATTRIBUTES
        ),
    ]
    result = session.run(
        f"""
        MATCH (
          s:{quote_ident(MV_SUPPLIER_LABEL)}
          {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
        )
        RETURN
          {", ".join(projections)}
        ORDER BY s.{quote_ident(SUPPLIER_ID_PROPERTY)}
        """,
        run_id=run_id,
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
        raise ExperimentValidationError(
            "the folded shadow projection has no Product rows"
        )

    product_ids = [row[PRODUCT_ID_PROPERTY] for row in rows]
    if len(product_ids) != len(set(product_ids)):
        raise ExperimentValidationError(
            "folded shadow rows contain duplicate ProductID values"
        )

    groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        supplier_id = row[SUPPLIER_ID_PROPERTY]
        if supplier_id is None:
            raise ExperimentValidationError(
                "folded shadow Product has NULL SupplierID"
            )
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
    redundant_non_null_values = (
        baseline_non_null_values - normalized_non_null_values
    )
    redundant_payload_bytes = baseline_payload_bytes - normalized_payload_bytes
    fanout_histogram = Counter(len(copies) for copies in groups.values())

    return {
        "scope": "logical redundancy within the run-scoped shadow projection",
        "functional_dependency": (
            f"{SUPPLIER_ID_PROPERTY} -> " + ", ".join(SUPPLIER_ATTRIBUTES)
        ),
        "dependent_attributes": list(SUPPLIER_ATTRIBUTES),
        "dependent_attribute_count": dependent_attribute_count,
        "product_occurrences": product_occurrences,
        "distinct_suppliers": distinct_suppliers,
        "average_products_per_supplier": product_occurrences / distinct_suppliers,
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
            "indexes, nodes, relationships, and Neo4j store overhead.  The "
            "original normalized graph coexists during this experiment."
        ),
    }


def print_redundancy(metrics: dict[str, Any]) -> None:
    print(
        "  Shadow Product occurrences: "
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
        for products, suppliers
        in metrics["fanout_by_products_per_supplier"].items()
    )
    print(f"  Supplier fanout histogram: {fanout}")


def domain_value_token(value: Any) -> str:
    """Return a deterministic, type-aware token for an observed property."""

    if isinstance(value, float) and not math.isfinite(value):
        raise ExperimentValidationError(
            "active-domain rotation does not support NaN or infinite floats"
        )
    value_type = f"{type(value).__module__}.{type(value).__qualname__}"
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return f"{value_type}:{payload}"


def canonical_folded_values(
    rows: Sequence[dict[str, Any]],
    property_name: str,
) -> dict[Any, Any]:
    values_by_supplier: dict[Any, dict[str, Any]] = defaultdict(dict)
    for row in rows:
        value = row[property_name]
        values_by_supplier[row[SUPPLIER_ID_PROPERTY]][
            domain_value_token(value)
        ] = value

    canonical: dict[Any, Any] = {}
    for supplier_id, values_by_token in values_by_supplier.items():
        if len(values_by_token) != 1:
            raise ExperimentValidationError(
                f"SupplierID={supplier_id!r} has inconsistent "
                f"{property_name}: {list(values_by_token.values())!r}"
            )
        canonical[supplier_id] = next(iter(values_by_token.values()))
    return canonical


def sha256_of_tokens(tokens: Sequence[Any]) -> str:
    payload = json.dumps(
        tokens,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_active_domain_update_workload(
    originals: dict[Any, Any],
    property_name: str,
    *,
    active_domain_values: Sequence[Any] | None = None,
    active_domain_scope: str = "updated_logical_tuples",
) -> UpdateWorkload:
    """Map every logical tuple to the next distinct observed domain value."""

    if property_name not in UPDATE_PROPERTIES:
        raise ExperimentValidationError(
            f"{property_name!r} is not a selectable non-key Supplier attribute"
        )
    if not originals:
        raise ExperimentValidationError(
            f"cannot build an update workload for empty {property_name}"
        )
    domain_source = list(
        originals.values()
        if active_domain_values is None
        else active_domain_values
    )
    if not domain_source:
        raise ExperimentValidationError(
            f"cannot build an active domain for empty {property_name}"
        )

    null_supplier_ids = [
        supplier_id
        for supplier_id, value in originals.items()
        if value is None
    ]
    if null_supplier_ids:
        preview = sorted(
            null_supplier_ids,
            key=domain_value_token,
        )[:10]
        suffix = (
            ""
            if len(null_supplier_ids) <= len(preview)
            else ", additional SupplierID values omitted"
        )
        raise ExperimentValidationError(
            f"{property_name} contains NULL for "
            f"{len(null_supplier_ids)} logical Supplier tuple(s), including "
            f"{preview!r}{suffix}. NULL property creation/removal must be "
            "measured as a separate workload."
        )
    domain_null_count = sum(value is None for value in domain_source)
    if domain_null_count:
        raise ExperimentValidationError(
            f"{property_name} contains NULL in {domain_null_count} of "
            f"{len(domain_source)} tuple(s) in active-domain scope "
            f"{active_domain_scope!r}. NULL property creation/removal must "
            "be measured as a separate workload."
        )

    value_types = {
        f"{type(value).__module__}.{type(value).__qualname__}"
        for value in domain_source
    }
    if len(value_types) != 1:
        raise ExperimentValidationError(
            f"{property_name} has mixed active-domain types: "
            f"{sorted(value_types)!r}"
        )
    value_type = next(iter(value_types))

    values_by_token: dict[str, Any] = {}
    for value in domain_source:
        values_by_token[domain_value_token(value)] = value
    ordered_tokens = sorted(values_by_token)
    active_domain = [values_by_token[token] for token in ordered_tokens]
    if len(active_domain) < 2:
        raise ExperimentValidationError(
            f"{property_name} active domain contains "
            f"{len(active_domain)} distinct non-NULL value(s); at least two "
            "are required for a value-changing update."
        )

    successor_by_token = {
        token: active_domain[(index + 1) % len(active_domain)]
        for index, token in enumerate(ordered_tokens)
    }
    missing_original_tokens = sorted(
        {
            domain_value_token(value)
            for value in originals.values()
            if domain_value_token(value) not in values_by_token
        }
    )
    if missing_original_tokens:
        raise ExperimentValidationError(
            f"{property_name} update values are missing from the selected "
            f"active-domain scope: {missing_original_tokens[:10]!r}"
        )
    ordered_originals = sorted(
        originals.items(),
        key=lambda item: domain_value_token(item[0]),
    )
    updates: list[dict[str, Any]] = []
    restores: list[dict[str, Any]] = []
    mapping_tokens: list[dict[str, str]] = []
    for supplier_id, original in ordered_originals:
        original_token = domain_value_token(original)
        updated = successor_by_token[original_token]
        updated_token = domain_value_token(updated)
        if updated_token == original_token:
            raise ExperimentValidationError(
                f"{property_name} update for SupplierID={supplier_id!r} "
                "would not change the value"
            )
        updates.append(
            {
                SUPPLIER_ID_PROPERTY: supplier_id,
                "value": updated,
            }
        )
        restores.append(
            {
                SUPPLIER_ID_PROPERTY: supplier_id,
                "value": original,
            }
        )
        mapping_tokens.append(
            {
                "key": domain_value_token(supplier_id),
                "original": original_token,
                "updated": updated_token,
            }
        )

    return UpdateWorkload(
        property_name=property_name,
        originals=dict(originals),
        updates=updates,
        restores=restores,
        active_domain_size=len(active_domain),
        active_domain_scope=active_domain_scope,
        active_domain_source_tuple_count=len(domain_source),
        active_domain_sha256=sha256_of_tokens(ordered_tokens),
        mapping_sha256=sha256_of_tokens(mapping_tokens),
        value_type=value_type,
        logical_original_payload_bytes=sum(
            value_payload_bytes(value) for value in originals.values()
        ),
        logical_updated_payload_bytes=sum(
            value_payload_bytes(row["value"]) for row in updates
        ),
    )


def calculate_update_payload_metrics(
    joined_rows: Sequence[dict[str, Any]],
    workload: UpdateWorkload,
) -> dict[str, Any]:
    """Calculate logical and join-fanout-weighted value payload proxies."""

    updated_by_supplier = {
        row[SUPPLIER_ID_PROPERTY]: row["value"]
        for row in workload.updates
    }
    if set(updated_by_supplier) != set(workload.originals):
        raise ExperimentValidationError(
            "update workload keys are inconsistent while calculating payload "
            "metrics"
        )

    folded_original_bytes = 0
    folded_updated_bytes = 0
    folded_supplier_ids: set[Any] = set()
    for row in joined_rows:
        supplier_id = row[SUPPLIER_ID_PROPERTY]
        if supplier_id not in workload.originals:
            raise ExperimentValidationError(
                f"joined row has SupplierID={supplier_id!r} outside the "
                "update workload"
            )
        original = row[workload.property_name]
        if (
            domain_value_token(original)
            != domain_value_token(workload.originals[supplier_id])
        ):
            raise ExperimentValidationError(
                f"joined row has unexpected {workload.property_name} for "
                f"SupplierID={supplier_id!r}"
            )
        folded_supplier_ids.add(supplier_id)
        folded_original_bytes += value_payload_bytes(original)
        folded_updated_bytes += value_payload_bytes(
            updated_by_supplier[supplier_id]
        )

    missing_supplier_ids = set(workload.originals) - folded_supplier_ids
    if missing_supplier_ids:
        raise ExperimentValidationError(
            "update workload contains SupplierID values absent from the join: "
            f"{sorted(missing_supplier_ids, key=domain_value_token)[:10]!r}"
        )

    return {
        "metric": (
            "UTF-8 bytes for strings and UTF-8 bytes of str(value) for other "
            "types; this is a value-payload proxy, not Neo4j store bytes"
        ),
        "normalized_logical": {
            "property_writes": workload.logical_tuple_count,
            "original_payload_bytes": (
                workload.logical_original_payload_bytes
            ),
            "updated_payload_bytes": (
                workload.logical_updated_payload_bytes
            ),
            "payload_delta_bytes": workload.logical_payload_delta_bytes,
        },
        "folded_join_fanout_weighted": {
            "property_writes": len(joined_rows),
            "original_payload_bytes": folded_original_bytes,
            "updated_payload_bytes": folded_updated_bytes,
            "payload_delta_bytes": (
                folded_updated_bytes - folded_original_bytes
            ),
        },
    }


def update_query(label: str, property_name: str) -> str:
    return f"""
    UNWIND $updates AS update
    MATCH (
      n:{quote_ident(label)}
      {{
        {quote_ident(RUN_ID_PROPERTY)}: $run_id,
        {quote_ident(SUPPLIER_ID_PROPERTY)}:
          update.{quote_ident(SUPPLIER_ID_PROPERTY)}
      }}
    )
    USING INDEX n:{quote_ident(label)}(
      {quote_ident(RUN_ID_PROPERTY)},
      {quote_ident(SUPPLIER_ID_PROPERTY)})
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
        relationships_created=int(
            getattr(counters, "relationships_created", 0)
        ),
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
    *,
    run_id: str,
    label: str,
    property_name: str,
    originals: dict[Any, Any],
    expected_nodes: int,
    context: str,
) -> None:
    result = session.run(
        f"""
        MATCH (
          n:{quote_ident(label)}
          {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
        )
        RETURN
          n.{quote_ident(SUPPLIER_ID_PROPERTY)} AS supplier_id,
          n.{quote_ident(property_name)} AS property_value
        """,
        run_id=run_id,
    )
    records, _ = result_records_and_summary(result)
    if len(records) != expected_nodes:
        raise ExperimentValidationError(
            f"{label}: expected {expected_nodes} nodes while verifying "
            f"{property_name}, found {len(records)}"
        )
    for record in records:
        supplier_id = record["supplier_id"]
        if supplier_id not in originals:
            raise ExperimentValidationError(
                f"{label} has unexpected SupplierID={supplier_id!r} while "
                f"verifying {property_name}"
            )
        expected = originals[supplier_id]
        if record["property_value"] != expected:
            raise ExperimentValidationError(
                f"{label} SupplierID={supplier_id!r} has unexpected "
                f"{property_name}: expected {expected!r}, "
                f"found {record['property_value']!r} ({context})"
            )


def benchmark_updates(
    session: Any,
    *,
    run_id: str,
    phase_name: str,
    label: str,
    workload: UpdateWorkload,
    expected_nodes: int,
    warmup_runs: int,
    measured_runs: int,
) -> list[TimingSample]:
    query = update_query(label, workload.property_name)
    updated_values = {
        row[SUPPLIER_ID_PROPERTY]: row["value"]
        for row in workload.updates
    }
    samples: list[TimingSample] = []
    total_runs = warmup_runs + measured_runs
    successor_validation_ordinal = (
        1 if warmup_runs > 0 else total_runs
    )

    for ordinal in range(1, total_runs + 1):
        measured_number = ordinal - warmup_runs
        try:
            sample = timed_query(
                session,
                query,
                {"run_id": run_id, "updates": workload.updates},
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
            if ordinal == successor_validation_ordinal:
                verify_current_values(
                    session,
                    run_id=run_id,
                    label=label,
                    property_name=workload.property_name,
                    originals=updated_values,
                    expected_nodes=expected_nodes,
                    context=(
                        f"{phase_name} active-domain successor validation"
                    ),
                )
        except BaseException:
            # The autocommit write can have committed even if receiving the
            # result summary failed, so always attempt the run-scoped restore.
            try:
                restore_result = session.run(
                    query,
                    run_id=run_id,
                    updates=workload.restores,
                )
                one_record(restore_result, f"{phase_name} best-effort restore")
            except BaseException as restore_error:
                print(
                    f"WARNING: {phase_name} in-session restore failed: "
                    f"{type(restore_error).__name__}: {restore_error}",
                    file=sys.stderr,
                )
            raise
        else:
            restore_result = session.run(
                query,
                run_id=run_id,
                updates=workload.restores,
            )
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
        run_id=run_id,
        label=label,
        property_name=workload.property_name,
        originals=workload.originals,
        expected_nodes=expected_nodes,
        context=f"{phase_name} final restore",
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
        else (
            f"median={server['median']:.3f} ms, "
            f"mean={server['mean']:.3f} ms"
        )
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
        f"{quote_ident(attribute)}: "
        f"{source_alias}.{quote_ident(attribute)}"
        for attribute in SUPPLIER_ATTRIBUTES
    )
    return "{" + assignments + "}"


def supplier_copy_set_clause(
    target_alias: str,
    source_alias: str,
) -> str:
    assignments = ",\n      ".join(
        f"{target_alias}.{quote_ident(attribute)} = "
        f"{source_alias}.{quote_ident(attribute)}"
        for attribute in SUPPLIER_ATTRIBUTES
    )
    return f"SET {assignments}"


def normalization_query() -> str:
    remove_items = [
        f"p.{quote_ident(attribute)}" for attribute in SUPPLIER_ATTRIBUTES
    ]
    remove_items.append(f"p:{quote_ident(MV_FOLDED_LABEL)}")
    return f"""
    MATCH (
      folded:{quote_ident(MV_ARTIFACT_LABEL)}:{quote_ident(MV_FOLDED_LABEL)}
      {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
    )
    WITH
      folded.{quote_ident(SUPPLIER_ID_PROPERTY)} AS supplier_id,
      head(collect(folded)) AS representative
    CREATE (
      supplier:{quote_ident(MV_SUPPLIER_LABEL)}
      {{
        {quote_ident(RUN_ID_PROPERTY)}: $run_id,
        {quote_ident(SUPPLIER_ID_PROPERTY)}: supplier_id
      }}
    )
    SET supplier += {supplier_property_map("representative")}
    WITH count(supplier) AS supplier_count
    MATCH (
      p:{quote_ident(MV_ARTIFACT_LABEL)}:{quote_ident(MV_FOLDED_LABEL)}
      {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
    )
    MATCH (
      s:{quote_ident(MV_SUPPLIER_LABEL)}
      {{
        {quote_ident(RUN_ID_PROPERTY)}: $run_id,
        {quote_ident(SUPPLIER_ID_PROPERTY)}:
          p.{quote_ident(SUPPLIER_ID_PROPERTY)}
      }}
    )
    USING INDEX s:{quote_ident(MV_SUPPLIER_LABEL)}(
      {quote_ident(RUN_ID_PROPERTY)},
      {quote_ident(SUPPLIER_ID_PROPERTY)})
    CREATE (p)-[:{quote_ident(JOIN_SUPPLIED_BY_RELATIONSHIP)}]->(s)
    REMOVE {", ".join(remove_items)}
    RETURN supplier_count, count(p) AS product_count
    """


def normalize_once(
    session: Any,
    *,
    run_id: str,
    run: int,
    expected_products: int,
    expected_suppliers: int,
    source_edges: SourceEdgeCounts,
) -> TimingSample:
    # Step 3 uses an explicit transaction so cardinality/counter failures can
    # still roll back.  The measured wall time includes begin/run/consume and
    # commit, while excluding the small Python validation interval between
    # consume and commit.
    started_ns = perf_counter_ns()
    transaction = session.begin_transaction()
    try:
        result = transaction.run(normalization_query(), run_id=run_id)
        records, summary = result_records_and_summary(result)
        before_validation_ns = perf_counter_ns()
        if len(records) != 1:
            raise ExperimentValidationError(
                "Step 3 shadow normalization: expected one result record, "
                f"found {len(records)}"
            )
        product_count = int(records[0]["product_count"])
        supplier_count = int(records[0]["supplier_count"])
        provisional = make_timing_sample(
            run=run,
            wall_ms=0.0,
            summary=summary,
            affected_nodes=product_count,
        )
        if product_count != expected_products:
            raise ExperimentValidationError(
                f"Step 3 normalized {product_count} shadow Product nodes; "
                f"expected {expected_products}"
            )
        if supplier_count != expected_suppliers:
            raise ExperimentValidationError(
                f"Step 3 created {supplier_count} mv_supplier nodes; "
                f"expected {expected_suppliers}"
            )
        if provisional.nodes_created != expected_suppliers:
            raise ExperimentValidationError(
                "Step 3 Neo4j counter reported "
                f"{provisional.nodes_created} new mv_supplier nodes; "
                f"expected {expected_suppliers}"
            )
        if provisional.relationships_created != expected_products:
            raise ExperimentValidationError(
                "Step 3 Neo4j counter reported "
                f"{provisional.relationships_created} relationships; "
                f"expected {expected_products}"
            )
        commit_started_ns = perf_counter_ns()
        transaction.commit()
        finished_ns = perf_counter_ns()
    except BaseException:
        try:
            transaction.rollback()
        except BaseException:
            pass
        raise

    measured_ns = (
        before_validation_ns - started_ns
        + finished_ns - commit_started_ns
    )
    sample = make_timing_sample(
        run=run,
        wall_ms=measured_ns / 1_000_000,
        summary=summary,
        affected_nodes=product_count,
    )

    validate_normalized_shadow_state(
        session,
        run_id,
        expected_products=expected_products,
        expected_suppliers=expected_suppliers,
        source_edges=source_edges,
    )
    return sample


def refold_shadow_projection(
    session: Any,
    run_id: str,
    *,
    expected_products: int,
    expected_suppliers: int,
    source_edges: SourceEdgeCounts,
) -> None:
    """Undo Step 3 outside the timer while retaining copied query topology."""

    current = inspect_shadow_state(session, run_id)
    if current.phase != "normalized":
        raise ExperimentStateError(
            f"cannot refold a non-normalized shadow graph: {current}"
        )

    expected = {
        "products": expected_products,
        "suppliers_read": expected_suppliers,
        "relationships_deleted": expected_products,
        "suppliers_deleted": expected_suppliers,
    }
    transaction = session.begin_transaction()
    try:
        product_record = transaction_record(
            transaction,
            f"""
            MATCH
              (
                p:{quote_ident(MV_ARTIFACT_LABEL)}:{quote_ident(MV_PRODUCT_LABEL)}
                {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
              )
              -[:{quote_ident(JOIN_SUPPLIED_BY_RELATIONSHIP)}]->
              (
                s:{quote_ident(MV_SUPPLIER_LABEL)}
                {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
            )
            {supplier_copy_set_clause("p", "s")}
            SET p:{quote_ident(MV_FOLDED_LABEL)}
            RETURN
              count(p) AS product_count,
              count(DISTINCT s) AS supplier_count
            """,
            "refold shadow Supplier properties",
            run_id=run_id,
        )
        relationship_record = transaction_record(
            transaction,
            f"""
            MATCH
              (
                p:{quote_ident(MV_ARTIFACT_LABEL)}:{quote_ident(MV_FOLDED_LABEL)}
                {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
              )
              -[r:{quote_ident(JOIN_SUPPLIED_BY_RELATIONSHIP)}]->
              (
                s:{quote_ident(MV_SUPPLIER_LABEL)}
                {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
              )
            WITH collect(r) AS relationships
            FOREACH (relationship IN relationships | DELETE relationship)
            RETURN size(relationships) AS relationship_count
            """,
            "delete join_supplied_by during refold",
            run_id=run_id,
        )
        supplier_record = transaction_record(
            transaction,
            f"""
            MATCH (
              s:{quote_ident(MV_SUPPLIER_LABEL)}
              {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
            )
            WITH collect(s) AS suppliers
            FOREACH (supplier IN suppliers | DELETE supplier)
            RETURN size(suppliers) AS supplier_count
            """,
            "delete mv_supplier during refold",
            run_id=run_id,
        )
        counts = {
            "products": int(product_record["product_count"]),
            "suppliers_read": int(product_record["supplier_count"]),
            "relationships_deleted": int(
                relationship_record["relationship_count"]
            ),
            "suppliers_deleted": int(supplier_record["supplier_count"]),
        }
        if counts != expected:
            raise ExperimentValidationError(
                "shadow refold counters were wrong before commit: "
                f"actual={counts!r}, expected={expected!r}"
            )
        transaction.commit()
    except BaseException:
        try:
            transaction.rollback()
        except BaseException:
            pass
        raise

    validate_folded_shadow_state(
        session,
        run_id,
        expected_products=expected_products,
        expected_suppliers=expected_suppliers,
        source_edges=source_edges,
    )


def cleanup_shadow_artifacts(
    session: Any,
    run_id: str,
) -> CleanupResult:
    """Delete only known shadow shapes for this run in one transaction.

    Relationships are deleted with their exact direction, type, and endpoint
    labels.  The nodes are then removed with ordinary ``DELETE`` rather than
    ``DETACH DELETE``.  An unexpected or concurrently attached relationship
    therefore makes the transaction roll back instead of being erased.
    """

    transaction = session.begin_transaction()
    try:
        node_record = transaction_record(
            transaction,
            f"""
            MATCH (n)
            WHERE any(
              label IN labels(n)
              WHERE label IN $artifact_labels
            )
              AND n.{quote_ident(RUN_ID_PROPERTY)} = $run_id
            RETURN count(n) AS count
            """,
            "count shadow nodes before cleanup",
            artifact_labels=[
                MV_ARTIFACT_LABEL,
                MV_FOLDED_LABEL,
                MV_PRODUCT_LABEL,
                MV_SUPPLIER_LABEL,
            ],
            run_id=run_id,
        )
        deleted_nodes = int(node_record["count"])
        if deleted_nodes == 0:
            transaction.commit()
            return CleanupResult(
                deleted_nodes=0,
                deleted_relationships=0,
            )

        relationship_record = transaction_record(
            transaction,
            f"""
            MATCH (n)-[r]-()
            WHERE any(
              label IN labels(n)
              WHERE label IN $artifact_labels
            )
              AND n.{quote_ident(RUN_ID_PROPERTY)} = $run_id
            RETURN count(DISTINCT r) AS count
            """,
            "count shadow relationships before cleanup",
            artifact_labels=[
                MV_ARTIFACT_LABEL,
                MV_FOLDED_LABEL,
                MV_PRODUCT_LABEL,
                MV_SUPPLIER_LABEL,
            ],
            run_id=run_id,
        )
        deleted_relationships = int(relationship_record["count"])

        order_record = transaction_record(
            transaction,
            f"""
            MATCH
              (:{quote_ident(ORDER_DETAIL_LABEL)})
              -[r:{quote_ident(JOIN_OF_PRODUCT_RELATIONSHIP)}]->
              (
                n:{quote_ident(MV_ARTIFACT_LABEL)}
                {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
              )
            WITH collect(DISTINCT r) AS relationships
            FOREACH (relationship IN relationships | DELETE relationship)
            RETURN size(relationships) AS count
            """,
            "delete join_of_product relationships",
            run_id=run_id,
        )
        category_record = transaction_record(
            transaction,
            f"""
            MATCH
              (
                n:{quote_ident(MV_ARTIFACT_LABEL)}
                {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
              )
              -[r:{quote_ident(JOIN_IN_CATEGORY_RELATIONSHIP)}]->
              (:{quote_ident(CATEGORY_LABEL)})
            WITH collect(DISTINCT r) AS relationships
            FOREACH (relationship IN relationships | DELETE relationship)
            RETURN size(relationships) AS count
            """,
            "delete join_in_category relationships",
            run_id=run_id,
        )
        supplier_record = transaction_record(
            transaction,
            f"""
            MATCH
              (
                p:{quote_ident(MV_ARTIFACT_LABEL)}
                {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
              )
              -[r:{quote_ident(JOIN_SUPPLIED_BY_RELATIONSHIP)}]->
              (
                s:{quote_ident(MV_SUPPLIER_LABEL)}
                {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
              )
            WITH collect(DISTINCT r) AS relationships
            FOREACH (relationship IN relationships | DELETE relationship)
            RETURN size(relationships) AS count
            """,
            "delete join_supplied_by relationships",
            run_id=run_id,
        )
        explicitly_deleted_relationships = sum(
            int(record["count"])
            for record in (
                order_record,
                category_record,
                supplier_record,
            )
        )
        if explicitly_deleted_relationships != deleted_relationships:
            raise ExperimentStateError(
                "cleanup found a relationship with an unexpected type, "
                "direction, endpoint, or run_id: "
                f"adjacent={deleted_relationships}, "
                f"recognized={explicitly_deleted_relationships}"
            )

        deleted_node_record = transaction_record(
            transaction,
            f"""
            MATCH (n)
            WHERE any(
              label IN labels(n)
              WHERE label IN $artifact_labels
            )
              AND n.{quote_ident(RUN_ID_PROPERTY)} = $run_id
            WITH collect(n) AS nodes
            FOREACH (node IN nodes | DELETE node)
            RETURN size(nodes) AS count
            """,
            "delete shadow nodes",
            artifact_labels=[
                MV_ARTIFACT_LABEL,
                MV_FOLDED_LABEL,
                MV_PRODUCT_LABEL,
                MV_SUPPLIER_LABEL,
            ],
            run_id=run_id,
        )
        if int(deleted_node_record["count"]) != deleted_nodes:
            raise ExperimentValidationError(
                "cleanup shadow node count changed within its transaction"
            )
        transaction.commit()
    except BaseException:
        try:
            transaction.rollback()
        except BaseException:
            pass
        raise

    state = inspect_shadow_state(session, run_id)
    if state.phase != "clean":
        raise ExperimentValidationError(
            f"shadow cleanup left artifacts for run {run_id!r}: {state}"
        )
    return CleanupResult(
        deleted_nodes=deleted_nodes,
        deleted_relationships=deleted_relationships,
    )


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
        metadata["dbms_components_unavailable"] = (
            f"{type(exc).__name__}: {exc}"
        )
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
    normalized_median = median(
        sample.client_wall_ms for sample in normalized_updates
    )
    normalization_median = median(
        sample.client_wall_ms for sample in normalization_samples
    )
    saved_per_update = denormalized_median - normalized_median
    break_even = (
        normalization_median / saved_per_update
        if saved_per_update > 0
        else None
    )
    return {
        "write_amplification_nodes": (
            redundancy["product_occurrences"]
            / redundancy["distinct_suppliers"]
        ),
        "denormalized_to_normalized_update_median_ratio": (
            denormalized_median / normalized_median
            if normalized_median > 0
            else None
        ),
        "median_client_ms_saved_per_logical_update_batch": saved_per_update,
        "data_rewrite_normalization_break_even_update_batches": break_even,
        "shadow_data_rewrite_normalization_break_even_update_batches": break_even,
    }


def cleanup_stale_run(
    driver: Any,
    database: str,
    run_id: str,
) -> dict[str, Any]:
    """Cleanup-only recovery for a run left by SIGKILL or machine failure."""

    from neo4j import WRITE_ACCESS

    with driver.session(
        database=database,
        default_access_mode=WRITE_ACCESS,
    ) as session:
        cleanup_result = cleanup_shadow_artifacts(session, run_id)
        remaining_nodes, remaining_relationships = (
            existing_shadow_artifact_counts(session)
        )
        dropped_constraints: list[str] = []
        dropped_indexes: list[str] = []

        # UUID-named schema created by this script is safe to remove only when
        # no shadow run remains.  A manually named compatible schema object is
        # never removed by cleanup-only mode.
        if remaining_nodes == 0 and remaining_relationships == 0:
            constraint_result = session.run(
                "SHOW CONSTRAINTS YIELD name RETURN name"
            )
            constraint_records, _ = result_records_and_summary(
                constraint_result
            )
            owned_constraint_prefixes = (
                MV_PRODUCT_CONSTRAINT_PREFIX + "_",
                MV_SUPPLIER_CONSTRAINT_PREFIX + "_",
            )
            for record in constraint_records:
                name = str(record["name"])
                if name.startswith(owned_constraint_prefixes):
                    session.run(
                        f"DROP CONSTRAINT {quote_ident(name)} IF EXISTS"
                    ).consume()
                    dropped_constraints.append(name)

            index_result = session.run(
                "SHOW INDEXES YIELD name RETURN name"
            )
            index_records, _ = result_records_and_summary(index_result)
            owned_index_prefixes = (
                MV_FOLDED_INDEX_PREFIX + "_",
                MV_SUPPLIER_CONSTRAINT_PREFIX + "_",
                LEGACY_MV_PRODUCT_ID_INDEX_PREFIX + "_",
            )
            for record in index_records:
                name = str(record["name"])
                if name.startswith(owned_index_prefixes):
                    session.run(
                        f"DROP INDEX {quote_ident(name)} IF EXISTS"
                    ).consume()
                    dropped_indexes.append(name)

        original_state = validate_original_state(session)
        original_rows = fetch_original_join_rows(session)
        data_fingerprint = validate_case_study_dataset(original_rows)
        source_edges = fetch_source_edge_counts(session)
        topology_fingerprint = source_topology_sha256(session)
        return {
            "run_id": run_id,
            "cleanup": asdict(cleanup_result),
            "remaining_shadow_nodes": remaining_nodes,
            "remaining_shadow_relationships": remaining_relationships,
            "dropped_constraints": dropped_constraints,
            "dropped_indexes": dropped_indexes,
            "original_state": asdict(original_state),
            "relevant_data_sha256": data_fingerprint,
            "source_topology_sha256": topology_fingerprint,
            "source_edge_counts": asdict(source_edges),
        }


def cleanup_after_failure(
    driver: Any,
    database: str,
    run_id: str,
    index_leases: Sequence[IndexLease],
    constraint_leases: Sequence[ConstraintLease],
    expected_data_fingerprint: str | None,
    expected_topology_fingerprint: str | None,
    expected_supplier_snapshot: dict[Any, dict[str, Any]] | None,
) -> None:
    """Best-effort run-scoped cleanup.  No original data is rewritten."""

    from neo4j import WRITE_ACCESS

    with driver.session(
        database=database,
        default_access_mode=WRITE_ACCESS,
    ) as session:
        cleanup_result: CleanupResult | None = None
        cleanup_errors: list[BaseException] = []
        try:
            cleanup_result = cleanup_shadow_artifacts(session, run_id)
        except BaseException as exc:
            cleanup_errors.append(exc)
        finally:
            for constraint_lease in reversed(constraint_leases):
                try:
                    drop_experiment_constraint(session, constraint_lease)
                except BaseException as exc:
                    cleanup_errors.append(exc)
            for index_lease in reversed(index_leases):
                try:
                    drop_experiment_index(session, index_lease)
                except BaseException as exc:
                    cleanup_errors.append(exc)

        try:
            validate_original_state(session)
            if expected_data_fingerprint is not None:
                actual_data_fingerprint = relevant_data_sha256(
                    fetch_original_join_rows(session)
                )
                if actual_data_fingerprint != expected_data_fingerprint:
                    raise ExperimentValidationError(
                        "failure cleanup found that the original "
                        "Product-Supplier fingerprint changed"
                    )
            if expected_topology_fingerprint is not None:
                actual_topology_fingerprint = source_topology_sha256(session)
                if (
                    actual_topology_fingerprint
                    != expected_topology_fingerprint
                ):
                    raise ExperimentValidationError(
                        "failure cleanup found that the original "
                        "OF_PRODUCT/IN_CATEGORY topology changed"
                    )
            if expected_supplier_snapshot is not None:
                verify_supplier_snapshot(
                    expected_supplier_snapshot,
                    fetch_original_supplier_snapshot(session),
                    "failure cleanup original graph",
                )
        except BaseException as exc:
            cleanup_errors.append(exc)

        if cleanup_errors:
            details = "; ".join(
                f"{type(error).__name__}: {error}"
                for error in cleanup_errors
            )
            raise RuntimeError(
                f"one or more failure-cleanup checks failed: {details}"
            ) from cleanup_errors[0]

        assert cleanup_result is not None
        print(
            "  Failure cleanup removed "
            f"{cleanup_result.deleted_nodes:,} shadow nodes and "
            f"{cleanup_result.deleted_relationships:,} shadow relationships",
            file=sys.stderr,
        )


def run_experiment(
    driver: Any,
    database: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    from neo4j import WRITE_ACCESS, __version__ as neo4j_driver_version

    started_at = datetime.now(timezone.utc)
    run_id = uuid4().hex
    folded_index_lease = IndexLease()
    product_constraint_lease = ConstraintLease()
    supplier_constraint_lease = ConstraintLease()
    index_leases = [folded_index_lease]
    constraint_leases = [
        product_constraint_lease,
        supplier_constraint_lease,
    ]
    dataset_fingerprint_before: str | None = None
    source_topology_fingerprint_before: str | None = None
    original_supplier_snapshot: dict[Any, dict[str, Any]] | None = None

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
            initial_state = validate_original_state(session)
            ensure_no_existing_shadow_artifacts(session)
            original_supplier_index = require_range_index(
                session,
                label=SUPPLIER_LABEL,
                property_names=(SUPPLIER_ID_PROPERTY,),
                index_wait_seconds=args.index_wait_seconds,
            )

            original_rows = fetch_original_join_rows(session)
            dataset_fingerprint_before = validate_case_study_dataset(
                original_rows
            )
            original_supplier_snapshot = fetch_original_supplier_snapshot(
                session
            )
            source_edges = fetch_source_edge_counts(session)
            source_topology_fingerprint_before = source_topology_sha256(
                session
            )
            update_workload = build_active_domain_update_workload(
                canonical_folded_values(
                    original_rows,
                    args.update_property,
                ),
                args.update_property,
                active_domain_values=[
                    attributes[args.update_property]
                    for attributes in original_supplier_snapshot.values()
                ],
                active_domain_scope="full_original_Supplier_relation",
            )
            update_payload_metrics = calculate_update_payload_metrics(
                original_rows,
                update_workload,
            )

            ensure_experiment_range_index(
                session,
                label=MV_FOLDED_LABEL,
                property_names=(
                    RUN_ID_PROPERTY,
                    SUPPLIER_ID_PROPERTY,
                ),
                prefix=MV_FOLDED_INDEX_PREFIX,
                index_wait_seconds=args.index_wait_seconds,
                lease=folded_index_lease,
            )
            ensure_experiment_uniqueness_constraint(
                session,
                label=MV_PRODUCT_LABEL,
                property_names=(
                    RUN_ID_PROPERTY,
                    PRODUCT_ID_PROPERTY,
                ),
                prefix=MV_PRODUCT_CONSTRAINT_PREFIX,
                index_wait_seconds=args.index_wait_seconds,
                lease=product_constraint_lease,
            )
            ensure_experiment_uniqueness_constraint(
                session,
                label=MV_SUPPLIER_LABEL,
                property_names=(
                    RUN_ID_PROPERTY,
                    SUPPLIER_ID_PROPERTY,
                ),
                prefix=MV_SUPPLIER_CONSTRAINT_PREFIX,
                index_wait_seconds=args.index_wait_seconds,
                lease=supplier_constraint_lease,
            )

            print(
                "\nStep 0: creating mv_product_supplier shadow projection "
                "(not timed)"
            )
            print(f"  Run ID: {run_id}", flush=True)
            step0_counts = create_shadow_projection(
                session,
                run_id,
                original_state=initial_state,
                source_edges=source_edges,
            )
            folded_state = inspect_shadow_state(session, run_id)
            print(
                "  Created "
                f"{step0_counts['mv_product_supplier_nodes']:,} "
                "mv_product_supplier nodes, "
                f"{step0_counts['join_of_product_relationships']:,} "
                "join_of_product relationships, and "
                f"{step0_counts['join_in_category_relationships']:,} "
                "join_in_category relationships"
            )
            print(
                "  Original Product, Supplier, OF_PRODUCT, IN_CATEGORY, and "
                "SUPPLIED_BY data were retained"
            )

            print("\nStep 1: measuring FD redundancy in the shadow projection")
            folded_rows = fetch_folded_rows(session, run_id)
            redundancy = calculate_redundancy(folded_rows)
            shadow_fingerprint = validate_case_study_dataset(
                folded_rows,
                redundancy,
            )
            if shadow_fingerprint != dataset_fingerprint_before:
                raise ExperimentValidationError(
                    "Step 0 changed the relevant Product-Supplier values"
                )
            print(f"  Dataset fingerprint verified: {shadow_fingerprint}")
            print_redundancy(redundancy)

            folded_supplier_snapshot = canonical_supplier_snapshot(
                folded_rows
            )
            verify_supplier_snapshot(
                original_supplier_snapshot,
                folded_supplier_snapshot,
                "Step 0 shadow projection",
            )
            folded_update_values = canonical_folded_values(
                folded_rows,
                args.update_property,
            )
            if folded_update_values != update_workload.originals:
                raise ExperimentValidationError(
                    "Step 0 changed the logical values used by the update "
                    "workload"
                )

            print(
                "\nStep 2: updating mv_product_supplier copies "
                f"({args.update_property}; "
                f"{args.warmup_runs} warmups, "
                f"{args.runs} measured runs)"
            )
            print(
                f"  Batch: {update_workload.logical_tuple_count} logical "
                "Supplier "
                "updates in one transaction; restore is outside the timer"
            )
            print(
                "  Value strategy: deterministic active-domain rotation; "
                f"domain size={update_workload.active_domain_size}, "
                f"type={update_workload.value_type}"
            )
            print(
                "  Payload proxy delta: folded="
                f"{update_payload_metrics['folded_join_fanout_weighted']['payload_delta_bytes']:+,} "
                "bytes; normalized="
                f"{update_payload_metrics['normalized_logical']['payload_delta_bytes']:+,} "
                "bytes"
            )
            denormalized_update_samples = benchmark_updates(
                session,
                run_id=run_id,
                phase_name="shadow folded",
                label=MV_FOLDED_LABEL,
                workload=update_workload,
                expected_nodes=folded_state.folded_product_nodes,
                warmup_runs=args.warmup_runs,
                measured_runs=args.runs,
            )
            print_timing_summary(
                "shadow folded update",
                denormalized_update_samples,
            )
            verify_supplier_snapshot(
                original_supplier_snapshot,
                canonical_supplier_snapshot(
                    fetch_folded_rows(session, run_id)
                ),
                "Step 2 restore",
            )

            print(
                "\nStep 3: normalizing the shadow projection "
                f"({args.normalization_warmup_runs} warmups, "
                f"{args.normalization_runs} measured runs)"
            )
            print(
                "  Timing scope: shadow Supplier decomposition only; "
                "schema DDL, validation, refolding, and copied query topology "
                "are excluded"
            )
            normalization_samples: list[TimingSample] = []
            normalization_total_runs = (
                args.normalization_warmup_runs
                + args.normalization_runs
            )
            for ordinal in range(1, normalization_total_runs + 1):
                measured_number = (
                    ordinal - args.normalization_warmup_runs
                )
                sample = normalize_once(
                    session,
                    run_id=run_id,
                    run=max(measured_number, 0),
                    expected_products=redundancy["product_occurrences"],
                    expected_suppliers=redundancy["distinct_suppliers"],
                    source_edges=source_edges,
                )
                verify_supplier_snapshot(
                    original_supplier_snapshot,
                    fetch_shadow_supplier_snapshot(session, run_id),
                    "Step 3",
                )
                if ordinal > args.normalization_warmup_runs:
                    normalization_samples.append(sample)
                if ordinal < normalization_total_runs:
                    refold_shadow_projection(
                        session,
                        run_id,
                        expected_products=redundancy[
                            "product_occurrences"
                        ],
                        expected_suppliers=redundancy[
                            "distinct_suppliers"
                        ],
                        source_edges=source_edges,
                    )
                    refolded_rows = fetch_folded_rows(session, run_id)
                    if (
                        relevant_data_sha256(refolded_rows)
                        != shadow_fingerprint
                    ):
                        raise ExperimentValidationError(
                            "Step 3 repetition refold changed the shadow "
                            "dataset fingerprint"
                        )
                    verify_supplier_snapshot(
                        original_supplier_snapshot,
                        canonical_supplier_snapshot(refolded_rows),
                        "Step 3 repetition setup",
                    )
            print_timing_summary(
                "shadow normalization",
                normalization_samples,
            )

            normalized_shadow_state = validate_normalized_shadow_state(
                session,
                run_id,
                expected_products=redundancy["product_occurrences"],
                expected_suppliers=redundancy["distinct_suppliers"],
                source_edges=source_edges,
            )
            verify_supplier_snapshot(
                original_supplier_snapshot,
                fetch_shadow_supplier_snapshot(session, run_id),
                "final Step 3",
            )

            print(
                "\nStep 4: updating mv_supplier nodes "
                f"({args.update_property}; "
                f"{args.warmup_runs} warmups, "
                f"{args.runs} measured runs)"
            )
            print(
                f"  Batch: {update_workload.logical_tuple_count} logical "
                "Supplier "
                "updates in one transaction; restore is outside the timer"
            )
            normalized_update_samples = benchmark_updates(
                session,
                run_id=run_id,
                phase_name="shadow normalized",
                label=MV_SUPPLIER_LABEL,
                workload=update_workload,
                expected_nodes=(
                    normalized_shadow_state.normalized_supplier_nodes
                ),
                warmup_runs=args.warmup_runs,
                measured_runs=args.runs,
            )
            print_timing_summary(
                "shadow normalized update",
                normalized_update_samples,
            )
            verify_supplier_snapshot(
                original_supplier_snapshot,
                fetch_shadow_supplier_snapshot(session, run_id),
                "Step 4 restore",
            )

            # Revalidate the source before cleanup: no source node/property or
            # original relationship has been updated by Steps 0-4.
            validate_original_state(session)
            dataset_fingerprint_during_shadow = relevant_data_sha256(
                fetch_original_join_rows(session)
            )
            if dataset_fingerprint_during_shadow != dataset_fingerprint_before:
                raise ExperimentValidationError(
                    "the original Product-Supplier fingerprint changed "
                    "during the shadow experiment"
                )
            source_topology_fingerprint_during_shadow = (
                source_topology_sha256(session)
            )
            if (
                source_topology_fingerprint_during_shadow
                != source_topology_fingerprint_before
            ):
                raise ExperimentValidationError(
                    "the original OF_PRODUCT/IN_CATEGORY topology changed "
                    "during the shadow experiment"
                )
            verify_supplier_snapshot(
                original_supplier_snapshot,
                fetch_original_supplier_snapshot(session),
                "original graph before cleanup",
            )

            print("\nCleanup: deleting this run's shadow projection (not timed)")
            cleanup_result = cleanup_shadow_artifacts(session, run_id)
            schema_cleanup_errors: list[BaseException] = []
            for constraint_lease in reversed(constraint_leases):
                try:
                    drop_experiment_constraint(session, constraint_lease)
                except BaseException as exc:
                    schema_cleanup_errors.append(exc)
            for index_lease in reversed(index_leases):
                try:
                    drop_experiment_index(session, index_lease)
                except BaseException as exc:
                    schema_cleanup_errors.append(exc)
            if schema_cleanup_errors:
                details = "; ".join(
                    f"{type(error).__name__}: {error}"
                    for error in schema_cleanup_errors
                )
                raise RuntimeError(
                    f"shadow schema cleanup failed: {details}"
                ) from schema_cleanup_errors[0]
            final_state = validate_original_state(session)
            dataset_fingerprint_after = relevant_data_sha256(
                fetch_original_join_rows(session)
            )
            if dataset_fingerprint_after != dataset_fingerprint_before:
                raise ExperimentValidationError(
                    "the original Product-Supplier fingerprint changed "
                    "after shadow cleanup"
                )
            source_topology_fingerprint_after = source_topology_sha256(
                session
            )
            if (
                source_topology_fingerprint_after
                != source_topology_fingerprint_before
            ):
                raise ExperimentValidationError(
                    "the original OF_PRODUCT/IN_CATEGORY topology changed "
                    "after shadow cleanup"
                )
            verify_supplier_snapshot(
                original_supplier_snapshot,
                fetch_original_supplier_snapshot(session),
                "original graph after cleanup",
            )
            final_shadow_state = inspect_shadow_state(session, run_id)
            if final_shadow_state.phase != "clean":
                raise ExperimentValidationError(
                    f"cleanup did not remove the run: {final_shadow_state}"
                )
            print(
                f"  Removed {cleanup_result.deleted_nodes:,} shadow nodes "
                f"and {cleanup_result.deleted_relationships:,} shadow "
                "relationships; original graph verified unchanged"
            )

        completed_at = datetime.now(timezone.utc)
        legacy_initial_state = {
            "product_nodes": initial_state.product_nodes,
            "folded_product_nodes": 0,
            "supplier_nodes": initial_state.supplier_nodes,
            "supplied_by_relationships": (
                initial_state.supplied_by_relationships
            ),
        }
        legacy_final_state = {
            "product_nodes": final_state.product_nodes,
            "folded_product_nodes": 0,
            "supplier_nodes": final_state.supplier_nodes,
            "supplied_by_relationships": (
                final_state.supplied_by_relationships
            ),
        }
        report = {
            "report_schema_version": 3,
            "experiment": "northwind_supplier_product_fd",
            "run_id": run_id,
            "execution": {
                "mode": "run_id_shadow_projection",
                "source_policy": (
                    "original Product/Supplier nodes, properties, and "
                    "relationships are not rewritten"
                ),
                "boundary_overlay_note": (
                    "join_of_product and join_in_category temporarily attach "
                    "to original OrderDetail/Category nodes, so wildcard "
                    "degree/traversal queries must not run concurrently"
                ),
                "local_concurrency_policy": (
                    "exclusive advisory process lock; do not run this "
                    "benchmark concurrently from another host or script copy"
                ),
            },
            "started_at_utc": started_at.isoformat(),
            "completed_at_utc": completed_at.isoformat(),
            "database": database,
            "environment": environment_metadata,
            "dataset": {
                "name": "case_study Northwind",
                "relevant_data_sha256_before": dataset_fingerprint_before,
                "relevant_data_sha256_after": dataset_fingerprint_after,
                "relevant_data_sha256": dataset_fingerprint_before,
                "source_topology_sha256_before": (
                    source_topology_fingerprint_before
                ),
                "source_topology_sha256_after": (
                    source_topology_fingerprint_after
                ),
                "expected_product_count": EXPECTED_PRODUCT_COUNT,
                "expected_supplier_count": EXPECTED_SUPPLIER_COUNT,
                "expected_supplied_by_count": EXPECTED_SUPPLIED_BY_COUNT,
                "expected_of_product_count": EXPECTED_OF_PRODUCT_COUNT,
                "expected_in_category_count": EXPECTED_IN_CATEGORY_COUNT,
                "source_edge_counts": asdict(source_edges),
                "fingerprint_scope": (
                    "ProductID, Product.SupplierID, and the 11 dependent "
                    "Supplier attributes"
                ),
            },
            "shadow_schema": {
                "product_artifact_label": MV_ARTIFACT_LABEL,
                "cleanup_scope_labels": [
                    MV_ARTIFACT_LABEL,
                    MV_FOLDED_LABEL,
                    MV_PRODUCT_LABEL,
                    MV_SUPPLIER_LABEL,
                ],
                "folded_product_label": MV_FOLDED_LABEL,
                "normalized_product_label": MV_PRODUCT_LABEL,
                "normalized_supplier_label": MV_SUPPLIER_LABEL,
                "run_id_property": RUN_ID_PROPERTY,
                "relationships": {
                    "order_to_product": JOIN_OF_PRODUCT_RELATIONSHIP,
                    "product_to_category": JOIN_IN_CATEGORY_RELATIONSHIP,
                    "product_to_supplier": (
                        JOIN_SUPPLIED_BY_RELATIONSHIP
                    ),
                },
                "relationship_properties": (
                    "join_of_product and join_in_category copy the complete "
                    "source relationship property maps"
                ),
            },
            "configuration": {
                "update_property": args.update_property,
                "update_key_properties": list(SUPPLIER_KEY_PROPERTIES),
                "warmup_runs": args.warmup_runs,
                "measured_update_runs": args.runs,
                "normalization_warmup_runs": (
                    args.normalization_warmup_runs
                ),
                "normalization_runs": args.normalization_runs,
                "original_supplier_id_range_index": (
                    original_supplier_index
                ),
                "folded_composite_range_index": (
                    folded_index_lease.name
                ),
                "folded_index_created_by_experiment": (
                    folded_index_lease.created_by_experiment
                ),
                "stable_product_composite_uniqueness_constraint": (
                    product_constraint_lease.name
                ),
                "stable_product_constraint_created_by_experiment": (
                    product_constraint_lease.created_by_experiment
                ),
                "normalized_composite_uniqueness_constraint": (
                    supplier_constraint_lease.name
                ),
                "normalized_constraint_created_by_experiment": (
                    supplier_constraint_lease.created_by_experiment
                ),
                # Compatibility aliases retained for existing result readers.
                "normalized_supplier_id_range_index": (
                    supplier_constraint_lease.name
                ),
                "folded_supplier_id_range_index": (
                    folded_index_lease.name
                ),
                "update_workload": (
                    "one transaction containing one logical update for "
                    "every distinct Supplier"
                ),
                "update_value_strategy": (
                    "each original value is replaced by the next distinct "
                    "value in the deterministically ordered observed active "
                    "domain; the exact same precomputed mapping is used in "
                    "Steps 2 and 4; values are restored outside the timer"
                ),
                "update_strategy_id": UPDATE_STRATEGY_ID,
                "update_active_domain": {
                    "definition": (
                        "distinct non-NULL values across the complete original "
                        "Supplier relation"
                    ),
                    "size": update_workload.active_domain_size,
                    "scope": update_workload.active_domain_scope,
                    "source_tuple_count": (
                        update_workload.active_domain_source_tuple_count
                    ),
                    "updated_tuple_scope": (
                        "distinct SupplierID values participating in the "
                        "Product-Supplier join"
                    ),
                    "sha256": update_workload.active_domain_sha256,
                    "value_type": update_workload.value_type,
                    "ordering": (
                        "lexicographic order of deterministic type-aware "
                        "value tokens"
                    ),
                    "null_policy": UPDATE_NULL_POLICY,
                },
                "update_mapping": {
                    "logical_tuple_count": (
                        update_workload.logical_tuple_count
                    ),
                    "sha256": update_workload.mapping_sha256,
                    "logical_original_payload_bytes": (
                        update_workload.logical_original_payload_bytes
                    ),
                    "logical_updated_payload_bytes": (
                        update_workload.logical_updated_payload_bytes
                    ),
                    "logical_payload_delta_bytes": (
                        update_workload.logical_payload_delta_bytes
                    ),
                    "payload_by_phase": update_payload_metrics,
                },
                "update_timing_scope": (
                    "one autocommit run-scoped batch write through full "
                    "result consumption/commit; restore and validation "
                    "excluded"
                ),
                "update_successor_validation": (
                    "one exact out-of-timer value check per phase after the "
                    "first warmup update, or after the final measured update "
                    "when warmup-runs is zero"
                ),
                "normalization_timing_scope": (
                    "explicit transaction begin, shadow data decomposition, "
                    "and commit; Step 0, copied "
                    "order/category topology, schema DDL, validation, "
                    "refolding, and cleanup excluded"
                ),
                "stable_product_label_policy": (
                    "mv_product is present before and after normalization; "
                    "Step 3 only removes mv_product_supplier"
                ),
                "index_transition_policy": (
                    "the stable mv_product(run_id, ProductID) key remains "
                    "indexed; Step 3 includes removal of the folded-only "
                    "(run_id, SupplierID) lookup entries"
                ),
                "normalization_source": (
                    "run-scoped mv_product_supplier nodes only; original "
                    "Supplier nodes are used solely for out-of-timer "
                    "validation"
                ),
                "cache_policy": (
                    "warm application workload; server page/query caches "
                    "are not cleared"
                ),
                "execution_order_note": (
                    "Step 2 always precedes Step 4; warmups are used, but "
                    "formal AB/BA counterbalancing requires separate runs"
                ),
                "isolation_overhead_note": (
                    "run_id properties, the Product-side mv_experiment "
                    "label, and the coexisting original graph are "
                    "experimental isolation overhead"
                ),
            },
            "initial_original_state": asdict(initial_state),
            # Backward-compatible aliases: these now describe the unchanged
            # original graph, not an in-place fold/normalize lifecycle.
            "initial_state": legacy_initial_state,
            "step0_artifacts": step0_counts,
            "folded_shadow_state": asdict(folded_state),
            "normalized_shadow_state": asdict(normalized_shadow_state),
            "cleanup": {
                **asdict(cleanup_result),
                "final_shadow_state": asdict(final_shadow_state),
                "experiment_owned_schema_dropped": True,
                "experiment_owned_indexes_dropped": True,
            },
            "final_original_state": asdict(final_state),
            "final_state": legacy_final_state,
            "redundancy": redundancy,
            "timings": {
                "denormalized_update": {
                    "samples": samples_as_dicts(
                        denormalized_update_samples
                    ),
                    "summary": timing_summary(
                        denormalized_update_samples
                    ),
                },
                "normalization": {
                    "samples": samples_as_dicts(normalization_samples),
                    "summary": timing_summary(normalization_samples),
                },
                "normalized_update": {
                    "samples": samples_as_dicts(
                        normalized_update_samples
                    ),
                    "summary": timing_summary(
                        normalized_update_samples
                    ),
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
        print(
            "\nWARNING: experiment stopped; attempting run-scoped shadow "
            f"cleanup for run_id={run_id}",
            file=sys.stderr,
        )
        try:
            cleanup_after_failure(
                driver,
                database,
                run_id,
                index_leases,
                constraint_leases,
                dataset_fingerprint_before,
                source_topology_fingerprint_before,
                original_supplier_snapshot,
            )
        except BaseException as cleanup_error:
            print(
                "WARNING: automatic shadow cleanup also failed: "
                f"{type(cleanup_error).__name__}: {cleanup_error}. "
                f"Manual cleanup must target run_id={run_id}",
                file=sys.stderr,
            )
        raise


def print_final_comparison(report: dict[str, Any]) -> None:
    derived = report["derived"]
    break_even = derived[
        "shadow_data_rewrite_normalization_break_even_update_batches"
    ]
    break_even_text = (
        "N/A"
        if break_even is None
        else f"{break_even:.3f} logical update batches"
    )
    print("\nFinal comparison")
    print(
        "  Write amplification by updated shadow nodes: "
        f"{derived['write_amplification_nodes']:.3f}x"
    )
    ratio = derived["denormalized_to_normalized_update_median_ratio"]
    print(
        "  Folded/normalized shadow median client-time ratio: "
        + ("N/A" if ratio is None else f"{ratio:.3f}x")
    )
    print(
        "  Estimated shadow-data-rewrite normalization break-even: "
        f"{break_even_text}"
    )
    print(
        "  Final database state: original normalized graph unchanged; "
        "experiment shadow projection removed"
    )


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
    cleanup_run_id = (
        args.cleanup_run_id.strip()
        if args.cleanup_run_id is not None
        else None
    )
    if cleanup_run_id is not None and (
        not cleanup_run_id or len(cleanup_run_id) > 128
    ):
        print(
            "ERROR: --cleanup-run-id must contain 1 to 128 characters",
            file=sys.stderr,
        )
        return 2

    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        print(
            'ERROR: install the Neo4j driver with: '
            'python -m pip install "neo4j>=5.7"',
            file=sys.stderr,
        )
        print(f"CAUSE: {exc}", file=sys.stderr)
        return 1

    password = os.getenv("NEO4J_PASSWORD") or DEBUG_NEO4J_PASSWORD
    if password is None:
        password = getpass(f"Neo4j password for {args.user}: ")

    try:
        print(
            f"Connecting to {args.uri} as {args.user!r}; "
            f"database={database!r}"
        )
        with single_process_experiment_lock():
            with GraphDatabase.driver(
                args.uri,
                auth=(args.user, password),
            ) as driver:
                if not hasattr(driver, "execute_query"):
                    raise RuntimeError(
                        "Neo4j Python Driver 5.7 or newer is required"
                    )
                driver.verify_connectivity()
                driver.execute_query(
                    "RETURN 1 AS ready",
                    database_=database,
                )
                if cleanup_run_id is None:
                    report = run_experiment(driver, database, args)
                    cleanup_report = None
                else:
                    cleanup_report = cleanup_stale_run(
                        driver,
                        database,
                        cleanup_run_id,
                    )
                    report = None

        if cleanup_report is not None:
            cleanup_counts = cleanup_report["cleanup"]
            print(
                "Cleanup-only complete: removed "
                f"{cleanup_counts['deleted_nodes']:,} nodes and "
                f"{cleanup_counts['deleted_relationships']:,} "
                f"relationships for run_id={cleanup_run_id}"
            )
            print(
                "  Remaining shadow artifacts: "
                f"{cleanup_report['remaining_shadow_nodes']:,} nodes, "
                f"{cleanup_report['remaining_shadow_relationships']:,} "
                "relationships"
            )
            if args.output_json is not None:
                output_path = write_json_report(
                    args.output_json,
                    cleanup_report,
                )
                print(f"  JSON cleanup report: {output_path}")
        else:
            assert report is not None
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
