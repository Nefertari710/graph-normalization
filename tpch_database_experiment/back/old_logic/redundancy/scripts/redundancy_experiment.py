#!/usr/bin/env python3
"""Run pair-wise TPC-H functional-dependency experiments in Neo4j.

The experiment covers every direct foreign-key relationship in the embedded
TPC-H graph schema.  It deliberately uses only relationships present in that
schema; query-specific virtual joins are not treated as foreign keys.
For each case, the referencing table contains the foreign key and the
referenced table supplies the referenced key.  One run-scoped shadow node
represents each row of their join.

With no case-selection argument, all eight direct TPC-H cases run
sequentially.  Use ``--list-cases`` to inspect the complete catalog or
``--case CASE_ID`` to run only one case.  ``--all-cases`` is retained as an
explicit alias for the default batch behavior.

TPC-H column names are already table-namespaced (``l_``, ``ps_``, ``o_``, and
so on).  Folded join nodes therefore preserve the native imported names.
Referenced keys are not copied; the referencing foreign-key properties
represent them.  Normalized shadow nodes also use the native imported names.

Two topology modes are available:

``complete``
    Copy all declared boundary relationships incident to the referencing and
    referenced tables, excluding their folded relationship.  Referencing-owned
    copies stay on the referencing shadow.  Referenced-owned copies are
    duplicated in the folded phase, deduplicated onto the normalized referenced
    node in Step 3, and copied back during out-of-timer refolding.

``property-fd``
    Materialize only namespaced referencing/referenced properties and their
    normalized relationship.  This isolates property redundancy from
    query-topology maintenance and is the default because complete boundary
    copying can grow very large for TPC-H Nation joins.

Scope deliberately excludes conceptual multi-table joins and arbitrary
multi-hop paths.  Those require different normalization semantics.

The live experiment expects the primary-key constraints created by the TPC-H
importer and a Neo4j server that supports scoped ``CALL (...)`` subqueries with
``IN TRANSACTIONS`` (Neo4j 5.23 or newer).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import pickle
import platform
import re
import sqlite3
import sys
import tempfile
from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from getpass import getpass
from pathlib import Path
from statistics import fmean, median, stdev
from time import perf_counter_ns
from typing import Any, Callable, Iterable, Iterator, Literal, Sequence
from uuid import uuid4


DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_DATABASE = "tpch-sf-001-redundancy"
DEFAULT_DATA_DIR = (
    Path(__file__).resolve().parents[1] / "data" / "tbl_sf_001"
)
DEFAULT_WARMUP_RUNS = 1                 # 5
DEFAULT_RUNS = 1                        # 20
DEFAULT_NORMALIZATION_WARMUP_RUNS = 1   # 5
DEFAULT_NORMALIZATION_RUNS = 1         # 20
DEFAULT_INDEX_WAIT_SECONDS = 300
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_TOPOLOGY_MODE = "property-fd"
DEFAULT_OUTPUT_JSON = (
    Path(__file__).resolve().parents[1] / "results" / "simple" / "tpch_fd_experiment_results.json"
)

RUN_ID_PROPERTY = "run_id"
CASE_ID_PROPERTY = "mv_case_id"
ARTIFACT_LABEL = "mv_experiment"
SCHEMA_PREFIX = "mvfd"
SHADOW_RELATIONSHIP_PREFIX = "JOIN_"
REPORT_SCHEMA_VERSION = 8
JOIN_FINGERPRINT_SCHEMA_VERSION = 3
JOIN_FINGERPRINT_ALGORITHM = "commutative_join_row_sha256_sum_xor_v1"
SOURCE_GRAPH_FINGERPRINT_SCHEMA_VERSION = 3
SOURCE_GRAPH_FINGERPRINT_ALGORITHM = (
    "tpch_schema_keyed_commutative_sha256_sum_xor_v1"
)
FOLDED_PROPERTY_NAMING_STRATEGY_ID = (
    "tpch_native_column_names_no_referenced_key_v1"
)
UPDATE_STRATEGY_ID = (
    "deterministic_full_referenced_relation_domain_rotation_v2"
)
UPDATE_NULL_POLICY = "reject_attribute_if_any_domain_or_updated_value_is_null"

# QUICK LOCAL DEBUGGING ONLY:
# Replace None with a local password string to skip the interactive prompt.
# Never commit or share the file while a real password is present here.
QUICK_DEBUG_NEO4J_PASSWORD: str | None = ""


class ExperimentStateError(RuntimeError):
    """Raised when the original or shadow graph is in an unsafe state."""


class ExperimentValidationError(RuntimeError):
    """Raised when a transformation or measured workload is inconsistent."""


class TblFormatError(ValueError):
    """Raised when a TPC-H .tbl row does not match its embedded schema."""


TblConverter = Callable[[str], Any]


@dataclass(frozen=True)
class TblColumnSpec:
    name: str
    converter: TblConverter


@dataclass(frozen=True)
class TblTableSpec:
    filename: str
    label: str
    columns: tuple[TblColumnSpec, ...]
    key_fields: tuple[str, ...]


@dataclass(frozen=True)
class NodeSpec:
    filename: str
    label: str
    columns: tuple[str, ...]
    key_fields: tuple[str, ...]


@dataclass(frozen=True)
class RelationshipSpec:
    row_label: str
    source_label: str
    target_label: str
    relationship_type: str
    source_keys: tuple[tuple[str, str], ...]
    target_keys: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class PropertyBinding:
    logical_name: str
    folded_name: str


@dataclass(frozen=True)
class BoundaryEdgeSpec:
    relationship: RelationshipSpec
    owner: Literal["referencing", "referenced"]
    owner_is_source: bool
    external_label: str
    copy_type: str

    @property
    def boundary_id(self) -> str:
        return (
            f"{self.owner}:"
            f"{self.relationship.source_label}"
            f"-[:{self.relationship.relationship_type}]->"
            f"{self.relationship.target_label}"
        )


@dataclass(frozen=True)
class PairNames:
    folded_label: str
    referencing_label: str
    referenced_label: str
    normalized_join_type: str


@dataclass(frozen=True)
class CaseMetrics:
    referenced_rows: int
    join_rows: int
    participating_referenced_rows: int
    redundant_rows: int
    redundant_slots: int
    complete_folded_boundary_edges: int


@dataclass(frozen=True)
class PairSpec:
    case_id: str
    referencing: NodeSpec
    referenced: NodeSpec
    join: RelationshipSpec
    referencing_is_source: bool
    fk_mapping: tuple[tuple[str, str], ...]
    dependent_properties: tuple[str, ...]
    referencing_alias: str
    referenced_alias: str
    referencing_property_bindings: tuple[PropertyBinding, ...]
    referenced_property_bindings: tuple[PropertyBinding, ...]
    topology_fk_properties: tuple[str, ...]
    default_update_property: str
    boundaries: tuple[BoundaryEdgeSpec, ...]
    names: PairNames

    @property
    def referencing_fk_fields(self) -> tuple[str, ...]:
        by_referenced_key = {
            referenced_key: referencing_fk
            for referencing_fk, referenced_key in self.fk_mapping
        }
        return tuple(
            by_referenced_key[key] for key in self.referenced.key_fields
        )

    @property
    def referencing_binding_by_logical_name(self) -> dict[str, str]:
        return {
            binding.logical_name: binding.folded_name
            for binding in self.referencing_property_bindings
        }

    @property
    def referenced_binding_by_logical_name(self) -> dict[str, str]:
        return {
            binding.logical_name: binding.folded_name
            for binding in self.referenced_property_bindings
        }

    @property
    def folded_referencing_columns(self) -> tuple[str, ...]:
        by_logical = self.referencing_binding_by_logical_name
        return tuple(by_logical[prop] for prop in self.referencing.columns)

    @property
    def folded_referencing_key_fields(self) -> tuple[str, ...]:
        by_logical = self.referencing_binding_by_logical_name
        return tuple(by_logical[prop] for prop in self.referencing.key_fields)

    @property
    def folded_referencing_fk_fields(self) -> tuple[str, ...]:
        by_logical = self.referencing_binding_by_logical_name
        return tuple(by_logical[prop] for prop in self.referencing_fk_fields)

    @property
    def allowed_update_properties(self) -> tuple[str, ...]:
        blocked = set(self.referenced.key_fields)
        blocked.update(self.topology_fk_properties)
        return tuple(
            prop for prop in self.dependent_properties if prop not in blocked
        )

    @property
    def all_copy_relationship_types(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    self.names.normalized_join_type,
                    *(boundary.copy_type for boundary in self.boundaries),
                }
            )
        )


@dataclass(frozen=True)
class BoundaryExpectation:
    boundary_id: str
    owner: str
    copy_type: str
    folded_count: int
    normalized_count: int

    @property
    def redundant_folded_copies(self) -> int:
        return self.folded_count - self.normalized_count


@dataclass(frozen=True)
class CaseBaseline:
    referencing_rows: int
    referenced_rows: int
    join_rows: int
    participating_referenced_rows: int
    join_sha256: str
    source_sha256: str
    boundary_expectations: tuple[BoundaryExpectation, ...]


@dataclass(frozen=True)
class ShadowState:
    folded_join_nodes: int
    normalized_referencing_nodes: int
    normalized_referenced_nodes: int
    normalized_join_relationships: int
    boundary_relationships: dict[str, int]

    @property
    def phase(self) -> str:
        boundary_total = sum(self.boundary_relationships.values())
        if (
            self.folded_join_nodes == 0
            and self.normalized_referencing_nodes == 0
            and self.normalized_referenced_nodes == 0
            and self.normalized_join_relationships == 0
            and boundary_total == 0
        ):
            return "clean"
        if (
            self.folded_join_nodes > 0
            and self.normalized_referencing_nodes == 0
            and self.normalized_referenced_nodes == 0
            and self.normalized_join_relationships == 0
        ):
            return "folded"
        if (
            self.folded_join_nodes == 0
            and self.normalized_referencing_nodes > 0
            and self.normalized_referenced_nodes > 0
            and self.normalized_join_relationships
            == self.normalized_referencing_nodes
        ):
            return "normalized"
        return "inconsistent"


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
    nodes_deleted: int
    relationships_created: int
    relationships_deleted: int
    labels_added: int
    labels_removed: int


@dataclass
class IndexLease:
    name: str | None = None
    created_by_experiment: bool = False


@dataclass
class ConstraintLease:
    name: str | None = None
    created_by_experiment: bool = False


@dataclass
class UpdateWorkload:
    property_name: str
    folded_property_name: str
    logical_tuple_count: int
    active_domain_size: int
    active_domain_source_tuple_count: int
    active_domain_sha256: str
    mapping_sha256: str
    value_type: str
    logical_original_payload_bytes: int
    logical_updated_payload_bytes: int
    _database_path: Path
    _connection: sqlite3.Connection | None

    @property
    def logical_payload_delta_bytes(self) -> int:
        return (
            self.logical_updated_payload_bytes
            - self.logical_original_payload_bytes
        )

    def parameter_batches(
        self,
        batch_size: int,
        *,
        restore: bool,
    ) -> Iterator[list[dict[str, Any]]]:
        connection = self._require_connection()
        value_expression = (
            "original.value_blob" if restore else "successor.value_blob"
        )
        cursor = connection.execute(
            f"""
            SELECT
              workload.key_blob,
              {value_expression},
              workload.fanout
            FROM logical_update AS workload
            JOIN domain AS original
              ON original.token = workload.original_token
            JOIN domain AS successor
              ON successor.token = original.successor_token
            ORDER BY workload.key_token
            """
        )
        batch: list[dict[str, Any]] = []
        for key_blob, value_blob, fanout in cursor:
            batch.append(
                {
                    "key": list(pickle.loads(key_blob)),
                    "value": pickle.loads(value_blob),
                    "expected_fanout": int(fanout),
                }
            )
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise ExperimentStateError("update workload spool is closed")
        return self._connection

    def close(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is not None:
            connection.close()
        self._database_path.unlink(missing_ok=True)


@dataclass
class MutationTotals:
    available_after_ms: int = 0
    consumed_after_ms: int = 0
    properties_set: int = 0
    nodes_created: int = 0
    nodes_deleted: int = 0
    relationships_created: int = 0
    relationships_deleted: int = 0
    labels_added: int = 0
    labels_removed: int = 0

    def add_summary(self, summary: Any) -> None:
        self.available_after_ms += int(summary.result_available_after or 0)
        self.consumed_after_ms += int(summary.result_consumed_after or 0)
        counters = summary.counters
        self.properties_set += int(getattr(counters, "properties_set", 0))
        self.nodes_created += int(getattr(counters, "nodes_created", 0))
        self.nodes_deleted += int(getattr(counters, "nodes_deleted", 0))
        self.relationships_created += int(
            getattr(counters, "relationships_created", 0)
        )
        self.relationships_deleted += int(
            getattr(counters, "relationships_deleted", 0)
        )
        self.labels_added += int(getattr(counters, "labels_added", 0))
        self.labels_removed += int(getattr(counters, "labels_removed", 0))


def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def snake_case(name: str) -> str:
    first = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", first).lower()


def shadow_relationship_type(source_relationship_type: str) -> str:
    """Return the conventional upper-case type used by shadow relationships."""
    return f"{SHADOW_RELATIONSHIP_PREFIX}{source_relationship_type}".upper()


def safe_schema_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", value)


def tbl_text(raw: str) -> str:
    """Preserve TPC-H text exactly, including meaningful leading spaces."""
    return raw


def tbl_int(raw: str) -> int:
    return int(raw)


def tbl_float(raw: str) -> float:
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("value must be finite")
    return value


def tbl_date(raw: str) -> date:
    return date.fromisoformat(raw)


# This is the complete table metadata needed by this experiment.  It is kept
# local so running or validating the experiment never executes the importer.
TPC_H_TBL_TABLES: tuple[TblTableSpec, ...] = (
    TblTableSpec(
        filename="region.tbl",
        label="REGION",
        columns=(
            TblColumnSpec("r_regionkey", tbl_int),
            TblColumnSpec("r_name", tbl_text),
            TblColumnSpec("r_comment", tbl_text),
        ),
        key_fields=("r_regionkey",),
    ),
    TblTableSpec(
        filename="nation.tbl",
        label="NATION",
        columns=(
            TblColumnSpec("n_nationkey", tbl_int),
            TblColumnSpec("n_name", tbl_text),
            TblColumnSpec("n_regionkey", tbl_int),
            TblColumnSpec("n_comment", tbl_text),
        ),
        key_fields=("n_nationkey",),
    ),
    TblTableSpec(
        filename="supplier.tbl",
        label="SUPPLIER",
        columns=(
            TblColumnSpec("s_suppkey", tbl_int),
            TblColumnSpec("s_name", tbl_text),
            TblColumnSpec("s_address", tbl_text),
            TblColumnSpec("s_nationkey", tbl_int),
            TblColumnSpec("s_phone", tbl_text),
            TblColumnSpec("s_acctbal", tbl_float),
            TblColumnSpec("s_comment", tbl_text),
        ),
        key_fields=("s_suppkey",),
    ),
    TblTableSpec(
        filename="customer.tbl",
        label="CUSTOMER",
        columns=(
            TblColumnSpec("c_custkey", tbl_int),
            TblColumnSpec("c_name", tbl_text),
            TblColumnSpec("c_address", tbl_text),
            TblColumnSpec("c_nationkey", tbl_int),
            TblColumnSpec("c_phone", tbl_text),
            TblColumnSpec("c_acctbal", tbl_float),
            TblColumnSpec("c_mktsegment", tbl_text),
            TblColumnSpec("c_comment", tbl_text),
        ),
        key_fields=("c_custkey",),
    ),
    TblTableSpec(
        filename="part.tbl",
        label="PART",
        columns=(
            TblColumnSpec("p_partkey", tbl_int),
            TblColumnSpec("p_name", tbl_text),
            TblColumnSpec("p_mfgr", tbl_text),
            TblColumnSpec("p_brand", tbl_text),
            TblColumnSpec("p_type", tbl_text),
            TblColumnSpec("p_size", tbl_int),
            TblColumnSpec("p_container", tbl_text),
            TblColumnSpec("p_retailprice", tbl_float),
            TblColumnSpec("p_comment", tbl_text),
        ),
        key_fields=("p_partkey",),
    ),
    TblTableSpec(
        filename="partsupp.tbl",
        label="PARTSUPP",
        columns=(
            TblColumnSpec("ps_partkey", tbl_int),
            TblColumnSpec("ps_suppkey", tbl_int),
            TblColumnSpec("ps_availqty", tbl_int),
            TblColumnSpec("ps_supplycost", tbl_float),
            TblColumnSpec("ps_comment", tbl_text),
        ),
        key_fields=("ps_partkey", "ps_suppkey"),
    ),
    TblTableSpec(
        filename="orders.tbl",
        label="ORDERS",
        columns=(
            TblColumnSpec("o_orderkey", tbl_int),
            TblColumnSpec("o_custkey", tbl_int),
            TblColumnSpec("o_orderstatus", tbl_text),
            TblColumnSpec("o_totalprice", tbl_float),
            TblColumnSpec("o_orderdate", tbl_date),
            TblColumnSpec("o_orderpriority", tbl_text),
            TblColumnSpec("o_clerk", tbl_text),
            TblColumnSpec("o_shippriority", tbl_int),
            TblColumnSpec("o_comment", tbl_text),
        ),
        key_fields=("o_orderkey",),
    ),
    TblTableSpec(
        filename="lineitem.tbl",
        label="LINEITEM",
        columns=(
            TblColumnSpec("l_orderkey", tbl_int),
            TblColumnSpec("l_partkey", tbl_int),
            TblColumnSpec("l_suppkey", tbl_int),
            TblColumnSpec("l_linenumber", tbl_int),
            TblColumnSpec("l_quantity", tbl_float),
            TblColumnSpec("l_extendedprice", tbl_float),
            TblColumnSpec("l_discount", tbl_float),
            TblColumnSpec("l_tax", tbl_float),
            TblColumnSpec("l_returnflag", tbl_text),
            TblColumnSpec("l_linestatus", tbl_text),
            TblColumnSpec("l_shipdate", tbl_date),
            TblColumnSpec("l_commitdate", tbl_date),
            TblColumnSpec("l_receiptdate", tbl_date),
            TblColumnSpec("l_shipinstruct", tbl_text),
            TblColumnSpec("l_shipmode", tbl_text),
            TblColumnSpec("l_comment", tbl_text),
        ),
        key_fields=("l_orderkey", "l_linenumber"),
    ),
)

TBL_TABLES_BY_LABEL: dict[str, TblTableSpec] = {
    table.label: table for table in TPC_H_TBL_TABLES
}
TABLES: dict[str, NodeSpec] = {
    table.label: NodeSpec(
        filename=table.filename,
        label=table.label,
        columns=tuple(column.name for column in table.columns),
        key_fields=table.key_fields,
    )
    for table in TPC_H_TBL_TABLES
}

# Stable table aliases retained in metadata.  TPC-H properties themselves are
# already globally namespaced, so physical folded properties keep their native
# names instead of adding a second prefix.
TABLE_ALIASES: dict[str, str] = {
    "REGION": "r",
    "NATION": "n",
    "SUPPLIER": "s",
    "CUSTOMER": "c",
    "PART": "p",
    "PARTSUPP": "ps",
    "ORDERS": "o",
    "LINEITEM": "l",
}

RELATIONSHIPS: tuple[RelationshipSpec, ...] = (
    RelationshipSpec(
        row_label="NATION",
        source_label="NATION",
        target_label="REGION",
        relationship_type="NATION_REGION",
        source_keys=(("n_nationkey", "n_nationkey"),),
        target_keys=(("r_regionkey", "n_regionkey"),),
    ),
    RelationshipSpec(
        row_label="SUPPLIER",
        source_label="SUPPLIER",
        target_label="NATION",
        relationship_type="SUPPLIER_NATION",
        source_keys=(("s_suppkey", "s_suppkey"),),
        target_keys=(("n_nationkey", "s_nationkey"),),
    ),
    RelationshipSpec(
        row_label="CUSTOMER",
        source_label="CUSTOMER",
        target_label="NATION",
        relationship_type="CUSTOMER_NATION",
        source_keys=(("c_custkey", "c_custkey"),),
        target_keys=(("n_nationkey", "c_nationkey"),),
    ),
    RelationshipSpec(
        row_label="ORDERS",
        source_label="ORDERS",
        target_label="CUSTOMER",
        relationship_type="ORDERS_CUSTOMER",
        source_keys=(("o_orderkey", "o_orderkey"),),
        target_keys=(("c_custkey", "o_custkey"),),
    ),
    RelationshipSpec(
        row_label="PARTSUPP",
        source_label="PARTSUPP",
        target_label="PART",
        relationship_type="PARTSUPP_PART",
        source_keys=(
            ("ps_partkey", "ps_partkey"),
            ("ps_suppkey", "ps_suppkey"),
        ),
        target_keys=(("p_partkey", "ps_partkey"),),
    ),
    RelationshipSpec(
        row_label="PARTSUPP",
        source_label="PARTSUPP",
        target_label="SUPPLIER",
        relationship_type="PARTSUPP_SUPPLIER",
        source_keys=(
            ("ps_partkey", "ps_partkey"),
            ("ps_suppkey", "ps_suppkey"),
        ),
        target_keys=(("s_suppkey", "ps_suppkey"),),
    ),
    RelationshipSpec(
        row_label="LINEITEM",
        source_label="LINEITEM",
        target_label="ORDERS",
        relationship_type="LINEITEM_ORDERS",
        source_keys=(
            ("l_orderkey", "l_orderkey"),
            ("l_linenumber", "l_linenumber"),
        ),
        target_keys=(("o_orderkey", "l_orderkey"),),
    ),
    RelationshipSpec(
        row_label="LINEITEM",
        source_label="LINEITEM",
        target_label="PARTSUPP",
        relationship_type="LINEITEM_PARTSUPP",
        source_keys=(
            ("l_orderkey", "l_orderkey"),
            ("l_linenumber", "l_linenumber"),
        ),
        target_keys=(
            ("ps_partkey", "l_partkey"),
            ("ps_suppkey", "l_suppkey"),
        ),
    ),
)

DEFAULT_UPDATE_PROPERTIES: dict[str, str] = {
    "nation_region": "r_name",
    "supplier_nation": "n_name",
    "customer_nation": "n_name",
    "orders_customer": "c_mktsegment",
    "partsupp_part": "p_name",
    "partsupp_supplier": "s_phone",
    "lineitem_orders": "o_orderpriority",
    "lineitem_partsupp": "ps_supplycost",
}
CASE_ORDER = (
    "nation_region",
    "supplier_nation",
    "customer_nation",
    "orders_customer",
    "partsupp_part",
    "partsupp_supplier",
    "lineitem_orders",
    "lineitem_partsupp",
)

MANUAL_TOPOLOGY_PROPERTIES: dict[str, tuple[str, ...]] = {}


def relationship_id(relationship: RelationshipSpec) -> str:
    return (
        f"{relationship.source_label}"
        f"-[:{relationship.relationship_type}]->"
        f"{relationship.target_label}"
    )


def topology_fk_properties_for(table: NodeSpec) -> tuple[str, ...]:
    fields: set[str] = set(MANUAL_TOPOLOGY_PROPERTIES.get(table.label, ()))
    for relationship in RELATIONSHIPS:
        if relationship.row_label != table.label:
            continue
        if relationship.source_label == table.label:
            endpoint_keys = relationship.target_keys
        elif relationship.target_label == table.label:
            endpoint_keys = relationship.source_keys
        else:
            raise ExperimentValidationError(
                f"{relationship_id(relationship)} row table is not an endpoint"
            )
        fields.update(row_property for _, row_property in endpoint_keys)
    return tuple(sorted(fields))


def make_folded_property_bindings(
    table: NodeSpec,
    properties: Sequence[str],
) -> tuple[PropertyBinding, ...]:
    return tuple(
        PropertyBinding(
            logical_name=prop,
            folded_name=prop,
        )
        for prop in properties
    )


def build_boundary_specs(
    join: RelationshipSpec,
    referencing: NodeSpec,
    referenced: NodeSpec,
) -> tuple[BoundaryEdgeSpec, ...]:
    boundaries: list[BoundaryEdgeSpec] = []
    owners: tuple[
        tuple[Literal["referencing", "referenced"], NodeSpec],
        ...,
    ] = (
        ("referencing", referencing),
        ("referenced", referenced),
    )
    for relationship in RELATIONSHIPS:
        if relationship == join:
            continue
        for owner, table in owners:
            if relationship.source_label == table.label:
                boundaries.append(
                    BoundaryEdgeSpec(
                        relationship=relationship,
                        owner=owner,
                        owner_is_source=True,
                        external_label=relationship.target_label,
                        copy_type=shadow_relationship_type(
                            relationship.relationship_type
                        ),
                    )
                )
            elif relationship.target_label == table.label:
                boundaries.append(
                    BoundaryEdgeSpec(
                        relationship=relationship,
                        owner=owner,
                        owner_is_source=False,
                        external_label=relationship.source_label,
                        copy_type=shadow_relationship_type(
                            relationship.relationship_type
                        ),
                    )
                )
    identifiers = [boundary.boundary_id for boundary in boundaries]
    if len(identifiers) != len(set(identifiers)):
        raise ExperimentValidationError(
            f"duplicate boundary definitions for {relationship_id(join)}"
        )
    copy_types = [boundary.copy_type for boundary in boundaries]
    if len(copy_types) != len(set(copy_types)):
        raise ExperimentValidationError(
            f"boundary copy type collision for {relationship_id(join)}: {copy_types!r}"
        )
    return tuple(boundaries)


def build_pair_spec(join: RelationshipSpec) -> PairSpec:
    referencing = TABLES[join.row_label]
    if join.source_label == referencing.label:
        referencing_is_source = True
        referenced = TABLES[join.target_label]
        referenced_endpoint_keys = join.target_keys
    elif join.target_label == referencing.label:
        referencing_is_source = False
        referenced = TABLES[join.source_label]
        referenced_endpoint_keys = join.source_keys
    else:
        raise ExperimentValidationError(
            f"{relationship_id(join)} does not contain row table {referencing.label}"
        )

    fk_mapping = tuple(
        (row_property, referenced_property)
        for referenced_property, row_property in referenced_endpoint_keys
    )
    mapped_referenced_keys = tuple(
        referenced_property for _, referenced_property in fk_mapping
    )
    if set(mapped_referenced_keys) != set(referenced.key_fields):
        raise ExperimentValidationError(
            f"{relationship_id(join)} FK does not cover referenced key: "
            f"{fk_mapping!r} vs {referenced.key_fields!r}"
        )

    case_id = f"{snake_case(referencing.label)}_{snake_case(referenced.label)}"
    dependent = tuple(
        prop
        for prop in referenced.columns
        if prop not in referenced.key_fields
    )
    referencing_bindings = make_folded_property_bindings(
        referencing,
        referencing.columns,
    )
    referenced_bindings = make_folded_property_bindings(
        referenced,
        dependent,
    )
    referencing_alias = TABLE_ALIASES[referencing.label]
    referenced_alias = TABLE_ALIASES[referenced.label]
    names = PairNames(
        folded_label=f"mv_{case_id}",
        referencing_label=f"mv_{snake_case(referencing.label)}",
        referenced_label=f"mv_{snake_case(referenced.label)}",
        normalized_join_type=shadow_relationship_type(
            join.relationship_type
        ),
    )
    default_update = DEFAULT_UPDATE_PROPERTIES.get(case_id)
    if default_update is None:
        raise ExperimentValidationError(
            f"missing TPC-H update metadata for {case_id}"
        )
    pair = PairSpec(
        case_id=case_id,
        referencing=referencing,
        referenced=referenced,
        join=join,
        referencing_is_source=referencing_is_source,
        fk_mapping=fk_mapping,
        dependent_properties=dependent,
        referencing_alias=referencing_alias,
        referenced_alias=referenced_alias,
        referencing_property_bindings=referencing_bindings,
        referenced_property_bindings=referenced_bindings,
        topology_fk_properties=topology_fk_properties_for(referenced),
        default_update_property=default_update,
        boundaries=build_boundary_specs(join, referencing, referenced),
        names=names,
    )
    if pair.default_update_property not in pair.allowed_update_properties:
        raise ExperimentValidationError(
            f"default update property {pair.default_update_property!r} is "
            f"not eligible for {case_id}"
        )
    return pair


CASES: dict[str, PairSpec] = {
    pair.case_id: pair
    for pair in (
        build_pair_spec(relationship) for relationship in RELATIONSHIPS
    )
}


def validate_case_catalog() -> None:
    if set(TABLE_ALIASES) != set(TABLES):
        raise ExperimentValidationError(
            "table-alias catalog must exactly match imported "
            f"tables: aliases={sorted(TABLE_ALIASES)!r}, "
            f"tables={sorted(TABLES)!r}"
        )
    aliases = tuple(TABLE_ALIASES.values())
    if len(aliases) != len(set(aliases)):
        raise ExperimentValidationError(
            "TPC-H table aliases must be globally unique"
        )
    invalid_aliases = sorted(
        alias
        for alias in aliases
        if re.fullmatch(r"[a-z][a-z0-9]*", alias) is None
    )
    if invalid_aliases:
        raise ExperimentValidationError(
            "TPC-H table aliases must match [a-z][a-z0-9]*: "
            f"{invalid_aliases!r}"
        )
    if set(CASES) != set(CASE_ORDER):
        raise ExperimentValidationError(
            "case catalog does not match the eight expected TPC-H pairs: "
            f"actual={sorted(CASES)!r}, expected={sorted(CASE_ORDER)!r}"
        )
    if tuple(DEFAULT_UPDATE_PROPERTIES) != CASE_ORDER:
        raise ExperimentValidationError(
            "default update-property order must match CASE_ORDER"
        )
    if len(CASES) != len(RELATIONSHIPS):
        raise ExperimentValidationError(
            "each imported relationship must map to exactly one pair case"
        )
    reverse_cases = [
        pair.case_id
        for pair in CASES.values()
        if not pair.referencing_is_source
    ]
    if reverse_cases:
        raise ExperimentValidationError(
            "every relationship must point from its referencing row table; "
            f"reverse-direction cases={reverse_cases!r}"
        )
    for pair in CASES.values():
        if tuple(
            binding.logical_name
            for binding in pair.referencing_property_bindings
        ) != pair.referencing.columns:
            raise ExperimentValidationError(
                f"{pair.case_id} referencing bindings do not cover all "
                "referencing columns in source order"
            )
        if tuple(
            binding.logical_name
            for binding in pair.referenced_property_bindings
        ) != pair.dependent_properties:
            raise ExperimentValidationError(
                f"{pair.case_id} referenced bindings must cover exactly the "
                "referenced non-key properties"
            )
        aliases = [
            binding.folded_name
            for binding in (
                *pair.referencing_property_bindings,
                *pair.referenced_property_bindings,
            )
        ]
        if len(aliases) != len(set(aliases)):
            raise ExperimentValidationError(
                f"{pair.case_id} has duplicate folded property aliases"
            )
        if any(
            binding.folded_name != binding.logical_name
            for binding in (
                *pair.referencing_property_bindings,
                *pair.referenced_property_bindings,
            )
        ):
            raise ExperimentValidationError(
                f"{pair.case_id} must preserve native TPC-H property names"
            )
        collisions = sorted(
            set(aliases)
            & {RUN_ID_PROPERTY, CASE_ID_PROPERTY}
        )
        if collisions:
            raise ExperimentValidationError(
                f"{pair.case_id} folded property aliases collide with "
                f"identity properties: {collisions!r}"
            )
        if any(
            binding.logical_name in pair.referenced.key_fields
            for binding in pair.referenced_property_bindings
        ):
            raise ExperimentValidationError(
                f"{pair.case_id} must not copy referenced key properties"
            )


validate_case_catalog()


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
            "Measure FD redundancy, update amplification, and normalization "
            "cost for direct TPC-H FK pairs in isolated shadow graphs."
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
            "existing imported TPC-H database "
            f"(default: NEO4J_DATABASE or {DEFAULT_DATABASE})"
        ),
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--case",
        dest="case_id",
        choices=CASE_ORDER,
        help=("run only this pair case; when omitted, all eight cases run"),
    )
    selection.add_argument(
        "--all-cases",
        "--all",
        action="store_true",
        help=(
            "explicitly run all eight cases sequentially (this is also the default)"
        ),
    )
    parser.add_argument(
        "--list-cases",
        action="store_true",
        help="print the pair catalog without connecting to Neo4j",
    )
    parser.add_argument(
        "--validate-cases-only",
        action="store_true",
        help="validate all pair metadata against local TPC-H .tbl files only",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="TPC-H .tbl directory used by --validate-cases-only",
    )
    parser.add_argument(
        "--topology-mode",
        choices=("complete", "property-fd"),
        default=DEFAULT_TOPOLOGY_MODE,
        help=(
            "complete copies declared boundary edges; property-fd isolates "
            f"node-property effects (default: {DEFAULT_TOPOLOGY_MODE})"
        ),
    )
    parser.add_argument(
        "--update-property",
        help=(
            "referenced-table non-key/non-topology property; omitted uses "
            "the case default; custom properties require --case"
        ),
    )
    parser.add_argument(
        "--warmup-runs",
        type=nonnegative_int,
        default=DEFAULT_WARMUP_RUNS,
    )
    parser.add_argument(
        "--runs",
        type=positive_int,
        default=DEFAULT_RUNS,
    )
    parser.add_argument(
        "--normalization-runs",
        type=positive_int,
        default=DEFAULT_NORMALIZATION_RUNS,
    )
    parser.add_argument(
        "--normalization-warmup-runs",
        type=nonnegative_int,
        default=DEFAULT_NORMALIZATION_WARMUP_RUNS,
    )
    parser.add_argument(
        "--index-wait-seconds",
        type=positive_int,
        default=DEFAULT_INDEX_WAIT_SECONDS,
    )
    parser.add_argument(
        "--batch-size",
        type=positive_int,
        default=DEFAULT_BATCH_SIZE,
        help=(
            "rows per committed mutation/update batch "
            f"(default: {DEFAULT_BATCH_SIZE})"
        ),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=DEFAULT_OUTPUT_JSON,
        help=(
            "path for the JSON report; defaults to "
            "test/redundancy/results/simple/"
            "tpch_fd_experiment_results.json"
        ),
    )
    parser.add_argument(
        "--cleanup-run-id",
        help=(
            "cleanup-only recovery for one printed run_id left by a hard stop"
        ),
    )
    return parser.parse_args(argv)


@contextmanager
def single_process_experiment_lock() -> Iterator[None]:
    try:
        import fcntl
    except ImportError:
        yield
        return
    handle = Path(__file__).resolve().open("rb")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ExperimentStateError(
                "another local TPC-H FD experiment is already running"
            ) from exc
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def result_records_and_summary(result: Any) -> tuple[list[Any], Any]:
    records = list(result)
    summary = result.consume()
    return records, summary


def one_record(result: Any, context: str) -> tuple[Any, Any]:
    records, summary = result_records_and_summary(result)
    if len(records) != 1:
        raise ExperimentValidationError(
            f"{context}: expected one record, found {len(records)}"
        )
    return records[0], summary


def scalar_count(
    session: Any,
    query: str,
    field: str,
    context: str,
    **parameters: Any,
) -> int:
    record, _ = one_record(
        session.run(query, parameters),
        context,
    )
    return int(record[field])


def cypher_map(entries: Sequence[tuple[str, str]]) -> str:
    return (
        "{"
        + ", ".join(
            f"{quote_ident(key)}: {expression}" for key, expression in entries
        )
        + "}"
    )


def equality_predicate(left: str, right: str) -> str:
    return (
        f"(({left} IS NULL AND {right} IS NULL) OR "
        f"({left} IS NOT NULL AND {right} IS NOT NULL AND {left} = {right}))"
    )


def join_pattern(
    pair: PairSpec,
    referencing_alias: str = "referencing",
    referenced_alias: str = "referenced",
    relationship_alias: str = "join_rel",
    include_labels: bool = True,
) -> str:
    referencing = (
        f"{referencing_alias}:{quote_ident(pair.referencing.label)}"
        if include_labels
        else referencing_alias
    )
    referenced = (
        f"{referenced_alias}:{quote_ident(pair.referenced.label)}"
        if include_labels
        else referenced_alias
    )
    relationship = (
        f"{relationship_alias}:{quote_ident(pair.join.relationship_type)}"
    )
    if pair.referencing_is_source:
        return f"({referencing})-[{relationship}]->({referenced})"
    return f"({referenced})-[{relationship}]->({referencing})"


def normalized_join_pattern(
    pair: PairSpec,
    referencing_alias: str = "referencing",
    referenced_alias: str = "referenced",
    relationship_alias: str = "join_rel",
    include_labels: bool = True,
    include_relationship_type: bool = True,
) -> str:
    referencing = (
        f"{referencing_alias}:{quote_ident(pair.names.referencing_label)}"
        if include_labels
        else referencing_alias
    )
    referenced = (
        f"{referenced_alias}:{quote_ident(pair.names.referenced_label)}"
        if include_labels
        else referenced_alias
    )
    relationship = (
        (
            f"{relationship_alias}:{quote_ident(pair.names.normalized_join_type)}"
        )
        if include_relationship_type
        else relationship_alias
    )
    if pair.referencing_is_source:
        return f"({referencing})-[{relationship}]->({referenced})"
    return f"({referenced})-[{relationship}]->({referencing})"


def boundary_pattern(
    boundary: BoundaryEdgeSpec,
    owner_alias: str,
    other_alias: str,
    relationship_alias: str,
    *,
    copied: bool,
    owner_label: str | None = None,
    include_other_label: bool = True,
) -> str:
    owner = (
        f"{owner_alias}:{quote_ident(owner_label)}"
        if owner_label is not None
        else owner_alias
    )
    other = (
        f"{other_alias}:{quote_ident(boundary.external_label)}"
        if include_other_label
        else other_alias
    )
    relationship_type = (
        boundary.copy_type
        if copied
        else boundary.relationship.relationship_type
    )
    relationship = f"{relationship_alias}:{quote_ident(relationship_type)}"
    if boundary.owner_is_source:
        return f"({owner})-[{relationship}]->({other})"
    return f"({other})-[{relationship}]->({owner})"


def node_key_predicates(
    node_alias: str,
    properties: Sequence[str],
    expressions: Sequence[str],
) -> str:
    if len(properties) != len(expressions):
        raise ValueError("key property/expression length mismatch")
    return " AND ".join(
        equality_predicate(
            f"{node_alias}.{quote_ident(prop)}",
            expression,
        )
        for prop, expression in zip(properties, expressions, strict=True)
    )


def strict_node_key_predicates(
    node_alias: str,
    properties: Sequence[str],
    expressions: Sequence[str],
) -> str:
    if len(properties) != len(expressions):
        raise ValueError("key property/expression length mismatch")
    return " AND ".join(
        (f"{node_alias}.{quote_ident(prop)} = {expression}")
        for prop, expression in zip(
            properties,
            expressions,
            strict=True,
        )
    )


def referencing_key_match_predicate(
    pair: PairSpec,
    shadow_alias: str,
    referencing_alias: str,
) -> str:
    return strict_node_key_predicates(
        shadow_alias,
        pair.folded_referencing_key_fields,
        tuple(
            f"{referencing_alias}.{quote_ident(prop)}"
            for prop in pair.referencing.key_fields
        ),
    )


def folded_referenced_key_expressions(
    pair: PairSpec,
    alias: str,
) -> tuple[str, ...]:
    return tuple(
        f"{alias}.{quote_ident(prop)}"
        for prop in pair.folded_referencing_fk_fields
    )


def normalized_referenced_key_expressions(
    pair: PairSpec,
    referencing_alias: str,
) -> tuple[str, ...]:
    return tuple(
        f"{referencing_alias}.{quote_ident(prop)}"
        for prop in pair.referencing_fk_fields
    )


def referenced_key_expressions(
    pair: PairSpec,
    alias: str,
) -> tuple[str, ...]:
    return tuple(
        f"{alias}.{quote_ident(prop)}" for prop in pair.referenced.key_fields
    )


def domain_value_token(value: Any) -> str:
    if isinstance(value, float) and not math.isfinite(value):
        raise ExperimentValidationError(
            "active-domain rotation does not support NaN or infinite floats"
        )
    if isinstance(value, float) and value == 0.0:
        value = 0.0
    value_type = f"{type(value).__module__}.{type(value).__qualname__}"
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return f"{value_type}:{payload}"


def sha256_json(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def value_payload_bytes(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    return len(str(value).encode("utf-8"))


def canonical_key(values: Iterable[Any]) -> tuple[Any, ...]:
    return tuple(values)


def node_projection(alias: str, properties: Sequence[str]) -> str:
    return cypher_map(
        tuple((prop, f"{alias}.{quote_ident(prop)}") for prop in properties)
    )


def join_result_sha256(
    result: Any,
    pair: PairSpec,
) -> tuple[str, int]:
    modulus = 1 << 256
    count = 0
    digest_sum = 0
    digest_xor = 0
    for record in result:
        referencing_values = dict(record["referencing_values"])
        referenced_values = dict(record["referenced_values"])
        payload = {
            "referencing_key": [
                referencing_values[prop]
                for prop in pair.referencing.key_fields
            ],
            "referenced_key": [
                referenced_values[prop] for prop in pair.referenced.key_fields
            ],
            "referencing_values": referencing_values,
            "referenced_values": referenced_values,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        row_digest = int.from_bytes(hashlib.sha256(encoded).digest(), "big")
        count += 1
        digest_sum = (digest_sum + row_digest) % modulus
        digest_xor ^= row_digest
    result.consume()
    fingerprint = sha256_json(
        {
            "algorithm": JOIN_FINGERPRINT_ALGORITHM,
            "count": count,
            "sum_sha256": f"{digest_sum:064x}",
            "xor_sha256": f"{digest_xor:064x}",
        }
    )
    return fingerprint, count


def original_join_sha256(
    session: Any,
    pair: PairSpec,
) -> tuple[str, int]:
    result = session.run(
        f"""
        MATCH {join_pattern(pair)}
        RETURN
          {node_projection("referencing", pair.referencing.columns)}
            AS referencing_values,
          {node_projection("referenced", pair.referenced.columns)}
            AS referenced_values
        """
    )
    return join_result_sha256(result, pair)


def original_graph_sha256(session: Any) -> str:
    """Stream-hash the imported TPC-H graph in one read transaction.

    Node identities use declared TPC-H labels and primary keys. Relationship
    identities use their type and both endpoint keys.  Commutative SHA-256
    accumulators make record order irrelevant while keeping client and server
    memory bounded.  Run-scoped shadow labels/types are outside this schema.
    """

    modulus = 1 << 256
    accumulators: dict[str, dict[str, int]] = {
        "node": {"count": 0, "sum": 0, "xor": 0},
        "relationship": {"count": 0, "sum": 0, "xor": 0},
    }

    def add_record(kind: str, payload: dict[str, Any]) -> None:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        record_digest = int.from_bytes(hashlib.sha256(encoded).digest(), "big")
        accumulator = accumulators[kind]
        accumulator["count"] += 1
        accumulator["sum"] = (
            accumulator["sum"] + record_digest
        ) % modulus
        accumulator["xor"] ^= record_digest

    transaction = session.begin_transaction()
    try:
        for table in TABLES.values():
            key_expressions = ", ".join(
                f"node.{quote_ident(prop)}" for prop in table.key_fields
            )
            node_result = transaction.run(
                f"""
                MATCH (node:{quote_ident(table.label)})
                WHERE NOT $artifact_label IN labels(node)
                RETURN
                  [{key_expressions}] AS node_key,
                  properties(node) AS properties
                """,
                artifact_label=ARTIFACT_LABEL,
            )
            for record in node_result:
                add_record(
                    "node",
                    {
                        "label": table.label,
                        "key": list(record["node_key"]),
                        "properties": dict(record["properties"]),
                    },
                )
            node_result.consume()

        for relationship in RELATIONSHIPS:
            source_key_expressions = ", ".join(
                f"source.{quote_ident(node_property)}"
                for node_property, _ in relationship.source_keys
            )
            target_key_expressions = ", ".join(
                f"target.{quote_ident(node_property)}"
                for node_property, _ in relationship.target_keys
            )
            relationship_result = transaction.run(
                f"""
                MATCH
                  (source:{quote_ident(relationship.source_label)})
                  -[relationship:{quote_ident(relationship.relationship_type)}]->
                  (target:{quote_ident(relationship.target_label)})
                WHERE NOT $artifact_label IN labels(source)
                  AND NOT $artifact_label IN labels(target)
                RETURN
                  [{source_key_expressions}] AS source_key,
                  properties(relationship) AS relationship_properties,
                  [{target_key_expressions}] AS target_key
                """,
                artifact_label=ARTIFACT_LABEL,
            )
            for record in relationship_result:
                add_record(
                    "relationship",
                    {
                        "source_label": relationship.source_label,
                        "source_key": list(record["source_key"]),
                        "type": relationship.relationship_type,
                        "properties": dict(
                            record["relationship_properties"]
                        ),
                        "target_label": relationship.target_label,
                        "target_key": list(record["target_key"]),
                    },
                )
            relationship_result.consume()
        transaction.commit()
    except BaseException:
        try:
            transaction.rollback()
        except BaseException:
            pass
        raise
    fingerprint_state: dict[str, Any] = {
        "algorithm": SOURCE_GRAPH_FINGERPRINT_ALGORITHM,
        "accumulators": {
            kind: {
                "count": accumulator["count"],
                "sum_sha256": f"{accumulator['sum']:064x}",
                "xor_sha256": f"{accumulator['xor']:064x}",
            }
            for kind, accumulator in accumulators.items()
        },
    }
    return sha256_json(fingerprint_state)


def database_node_labels(session: Any) -> frozenset[str]:
    """Return visible node-label tokens without scanning graph nodes."""
    records, _ = result_records_and_summary(
        session.run("CALL db.labels() YIELD label RETURN label")
    )
    return frozenset(str(record["label"]) for record in records)


def artifact_counts(session: Any) -> tuple[int, int]:
    # A static label reference produces Neo4j notification 01N50 when the
    # label token has never existed.  A fresh database is already clean, so
    # avoid all three labelled count queries in that state.
    if ARTIFACT_LABEL not in database_node_labels(session):
        return 0, 0

    nodes = scalar_count(
        session,
        f"MATCH (node:{quote_ident(ARTIFACT_LABEL)}) "
        "RETURN count(node) AS count",
        "count",
        "count experiment artifact nodes",
    )
    outgoing = scalar_count(
        session,
        f"MATCH (node:{quote_ident(ARTIFACT_LABEL)})-[relationship]->() "
        "RETURN count(relationship) AS count",
        "count",
        "count outgoing experiment artifact relationships",
    )
    incoming_from_non_artifact = scalar_count(
        session,
        f"""
        MATCH (other)-[relationship]->(
          node:{quote_ident(ARTIFACT_LABEL)}
        )
        WHERE NOT other:{quote_ident(ARTIFACT_LABEL)}
        RETURN count(relationship) AS count
        """,
        "count",
        "count incoming experiment artifact relationships",
    )
    return nodes, outgoing + incoming_from_non_artifact


def existing_artifact_run_ids(session: Any) -> list[str]:
    result = session.run(
        f"""
        MATCH (node:{quote_ident(ARTIFACT_LABEL)})
        RETURN DISTINCT
          node.{quote_ident(RUN_ID_PROPERTY)} AS run_id,
          node.{quote_ident(CASE_ID_PROPERTY)} AS case_id
        ORDER BY run_id, case_id
        """
    )
    records, _ = result_records_and_summary(result)
    return [f"{record['run_id']} ({record['case_id']})" for record in records]


def ensure_no_existing_artifacts(
    session: Any,
    pairs: Sequence[PairSpec],
) -> None:
    nodes, relationships = artifact_counts(session)
    if nodes or relationships:
        run_ids = existing_artifact_run_ids(session)
        raise ExperimentStateError(
            "existing MV experiment artifacts must be cleaned first: "
            f"nodes={nodes}, relationships={relationships}, runs={run_ids!r}"
        )
    labels = sorted(
        {
            label
            for pair in pairs
            for label in (
                pair.names.folded_label,
                pair.names.referencing_label,
                pair.names.referenced_label,
            )
        }
    )
    record, _ = one_record(
        session.run(
            """
            MATCH (node)
            WHERE any(label IN labels(node) WHERE label IN $reserved_labels)
            RETURN count(node) AS count
            """,
            reserved_labels=labels,
        ),
        "check reserved MV labels",
    )
    if int(record["count"]):
        raise ExperimentStateError(
            f"nodes already use one or more reserved MV labels: {labels!r}"
        )


def validate_original_pair_shape(
    session: Any,
    pair: PairSpec,
) -> tuple[int, int, int, int]:
    referencing_count = scalar_count(
        session,
        (
            f"MATCH (node:{quote_ident(pair.referencing.label)}) "
            "RETURN count(node) AS count"
        ),
        "count",
        f"count {pair.referencing.label} nodes",
    )
    referenced_count = scalar_count(
        session,
        (
            f"MATCH (node:{quote_ident(pair.referenced.label)}) "
            "RETURN count(node) AS count"
        ),
        "count",
        f"count {pair.referenced.label} nodes",
    )

    for table in (pair.referencing, pair.referenced):
        shape_record, _ = one_record(
            session.run(
                f"""
                MATCH (node:{quote_ident(table.label)})
                WITH
                  node,
                  [
                    label IN labels(node)
                    WHERE label <> $expected_label
                  ] AS extra_labels,
                  [
                    property IN keys(node)
                    WHERE NOT property IN $allowed_properties
                  ] AS extra_properties
                RETURN
                  sum(
                    CASE WHEN size(extra_labels) = 0 THEN 0 ELSE 1 END
                  ) AS nodes_with_extra_labels,
                  sum(
                    CASE WHEN size(extra_properties) = 0 THEN 0 ELSE 1 END
                  ) AS nodes_with_extra_properties
                """,
                expected_label=table.label,
                allowed_properties=list(table.columns),
            ),
            f"validate {table.label} node shape",
        )
        extra_labels = int(shape_record["nodes_with_extra_labels"])
        extra_properties = int(shape_record["nodes_with_extra_properties"])
        if extra_labels or extra_properties:
            raise ExperimentValidationError(
                f"{pair.case_id}: {table.label} has nodes outside the "
                "declared import schema; extra-label nodes="
                f"{extra_labels}, extra-property nodes={extra_properties}"
            )

    mismatch_terms = [
        (
            "NOT "
            + equality_predicate(
                f"referencing.{quote_ident(referencing_fk)}",
                f"referenced.{quote_ident(referenced_key)}",
            )
        )
        for referencing_fk, referenced_key in pair.fk_mapping
    ]
    mismatch_predicate = " OR ".join(mismatch_terms) or "false"
    record, _ = one_record(
        session.run(
            f"""
            MATCH (referencing:{quote_ident(pair.referencing.label)})
            CALL (referencing) {{
              OPTIONAL MATCH {join_pattern(pair)}
              RETURN
                count(join_rel) AS degree,
                sum(
                  CASE
                    WHEN referenced IS NOT NULL
                     AND ({mismatch_predicate})
                    THEN 1 ELSE 0
                  END
                ) AS mismatch_edges
            }}
            RETURN
              count(referencing) AS referencing_count,
              sum(CASE WHEN degree = 1 THEN 0 ELSE 1 END)
                AS invalid_degrees,
              sum(
                CASE
                  WHEN degree = 1
                   AND mismatch_edges = 0
                  THEN 0 ELSE 1
                END
              ) AS fk_mismatches
            """
        ),
        f"validate {pair.case_id} join cardinality",
    )
    invalid_degrees = int(record["invalid_degrees"])
    fk_mismatches = int(record["fk_mismatches"])
    if int(record["referencing_count"]) != referencing_count:
        raise ExperimentValidationError(
            f"{pair.case_id}: referencing count changed during validation"
        )
    if invalid_degrees or fk_mismatches:
        raise ExperimentValidationError(
            f"{pair.case_id}: every {pair.referencing.label} must have exactly "
            f"one {pair.join.relationship_type} edge to the FK-matching "
            f"{pair.referenced.label}; invalid degrees={invalid_degrees}, "
            f"FK mismatches={fk_mismatches}"
        )

    join_record, _ = one_record(
        session.run(
            f"""
            MATCH {join_pattern(pair)}
            RETURN
              count(join_rel) AS join_rows,
              sum(CASE WHEN size(keys(join_rel)) = 0 THEN 0 ELSE 1 END)
                AS relationships_with_properties,
              sum(
                CASE
                  WHEN $run_property IN keys(referencing)
                    OR $case_property IN keys(referencing)
                    OR $run_property IN keys(referenced)
                    OR $case_property IN keys(referenced)
                  THEN 1 ELSE 0
                END
              ) AS reserved_property_uses
            """,
            run_property=RUN_ID_PROPERTY,
            case_property=CASE_ID_PROPERTY,
        ),
        f"validate {pair.case_id} source join",
    )
    join_rows = int(join_record["join_rows"])
    participating = scalar_count(
        session,
        f"""
        MATCH (referenced:{quote_ident(pair.referenced.label)})
        WHERE EXISTS {{
          MATCH {join_pattern(pair)}
        }}
        RETURN count(referenced) AS count
        """,
        "count",
        f"count participating {pair.referenced.label} rows",
    )
    if int(join_record["relationships_with_properties"]):
        raise ExperimentValidationError(
            f"{pair.case_id}: the folded representation cannot preserve "
            "properties on the selected join relationship"
        )
    if int(join_record["reserved_property_uses"]):
        raise ExperimentValidationError(
            f"{pair.case_id}: source nodes use reserved properties "
            f"{RUN_ID_PROPERTY!r}/{CASE_ID_PROPERTY!r}"
        )

    problems: list[str] = []
    if referencing_count != join_rows:
        problems.append(
            f"referencing rows={referencing_count}, but join rows={join_rows}"
        )
    if participating > referenced_count:
        problems.append(
            f"participating referenced rows={participating}, but the full "
            f"referenced relation has only {referenced_count} rows"
        )
    if problems:
        raise ExperimentValidationError(
            f"{pair.case_id} violates the imported TPC-H FK invariants: "
            + "; ".join(problems)
        )
    return referencing_count, referenced_count, join_rows, participating


def boundary_expected_counts(
    session: Any,
    pair: PairSpec,
    topology_mode: str,
) -> tuple[BoundaryExpectation, ...]:
    if topology_mode == "property-fd":
        return ()
    expectations: list[BoundaryExpectation] = []
    for boundary in pair.boundaries:
        owner_alias = (
            "referencing" if boundary.owner == "referencing" else "referenced"
        )
        other_alias = "external"
        source_pattern = boundary_pattern(
            boundary,
            owner_alias,
            other_alias,
            "boundary_rel",
            copied=False,
        )
        folded_record, _ = one_record(
            session.run(
                f"""
                MATCH {join_pattern(pair)}
                MATCH {source_pattern}
                RETURN count(boundary_rel) AS count
                """
            ),
            f"count folded boundary {boundary.boundary_id}",
        )
        normalized_record, _ = one_record(
            session.run(
                f"""
                MATCH (
                  {owner_alias}:{quote_ident(
                      pair.referencing.label
                      if boundary.owner == "referencing"
                      else pair.referenced.label
                  )}
                )
                WHERE EXISTS {{
                  MATCH {join_pattern(pair)}
                }}
                MATCH {source_pattern}
                RETURN count(boundary_rel) AS count
                """
            ),
            f"count normalized boundary {boundary.boundary_id}",
        )
        expectations.append(
            BoundaryExpectation(
                boundary_id=boundary.boundary_id,
                owner=boundary.owner,
                copy_type=boundary.copy_type,
                folded_count=int(folded_record["count"]),
                normalized_count=int(normalized_record["count"]),
            )
        )
    return tuple(expectations)


def validate_source_primary_key_constraints(
    session: Any,
    pair: PairSpec,
) -> None:
    result = session.run(
        """
        SHOW CONSTRAINTS
        YIELD type, entityType, labelsOrTypes, properties
        RETURN type, entityType, labelsOrTypes, properties
        """
    )
    records, _ = result_records_and_summary(result)
    missing: list[str] = []
    for table in (pair.referencing, pair.referenced):
        found = any(
            str(record["entityType"]).upper() == "NODE"
            and table.label in (record["labelsOrTypes"] or ())
            and tuple(record["properties"] or ()) == table.key_fields
            and (
                "UNIQUE" in str(record["type"]).upper()
                or "KEY" in str(record["type"]).upper()
            )
            for record in records
        )
        if not found:
            missing.append(
                f"{table.label}{tuple(table.key_fields)!r}"
            )
    if missing:
        raise ExperimentValidationError(
            f"{pair.case_id}: missing imported TPC-H primary-key "
            f"constraint(s): {', '.join(missing)}"
        )


def build_case_baseline(
    session: Any,
    pair: PairSpec,
    topology_mode: str,
    source_sha256: str,
) -> CaseBaseline:
    validate_source_primary_key_constraints(session, pair)
    referencing_rows, referenced_rows, join_count, participating = (
        validate_original_pair_shape(session, pair)
    )
    join_sha256, fingerprint_rows = original_join_sha256(session, pair)
    if fingerprint_rows != join_count:
        raise ExperimentValidationError(
            f"{pair.case_id}: join fingerprint read {fingerprint_rows} rows; "
            f"expected {join_count}"
        )
    baseline = CaseBaseline(
        referencing_rows=referencing_rows,
        referenced_rows=referenced_rows,
        join_rows=join_count,
        participating_referenced_rows=participating,
        join_sha256=join_sha256,
        source_sha256=source_sha256,
        boundary_expectations=boundary_expected_counts(
            session,
            pair,
            topology_mode,
        ),
    )
    return baseline


def relevant_relationships(pair: PairSpec) -> tuple[RelationshipSpec, ...]:
    labels = {pair.referencing.label, pair.referenced.label}
    return tuple(
        relationship
        for relationship in RELATIONSHIPS
        if (
            relationship.source_label in labels
            or relationship.target_label in labels
        )
    )


def validate_complete_source_topology(
    session: Any,
    pair: PairSpec,
) -> None:
    """Reject undeclared, misdirected, and parallel boundary relationships."""

    relevant = relevant_relationships(pair)
    allowed_types = sorted(
        {relationship.relationship_type for relationship in relevant}
    )
    for table in (pair.referencing, pair.referenced):
        result = session.run(
            f"""
            MATCH (owner:{quote_ident(table.label)})-[relationship]-()
            WHERE NOT type(relationship) IN $allowed_types
            RETURN type(relationship) AS relationship_type
            LIMIT 1
            """,
            allowed_types=allowed_types,
        )
        unexpected = result.single()
        result.consume()
        if unexpected is not None:
            raise ExperimentValidationError(
                f"{pair.case_id}: undeclared "
                f"{unexpected['relationship_type']!r} relationship(s) are "
                f"adjacent to {table.label} and cannot be copied safely"
            )

    for relationship in relevant:
        exact = scalar_count(
            session,
            (
                f"MATCH (source:{quote_ident(relationship.source_label)})"
                f"-[rel:{quote_ident(relationship.relationship_type)}]->"
                f"(target:{quote_ident(relationship.target_label)}) "
                "RETURN count(rel) AS count"
            ),
            "count",
            f"count exact {relationship_id(relationship)}",
        )
        incident = scalar_count(
            session,
            f"""
            MATCH (source)-[relationship:{quote_ident(
                relationship.relationship_type
            )}]->(target)
            WHERE source:{quote_ident(pair.referencing.label)}
               OR source:{quote_ident(pair.referenced.label)}
               OR target:{quote_ident(pair.referencing.label)}
               OR target:{quote_ident(pair.referenced.label)}
            RETURN count(relationship) AS count
            """,
            "count",
            f"count incident {relationship.relationship_type}",
        )
        if incident != exact:
            raise ExperimentValidationError(
                f"{pair.case_id}: {relationship.relationship_type} has "
                f"{incident - exact} relationship(s) with an unsupported "
                "direction or endpoint label"
            )
        parallel = scalar_count(
            session,
            f"""
            MATCH (source:{quote_ident(relationship.source_label)})
            CALL (source) {{
              MATCH
                (source)-[rel:{quote_ident(relationship.relationship_type)}]->
                (target:{quote_ident(relationship.target_label)})
              WITH target, count(rel) AS multiplicity
              RETURN sum(
                CASE WHEN multiplicity > 1 THEN 1 ELSE 0 END
              ) AS parallel_targets
            }}
            RETURN coalesce(sum(parallel_targets), 0) AS count
            """,
            "count",
            f"count parallel {relationship_id(relationship)}",
        )
        if parallel:
            raise ExperimentValidationError(
                f"{pair.case_id}: {relationship_id(relationship)} contains "
                f"{parallel} parallel endpoint pair(s); v1 complete topology "
                "requires at most one relationship per endpoint pair"
            )


def schema_name(
    pair: PairSpec,
    run_id: str,
    kind: str,
) -> str:
    return safe_schema_token(f"{SCHEMA_PREFIX}_{pair.case_id}_{run_id}_{kind}")


def case_schema_names(
    pair: PairSpec,
    run_id: str,
) -> dict[str, str]:
    return {
        kind: schema_name(pair, run_id, kind)
        for kind in (
            "folded_key",
            "referencing_key",
            "referenced_key",
            "folded_fk",
            "referencing_fk",
        )
    }


def composite_property_expression(
    alias: str,
    properties: Sequence[str],
) -> str:
    expressions = ", ".join(
        f"{alias}.{quote_ident(prop)}" for prop in properties
    )
    return f"({expressions})"


def create_case_schema(
    session: Any,
    pair: PairSpec,
    run_id: str,
    wait_seconds: int,
) -> tuple[list[ConstraintLease], list[IndexLease]]:
    names = case_schema_names(pair, run_id)
    constraint_definitions = (
        (
            names["folded_key"],
            pair.names.folded_label,
            (RUN_ID_PROPERTY, *pair.folded_referencing_key_fields),
        ),
        (
            names["referencing_key"],
            pair.names.referencing_label,
            (RUN_ID_PROPERTY, *pair.referencing.key_fields),
        ),
        (
            names["referenced_key"],
            pair.names.referenced_label,
            (RUN_ID_PROPERTY, *pair.referenced.key_fields),
        ),
    )
    index_definitions = (
        (
            names["folded_fk"],
            pair.names.folded_label,
            (RUN_ID_PROPERTY, *pair.folded_referencing_fk_fields),
        ),
        (
            names["referencing_fk"],
            pair.names.referencing_label,
            (RUN_ID_PROPERTY, *pair.referencing_fk_fields),
        ),
    )

    constraints: list[ConstraintLease] = []
    indexes: list[IndexLease] = []
    try:
        for name, label, properties in constraint_definitions:
            session.run(
                f"""
                CREATE CONSTRAINT {quote_ident(name)} IF NOT EXISTS
                FOR (node:{quote_ident(label)})
                REQUIRE {composite_property_expression("node", properties)}
                  IS UNIQUE
                """
            ).consume()
            constraints.append(
                ConstraintLease(
                    name=name,
                    created_by_experiment=True,
                )
            )
        for name, label, properties in index_definitions:
            session.run(
                f"""
                CREATE INDEX {quote_ident(name)} IF NOT EXISTS
                FOR (node:{quote_ident(label)})
                ON {composite_property_expression("node", properties)}
                """
            ).consume()
            indexes.append(
                IndexLease(
                    name=name,
                    created_by_experiment=True,
                )
            )
        session.run(
            "CALL db.awaitIndexes($timeout_seconds)",
            timeout_seconds=wait_seconds,
        ).consume()
    except BaseException:
        drop_case_schema(session, constraints, indexes)
        raise
    return constraints, indexes


def drop_case_schema(
    session: Any,
    constraints: Sequence[ConstraintLease],
    indexes: Sequence[IndexLease],
) -> None:
    errors: list[BaseException] = []
    for index in reversed(indexes):
        if not index.created_by_experiment or index.name is None:
            continue
        try:
            session.run(
                f"DROP INDEX {quote_ident(index.name)} IF EXISTS"
            ).consume()
        except BaseException as exc:
            errors.append(exc)
    for constraint in reversed(constraints):
        if not constraint.created_by_experiment or constraint.name is None:
            continue
        try:
            session.run(
                f"DROP CONSTRAINT {quote_ident(constraint.name)} IF EXISTS"
            ).consume()
        except BaseException as exc:
            errors.append(exc)
    if errors:
        details = "; ".join(
            f"{type(error).__name__}: {error}" for error in errors
        )
        raise RuntimeError(f"failed to drop experiment schema: {details}")


def drop_recovery_schema(
    session: Any,
    run_id: str,
) -> tuple[list[str], list[str]]:
    expected = {
        name
        for pair in CASES.values()
        for name in case_schema_names(pair, run_id).values()
    }
    dropped_constraints: list[str] = []
    dropped_indexes: list[str] = []

    result = session.run("SHOW CONSTRAINTS YIELD name RETURN name")
    records, _ = result_records_and_summary(result)
    for record in records:
        name = str(record["name"])
        if name in expected:
            session.run(
                f"DROP CONSTRAINT {quote_ident(name)} IF EXISTS"
            ).consume()
            dropped_constraints.append(name)

    result = session.run("SHOW INDEXES YIELD name RETURN name")
    records, _ = result_records_and_summary(result)
    for record in records:
        name = str(record["name"])
        if name in expected:
            session.run(f"DROP INDEX {quote_ident(name)} IF EXISTS").consume()
            dropped_indexes.append(name)
    return dropped_constraints, dropped_indexes


def artifact_identity_map() -> str:
    return cypher_map(
        (
            (RUN_ID_PROPERTY, "$run_id"),
            (CASE_ID_PROPERTY, "$case_id"),
        )
    )


def case_parameters(pair: PairSpec, run_id: str) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "case_id": pair.case_id,
    }


def folded_projection(
    bindings: Sequence[PropertyBinding],
    alias: str,
) -> str:
    return cypher_map(
        tuple(
            (
                binding.logical_name,
                f"{alias}.{quote_ident(binding.folded_name)}",
            )
            for binding in bindings
        )
    )


def folded_referencing_projection(
    pair: PairSpec,
    alias: str,
) -> str:
    return folded_projection(pair.referencing_property_bindings, alias)


def folded_referenced_projection(
    pair: PairSpec,
    alias: str,
) -> str:
    fk_by_referenced_key = {
        referenced_key: referencing_fk
        for referencing_fk, referenced_key in pair.fk_mapping
    }
    entries: list[tuple[str, str]] = [
        (
            referenced_key,
            f"{alias}.{quote_ident(
                pair.referencing_binding_by_logical_name[
                    fk_by_referenced_key[referenced_key]
                ]
            )}",
        )
        for referenced_key in pair.referenced.key_fields
    ]
    entries.extend(
        (
            binding.logical_name,
            f"{alias}.{quote_ident(binding.folded_name)}",
        )
        for binding in pair.referenced_property_bindings
    )
    return cypher_map(entries)


def folded_property_map(
    bindings: Sequence[PropertyBinding],
    source_alias: str,
) -> str:
    return cypher_map(
        tuple(
            (
                binding.folded_name,
                f"{source_alias}.{quote_ident(binding.logical_name)}",
            )
            for binding in bindings
        )
    )


def folded_referencing_property_map(
    pair: PairSpec,
    referencing_alias: str,
) -> str:
    return folded_property_map(
        pair.referencing_property_bindings,
        referencing_alias,
    )


def folded_referenced_property_map(
    pair: PairSpec,
    referenced_alias: str,
) -> str:
    return folded_property_map(
        pair.referenced_property_bindings,
        referenced_alias,
    )


def referenced_property_map_from_folded(
    pair: PairSpec,
    folded_alias: str,
) -> str:
    fk_by_referenced_key = {
        referenced_key: referencing_fk
        for referencing_fk, referenced_key in pair.fk_mapping
    }
    entries: list[tuple[str, str]] = [
        (
            referenced_key,
            f"{folded_alias}.{quote_ident(
                pair.referencing_binding_by_logical_name[
                    fk_by_referenced_key[referenced_key]
                ]
            )}",
        )
        for referenced_key in pair.referenced.key_fields
    ]
    entries.extend(
        (
            binding.logical_name,
            f"{folded_alias}.{quote_ident(binding.folded_name)}",
        )
        for binding in pair.referenced_property_bindings
    )
    return cypher_map(entries)


def create_shadow_projection(
    session: Any,
    pair: PairSpec,
    run_id: str,
    topology_mode: str,
    batch_size: int,
) -> dict[str, Any]:
    parameters = case_parameters(pair, run_id)
    boundary_counts: dict[str, int] = {}
    node_result = session.run(
        f"""
        MATCH {join_pattern(pair)}
        CALL (referencing, referenced) {{
            CREATE (
              shadow:{quote_ident(ARTIFACT_LABEL)}
                    :{quote_ident(pair.names.folded_label)}
            )
            SET shadow =
              {folded_referencing_property_map(pair, "referencing")}
            SET shadow +=
              {folded_referenced_property_map(pair, "referenced")}
            SET shadow.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
                shadow.{quote_ident(CASE_ID_PROPERTY)} = $case_id
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    node_summary = node_result.consume()
    node_count = int(node_summary.counters.nodes_created)

    if topology_mode == "complete":
        for boundary in pair.boundaries:
            owner_alias = (
                "referencing"
                if boundary.owner == "referencing"
                else "referenced"
            )
            source_pattern = boundary_pattern(
                boundary,
                owner_alias,
                "external",
                "source_rel",
                copied=False,
            )
            copy_pattern = boundary_pattern(
                boundary,
                "shadow",
                "external",
                "copy_rel",
                copied=True,
                include_other_label=False,
            )
            boundary_result = session.run(
                f"""
                MATCH {join_pattern(pair)}
                MATCH {source_pattern}
                MATCH (
                  shadow:{quote_ident(ARTIFACT_LABEL)}
                        :{quote_ident(pair.names.folded_label)}
                  {artifact_identity_map()}
                )
                USING INDEX shadow:{quote_ident(pair.names.folded_label)}(
                  {
                    ", ".join(
                        quote_ident(prop)
                        for prop in (
                            RUN_ID_PROPERTY,
                            *pair.folded_referencing_key_fields,
                        )
                    )
                }
                )
                WHERE {
                    referencing_key_match_predicate(
                        pair,
                        "shadow",
                        "referencing",
                    )
                }
                CALL (shadow, external, source_rel) {{
                    CREATE {copy_pattern}
                    SET copy_rel = properties(source_rel)
                }} IN TRANSACTIONS OF {batch_size} ROWS
                """,
                parameters,
            )
            boundary_summary = boundary_result.consume()
            boundary_counts[boundary.boundary_id] = int(
                boundary_summary.counters.relationships_created
            )
    return {
        "folded_nodes": node_count,
        "boundary_relationships": boundary_counts,
        "boundary_relationship_total": sum(boundary_counts.values()),
    }


def count_case_nodes(
    session: Any,
    pair: PairSpec,
    run_id: str,
    label: str,
) -> int:
    return scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
          {artifact_identity_map()}
        )
        RETURN count(node) AS count
        """,
        "count",
        f"count {label} nodes",
        **case_parameters(pair, run_id),
    )


def inspect_shadow_state(
    session: Any,
    pair: PairSpec,
    run_id: str,
) -> ShadowState:
    folded = count_case_nodes(
        session,
        pair,
        run_id,
        pair.names.folded_label,
    )
    referencing_nodes = count_case_nodes(
        session,
        pair,
        run_id,
        pair.names.referencing_label,
    )
    referenced_nodes = count_case_nodes(
        session,
        pair,
        run_id,
        pair.names.referenced_label,
    )
    normalized_relationships = scalar_count(
        session,
        f"""
        MATCH {
            normalized_join_pattern(
                pair,
                include_relationship_type=False,
            )
        }
        WHERE type(join_rel) = $relationship_type
          AND referencing.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND referencing.{quote_ident(CASE_ID_PROPERTY)} = $case_id
          AND referenced.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND referenced.{quote_ident(CASE_ID_PROPERTY)} = $case_id
        RETURN count(join_rel) AS count
        """,
        "count",
        f"count normalized joins for {pair.case_id}",
        relationship_type=pair.names.normalized_join_type,
        **case_parameters(pair, run_id),
    )
    boundary_counts: dict[str, int] = {}
    for boundary in pair.boundaries:
        boundary_counts[boundary.boundary_id] = scalar_count(
            session,
            f"""
            MATCH (
              node:{quote_ident(ARTIFACT_LABEL)}
              {artifact_identity_map()}
            )-[relationship]-()
            WHERE type(relationship) = $relationship_type
            RETURN count(relationship) AS count
            """,
            "count",
            f"count copied boundary {boundary.boundary_id}",
            relationship_type=boundary.copy_type,
            **case_parameters(pair, run_id),
        )
    return ShadowState(
        folded_join_nodes=folded,
        normalized_referencing_nodes=referencing_nodes,
        normalized_referenced_nodes=referenced_nodes,
        normalized_join_relationships=normalized_relationships,
        boundary_relationships=boundary_counts,
    )


def folded_join_sha256(
    session: Any,
    pair: PairSpec,
    run_id: str,
) -> tuple[str, int]:
    result = session.run(
        f"""
        MATCH (
          shadow:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(pair.names.folded_label)}
          {artifact_identity_map()}
        )
        RETURN
          {folded_referencing_projection(pair, "shadow")}
            AS referencing_values,
          {folded_referenced_projection(pair, "shadow")}
            AS referenced_values
        """,
        **case_parameters(pair, run_id),
    )
    return join_result_sha256(result, pair)


def normalized_join_sha256(
    session: Any,
    pair: PairSpec,
    run_id: str,
) -> tuple[str, int]:
    result = session.run(
        f"""
        MATCH {normalized_join_pattern(pair)}
        WHERE referencing.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND referencing.{quote_ident(CASE_ID_PROPERTY)} = $case_id
          AND referenced.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND referenced.{quote_ident(CASE_ID_PROPERTY)} = $case_id
        RETURN
          {node_projection("referencing", pair.referencing.columns)}
            AS referencing_values,
          {node_projection("referenced", pair.referenced.columns)}
            AS referenced_values
        """,
        **case_parameters(pair, run_id),
    )
    return join_result_sha256(result, pair)


def validate_artifact_relationship_types(
    session: Any,
    pair: PairSpec,
    run_id: str,
) -> None:
    result = session.run(
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )-[relationship]-()
        WHERE NOT type(relationship) IN $allowed_types
        RETURN type(relationship) AS relationship_type
        LIMIT 1
        """,
        allowed_types=list(pair.all_copy_relationship_types),
        **case_parameters(pair, run_id),
    )
    unexpected = result.single()
    result.consume()
    if unexpected is not None:
        raise ExperimentStateError(
            f"{pair.case_id}: unexpected "
            f"{unexpected['relationship_type']!r} relationship(s) touch run "
            f"{run_id}; cleanup is fail-closed"
        )


def count_all_artifact_relationships(
    session: Any,
    pair: PairSpec,
    run_id: str,
) -> int:
    parameters = case_parameters(pair, run_id)
    outgoing = scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )-[relationship]->()
        RETURN count(relationship) AS count
        """,
        "count",
        f"count outgoing artifact relationships for {pair.case_id}",
        **parameters,
    )
    incoming = scalar_count(
        session,
        f"""
        MATCH (other)-[relationship]->(
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )
        WHERE NOT (
          other:{quote_ident(ARTIFACT_LABEL)}
          AND other.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND other.{quote_ident(CASE_ID_PROPERTY)} = $case_id
        )
        RETURN count(relationship) AS count
        """,
        "count",
        f"count incoming artifact relationships for {pair.case_id}",
        **parameters,
    )
    return outgoing + incoming


def validate_artifact_node_labels(
    session: Any,
    pair: PairSpec,
    run_id: str,
    *,
    phase: Literal["folded", "normalized"],
    expected_total: int,
) -> None:
    allowed_phase_labels = (
        [ARTIFACT_LABEL, pair.names.folded_label]
        if phase == "folded"
        else [
            ARTIFACT_LABEL,
            pair.names.referencing_label,
            pair.names.referenced_label,
        ]
    )
    expected_label_sets = (
        [[ARTIFACT_LABEL, pair.names.folded_label]]
        if phase == "folded"
        else [
            [ARTIFACT_LABEL, pair.names.referencing_label],
            [ARTIFACT_LABEL, pair.names.referenced_label],
        ]
    )
    record, _ = one_record(
        session.run(
            f"""
            MATCH (
              node:{quote_ident(ARTIFACT_LABEL)}
              {artifact_identity_map()}
            )
            WITH node, labels(node) AS node_labels
            RETURN
              count(node) AS total,
              sum(
                CASE
                  WHEN all(
                    label IN node_labels
                    WHERE label IN $allowed_phase_labels
                  )
                   AND any(
                    expected IN $expected_label_sets
                    WHERE size(node_labels) = size(expected)
                      AND all(
                        label IN expected
                        WHERE label IN node_labels
                      )
                  )
                  THEN 0 ELSE 1
                END
              ) AS invalid_labels
            """,
            allowed_phase_labels=allowed_phase_labels,
            expected_label_sets=expected_label_sets,
            **case_parameters(pair, run_id),
        ),
        f"validate artifact labels for {pair.case_id}",
    )
    total = int(record["total"])
    invalid = int(record["invalid_labels"])
    if total != expected_total or invalid:
        raise ExperimentValidationError(
            f"{pair.case_id}: {phase} run-scoped artifact labels are invalid; "
            f"total={total} (expected {expected_total}), "
            f"invalid label sets={invalid}"
        )


def validate_artifact_node_properties(
    session: Any,
    pair: PairSpec,
    run_id: str,
    *,
    phase: Literal["folded", "normalized"],
) -> None:
    identity_properties = (RUN_ID_PROPERTY, CASE_ID_PROPERTY)
    specifications: tuple[tuple[str, tuple[str, ...]], ...]
    if phase == "folded":
        specifications = (
            (
                pair.names.folded_label,
                (
                    *identity_properties,
                    *pair.folded_referencing_columns,
                    *(
                        binding.folded_name
                        for binding in pair.referenced_property_bindings
                    ),
                ),
            ),
        )
    else:
        specifications = (
            (
                pair.names.referencing_label,
                (*identity_properties, *pair.referencing.columns),
            ),
            (
                pair.names.referenced_label,
                (*identity_properties, *pair.referenced.columns),
            ),
        )

    for label, allowed_properties in specifications:
        record, _ = one_record(
            session.run(
                f"""
                MATCH (
                  node:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
                  {artifact_identity_map()}
                )
                WITH
                  node,
                  [
                    property IN keys(node)
                    WHERE NOT property IN $allowed_properties
                  ] AS unexpected_properties
                RETURN
                  count(node) AS total,
                  sum(
                    CASE
                      WHEN size(unexpected_properties) = 0 THEN 0 ELSE 1
                    END
                  ) AS nodes_with_unexpected_properties,
                  collect(DISTINCT unexpected_properties)
                    AS unexpected_property_sets
                """,
                allowed_properties=list(allowed_properties),
                **case_parameters(pair, run_id),
            ),
            f"validate {phase} properties for {pair.case_id}/{label}",
        )
        invalid = int(record["nodes_with_unexpected_properties"])
        if invalid:
            raise ExperimentValidationError(
                f"{pair.case_id}: {invalid} {phase} {label} node(s) contain "
                "properties outside the namespace contract: "
                f"{record['unexpected_property_sets']!r}"
            )


def normalized_owner_key_predicate(
    pair: PairSpec,
    boundary: BoundaryEdgeSpec,
    shadow_alias: str,
    original_alias: str,
) -> str:
    table = (
        pair.referencing
        if boundary.owner == "referencing"
        else pair.referenced
    )
    return strict_node_key_predicates(
        shadow_alias,
        table.key_fields,
        tuple(
            f"{original_alias}.{quote_ident(prop)}"
            for prop in table.key_fields
        ),
    )


def validate_boundary_copy(
    session: Any,
    pair: PairSpec,
    run_id: str,
    boundary: BoundaryEdgeSpec,
    *,
    phase: Literal["folded", "normalized"],
) -> None:
    owner_alias = (
        "referencing" if boundary.owner == "referencing" else "referenced"
    )
    owner_table = (
        pair.referencing
        if boundary.owner == "referencing"
        else pair.referenced
    )
    source_pattern = boundary_pattern(
        boundary,
        owner_alias,
        "external",
        "source_rel",
        copied=False,
        owner_label=owner_table.label,
    )
    if phase == "folded":
        shadow_label = pair.names.folded_label
        prefix = f"""
        MATCH {join_pattern(pair)}
        MATCH {source_pattern}
        MATCH (
          shadow:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(shadow_label)}
          {artifact_identity_map()}
        )
        USING INDEX shadow:{quote_ident(shadow_label)}(
          {
            ", ".join(
                quote_ident(prop)
                for prop in (
                    RUN_ID_PROPERTY,
                    *pair.folded_referencing_key_fields,
                )
            )
        }
        )
        WHERE {referencing_key_match_predicate(pair, "shadow", "referencing")}
        """
    else:
        shadow_label = (
            pair.names.referencing_label
            if boundary.owner == "referencing"
            else pair.names.referenced_label
        )
        prefix = f"""
        MATCH {source_pattern}
        MATCH (
          shadow:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(shadow_label)}
          {artifact_identity_map()}
        )
        WHERE {
            normalized_owner_key_predicate(
                pair,
                boundary,
                "shadow",
                owner_alias,
            )
        }
        """
    copy_pattern = boundary_pattern(
        boundary,
        "shadow",
        "external",
        "copy_rel",
        copied=True,
    )
    record, _ = one_record(
        session.run(
            f"""
            {prefix}
            CALL (source_rel, shadow, external) {{
              OPTIONAL MATCH {copy_pattern}
              RETURN
                count(copy_rel) AS copy_count,
                sum(
                  CASE
                    WHEN properties(copy_rel) = properties(source_rel)
                    THEN 1 ELSE 0
                  END
                ) AS matching_property_count
            }}
            RETURN
              count(*) AS expected,
              sum(CASE WHEN copy_count = 1 THEN 0 ELSE 1 END)
                AS invalid_multiplicity,
              sum(
                CASE
                  WHEN copy_count = 1
                   AND matching_property_count = 1
                  THEN 0 ELSE 1
                END
              ) AS property_mismatches
            """,
            **case_parameters(pair, run_id),
        ),
        f"validate {phase} boundary {boundary.boundary_id}",
    )
    invalid = int(record["invalid_multiplicity"])
    mismatches = int(record["property_mismatches"])
    if invalid or mismatches:
        raise ExperimentValidationError(
            f"{pair.case_id}: {phase} copy of {boundary.boundary_id} failed; "
            f"invalid multiplicity={invalid}, property mismatches={mismatches}"
        )


def validate_folded_shadow_state(
    session: Any,
    pair: PairSpec,
    run_id: str,
    baseline: CaseBaseline,
    topology_mode: str,
    *,
    deep: bool = True,
) -> ShadowState:
    state = inspect_shadow_state(session, pair, run_id)
    expected_boundaries = {
        expectation.boundary_id: expectation.folded_count
        for expectation in baseline.boundary_expectations
    }
    actual_boundaries = (
        state.boundary_relationships
        if topology_mode == "complete"
        else {
            key: value
            for key, value in state.boundary_relationships.items()
            if value
        }
    )
    if (
        state.phase != "folded"
        or state.folded_join_nodes != baseline.join_rows
        or (
            topology_mode == "complete"
            and actual_boundaries != expected_boundaries
        )
        or (
            topology_mode == "property-fd"
            and any(state.boundary_relationships.values())
        )
    ):
        raise ExperimentValidationError(
            f"{pair.case_id}: invalid folded shadow state: {state!r}; "
            f"expected boundaries={expected_boundaries!r}"
        )
    if not deep:
        return state
    validate_artifact_node_labels(
        session,
        pair,
        run_id,
        phase="folded",
        expected_total=baseline.join_rows,
    )
    validate_artifact_node_properties(
        session,
        pair,
        run_id,
        phase="folded",
    )
    expected_relationship_total = sum(expected_boundaries.values())
    actual_relationship_total = count_all_artifact_relationships(
        session,
        pair,
        run_id,
    )
    if actual_relationship_total != expected_relationship_total:
        raise ExperimentValidationError(
            f"{pair.case_id}: folded artifact relationships="
            f"{actual_relationship_total}, expected "
            f"{expected_relationship_total}"
        )
    fingerprint, fingerprint_rows = folded_join_sha256(
        session,
        pair,
        run_id,
    )
    if fingerprint_rows != baseline.join_rows:
        raise ExperimentValidationError(
            f"{pair.case_id}: folded fingerprint rows={fingerprint_rows}, "
            f"expected {baseline.join_rows}"
        )
    if fingerprint != baseline.join_sha256:
        raise ExperimentValidationError(
            f"{pair.case_id}: folded shadow values differ from source join"
        )
    validate_artifact_relationship_types(session, pair, run_id)
    if topology_mode == "complete":
        for boundary in pair.boundaries:
            validate_boundary_copy(
                session,
                pair,
                run_id,
                boundary,
                phase="folded",
            )
    return state


def validate_normalized_shadow_state(
    session: Any,
    pair: PairSpec,
    run_id: str,
    baseline: CaseBaseline,
    topology_mode: str,
    *,
    deep: bool = True,
) -> ShadowState:
    state = inspect_shadow_state(session, pair, run_id)
    expected_boundaries = {
        expectation.boundary_id: expectation.normalized_count
        for expectation in baseline.boundary_expectations
    }
    if (
        state.phase != "normalized"
        or state.normalized_referencing_nodes != baseline.join_rows
        or state.normalized_referenced_nodes
        != baseline.participating_referenced_rows
        or state.normalized_join_relationships != baseline.join_rows
        or (
            topology_mode == "complete"
            and state.boundary_relationships != expected_boundaries
        )
        or (
            topology_mode == "property-fd"
            and any(state.boundary_relationships.values())
        )
    ):
        raise ExperimentValidationError(
            f"{pair.case_id}: invalid normalized shadow state: {state!r}; "
            f"expected boundaries={expected_boundaries!r}"
        )
    if not deep:
        return state
    validate_artifact_node_labels(
        session,
        pair,
        run_id,
        phase="normalized",
        expected_total=(
            baseline.join_rows + baseline.participating_referenced_rows
        ),
    )
    validate_artifact_node_properties(
        session,
        pair,
        run_id,
        phase="normalized",
    )
    expected_relationship_total = baseline.join_rows + sum(
        expected_boundaries.values()
    )
    actual_relationship_total = count_all_artifact_relationships(
        session,
        pair,
        run_id,
    )
    if actual_relationship_total != expected_relationship_total:
        raise ExperimentValidationError(
            f"{pair.case_id}: normalized artifact relationships="
            f"{actual_relationship_total}, expected "
            f"{expected_relationship_total}"
        )
    fingerprint, fingerprint_rows = normalized_join_sha256(
        session,
        pair,
        run_id,
    )
    if fingerprint_rows != baseline.join_rows:
        raise ExperimentValidationError(
            f"{pair.case_id}: normalized fingerprint rows={fingerprint_rows}, "
            f"expected {baseline.join_rows}"
        )
    if fingerprint != baseline.join_sha256:
        raise ExperimentValidationError(
            f"{pair.case_id}: normalized shadow values differ from source join"
        )
    validate_artifact_relationship_types(session, pair, run_id)
    if topology_mode == "complete":
        for boundary in pair.boundaries:
            validate_boundary_copy(
                session,
                pair,
                run_id,
                boundary,
                phase="normalized",
            )
    return state


def remove_properties_clause(
    properties: Sequence[str],
    alias: str,
) -> str:
    return ", ".join(
        f"{alias}.{quote_ident(property_name)}"
        for property_name in properties
    )


def remove_folded_referenced_properties_clause(
    pair: PairSpec,
    alias: str,
) -> str:
    return remove_properties_clause(
        tuple(
            binding.folded_name
            for binding in pair.referenced_property_bindings
        ),
        alias,
    )


def totals_timing_sample(
    run: int,
    wall_ms: float,
    totals: MutationTotals,
    affected_nodes: int,
) -> TimingSample:
    return TimingSample(
        run=run,
        client_wall_ms=wall_ms,
        available_after_ms=totals.available_after_ms,
        consumed_after_ms=totals.consumed_after_ms,
        neo4j_reported_total_ms=(
            totals.available_after_ms + totals.consumed_after_ms
        ),
        affected_nodes=affected_nodes,
        properties_set=totals.properties_set,
        nodes_created=totals.nodes_created,
        nodes_deleted=totals.nodes_deleted,
        relationships_created=totals.relationships_created,
        relationships_deleted=totals.relationships_deleted,
        labels_added=totals.labels_added,
        labels_removed=totals.labels_removed,
    )


def normalize_once(
    session: Any,
    pair: PairSpec,
    run_id: str,
    baseline: CaseBaseline,
    topology_mode: str,
    run: int,
    batch_size: int,
) -> TimingSample:
    parameters = case_parameters(pair, run_id)
    referenced_merge_identity = cypher_map(
        (
            (RUN_ID_PROPERTY, "$run_id"),
            *tuple(
                (property_name, expression)
                for property_name, expression in zip(
                    pair.referenced.key_fields,
                    folded_referenced_key_expressions(pair, "folded"),
                    strict=True,
                )
            ),
        )
    )
    totals = MutationTotals()
    started_ns = perf_counter_ns()

    referenced_result = session.run(
        f"""
        MATCH (
          folded:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(pair.names.folded_label)}
          {artifact_identity_map()}
        )
        CALL (folded) {{
          MERGE (
            referenced:{quote_ident(ARTIFACT_LABEL)}
                      :{quote_ident(pair.names.referenced_label)}
            {referenced_merge_identity}
          )
          ON CREATE SET referenced +=
            {referenced_property_map_from_folded(pair, "folded")},
            referenced.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
            referenced.{quote_ident(CASE_ID_PROPERTY)} = $case_id
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    summary = referenced_result.consume()
    totals.add_summary(summary)
    referenced_count = int(summary.counters.nodes_created)

    referencing_result = session.run(
        f"""
        MATCH (
          referencing:{quote_ident(ARTIFACT_LABEL)}
                 :{quote_ident(pair.names.folded_label)}
          {artifact_identity_map()}
        )
        CALL (referencing) {{
          SET referencing:{quote_ident(pair.names.referencing_label)}
          REMOVE {
              remove_folded_referenced_properties_clause(
                  pair,
                  "referencing",
              )
          }
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    summary = referencing_result.consume()
    totals.add_summary(summary)
    referencing_count = int(summary.counters.labels_added)

    normalized_create_pattern = normalized_join_pattern(
        pair,
        relationship_alias="normalized_rel",
        include_labels=False,
    )
    join_result = session.run(
        f"""
        MATCH (
          referencing:{quote_ident(ARTIFACT_LABEL)}
                 :{quote_ident(pair.names.referencing_label)}
          {artifact_identity_map()}
        )
        MATCH (
          referenced:{quote_ident(ARTIFACT_LABEL)}
                   :{quote_ident(pair.names.referenced_label)}
          {artifact_identity_map()}
        )
        USING INDEX referenced:{quote_ident(pair.names.referenced_label)}(
          {
            ", ".join(
                quote_ident(prop)
                for prop in (RUN_ID_PROPERTY, *pair.referenced.key_fields)
            )
        }
        )
        WHERE {
            strict_node_key_predicates(
                "referenced",
                pair.referenced.key_fields,
                normalized_referenced_key_expressions(
                    pair,
                    "referencing",
                ),
            )
        }
        CALL (referencing, referenced) {{
          CREATE {normalized_create_pattern}
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    summary = join_result.consume()
    totals.add_summary(summary)
    normalized_join_count = int(summary.counters.relationships_created)

    boundary_results: dict[str, dict[str, int]] = {}
    if topology_mode == "complete":
        for boundary in pair.boundaries:
            if boundary.owner != "referenced":
                continue
            normalized_boundary_join = normalized_join_pattern(pair)
            old_pattern = boundary_pattern(
                boundary,
                "referencing",
                "external",
                "old_copy",
                copied=True,
            )
            create_pattern = boundary_pattern(
                boundary,
                "referenced",
                "external",
                "new_copy",
                copied=True,
                include_other_label=False,
            )
            create_result = session.run(
                f"""
                MATCH {normalized_boundary_join}
                WHERE referencing.{quote_ident(RUN_ID_PROPERTY)} = $run_id
                  AND referencing.{quote_ident(CASE_ID_PROPERTY)} = $case_id
                  AND referenced.{quote_ident(RUN_ID_PROPERTY)} = $run_id
                  AND referenced.{quote_ident(CASE_ID_PROPERTY)} = $case_id
                MATCH {old_pattern}
                CALL (referenced, external, old_copy) {{
                  MERGE {create_pattern}
                  ON CREATE SET new_copy = properties(old_copy)
                }} IN TRANSACTIONS OF {batch_size} ROWS
                """,
                parameters,
            )
            summary = create_result.consume()
            totals.add_summary(summary)
            created_relationships = int(
                summary.counters.relationships_created
            )

            delete_result = session.run(
                f"""
                MATCH (
                  referencing:{quote_ident(ARTIFACT_LABEL)}
                         :{quote_ident(pair.names.referencing_label)}
                  {artifact_identity_map()}
                )
                MATCH {old_pattern}
                CALL (old_copy) {{
                  DELETE old_copy
                }} IN TRANSACTIONS OF {batch_size} ROWS
                """,
                parameters,
            )
            summary = delete_result.consume()
            totals.add_summary(summary)
            boundary_results[boundary.boundary_id] = {
                "created": created_relationships,
                "deleted": int(summary.counters.relationships_deleted),
            }

    label_result = session.run(
        f"""
        MATCH (
          referencing:{quote_ident(ARTIFACT_LABEL)}
                 :{quote_ident(pair.names.folded_label)}
                 :{quote_ident(pair.names.referencing_label)}
          {artifact_identity_map()}
        )
        CALL (referencing) {{
          REMOVE referencing:{quote_ident(pair.names.folded_label)}
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    summary = label_result.consume()
    totals.add_summary(summary)
    removed_folded_labels = int(summary.counters.labels_removed)

    wall_ms = (perf_counter_ns() - started_ns) / 1_000_000
    problems: list[str] = []
    if referenced_count != baseline.participating_referenced_rows:
        problems.append(
            f"created referenced nodes={referenced_count}, "
            f"expected {baseline.participating_referenced_rows}"
        )
    if referencing_count != baseline.join_rows:
        problems.append(
            f"referencing nodes={referencing_count}, expected {baseline.join_rows}"
        )
    if normalized_join_count != baseline.join_rows:
        problems.append(
            f"join edges={normalized_join_count}, expected {baseline.join_rows}"
        )
    if removed_folded_labels != baseline.join_rows:
        problems.append(
            f"removed folded labels={removed_folded_labels}, "
            f"expected {baseline.join_rows}"
        )
    expectation_by_id = {
        item.boundary_id: item for item in baseline.boundary_expectations
    }
    for boundary_id, counts in boundary_results.items():
        expectation = expectation_by_id[boundary_id]
        if counts["created"] != expectation.normalized_count:
            problems.append(
                f"{boundary_id} created={counts['created']}, "
                f"expected {expectation.normalized_count}"
            )
        if counts["deleted"] != expectation.folded_count:
            problems.append(
                f"{boundary_id} deleted={counts['deleted']}, "
                f"expected {expectation.folded_count}"
            )
    if problems:
        raise ExperimentValidationError(
            f"{pair.case_id} normalization counters failed: "
            + "; ".join(problems)
        )

    sample = totals_timing_sample(
        run,
        wall_ms,
        totals,
        affected_nodes=baseline.join_rows,
    )
    validate_normalized_shadow_state(
        session,
        pair,
        run_id,
        baseline,
        topology_mode,
        deep=False,
    )
    return sample


def refold_shadow_projection(
    session: Any,
    pair: PairSpec,
    run_id: str,
    baseline: CaseBaseline,
    topology_mode: str,
    batch_size: int,
) -> None:
    state = inspect_shadow_state(session, pair, run_id)
    if state.phase != "normalized":
        raise ExperimentStateError(
            f"{pair.case_id}: cannot refold phase {state.phase!r}"
        )
    parameters = case_parameters(pair, run_id)
    boundary_results: dict[str, dict[str, int]] = {}
    if topology_mode == "complete":
        for boundary in pair.boundaries:
            if boundary.owner != "referenced":
                continue
            normalized_pattern = normalized_join_pattern(pair)
            referenced_copy_pattern = boundary_pattern(
                boundary,
                "referenced",
                "external",
                "referenced_copy",
                copied=True,
            )
            referencing_copy_pattern = boundary_pattern(
                boundary,
                "referencing",
                "external",
                "referencing_copy",
                copied=True,
                include_other_label=False,
            )
            create_result = session.run(
                f"""
                MATCH {normalized_pattern}
                WHERE referencing.{quote_ident(RUN_ID_PROPERTY)} = $run_id
                  AND referencing.{quote_ident(CASE_ID_PROPERTY)} = $case_id
                  AND referenced.{quote_ident(RUN_ID_PROPERTY)} = $run_id
                  AND referenced.{quote_ident(CASE_ID_PROPERTY)} = $case_id
                MATCH {referenced_copy_pattern}
                CALL (referencing, external, referenced_copy) {{
                  CREATE {referencing_copy_pattern}
                  SET referencing_copy = properties(referenced_copy)
                }} IN TRANSACTIONS OF {batch_size} ROWS
                """,
                parameters,
            )
            create_summary = create_result.consume()
            delete_result = session.run(
                f"""
                MATCH (
                  referenced:{quote_ident(ARTIFACT_LABEL)}
                           :{quote_ident(pair.names.referenced_label)}
                  {artifact_identity_map()}
                )
                MATCH {referenced_copy_pattern}
                CALL (referenced_copy) {{
                  DELETE referenced_copy
                }} IN TRANSACTIONS OF {batch_size} ROWS
                """,
                parameters,
            )
            delete_summary = delete_result.consume()
            boundary_results[boundary.boundary_id] = {
                "created": int(
                    create_summary.counters.relationships_created
                ),
                "deleted": int(
                    delete_summary.counters.relationships_deleted
                ),
            }

    normalized_pattern = normalized_join_pattern(pair)
    referencing_result = session.run(
        f"""
        MATCH {normalized_pattern}
        WHERE referencing.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND referencing.{quote_ident(CASE_ID_PROPERTY)} = $case_id
          AND referenced.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND referenced.{quote_ident(CASE_ID_PROPERTY)} = $case_id
        CALL (referencing, referenced) {{
          SET referencing +=
            {folded_referenced_property_map(pair, "referenced")}
          SET referencing:{quote_ident(pair.names.folded_label)}
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    referencing_summary = referencing_result.consume()
    referencing_count = int(referencing_summary.counters.labels_added)

    join_result = session.run(
        f"""
        MATCH {normalized_pattern}
        WHERE referencing.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND referencing.{quote_ident(CASE_ID_PROPERTY)} = $case_id
          AND referenced.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND referenced.{quote_ident(CASE_ID_PROPERTY)} = $case_id
        CALL (join_rel) {{
          DELETE join_rel
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    join_summary = join_result.consume()
    deleted_joins = int(join_summary.counters.relationships_deleted)

    referenced_result = session.run(
        f"""
        MATCH (
          referenced:{quote_ident(ARTIFACT_LABEL)}
                   :{quote_ident(pair.names.referenced_label)}
          {artifact_identity_map()}
        )
        CALL (referenced) {{
          DELETE referenced
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    referenced_summary = referenced_result.consume()
    deleted_referenced_nodes = int(referenced_summary.counters.nodes_deleted)

    label_result = session.run(
        f"""
        MATCH (
          referencing:{quote_ident(ARTIFACT_LABEL)}
                 :{quote_ident(pair.names.folded_label)}
                 :{quote_ident(pair.names.referencing_label)}
          {artifact_identity_map()}
        )
        CALL (referencing) {{
          REMOVE referencing:{quote_ident(pair.names.referencing_label)}
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    label_summary = label_result.consume()
    removed_referencing_labels = int(label_summary.counters.labels_removed)

    problems: list[str] = []
    expected_simple = {
        "referencing_count": baseline.join_rows,
        "deleted_joins": baseline.join_rows,
        "deleted_referenced_nodes": baseline.participating_referenced_rows,
        "removed_referencing_labels": baseline.join_rows,
    }
    actual_simple = {
        "referencing_count": referencing_count,
        "deleted_joins": deleted_joins,
        "deleted_referenced_nodes": deleted_referenced_nodes,
        "removed_referencing_labels": removed_referencing_labels,
    }
    if actual_simple != expected_simple:
        problems.append(f"counts={actual_simple!r}, expected={expected_simple!r}")
    expectation_by_id = {
        item.boundary_id: item for item in baseline.boundary_expectations
    }
    for boundary_id, counts in boundary_results.items():
        expectation = expectation_by_id[boundary_id]
        if counts["created"] != expectation.folded_count:
            problems.append(
                f"{boundary_id} recreated={counts['created']}, expected "
                f"{expectation.folded_count}"
            )
        if counts["deleted"] != expectation.normalized_count:
            problems.append(
                f"{boundary_id} normalized deleted={counts['deleted']}, expected "
                f"{expectation.normalized_count}"
            )
    if problems:
        raise ExperimentValidationError(
            f"{pair.case_id} refold counters failed: " + "; ".join(problems)
        )
    validate_folded_shadow_state(
        session,
        pair,
        run_id,
        baseline,
        topology_mode,
        deep=False,
    )


def cleanup_shadow_artifacts(
    session: Any,
    pair: PairSpec,
    run_id: str,
    batch_size: int,
) -> CleanupResult:
    # Failure cleanup can run before the first shadow node was created.  In
    # that case there is nothing to remove, and static label references would
    # only emit Neo4j notification 01N50.
    if ARTIFACT_LABEL not in database_node_labels(session):
        return CleanupResult(deleted_nodes=0, deleted_relationships=0)

    parameters = case_parameters(pair, run_id)
    unexpected_result = session.run(
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )-[relationship]-()
        WHERE NOT type(relationship) IN $allowed_types
        RETURN type(relationship) AS relationship_type
        LIMIT 1
        """,
        parameters | {"allowed_types": list(pair.all_copy_relationship_types)},
    )
    unexpected_record = unexpected_result.single()
    unexpected_result.consume()
    if unexpected_record is not None:
        raise ExperimentStateError(
            f"{pair.case_id}: cleanup refused to delete unexpected "
            f"{unexpected_record['relationship_type']!r} relationship(s) "
            "attached to run-scoped nodes"
        )

    expected_nodes = scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )
        RETURN count(node) AS count
        """,
        "count",
        f"count cleanup nodes for {pair.case_id}",
        **parameters,
    )
    deleted_nodes = 0
    deleted_relationships = 0
    while deleted_nodes < expected_nodes:
        record, summary = one_record(
            session.run(
                f"""
                MATCH (
                  node:{quote_ident(ARTIFACT_LABEL)}
                  {artifact_identity_map()}
                )
                WITH node
                LIMIT $cleanup_batch_size
                DETACH DELETE node
                RETURN count(*) AS selected_nodes
                """,
                parameters | {"cleanup_batch_size": batch_size},
            ),
            f"delete cleanup batch for {pair.case_id}",
        )
        selected_nodes = int(record["selected_nodes"])
        batch_nodes = int(summary.counters.nodes_deleted)
        if selected_nodes <= 0:
            raise ExperimentValidationError(
                f"{pair.case_id}: cleanup stopped before deleting all "
                f"{expected_nodes} scoped node(s)"
            )
        if batch_nodes != selected_nodes:
            raise ExperimentValidationError(
                f"{pair.case_id}: cleanup selected {selected_nodes} node(s) "
                f"but Neo4j deleted {batch_nodes}"
            )
        deleted_nodes += batch_nodes
        deleted_relationships += int(
            summary.counters.relationships_deleted
        )
    if deleted_nodes != expected_nodes:
        raise ExperimentValidationError(
            f"{pair.case_id}: cleanup deleted {deleted_nodes} node(s), "
            f"expected {expected_nodes}"
        )

    remaining = inspect_shadow_state(session, pair, run_id)
    remaining_artifacts = scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )
        RETURN count(node) AS count
        """,
        "count",
        f"verify cleanup for {pair.case_id}",
        **parameters,
    )
    if remaining.phase != "clean" or remaining_artifacts:
        raise ExperimentValidationError(
            f"{pair.case_id}: cleanup left shadow artifacts: "
            f"state={remaining!r}, all_artifacts={remaining_artifacts}"
        )
    return CleanupResult(
        deleted_nodes=deleted_nodes,
        deleted_relationships=deleted_relationships,
    )


def build_case_measurements(
    session: Any,
    pair: PairSpec,
    baseline: CaseBaseline,
    property_name: str,
    batch_size: int,
) -> tuple[dict[str, Any], UpdateWorkload, dict[str, Any]]:
    if property_name not in pair.allowed_update_properties:
        raise ExperimentValidationError(
            f"{pair.case_id}: {property_name!r} is not an eligible update "
            "property. Choose a referenced-table non-key/non-topology "
            f"property from {list(pair.allowed_update_properties)!r}"
        )

    descriptor, spool_name = tempfile.mkstemp(
        prefix=f"tpch_fd_{pair.case_id}_",
        suffix=".sqlite3",
    )
    os.close(descriptor)
    spool_path = Path(spool_name)
    connection = sqlite3.connect(spool_path)
    try:
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.executescript(
            """
            CREATE TABLE domain (
              token TEXT PRIMARY KEY,
              value_blob BLOB NOT NULL,
              payload_bytes INTEGER NOT NULL,
              successor_token TEXT
            ) WITHOUT ROWID;
            CREATE TABLE logical_update (
              key_token TEXT PRIMARY KEY,
              key_blob BLOB NOT NULL,
              original_token TEXT NOT NULL REFERENCES domain(token),
              fanout INTEGER NOT NULL CHECK (fanout > 0)
            ) WITHOUT ROWID;
            CREATE INDEX logical_update_original
              ON logical_update(original_token);
            """
        )

        domain_result = session.run(
            f"""
            MATCH (referenced:{quote_ident(pair.referenced.label)})
            RETURN referenced.{quote_ident(property_name)} AS property_value
            """
        )
        domain_source_count = 0
        null_domain_count = 0
        value_types: set[str] = set()
        domain_batch: list[tuple[str, bytes, int]] = []
        for record in domain_result:
            value = record["property_value"]
            domain_source_count += 1
            if value is None:
                null_domain_count += 1
                continue
            value_types.add(
                f"{type(value).__module__}.{type(value).__qualname__}"
            )
            domain_batch.append(
                (
                    domain_value_token(value),
                    pickle.dumps(value, protocol=5),
                    value_payload_bytes(value),
                )
            )
            if len(domain_batch) == batch_size:
                connection.executemany(
                    "INSERT OR IGNORE INTO domain VALUES (?, ?, ?, NULL)",
                    domain_batch,
                )
                domain_batch = []
        domain_result.consume()
        if domain_batch:
            connection.executemany(
                "INSERT OR IGNORE INTO domain VALUES (?, ?, ?, NULL)",
                domain_batch,
            )
        connection.commit()
        if domain_source_count != baseline.referenced_rows:
            raise ExperimentValidationError(
                f"{pair.case_id}: active-domain scan read "
                f"{domain_source_count} rows, expected "
                f"{baseline.referenced_rows}"
            )
        if null_domain_count:
            raise ExperimentValidationError(
                f"{pair.case_id}: {property_name} contains NULL in "
                f"{null_domain_count} of {domain_source_count} full "
                "referenced rows; choose another property or define a "
                "separate NULL workload"
            )
        if len(value_types) != 1:
            raise ExperimentValidationError(
                f"{pair.case_id}: {property_name} has mixed active-domain "
                f"types: {sorted(value_types)!r}"
            )
        value_type = next(iter(value_types))
        active_domain_size = int(
            connection.execute("SELECT count(*) FROM domain").fetchone()[0]
        )
        if active_domain_size < 2:
            raise ExperimentValidationError(
                f"{pair.case_id}: {property_name} active domain has only "
                f"{active_domain_size} distinct value(s); at least two are "
                "needed"
            )

        active_domain_hasher = hashlib.sha256()
        active_domain_hasher.update(b"[")
        first_token: str | None = None
        previous_token: str | None = None
        successor_batch: list[tuple[str, str]] = []
        for (token,) in connection.execute(
            "SELECT token FROM domain ORDER BY token"
        ):
            if first_token is None:
                first_token = str(token)
            if previous_token is not None:
                successor_batch.append((str(token), previous_token))
            if previous_token is not None:
                active_domain_hasher.update(b",")
            active_domain_hasher.update(
                json.dumps(
                    str(token),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            previous_token = str(token)
            if len(successor_batch) == batch_size:
                connection.executemany(
                    "UPDATE domain SET successor_token=? WHERE token=?",
                    successor_batch,
                )
                successor_batch = []
        assert first_token is not None and previous_token is not None
        successor_batch.append((first_token, previous_token))
        if successor_batch:
            connection.executemany(
                "UPDATE domain SET successor_token=? WHERE token=?",
                successor_batch,
            )
        connection.commit()
        active_domain_hasher.update(b"]")
        active_domain_sha256 = active_domain_hasher.hexdigest()

        grouped_result = session.run(
            f"""
            MATCH (referenced:{quote_ident(pair.referenced.label)})
            CALL (referenced) {{
              MATCH {join_pattern(pair)}
              RETURN count(join_rel) AS fanout
            }}
            WITH referenced, fanout
            WHERE fanout > 0
            RETURN
              {node_projection("referenced", pair.referenced.columns)}
                AS referenced_values,
              fanout
            """
        )
        occurrences = 0
        distinct_referenced_rows = 0
        dependent_count = len(pair.dependent_properties)
        redundant_non_null_values = 0
        total_non_null_values = 0
        redundant_payload_bytes = 0
        total_payload_bytes = 0
        fanout_histogram: Counter[int] = Counter()
        workload_batch: list[tuple[str, bytes, str, int]] = []

        for record in grouped_result:
            values = dict(record["referenced_values"])
            fanout = int(record["fanout"])
            key = canonical_key(
                values[prop] for prop in pair.referenced.key_fields
            )
            original = values[property_name]
            if original is None:
                raise ExperimentValidationError(
                    f"{pair.case_id}: {property_name} is NULL for "
                    f"participating referenced key {key!r}"
                )
            original_token = domain_value_token(original)
            key_token = json.dumps(
                [domain_value_token(value) for value in key],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            workload_batch.append(
                (
                    key_token,
                    pickle.dumps(key, protocol=5),
                    original_token,
                    fanout,
                )
            )
            if len(workload_batch) == batch_size:
                try:
                    connection.executemany(
                        "INSERT INTO logical_update VALUES (?, ?, ?, ?)",
                        workload_batch,
                    )
                except sqlite3.IntegrityError as exc:
                    raise ExperimentValidationError(
                        f"{pair.case_id}: duplicate key or missing active "
                        "domain value while building update workload"
                    ) from exc
                workload_batch = []

            occurrences += fanout
            distinct_referenced_rows += 1
            fanout_histogram[fanout] += 1
            for prop in pair.dependent_properties:
                value = values[prop]
                if value is None:
                    continue
                payload = value_payload_bytes(value)
                total_non_null_values += fanout
                total_payload_bytes += fanout * payload
                redundant_non_null_values += fanout - 1
                redundant_payload_bytes += (fanout - 1) * payload
        grouped_result.consume()
        if workload_batch:
            try:
                connection.executemany(
                    "INSERT INTO logical_update VALUES (?, ?, ?, ?)",
                    workload_batch,
                )
            except sqlite3.IntegrityError as exc:
                raise ExperimentValidationError(
                    f"{pair.case_id}: duplicate key or missing active "
                    "domain value while building update workload"
                ) from exc
        connection.commit()

        payload_row = connection.execute(
            """
            SELECT
              count(*),
              coalesce(sum(original.payload_bytes), 0),
              coalesce(sum(successor.payload_bytes), 0),
              coalesce(sum(workload.fanout * original.payload_bytes), 0),
              coalesce(sum(workload.fanout * successor.payload_bytes), 0),
              coalesce(sum(
                CASE
                  WHEN original.token = successor.token THEN 1
                  ELSE 0
                END
              ), 0)
            FROM logical_update AS workload
            JOIN domain AS original
              ON original.token = workload.original_token
            JOIN domain AS successor
              ON successor.token = original.successor_token
            """
        ).fetchone()
        logical_tuple_count = int(payload_row[0])
        logical_original_payload_bytes = int(payload_row[1])
        logical_updated_payload_bytes = int(payload_row[2])
        folded_original_payload_bytes = int(payload_row[3])
        folded_updated_payload_bytes = int(payload_row[4])
        unchanged_update_count = int(payload_row[5])
        if unchanged_update_count:
            raise ExperimentValidationError(
                f"{pair.case_id}: active-domain rotation left "
                f"{unchanged_update_count} logical update(s) unchanged"
            )

        mapping_count = 0
        mapping_sum = 0
        mapping_xor = 0
        mapping_modulus = 1 << 256
        for key_token, original_token, updated_token in connection.execute(
            """
            SELECT
              workload.key_token,
              workload.original_token,
              original.successor_token
            FROM logical_update AS workload
            JOIN domain AS original
              ON original.token = workload.original_token
            ORDER BY workload.key_token
            """
        ):
            mapping_payload = {
                "key": json.loads(key_token),
                "original": original_token,
                "updated": updated_token,
            }
            mapping_digest = int.from_bytes(
                hashlib.sha256(
                    json.dumps(
                        mapping_payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).digest(),
                "big",
            )
            mapping_count += 1
            mapping_sum = (mapping_sum + mapping_digest) % mapping_modulus
            mapping_xor ^= mapping_digest
    except BaseException:
        connection.close()
        spool_path.unlink(missing_ok=True)
        raise

    if occurrences != baseline.join_rows:
        connection.close()
        spool_path.unlink(missing_ok=True)
        raise ExperimentValidationError(
            f"{pair.case_id}: calculated J={occurrences}, dynamic baseline "
            f"expected {baseline.join_rows}"
        )
    if distinct_referenced_rows != baseline.participating_referenced_rows:
        connection.close()
        spool_path.unlink(missing_ok=True)
        raise ExperimentValidationError(
            f"{pair.case_id}: calculated D={distinct_referenced_rows}, "
            "dynamic baseline expected "
            f"{baseline.participating_referenced_rows}"
        )
    if logical_tuple_count != distinct_referenced_rows:
        connection.close()
        spool_path.unlink(missing_ok=True)
        raise ExperimentValidationError(
            f"{pair.case_id}: update spool contains {logical_tuple_count} "
            f"logical rows, expected {distinct_referenced_rows}"
        )
    redundant_rows = occurrences - distinct_referenced_rows
    theoretical_slots = redundant_rows * dependent_count

    expected_redundant_rows = (
        baseline.join_rows - baseline.participating_referenced_rows
    )
    if redundant_rows != expected_redundant_rows:
        connection.close()
        spool_path.unlink(missing_ok=True)
        raise ExperimentValidationError(
            f"{pair.case_id}: redundant rows={redundant_rows}, "
            f"dynamic baseline expected {expected_redundant_rows}"
        )
    expected_slots = expected_redundant_rows * dependent_count
    if theoretical_slots != expected_slots:
        connection.close()
        spool_path.unlink(missing_ok=True)
        raise ExperimentValidationError(
            f"{pair.case_id}: redundant slots={theoretical_slots}, "
            f"dynamic baseline expected {expected_slots}"
        )

    boundary_folded = sum(
        item.folded_count for item in baseline.boundary_expectations
    )
    boundary_normalized = sum(
        item.normalized_count for item in baseline.boundary_expectations
    )
    metrics: dict[str, Any] = {
        "definition": {
            "referencing_table": pair.referencing.label,
            "referenced_table": pair.referenced.label,
            "join_tuple_count_J": occurrences,
            "participating_distinct_referenced_keys_D": (
                distinct_referenced_rows
            ),
            "full_referenced_relation_rows": baseline.referenced_rows,
            "redundant_referenced_tuple_copies": "J - D",
            "redundant_referenced_property_slots": (
                "(J - D) * (column_count(referenced) - key_length(referenced))"
            ),
        },
        "join_rows_J": occurrences,
        "full_referenced_rows": baseline.referenced_rows,
        "participating_referenced_rows_D": distinct_referenced_rows,
        "unused_referenced_rows": (
            baseline.referenced_rows - distinct_referenced_rows
        ),
        "redundant_referenced_tuple_copies": redundant_rows,
        "redundant_referenced_tuple_percentage_of_join": (
            100.0 * redundant_rows / occurrences if occurrences else 0.0
        ),
        "referenced_column_count": len(pair.referenced.columns),
        "referenced_key_length": len(pair.referenced.key_fields),
        "dependent_property_count": dependent_count,
        "theoretical_redundant_dependent_property_slots": theoretical_slots,
        "redundant_non_null_dependent_values": redundant_non_null_values,
        "total_non_null_dependent_values_in_folded_join": (
            total_non_null_values
        ),
        "redundant_non_null_value_percentage": (
            100.0 * redundant_non_null_values / total_non_null_values
            if total_non_null_values
            else 0.0
        ),
        "redundant_value_payload_bytes": redundant_payload_bytes,
        "total_value_payload_bytes_in_folded_join": total_payload_bytes,
        "redundant_value_payload_percentage": (
            100.0 * redundant_payload_bytes / total_payload_bytes
            if total_payload_bytes
            else 0.0
        ),
        "value_payload_metric": (
            "UTF-8 bytes for strings; UTF-8 bytes of str(value) for other "
            "non-NULL values; this is not Neo4j on-disk storage"
        ),
        "referenced_fanout_histogram": {
            str(fanout): count
            for fanout, count in sorted(fanout_histogram.items())
        },
        "topology": {
            "folded_boundary_relationships": boundary_folded,
            "normalized_boundary_relationships": boundary_normalized,
            "redundant_folded_boundary_relationship_copies": (
                boundary_folded - boundary_normalized
            ),
            "by_boundary": [
                asdict(expectation)
                | {
                    "redundant_folded_copies": (
                        expectation.redundant_folded_copies
                    )
                }
                for expectation in baseline.boundary_expectations
            ],
        },
    }
    workload = UpdateWorkload(
        property_name=property_name,
        folded_property_name=(
            pair.referenced_binding_by_logical_name[property_name]
        ),
        logical_tuple_count=logical_tuple_count,
        active_domain_size=active_domain_size,
        active_domain_source_tuple_count=domain_source_count,
        active_domain_sha256=active_domain_sha256,
        mapping_sha256=sha256_json(
            {
                "algorithm": "commutative_update_mapping_sha256_sum_xor_v1",
                "count": mapping_count,
                "sum_sha256": f"{mapping_sum:064x}",
                "xor_sha256": f"{mapping_xor:064x}",
            }
        ),
        value_type=value_type,
        logical_original_payload_bytes=logical_original_payload_bytes,
        logical_updated_payload_bytes=logical_updated_payload_bytes,
        _database_path=spool_path,
        _connection=connection,
    )
    payload_metrics = {
        "metric": (
            "UTF-8 bytes for strings and UTF-8 bytes of str(value) for other "
            "types; this is a value-payload proxy, not Neo4j store bytes"
        ),
        "normalized_logical": {
            "property_writes": workload.logical_tuple_count,
            "original_payload_bytes": logical_original_payload_bytes,
            "updated_payload_bytes": logical_updated_payload_bytes,
            "payload_delta_bytes": workload.logical_payload_delta_bytes,
        },
        "folded_join_fanout_weighted": {
            "property_writes": occurrences,
            "original_payload_bytes": folded_original_payload_bytes,
            "updated_payload_bytes": folded_updated_payload_bytes,
            "payload_delta_bytes": (
                folded_updated_payload_bytes - folded_original_payload_bytes
            ),
        },
    }
    return metrics, workload, payload_metrics


def print_redundancy(pair: PairSpec, metrics: dict[str, Any]) -> None:
    histogram = ", ".join(
        f"{fanout} row(s) -> {count} referenced key(s)"
        for fanout, count in metrics["referenced_fanout_histogram"].items()
    )
    print(
        f"  Join tuples J: {metrics['join_rows_J']:,}; "
        "distinct participating referenced keys D: "
        f"{metrics['participating_referenced_rows_D']:,}; "
        "full referenced relation: "
        f"{metrics['full_referenced_rows']:,} row(s)"
    )
    print(
        "  Redundant referenced tuple copies (J-D): "
        f"{metrics['redundant_referenced_tuple_copies']:,} "
        f"({metrics['redundant_referenced_tuple_percentage_of_join']:.3f}%)"
    )
    print(
        "  Redundant property slots "
        "((J-D) * "
        "(columns(referenced)-key_length(referenced))): "
        f"{metrics['theoretical_redundant_dependent_property_slots']:,}"
    )
    print(
        "  Redundant non-NULL values: "
        f"{metrics['redundant_non_null_dependent_values']:,}; "
        "redundant value payload: "
        f"{metrics['redundant_value_payload_bytes']:,} bytes"
    )
    if metrics["topology"]["folded_boundary_relationships"]:
        print(
            "  Boundary relationships: folded="
            f"{metrics['topology']['folded_boundary_relationships']:,}, "
            "normalized="
            f"{metrics['topology']['normalized_boundary_relationships']:,}"
        )
    print(f"  Referenced-key fanout histogram: {histogram}")


def update_query(
    pair: PairSpec,
    *,
    phase: Literal["folded", "normalized"],
    property_name: str,
    batch_size: int,
) -> str:
    if phase == "folded":
        label = pair.names.folded_label
        key_properties = pair.folded_referencing_fk_fields
        physical_property = pair.referenced_binding_by_logical_name[
            property_name
        ]
    else:
        label = pair.names.referenced_label
        key_properties = pair.referenced.key_fields
        physical_property = property_name
    key_predicate = " AND ".join(
        (f"node.{quote_ident(prop)} = update.key[{index}]")
        for index, prop in enumerate(key_properties)
    )
    index_properties = ", ".join(
        quote_ident(prop) for prop in (RUN_ID_PROPERTY, *key_properties)
    )
    return f"""
    UNWIND $updates AS update
    MATCH (
      node:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
      {artifact_identity_map()}
    )
    USING INDEX node:{quote_ident(label)}({index_properties})
    WHERE {key_predicate}
    CALL (node, update) {{
      SET node.{quote_ident(physical_property)} = update.value
    }} IN TRANSACTIONS OF {batch_size} ROWS
    RETURN count(node) AS updated_nodes
    """


def verify_current_update_values(
    session: Any,
    pair: PairSpec,
    run_id: str,
    *,
    phase: Literal["folded", "normalized"],
    workload: UpdateWorkload,
    updated: bool,
    expected_nodes: int,
    context: str,
    batch_size: int,
) -> None:
    if phase == "folded":
        label = pair.names.folded_label
        key_properties = pair.folded_referencing_fk_fields
        physical_property = workload.folded_property_name
    else:
        label = pair.names.referenced_label
        key_properties = pair.referenced.key_fields
        physical_property = workload.property_name
    key_predicate = " AND ".join(
        f"node.{quote_ident(prop)} = expected.key[{index}]"
        for index, prop in enumerate(key_properties)
    )
    index_properties = ", ".join(
        quote_ident(prop) for prop in (RUN_ID_PROPERTY, *key_properties)
    )
    found_nodes = 0
    found_logical_keys = 0
    invalid_keys = 0
    for expected_values in workload.parameter_batches(
        batch_size,
        restore=not updated,
    ):
        for expected in expected_values:
            expected["expected_nodes"] = (
                expected["expected_fanout"] if phase == "folded" else 1
            )
        record, _ = one_record(
            session.run(
                f"""
                UNWIND $expected_values AS expected
                CALL (expected) {{
                  MATCH (
                    node:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
                    {artifact_identity_map()}
                  )
                  USING INDEX node:{quote_ident(label)}({index_properties})
                  WHERE {key_predicate}
                  RETURN
                    count(node) AS matched_nodes,
                    coalesce(
                      sum(
                        CASE
                          WHEN node.{quote_ident(physical_property)} = expected.value
                          THEN 1 ELSE 0
                        END
                      ),
                      0
                    ) AS correct_nodes
                }}
                RETURN
                  count(*) AS logical_keys,
                  coalesce(sum(matched_nodes), 0) AS matched_nodes,
                  coalesce(
                    sum(
                      CASE
                        WHEN matched_nodes = expected.expected_nodes
                         AND correct_nodes = matched_nodes
                        THEN 0 ELSE 1
                      END
                    ),
                    0
                  ) AS invalid_keys
                """,
                case_parameters(pair, run_id)
                | {"expected_values": expected_values},
            ),
            f"{pair.case_id} {context} batch",
        )
        found_logical_keys += int(record["logical_keys"])
        found_nodes += int(record["matched_nodes"])
        invalid_keys += int(record["invalid_keys"])
    if found_logical_keys != workload.logical_tuple_count:
        raise ExperimentValidationError(
            f"{pair.case_id} {context}: checked {found_logical_keys} logical "
            f"keys, expected {workload.logical_tuple_count}"
        )
    if invalid_keys:
        raise ExperimentValidationError(
            f"{pair.case_id} {context}: {invalid_keys} logical key(s) have "
            "the wrong value or physical fanout"
        )
    if found_nodes != expected_nodes:
        raise ExperimentValidationError(
            f"{pair.case_id} {context}: found {found_nodes} nodes, "
            f"expected {expected_nodes}"
        )


def execute_update_batches(
    session: Any,
    query: str,
    pair: PairSpec,
    run_id: str,
    workload: UpdateWorkload,
    *,
    restore: bool,
    batch_size: int,
    run: int,
    context: str,
) -> TimingSample:
    totals = MutationTotals()
    affected_nodes = 0
    measured_ns = 0
    for updates in workload.parameter_batches(
        batch_size,
        restore=restore,
    ):
        started_ns = perf_counter_ns()
        record, summary = one_record(
            session.run(
                query,
                case_parameters(pair, run_id) | {"updates": updates},
            ),
            context,
        )
        measured_ns += perf_counter_ns() - started_ns
        affected_nodes += int(record["updated_nodes"])
        totals.add_summary(summary)
    wall_ms = measured_ns / 1_000_000
    return totals_timing_sample(
        run,
        wall_ms,
        totals,
        affected_nodes,
    )


def benchmark_updates(
    session: Any,
    pair: PairSpec,
    run_id: str,
    *,
    phase: Literal["folded", "normalized"],
    workload: UpdateWorkload,
    expected_nodes: int,
    warmup_runs: int,
    measured_runs: int,
    batch_size: int,
) -> list[TimingSample]:
    query = update_query(
        pair,
        phase=phase,
        property_name=workload.property_name,
        batch_size=batch_size,
    )
    samples: list[TimingSample] = []
    total_runs = warmup_runs + measured_runs
    validation_ordinal = 1 if warmup_runs else total_runs

    for ordinal in range(1, total_runs + 1):
        measured_number = ordinal - warmup_runs
        try:
            sample = execute_update_batches(
                session,
                query,
                pair,
                run_id,
                workload,
                restore=False,
                batch_size=batch_size,
                run=max(measured_number, 0),
                context=f"{pair.case_id} {phase} update batch",
            )
            if sample.affected_nodes != expected_nodes:
                raise ExperimentValidationError(
                    f"{pair.case_id} {phase}: updated "
                    f"{sample.affected_nodes} nodes, expected {expected_nodes}"
                )
            if sample.properties_set != expected_nodes:
                raise ExperimentValidationError(
                    f"{pair.case_id} {phase}: Neo4j reported "
                    f"{sample.properties_set} property writes, "
                    f"expected {expected_nodes}"
                )
            if ordinal == validation_ordinal:
                verify_current_update_values(
                    session,
                    pair,
                    run_id,
                    phase=phase,
                    workload=workload,
                    updated=True,
                    expected_nodes=expected_nodes,
                    context="successor validation",
                    batch_size=batch_size,
                )
        except BaseException:
            try:
                execute_update_batches(
                    session,
                    query,
                    pair,
                    run_id,
                    workload,
                    restore=True,
                    batch_size=batch_size,
                    run=0,
                    context=(
                        f"{pair.case_id} {phase} best-effort restore batch"
                    ),
                )
            except BaseException as restore_error:
                print(
                    f"WARNING: {pair.case_id} {phase} restore failed: "
                    f"{type(restore_error).__name__}: {restore_error}",
                    file=sys.stderr,
                )
            raise
        else:
            restore_sample = execute_update_batches(
                session,
                query,
                pair,
                run_id,
                workload,
                restore=True,
                batch_size=batch_size,
                run=0,
                context=f"{pair.case_id} {phase} restore batch",
            )
            if restore_sample.affected_nodes != expected_nodes:
                raise ExperimentValidationError(
                    f"{pair.case_id} {phase}: restore cardinality changed"
                )
            if ordinal > warmup_runs:
                samples.append(sample)

    verify_current_update_values(
        session,
        pair,
        run_id,
        phase=phase,
        workload=workload,
        updated=False,
        expected_nodes=expected_nodes,
        context="final restore",
        batch_size=batch_size,
    )
    return samples


def percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def numeric_summary(
    values: Sequence[float],
) -> dict[str, float | int]:
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
        "affected_nodes": sorted(
            {sample.affected_nodes for sample in samples}
        ),
        "properties_set": sorted(
            {sample.properties_set for sample in samples}
        ),
        "nodes_created": sorted({sample.nodes_created for sample in samples}),
        "nodes_deleted": sorted({sample.nodes_deleted for sample in samples}),
        "relationships_created": sorted(
            {sample.relationships_created for sample in samples}
        ),
        "relationships_deleted": sorted(
            {sample.relationships_deleted for sample in samples}
        ),
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
            f"median={server['median']:.3f} ms, mean={server['mean']:.3f} ms"
        )
    )
    print(
        f"  {title}: n={client['n']}, "
        f"client median={client['median']:.3f} ms, "
        f"mean={client['mean']:.3f} ms, "
        f"p95={client['p95']:.3f} ms; Neo4j total {server_text}"
    )


def samples_as_dicts(
    samples: Iterable[TimingSample],
) -> list[dict[str, Any]]:
    return [asdict(sample) for sample in samples]


def derived_comparison(
    redundancy: dict[str, Any],
    folded_updates: Sequence[TimingSample],
    normalization_samples: Sequence[TimingSample],
    normalized_updates: Sequence[TimingSample],
) -> dict[str, Any]:
    folded_median = median(sample.client_wall_ms for sample in folded_updates)
    normalized_median = median(
        sample.client_wall_ms for sample in normalized_updates
    )
    normalization_median = median(
        sample.client_wall_ms for sample in normalization_samples
    )
    saved_per_batch = folded_median - normalized_median
    join_rows = int(redundancy["join_rows_J"])
    referenced_rows = int(redundancy["participating_referenced_rows_D"])
    has_fd_redundancy = join_rows > referenced_rows
    return {
        "write_amplification_nodes": (
            join_rows / referenced_rows if referenced_rows else None
        ),
        "folded_to_normalized_update_median_ratio": (
            folded_median / normalized_median
            if normalized_median > 0
            else None
        ),
        "median_client_ms_saved_per_logical_update_batch": saved_per_batch,
        "has_fd_tuple_redundancy": has_fd_redundancy,
        "data_rewrite_normalization_break_even_update_batches": (
            normalization_median / saved_per_batch
            if has_fd_redundancy and saved_per_batch > 0
            else None
        ),
    }


def iter_tbl_rows(
    data_dir: Path,
    table: TblTableSpec,
) -> Iterator[dict[str, Any]]:
    """Yield typed rows from one TPC-H .tbl file."""
    path = data_dir / table.filename
    expected_columns = len(table.columns) + 1

    with path.open("r", encoding="utf-8", newline="") as source:
        reader = csv.reader(
            source,
            delimiter="|",
            quoting=csv.QUOTE_NONE,
            strict=True,
        )
        for line_number, raw_row in enumerate(reader, start=1):
            if len(raw_row) != expected_columns or raw_row[-1:] != [""]:
                raise TblFormatError(
                    f"{path}:{line_number}: expected {len(table.columns)} "
                    "fields followed by a trailing '|', "
                    f"but found {len(raw_row)} parsed fields"
                )

            typed_row: dict[str, Any] = {}
            for column, raw_value in zip(
                table.columns,
                raw_row[:-1],
                strict=True,
            ):
                try:
                    typed_row[column.name] = column.converter(raw_value)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise TblFormatError(
                        f"{path}:{line_number}: invalid value for "
                        f"{column.name}: {raw_value!r} ({exc})"
                    ) from exc
            yield typed_row


def validate_source_files(data_dir: Path) -> None:
    if not data_dir.is_dir():
        raise FileNotFoundError(
            f"TPC-H data directory does not exist: {data_dir}"
        )

    missing = [
        str(data_dir / table.filename)
        for table in TPC_H_TBL_TABLES
        if not (data_dir / table.filename).is_file()
    ]
    if missing:
        raise FileNotFoundError(
            "Missing required TPC-H files:\n  - " + "\n  - ".join(missing)
        )


def iter_source_table_rows(
    data_dir: Path,
    node: NodeSpec,
) -> Iterator[dict[str, Any]]:
    yield from iter_tbl_rows(
        data_dir,
        TBL_TABLES_BY_LABEL[node.label],
    )


def source_row_key(
    row: dict[str, Any],
    properties: Sequence[str],
) -> tuple[Any, ...]:
    return canonical_key(row[property_name] for property_name in properties)


def compact_key_token(key: tuple[Any, ...]) -> Any:
    """Return a compact exact token for TPC-H integer primary keys."""
    if key and all(
        isinstance(value, int) and 0 <= value < (1 << 64)
        for value in key
    ):
        token = 0
        for value in key:
            token = (token << 64) | value
        return token
    return tuple(domain_value_token(value) for value in key)


def relationship_endpoint_key(
    relationship: RelationshipSpec,
    row: dict[str, Any],
    *,
    source: bool,
) -> tuple[Any, ...]:
    keys = relationship.source_keys if source else relationship.target_keys
    return canonical_key(row[row_property] for _, row_property in keys)


def tbl_boundary_expectations(
    data_dir: Path,
    pair: PairSpec,
    referenced_occurrences: Counter[Any],
) -> tuple[BoundaryExpectation, ...]:
    expectations: list[BoundaryExpectation] = []
    for boundary in pair.boundaries:
        relationship = boundary.relationship
        relationship_rows = iter_source_table_rows(
            data_dir,
            TABLES[relationship.row_label],
        )
        if boundary.owner == "referencing":
            # Every referencing row occurs exactly once in this direct FK join.
            # Therefore each adjacent source relationship is copied once in
            # both folded and normalized representations.
            relationship_count = sum(1 for _ in relationship_rows)
            folded_count = relationship_count
            normalized_count = relationship_count
        else:
            degree_by_key: Counter[Any] = Counter(
                compact_key_token(
                    relationship_endpoint_key(
                        relationship,
                        row,
                        source=boundary.owner_is_source,
                    )
                )
                for row in relationship_rows
            )
            folded_count = sum(
                degree_by_key[key] * occurrences
                for key, occurrences in referenced_occurrences.items()
            )
            normalized_count = sum(
                degree_by_key[key] for key in referenced_occurrences
            )
        expectations.append(
            BoundaryExpectation(
                boundary_id=boundary.boundary_id,
                owner=boundary.owner,
                copy_type=boundary.copy_type,
                folded_count=folded_count,
                normalized_count=normalized_count,
            )
        )
    return tuple(expectations)


def validate_tbl_case(
    data_dir: Path,
    pair: PairSpec,
) -> tuple[CaseMetrics, tuple[BoundaryExpectation, ...], int, str]:
    referenced_keys: set[Any] = set()
    domain_tokens: set[str] = set()
    value_types: set[str] = set()
    null_domain_count = 0
    update_property = pair.default_update_property
    for row in iter_source_table_rows(data_dir, pair.referenced):
        key = source_row_key(row, pair.referenced.key_fields)
        key_token = compact_key_token(key)
        if key_token in referenced_keys:
            raise ExperimentValidationError(
                f"{pair.case_id}: duplicate {pair.referenced.label} key "
                f"in .tbl data: {key!r}"
            )
        referenced_keys.add(key_token)
        value = row[update_property]
        if value is None:
            null_domain_count += 1
        else:
            domain_tokens.add(domain_value_token(value))
            value_types.add(
                f"{type(value).__module__}.{type(value).__qualname__}"
            )
    if null_domain_count:
        raise ExperimentValidationError(
            f"{pair.case_id}: default update property {update_property!r} "
            f"contains {null_domain_count} NULL value(s)"
        )
    if len(value_types) != 1:
        raise ExperimentValidationError(
            f"{pair.case_id}: default update property {update_property!r} "
            f"has mixed value types: {sorted(value_types)!r}"
        )
    if len(domain_tokens) < 2:
        raise ExperimentValidationError(
            f"{pair.case_id}: default update property {update_property!r} "
            "needs at least two active-domain values"
        )

    fk_by_referenced_key = {
        referenced_key: referencing_fk
        for referencing_fk, referenced_key in pair.fk_mapping
    }
    referenced_occurrences: Counter[Any] = Counter()
    seen_referencing_keys: set[Any] = set()
    join_rows = 0
    missing_keys: list[tuple[Any, ...]] = []
    for row in iter_source_table_rows(data_dir, pair.referencing):
        referencing_key = source_row_key(row, pair.referencing.key_fields)
        referencing_key_token = compact_key_token(referencing_key)
        if referencing_key_token in seen_referencing_keys:
            raise ExperimentValidationError(
                f"{pair.case_id}: duplicate {pair.referencing.label} key "
                f"in .tbl data: {referencing_key!r}"
            )
        seen_referencing_keys.add(referencing_key_token)
        referenced_key = canonical_key(
            row[fk_by_referenced_key[property_name]]
            for property_name in pair.referenced.key_fields
        )
        if compact_key_token(referenced_key) not in referenced_keys:
            if len(missing_keys) < 5:
                missing_keys.append(referenced_key)
        else:
            referenced_occurrences[compact_key_token(referenced_key)] += 1
        join_rows += 1
    if missing_keys:
        raise ExperimentValidationError(
            f"{pair.case_id}: referencing .tbl rows contain missing FK "
            f"targets; examples={missing_keys!r}"
        )

    participating = len(referenced_occurrences)
    redundant_rows = join_rows - participating
    redundant_slots = redundant_rows * len(pair.dependent_properties)
    boundaries = tbl_boundary_expectations(
        data_dir,
        pair,
        referenced_occurrences,
    )
    metrics = CaseMetrics(
        referenced_rows=len(referenced_keys),
        join_rows=join_rows,
        participating_referenced_rows=participating,
        redundant_rows=redundant_rows,
        redundant_slots=redundant_slots,
        complete_folded_boundary_edges=sum(
            boundary.folded_count for boundary in boundaries
        ),
    )
    return metrics, boundaries, len(domain_tokens), next(iter(value_types))


def validate_cases_against_tbl(data_dir: Path) -> dict[str, Any]:
    resolved_data_dir = data_dir.expanduser().resolve()
    validate_source_files(resolved_data_dir)
    summaries: list[dict[str, Any]] = []
    for case_id in CASE_ORDER:
        pair = CASES[case_id]
        actual, boundaries, active_domain_size, value_type = validate_tbl_case(
            resolved_data_dir,
            pair,
        )
        summaries.append(
            {
                "case_id": case_id,
                "referencing_table": pair.referencing.label,
                "referenced_table": pair.referenced.label,
                "join_relationship": relationship_id(pair.join),
                "referencing_is_relationship_source": (
                    pair.referencing_is_source
                ),
                "referencing_key": list(pair.referencing.key_fields),
                "referenced_key": list(pair.referenced.key_fields),
                "fk_mapping": [list(item) for item in pair.fk_mapping],
                "table_property_aliases": {
                    "referencing": pair.referencing_alias,
                    "referenced": pair.referenced_alias,
                },
                "folded_property_bindings": {
                    "referencing": [
                        asdict(binding)
                        for binding in pair.referencing_property_bindings
                    ],
                    "referenced_non_key": [
                        asdict(binding)
                        for binding in pair.referenced_property_bindings
                    ],
                },
                "metrics": asdict(actual),
                "default_update_property": pair.default_update_property,
                "active_domain_size": active_domain_size,
                "update_value_type": value_type,
                "boundary_expectations": [
                    asdict(boundary) for boundary in boundaries
                ],
            }
        )

    composite_pair = CASES["lineitem_partsupp"]
    referenced_aliases = {
        binding.folded_name
        for binding in composite_pair.referenced_property_bindings
    }
    if (
        composite_pair.fk_mapping
        != (
            ("l_partkey", "ps_partkey"),
            ("l_suppkey", "ps_suppkey"),
        )
        or set(composite_pair.referenced.key_fields) & referenced_aliases
    ):
        raise ExperimentValidationError(
            "LINEITEM/PARTSUPP composite-FK regression failed: "
            f"mapping={composite_pair.fk_mapping!r}, "
            f"referenced aliases={sorted(referenced_aliases)!r}"
        )

    report = {
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "mode": "tbl_validation_only",
        "validated_data_dir": str(resolved_data_dir),
        "case_count": len(summaries),
        "relationship_count": len(RELATIONSHIPS),
        "cases": summaries,
        "lineitem_partsupp_composite_fk": {
            "mapping": [list(item) for item in composite_pair.fk_mapping],
            "referenced_key_copied": False,
        },
    }
    print(
        f"Validated {len(summaries)} direct TPC-H pair cases against "
        f"{resolved_data_dir}"
    )
    for summary in summaries:
        metrics = summary["metrics"]
        print(
            f"  {summary['case_id']:<30} "
            f"J={metrics['join_rows']:>5,} "
            f"D={metrics['participating_referenced_rows']:>3,} "
            f"J-D={metrics['redundant_rows']:>5,} "
            f"slots={metrics['redundant_slots']:>6,} "
            "boundaries="
            f"{metrics['complete_folded_boundary_edges']:>6,}"
        )
    return report


def print_case_catalog() -> None:
    print(
        f"{'case_id':<31} "
        f"{'referencing table':<20} "
        f"{'referenced table':<20} "
        f"{'relationship':<18} "
        "default update"
    )
    for case_id in CASE_ORDER:
        pair = CASES[case_id]
        arrow = (
            f"{pair.referencing.label}->{pair.referenced.label}"
            if pair.referencing_is_source
            else f"{pair.referenced.label}->{pair.referencing.label}"
        )
        print(
            f"{case_id:<31} "
            f"{pair.referencing.label:<20} "
            f"{pair.referenced.label:<20} "
            f"{pair.join.relationship_type:<18} "
            f"{pair.default_update_property}"
        )
        if not pair.referencing_is_source:
            print(f"  relationship direction: {arrow}")


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


def case_descriptor(pair: PairSpec) -> dict[str, Any]:
    return {
        "case_id": pair.case_id,
        "referencing_table": {
            "label": pair.referencing.label,
            "columns": list(pair.referencing.columns),
            "key": list(pair.referencing.key_fields),
        },
        "referenced_table": {
            "label": pair.referenced.label,
            "columns": list(pair.referenced.columns),
            "key": list(pair.referenced.key_fields),
            "dependent_properties": list(pair.dependent_properties),
        },
        "source_join": {
            "relationship_type": pair.join.relationship_type,
            "direction": (
                f"{pair.join.source_label}->{pair.join.target_label}"
            ),
            "referencing_is_relationship_source": pair.referencing_is_source,
            "fk_mapping_referencing_to_referenced": [
                list(mapping) for mapping in pair.fk_mapping
            ],
        },
        "folded_property_namespace": {
            "strategy_id": FOLDED_PROPERTY_NAMING_STRATEGY_ID,
            "physical_names": "native TPC-H column names",
            "table_aliases": {
                "referencing": pair.referencing_alias,
                "referenced": pair.referenced_alias,
            },
            "bindings": {
                "referencing": [
                    asdict(binding)
                    for binding in pair.referencing_property_bindings
                ],
                "referenced_non_key": [
                    asdict(binding)
                    for binding in pair.referenced_property_bindings
                ],
            },
            "referenced_key_copied": False,
            "referenced_key_representation": [
                {
                    "referencing_fk": referencing_fk,
                    "folded_referencing_fk": (
                        pair.referencing_binding_by_logical_name[
                            referencing_fk
                        ]
                    ),
                    "referenced_key": referenced_key,
                }
                for referencing_fk, referenced_key in pair.fk_mapping
            ],
        },
        "topology_fk_properties_excluded_from_updates": list(
            pair.topology_fk_properties
        ),
        "eligible_update_properties_before_domain_checks": list(
            pair.allowed_update_properties
        ),
        "default_update_property": pair.default_update_property,
        "boundaries": [
            {
                **asdict(boundary),
                "relationship": asdict(boundary.relationship),
                "boundary_id": boundary.boundary_id,
            }
            for boundary in pair.boundaries
        ],
    }


def print_final_comparison(report: dict[str, Any]) -> None:
    derived = report["derived"]
    break_even = derived[
        "data_rewrite_normalization_break_even_update_batches"
    ]
    ratio = derived["folded_to_normalized_update_median_ratio"]
    amplification = derived["write_amplification_nodes"]
    print(f"\nFinal comparison: {report['case_id']}")
    print(
        "  Write amplification by updated nodes: "
        + ("N/A" if amplification is None else f"{amplification:.3f}x")
    )
    print(
        "  Folded/normalized median client-time ratio: "
        + ("N/A" if ratio is None else f"{ratio:.3f}x")
    )
    print(
        "  Estimated normalization break-even: "
        + (
            "N/A"
            if break_even is None
            else f"{break_even:.3f} logical update batches"
        )
    )
    print(
        "  Final database state: original graph unchanged; "
        "run-scoped MV nodes, edges, indexes, and constraints removed"
    )


def write_json_report(path: Path, report: dict[str, Any]) -> Path:
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return resolved


def run_case_experiment(
    driver: Any,
    database: str,
    args: argparse.Namespace,
    pair: PairSpec,
    source_sha256_before: str,
) -> dict[str, Any]:
    from neo4j import WRITE_ACCESS, __version__ as neo4j_driver_version

    started_at = datetime.now(timezone.utc)
    run_id = uuid4().hex
    constraints: list[ConstraintLease] = []
    indexes: list[IndexLease] = []
    cleanup_completed = False
    workload: UpdateWorkload | None = None

    try:
        with driver.session(
            database=database,
            default_access_mode=WRITE_ACCESS,
        ) as session:
            ensure_no_existing_artifacts(session, [pair])
            if args.topology_mode == "complete":
                validate_complete_source_topology(session, pair)
            baseline = build_case_baseline(
                session,
                pair,
                args.topology_mode,
                source_sha256_before,
            )
            environment = collect_environment_metadata(
                session,
                driver,
                neo4j_driver_version,
            )
            update_property = (
                args.update_property or pair.default_update_property
            )
            redundancy, workload, payload_metrics = build_case_measurements(
                session,
                pair,
                baseline,
                update_property,
                args.batch_size,
            )

            constraints, indexes = create_case_schema(
                session,
                pair,
                run_id,
                args.index_wait_seconds,
            )

            print(
                f"\n[{pair.case_id}] Step 0: creating "
                f"{pair.names.folded_label} shadow projection (not timed)"
            )
            print(f"  Run ID: {run_id}", flush=True)
            step0 = create_shadow_projection(
                session,
                pair,
                run_id,
                args.topology_mode,
                args.batch_size,
            )
            folded_state = validate_folded_shadow_state(
                session,
                pair,
                run_id,
                baseline,
                args.topology_mode,
            )
            print(
                f"  Created {step0['folded_nodes']:,} folded nodes and "
                f"{step0['boundary_relationship_total']:,} boundary "
                "relationship copies"
            )

            print(
                f"\n[{pair.case_id}] Step 1: measuring referenced-table FD redundancy"
            )
            print(
                "  Join fingerprint "
                f"(schema v{JOIN_FINGERPRINT_SCHEMA_VERSION}): "
                f"{baseline.join_sha256}"
            )
            print_redundancy(pair, redundancy)

            print(
                f"\n[{pair.case_id}] Step 2: updating folded referenced "
                "copies "
                f"({workload.property_name}; {args.warmup_runs} warmups, "
                f"{args.runs} measured runs)"
            )
            print(
                f"  Batch: {workload.logical_tuple_count:,} logical "
                "referenced-row updates; physical folded writes="
                f"{baseline.join_rows:,}; "
                "restore is outside the timer"
            )
            print(
                "  Value strategy: deterministic full referenced-relation "
                "active-domain "
                f"rotation; domain={workload.active_domain_size:,}, "
                f"type={workload.value_type}"
            )
            folded_update_samples = benchmark_updates(
                session,
                pair,
                run_id,
                phase="folded",
                workload=workload,
                expected_nodes=baseline.join_rows,
                warmup_runs=args.warmup_runs,
                measured_runs=args.runs,
                batch_size=args.batch_size,
            )
            print_timing_summary(
                "folded update",
                folded_update_samples,
            )
            validate_folded_shadow_state(
                session,
                pair,
                run_id,
                baseline,
                args.topology_mode,
                deep=False,
            )

            print(
                f"\n[{pair.case_id}] Step 3: normalizing the shadow "
                f"projection ({args.normalization_warmup_runs} warmups, "
                f"{args.normalization_runs} measured runs)"
            )
            if args.topology_mode == "complete":
                print(
                    "  Timing scope: referenced-property extraction, "
                    "node decomposition, normalized join creation, "
                    "referenced-boundary "
                    "deduplication/migration, and all batch commits"
                )
            else:
                print(
                    "  Timing scope: referenced-property extraction, "
                    "node decomposition, normalized join creation, and "
                    "all batch commits (no boundary topology)"
                )
            normalization_samples: list[TimingSample] = []
            normalization_total = (
                args.normalization_warmup_runs + args.normalization_runs
            )
            for ordinal in range(1, normalization_total + 1):
                measured_number = ordinal - args.normalization_warmup_runs
                sample = normalize_once(
                    session,
                    pair,
                    run_id,
                    baseline,
                    args.topology_mode,
                    max(measured_number, 0),
                    args.batch_size,
                )
                if ordinal > args.normalization_warmup_runs:
                    normalization_samples.append(sample)
                if ordinal < normalization_total:
                    refold_shadow_projection(
                        session,
                        pair,
                        run_id,
                        baseline,
                        args.topology_mode,
                        args.batch_size,
                    )
            print_timing_summary(
                "normalization",
                normalization_samples,
            )
            normalized_state = validate_normalized_shadow_state(
                session,
                pair,
                run_id,
                baseline,
                args.topology_mode,
            )

            print(
                f"\n[{pair.case_id}] Step 4: updating normalized referenced "
                "nodes "
                f"({workload.property_name}; {args.warmup_runs} warmups, "
                f"{args.runs} measured runs)"
            )
            print(
                f"  Batch: {workload.logical_tuple_count:,} logical "
                "referenced-row updates; physical normalized writes="
                f"{baseline.participating_referenced_rows:,}; restore is "
                "outside the timer"
            )
            normalized_update_samples = benchmark_updates(
                session,
                pair,
                run_id,
                phase="normalized",
                workload=workload,
                expected_nodes=baseline.participating_referenced_rows,
                warmup_runs=args.warmup_runs,
                measured_runs=args.runs,
                batch_size=args.batch_size,
            )
            print_timing_summary(
                "normalized update",
                normalized_update_samples,
            )
            validate_normalized_shadow_state(
                session,
                pair,
                run_id,
                baseline,
                args.topology_mode,
                deep=False,
            )

            print(
                f"\n[{pair.case_id}] Cleanup: deleting this run's shadow "
                "projection (not timed)"
            )
            cleanup_result = cleanup_shadow_artifacts(
                session,
                pair,
                run_id,
                args.batch_size,
            )
            drop_case_schema(
                session,
                constraints,
                indexes,
            )
            constraints = []
            indexes = []
            cleanup_completed = True
            print(
                f"  Removed {cleanup_result.deleted_nodes:,} nodes and "
                f"{cleanup_result.deleted_relationships:,} relationships; "
                "shadow cleanup verified"
            )
            print(
                "  Fingerprinting the original TPC-H graph after cleanup...",
                flush=True,
            )
            source_sha256_after = original_graph_sha256(session)
            print(
                "  Original graph fingerprint after cleanup: "
                f"{source_sha256_after}",
                flush=True,
            )
            if source_sha256_after != source_sha256_before:
                raise ExperimentValidationError(
                    f"{pair.case_id}: original graph fingerprint changed "
                    "after cleanup"
                )
            print(
                "  Original graph unchanged: cleanup fingerprint matches "
                "the suite baseline",
                flush=True,
            )

        completed_at = datetime.now(timezone.utc)
        derived = derived_comparison(
            redundancy,
            folded_update_samples,
            normalization_samples,
            normalized_update_samples,
        )
        # Generic aliases retained for existing FD-report readers.
        derived["denormalized_to_normalized_update_median_ratio"] = derived[
            "folded_to_normalized_update_median_ratio"
        ]
        derived[
            "shadow_data_rewrite_normalization_break_even_update_batches"
        ] = derived["data_rewrite_normalization_break_even_update_batches"]
        folded_timing = {
            "samples": samples_as_dicts(folded_update_samples),
            "summary": timing_summary(folded_update_samples),
        }
        normalization_timing = {
            "samples": samples_as_dicts(normalization_samples),
            "summary": timing_summary(normalization_samples),
        }
        normalized_timing = {
            "samples": samples_as_dicts(normalized_update_samples),
            "summary": timing_summary(normalized_update_samples),
        }
        report = {
            "report_schema_version": REPORT_SCHEMA_VERSION,
            "experiment": "tpch_direct_pair_fd",
            "experiment_family": "tpch_direct_pair_fd",
            "case_id": pair.case_id,
            "run_id": run_id,
            "started_at_utc": started_at.isoformat(),
            "completed_at_utc": completed_at.isoformat(),
            "database": database,
            "environment": environment,
            "pair": case_descriptor(pair),
            "execution": {
                "mode": "run_scoped_batched_shadow_projection",
                "topology_mode": args.topology_mode,
                "source_policy": (
                    "the original graph is read-only; every experimental "
                    "node and relationship is run-scoped and removed"
                ),
                "boundary_overlay_note": (
                    "complete mode temporarily attaches copied boundary "
                    "relationships to original external nodes; do not run "
                    "wildcard degree/traversal queries concurrently"
                    if args.topology_mode == "complete"
                    else "property-fd mode creates no boundary overlay"
                ),
                "local_concurrency_policy": (
                    "exclusive advisory process lock"
                ),
                "mutation_atomicity": (
                    "each configured-size subtransaction is atomic; a "
                    "multi-batch phase is compensated by run-scoped cleanup "
                    "if a later batch fails"
                ),
            },
            "dataset": {
                "name": "TPC-H",
                "source_graph_fingerprint_schema_version": (
                    SOURCE_GRAPH_FINGERPRINT_SCHEMA_VERSION
                ),
                "source_graph_fingerprint_algorithm": (
                    SOURCE_GRAPH_FINGERPRINT_ALGORITHM
                ),
                "source_graph_sha256_before": source_sha256_before,
                "source_graph_sha256_during": None,
                "source_graph_during_scan": (
                    "not scanned while artifacts exist; recomputed "
                    "immediately after this case cleanup"
                ),
                "source_graph_sha256_after": source_sha256_after,
                "source_graph_verification_scope": (
                    "suite baseline fingerprint compared with a fresh "
                    "fingerprint immediately after this case cleanup"
                ),
                "join_fingerprint_schema_version": (
                    JOIN_FINGERPRINT_SCHEMA_VERSION
                ),
                "join_fingerprint_algorithm": JOIN_FINGERPRINT_ALGORITHM,
                "join_sha256": baseline.join_sha256,
                "referencing_rows": baseline.referencing_rows,
                "full_referenced_rows": baseline.referenced_rows,
                "join_rows": baseline.join_rows,
                "participating_referenced_rows": (
                    baseline.participating_referenced_rows
                ),
                "boundary_expectations": [
                    asdict(expectation)
                    for expectation in baseline.boundary_expectations
                ],
            },
            "configuration": {
                "update_property": workload.property_name,
                "batch_size": args.batch_size,
                "batch_size_scope": (
                    "logical update parameters and physical graph mutation "
                    "subtransactions"
                ),
                "update_key_properties": list(pair.referenced.key_fields),
                "warmup_runs": args.warmup_runs,
                "measured_update_runs": args.runs,
                "normalization_warmup_runs": (args.normalization_warmup_runs),
                "normalization_runs": args.normalization_runs,
                "folded_property_naming_strategy_id": (
                    FOLDED_PROPERTY_NAMING_STRATEGY_ID
                ),
                "update_strategy_id": UPDATE_STRATEGY_ID,
                "update_null_policy": UPDATE_NULL_POLICY,
                "active_domain_scope": (
                    f"full_original_{pair.referenced.label}_relation"
                ),
                "active_domain_size": workload.active_domain_size,
                "active_domain_source_tuple_count": (
                    workload.active_domain_source_tuple_count
                ),
                "logical_update_tuple_count": (workload.logical_tuple_count),
                "active_domain_sha256": workload.active_domain_sha256,
                "update_mapping_sha256": workload.mapping_sha256,
                "update_value_type": workload.value_type,
                "update_workload_storage": (
                    "run-local temporary SQLite spool, removed when the case "
                    "finishes or fails"
                ),
                "schema_objects_created_and_dropped": (
                    case_schema_names(pair, run_id)
                ),
                "update_timing_scope": (
                    "sum of sequential database-batch wall times through "
                    "result consumption and commit; spool iteration, restore, "
                    "and validation excluded"
                ),
                "normalization_timing_scope": (
                    "sequential batched subtransactions for referenced-property "
                    "extraction and shadow data decomposition"
                    + (
                        ", referenced-boundary deduplication/migration"
                        if args.topology_mode == "complete"
                        else ""
                    )
                    + "; DDL, validation, refolding, and cleanup excluded"
                ),
                "transformation_validation_policy": (
                    "structural cardinality checks after every normalize/refold "
                    "cycle; full label/property/relationship/value fingerprint "
                    "checks at stable phase checkpoints"
                ),
                "cache_policy": (
                    "warm workload; server page/query caches are not cleared"
                ),
            },
            "update_payload": payload_metrics,
            "step0_artifacts": step0,
            "folded_shadow_state": asdict(folded_state),
            "normalized_shadow_state": asdict(normalized_state),
            "redundancy": redundancy,
            "timings": {
                "folded_update": folded_timing,
                "denormalized_update": folded_timing,
                "normalization": normalization_timing,
                "normalized_update": normalized_timing,
            },
            "derived": derived,
            "cleanup": {
                **asdict(cleanup_result),
                "final_shadow_state": "clean",
                "experiment_owned_schema_dropped": True,
                "original_graph_verified_unchanged": True,
                "original_graph_verification": (
                    "case cleanup fingerprint matches suite baseline"
                ),
            },
            "final_state": {
                "original_graph": "unchanged",
                "shadow_graph": "removed",
            },
        }
        return report
    except BaseException:
        print(
            f"\nWARNING: {pair.case_id} stopped; attempting cleanup for "
            f"run_id={run_id}",
            file=sys.stderr,
        )
        cleanup_errors: list[BaseException] = []
        try:
            with driver.session(
                database=database,
                default_access_mode=WRITE_ACCESS,
            ) as cleanup_session:
                try:
                    cleanup_result = cleanup_shadow_artifacts(
                        cleanup_session,
                        pair,
                        run_id,
                        args.batch_size,
                    )
                    print(
                        "  Failure cleanup removed "
                        f"{cleanup_result.deleted_nodes:,} nodes and "
                        f"{cleanup_result.deleted_relationships:,} "
                        "relationships",
                        file=sys.stderr,
                    )
                except BaseException as cleanup_error:
                    cleanup_errors.append(cleanup_error)
                try:
                    if constraints or indexes:
                        drop_case_schema(
                            cleanup_session,
                            constraints,
                            indexes,
                        )
                    else:
                        drop_recovery_schema(cleanup_session, run_id)
                except BaseException as cleanup_error:
                    cleanup_errors.append(cleanup_error)
                try:
                    failure_source_sha256 = original_graph_sha256(
                        cleanup_session
                    )
                    print(
                        "  Original graph fingerprint after failure cleanup: "
                        f"{failure_source_sha256}",
                        file=sys.stderr,
                        flush=True,
                    )
                    if failure_source_sha256 != source_sha256_before:
                        raise ExperimentValidationError(
                            "original graph fingerprint changed"
                        )
                    print(
                        "  Original graph unchanged after failure cleanup",
                        file=sys.stderr,
                        flush=True,
                    )
                except BaseException as cleanup_error:
                    cleanup_errors.append(cleanup_error)
        except BaseException as cleanup_session_error:
            cleanup_errors.append(cleanup_session_error)
        if cleanup_errors:
            print(
                "WARNING: automatic cleanup checks failed: "
                + "; ".join(
                    f"{type(error).__name__}: {error}"
                    for error in cleanup_errors
                ),
                file=sys.stderr,
            )
        raise
    finally:
        if workload is not None:
            workload.close()
        # Makes the intended success condition explicit for future changes.
        if cleanup_completed:
            assert not constraints and not indexes


def cleanup_stale_run(
    driver: Any,
    database: str,
    run_id: str,
    batch_size: int,
) -> dict[str, Any]:
    from neo4j import WRITE_ACCESS

    with driver.session(
        database=database,
        default_access_mode=WRITE_ACCESS,
    ) as session:
        if ARTIFACT_LABEL in database_node_labels(session):
            case_result = session.run(
                f"""
                MATCH (
                  node:{quote_ident(ARTIFACT_LABEL)}
                  {{{quote_ident(RUN_ID_PROPERTY)}: $run_id}}
                )
                RETURN DISTINCT
                  node.{quote_ident(CASE_ID_PROPERTY)} AS case_id
                ORDER BY case_id
                """,
                run_id=run_id,
            )
            case_records, _ = result_records_and_summary(case_result)
        else:
            case_records = []
        if any(record["case_id"] is None for record in case_records):
            raise ExperimentStateError(
                f"run {run_id!r} contains artifact nodes without a case ID; "
                "automatic cleanup is fail-closed"
            )
        case_ids = [
            str(record["case_id"])
            for record in case_records
            if record["case_id"] is not None
        ]
        unknown = sorted(set(case_ids) - set(CASES))
        if unknown:
            raise ExperimentStateError(
                f"run {run_id!r} contains unknown case IDs: {unknown!r}"
            )
        cleanup_results: dict[str, dict[str, int]] = {}
        for case_id in case_ids:
            cleanup_results[case_id] = asdict(
                cleanup_shadow_artifacts(
                    session,
                    CASES[case_id],
                    run_id,
                    batch_size,
                )
            )
        dropped_constraints, dropped_indexes = drop_recovery_schema(
            session,
            run_id,
        )
        remaining_nodes, remaining_relationships = artifact_counts(session)
        return {
            "report_schema_version": REPORT_SCHEMA_VERSION,
            "mode": "cleanup_only",
            "run_id": run_id,
            "cleaned_cases": cleanup_results,
            "dropped_constraints": dropped_constraints,
            "dropped_indexes": dropped_indexes,
            "remaining_global_artifact_nodes": remaining_nodes,
            "remaining_global_artifact_relationships": (
                remaining_relationships
            ),
            "source_graph_fingerprint_schema_version": (
                SOURCE_GRAPH_FINGERPRINT_SCHEMA_VERSION
            ),
            "source_graph_fingerprint_algorithm": (
                SOURCE_GRAPH_FINGERPRINT_ALGORITHM
            ),
            "source_graph_sha256_after_cleanup": (
                original_graph_sha256(session)
            ),
        }


def validate_main_args(args: argparse.Namespace) -> None:
    if args.case_id is None and args.update_property is not None:
        raise ExperimentValidationError(
            "--update-property requires --case because each referenced table "
            "has a different property domain"
        )
    if (args.list_cases or args.validate_cases_only) and (
        args.all_cases
        or args.case_id is not None
        or args.update_property is not None
    ):
        raise ExperimentValidationError(
            "--list-cases/--validate-cases-only cannot be combined with "
            "--case, --all-cases, or --update-property"
        )
    if args.cleanup_run_id is not None:
        raw = args.cleanup_run_id.strip()
        if not raw or len(raw) > 128:
            raise ExperimentValidationError(
                "--cleanup-run-id must contain 1 to 128 characters"
            )
        if (
            args.all_cases
            or args.case_id is not None
            or args.update_property is not None
            or args.list_cases
            or args.validate_cases_only
        ):
            raise ExperimentValidationError(
                "--cleanup-run-id cannot be combined with experiment or "
                "catalog-validation options"
            )


def resolve_neo4j_password(user: str) -> str:
    environment_password = os.getenv("NEO4J_PASSWORD")
    if environment_password:
        return environment_password
    if QUICK_DEBUG_NEO4J_PASSWORD:
        return QUICK_DEBUG_NEO4J_PASSWORD
    return getpass(f"Neo4j password for {user}: ")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        validate_main_args(args)
        offline_report: dict[str, Any] | None = None
        if args.list_cases:
            print_case_catalog()
        if args.validate_cases_only:
            offline_report = validate_cases_against_tbl(args.data_dir)
        if args.list_cases or args.validate_cases_only:
            if args.output_json is not None and offline_report is not None:
                output = write_json_report(
                    args.output_json,
                    offline_report,
                )
                print(f"JSON validation report: {output}")
            return 0

        database = args.database.strip().lower()
        if not database or database == "system":
            raise ExperimentValidationError(
                "--database must name an existing TPC-H data database"
            )
        cleanup_run_id = (
            args.cleanup_run_id.strip()
            if args.cleanup_run_id is not None
            else None
        )

        try:
            from neo4j import GraphDatabase
        except ImportError as exc:
            raise RuntimeError(
                'install the Neo4j driver with: python -m pip install "neo4j>=5.7"'
            ) from exc

        password = resolve_neo4j_password(args.user)

        print(
            f"Connecting to {args.uri} as {args.user!r}; database={database!r}"
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
                if cleanup_run_id is not None:
                    cleanup_report = cleanup_stale_run(
                        driver,
                        database,
                        cleanup_run_id,
                        args.batch_size,
                    )
                    reports: list[dict[str, Any]] = []
                else:
                    cleanup_report = None
                    selected_ids = (
                        CASE_ORDER if args.case_id is None else (args.case_id,)
                    )
                    selected_pairs = [CASES[case_id] for case_id in selected_ids]
                    print(
                        "Fingerprinting the original TPC-H graph before the "
                        "selected-case suite...",
                        flush=True,
                    )
                    with driver.session(database=database) as source_session:
                        ensure_no_existing_artifacts(
                            source_session,
                            selected_pairs,
                        )
                        suite_source_sha256_before = original_graph_sha256(
                            source_session
                        )
                    print(
                        "Original TPC-H graph fingerprint before suite: "
                        f"{suite_source_sha256_before}",
                        flush=True,
                    )
                    reports = []
                    for index, case_id in enumerate(
                        selected_ids,
                        start=1,
                    ):
                        if len(selected_ids) > 1:
                            print(
                                f"\n=== Case {index}/{len(selected_ids)}: {case_id} ==="
                            )
                        report = run_case_experiment(
                            driver,
                            database,
                            args,
                            CASES[case_id],
                            suite_source_sha256_before,
                        )
                        reports.append(report)
                    after_fingerprints = {
                        report["dataset"]["source_graph_sha256_after"]
                        for report in reports
                    }
                    if after_fingerprints != {suite_source_sha256_before}:
                        raise ExperimentValidationError(
                            "one or more per-case cleanup fingerprints differ "
                            "from the suite baseline"
                        )
                    print(
                        "All per-case cleanup fingerprints match the "
                        "original TPC-H suite baseline.",
                        flush=True,
                    )
                    for report in reports:
                        print_final_comparison(report)

        if cleanup_report is not None:
            cleaned_nodes = sum(
                result["deleted_nodes"]
                for result in cleanup_report["cleaned_cases"].values()
            )
            cleaned_relationships = sum(
                result["deleted_relationships"]
                for result in cleanup_report["cleaned_cases"].values()
            )
            print(
                f"Cleanup-only complete for run_id={cleanup_run_id}: "
                f"removed {cleaned_nodes:,} nodes and "
                f"{cleaned_relationships:,} relationships"
            )
            output_report: dict[str, Any] = cleanup_report
        elif len(reports) > 1:
            output_report = {
                "report_schema_version": REPORT_SCHEMA_VERSION,
                "experiment": "tpch_direct_pair_fd_suite",
                "database": database,
                "topology_mode": args.topology_mode,
                "case_count": len(reports),
                "case_order": list(CASE_ORDER),
                "source_graph_sha256_before": reports[0]["dataset"][
                    "source_graph_sha256_before"
                ],
                "source_graph_sha256_after": reports[-1]["dataset"][
                    "source_graph_sha256_after"
                ],
                "cases": reports,
            }
            print(
                f"\nAll {len(reports)} direct TPC-H pair cases "
                "completed; every shadow projection was removed"
            )
        else:
            output_report = reports[0]

        if args.output_json is not None:
            output = write_json_report(
                args.output_json,
                output_report,
            )
            print(f"JSON report: {output}")
        return 0
    except KeyboardInterrupt:
        print("\nExperiment interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        if exc.__cause__ is not None:
            print(f"CAUSE: {exc.__cause__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
