#!/usr/bin/env python3
"""Run pair-wise Northwind functional-dependency experiments in Neo4j.

The original Product/Supplier experiment has been generalized to every direct
foreign-key relationship imported by ``case_study/data/import_data.py``.
For each case, the FK-bearing table is the carrier A and the referenced table
is the dimension B.  One run-scoped shadow node represents each row of
``A JOIN B``.

With no case-selection argument, all ten direct Northwind cases run
sequentially.  Use ``--list-cases`` to inspect the complete catalog or
``--case CASE_ID`` to run only one case.  ``--all-cases`` is retained as an
explicit alias for the default batch behavior.

Two topology modes are available:

``complete`` (default)
    Copy all declared boundary relationships incident to A and B, excluding
    the folded A-B relationship.  Carrier-owned copies stay on the carrier
    shadow.  Dimension-owned copies are duplicated in the folded phase,
    deduplicated onto the normalized dimension in Step 3, and copied back
    during out-of-timer refolding.

``property-fd``
    Materialize only namespaced A/B properties and the normalized A-B
    relationship.  This isolates property redundancy from query-topology
    maintenance.

Scope deliberately excludes conceptual three-table many-to-many joins,
arbitrary multi-hop paths, and the Employee.ReportsTo self-reference.  Those
require different normalization semantics.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import platform
import re
import sys
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from getpass import getpass
from pathlib import Path
from statistics import fmean, median, stdev
from time import perf_counter_ns
from types import ModuleType
from typing import Any, Iterable, Iterator, Literal, Sequence
from uuid import uuid4


DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_DATABASE = "northwind"
DEFAULT_WARMUP_RUNS = 5
DEFAULT_RUNS = 20
DEFAULT_NORMALIZATION_WARMUP_RUNS = 1
DEFAULT_NORMALIZATION_RUNS = 20
DEFAULT_INDEX_WAIT_SECONDS = 300
DEFAULT_TOPOLOGY_MODE = "complete"
DEFAULT_OUTPUT_JSON = (
    Path(__file__).resolve().parent
    / "northwind_fd_experiment_results.json"
)

RUN_ID_PROPERTY = "run_id"
CASE_ID_PROPERTY = "mv_case_id"
ARTIFACT_LABEL = "mv_experiment"
SCHEMA_PREFIX = "mvfd"
UPDATE_STRATEGY_ID = "deterministic_full_dimension_domain_rotation_v1"
UPDATE_NULL_POLICY = "reject_attribute_if_any_domain_or_updated_value_is_null"

# QUICK LOCAL DEBUGGING ONLY:
# Replace None with a local password string to skip the interactive prompt.
# Never commit or share the file while a real password is present here.
QUICK_DEBUG_NEO4J_PASSWORD: str | None = ""


class ExperimentStateError(RuntimeError):
    """Raised when the original or shadow graph is in an unsafe state."""


class ExperimentValidationError(RuntimeError):
    """Raised when a transformation or measured workload is inconsistent."""


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
    owner: Literal["carrier", "dimension"]
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
    carrier_label: str
    dimension_label: str
    normalized_join_type: str


@dataclass(frozen=True)
class ExpectedCaseMetrics:
    dimension_rows: int
    join_rows: int
    participating_dimensions: int
    redundant_rows: int
    redundant_slots: int
    complete_folded_boundary_edges: int


@dataclass(frozen=True)
class PairSpec:
    case_id: str
    carrier: NodeSpec
    dimension: NodeSpec
    join: RelationshipSpec
    carrier_is_source: bool
    fk_mapping: tuple[tuple[str, str], ...]
    dependent_properties: tuple[str, ...]
    property_bindings: tuple[PropertyBinding, ...]
    topology_fk_properties: tuple[str, ...]
    default_update_property: str
    boundaries: tuple[BoundaryEdgeSpec, ...]
    names: PairNames
    expected: ExpectedCaseMetrics

    @property
    def carrier_fk_fields(self) -> tuple[str, ...]:
        by_dimension_key = {
            dimension_key: carrier_fk
            for carrier_fk, dimension_key in self.fk_mapping
        }
        return tuple(
            by_dimension_key[key] for key in self.dimension.key_fields
        )

    @property
    def binding_by_logical_name(self) -> dict[str, str]:
        return {
            binding.logical_name: binding.folded_name
            for binding in self.property_bindings
        }

    @property
    def allowed_update_properties(self) -> tuple[str, ...]:
        blocked = set(self.dimension.key_fields)
        blocked.update(self.topology_fk_properties)
        return tuple(
            prop
            for prop in self.dependent_properties
            if prop not in blocked
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
class JoinRow:
    carrier_key: tuple[Any, ...]
    dimension_key: tuple[Any, ...]
    carrier_values: dict[str, Any]
    dimension_values: dict[str, Any]


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
    carrier_rows: int
    dimension_rows: int
    join_rows: int
    participating_dimensions: int
    join_sha256: str
    source_sha256: str
    boundary_expectations: tuple[BoundaryExpectation, ...]


@dataclass(frozen=True)
class ShadowState:
    folded_carriers: int
    normalized_carriers: int
    normalized_dimensions: int
    normalized_join_relationships: int
    boundary_relationships: dict[str, int]

    @property
    def phase(self) -> str:
        boundary_total = sum(self.boundary_relationships.values())
        if (
            self.folded_carriers == 0
            and self.normalized_carriers == 0
            and self.normalized_dimensions == 0
            and self.normalized_join_relationships == 0
            and boundary_total == 0
        ):
            return "clean"
        if (
            self.folded_carriers > 0
            and self.normalized_carriers == 0
            and self.normalized_dimensions == 0
            and self.normalized_join_relationships == 0
        ):
            return "folded"
        if (
            self.folded_carriers == 0
            and self.normalized_carriers > 0
            and self.normalized_dimensions > 0
            and self.normalized_join_relationships
            == self.normalized_carriers
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


@dataclass(frozen=True)
class UpdateWorkload:
    property_name: str
    folded_property_name: str
    originals: dict[tuple[Any, ...], Any]
    updates: list[dict[str, Any]]
    restores: list[dict[str, Any]]
    active_domain_size: int
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
        self.properties_set += int(
            getattr(counters, "properties_set", 0)
        )
        self.nodes_created += int(getattr(counters, "nodes_created", 0))
        self.nodes_deleted += int(getattr(counters, "nodes_deleted", 0))
        self.relationships_created += int(
            getattr(counters, "relationships_created", 0)
        )
        self.relationships_deleted += int(
            getattr(counters, "relationships_deleted", 0)
        )
        self.labels_added += int(getattr(counters, "labels_added", 0))
        self.labels_removed += int(
            getattr(counters, "labels_removed", 0)
        )


def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def snake_case(name: str) -> str:
    first = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", first).lower()


def safe_schema_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", value)


def load_import_schema_module() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "data" / "import_data.py"
    module_name = "_northwind_import_schema_for_fd_experiment"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load Northwind schema from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


IMPORT_SCHEMA = load_import_schema_module()

TABLES: dict[str, NodeSpec] = {
    table.label: NodeSpec(
        filename=table.filename,
        label=table.label,
        columns=tuple(table.column_names),
        key_fields=tuple(table.key_fields),
    )
    for table in IMPORT_SCHEMA.NODE_TABLES
}

RELATIONSHIPS: tuple[RelationshipSpec, ...] = tuple(
    RelationshipSpec(
        row_label=relationship.row_table.label,
        source_label=relationship.source.table.label,
        target_label=relationship.target.table.label,
        relationship_type=relationship.relationship_type,
        source_keys=tuple(relationship.source.keys),
        target_keys=tuple(relationship.target.keys),
    )
    for relationship in IMPORT_SCHEMA.RELATIONSHIPS
)

DEFAULT_UPDATE_PROPERTIES: dict[str, str] = {
    "order_customer": "Phone",
    "order_detail_order": "Freight",
    "order_detail_product": "ProductName",
    "product_category": "CategoryName",
    "product_supplier": "Phone",
    "order_employee": "LastName",
    "order_shipper": "Phone",
    "employee_territory_employee": "LastName",
    "employee_territory_territory": "TerritoryDescription",
    "territory_region": "RegionDescription",
}

EXPECTED_CASE_METRICS: dict[str, ExpectedCaseMetrics] = {
    "order_customer": ExpectedCaseMetrics(91, 830, 89, 741, 7_410, 3_815),
    "order_detail_order": ExpectedCaseMetrics(
        830, 2_155, 830, 1_325, 17_225, 8_620
    ),
    "order_detail_product": ExpectedCaseMetrics(
        77, 2_155, 77, 2_078, 18_702, 6_465
    ),
    "product_category": ExpectedCaseMetrics(8, 77, 8, 69, 207, 2_232),
    "product_supplier": ExpectedCaseMetrics(29, 77, 29, 48, 528, 2_232),
    "order_employee": ExpectedCaseMetrics(
        9, 830, 9, 821, 13_957, 7_775
    ),
    "order_shipper": ExpectedCaseMetrics(3, 830, 3, 827, 1_654, 3_815),
    "employee_territory_employee": ExpectedCaseMetrics(
        9, 49, 9, 40, 680, 4_009
    ),
    "employee_territory_territory": ExpectedCaseMetrics(
        53, 49, 49, 0, 0, 98
    ),
    "territory_region": ExpectedCaseMetrics(4, 53, 4, 49, 49, 49),
}

# ReportsTo is a self-FK in the CSV but intentionally is not an imported edge.
MANUAL_TOPOLOGY_PROPERTIES: dict[str, tuple[str, ...]] = {
    "Employee": ("ReportsTo",),
}


def relationship_id(relationship: RelationshipSpec) -> str:
    return (
        f"{relationship.source_label}"
        f"-[:{relationship.relationship_type}]->"
        f"{relationship.target_label}"
    )


def find_import_relationship(raw: RelationshipSpec) -> Any:
    for relationship in IMPORT_SCHEMA.RELATIONSHIPS:
        if (
            relationship.row_table.label == raw.row_label
            and relationship.source.table.label == raw.source_label
            and relationship.target.table.label == raw.target_label
            and relationship.relationship_type == raw.relationship_type
        ):
            return relationship
    raise KeyError(relationship_id(raw))


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


def build_boundary_specs(
    join: RelationshipSpec,
    carrier: NodeSpec,
    dimension: NodeSpec,
) -> tuple[BoundaryEdgeSpec, ...]:
    boundaries: list[BoundaryEdgeSpec] = []
    owners: tuple[
        tuple[Literal["carrier", "dimension"], NodeSpec],
        ...,
    ] = (
        ("carrier", carrier),
        ("dimension", dimension),
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
                        copy_type=(
                            f"join_{relationship.relationship_type.lower()}"
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
                        copy_type=(
                            f"join_{relationship.relationship_type.lower()}"
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
            f"boundary copy type collision for {relationship_id(join)}: "
            f"{copy_types!r}"
        )
    return tuple(boundaries)


def build_pair_spec(join: RelationshipSpec) -> PairSpec:
    carrier = TABLES[join.row_label]
    if join.source_label == carrier.label:
        carrier_is_source = True
        dimension = TABLES[join.target_label]
        dimension_endpoint_keys = join.target_keys
    elif join.target_label == carrier.label:
        carrier_is_source = False
        dimension = TABLES[join.source_label]
        dimension_endpoint_keys = join.source_keys
    else:
        raise ExperimentValidationError(
            f"{relationship_id(join)} does not contain row table "
            f"{carrier.label}"
        )

    fk_mapping = tuple(
        (row_property, dimension_property)
        for dimension_property, row_property in dimension_endpoint_keys
    )
    mapped_dimension_keys = tuple(
        dimension_property for _, dimension_property in fk_mapping
    )
    if set(mapped_dimension_keys) != set(dimension.key_fields):
        raise ExperimentValidationError(
            f"{relationship_id(join)} FK does not cover dimension key: "
            f"{fk_mapping!r} vs {dimension.key_fields!r}"
        )

    case_id = f"{snake_case(carrier.label)}_{snake_case(dimension.label)}"
    dependent = tuple(
        prop
        for prop in dimension.columns
        if prop not in dimension.key_fields
    )
    reserved = {
        RUN_ID_PROPERTY,
        CASE_ID_PROPERTY,
        *carrier.columns,
    }
    dimension_prefix = snake_case(dimension.label)
    bindings = tuple(
        PropertyBinding(
            logical_name=prop,
            folded_name=(
                f"{dimension_prefix}__{prop}" if prop in reserved else prop
            ),
        )
        for prop in dependent
    )
    names = PairNames(
        folded_label=f"mv_{case_id}",
        carrier_label=f"mv_{snake_case(carrier.label)}",
        dimension_label=f"mv_{snake_case(dimension.label)}",
        normalized_join_type=f"join_{join.relationship_type.lower()}",
    )
    expected = EXPECTED_CASE_METRICS.get(case_id)
    default_update = DEFAULT_UPDATE_PROPERTIES.get(case_id)
    if expected is None or default_update is None:
        raise ExperimentValidationError(
            f"missing Northwind fixture metadata for {case_id}"
        )
    pair = PairSpec(
        case_id=case_id,
        carrier=carrier,
        dimension=dimension,
        join=join,
        carrier_is_source=carrier_is_source,
        fk_mapping=fk_mapping,
        dependent_properties=dependent,
        property_bindings=bindings,
        topology_fk_properties=topology_fk_properties_for(dimension),
        default_update_property=default_update,
        boundaries=build_boundary_specs(join, carrier, dimension),
        names=names,
        expected=expected,
    )
    if pair.default_update_property not in pair.allowed_update_properties:
        raise ExperimentValidationError(
            f"default update property {pair.default_update_property!r} is "
            f"not eligible for {case_id}"
        )
    return pair


CASES: dict[str, PairSpec] = {
    pair.case_id: pair
    for pair in (build_pair_spec(relationship) for relationship in RELATIONSHIPS)
}
CASE_ORDER = tuple(DEFAULT_UPDATE_PROPERTIES)


def validate_case_catalog() -> None:
    if set(CASES) != set(CASE_ORDER):
        raise ExperimentValidationError(
            "case catalog does not match the ten expected Northwind pairs: "
            f"actual={sorted(CASES)!r}, expected={sorted(CASE_ORDER)!r}"
        )
    if len(CASES) != len(RELATIONSHIPS):
        raise ExperimentValidationError(
            "each imported relationship must map to exactly one pair case"
        )
    reverse_cases = [
        pair.case_id for pair in CASES.values() if not pair.carrier_is_source
    ]
    if reverse_cases != ["employee_territory_employee"]:
        raise ExperimentValidationError(
            f"unexpected reverse-direction cases: {reverse_cases!r}"
        )
    for pair in CASES.values():
        if (
            pair.expected.redundant_slots
            != pair.expected.redundant_rows
            * len(pair.dependent_properties)
        ):
            raise ExperimentValidationError(
                f"{pair.case_id} fixture redundant-slot arithmetic is wrong"
            )
        aliases = [
            binding.folded_name for binding in pair.property_bindings
        ]
        if len(aliases) != len(set(aliases)):
            raise ExperimentValidationError(
                f"{pair.case_id} has duplicate folded property aliases"
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
            "cost for direct Northwind FK pairs in isolated shadow graphs."
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
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--case",
        dest="case_id",
        choices=CASE_ORDER,
        help=(
            "run only this pair case; when omitted, all ten cases run"
        ),
    )
    selection.add_argument(
        "--all-cases",
        "--all",
        action="store_true",
        help=(
            "explicitly run all ten cases sequentially "
            "(this is also the default)"
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
        help="validate all pair metadata against the local CSV files only",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "dataset",
        help="Northwind CSV directory used by --validate-cases-only",
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
            "dimension non-key/non-topology property; omitted uses the case "
            "default; custom properties require --case"
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
        "--output-json",
        type=Path,
        default=DEFAULT_OUTPUT_JSON,
        help=(
            "path for the JSON report; defaults to "
            "case_study/query/northwind_fd_experiment_results.json"
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
                "another local Northwind FD experiment is already running"
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
            f"{quote_ident(key)}: {expression}"
            for key, expression in entries
        )
        + "}"
    )


def equality_predicate(left: str, right: str) -> str:
    return f"(({left} = {right}) OR ({left} IS NULL AND {right} IS NULL))"


def join_pattern(
    pair: PairSpec,
    carrier_alias: str = "carrier",
    dimension_alias: str = "dimension",
    relationship_alias: str = "join_rel",
    include_labels: bool = True,
) -> str:
    carrier = (
        f"{carrier_alias}:{quote_ident(pair.carrier.label)}"
        if include_labels
        else carrier_alias
    )
    dimension = (
        f"{dimension_alias}:{quote_ident(pair.dimension.label)}"
        if include_labels
        else dimension_alias
    )
    relationship = (
        f"{relationship_alias}:{quote_ident(pair.join.relationship_type)}"
    )
    if pair.carrier_is_source:
        return f"({carrier})-[{relationship}]->({dimension})"
    return f"({dimension})-[{relationship}]->({carrier})"


def normalized_join_pattern(
    pair: PairSpec,
    carrier_alias: str = "carrier",
    dimension_alias: str = "dimension",
    relationship_alias: str = "join_rel",
    include_labels: bool = True,
    include_relationship_type: bool = True,
) -> str:
    carrier = (
        f"{carrier_alias}:{quote_ident(pair.names.carrier_label)}"
        if include_labels
        else carrier_alias
    )
    dimension = (
        f"{dimension_alias}:{quote_ident(pair.names.dimension_label)}"
        if include_labels
        else dimension_alias
    )
    relationship = (
        (
            f"{relationship_alias}:"
            f"{quote_ident(pair.names.normalized_join_type)}"
        )
        if include_relationship_type
        else relationship_alias
    )
    if pair.carrier_is_source:
        return f"({carrier})-[{relationship}]->({dimension})"
    return f"({dimension})-[{relationship}]->({carrier})"


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
    relationship = (
        f"{relationship_alias}:{quote_ident(relationship_type)}"
    )
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
        (
            f"{node_alias}.{quote_ident(prop)} = "
            f"{expression}"
        )
        for prop, expression in zip(
            properties,
            expressions,
            strict=True,
        )
    )


def carrier_key_match_predicate(
    pair: PairSpec,
    shadow_alias: str,
    carrier_alias: str,
) -> str:
    return strict_node_key_predicates(
        shadow_alias,
        pair.carrier.key_fields,
        tuple(
            f"{carrier_alias}.{quote_ident(prop)}"
            for prop in pair.carrier.key_fields
        ),
    )


def folded_dimension_key_expressions(
    pair: PairSpec,
    alias: str,
) -> tuple[str, ...]:
    return tuple(
        f"{alias}.{quote_ident(prop)}"
        for prop in pair.carrier_fk_fields
    )


def dimension_key_expressions(
    pair: PairSpec,
    alias: str,
) -> tuple[str, ...]:
    return tuple(
        f"{alias}.{quote_ident(prop)}"
        for prop in pair.dimension.key_fields
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


def sorted_keys(keys: Iterable[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return sorted(
        keys,
        key=lambda key: tuple(domain_value_token(value) for value in key),
    )


def transaction_record(
    transaction: Any,
    query: str,
    context: str,
    **parameters: Any,
) -> tuple[Any, Any]:
    return one_record(
        transaction.run(query, parameters),
        context,
    )


def node_projection(alias: str, properties: Sequence[str]) -> str:
    return cypher_map(
        tuple(
            (prop, f"{alias}.{quote_ident(prop)}")
            for prop in properties
        )
    )


def canonical_join_rows(rows: Sequence[JoinRow]) -> list[dict[str, Any]]:
    canonical = [
        {
            "carrier_key": list(row.carrier_key),
            "dimension_key": list(row.dimension_key),
            "carrier_values": row.carrier_values,
            "dimension_values": row.dimension_values,
        }
        for row in rows
    ]
    return sorted(
        canonical,
        key=lambda row: (
            tuple(
                domain_value_token(value)
                for value in row["carrier_key"]
            ),
            tuple(
                domain_value_token(value)
                for value in row["dimension_key"]
            ),
        ),
    )


def join_rows_sha256(rows: Sequence[JoinRow]) -> str:
    return sha256_json(canonical_join_rows(rows))


def fetch_original_join_rows(
    session: Any,
    pair: PairSpec,
) -> list[JoinRow]:
    result = session.run(
        f"""
        MATCH {join_pattern(pair)}
        RETURN
          {node_projection("carrier", pair.carrier.columns)}
            AS carrier_values,
          {node_projection("dimension", pair.dimension.columns)}
            AS dimension_values
        """
    )
    records, _ = result_records_and_summary(result)
    rows: list[JoinRow] = []
    for record in records:
        carrier_values = dict(record["carrier_values"])
        dimension_values = dict(record["dimension_values"])
        rows.append(
            JoinRow(
                carrier_key=canonical_key(
                    carrier_values[prop]
                    for prop in pair.carrier.key_fields
                ),
                dimension_key=canonical_key(
                    dimension_values[prop]
                    for prop in pair.dimension.key_fields
                ),
                carrier_values=carrier_values,
                dimension_values=dimension_values,
            )
        )
    rows.sort(
        key=lambda row: tuple(
            domain_value_token(value) for value in row.carrier_key
        )
    )
    return rows


def fetch_full_dimension_rows(
    session: Any,
    pair: PairSpec,
) -> list[dict[str, Any]]:
    result = session.run(
        f"""
        MATCH (dimension:{quote_ident(pair.dimension.label)})
        RETURN {node_projection("dimension", pair.dimension.columns)}
          AS dimension_values
        """
    )
    records, _ = result_records_and_summary(result)
    rows = [dict(record["dimension_values"]) for record in records]
    rows.sort(
        key=lambda row: tuple(
            domain_value_token(row[prop])
            for prop in pair.dimension.key_fields
        )
    )
    return rows


def original_graph_sha256(session: Any) -> str:
    """Hash all non-experiment nodes and relationships.

    Copied boundary relationships touch a shadow endpoint and are therefore
    excluded while an experiment is running.  The hash still covers the
    complete original Northwind graph, not merely the selected pair.
    """

    node_result = session.run(
        """
        MATCH (node)
        WHERE NOT $artifact_label IN labels(node)
        RETURN labels(node) AS labels, properties(node) AS properties
        """,
        artifact_label=ARTIFACT_LABEL,
    )
    node_records, _ = result_records_and_summary(node_result)
    nodes = [
        {
            "labels": sorted(str(label) for label in record["labels"]),
            "properties": dict(record["properties"]),
        }
        for record in node_records
    ]
    nodes.sort(
        key=lambda item: json.dumps(
            item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    )

    relationship_result = session.run(
        """
        MATCH (source)-[relationship]->(target)
        WHERE NOT $artifact_label IN labels(source)
          AND NOT $artifact_label IN labels(target)
        RETURN
          labels(source) AS source_labels,
          properties(source) AS source_properties,
          type(relationship) AS relationship_type,
          properties(relationship) AS relationship_properties,
          labels(target) AS target_labels,
          properties(target) AS target_properties
        """,
        artifact_label=ARTIFACT_LABEL,
    )
    relationship_records, _ = result_records_and_summary(
        relationship_result
    )
    relationships = [
        {
            "source_labels": sorted(
                str(label) for label in record["source_labels"]
            ),
            "source_properties": dict(record["source_properties"]),
            "type": str(record["relationship_type"]),
            "properties": dict(record["relationship_properties"]),
            "target_labels": sorted(
                str(label) for label in record["target_labels"]
            ),
            "target_properties": dict(record["target_properties"]),
        }
        for record in relationship_records
    ]
    relationships.sort(
        key=lambda item: json.dumps(
            item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    )
    return sha256_json(
        {
            "nodes": nodes,
            "relationships": relationships,
        }
    )


def artifact_counts(session: Any) -> tuple[int, int]:
    record, _ = one_record(
        session.run(
            """
            MATCH (node)
            WHERE $artifact_label IN labels(node)
            OPTIONAL MATCH (node)-[relationship]-()
            RETURN
              count(DISTINCT node) AS nodes,
              count(DISTINCT relationship) AS relationships
            """,
            artifact_label=ARTIFACT_LABEL,
        ),
        "count experiment artifacts",
    )
    return int(record["nodes"]), int(record["relationships"])


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
    return [
        f"{record['run_id']} ({record['case_id']})"
        for record in records
    ]


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
                pair.names.carrier_label,
                pair.names.dimension_label,
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
            "nodes already use one or more reserved MV labels: "
            f"{labels!r}"
        )


def validate_original_pair_shape(
    session: Any,
    pair: PairSpec,
) -> tuple[int, int, int, int]:
    carrier_count = scalar_count(
        session,
        (
            f"MATCH (node:{quote_ident(pair.carrier.label)}) "
            "RETURN count(node) AS count"
        ),
        "count",
        f"count {pair.carrier.label} nodes",
    )
    dimension_count = scalar_count(
        session,
        (
            f"MATCH (node:{quote_ident(pair.dimension.label)}) "
            "RETURN count(node) AS count"
        ),
        "count",
        f"count {pair.dimension.label} nodes",
    )

    for table in (pair.carrier, pair.dimension):
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
        extra_properties = int(
            shape_record["nodes_with_extra_properties"]
        )
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
                f"carrier.{quote_ident(carrier_fk)}",
                f"dimension.{quote_ident(dimension_key)}",
            )
        )
        for carrier_fk, dimension_key in pair.fk_mapping
    ]
    mismatch_predicate = " OR ".join(mismatch_terms) or "false"
    record, _ = one_record(
        session.run(
            f"""
            MATCH (carrier:{quote_ident(pair.carrier.label)})
            OPTIONAL MATCH {join_pattern(pair)}
            WITH
              carrier,
              count(join_rel) AS degree,
              collect(dimension) AS dimensions
            RETURN
              count(carrier) AS carrier_count,
              sum(CASE WHEN degree = 1 THEN 0 ELSE 1 END)
                AS invalid_degrees,
              sum(
                CASE
                  WHEN degree = 1
                   AND NOT ({mismatch_predicate.replace("dimension.", "dimensions[0].")})
                  THEN 0 ELSE 1
                END
              ) AS fk_mismatches
            """
        ),
        f"validate {pair.case_id} join cardinality",
    )
    invalid_degrees = int(record["invalid_degrees"])
    fk_mismatches = int(record["fk_mismatches"])
    if int(record["carrier_count"]) != carrier_count:
        raise ExperimentValidationError(
            f"{pair.case_id}: carrier count changed during validation"
        )
    if invalid_degrees or fk_mismatches:
        raise ExperimentValidationError(
            f"{pair.case_id}: every {pair.carrier.label} must have exactly "
            f"one {pair.join.relationship_type} edge to the FK-matching "
            f"{pair.dimension.label}; invalid degrees={invalid_degrees}, "
            f"FK mismatches={fk_mismatches}"
        )

    join_record, _ = one_record(
        session.run(
            f"""
            MATCH {join_pattern(pair)}
            RETURN
              count(join_rel) AS join_rows,
              count(DISTINCT dimension) AS participating_dimensions,
              sum(CASE WHEN size(keys(join_rel)) = 0 THEN 0 ELSE 1 END)
                AS relationships_with_properties,
              sum(
                CASE
                  WHEN $run_property IN keys(carrier)
                    OR $case_property IN keys(carrier)
                    OR $run_property IN keys(dimension)
                    OR $case_property IN keys(dimension)
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
    participating = int(join_record["participating_dimensions"])
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

    expected = pair.expected
    problems: list[str] = []
    if dimension_count != expected.dimension_rows:
        problems.append(
            f"|B|={dimension_count}, expected {expected.dimension_rows}"
        )
    if join_rows != expected.join_rows:
        problems.append(f"J={join_rows}, expected {expected.join_rows}")
    if participating != expected.participating_dimensions:
        problems.append(
            f"D={participating}, expected "
            f"{expected.participating_dimensions}"
        )
    if carrier_count != join_rows:
        problems.append(
            f"carrier rows={carrier_count}, but join rows={join_rows}"
        )
    if problems:
        raise ExperimentValidationError(
            f"{pair.case_id} does not match the validated Northwind fixture: "
            + "; ".join(problems)
        )
    return carrier_count, dimension_count, join_rows, participating


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
            "carrier" if boundary.owner == "carrier" else "dimension"
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
                MATCH {join_pattern(pair)}
                WITH DISTINCT {owner_alias}
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
    folded_total = sum(item.folded_count for item in expectations)
    if folded_total != pair.expected.complete_folded_boundary_edges:
        raise ExperimentValidationError(
            f"{pair.case_id}: complete boundary total={folded_total}, "
            f"expected {pair.expected.complete_folded_boundary_edges}"
        )
    return tuple(expectations)


def build_case_baseline(
    session: Any,
    pair: PairSpec,
    topology_mode: str,
) -> tuple[CaseBaseline, list[JoinRow], list[dict[str, Any]]]:
    carrier_rows, dimension_rows, join_count, participating = (
        validate_original_pair_shape(session, pair)
    )
    join_rows = fetch_original_join_rows(session, pair)
    if len(join_rows) != join_count:
        raise ExperimentValidationError(
            f"{pair.case_id}: join fetch returned {len(join_rows)} rows; "
            f"expected {join_count}"
        )
    carrier_keys = [row.carrier_key for row in join_rows]
    if len(carrier_keys) != len(set(carrier_keys)):
        raise ExperimentValidationError(
            f"{pair.case_id}: duplicate carrier keys in source join"
        )
    dimension_rows_full = fetch_full_dimension_rows(session, pair)
    baseline = CaseBaseline(
        carrier_rows=carrier_rows,
        dimension_rows=dimension_rows,
        join_rows=join_count,
        participating_dimensions=participating,
        join_sha256=join_rows_sha256(join_rows),
        source_sha256=original_graph_sha256(session),
        boundary_expectations=boundary_expected_counts(
            session,
            pair,
            topology_mode,
        ),
    )
    return baseline, join_rows, dimension_rows_full


def relevant_relationships(pair: PairSpec) -> tuple[RelationshipSpec, ...]:
    labels = {pair.carrier.label, pair.dimension.label}
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
    record, _ = one_record(
        session.run(
            """
            MATCH (owner)-[relationship]-()
            WHERE (
              $carrier_label IN labels(owner)
              OR $dimension_label IN labels(owner)
            )
              AND NOT type(relationship) IN $allowed_types
            RETURN count(DISTINCT relationship) AS count
            """,
            carrier_label=pair.carrier.label,
            dimension_label=pair.dimension.label,
            allowed_types=allowed_types,
        ),
        f"check unsupported topology for {pair.case_id}",
    )
    unexpected = int(record["count"])
    if unexpected:
        raise ExperimentValidationError(
            f"{pair.case_id}: {unexpected} relationships adjacent to "
            f"{pair.carrier.label}/{pair.dimension.label} are not declared "
            "by import_data.py and cannot be copied safely"
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
        incident_record, _ = one_record(
            session.run(
                """
                MATCH (owner)-[relationship]-()
                WHERE (
                  $carrier_label IN labels(owner)
                  OR $dimension_label IN labels(owner)
                )
                  AND type(relationship) = $relationship_type
                RETURN count(DISTINCT relationship) AS count
                """,
                carrier_label=pair.carrier.label,
                dimension_label=pair.dimension.label,
                relationship_type=relationship.relationship_type,
            ),
            f"count incident {relationship.relationship_type}",
        )
        incident = int(incident_record["count"])
        if incident != exact:
            raise ExperimentValidationError(
                f"{pair.case_id}: {relationship.relationship_type} has "
                f"{incident - exact} relationship(s) with an unsupported "
                "direction or endpoint label"
            )
        parallel = scalar_count(
            session,
            f"""
            MATCH
              (source:{quote_ident(relationship.source_label)})
              -[rel:{quote_ident(relationship.relationship_type)}]->
              (target:{quote_ident(relationship.target_label)})
            WITH source, target, count(rel) AS multiplicity
            WHERE multiplicity > 1
            RETURN count(*) AS count
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
    return safe_schema_token(
        f"{SCHEMA_PREFIX}_{pair.case_id}_{run_id}_{kind}"
    )


def case_schema_names(
    pair: PairSpec,
    run_id: str,
) -> dict[str, str]:
    return {
        kind: schema_name(pair, run_id, kind)
        for kind in (
            "folded_key",
            "carrier_key",
            "dimension_key",
            "folded_fk",
            "carrier_fk",
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
            (RUN_ID_PROPERTY, *pair.carrier.key_fields),
        ),
        (
            names["carrier_key"],
            pair.names.carrier_label,
            (RUN_ID_PROPERTY, *pair.carrier.key_fields),
        ),
        (
            names["dimension_key"],
            pair.names.dimension_label,
            (RUN_ID_PROPERTY, *pair.dimension.key_fields),
        ),
    )
    index_definitions = (
        (
            names["folded_fk"],
            pair.names.folded_label,
            (RUN_ID_PROPERTY, *pair.carrier_fk_fields),
        ),
        (
            names["carrier_fk"],
            pair.names.carrier_label,
            (RUN_ID_PROPERTY, *pair.carrier_fk_fields),
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
        if (
            not constraint.created_by_experiment
            or constraint.name is None
        ):
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
            session.run(
                f"DROP INDEX {quote_ident(name)} IF EXISTS"
            ).consume()
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


def folded_dimension_projection(
    pair: PairSpec,
    alias: str,
) -> str:
    fk_by_dimension_key = {
        dimension_key: carrier_fk
        for carrier_fk, dimension_key in pair.fk_mapping
    }
    entries: list[tuple[str, str]] = [
        (
            dimension_key,
            f"{alias}.{quote_ident(fk_by_dimension_key[dimension_key])}",
        )
        for dimension_key in pair.dimension.key_fields
    ]
    entries.extend(
        (
            binding.logical_name,
            f"{alias}.{quote_ident(binding.folded_name)}",
        )
        for binding in pair.property_bindings
    )
    return cypher_map(entries)


def folded_dependent_property_map(
    pair: PairSpec,
    dimension_alias: str,
) -> str:
    return cypher_map(
        tuple(
            (
                binding.folded_name,
                f"{dimension_alias}.{quote_ident(binding.logical_name)}",
            )
            for binding in pair.property_bindings
        )
    )


def dimension_property_map_from_folded(
    pair: PairSpec,
    folded_alias: str,
) -> str:
    fk_by_dimension_key = {
        dimension_key: carrier_fk
        for carrier_fk, dimension_key in pair.fk_mapping
    }
    entries: list[tuple[str, str]] = [
        (
            dimension_key,
            f"{folded_alias}.{quote_ident(fk_by_dimension_key[dimension_key])}",
        )
        for dimension_key in pair.dimension.key_fields
    ]
    entries.extend(
        (
            binding.logical_name,
            f"{folded_alias}.{quote_ident(binding.folded_name)}",
        )
        for binding in pair.property_bindings
    )
    return cypher_map(entries)


def create_shadow_projection(
    session: Any,
    pair: PairSpec,
    run_id: str,
    topology_mode: str,
) -> dict[str, Any]:
    parameters = case_parameters(pair, run_id)
    transaction = session.begin_transaction()
    boundary_counts: dict[str, int] = {}
    try:
        node_record, _ = transaction_record(
            transaction,
            f"""
            MATCH {join_pattern(pair)}
            CREATE (
              shadow:{quote_ident(ARTIFACT_LABEL)}
                    :{quote_ident(pair.names.folded_label)}
            )
            SET shadow = properties(carrier)
            SET shadow += {folded_dependent_property_map(pair, "dimension")}
            SET shadow.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
                shadow.{quote_ident(CASE_ID_PROPERTY)} = $case_id
            RETURN count(shadow) AS count
            """,
            f"create folded nodes for {pair.case_id}",
            **parameters,
        )
        node_count = int(node_record["count"])

        if topology_mode == "complete":
            for boundary in pair.boundaries:
                owner_alias = (
                    "carrier"
                    if boundary.owner == "carrier"
                    else "dimension"
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
                boundary_record, _ = transaction_record(
                    transaction,
                    f"""
                    MATCH {join_pattern(pair)}
                    MATCH {source_pattern}
                    MATCH (
                      shadow:{quote_ident(ARTIFACT_LABEL)}
                            :{quote_ident(pair.names.folded_label)}
                      {artifact_identity_map()}
                    )
                    WHERE {
                        carrier_key_match_predicate(
                            pair,
                            "shadow",
                            "carrier",
                        )
                    }
                    CREATE {copy_pattern}
                    SET copy_rel = properties(source_rel)
                    RETURN count(copy_rel) AS count
                    """,
                    f"copy folded boundary {boundary.boundary_id}",
                    **parameters,
                )
                boundary_counts[boundary.boundary_id] = int(
                    boundary_record["count"]
                )
        transaction.commit()
    except BaseException:
        try:
            transaction.rollback()
        except BaseException:
            pass
        raise
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
    carriers = count_case_nodes(
        session,
        pair,
        run_id,
        pair.names.carrier_label,
    )
    dimensions = count_case_nodes(
        session,
        pair,
        run_id,
        pair.names.dimension_label,
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
          AND carrier.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND carrier.{quote_ident(CASE_ID_PROPERTY)} = $case_id
          AND dimension.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND dimension.{quote_ident(CASE_ID_PROPERTY)} = $case_id
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
            RETURN count(DISTINCT relationship) AS count
            """,
            "count",
            f"count copied boundary {boundary.boundary_id}",
            relationship_type=boundary.copy_type,
            **case_parameters(pair, run_id),
        )
    return ShadowState(
        folded_carriers=folded,
        normalized_carriers=carriers,
        normalized_dimensions=dimensions,
        normalized_join_relationships=normalized_relationships,
        boundary_relationships=boundary_counts,
    )


def fetch_folded_join_rows(
    session: Any,
    pair: PairSpec,
    run_id: str,
) -> list[JoinRow]:
    result = session.run(
        f"""
        MATCH (
          shadow:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(pair.names.folded_label)}
          {artifact_identity_map()}
        )
        RETURN
          {node_projection("shadow", pair.carrier.columns)}
            AS carrier_values,
          {folded_dimension_projection(pair, "shadow")}
            AS dimension_values
        """,
        **case_parameters(pair, run_id),
    )
    records, _ = result_records_and_summary(result)
    rows: list[JoinRow] = []
    for record in records:
        carrier_values = dict(record["carrier_values"])
        dimension_values = dict(record["dimension_values"])
        rows.append(
            JoinRow(
                carrier_key=canonical_key(
                    carrier_values[prop]
                    for prop in pair.carrier.key_fields
                ),
                dimension_key=canonical_key(
                    dimension_values[prop]
                    for prop in pair.dimension.key_fields
                ),
                carrier_values=carrier_values,
                dimension_values=dimension_values,
            )
        )
    rows.sort(
        key=lambda row: tuple(
            domain_value_token(value) for value in row.carrier_key
        )
    )
    return rows


def fetch_normalized_join_rows(
    session: Any,
    pair: PairSpec,
    run_id: str,
) -> list[JoinRow]:
    result = session.run(
        f"""
        MATCH {normalized_join_pattern(pair)}
        WHERE carrier.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND carrier.{quote_ident(CASE_ID_PROPERTY)} = $case_id
          AND dimension.{quote_ident(RUN_ID_PROPERTY)} = $run_id
          AND dimension.{quote_ident(CASE_ID_PROPERTY)} = $case_id
        RETURN
          {node_projection("carrier", pair.carrier.columns)}
            AS carrier_values,
          {node_projection("dimension", pair.dimension.columns)}
            AS dimension_values
        """,
        **case_parameters(pair, run_id),
    )
    records, _ = result_records_and_summary(result)
    rows: list[JoinRow] = []
    for record in records:
        carrier_values = dict(record["carrier_values"])
        dimension_values = dict(record["dimension_values"])
        rows.append(
            JoinRow(
                carrier_key=canonical_key(
                    carrier_values[prop]
                    for prop in pair.carrier.key_fields
                ),
                dimension_key=canonical_key(
                    dimension_values[prop]
                    for prop in pair.dimension.key_fields
                ),
                carrier_values=carrier_values,
                dimension_values=dimension_values,
            )
        )
    rows.sort(
        key=lambda row: tuple(
            domain_value_token(value) for value in row.carrier_key
        )
    )
    return rows


def validate_artifact_relationship_types(
    session: Any,
    pair: PairSpec,
    run_id: str,
) -> None:
    record, _ = one_record(
        session.run(
            f"""
            MATCH (
              node:{quote_ident(ARTIFACT_LABEL)}
              {artifact_identity_map()}
            )-[relationship]-()
            WHERE NOT type(relationship) IN $allowed_types
            RETURN count(DISTINCT relationship) AS count
            """,
            allowed_types=list(pair.all_copy_relationship_types),
            **case_parameters(pair, run_id),
        ),
        f"validate artifact relationship types for {pair.case_id}",
    )
    unexpected = int(record["count"])
    if unexpected:
        raise ExperimentStateError(
            f"{pair.case_id}: {unexpected} unexpected relationship(s) touch "
            f"run {run_id}; cleanup is fail-closed"
        )


def count_all_artifact_relationships(
    session: Any,
    pair: PairSpec,
    run_id: str,
) -> int:
    return scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )-[relationship]-()
        RETURN count(DISTINCT relationship) AS count
        """,
        "count",
        f"count all artifact relationships for {pair.case_id}",
        **case_parameters(pair, run_id),
    )


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
            pair.names.carrier_label,
            pair.names.dimension_label,
        ]
    )
    expected_label_sets = (
        [[ARTIFACT_LABEL, pair.names.folded_label]]
        if phase == "folded"
        else [
            [ARTIFACT_LABEL, pair.names.carrier_label],
            [ARTIFACT_LABEL, pair.names.dimension_label],
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


def normalized_owner_key_predicate(
    pair: PairSpec,
    boundary: BoundaryEdgeSpec,
    shadow_alias: str,
    original_alias: str,
) -> str:
    table = (
        pair.carrier
        if boundary.owner == "carrier"
        else pair.dimension
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
        "carrier" if boundary.owner == "carrier" else "dimension"
    )
    owner_table = (
        pair.carrier
        if boundary.owner == "carrier"
        else pair.dimension
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
        WHERE {carrier_key_match_predicate(pair, "shadow", "carrier")}
        """
    else:
        shadow_label = (
            pair.names.carrier_label
            if boundary.owner == "carrier"
            else pair.names.dimension_label
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
            OPTIONAL MATCH {copy_pattern}
            WITH
              source_rel,
              shadow,
              [rel IN collect(copy_rel) WHERE rel IS NOT NULL] AS copies
            RETURN
              count(*) AS expected,
              sum(CASE WHEN size(copies) = 1 THEN 0 ELSE 1 END)
                AS invalid_multiplicity,
              sum(
                CASE
                  WHEN size(copies) = 1
                   AND properties(copies[0]) = properties(source_rel)
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
        or state.folded_carriers != baseline.join_rows
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
    validate_artifact_node_labels(
        session,
        pair,
        run_id,
        phase="folded",
        expected_total=baseline.join_rows,
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
    rows = fetch_folded_join_rows(session, pair, run_id)
    if join_rows_sha256(rows) != baseline.join_sha256:
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
) -> ShadowState:
    state = inspect_shadow_state(session, pair, run_id)
    expected_boundaries = {
        expectation.boundary_id: expectation.normalized_count
        for expectation in baseline.boundary_expectations
    }
    if (
        state.phase != "normalized"
        or state.normalized_carriers != baseline.join_rows
        or state.normalized_dimensions
        != baseline.participating_dimensions
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
    validate_artifact_node_labels(
        session,
        pair,
        run_id,
        phase="normalized",
        expected_total=(
            baseline.join_rows + baseline.participating_dimensions
        ),
    )
    expected_relationship_total = (
        baseline.join_rows + sum(expected_boundaries.values())
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
    rows = fetch_normalized_join_rows(session, pair, run_id)
    if join_rows_sha256(rows) != baseline.join_sha256:
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


def remove_dependent_clause(
    pair: PairSpec,
    alias: str,
) -> str:
    return ", ".join(
        f"{alias}.{quote_ident(binding.folded_name)}"
        for binding in pair.property_bindings
    )


def set_folded_dependent_clause(
    pair: PairSpec,
    carrier_alias: str,
    dimension_alias: str,
) -> str:
    return ", ".join(
        (
            f"{carrier_alias}.{quote_ident(binding.folded_name)} = "
            f"{dimension_alias}.{quote_ident(binding.logical_name)}"
        )
        for binding in pair.property_bindings
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
) -> TimingSample:
    parameters = case_parameters(pair, run_id)
    totals = MutationTotals()
    started_ns = perf_counter_ns()
    transaction = session.begin_transaction()
    try:
        dimension_record, summary = transaction_record(
            transaction,
            f"""
            MATCH (
              folded:{quote_ident(ARTIFACT_LABEL)}
                    :{quote_ident(pair.names.folded_label)}
              {artifact_identity_map()}
            )
            WITH
              [{", ".join(f"folded.{quote_ident(prop)}" for prop in pair.carrier_fk_fields)}]
                AS dimension_key,
              head(collect(folded)) AS representative
            CREATE (
              dimension:{quote_ident(ARTIFACT_LABEL)}
                       :{quote_ident(pair.names.dimension_label)}
            )
            SET dimension =
              {dimension_property_map_from_folded(pair, "representative")}
            SET dimension.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
                dimension.{quote_ident(CASE_ID_PROPERTY)} = $case_id
            RETURN count(dimension) AS count
            """,
            f"create normalized dimensions for {pair.case_id}",
            **parameters,
        )
        totals.add_summary(summary)
        dimension_count = int(dimension_record["count"])

        carrier_record, summary = transaction_record(
            transaction,
            f"""
            MATCH (
              carrier:{quote_ident(ARTIFACT_LABEL)}
                     :{quote_ident(pair.names.folded_label)}
              {artifact_identity_map()}
            )
            SET carrier:{quote_ident(pair.names.carrier_label)}
            REMOVE {remove_dependent_clause(pair, "carrier")}
            RETURN count(carrier) AS count
            """,
            f"prepare normalized carriers for {pair.case_id}",
            **parameters,
        )
        totals.add_summary(summary)
        carrier_count = int(carrier_record["count"])

        normalized_create_pattern = normalized_join_pattern(
            pair,
            relationship_alias="normalized_rel",
            include_labels=False,
        )
        join_record, summary = transaction_record(
            transaction,
            f"""
            MATCH (
              carrier:{quote_ident(ARTIFACT_LABEL)}
                     :{quote_ident(pair.names.carrier_label)}
              {artifact_identity_map()}
            )
            MATCH (
              dimension:{quote_ident(ARTIFACT_LABEL)}
                       :{quote_ident(pair.names.dimension_label)}
              {artifact_identity_map()}
            )
            USING INDEX dimension:{quote_ident(pair.names.dimension_label)}(
              {", ".join(quote_ident(prop) for prop in (RUN_ID_PROPERTY, *pair.dimension.key_fields))}
            )
            WHERE {
                strict_node_key_predicates(
                    "dimension",
                    pair.dimension.key_fields,
                    folded_dimension_key_expressions(pair, "carrier"),
                )
            }
            CREATE {normalized_create_pattern}
            RETURN count(normalized_rel) AS count
            """,
            f"create normalized join for {pair.case_id}",
            **parameters,
        )
        totals.add_summary(summary)
        normalized_join_count = int(join_record["count"])

        boundary_results: dict[str, dict[str, int]] = {}
        if topology_mode == "complete":
            for boundary in pair.boundaries:
                if boundary.owner != "dimension":
                    continue
                normalized_boundary_join = normalized_join_pattern(pair)
                old_pattern = boundary_pattern(
                    boundary,
                    "carrier",
                    "external",
                    "old_copy",
                    copied=True,
                )
                create_pattern = boundary_pattern(
                    boundary,
                    "dimension",
                    "external",
                    "new_copy",
                    copied=True,
                    include_other_label=False,
                )
                create_record, summary = transaction_record(
                    transaction,
                    f"""
                    MATCH {normalized_boundary_join}
                    WHERE carrier.{quote_ident(RUN_ID_PROPERTY)} = $run_id
                      AND carrier.{quote_ident(CASE_ID_PROPERTY)} = $case_id
                      AND dimension.{quote_ident(RUN_ID_PROPERTY)} = $run_id
                      AND dimension.{quote_ident(CASE_ID_PROPERTY)} = $case_id
                    MATCH {old_pattern}
                    WITH DISTINCT
                      dimension,
                      external,
                      properties(old_copy) AS relationship_properties
                    CREATE {create_pattern}
                    SET new_copy = relationship_properties
                    RETURN count(new_copy) AS count
                    """,
                    (
                        "create normalized dimension boundary "
                        f"{boundary.boundary_id}"
                    ),
                    **parameters,
                )
                totals.add_summary(summary)

                delete_record, summary = transaction_record(
                    transaction,
                    f"""
                    MATCH (
                      carrier:{quote_ident(ARTIFACT_LABEL)}
                             :{quote_ident(pair.names.carrier_label)}
                      {artifact_identity_map()}
                    )
                    MATCH {old_pattern}
                    DELETE old_copy
                    RETURN count(old_copy) AS count
                    """,
                    (
                        "delete folded dimension boundary "
                        f"{boundary.boundary_id}"
                    ),
                    **parameters,
                )
                totals.add_summary(summary)
                boundary_results[boundary.boundary_id] = {
                    "created": int(create_record["count"]),
                    "deleted": int(delete_record["count"]),
                }

        label_record, summary = transaction_record(
            transaction,
            f"""
            MATCH (
              carrier:{quote_ident(ARTIFACT_LABEL)}
                     :{quote_ident(pair.names.folded_label)}
                     :{quote_ident(pair.names.carrier_label)}
              {artifact_identity_map()}
            )
            REMOVE carrier:{quote_ident(pair.names.folded_label)}
            RETURN count(carrier) AS count
            """,
            f"finish normalized carrier labels for {pair.case_id}",
            **parameters,
        )
        totals.add_summary(summary)
        removed_folded_labels = int(label_record["count"])

        counter_validation_started_ns = perf_counter_ns()
        problems: list[str] = []
        if dimension_count != baseline.participating_dimensions:
            problems.append(
                f"created dimensions={dimension_count}, "
                f"expected {baseline.participating_dimensions}"
            )
        if carrier_count != baseline.join_rows:
            problems.append(
                f"carriers={carrier_count}, expected {baseline.join_rows}"
            )
        if normalized_join_count != baseline.join_rows:
            problems.append(
                f"join edges={normalized_join_count}, "
                f"expected {baseline.join_rows}"
            )
        if removed_folded_labels != baseline.join_rows:
            problems.append(
                f"removed folded labels={removed_folded_labels}, "
                f"expected {baseline.join_rows}"
            )
        expectation_by_id = {
            item.boundary_id: item
            for item in baseline.boundary_expectations
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
        counter_validation_started_ns - started_ns
        + finished_ns - commit_started_ns
    )
    wall_ms = measured_ns / 1_000_000
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
    )
    return sample


def refold_shadow_projection(
    session: Any,
    pair: PairSpec,
    run_id: str,
    baseline: CaseBaseline,
    topology_mode: str,
) -> None:
    state = inspect_shadow_state(session, pair, run_id)
    if state.phase != "normalized":
        raise ExperimentStateError(
            f"{pair.case_id}: cannot refold phase {state.phase!r}"
        )
    parameters = case_parameters(pair, run_id)
    transaction = session.begin_transaction()
    try:
        boundary_results: dict[str, dict[str, int]] = {}
        if topology_mode == "complete":
            for boundary in pair.boundaries:
                if boundary.owner != "dimension":
                    continue
                normalized_pattern = normalized_join_pattern(pair)
                dimension_copy_pattern = boundary_pattern(
                    boundary,
                    "dimension",
                    "external",
                    "dimension_copy",
                    copied=True,
                )
                carrier_copy_pattern = boundary_pattern(
                    boundary,
                    "carrier",
                    "external",
                    "carrier_copy",
                    copied=True,
                    include_other_label=False,
                )
                create_record, _ = transaction_record(
                    transaction,
                    f"""
                    MATCH {normalized_pattern}
                    WHERE carrier.{quote_ident(RUN_ID_PROPERTY)} = $run_id
                      AND carrier.{quote_ident(CASE_ID_PROPERTY)} = $case_id
                      AND dimension.{quote_ident(RUN_ID_PROPERTY)} = $run_id
                      AND dimension.{quote_ident(CASE_ID_PROPERTY)} = $case_id
                    MATCH {dimension_copy_pattern}
                    CREATE {carrier_copy_pattern}
                    SET carrier_copy = properties(dimension_copy)
                    RETURN count(carrier_copy) AS count
                    """,
                    f"refold boundary {boundary.boundary_id}",
                    **parameters,
                )
                delete_record, _ = transaction_record(
                    transaction,
                    f"""
                    MATCH (
                      dimension:{quote_ident(ARTIFACT_LABEL)}
                               :{quote_ident(pair.names.dimension_label)}
                      {artifact_identity_map()}
                    )
                    MATCH {dimension_copy_pattern}
                    DELETE dimension_copy
                    RETURN count(dimension_copy) AS count
                    """,
                    (
                        "delete normalized boundary during refold "
                        f"{boundary.boundary_id}"
                    ),
                    **parameters,
                )
                boundary_results[boundary.boundary_id] = {
                    "created": int(create_record["count"]),
                    "deleted": int(delete_record["count"]),
                }

        normalized_pattern = normalized_join_pattern(pair)
        carrier_record, _ = transaction_record(
            transaction,
            f"""
            MATCH {normalized_pattern}
            WHERE carrier.{quote_ident(RUN_ID_PROPERTY)} = $run_id
              AND carrier.{quote_ident(CASE_ID_PROPERTY)} = $case_id
              AND dimension.{quote_ident(RUN_ID_PROPERTY)} = $run_id
              AND dimension.{quote_ident(CASE_ID_PROPERTY)} = $case_id
            SET {set_folded_dependent_clause(pair, "carrier", "dimension")}
            SET carrier:{quote_ident(pair.names.folded_label)}
            RETURN
              count(carrier) AS carrier_count,
              count(DISTINCT dimension) AS dimension_count
            """,
            f"restore folded properties for {pair.case_id}",
            **parameters,
        )
        carrier_count = int(carrier_record["carrier_count"])
        dimension_count = int(carrier_record["dimension_count"])

        join_record, _ = transaction_record(
            transaction,
            f"""
            MATCH {normalized_pattern}
            WHERE carrier.{quote_ident(RUN_ID_PROPERTY)} = $run_id
              AND carrier.{quote_ident(CASE_ID_PROPERTY)} = $case_id
              AND dimension.{quote_ident(RUN_ID_PROPERTY)} = $run_id
              AND dimension.{quote_ident(CASE_ID_PROPERTY)} = $case_id
            DELETE join_rel
            RETURN count(join_rel) AS count
            """,
            f"delete normalized joins during refold for {pair.case_id}",
            **parameters,
        )
        deleted_joins = int(join_record["count"])

        dimension_record, _ = transaction_record(
            transaction,
            f"""
            MATCH (
              dimension:{quote_ident(ARTIFACT_LABEL)}
                       :{quote_ident(pair.names.dimension_label)}
              {artifact_identity_map()}
            )
            DELETE dimension
            RETURN count(dimension) AS count
            """,
            f"delete normalized dimensions for {pair.case_id}",
            **parameters,
        )
        deleted_dimensions = int(dimension_record["count"])

        label_record, _ = transaction_record(
            transaction,
            f"""
            MATCH (
              carrier:{quote_ident(ARTIFACT_LABEL)}
                     :{quote_ident(pair.names.folded_label)}
                     :{quote_ident(pair.names.carrier_label)}
              {artifact_identity_map()}
            )
            REMOVE carrier:{quote_ident(pair.names.carrier_label)}
            RETURN count(carrier) AS count
            """,
            f"finish refold labels for {pair.case_id}",
            **parameters,
        )
        removed_carrier_labels = int(label_record["count"])

        problems: list[str] = []
        expected_simple = {
            "carrier_count": baseline.join_rows,
            "dimension_count": baseline.participating_dimensions,
            "deleted_joins": baseline.join_rows,
            "deleted_dimensions": baseline.participating_dimensions,
            "removed_carrier_labels": baseline.join_rows,
        }
        actual_simple = {
            "carrier_count": carrier_count,
            "dimension_count": dimension_count,
            "deleted_joins": deleted_joins,
            "deleted_dimensions": deleted_dimensions,
            "removed_carrier_labels": removed_carrier_labels,
        }
        if actual_simple != expected_simple:
            problems.append(
                f"counts={actual_simple!r}, expected={expected_simple!r}"
            )
        expectation_by_id = {
            item.boundary_id: item
            for item in baseline.boundary_expectations
        }
        for boundary_id, counts in boundary_results.items():
            expectation = expectation_by_id[boundary_id]
            if counts["created"] != expectation.folded_count:
                problems.append(
                    f"{boundary_id} recreated={counts['created']}, "
                    f"expected {expectation.folded_count}"
                )
            if counts["deleted"] != expectation.normalized_count:
                problems.append(
                    f"{boundary_id} normalized deleted={counts['deleted']}, "
                    f"expected {expectation.normalized_count}"
                )
        if problems:
            raise ExperimentValidationError(
                f"{pair.case_id} refold counters failed: "
                + "; ".join(problems)
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
        pair,
        run_id,
        baseline,
        topology_mode,
    )


def cleanup_shadow_artifacts(
    session: Any,
    pair: PairSpec,
    run_id: str,
) -> CleanupResult:
    parameters = case_parameters(pair, run_id)
    transaction = session.begin_transaction()
    try:
        record, _ = transaction_record(
            transaction,
            f"""
            MATCH (
              node:{quote_ident(ARTIFACT_LABEL)}
              {artifact_identity_map()}
            )
            OPTIONAL MATCH (node)-[relationship]-()
            RETURN
              count(DISTINCT node) AS nodes,
              count(DISTINCT relationship) AS relationships,
              count(
                DISTINCT CASE
                  WHEN relationship IS NOT NULL
                   AND NOT type(relationship) IN $allowed_types
                  THEN relationship
                END
              ) AS unexpected_relationships
            """,
            f"inspect cleanup scope for {pair.case_id}",
            allowed_types=list(pair.all_copy_relationship_types),
            **parameters,
        )
        deleted_nodes = int(record["nodes"])
        deleted_relationships = int(record["relationships"])
        unexpected = int(record["unexpected_relationships"])
        if unexpected:
            raise ExperimentStateError(
                f"{pair.case_id}: cleanup refused to delete {unexpected} "
                "unexpected relationship(s) attached to run-scoped nodes"
            )
        if deleted_nodes:
            deleted_record, _ = transaction_record(
                transaction,
                f"""
                MATCH (
                  node:{quote_ident(ARTIFACT_LABEL)}
                  {artifact_identity_map()}
                )
                OPTIONAL MATCH (node)-[relationship]-()
                WITH
                  collect(DISTINCT relationship) AS relationships,
                  collect(DISTINCT node) AS nodes
                FOREACH (
                  relationship IN relationships | DELETE relationship
                )
                FOREACH (node IN nodes | DELETE node)
                RETURN
                  size(nodes) AS nodes,
                  size(relationships) AS relationships
                """,
                f"delete artifacts for {pair.case_id}",
                **parameters,
            )
            if (
                int(deleted_record["nodes"]) != deleted_nodes
                or int(deleted_record["relationships"])
                != deleted_relationships
            ):
                raise ExperimentValidationError(
                    f"{pair.case_id}: cleanup scope changed in transaction"
                )
        transaction.commit()
    except BaseException:
        try:
            transaction.rollback()
        except BaseException:
            pass
        raise

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


def calculate_redundancy(
    pair: PairSpec,
    rows: Sequence[JoinRow],
    baseline: CaseBaseline,
) -> dict[str, Any]:
    groups: dict[tuple[Any, ...], list[JoinRow]] = defaultdict(list)
    for row in rows:
        groups[row.dimension_key].append(row)

    for dimension_key, group in groups.items():
        representative = group[0].dimension_values
        for row in group[1:]:
            for prop in pair.dimension.columns:
                if (
                    domain_value_token(row.dimension_values[prop])
                    != domain_value_token(representative[prop])
                ):
                    raise ExperimentValidationError(
                        f"{pair.case_id}: FD violation for B key "
                        f"{dimension_key!r}; {prop!r} has inconsistent values"
                    )

    occurrences = len(rows)
    distinct_dimensions = len(groups)
    redundant_rows = occurrences - distinct_dimensions
    dependent_count = len(pair.dependent_properties)
    theoretical_slots = redundant_rows * dependent_count
    redundant_non_null_values = 0
    total_non_null_values = 0
    redundant_payload_bytes = 0
    total_payload_bytes = 0
    fanout_histogram: Counter[int] = Counter()

    for group in groups.values():
        fanout = len(group)
        fanout_histogram[fanout] += 1
        representative = group[0].dimension_values
        for prop in pair.dependent_properties:
            value = representative[prop]
            if value is None:
                continue
            payload = value_payload_bytes(value)
            total_non_null_values += fanout
            total_payload_bytes += fanout * payload
            redundant_non_null_values += fanout - 1
            redundant_payload_bytes += (fanout - 1) * payload

    if redundant_rows != pair.expected.redundant_rows:
        raise ExperimentValidationError(
            f"{pair.case_id}: redundant rows={redundant_rows}, "
            f"expected {pair.expected.redundant_rows}"
        )
    if theoretical_slots != pair.expected.redundant_slots:
        raise ExperimentValidationError(
            f"{pair.case_id}: redundant slots={theoretical_slots}, "
            f"expected {pair.expected.redundant_slots}"
        )

    boundary_folded = sum(
        item.folded_count for item in baseline.boundary_expectations
    )
    boundary_normalized = sum(
        item.normalized_count for item in baseline.boundary_expectations
    )
    metrics: dict[str, Any] = {
        "definition": {
            "A": pair.carrier.label,
            "B": pair.dimension.label,
            "join_tuple_count_J": occurrences,
            "participating_distinct_B_keys_D": distinct_dimensions,
            "full_B_relation_rows": baseline.dimension_rows,
            "redundant_B_tuple_copies": "J - D",
            "redundant_B_property_slots": (
                "(J - D) * (column_count(B) - key_length(B))"
            ),
        },
        "join_rows_J": occurrences,
        "full_dimension_rows": baseline.dimension_rows,
        "participating_dimension_rows_D": distinct_dimensions,
        "unused_dimension_rows": (
            baseline.dimension_rows - distinct_dimensions
        ),
        "redundant_dimension_tuple_copies": redundant_rows,
        "redundant_dimension_tuple_percentage_of_join": (
            100.0 * redundant_rows / occurrences if occurrences else 0.0
        ),
        "dimension_column_count": len(pair.dimension.columns),
        "dimension_key_length": len(pair.dimension.key_fields),
        "dependent_property_count": dependent_count,
        "theoretical_redundant_dependent_property_slots": theoretical_slots,
        "redundant_non_null_dependent_values": redundant_non_null_values,
        "total_non_null_dependent_values_in_folded_join": (
            total_non_null_values
        ),
        "redundant_non_null_value_percentage": (
            100.0
            * redundant_non_null_values
            / total_non_null_values
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
        "dimension_fanout_histogram": {
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
    if pair.case_id == "product_supplier":
        metrics.update(
            {
                "product_occurrences": occurrences,
                "distinct_suppliers": distinct_dimensions,
                "redundant_supplier_copies": redundant_rows,
                "redundant_supplier_copy_percentage": (
                    metrics[
                        "redundant_dimension_tuple_percentage_of_join"
                    ]
                ),
                "supplier_fanout_histogram": metrics[
                    "dimension_fanout_histogram"
                ],
            }
        )
    return metrics


def print_redundancy(pair: PairSpec, metrics: dict[str, Any]) -> None:
    histogram = ", ".join(
        f"{fanout} row(s) -> {count} B key(s)"
        for fanout, count in metrics[
            "dimension_fanout_histogram"
        ].items()
    )
    print(
        f"  Join tuples J: {metrics['join_rows_J']:,}; "
        "distinct participating B keys D: "
        f"{metrics['participating_dimension_rows_D']:,}; "
        f"full |B|: {metrics['full_dimension_rows']:,}"
    )
    print(
        "  Redundant B tuple copies (J-D): "
        f"{metrics['redundant_dimension_tuple_copies']:,} "
        f"({metrics['redundant_dimension_tuple_percentage_of_join']:.3f}%)"
    )
    print(
        "  Redundant property slots "
        "((J-D) * (columns(B)-key_length(B))): "
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
    print(f"  B fanout histogram: {histogram}")


def build_active_domain_update_workload(
    pair: PairSpec,
    joined_rows: Sequence[JoinRow],
    full_dimension_rows: Sequence[dict[str, Any]],
    property_name: str,
) -> UpdateWorkload:
    if property_name not in pair.allowed_update_properties:
        raise ExperimentValidationError(
            f"{pair.case_id}: {property_name!r} is not an eligible update "
            "property. Choose a dimension non-key/non-topology property from "
            f"{list(pair.allowed_update_properties)!r}"
        )
    folded_property = pair.binding_by_logical_name[property_name]
    originals: dict[tuple[Any, ...], Any] = {}
    for row in joined_rows:
        value = row.dimension_values[property_name]
        existing = originals.get(row.dimension_key, value)
        if (
            row.dimension_key in originals
            and domain_value_token(existing) != domain_value_token(value)
        ):
            raise ExperimentValidationError(
                f"{pair.case_id}: {property_name} violates the FD for "
                f"B key {row.dimension_key!r}"
            )
        originals[row.dimension_key] = value

    null_keys = [
        key for key, value in originals.items() if value is None
    ]
    if null_keys:
        raise ExperimentValidationError(
            f"{pair.case_id}: {property_name} is NULL for "
            f"{len(null_keys)} participating B tuple(s); NULL creation/removal "
            "must be measured as a separate workload"
        )

    domain_source = [row[property_name] for row in full_dimension_rows]
    null_domain_count = sum(value is None for value in domain_source)
    if null_domain_count:
        raise ExperimentValidationError(
            f"{pair.case_id}: {property_name} contains NULL in "
            f"{null_domain_count} of {len(domain_source)} full B rows; choose "
            "another property or define a separate NULL workload"
        )
    value_types = {
        f"{type(value).__module__}.{type(value).__qualname__}"
        for value in domain_source
    }
    if len(value_types) != 1:
        raise ExperimentValidationError(
            f"{pair.case_id}: {property_name} has mixed active-domain types: "
            f"{sorted(value_types)!r}"
        )
    value_type = next(iter(value_types))
    values_by_token = {
        domain_value_token(value): value for value in domain_source
    }
    ordered_tokens = sorted(values_by_token)
    active_domain = [
        values_by_token[token] for token in ordered_tokens
    ]
    if len(active_domain) < 2:
        raise ExperimentValidationError(
            f"{pair.case_id}: {property_name} active domain has only "
            f"{len(active_domain)} distinct value(s); at least two are needed"
        )
    successor_by_token = {
        token: active_domain[(index + 1) % len(active_domain)]
        for index, token in enumerate(ordered_tokens)
    }

    updates: list[dict[str, Any]] = []
    restores: list[dict[str, Any]] = []
    mapping_tokens: list[dict[str, Any]] = []
    for key in sorted_keys(originals):
        original = originals[key]
        original_token = domain_value_token(original)
        if original_token not in successor_by_token:
            raise ExperimentValidationError(
                f"{pair.case_id}: joined {property_name} value is absent from "
                "the full B active domain"
            )
        updated = successor_by_token[original_token]
        if domain_value_token(updated) == original_token:
            raise ExperimentValidationError(
                f"{pair.case_id}: update rotation would not change key {key!r}"
            )
        updates.append({"key": list(key), "value": updated})
        restores.append({"key": list(key), "value": original})
        mapping_tokens.append(
            {
                "key": [domain_value_token(value) for value in key],
                "original": original_token,
                "updated": domain_value_token(updated),
            }
        )

    return UpdateWorkload(
        property_name=property_name,
        folded_property_name=folded_property,
        originals=originals,
        updates=updates,
        restores=restores,
        active_domain_size=len(active_domain),
        active_domain_source_tuple_count=len(domain_source),
        active_domain_sha256=sha256_json(ordered_tokens),
        mapping_sha256=sha256_json(mapping_tokens),
        value_type=value_type,
        logical_original_payload_bytes=sum(
            value_payload_bytes(value) for value in originals.values()
        ),
        logical_updated_payload_bytes=sum(
            value_payload_bytes(row["value"]) for row in updates
        ),
    )


def calculate_update_payload_metrics(
    joined_rows: Sequence[JoinRow],
    workload: UpdateWorkload,
) -> dict[str, Any]:
    updated_by_key = {
        tuple(row["key"]): row["value"] for row in workload.updates
    }
    folded_original = 0
    folded_updated = 0
    for row in joined_rows:
        original = row.dimension_values[workload.property_name]
        expected = workload.originals[row.dimension_key]
        if domain_value_token(original) != domain_value_token(expected):
            raise ExperimentValidationError(
                "joined update payload differs from logical workload"
            )
        folded_original += value_payload_bytes(original)
        folded_updated += value_payload_bytes(
            updated_by_key[row.dimension_key]
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
            "original_payload_bytes": folded_original,
            "updated_payload_bytes": folded_updated,
            "payload_delta_bytes": folded_updated - folded_original,
        },
    }


def update_query(
    pair: PairSpec,
    *,
    phase: Literal["folded", "normalized"],
    property_name: str,
) -> str:
    if phase == "folded":
        label = pair.names.folded_label
        key_properties = pair.carrier_fk_fields
        physical_property = pair.binding_by_logical_name[property_name]
    else:
        label = pair.names.dimension_label
        key_properties = pair.dimension.key_fields
        physical_property = property_name
    key_predicate = " AND ".join(
        (
            f"node.{quote_ident(prop)} = "
            f"update.key[{index}]"
        )
        for index, prop in enumerate(key_properties)
    )
    index_properties = ", ".join(
        quote_ident(prop)
        for prop in (RUN_ID_PROPERTY, *key_properties)
    )
    return f"""
    UNWIND $updates AS update
    MATCH (
      node:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
      {artifact_identity_map()}
    )
    USING INDEX node:{quote_ident(label)}({index_properties})
    WHERE {key_predicate}
    SET node.{quote_ident(physical_property)} = update.value
    RETURN count(node) AS updated_nodes
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
        nodes_deleted=int(getattr(counters, "nodes_deleted", 0)),
        relationships_created=int(
            getattr(counters, "relationships_created", 0)
        ),
        relationships_deleted=int(
            getattr(counters, "relationships_deleted", 0)
        ),
        labels_added=int(getattr(counters, "labels_added", 0)),
        labels_removed=int(getattr(counters, "labels_removed", 0)),
    )


def timed_update_query(
    session: Any,
    query: str,
    parameters: dict[str, Any],
    run: int,
    context: str,
) -> TimingSample:
    started_ns = perf_counter_ns()
    result = session.run(query, parameters)
    record, summary = one_record(result, context)
    wall_ms = (perf_counter_ns() - started_ns) / 1_000_000
    return make_timing_sample(
        run,
        wall_ms,
        summary,
        int(record["updated_nodes"]),
    )


def verify_current_update_values(
    session: Any,
    pair: PairSpec,
    run_id: str,
    *,
    phase: Literal["folded", "normalized"],
    workload: UpdateWorkload,
    expected_values: dict[tuple[Any, ...], Any],
    expected_nodes: int,
    context: str,
) -> None:
    if phase == "folded":
        label = pair.names.folded_label
        key_properties = pair.carrier_fk_fields
        physical_property = workload.folded_property_name
    else:
        label = pair.names.dimension_label
        key_properties = pair.dimension.key_fields
        physical_property = workload.property_name
    key_expression = ", ".join(
        f"node.{quote_ident(prop)}" for prop in key_properties
    )
    result = session.run(
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
          {artifact_identity_map()}
        )
        RETURN
          [{key_expression}] AS logical_key,
          node.{quote_ident(physical_property)} AS property_value
        """,
        **case_parameters(pair, run_id),
    )
    records, _ = result_records_and_summary(result)
    if len(records) != expected_nodes:
        raise ExperimentValidationError(
            f"{pair.case_id} {context}: found {len(records)} nodes, "
            f"expected {expected_nodes}"
        )
    for record in records:
        key = tuple(record["logical_key"])
        if key not in expected_values:
            raise ExperimentValidationError(
                f"{pair.case_id} {context}: unexpected key {key!r}"
            )
        if (
            domain_value_token(record["property_value"])
            != domain_value_token(expected_values[key])
        ):
            raise ExperimentValidationError(
                f"{pair.case_id} {context}: unexpected "
                f"{workload.property_name} for key {key!r}"
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
) -> list[TimingSample]:
    query = update_query(
        pair,
        phase=phase,
        property_name=workload.property_name,
    )
    updated_values = {
        tuple(row["key"]): row["value"] for row in workload.updates
    }
    samples: list[TimingSample] = []
    total_runs = warmup_runs + measured_runs
    validation_ordinal = 1 if warmup_runs else total_runs

    for ordinal in range(1, total_runs + 1):
        measured_number = ordinal - warmup_runs
        parameters = case_parameters(pair, run_id) | {
            "updates": workload.updates
        }
        try:
            sample = timed_update_query(
                session,
                query,
                parameters,
                max(measured_number, 0),
                f"{pair.case_id} {phase} update",
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
                    expected_values=updated_values,
                    expected_nodes=expected_nodes,
                    context="successor validation",
                )
        except BaseException:
            try:
                restore_result = session.run(
                    query,
                    case_parameters(pair, run_id)
                    | {"updates": workload.restores},
                )
                one_record(
                    restore_result,
                    f"{pair.case_id} {phase} best-effort restore",
                )
            except BaseException as restore_error:
                print(
                    f"WARNING: {pair.case_id} {phase} restore failed: "
                    f"{type(restore_error).__name__}: {restore_error}",
                    file=sys.stderr,
                )
            raise
        else:
            restore_result = session.run(
                query,
                case_parameters(pair, run_id)
                | {"updates": workload.restores},
            )
            restore_record, _ = one_record(
                restore_result,
                f"{pair.case_id} {phase} restore",
            )
            if int(restore_record["updated_nodes"]) != expected_nodes:
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
        expected_values=workload.originals,
        expected_nodes=expected_nodes,
        context="final restore",
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
        "nodes_created": sorted(
            {sample.nodes_created for sample in samples}
        ),
        "nodes_deleted": sorted(
            {sample.nodes_deleted for sample in samples}
        ),
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
            f"median={server['median']:.3f} ms, "
            f"mean={server['mean']:.3f} ms"
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
    folded_median = median(
        sample.client_wall_ms for sample in folded_updates
    )
    normalized_median = median(
        sample.client_wall_ms for sample in normalized_updates
    )
    normalization_median = median(
        sample.client_wall_ms for sample in normalization_samples
    )
    saved_per_batch = folded_median - normalized_median
    join_rows = int(redundancy["join_rows_J"])
    dimensions = int(redundancy["participating_dimension_rows_D"])
    has_fd_redundancy = join_rows > dimensions
    return {
        "write_amplification_nodes": (
            join_rows / dimensions if dimensions else None
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


def csv_table_rows(
    source_data: Any,
    node: NodeSpec,
) -> tuple[dict[str, Any], ...]:
    return source_data.rows_by_filename[node.filename]


def csv_join_rows(
    source_data: Any,
    pair: PairSpec,
) -> list[JoinRow]:
    raw = find_import_relationship(pair.join)
    raw_id = IMPORT_SCHEMA.relationship_id(raw)
    relationship_rows = source_data.relationship_rows[raw_id]
    carrier_endpoint = (
        raw.source if pair.carrier_is_source else raw.target
    )
    dimension_endpoint = (
        raw.target if pair.carrier_is_source else raw.source
    )
    carrier_lookup = {
        IMPORT_SCHEMA.row_key(row, pair.carrier.key_fields): row
        for row in csv_table_rows(source_data, pair.carrier)
    }
    dimension_lookup = {
        IMPORT_SCHEMA.row_key(row, pair.dimension.key_fields): row
        for row in csv_table_rows(source_data, pair.dimension)
    }
    rows: list[JoinRow] = []
    for relationship_row in relationship_rows:
        carrier_key = canonical_key(
            IMPORT_SCHEMA.endpoint_key(
                carrier_endpoint,
                relationship_row,
            )
        )
        dimension_key = canonical_key(
            IMPORT_SCHEMA.endpoint_key(
                dimension_endpoint,
                relationship_row,
            )
        )
        rows.append(
            JoinRow(
                carrier_key=carrier_key,
                dimension_key=dimension_key,
                carrier_values=dict(carrier_lookup[carrier_key]),
                dimension_values=dict(dimension_lookup[dimension_key]),
            )
        )
    rows.sort(
        key=lambda row: tuple(
            domain_value_token(value) for value in row.carrier_key
        )
    )
    return rows


def csv_boundary_expectations(
    source_data: Any,
    pair: PairSpec,
    join_rows: Sequence[JoinRow],
) -> tuple[BoundaryExpectation, ...]:
    expectations: list[BoundaryExpectation] = []
    for boundary in pair.boundaries:
        raw = find_import_relationship(boundary.relationship)
        raw_rows = source_data.relationship_rows[
            IMPORT_SCHEMA.relationship_id(raw)
        ]
        owner_endpoint = raw.source if boundary.owner_is_source else raw.target
        degree_by_key: Counter[tuple[Any, ...]] = Counter(
            canonical_key(
                IMPORT_SCHEMA.endpoint_key(owner_endpoint, row)
            )
            for row in raw_rows
        )
        occurrence_keys = [
            (
                row.carrier_key
                if boundary.owner == "carrier"
                else row.dimension_key
            )
            for row in join_rows
        ]
        folded_count = sum(
            degree_by_key[key] for key in occurrence_keys
        )
        normalized_count = sum(
            degree_by_key[key] for key in set(occurrence_keys)
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


def validate_cases_against_csv(data_dir: Path) -> dict[str, Any]:
    source_data = IMPORT_SCHEMA.prepare_source_data(
        data_dir.expanduser().resolve()
    )
    summaries: list[dict[str, Any]] = []
    for case_id in CASE_ORDER:
        pair = CASES[case_id]
        rows = csv_join_rows(source_data, pair)
        dimension_rows = list(csv_table_rows(source_data, pair.dimension))
        participating = len({row.dimension_key for row in rows})
        redundant_rows = len(rows) - participating
        redundant_slots = (
            redundant_rows * len(pair.dependent_properties)
        )
        boundaries = csv_boundary_expectations(
            source_data,
            pair,
            rows,
        )
        boundary_total = sum(
            boundary.folded_count for boundary in boundaries
        )
        actual = ExpectedCaseMetrics(
            dimension_rows=len(dimension_rows),
            join_rows=len(rows),
            participating_dimensions=participating,
            redundant_rows=redundant_rows,
            redundant_slots=redundant_slots,
            complete_folded_boundary_edges=boundary_total,
        )
        if actual != pair.expected:
            raise ExperimentValidationError(
                f"{case_id}: CSV metrics differ from the golden fixture: "
                f"actual={actual!r}, expected={pair.expected!r}"
            )
        workload = build_active_domain_update_workload(
            pair,
            rows,
            dimension_rows,
            pair.default_update_property,
        )
        summaries.append(
            {
                "case_id": case_id,
                "carrier_A": pair.carrier.label,
                "dimension_B": pair.dimension.label,
                "join_relationship": relationship_id(pair.join),
                "carrier_is_source": pair.carrier_is_source,
                "carrier_key": list(pair.carrier.key_fields),
                "dimension_key": list(pair.dimension.key_fields),
                "fk_mapping": [list(item) for item in pair.fk_mapping],
                "metrics": asdict(actual),
                "default_update_property": pair.default_update_property,
                "active_domain_size": workload.active_domain_size,
                "boundary_expectations": [
                    asdict(boundary) for boundary in boundaries
                ],
            }
        )

    collision_pair = CASES["order_detail_product"]
    collision_rows = csv_join_rows(source_data, collision_pair)
    differing_unit_prices = sum(
        (
            domain_value_token(row.carrier_values["UnitPrice"])
            != domain_value_token(row.dimension_values["UnitPrice"])
        )
        for row in collision_rows
    )
    unit_price_alias = collision_pair.binding_by_logical_name["UnitPrice"]
    if differing_unit_prices != 658 or unit_price_alias != "product__UnitPrice":
        raise ExperimentValidationError(
            "OrderDetail/Product UnitPrice collision regression failed: "
            f"differing rows={differing_unit_prices}, "
            f"folded alias={unit_price_alias!r}"
        )

    report = {
        "validated_data_dir": str(data_dir.expanduser().resolve()),
        "case_count": len(summaries),
        "relationship_count": len(RELATIONSHIPS),
        "cases": summaries,
        "order_detail_product_unit_price_collision": {
            "differing_join_rows": differing_unit_prices,
            "dimension_folded_property": unit_price_alias,
        },
    }
    print(
        f"Validated {len(summaries)} direct Northwind pair cases against "
        f"{data_dir.expanduser().resolve()}"
    )
    for summary in summaries:
        metrics = summary["metrics"]
        print(
            f"  {summary['case_id']:<30} "
            f"J={metrics['join_rows']:>5,} "
            f"D={metrics['participating_dimensions']:>3,} "
            f"J-D={metrics['redundant_rows']:>5,} "
            f"slots={metrics['redundant_slots']:>6,} "
            "boundaries="
            f"{metrics['complete_folded_boundary_edges']:>6,}"
        )
    return report


def print_case_catalog() -> None:
    print(
        "case_id                         A (carrier)       "
        "B (dimension)     relationship       default update"
    )
    for case_id in CASE_ORDER:
        pair = CASES[case_id]
        arrow = (
            f"{pair.carrier.label}->{pair.dimension.label}"
            if pair.carrier_is_source
            else f"{pair.dimension.label}->{pair.carrier.label}"
        )
        print(
            f"{case_id:<31} "
            f"{pair.carrier.label:<17} "
            f"{pair.dimension.label:<17} "
            f"{pair.join.relationship_type:<18} "
            f"{pair.default_update_property}"
        )
        if not pair.carrier_is_source:
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
                "versions": [
                    str(version) for version in record["versions"]
                ],
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
        "A_carrier": {
            "label": pair.carrier.label,
            "columns": list(pair.carrier.columns),
            "key": list(pair.carrier.key_fields),
        },
        "B_dimension": {
            "label": pair.dimension.label,
            "columns": list(pair.dimension.columns),
            "key": list(pair.dimension.key_fields),
            "dependent_properties": list(pair.dependent_properties),
        },
        "source_join": {
            "relationship_type": pair.join.relationship_type,
            "direction": (
                f"{pair.join.source_label}->{pair.join.target_label}"
            ),
            "carrier_is_relationship_source": pair.carrier_is_source,
            "fk_mapping_carrier_to_dimension": [
                list(mapping) for mapping in pair.fk_mapping
            ],
        },
        "folded_property_bindings": [
            asdict(binding) for binding in pair.property_bindings
        ],
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
        + (
            "N/A"
            if amplification is None
            else f"{amplification:.3f}x"
        )
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
) -> dict[str, Any]:
    from neo4j import WRITE_ACCESS, __version__ as neo4j_driver_version

    started_at = datetime.now(timezone.utc)
    run_id = uuid4().hex
    constraints: list[ConstraintLease] = []
    indexes: list[IndexLease] = []
    source_sha256_before: str | None = None
    cleanup_completed = False

    try:
        with driver.session(
            database=database,
            default_access_mode=WRITE_ACCESS,
        ) as session:
            ensure_no_existing_artifacts(session, [pair])
            if args.topology_mode == "complete":
                validate_complete_source_topology(session, pair)
            baseline, original_rows, full_dimension_rows = (
                build_case_baseline(
                    session,
                    pair,
                    args.topology_mode,
                )
            )
            source_sha256_before = baseline.source_sha256
            environment = collect_environment_metadata(
                session,
                driver,
                neo4j_driver_version,
            )
            update_property = (
                args.update_property or pair.default_update_property
            )
            workload = build_active_domain_update_workload(
                pair,
                original_rows,
                full_dimension_rows,
                update_property,
            )
            payload_metrics = calculate_update_payload_metrics(
                original_rows,
                workload,
            )
            redundancy = calculate_redundancy(
                pair,
                original_rows,
                baseline,
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
                f"\n[{pair.case_id}] Step 1: measuring B-side FD redundancy"
            )
            print(f"  Join fingerprint: {baseline.join_sha256}")
            print_redundancy(pair, redundancy)

            print(
                f"\n[{pair.case_id}] Step 2: updating folded B copies "
                f"({workload.property_name}; {args.warmup_runs} warmups, "
                f"{args.runs} measured runs)"
            )
            print(
                f"  Batch: {workload.logical_tuple_count:,} logical B "
                f"updates; physical folded writes={baseline.join_rows:,}; "
                "restore is outside the timer"
            )
            print(
                "  Value strategy: deterministic full-B active-domain "
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
            )

            print(
                f"\n[{pair.case_id}] Step 3: normalizing the shadow "
                f"projection ({args.normalization_warmup_runs} warmups, "
                f"{args.normalization_runs} measured runs)"
            )
            if args.topology_mode == "complete":
                print(
                    "  Timing scope: node decomposition, normalized join "
                    "creation, B-boundary deduplication/migration, and commit"
                )
            else:
                print(
                    "  Timing scope: node decomposition, normalized join "
                    "creation, and commit (no boundary topology)"
                )
            normalization_samples: list[TimingSample] = []
            normalization_total = (
                args.normalization_warmup_runs
                + args.normalization_runs
            )
            for ordinal in range(1, normalization_total + 1):
                measured_number = (
                    ordinal - args.normalization_warmup_runs
                )
                sample = normalize_once(
                    session,
                    pair,
                    run_id,
                    baseline,
                    args.topology_mode,
                    max(measured_number, 0),
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
                f"\n[{pair.case_id}] Step 4: updating normalized B nodes "
                f"({workload.property_name}; {args.warmup_runs} warmups, "
                f"{args.runs} measured runs)"
            )
            print(
                f"  Batch: {workload.logical_tuple_count:,} logical B "
                f"updates; physical normalized writes="
                f"{baseline.participating_dimensions:,}; restore is outside "
                "the timer"
            )
            normalized_update_samples = benchmark_updates(
                session,
                pair,
                run_id,
                phase="normalized",
                workload=workload,
                expected_nodes=baseline.participating_dimensions,
                warmup_runs=args.warmup_runs,
                measured_runs=args.runs,
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
            )

            source_sha256_during = original_graph_sha256(session)
            if source_sha256_during != source_sha256_before:
                raise ExperimentValidationError(
                    f"{pair.case_id}: original graph changed during shadow run"
                )

            print(
                f"\n[{pair.case_id}] Cleanup: deleting this run's shadow "
                "projection (not timed)"
            )
            cleanup_result = cleanup_shadow_artifacts(
                session,
                pair,
                run_id,
            )
            drop_case_schema(
                session,
                constraints,
                indexes,
            )
            constraints = []
            indexes = []
            source_sha256_after = original_graph_sha256(session)
            if source_sha256_after != source_sha256_before:
                raise ExperimentValidationError(
                    f"{pair.case_id}: original graph fingerprint changed "
                    "after cleanup"
                )
            cleanup_completed = True
            print(
                f"  Removed {cleanup_result.deleted_nodes:,} nodes and "
                f"{cleanup_result.deleted_relationships:,} relationships; "
                "original graph fingerprint verified"
            )

        completed_at = datetime.now(timezone.utc)
        derived = derived_comparison(
            redundancy,
            folded_update_samples,
            normalization_samples,
            normalized_update_samples,
        )
        # Compatibility aliases used by Product/Supplier result readers.
        derived["denormalized_to_normalized_update_median_ratio"] = (
            derived["folded_to_normalized_update_median_ratio"]
        )
        derived[
            "shadow_data_rewrite_normalization_break_even_update_batches"
        ] = derived[
            "data_rewrite_normalization_break_even_update_batches"
        ]
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
            "report_schema_version": 4,
            "experiment": (
                "northwind_supplier_product_fd"
                if pair.case_id == "product_supplier"
                else "northwind_direct_pair_fd"
            ),
            "experiment_family": "northwind_direct_pair_fd",
            "case_id": pair.case_id,
            "run_id": run_id,
            "started_at_utc": started_at.isoformat(),
            "completed_at_utc": completed_at.isoformat(),
            "database": database,
            "environment": environment,
            "pair": case_descriptor(pair),
            "execution": {
                "mode": "run_scoped_shadow_projection",
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
            },
            "dataset": {
                "name": "case_study Northwind",
                "source_graph_sha256_before": source_sha256_before,
                "source_graph_sha256_during": source_sha256_during,
                "source_graph_sha256_after": source_sha256_after,
                "join_sha256": baseline.join_sha256,
                "carrier_rows": baseline.carrier_rows,
                "full_dimension_rows": baseline.dimension_rows,
                "join_rows": baseline.join_rows,
                "participating_dimensions": (
                    baseline.participating_dimensions
                ),
                "boundary_expectations": [
                    asdict(expectation)
                    for expectation in baseline.boundary_expectations
                ],
            },
            "configuration": {
                "update_property": workload.property_name,
                "update_key_properties": list(
                    pair.dimension.key_fields
                ),
                "warmup_runs": args.warmup_runs,
                "measured_update_runs": args.runs,
                "normalization_warmup_runs": (
                    args.normalization_warmup_runs
                ),
                "normalization_runs": args.normalization_runs,
                "update_strategy_id": UPDATE_STRATEGY_ID,
                "update_null_policy": UPDATE_NULL_POLICY,
                "active_domain_scope": (
                    f"full_original_{pair.dimension.label}_relation"
                ),
                "active_domain_size": workload.active_domain_size,
                "active_domain_source_tuple_count": (
                    workload.active_domain_source_tuple_count
                ),
                "logical_update_tuple_count": (
                    workload.logical_tuple_count
                ),
                "active_domain_sha256": workload.active_domain_sha256,
                "update_mapping_sha256": workload.mapping_sha256,
                "update_value_type": workload.value_type,
                "schema_objects_created_and_dropped": (
                    case_schema_names(pair, run_id)
                ),
                "update_timing_scope": (
                    "one autocommit batch through result consumption/commit; "
                    "restore and validation excluded"
                ),
                "normalization_timing_scope": (
                    "explicit transaction begin, shadow data decomposition"
                    + (
                        ", dimension-boundary deduplication/migration"
                        if args.topology_mode == "complete"
                        else ""
                    )
                    + ", and commit; DDL, validation, refolding, and cleanup "
                    "excluded"
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
                if source_sha256_before is not None:
                    try:
                        if (
                            original_graph_sha256(cleanup_session)
                            != source_sha256_before
                        ):
                            raise ExperimentValidationError(
                                "original graph fingerprint changed"
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
        # Makes the intended success condition explicit for future changes.
        if cleanup_completed:
            assert not constraints and not indexes


def cleanup_stale_run(
    driver: Any,
    database: str,
    run_id: str,
) -> dict[str, Any]:
    from neo4j import WRITE_ACCESS

    with driver.session(
        database=database,
        default_access_mode=WRITE_ACCESS,
    ) as session:
        # Adopt artifacts created by report-schema v3, which used the same
        # labels/run_id but did not store mv_case_id on every node.
        legacy_pair = CASES["product_supplier"]
        legacy_labels = [
            legacy_pair.names.folded_label,
            legacy_pair.names.carrier_label,
            legacy_pair.names.dimension_label,
        ]
        session.run(
            f"""
            MATCH (node)
            WHERE node.{quote_ident(RUN_ID_PROPERTY)} = $run_id
              AND any(
                label IN labels(node)
                WHERE label IN $legacy_labels
              )
              AND node.{quote_ident(CASE_ID_PROPERTY)} IS NULL
            SET node:{quote_ident(ARTIFACT_LABEL)}
            SET node.{quote_ident(CASE_ID_PROPERTY)} = $case_id
            """,
            run_id=run_id,
            legacy_labels=legacy_labels,
            case_id=legacy_pair.case_id,
        ).consume()

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
                )
            )
        dropped_constraints, dropped_indexes = drop_recovery_schema(
            session,
            run_id,
        )
        remaining_nodes, remaining_relationships = artifact_counts(session)
        return {
            "report_schema_version": 4,
            "mode": "cleanup_only",
            "run_id": run_id,
            "cleaned_cases": cleanup_results,
            "dropped_constraints": dropped_constraints,
            "dropped_indexes": dropped_indexes,
            "remaining_global_artifact_nodes": remaining_nodes,
            "remaining_global_artifact_relationships": (
                remaining_relationships
            ),
            "source_graph_sha256_after_cleanup": (
                original_graph_sha256(session)
            ),
        }


def validate_main_args(args: argparse.Namespace) -> None:
    if args.case_id is None and args.update_property is not None:
        raise ExperimentValidationError(
            "--update-property requires --case because each B table has a "
            "different property domain"
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
        ):
            raise ExperimentValidationError(
                "--cleanup-run-id cannot be combined with experiment options"
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
            offline_report = validate_cases_against_csv(args.data_dir)
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
                "--database must name an existing Northwind data database"
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
                'install the Neo4j driver with: '
                'python -m pip install "neo4j>=5.7"'
            ) from exc

        password = resolve_neo4j_password(args.user)

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
                if cleanup_run_id is not None:
                    cleanup_report = cleanup_stale_run(
                        driver,
                        database,
                        cleanup_run_id,
                    )
                    reports: list[dict[str, Any]] = []
                else:
                    cleanup_report = None
                    selected_ids = (
                        CASE_ORDER
                        if args.case_id is None
                        else (args.case_id,)
                    )
                    reports = []
                    for index, case_id in enumerate(
                        selected_ids,
                        start=1,
                    ):
                        if len(selected_ids) > 1:
                            print(
                                f"\n=== Case {index}/{len(selected_ids)}: "
                                f"{case_id} ==="
                            )
                        report = run_case_experiment(
                            driver,
                            database,
                            args,
                            CASES[case_id],
                        )
                        reports.append(report)
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
                "report_schema_version": 4,
                "experiment": "northwind_direct_pair_fd_suite",
                "database": database,
                "topology_mode": args.topology_mode,
                "case_count": len(reports),
                "case_order": list(CASE_ORDER),
                "cases": reports,
            }
            print(
                f"\nAll {len(reports)} direct Northwind pair cases "
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
