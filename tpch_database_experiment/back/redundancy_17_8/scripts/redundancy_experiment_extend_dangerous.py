#!/usr/bin/env python3
"""Run pair-wise and recursive TPC-H replacement experiments in Neo4j.

The experiment covers every direct foreign-key relationship in the embedded
TPC-H graph schema.  It deliberately uses only relationships present in that
schema; query-specific virtual joins are not treated as foreign keys.
For each case, the referencing table contains the foreign key and the
referenced table supplies the referenced key.  One run-scoped shadow node
represents each row of their join.

With no case-selection argument, all eight direct TPC-H cases run
sequentially.  Use ``--list-cases`` to inspect the complete catalog or
``--case CASE_ID`` to run only one case.  ``--all-cases`` is retained as an
explicit alias for the default batch behavior.  Recursive cases are selected
individually with ``--case``.  ``--all-extended-cases`` runs the eight direct
pairs followed by all 51 recursive cases.

TPC-H column names are already table-namespaced (``l_``, ``ps_``, ``o_``, and
so on).  Direct-pair folded nodes therefore preserve the native imported
names.  Recursive aliases use ``n1_``/``n2_`` and ``r1_``/``r2_`` so both
logical branches can coexist on one MV node.  Referenced keys are not copied;
the referencing foreign-key properties represent them.  Normalized shadow
nodes use the native imported names.

Two topology modes are available:

``complete``
    Copy all declared boundary relationships incident to the referencing and
    referenced tables, excluding their folded relationship.  Referencing-owned
    copies stay on the referencing shadow.  Referenced-owned copies are
    duplicated in the folded primary.  The one
    structural exception is the opposite live Nation parent: folding S-N keeps
    C-N on the retained Nation representation, and folding C-N keeps S-N there,
    rather than creating the invalid C->SN or S->CN boundary.

``property-fd``
    Materialize only namespaced referencing/referenced properties on the
    folded primary, without boundary topology.  This isolates property
    redundancy from query-topology maintenance and is the default because
    complete boundary copying can grow very large for TPC-H Nation joins.

The recursive experiment extends an existing folded MV one logical role at a
time.  Its TPC-H role tree distinguishes the customer Nation/Region branch
(``N1``/``R1``) from the supplier Nation/Region branch (``N2``/``R2``), even
though each pair maps to the same physical ``NATION``/``REGION`` labels.
Every stage after the first must consume a relationship copy that was
physically inherited by the preceding MV; it never rediscovers a missing edge
from source-schema adjacency.  Version 10 supports rooted outgoing
foreign-key-to-primary-key fold sequences.  A later table may join from any
earlier role whose declared future boundary has been physically carried by the
MV.  This keeps one folded row per grain row and avoids incoming fanout.

Recursive transit inheritance is template-scoped: a stage copies every
currently exposed boundary required by a later declared stage, but no
unrelated schema edge.  Consumed join-back edges are not copied.  In
``property-fd`` mode the final recursive MV remains relationship-free, matching
the historical behavior.  In ``complete`` mode the final MV additionally
inherits every still-exposed boundary in the logical role-tree cut.  Those
copies preserve direction and relationship properties, never rediscover
physical-label cross-branch edges.  The timed recursive shadow continues to use
distinct customer and supplier roles, but the supplemental
configuration-redundancy snapshot reconciles those roles by physical source
identity.  Consequently a retained Nation representation and every companion
Nation/Region closure participate in the same NATION/REGION key union.
Recursive v10 also reports the cumulative construction lifecycle: artifacts
created across all stages, superseded intermediates deleted between stages,
and the folded artifacts retained when benchmarking begins.  That lifecycle
does not include paired updates, final cleanup, or any
source-graph deletion.
It also compares the complete original TPC-H graph with a counterfactual graph
in which the selected physical source-role footprint is replaced by the whole
logical folded configuration.  That configuration includes the primary MV and
any retained-Nation or Nation/Region closure companion required by the other
live Nation parent.  Objects outside the replacement footprint remain in the
post-fold total.  Repeated N1/N2 and R1/R2 physical objects are counted once,
while their configuration-wide property occurrences remain available as
separate NATION/REGION shared-dimension metrics rather than being misreported
as nodes.  This logical replacement comparison always uses complete boundary
semantics; ``property-fd`` versus ``complete`` remains a separate description
of what the benchmark physically materialized.  It also compares the complete
TPC-H business-property cell count before and after the logical replacement.
That cell metric includes declared PK, FK, and ordinary node properties while
excluding labels, relationship topology, and experiment bookkeeping fields.

The live experiment expects the primary-key constraints created by the TPC-H
importer and a Neo4j server that supports scoped ``CALL (...)`` subqueries with
``IN TRANSACTIONS`` (Neo4j 5.23 or newer).

Update timing uses *replacement* semantics.  One deterministic logical
workload updates one ordinary scalar property on every row of seven physical
TPC-H relations.  LINEITEM remains part of both complete graph layouts but is
deliberately excluded from scalar update timing because it is the root grain
and its selected property is not duplicated by these folds.  The same
key-to-successor mapping is applied to two isolated, run-scoped
representations:

``normalized``
    A complete eight-relation graph shadow with one node/property occurrence
    per logical fact and one copied relationship per source FK edge.

``replacement``
    An isolated clone of the final folded primary plus complete clones of
    unaffected relations, residual rows that do not participate in the folded
    join, and any retained Nation or Nation/Region closure needed by the other
    live branch.  All boundary relationships are materialized between
    run-scoped layout nodes; folded joins are represented by same-node
    co-location.  Every physical occurrence of a logical fact is updated.
    N1/N2 and R1/R2 are storage roles for the same physical NATION/REGION
    facts, not additional logical facts.

The scalar workload deliberately excludes PK/FK properties.  It holds the
complete materialized topology constant and therefore does not time
relationship rewiring; topology-changing updates are a separate workload.
Each logical-key batch updates all routed copies in one explicit transaction.
Layout construction, index creation, topology construction, validation,
restoration, and cleanup remain outside steady-state update timing.  The
imported source graph is read-only throughout.

After the paired updates have been restored, one additional destructive
conversion is timed.  The prebuilt normalized comparison layout is removed
before the timer starts; the code then reconstructs all eight normalized
relations exclusively from replacement occurrences, rebuilds the eight FK
relationship types from the converted FK properties, and deletes the
replacement input.  Conversion validation and final cleanup are outside that
single timing sample.
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
from itertools import combinations
from pathlib import Path
from statistics import fmean, median, stdev
from time import perf_counter_ns
from typing import Any, Callable, Iterable, Iterator, Literal, Mapping, Sequence
from uuid import uuid4


DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_DATABASE = "tpch-sf-001"
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "tbl_sf_001"
DEFAULT_WARMUP_RUNS = 1
DEFAULT_RUNS = 1
DEFAULT_INDEX_WAIT_SECONDS = 300
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_TOPOLOGY_MODE = "property-fd"
def default_output_json_for_database(database: str) -> Path:
    safe_database = database.strip().lower()
    if not safe_database or safe_database == "system":
        safe_database = DEFAULT_DATABASE
    return (
        Path(__file__).resolve().parents[1]
        / "results"
        / "test"
        / "extend"
        / safe_database
        / "tpch_fd_experiment_extend_results.json"
    )

RUN_ID_PROPERTY = "run_id"
CASE_ID_PROPERTY = "mv_case_id"
ARTIFACT_LABEL = "mv_experiment"
SCHEMA_PREFIX = "mvfd"
SHADOW_RELATIONSHIP_PREFIX = "JOIN_"
REPORT_SCHEMA_VERSION = 13
RECURSIVE_REPORT_SCHEMA_VERSION = 7
RECURSIVE_FOLD_SCHEMA_VERSION = 10
JOIN_FINGERPRINT_SCHEMA_VERSION = 3
JOIN_FINGERPRINT_ALGORITHM = "commutative_join_row_sha256_sum_xor_v1"
RECURSIVE_JOIN_FINGERPRINT_SCHEMA_VERSION = 2
RECURSIVE_JOIN_FINGERPRINT_ALGORITHM = (
    "commutative_symbolic_role_join_row_sha256_sum_xor_v2"
)
CONFIGURATION_REDUNDANCY_SCHEMA_VERSION = 1
SOURCE_GRAPH_FINGERPRINT_SCHEMA_VERSION = 3
SOURCE_GRAPH_FINGERPRINT_ALGORITHM = "tpch_schema_keyed_commutative_sha256_sum_xor_v1"
FOLDED_PROPERTY_NAMING_STRATEGY_ID = "tpch_native_column_names_no_referenced_key_v1"
RECURSIVE_FOLDED_PROPERTY_NAMING_STRATEGY_ID = (
    "tpch_role_scoped_nation_region_columns_omit_absorbed_pk_v2"
)
RECURSIVE_STAGE_PROPERTY = "mv_fold_stage"
UPDATE_STRATEGY_ID = "deterministic_full_referenced_relation_domain_rotation_v2"
UPDATE_NULL_POLICY = "reject_attribute_if_any_domain_or_updated_value_is_null"
FULL_REPLACEMENT_UPDATE_SCHEMA_VERSION = 4
FULL_REPLACEMENT_UPDATE_STRATEGY_ID = (
    "deterministic_7table_scalar_domain_rotation_no_lineitem_replacement_v3"
)
FULL_REPLACEMENT_SEMANTICS_ID = "lossless_graph_replacement_v2"
FULL_TOPOLOGY_RELATIONSHIP_PREFIX = "MV_FULL_"
FULL_TOPOLOGY_LOGICAL_EDGE_IDS_PROPERTY = "mv_full_logical_edge_ids"
FULL_TOPOLOGY_ANCHOR_LABEL = "mv_full_topology_anchor"
FULL_TOPOLOGY_ANCHOR_RELATIONSHIP = "MV_FULL_ANCHORS"

# One representative ordinary property per physical relation is used to build
# and validate complete eight-table layout occurrences.  LINEITEM remains here
# because conversion and topology validation must reconstruct all eight tables.
FULL_LAYOUT_OCCURRENCE_PROPERTIES: dict[str, str] = {
    "REGION": "r_name",
    "NATION": "n_name",
    "SUPPLIER": "s_phone",
    "CUSTOMER": "c_mktsegment",
    "PART": "p_name",
    "PARTSUPP": "ps_supplycost",
    "ORDERS": "o_orderpriority",
    "LINEITEM": "l_shipmode",
}

# Scalar update workload.  LINEITEM is intentionally absent: l_shipmode is not
# duplicated by the evaluated folds, so neither normalized nor replacement
# update timing writes it.  A logical fact is identified by (physical table,
# physical primary key, property); N1/N2 and R1/R2 do not create new facts.
FULL_UPDATE_PROPERTIES: dict[str, str] = {
    table_label: property_name
    for table_label, property_name in FULL_LAYOUT_OCCURRENCE_PROPERTIES.items()
    if table_label != "LINEITEM"
}
FULL_UPDATE_EXCLUDED_TABLES: tuple[str, ...] = ("LINEITEM",)

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
ConfigurationEntity = Literal["NATION", "REGION"]
CONFIGURATION_REDUNDANCY_ENTITY_ORDER: tuple[ConfigurationEntity, ...] = (
    "NATION",
    "REGION",
)


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
class ConfigurationComponentObservation:
    """One entity's occurrences in one component of a final MV snapshot.

    ``occurrence_count`` retains multiplicity, while ``distinct_keys`` contains
    the exact logical entity keys represented by the component.  Callers must
    submit the final configuration only: recursive stage history is not a
    configuration component and must not be accumulated here.
    """

    component_id: str
    entity: ConfigurationEntity
    occurrence_count: int
    distinct_keys: frozenset[tuple[Any, ...]]
    dependent_property_count: int

    def __post_init__(self) -> None:
        if not self.component_id.strip():
            raise ValueError("component_id must not be empty")
        if self.entity not in CONFIGURATION_REDUNDANCY_ENTITY_ORDER:
            raise ValueError(
                "configuration redundancy supports only NATION and REGION; "
                f"got {self.entity!r}"
            )
        if self.occurrence_count < 0:
            raise ValueError("occurrence_count must be non-negative")
        if self.dependent_property_count < 0:
            raise ValueError("dependent_property_count must be non-negative")

        normalized_keys: set[tuple[Any, ...]] = set()
        for key in self.distinct_keys:
            if not isinstance(key, tuple) or not key:
                raise ValueError("each distinct entity key must be a non-empty tuple")
            try:
                hash(key)
            except TypeError as exc:
                raise ValueError(f"entity key must be hashable; got {key!r}") from exc
            normalized_keys.add(key)
        normalized = frozenset(normalized_keys)
        object.__setattr__(self, "distinct_keys", normalized)

        if self.occurrence_count < len(normalized):
            raise ValueError(
                "occurrence_count cannot be smaller than the distinct key " "count"
            )
        if self.occurrence_count > 0 and not normalized:
            raise ValueError(
                "a positive occurrence_count requires at least one distinct " "key"
            )

    @classmethod
    def from_key_occurrences(
        cls,
        *,
        component_id: str,
        entity: ConfigurationEntity,
        key_occurrences: Iterable[tuple[Any, ...]],
        dependent_property_count: int,
    ) -> ConfigurationComponentObservation:
        """Build an observation from one logical key per stored occurrence."""

        occurrences = tuple(key_occurrences)
        return cls(
            component_id=component_id,
            entity=entity,
            occurrence_count=len(occurrences),
            distinct_keys=frozenset(occurrences),
            dependent_property_count=dependent_property_count,
        )


@dataclass(frozen=True)
class ConfigurationComponentRedundancy:
    """Within-component redundancy for one observed entity."""

    component_id: str
    entity: ConfigurationEntity
    occurrence_count: int
    distinct_count: int
    within_component_redundant_tuple_copies: int
    within_component_redundant_property_slots: int


@dataclass(frozen=True)
class ConfigurationEntityRedundancy:
    """Configuration-global redundancy decomposition for one entity type."""

    entity: ConfigurationEntity
    dependent_property_count: int
    occurrence_count: int
    summed_component_distinct_count: int
    union_distinct_count: int
    within_component_redundant_tuple_copies: int
    cross_component_redundant_tuple_copies: int
    total_redundant_tuple_copies: int
    within_component_redundant_property_slots: int
    cross_component_redundant_property_slots: int
    total_redundant_property_slots: int
    components: tuple[ConfigurationComponentRedundancy, ...]

    @property
    def across_component_redundant_tuple_copies(self) -> int:
        """Alias spelling that mirrors the within/across decomposition."""

        return self.cross_component_redundant_tuple_copies

    @property
    def across_component_redundant_property_slots(self) -> int:
        """Alias spelling that mirrors the within/across decomposition."""

        return self.cross_component_redundant_property_slots


@dataclass(frozen=True)
class ConfigurationRedundancySummary:
    """Redundancy for all supported entities in one final configuration."""

    schema_version: int
    entities: tuple[ConfigurationEntityRedundancy, ...]

    def for_entity(
        self,
        entity: ConfigurationEntity,
    ) -> ConfigurationEntityRedundancy | None:
        return next(
            (metrics for metrics in self.entities if metrics.entity == entity),
            None,
        )


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
        return tuple(by_referenced_key[key] for key in self.referenced.key_fields)

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
        return tuple(prop for prop in self.dependent_properties if prop not in blocked)

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
class RecursiveBoundarySpec:
    """One currently reachable source-graph edge at an MV boundary."""

    relationship: RelationshipSpec
    owner_role_index: int
    owner_role_symbol: str
    external_role_symbol: str
    direction: Literal["incoming", "outgoing"]
    external_label: str
    copy_type: str

    @property
    def boundary_id(self) -> str:
        return (
            f"role[{self.owner_role_index}:{self.owner_role_symbol}]:"
            f"{self.direction}:{self.external_role_symbol}:"
            f"{self.relationship.source_label}"
            f"-[:{self.relationship.relationship_type}]->"
            f"{self.relationship.target_label}"
        )


@dataclass(frozen=True)
class RecursiveRoleDefinition:
    """One logical role in the recursive TPC-H fold tree."""

    symbol: str
    display_name: str
    table_label: str
    parent_symbol: str | None
    relationship_type: str | None


@dataclass(frozen=True)
class RecursiveRoleSpec:
    """A logical table role and its representation on a folded node."""

    role_index: int
    symbol: str
    display_name: str
    table: NodeSpec
    property_bindings: tuple[PropertyBinding, ...]

    @property
    def display_label(self) -> str:
        return f"{self.symbol}:{self.table.label}"

    @property
    def binding_by_logical_name(self) -> dict[str, str]:
        return {
            binding.logical_name: binding.folded_name
            for binding in self.property_bindings
        }

    def folded_property(self, logical_name: str) -> str:
        try:
            return self.binding_by_logical_name[logical_name]
        except KeyError as exc:
            raise ExperimentValidationError(
                f"role {self.role_index} ({self.display_label}) has no folded "
                f"source for {logical_name!r}"
            ) from exc


@dataclass(frozen=True)
class RecursiveStageSpec:
    """One physical fold that consumes the preceding MV's inherited edge."""

    stage_number: int
    owner_role_index: int
    added_role_index: int
    join: RelationshipSpec
    input_label: str | None
    output_label: str
    selected_copy_type: str | None
    next_copy_type: str | None
    effective_boundaries_after: tuple[RecursiveBoundarySpec, ...]
    inherited_boundaries: tuple[RecursiveBoundarySpec, ...]


@dataclass(frozen=True)
class RecursiveNames:
    final_folded_label: str
    normalized_prefix_label: str
    normalized_factor_label: str
    normalized_join_type: str


@dataclass(frozen=True)
class RecursiveCaseSpec:
    """An ordered, rooted FK-to-PK fold built one stage at a time."""

    case_id: str
    roles: tuple[RecursiveRoleSpec, ...]
    joins: tuple[RelationshipSpec, ...]
    stages: tuple[RecursiveStageSpec, ...]
    default_update_property: str
    names: RecursiveNames

    @property
    def grain(self) -> RecursiveRoleSpec:
        return self.roles[0]

    @property
    def final_role(self) -> RecursiveRoleSpec:
        return self.roles[-1]

    @property
    def final_join(self) -> RelationshipSpec:
        return self.joins[-1]

    @property
    def final_owner(self) -> RecursiveRoleSpec:
        return self.roles[self.stages[-1].owner_role_index]

    @property
    def final_dependent_properties(self) -> tuple[str, ...]:
        keys = set(self.final_role.table.key_fields)
        return tuple(prop for prop in self.final_role.table.columns if prop not in keys)

    @property
    def final_folded_key_properties(self) -> tuple[str, ...]:
        return tuple(
            self.final_role.folded_property(prop)
            for prop in self.final_role.table.key_fields
        )

    @property
    def final_boundaries(self) -> tuple[RecursiveBoundarySpec, ...]:
        """Return the logical role-tree cut exposed by the final MV."""

        return self.stages[-1].effective_boundaries_after

    @property
    def all_artifact_labels(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (
                    *(stage.output_label for stage in self.stages),
                    self.names.normalized_prefix_label,
                    self.names.normalized_factor_label,
                )
            )
        )

    @property
    def all_copy_relationship_types(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    self.names.normalized_join_type,
                    *(
                        boundary.copy_type
                        for stage in self.stages
                        for boundary in stage.inherited_boundaries
                    ),
                    *(boundary.copy_type for boundary in self.final_boundaries),
                }
            )
        )


@dataclass(frozen=True)
class RecursiveBaseline:
    grain_rows: int
    final_table_rows: int
    chain_rows: int
    participating_final_rows: int
    prefix_join_sha256: tuple[str | None, ...]
    source_sha256: str

    @property
    def join_sha256(self) -> str:
        fingerprint = self.prefix_join_sha256[-1]
        if fingerprint is None:
            raise ExperimentStateError(
                "final recursive source fingerprint is unavailable"
            )
        return fingerprint


@dataclass(frozen=True)
class RecursiveShadowState:
    final_folded_nodes: int
    normalized_prefix_nodes: int
    normalized_factor_nodes: int
    normalized_join_relationships: int
    boundary_relationships: dict[str, int]
    all_case_nodes: int
    all_case_relationships: int

    @property
    def phase(self) -> str:
        if self.all_case_nodes == 0 and self.all_case_relationships == 0:
            return "clean"
        if (
            self.final_folded_nodes == self.all_case_nodes
            and self.normalized_prefix_nodes == 0
            and self.normalized_factor_nodes == 0
            and self.normalized_join_relationships == 0
        ):
            return "folded"
        if (
            self.final_folded_nodes == 0
            and self.normalized_prefix_nodes > 0
            and self.normalized_factor_nodes > 0
            and self.normalized_join_relationships == self.normalized_prefix_nodes
            and self.all_case_nodes
            == self.normalized_prefix_nodes + self.normalized_factor_nodes
        ):
            return "normalized"
        return "inconsistent"


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
class RecursiveBoundaryExpectation:
    """Expected final-MV copies for one role-scoped external boundary."""

    boundary: RecursiveBoundarySpec
    folded_count: int
    normalized_count: int

    @property
    def boundary_id(self) -> str:
        return self.boundary.boundary_id

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
            and self.normalized_join_relationships == self.normalized_referencing_nodes
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
        return self.logical_updated_payload_bytes - self.logical_original_payload_bytes

    def parameter_batches(
        self,
        batch_size: int,
        *,
        restore: bool,
        target_id: str | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        connection = self._require_connection()
        value_expression = "original.value_blob" if restore else "successor.value_blob"
        routing_join = ""
        routing_where = ""
        query_parameters: tuple[Any, ...] = ()
        if target_id is not None:
            routing_join = (
                "JOIN physical_target_key AS routing "
                "ON routing.key_token = workload.key_token"
            )
            routing_where = "WHERE routing.target_id = ?"
            query_parameters = (target_id,)
        cursor = connection.execute(
            f"""
            SELECT
              workload.key_blob,
              {value_expression},
              workload.fanout
            FROM logical_update AS workload
            {routing_join}
            JOIN domain AS original
              ON original.token = workload.original_token
            JOIN domain AS successor
              ON successor.token = original.successor_token
            {routing_where}
            ORDER BY workload.key_token
            """,
            query_parameters,
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

    def routed_transaction_batches(
        self,
        batch_size: int,
        *,
        restore: bool,
        target_ids: Sequence[str],
    ) -> Iterator[tuple[int, dict[str, list[dict[str, Any]]]]]:
        """Yield one logical-key batch routed to every represented target.

        A logical key appears once in the batch cardinality and may appear in
        several target lists.  Callers can therefore update every physical
        occurrence of that logical batch in one explicit database
        transaction without submitting keys to target families that do not
        represent them.
        """

        if not target_ids:
            raise ValueError("routed transaction batches require target IDs")
        connection = self._require_connection()
        value_expression = "original.value_blob" if restore else "successor.value_blob"
        placeholders = ", ".join("?" for _ in target_ids)
        cursor = connection.execute(
            f"""
            SELECT
              workload.key_token,
              workload.key_blob,
              {value_expression},
              workload.fanout,
              routing.target_id
            FROM logical_update AS workload
            JOIN domain AS original
              ON original.token = workload.original_token
            JOIN domain AS successor
              ON successor.token = original.successor_token
            JOIN physical_target_key AS routing
              ON routing.key_token = workload.key_token
            WHERE routing.target_id IN ({placeholders})
            ORDER BY workload.key_token, routing.target_id
            """,
            tuple(target_ids),
        )
        routed: dict[str, list[dict[str, Any]]] = {}
        logical_keys = 0
        current_token: str | None = None
        current_payload: dict[str, Any] | None = None

        def append_current() -> None:
            nonlocal logical_keys
            if current_payload is None:
                return
            logical_keys += 1

        for key_token, key_blob, value_blob, fanout, target_id in cursor:
            token = str(key_token)
            if current_token is not None and token != current_token:
                append_current()
                if logical_keys == batch_size:
                    yield logical_keys, routed
                    routed = {}
                    logical_keys = 0
                current_payload = None
            if current_payload is None:
                current_token = token
                current_payload = {
                    "key": list(pickle.loads(key_blob)),
                    "value": pickle.loads(value_blob),
                    "expected_fanout": int(fanout),
                }
            routed.setdefault(str(target_id), []).append(current_payload)
        append_current()
        if logical_keys:
            yield logical_keys, routed

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


@dataclass(frozen=True)
class FullUpdateTarget:
    """One physical property-cell family carrying one table's logical fact."""

    layout: Literal["normalized", "replacement"]
    component_id: str
    component_kind: Literal[
        "normalized",
        "primary",
        "unaffected",
        "residual",
        "shared_companion",
    ]
    table_label: str
    label: str
    key_properties: tuple[str, ...]
    physical_property: str

    @property
    def target_id(self) -> str:
        return ":".join(
            (
                self.layout,
                self.component_id,
                self.table_label,
                self.label,
                ",".join(self.key_properties),
                self.physical_property,
            )
        )

    @property
    def physical_cell_family(self) -> tuple[str, str]:
        """Identity used to reject duplicate target-plan entries."""

        return (self.label, self.physical_property)


@dataclass(frozen=True)
class FullTopologyOccurrence:
    """One logical table role represented by one physical node family."""

    layout: Literal["normalized", "replacement"]
    occurrence_id: str
    component_id: str
    component_kind: Literal[
        "normalized",
        "primary",
        "unaffected",
        "residual",
        "shared_companion",
    ]
    table_label: str
    label: str
    property_bindings: tuple[PropertyBinding, ...]
    role_symbols: tuple[str, ...]

    @property
    def binding_by_logical_name(self) -> dict[str, str]:
        return {
            binding.logical_name: binding.folded_name
            for binding in self.property_bindings
        }

    @property
    def key_properties(self) -> tuple[str, ...]:
        bindings = self.binding_by_logical_name
        return tuple(bindings[key] for key in TABLES[self.table_label].key_fields)


@dataclass(frozen=True)
class FullTopologyRoute:
    """One source occurrence routed to canonical targets for role edge(s).

    ``target_occurrence_ids`` is a candidate family, not a Cartesian fanout.
    Exactly one run-scoped anchor is selected per target role and logical key.
    A particular source row is implicit only when that same physical node
    carries a compatible target-role occurrence; otherwise one explicit edge
    is created to the canonical target anchor.
    """

    relationship_type: str
    full_relationship_type: str
    source_occurrence_id: str
    target_occurrence_ids: tuple[str, ...]
    logical_edge_ids: tuple[str, ...]
    target_role_symbols: tuple[str, ...]


@dataclass(frozen=True)
class FullTopologyAnchorPlan:
    """One canonical target family for a layout/table role and logical key."""

    layout: Literal["normalized", "replacement"]
    table_label: str
    role_symbol: str
    candidate_occurrence_ids: tuple[str, ...]


@dataclass
class FullReplacementBenchmarkResult:
    normalized_samples: list[TimingSample]
    replacement_samples: list[TimingSample]
    conversion_sample: TimingSample
    report: dict[str, Any]


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
        self.relationships_created += int(getattr(counters, "relationships_created", 0))
        self.relationships_deleted += int(getattr(counters, "relationships_deleted", 0))
        self.labels_added += int(getattr(counters, "labels_added", 0))
        self.labels_removed += int(getattr(counters, "labels_removed", 0))


def calculate_configuration_global_redundancy(
    observations: Iterable[ConfigurationComponentObservation],
) -> ConfigurationRedundancySummary:
    """Calculate exact NATION/REGION redundancy for a final component snapshot.

    For entity ``T`` in components ``i`` this decomposes total redundancy as::

        within(T) = sum_i(occurrences_i - distinct_i)
        cross(T)  = sum_i(distinct_i) - size(union_i(keys_i))
        total(T)  = sum_i(occurrences_i) - size(union_i(keys_i))

    Thus a preserved canonical NATION component contributes no within-component
    redundancy, but its overlap with an ``SN``/``CN`` primary component is
    captured by ``cross``.  Likewise, an ``NR`` closure participates in the
    REGION union instead of being treated as unrelated recursive stage history.

    There must be at most one observation per ``(component_id, entity)``.  The
    caller is responsible for aggregating multiple roles of the same entity in
    a component and for supplying only the final configuration, not prior fold
    stages.
    """

    grouped: dict[
        ConfigurationEntity,
        list[ConfigurationComponentObservation],
    ] = {entity: [] for entity in CONFIGURATION_REDUNDANCY_ENTITY_ORDER}
    seen_components: set[tuple[str, ConfigurationEntity]] = set()

    for observation in observations:
        if not isinstance(observation, ConfigurationComponentObservation):
            raise TypeError(
                "observations must contain ConfigurationComponentObservation "
                f"instances; got {type(observation).__name__}"
            )
        identity = (observation.component_id, observation.entity)
        if identity in seen_components:
            raise ValueError(
                "duplicate configuration component/entity observation: "
                f"{observation.component_id!r}/{observation.entity}"
            )
        seen_components.add(identity)
        grouped[observation.entity].append(observation)

    entity_metrics: list[ConfigurationEntityRedundancy] = []
    for entity in CONFIGURATION_REDUNDANCY_ENTITY_ORDER:
        entity_observations = grouped[entity]
        if not entity_observations:
            continue

        dependent_property_counts = {
            observation.dependent_property_count for observation in entity_observations
        }
        if len(dependent_property_counts) != 1:
            raise ValueError(
                f"{entity} observations disagree on dependent_property_count: "
                f"{sorted(dependent_property_counts)}"
            )
        dependent_property_count = next(iter(dependent_property_counts))

        union_keys: set[tuple[Any, ...]] = set()
        component_metrics: list[ConfigurationComponentRedundancy] = []
        occurrence_count = 0
        summed_component_distinct_count = 0
        within_tuple_copies = 0

        for observation in sorted(
            entity_observations,
            key=lambda item: item.component_id,
        ):
            distinct_count = len(observation.distinct_keys)
            component_within = observation.occurrence_count - distinct_count
            occurrence_count += observation.occurrence_count
            summed_component_distinct_count += distinct_count
            within_tuple_copies += component_within
            union_keys.update(observation.distinct_keys)
            component_metrics.append(
                ConfigurationComponentRedundancy(
                    component_id=observation.component_id,
                    entity=entity,
                    occurrence_count=observation.occurrence_count,
                    distinct_count=distinct_count,
                    within_component_redundant_tuple_copies=component_within,
                    within_component_redundant_property_slots=(
                        component_within * dependent_property_count
                    ),
                )
            )

        union_distinct_count = len(union_keys)
        cross_tuple_copies = summed_component_distinct_count - union_distinct_count
        total_tuple_copies = occurrence_count - union_distinct_count
        if total_tuple_copies != within_tuple_copies + cross_tuple_copies:
            raise AssertionError(
                "configuration redundancy decomposition is inconsistent"
            )

        entity_metrics.append(
            ConfigurationEntityRedundancy(
                entity=entity,
                dependent_property_count=dependent_property_count,
                occurrence_count=occurrence_count,
                summed_component_distinct_count=(summed_component_distinct_count),
                union_distinct_count=union_distinct_count,
                within_component_redundant_tuple_copies=within_tuple_copies,
                cross_component_redundant_tuple_copies=cross_tuple_copies,
                total_redundant_tuple_copies=total_tuple_copies,
                within_component_redundant_property_slots=(
                    within_tuple_copies * dependent_property_count
                ),
                cross_component_redundant_property_slots=(
                    cross_tuple_copies * dependent_property_count
                ),
                total_redundant_property_slots=(
                    total_tuple_copies * dependent_property_count
                ),
                components=tuple(component_metrics),
            )
        )

    return ConfigurationRedundancySummary(
        schema_version=CONFIGURATION_REDUNDANCY_SCHEMA_VERSION,
        entities=tuple(entity_metrics),
    )


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

# Recursive v10 uses ten logical roles over eight physical TPC-H labels.  N1/R1
# is the customer branch and N2/R2 is the supplier branch.  This role tree is
# deliberately stricter than physical-label adjacency: C->N2 and S->N1 are not
# declared edges, so folding S with N2 cannot inherit CUSTOMER_NATION from the
# physical Nation node.  Repeated physical labels are supported only through
# these explicitly declared aliases.
RECURSIVE_ROLE_DEFINITION_ORDER: tuple[RecursiveRoleDefinition, ...] = (
    RecursiveRoleDefinition("L", "lineitem", "LINEITEM", None, None),
    RecursiveRoleDefinition("O", "orders", "ORDERS", "L", "LINEITEM_ORDERS"),
    RecursiveRoleDefinition("C", "customer", "CUSTOMER", "O", "ORDERS_CUSTOMER"),
    RecursiveRoleDefinition("PS", "partsupp", "PARTSUPP", "L", "LINEITEM_PARTSUPP"),
    RecursiveRoleDefinition("S", "supplier", "SUPPLIER", "PS", "PARTSUPP_SUPPLIER"),
    RecursiveRoleDefinition("P", "part", "PART", "PS", "PARTSUPP_PART"),
    RecursiveRoleDefinition("N1", "nation1", "NATION", "C", "CUSTOMER_NATION"),
    RecursiveRoleDefinition("N2", "nation2", "NATION", "S", "SUPPLIER_NATION"),
    RecursiveRoleDefinition("R1", "region1", "REGION", "N1", "NATION_REGION"),
    RecursiveRoleDefinition("R2", "region2", "REGION", "N2", "NATION_REGION"),
)
RECURSIVE_ROLES_BY_SYMBOL: dict[str, RecursiveRoleDefinition] = {
    role.symbol: role for role in RECURSIVE_ROLE_DEFINITION_ORDER
}
RECURSIVE_CANONICAL_ROLE_ORDER = tuple(RECURSIVE_ROLES_BY_SYMBOL)
RECURSIVE_MIN_ROLE_COUNT = 3
RECURSIVE_MAX_ROLE_COUNT = 10
EXPECTED_RECURSIVE_CASE_COUNTS: dict[int, int] = {
    3: 9,
    4: 9,
    5: 9,
    6: 8,
    7: 7,
    8: 5,
    9: 3,
    10: 1,
}


def canonical_rooted_role_orders(
    role_count: int,
) -> tuple[tuple[str, ...], ...]:
    """Enumerate connected rooted folds in stable canonical role order."""

    valid: list[tuple[str, ...]] = []
    for symbols in combinations(RECURSIVE_CANONICAL_ROLE_ORDER, role_count):
        if all(
            RECURSIVE_ROLES_BY_SYMBOL[symbol].parent_symbol in symbols[:role_index]
            for role_index, symbol in enumerate(symbols[1:], start=1)
        ):
            valid.append(symbols)
    return tuple(valid)


def recursive_case_id(role_symbols: Sequence[str]) -> str:
    return "_".join(
        RECURSIVE_ROLES_BY_SYMBOL[symbol].display_name for symbol in role_symbols
    )


# The historical name is retained locally to minimize unrelated churn; values
# are logical role symbols, not physical labels.
RECURSIVE_CHAIN_LABELS: dict[str, tuple[str, ...]] = {
    recursive_case_id(symbols): symbols
    for role_count in range(
        RECURSIVE_MIN_ROLE_COUNT,
        RECURSIVE_MAX_ROLE_COUNT + 1,
    )
    for symbols in canonical_rooted_role_orders(role_count)
}
EXTENDED_CASE_ORDER = tuple(RECURSIVE_CHAIN_LABELS)

# Preserve the unambiguous pre-v4 CLI names while reports and new invocations
# use explicit branch roles.  Aliases do not add catalog cases.
LEGACY_RECURSIVE_CASE_ALIASES: dict[str, str] = {
    "supplier_nation_region": "supplier_nation2_region2",
    "customer_nation_region": "customer_nation1_region1",
    "orders_customer_nation": "orders_customer_nation1",
    "orders_customer_nation_region": "orders_customer_nation1_region1",
    "partsupp_supplier_nation": "partsupp_supplier_nation2",
    "partsupp_supplier_nation_region": "partsupp_supplier_nation2_region2",
    "partsupp_supplier_part_nation": "partsupp_supplier_part_nation2",
    "lineitem_orders_customer_nation": ("lineitem_orders_customer_nation1"),
    "lineitem_orders_customer_nation_region": (
        "lineitem_orders_customer_nation1_region1"
    ),
    "lineitem_partsupp_supplier_nation": ("lineitem_partsupp_supplier_nation2"),
    "lineitem_partsupp_supplier_nation_region": (
        "lineitem_partsupp_supplier_nation2_region2"
    ),
}

DEFAULT_RECURSIVE_UPDATE_BY_TABLE: dict[str, str] = {
    "CUSTOMER": "c_mktsegment",
    "PARTSUPP": "ps_supplycost",
    "SUPPLIER": "s_phone",
    "PART": "p_name",
    "NATION": "n_name",
    "REGION": "r_name",
}
DEFAULT_RECURSIVE_UPDATE_PROPERTIES: dict[str, str] = {
    case_id: DEFAULT_RECURSIVE_UPDATE_BY_TABLE[
        RECURSIVE_ROLES_BY_SYMBOL[symbols[-1]].table_label
    ]
    for case_id, symbols in RECURSIVE_CHAIN_LABELS.items()
}

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


def validate_full_update_property_catalog() -> None:
    if set(FULL_LAYOUT_OCCURRENCE_PROPERTIES) != set(TABLES):
        raise ExperimentValidationError(
            "full layout occurrence properties must cover exactly the eight "
            "TPC-H tables; got "
            f"{sorted(FULL_LAYOUT_OCCURRENCE_PROPERTIES)!r}"
        )
    expected_update_tables = set(TABLES) - set(FULL_UPDATE_EXCLUDED_TABLES)
    if set(FULL_UPDATE_PROPERTIES) != expected_update_tables:
        raise ExperimentValidationError(
            "full replacement scalar update properties must cover exactly "
            "the seven non-LINEITEM tables; got "
            f"{sorted(FULL_UPDATE_PROPERTIES)!r}"
        )
    if "LINEITEM" in FULL_UPDATE_PROPERTIES:
        raise ExperimentValidationError(
            "LINEITEM must remain excluded from the scalar update workload"
        )
    for table_label, property_name in FULL_LAYOUT_OCCURRENCE_PROPERTIES.items():
        table = TABLES[table_label]
        blocked = set(table.key_fields)
        blocked.update(topology_fk_properties_for(table))
        if property_name not in table.columns:
            raise ExperimentValidationError(
                f"full replacement update property {table_label}."
                f"{property_name} does not exist"
            )
        if property_name in blocked:
            raise ExperimentValidationError(
                f"full replacement update property {table_label}."
                f"{property_name} is a key/topology field"
            )


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


def direct_boundary_is_hard_blocked(
    join: RelationshipSpec,
    boundary_relationship: RelationshipSpec,
) -> bool:
    """Keep the opposite live Nation parent on the normalized Nation branch.

    Folding Supplier-Nation must not turn Customer-Nation into Customer->SN;
    the mirror rule applies when Customer-Nation is folded.  The source edge
    remains available on the read-only graph and is represented by the
    retained-Nation component in the configuration-redundancy snapshot.
    """

    blocked_by_join = {
        "SUPPLIER_NATION": "CUSTOMER_NATION",
        "CUSTOMER_NATION": "SUPPLIER_NATION",
    }
    return blocked_by_join.get(join.relationship_type) == (
        boundary_relationship.relationship_type
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
        if direct_boundary_is_hard_blocked(join, relationship):
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
        prop for prop in referenced.columns if prop not in referenced.key_fields
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
        normalized_join_type=shadow_relationship_type(join.relationship_type),
    )
    default_update = DEFAULT_UPDATE_PROPERTIES.get(case_id)
    if default_update is None:
        raise ExperimentValidationError(f"missing TPC-H update metadata for {case_id}")
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
    for pair in (build_pair_spec(relationship) for relationship in RELATIONSHIPS)
}


def recursive_relationship_for_role(
    role_symbol: str,
) -> RelationshipSpec:
    definition = RECURSIVE_ROLES_BY_SYMBOL[role_symbol]
    if definition.parent_symbol is None or definition.relationship_type is None:
        raise ExperimentValidationError(
            f"recursive root role {role_symbol} has no parent relationship"
        )
    parent = RECURSIVE_ROLES_BY_SYMBOL[definition.parent_symbol]
    matches = tuple(
        relationship
        for relationship in RELATIONSHIPS
        if relationship.relationship_type == definition.relationship_type
        and relationship.source_label == parent.table_label
        and relationship.target_label == definition.table_label
        and relationship.row_label == parent.table_label
    )
    if len(matches) != 1:
        raise ExperimentValidationError(
            f"recursive role edge {parent.symbol}->{role_symbol} must map "
            f"to exactly one physical FK edge; found "
            f"{[relationship_id(item) for item in matches]!r}"
        )
    return matches[0]


def recursive_stage_connection(
    selected_symbols: Sequence[str],
    added_symbol: str,
) -> tuple[int, RelationshipSpec]:
    definition = RECURSIVE_ROLES_BY_SYMBOL[added_symbol]
    parent_symbol = definition.parent_symbol
    if parent_symbol is None or parent_symbol not in selected_symbols:
        raise ExperimentValidationError(
            f"recursive v10 requires the declared parent of {added_symbol} "
            f"in selected roles {tuple(selected_symbols)!r}; expected "
            f"{parent_symbol!r}"
        )
    return (
        selected_symbols.index(parent_symbol),
        recursive_relationship_for_role(added_symbol),
    )


def recursive_boundary_copy_type(
    owner_role_symbol: str,
    external_role_symbol: str,
    relationship: RelationshipSpec,
) -> str:
    """Give every logical edge its own inherited relationship type."""

    return shadow_relationship_type(
        "RECURSIVE_"
        f"{owner_role_symbol}_{external_role_symbol}_"
        f"{relationship.relationship_type}"
    )


def recursive_role_folded_property(
    role_symbol: str,
    logical_name: str,
) -> str:
    """Namespace repeated Nation/Region role values on folded MV nodes."""

    prefix_by_role = {
        "N1": "n1",
        "N2": "n2",
        "R1": "r1",
        "R2": "r2",
    }
    role_prefix = prefix_by_role.get(role_symbol)
    if role_prefix is None:
        return logical_name
    _physical_prefix, separator, suffix = logical_name.partition("_")
    if not separator or not suffix:
        raise ExperimentValidationError(
            f"cannot namespace {logical_name!r} for role {role_symbol}"
        )
    return f"{role_prefix}_{suffix}"


def recursive_effective_boundaries(
    role_symbols: Sequence[str],
) -> tuple[RecursiveBoundarySpec, ...]:
    """Return the logical role-tree cut after one recursive fold."""

    if len(role_symbols) != len(set(role_symbols)):
        raise ExperimentValidationError("recursive v10 role symbols must be unique")
    selected = set(role_symbols)
    boundaries: list[RecursiveBoundarySpec] = []
    for external_definition in RECURSIVE_ROLE_DEFINITION_ORDER:
        parent_symbol = external_definition.parent_symbol
        if parent_symbol is None:
            continue
        child_symbol = external_definition.symbol
        parent_selected = parent_symbol in selected
        child_selected = child_symbol in selected
        if parent_selected == child_selected:
            continue
        relationship = recursive_relationship_for_role(child_symbol)
        if parent_selected:
            owner_symbol = parent_symbol
            external_symbol = child_symbol
            direction: Literal["incoming", "outgoing"] = "outgoing"
        else:
            owner_symbol = child_symbol
            external_symbol = parent_symbol
            direction = "incoming"
        external_label = RECURSIVE_ROLES_BY_SYMBOL[external_symbol].table_label
        boundaries.append(
            RecursiveBoundarySpec(
                relationship=relationship,
                owner_role_index=role_symbols.index(owner_symbol),
                owner_role_symbol=owner_symbol,
                external_role_symbol=external_symbol,
                direction=direction,
                external_label=external_label,
                copy_type=recursive_boundary_copy_type(
                    parent_symbol,
                    child_symbol,
                    relationship,
                ),
            )
        )
    return tuple(
        sorted(
            boundaries,
            key=lambda item: (
                item.owner_role_index,
                item.direction,
                item.external_role_symbol,
            ),
        )
    )


def build_recursive_case_spec(
    case_id: str,
    role_symbols: Sequence[str],
) -> RecursiveCaseSpec:
    if len(role_symbols) < RECURSIVE_MIN_ROLE_COUNT:
        raise ExperimentValidationError(
            f"{case_id}: recursive cases need at least three roles"
        )
    if len(role_symbols) != len(set(role_symbols)):
        raise ExperimentValidationError(
            f"{case_id}: recursive v10 rejects repeated role symbols"
        )
    try:
        definitions = tuple(
            RECURSIVE_ROLES_BY_SYMBOL[symbol] for symbol in role_symbols
        )
        tables = tuple(TABLES[definition.table_label] for definition in definitions)
    except KeyError as exc:
        raise ExperimentValidationError(
            f"{case_id}: unknown TPC-H recursive role {exc.args[0]!r}"
        ) from exc
    stage_connections = tuple(
        recursive_stage_connection(
            tuple(role_symbols[:added_role_index]),
            role_symbols[added_role_index],
        )
        for added_role_index in range(1, len(role_symbols))
    )
    owner_role_indices = tuple(
        owner_role_index for owner_role_index, _relationship in stage_connections
    )
    joins = tuple(relationship for _owner_role_index, relationship in stage_connections)

    roles: list[RecursiveRoleSpec] = []
    grain_bindings = tuple(
        PropertyBinding(
            logical_name=prop,
            folded_name=recursive_role_folded_property(
                role_symbols[0],
                prop,
            ),
        )
        for prop in tables[0].columns
    )
    roles.append(
        RecursiveRoleSpec(
            role_index=0,
            symbol=role_symbols[0],
            display_name=definitions[0].display_name,
            table=tables[0],
            property_bindings=grain_bindings,
        )
    )
    physically_copied = {binding.folded_name for binding in grain_bindings}
    for role_index, (definition, table, owner_role_index, join) in enumerate(
        zip(
            definitions[1:],
            tables[1:],
            owner_role_indices,
            joins,
            strict=True,
        ),
        start=1,
    ):
        owner = roles[owner_role_index]
        target_key_to_owner_property = {
            target_property: owner_property
            for target_property, owner_property in join.target_keys
        }
        if set(target_key_to_owner_property) != set(table.key_fields):
            raise ExperimentValidationError(
                f"{case_id}: stage {role_index} does not cover the "
                f"{table.label} primary key"
            )
        bindings: list[PropertyBinding] = []
        for property_name in table.columns:
            if property_name in table.key_fields:
                owner_property = target_key_to_owner_property[property_name]
                folded_name = owner.folded_property(owner_property)
            else:
                folded_name = recursive_role_folded_property(
                    definition.symbol,
                    property_name,
                )
                if folded_name in physically_copied:
                    raise ExperimentValidationError(
                        f"{case_id}: folded property collision for " f"{folded_name!r}"
                    )
                physically_copied.add(folded_name)
            bindings.append(
                PropertyBinding(
                    logical_name=property_name,
                    folded_name=folded_name,
                )
            )
        roles.append(
            RecursiveRoleSpec(
                role_index=role_index,
                symbol=definition.symbol,
                display_name=definition.display_name,
                table=table,
                property_bindings=tuple(bindings),
            )
        )

    stage_labels = tuple(
        f"mv_{case_id}_s{stage_number}" for stage_number in range(1, len(joins) + 1)
    )
    stages: list[RecursiveStageSpec] = []
    for stage_index, join in enumerate(joins):
        stage_number = stage_index + 1
        selected_role_symbols = tuple(role_symbols[: stage_index + 2])
        effective = recursive_effective_boundaries(
            selected_role_symbols,
        )
        inherited: tuple[RecursiveBoundarySpec, ...] = ()
        next_copy_type: str | None = None
        if stage_index + 1 < len(joins):
            planned_boundaries: list[RecursiveBoundarySpec] = []
            for future_stage_index in range(stage_index + 1, len(joins)):
                future_owner_index = owner_role_indices[future_stage_index]
                if future_owner_index > stage_index + 1:
                    continue
                future_join = joins[future_stage_index]
                matches = tuple(
                    boundary
                    for boundary in effective
                    if boundary.relationship == future_join
                    and boundary.owner_role_index == future_owner_index
                    and boundary.external_role_symbol
                    == role_symbols[future_stage_index + 1]
                    and boundary.direction == "outgoing"
                )
                if len(matches) != 1:
                    raise ExperimentValidationError(
                        f"{case_id}: stage {stage_number} cannot carry "
                        f"declared future edge {relationship_id(future_join)} "
                        "from its effective MV boundary"
                    )
                planned_boundaries.append(matches[0])
            inherited = tuple(planned_boundaries)
            next_join = joins[stage_index + 1]
            next_owner_role_index = owner_role_indices[stage_index + 1]
            next_matches = tuple(
                boundary
                for boundary in inherited
                if boundary.relationship == next_join
                and boundary.owner_role_index == next_owner_role_index
                and boundary.external_role_symbol == role_symbols[stage_index + 2]
            )
            if len(next_matches) != 1:
                raise ExperimentValidationError(
                    f"{case_id}: stage {stage_number} did not carry its "
                    f"immediate next edge {relationship_id(next_join)}"
                )
            next_copy_type = next_matches[0].copy_type
        stages.append(
            RecursiveStageSpec(
                stage_number=stage_number,
                owner_role_index=owner_role_indices[stage_index],
                added_role_index=stage_index + 1,
                join=join,
                input_label=(
                    None if stage_index == 0 else stage_labels[stage_index - 1]
                ),
                output_label=stage_labels[stage_index],
                selected_copy_type=(
                    None
                    if stage_index == 0
                    else recursive_boundary_copy_type(
                        role_symbols[owner_role_indices[stage_index]],
                        role_symbols[stage_index + 1],
                        join,
                    )
                ),
                next_copy_type=next_copy_type,
                effective_boundaries_after=effective,
                inherited_boundaries=inherited,
            )
        )

    names = RecursiveNames(
        final_folded_label=stage_labels[-1],
        normalized_prefix_label=f"mv_{case_id}_normalized_prefix",
        normalized_factor_label=(
            f"mv_{case_id}_normalized_{definitions[-1].display_name}"
        ),
        normalized_join_type=shadow_relationship_type(f"recursive_{case_id}_final"),
    )
    default_update = DEFAULT_RECURSIVE_UPDATE_PROPERTIES[case_id]
    final_table = tables[-1]
    blocked_update_properties = set(final_table.key_fields)
    blocked_update_properties.update(topology_fk_properties_for(final_table))
    eligible = tuple(
        prop for prop in final_table.columns if prop not in blocked_update_properties
    )
    if default_update not in eligible:
        raise ExperimentValidationError(
            f"{case_id}: default recursive update property "
            f"{default_update!r} is not eligible; choose from {eligible!r}"
        )
    return RecursiveCaseSpec(
        case_id=case_id,
        roles=tuple(roles),
        joins=joins,
        stages=tuple(stages),
        default_update_property=default_update,
        names=names,
    )


EXTENDED_CASES: dict[str, RecursiveCaseSpec] = {
    case_id: build_recursive_case_spec(case_id, labels)
    for case_id, labels in RECURSIVE_CHAIN_LABELS.items()
}
ALL_CASE_ORDER = (*CASE_ORDER, *EXTENDED_CASE_ORDER)
CASE_CHOICES = (*ALL_CASE_ORDER, *LEGACY_RECURSIVE_CASE_ALIASES)


def canonical_case_id(case_id: str) -> str:
    return LEGACY_RECURSIVE_CASE_ALIASES.get(case_id, case_id)


def validate_case_catalog() -> None:
    if set(TABLE_ALIASES) != set(TABLES):
        raise ExperimentValidationError(
            "table-alias catalog must exactly match imported "
            f"tables: aliases={sorted(TABLE_ALIASES)!r}, "
            f"tables={sorted(TABLES)!r}"
        )
    aliases = tuple(TABLE_ALIASES.values())
    if len(aliases) != len(set(aliases)):
        raise ExperimentValidationError("TPC-H table aliases must be globally unique")
    invalid_aliases = sorted(
        alias for alias in aliases if re.fullmatch(r"[a-z][a-z0-9]*", alias) is None
    )
    if invalid_aliases:
        raise ExperimentValidationError(
            "TPC-H table aliases must match [a-z][a-z0-9]*: " f"{invalid_aliases!r}"
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
        pair.case_id for pair in CASES.values() if not pair.referencing_is_source
    ]
    if reverse_cases:
        raise ExperimentValidationError(
            "every relationship must point from its referencing row table; "
            f"reverse-direction cases={reverse_cases!r}"
        )
    for pair in CASES.values():
        if (
            tuple(
                binding.logical_name for binding in pair.referencing_property_bindings
            )
            != pair.referencing.columns
        ):
            raise ExperimentValidationError(
                f"{pair.case_id} referencing bindings do not cover all "
                "referencing columns in source order"
            )
        if (
            tuple(binding.logical_name for binding in pair.referenced_property_bindings)
            != pair.dependent_properties
        ):
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
        collisions = sorted(set(aliases) & {RUN_ID_PROPERTY, CASE_ID_PROPERTY})
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


def validate_recursive_case_catalog() -> None:
    if tuple(EXTENDED_CASES) != EXTENDED_CASE_ORDER:
        raise ExperimentValidationError(
            "recursive case order does not match its catalog"
        )
    if set(CASES) & set(EXTENDED_CASES):
        raise ExperimentValidationError(
            "pair and recursive case identifiers must be disjoint"
        )
    alias_collisions = set(LEGACY_RECURSIVE_CASE_ALIASES) & set(ALL_CASE_ORDER)
    invalid_alias_targets = set(LEGACY_RECURSIVE_CASE_ALIASES.values()) - set(
        EXTENDED_CASES
    )
    if alias_collisions or invalid_alias_targets:
        raise ExperimentValidationError(
            "invalid legacy recursive case aliases: collisions="
            f"{sorted(alias_collisions)!r}, missing_targets="
            f"{sorted(invalid_alias_targets)!r}"
        )
    actual_counts = Counter(len(case.roles) for case in EXTENDED_CASES.values())
    if dict(sorted(actual_counts.items())) != EXPECTED_RECURSIVE_CASE_COUNTS:
        raise ExperimentValidationError(
            "recursive v10 case counts do not match the ten-role tree: "
            f"actual={dict(sorted(actual_counts.items()))!r}, "
            f"expected={EXPECTED_RECURSIVE_CASE_COUNTS!r}"
        )
    for role_count in range(
        RECURSIVE_MIN_ROLE_COUNT,
        RECURSIVE_MAX_ROLE_COUNT + 1,
    ):
        expected_orders = set(canonical_rooted_role_orders(role_count))
        actual_orders = [
            tuple(role.symbol for role in case.roles)
            for case in EXTENDED_CASES.values()
            if len(case.roles) == role_count
        ]
        if (
            len(actual_orders) != len(set(actual_orders))
            or set(actual_orders) != expected_orders
        ):
            raise ExperimentValidationError(
                f"recursive {role_count}-role catalog is not the complete "
                "canonical rooted-outgoing set: missing="
                f"{sorted(expected_orders - set(actual_orders))!r}, extra="
                f"{sorted(set(actual_orders) - expected_orders)!r}"
            )
    labels: list[str] = []
    relationship_types: list[str] = []
    for case in EXTENDED_CASES.values():
        role_symbols = tuple(role.symbol for role in case.roles)
        if case.case_id != recursive_case_id(role_symbols):
            raise ExperimentValidationError(
                f"{case.case_id}: case id does not match role order "
                f"{role_symbols!r}"
            )
        labels.extend(case.all_artifact_labels)
        relationship_types.append(case.names.normalized_join_type)
        if case.names.final_folded_label != case.stages[-1].output_label:
            raise ExperimentValidationError(
                f"{case.case_id}: final folded label does not match final stage"
            )
        if len(case.joins) != len(case.roles) - 1:
            raise ExperimentValidationError(
                f"{case.case_id}: role/join cardinality mismatch"
            )
        stored_properties: list[str] = []
        for role in case.roles:
            definition = RECURSIVE_ROLES_BY_SYMBOL[role.symbol]
            if (
                role.display_name != definition.display_name
                or role.table.label != definition.table_label
            ):
                raise ExperimentValidationError(
                    f"{case.case_id}: role {role.symbol} does not match its "
                    "recursive definition"
                )
            if (
                tuple(binding.logical_name for binding in role.property_bindings)
                != role.table.columns
            ):
                raise ExperimentValidationError(
                    f"{case.case_id}: role {role.symbol} bindings do not "
                    "cover every physical column in source order"
                )
            stored_properties.extend(
                binding.folded_name
                for binding in role.property_bindings
                if role.role_index == 0
                or binding.logical_name not in role.table.key_fields
            )
        if len(stored_properties) != len(set(stored_properties)):
            duplicates = sorted(
                name for name, count in Counter(stored_properties).items() if count > 1
            )
            raise ExperimentValidationError(
                f"{case.case_id}: duplicate stored folded properties " f"{duplicates!r}"
            )
        reserved_collisions = sorted(
            set(stored_properties)
            & {RUN_ID_PROPERTY, CASE_ID_PROPERTY, RECURSIVE_STAGE_PROPERTY}
        )
        if reserved_collisions:
            raise ExperimentValidationError(
                f"{case.case_id}: folded properties collide with artifact "
                f"metadata: {reserved_collisions!r}"
            )
        for stage_index, stage in enumerate(case.stages):
            if stage.stage_number != stage_index + 1:
                raise ExperimentValidationError(
                    f"{case.case_id}: non-contiguous stage numbering"
                )
            if stage.added_role_index != stage_index + 1:
                raise ExperimentValidationError(
                    f"{case.case_id}: stage {stage.stage_number} has an "
                    "invalid added-role index"
                )
            if not 0 <= stage.owner_role_index < stage.added_role_index:
                raise ExperimentValidationError(
                    f"{case.case_id}: stage {stage.stage_number} owner must "
                    "already be present in the preceding MV"
                )
            owner_label = case.roles[stage.owner_role_index].table.label
            added_label = case.roles[stage.added_role_index].table.label
            owner_symbol = case.roles[stage.owner_role_index].symbol
            added_symbol = case.roles[stage.added_role_index].symbol
            if RECURSIVE_ROLES_BY_SYMBOL[added_symbol].parent_symbol != owner_symbol:
                raise ExperimentValidationError(
                    f"{case.case_id}: stage {stage.stage_number} declares "
                    f"the invalid logical edge {owner_symbol}->{added_symbol}"
                )
            if (
                stage.join.source_label != owner_label
                or stage.join.target_label != added_label
            ):
                raise ExperimentValidationError(
                    f"{case.case_id}: stage {stage.stage_number} join does "
                    "not connect its declared owner and added roles"
                )
            if stage_index and (
                stage.selected_copy_type != case.stages[stage_index - 1].next_copy_type
            ):
                raise ExperimentValidationError(
                    f"{case.case_id}: stage {stage.stage_number} does not "
                    "consume the preceding stage's physical boundary type"
                )
            carried_ids = [
                boundary.boundary_id for boundary in stage.inherited_boundaries
            ]
            if len(carried_ids) != len(set(carried_ids)):
                raise ExperimentValidationError(
                    f"{case.case_id}: stage {stage.stage_number} carries "
                    "a duplicate future boundary"
                )
            carried_types = [
                boundary.copy_type for boundary in stage.inherited_boundaries
            ]
            if len(carried_types) != len(set(carried_types)):
                raise ExperimentValidationError(
                    f"{case.case_id}: stage {stage.stage_number} carries "
                    "ambiguous logical boundary relationship types"
                )
            for boundary in stage.effective_boundaries_after:
                if (
                    case.roles[boundary.owner_role_index].symbol
                    != boundary.owner_role_symbol
                ):
                    raise ExperimentValidationError(
                        f"{case.case_id}: boundary {boundary.boundary_id} "
                        "has an inconsistent owner role"
                    )
            if stage.next_copy_type is not None and stage.next_copy_type not in {
                boundary.copy_type for boundary in stage.inherited_boundaries
            }:
                raise ExperimentValidationError(
                    f"{case.case_id}: stage {stage.stage_number} does not "
                    "carry its immediate next boundary"
                )
            if stage_index:
                previous_boundaries = case.stages[stage_index - 1].inherited_boundaries
                for boundary in stage.inherited_boundaries:
                    if (
                        boundary.owner_role_index != stage.added_role_index
                        and boundary not in previous_boundaries
                    ):
                        raise ExperimentValidationError(
                            f"{case.case_id}: stage {stage.stage_number} "
                            f"cannot carry unavailable {boundary.boundary_id}"
                        )
            expected_selected_type = (
                None
                if stage_index == 0
                else recursive_boundary_copy_type(
                    owner_symbol,
                    added_symbol,
                    stage.join,
                )
            )
            if stage.selected_copy_type != expected_selected_type:
                raise ExperimentValidationError(
                    f"{case.case_id}: stage {stage.stage_number} selected "
                    "the wrong logical boundary type"
                )
        if case.stages[-1].inherited_boundaries:
            raise ExperimentValidationError(
                f"{case.case_id}: final stage must not carry unused boundaries"
            )
    if len(labels) != len(set(labels)):
        raise ExperimentValidationError(
            "recursive stage and normalized labels must be globally unique"
        )
    if len(relationship_types) != len(set(relationship_types)):
        raise ExperimentValidationError(
            "recursive normalized relationship types must be unique"
        )


validate_full_update_property_catalog()
validate_case_catalog()
validate_recursive_case_catalog()


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
            "Measure FD redundancy and compare normalized versus lossless "
            "replacement-layout scalar updates across seven TPC-H tables "
            "(LINEITEM is present in both layouts but not updated) for direct "
            "pairs and inheritance-aware recursive FK-to-PK folds."
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
        choices=CASE_CHOICES,
        help=(
            "run one direct-pair or recursive case; when omitted, the "
            "original eight direct-pair cases run"
        ),
    )
    selection.add_argument(
        "--all-cases",
        "--all",
        action="store_true",
        help=("explicitly run all eight cases sequentially (this is also the default)"),
    )
    selection.add_argument(
        "--all-extended-cases",
        action="store_true",
        help=(
            "run all 59 cases sequentially: the eight direct-pair cases "
            "followed by all 51 inheritance-aware recursive cases"
        ),
    )
    parser.add_argument(
        "--list-cases",
        action="store_true",
        help="print both pair and recursive catalogs without connecting to Neo4j",
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
            "complete copies declared direct-case boundaries and the final "
            "recursive role-tree boundary cut; property-fd isolates "
            f"node-property effects (default: {DEFAULT_TOPOLOGY_MODE})"
        ),
    )
    parser.add_argument(
        "--update-property",
        help=(
            "optional final-layer FD payload diagnostic property; this does "
            "not change the paired seven-table scalar update workload; "
            "custom properties require --case"
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
        "--deep-stage-fingerprints",
        action="store_true",
        help=(
            "recursive cases only: stream-hash every intermediate fold "
            "against its source prefix; by default only the final folded "
            "stage is hash-validated to avoid repeated full-row scans"
        ),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help=(
            "path for the JSON report; defaults to the selected database's "
            "test/redundancy/results/extend/<database>/ directory"
        ),
    )
    parser.add_argument(
        "--cleanup-run-id",
        help=("cleanup-only recovery for one printed run_id left by a hard stop"),
    )
    args = parser.parse_args(argv)
    if args.output_json is None:
        args.output_json = default_output_json_for_database(args.database)
    return args


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
        + ", ".join(f"{quote_ident(key)}: {expression}" for key, expression in entries)
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
    relationship = f"{relationship_alias}:{quote_ident(pair.join.relationship_type)}"
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
        (f"{relationship_alias}:{quote_ident(pair.names.normalized_join_type)}")
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
        boundary.copy_type if copied else boundary.relationship.relationship_type
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
        f"{alias}.{quote_ident(prop)}" for prop in pair.folded_referencing_fk_fields
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
    return tuple(f"{alias}.{quote_ident(prop)}" for prop in pair.referenced.key_fields)


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
                referencing_values[prop] for prop in pair.referencing.key_fields
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


def recursive_parameters(
    case: RecursiveCaseSpec,
    run_id: str,
) -> dict[str, Any]:
    return {"run_id": run_id, "case_id": case.case_id}


def recursive_chain_pattern(
    case: RecursiveCaseSpec,
    role_count: int | None = None,
) -> str:
    """Return one MATCH-clause body per role edge.

    Callers prepend the first ``MATCH``.  Keeping later edges in separate
    MATCH clauses permits R1 and R2 to reuse the same physical NATION_REGION
    relationship when the customer and supplier belong to the same Nation.
    """

    count = len(case.roles) if role_count is None else role_count
    if not 2 <= count <= len(case.roles):
        raise ValueError(f"invalid recursive role count: {count}")
    parts: list[str] = []
    for stage in case.stages[: count - 1]:
        owner_index = stage.owner_role_index
        added_index = stage.added_role_index
        owner = case.roles[owner_index]
        added = case.roles[added_index]
        parts.append(
            f"(role_{owner_index}:{quote_ident(owner.table.label)})"
            f"-[chain_rel_{stage.stage_number}:"
            f"{quote_ident(stage.join.relationship_type)}]->"
            f"(role_{added_index}:{quote_ident(added.table.label)})"
        )
    return "\n        MATCH ".join(parts)


def recursive_role_projection(
    role: RecursiveRoleSpec,
    alias: str,
    *,
    folded: bool,
) -> str:
    if folded:
        entries = tuple(
            (
                binding.logical_name,
                f"{alias}.{quote_ident(binding.folded_name)}",
            )
            for binding in role.property_bindings
        )
    else:
        entries = tuple(
            (prop, f"{alias}.{quote_ident(prop)}") for prop in role.table.columns
        )
    return cypher_map(entries)


def recursive_projection_list(
    case: RecursiveCaseSpec,
    role_count: int,
    alias_for_role: Callable[[int], str],
    *,
    folded: bool,
) -> str:
    return (
        "["
        + ", ".join(
            recursive_role_projection(
                case.roles[index],
                alias_for_role(index),
                folded=folded,
            )
            for index in range(role_count)
        )
        + "]"
    )


def recursive_join_result_sha256(
    result: Any,
    case: RecursiveCaseSpec,
    role_count: int,
) -> tuple[str, int]:
    modulus = 1 << 256
    count = 0
    digest_sum = 0
    digest_xor = 0
    for record in result:
        role_values = [dict(values) for values in record["role_values"]]
        if len(role_values) != role_count:
            raise ExperimentValidationError(
                f"{case.case_id}: fingerprint row contains "
                f"{len(role_values)} roles, expected {role_count}"
            )
        payload = {
            "roles": [
                {
                    "role_index": index,
                    "role_symbol": case.roles[index].symbol,
                    "label": case.roles[index].table.label,
                    "key": [
                        role_values[index][prop]
                        for prop in case.roles[index].table.key_fields
                    ],
                    "values": role_values[index],
                }
                for index in range(role_count)
            ]
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
    return (
        sha256_json(
            {
                "algorithm": RECURSIVE_JOIN_FINGERPRINT_ALGORITHM,
                "count": count,
                "sum_sha256": f"{digest_sum:064x}",
                "xor_sha256": f"{digest_xor:064x}",
            }
        ),
        count,
    )


def original_recursive_join_sha256(
    session: Any,
    case: RecursiveCaseSpec,
    role_count: int,
) -> tuple[str, int]:
    result = session.run(
        f"""
        MATCH {recursive_chain_pattern(case, role_count)}
        RETURN {
            recursive_projection_list(
                case,
                role_count,
                lambda index: f"role_{index}",
                folded=False,
            )
        } AS role_values
        """
    )
    return recursive_join_result_sha256(result, case, role_count)


def folded_recursive_join_sha256(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    role_count: int,
    label: str,
) -> tuple[str, int]:
    result = session.run(
        f"""
        MATCH (
          folded:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
          {artifact_identity_map()}
        )
        RETURN {
            recursive_projection_list(
                case,
                role_count,
                lambda _index: "folded",
                folded=True,
            )
        } AS role_values
        """,
        recursive_parameters(case, run_id),
    )
    return recursive_join_result_sha256(result, case, role_count)


def normalized_recursive_join_sha256(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
) -> tuple[str, int]:
    final_index = len(case.roles) - 1
    prefix_projections = [
        recursive_role_projection(
            case.roles[index],
            "prefix",
            folded=True,
        )
        for index in range(final_index)
    ]
    prefix_projections.append(
        recursive_role_projection(
            case.final_role,
            "factor",
            folded=False,
        )
    )
    result = session.run(
        f"""
        MATCH (
          prefix:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.normalized_prefix_label)}
          {artifact_identity_map()}
        )-[join_rel:{quote_ident(case.names.normalized_join_type)}]->(
          factor:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.normalized_factor_label)}
          {artifact_identity_map()}
        )
        RETURN [{", ".join(prefix_projections)}] AS role_values
        """,
        recursive_parameters(case, run_id),
    )
    return recursive_join_result_sha256(result, case, len(case.roles))


def validate_recursive_source_shape(
    session: Any,
    case: RecursiveCaseSpec,
    *,
    validated_tables: set[str] | None = None,
    validated_relationships: set[str] | None = None,
) -> None:
    """Validate every invariant needed by recursive v10 before any write."""

    table_cache = validated_tables if validated_tables is not None else set()
    relationship_cache = (
        validated_relationships if validated_relationships is not None else set()
    )
    role_by_physical_label: dict[str, RecursiveRoleSpec] = {}
    for role in case.roles:
        if role.table.label not in table_cache:
            role_by_physical_label.setdefault(role.table.label, role)
    roles_to_validate = tuple(role_by_physical_label.values())
    constraint_records: list[Any] = []
    if roles_to_validate:
        constraint_result = session.run(
            """
            SHOW CONSTRAINTS
            YIELD type, entityType, labelsOrTypes, properties
            RETURN type, entityType, labelsOrTypes, properties
            """
        )
        constraint_records, _ = result_records_and_summary(constraint_result)
    missing_constraints: list[str] = []
    for role in roles_to_validate:
        table = role.table
        found = any(
            str(record["entityType"]).upper() == "NODE"
            and table.label in (record["labelsOrTypes"] or ())
            and tuple(record["properties"] or ()) == table.key_fields
            and (
                "UNIQUE" in str(record["type"]).upper()
                or "KEY" in str(record["type"]).upper()
            )
            for record in constraint_records
        )
        if not found:
            missing_constraints.append(f"{table.label}{tuple(table.key_fields)!r}")
    if missing_constraints:
        raise ExperimentValidationError(
            f"{case.case_id}: missing imported TPC-H primary-key "
            f"constraint(s): {', '.join(missing_constraints)}"
        )

    for role in roles_to_validate:
        table = role.table
        key_null_predicate = " OR ".join(
            f"node.{quote_ident(prop)} IS NULL" for prop in table.key_fields
        )
        record, _ = one_record(
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
                  count(node) AS node_count,
                  coalesce(sum(
                    CASE WHEN size(extra_labels) = 0 THEN 0 ELSE 1 END
                  ), 0) AS nodes_with_extra_labels,
                  coalesce(sum(
                    CASE WHEN size(extra_properties) = 0 THEN 0 ELSE 1 END
                  ), 0) AS nodes_with_extra_properties,
                  coalesce(sum(
                    CASE WHEN {key_null_predicate} THEN 1 ELSE 0 END
                  ), 0) AS nodes_with_null_keys
                """,
                expected_label=table.label,
                allowed_properties=list(table.columns),
            ),
            f"validate recursive {table.label} node shape",
        )
        problems = {
            "extra_label_nodes": int(record["nodes_with_extra_labels"]),
            "extra_property_nodes": int(record["nodes_with_extra_properties"]),
            "null_key_nodes": int(record["nodes_with_null_keys"]),
        }
        if not int(record["node_count"]) or any(problems.values()):
            raise ExperimentValidationError(
                f"{case.case_id}: {table.label} violates the declared "
                f"recursive source-node shape: {problems!r}, "
                f"node_count={int(record['node_count'])}"
            )
        table_cache.add(table.label)

    for stage in case.stages:
        stage_number = stage.stage_number
        owner = case.roles[stage.owner_role_index]
        added = case.roles[stage.added_role_index]
        relationship = stage.join
        if relationship.relationship_type in relationship_cache:
            continue
        mismatch_terms = tuple(
            "NOT "
            + equality_predicate(
                f"owner.{quote_ident(owner_property)}",
                f"added.{quote_ident(target_property)}",
            )
            for target_property, owner_property in relationship.target_keys
        )
        mismatch_predicate = " OR ".join(mismatch_terms) or "false"
        record, _ = one_record(
            session.run(
                f"""
                MATCH (owner:{quote_ident(owner.table.label)})
                CALL (owner) {{
                  OPTIONAL MATCH (owner)-[
                    selected:{quote_ident(relationship.relationship_type)}
                  ]->(added:{quote_ident(added.table.label)})
                  RETURN
                    count(selected) AS degree,
                    coalesce(sum(
                      CASE
                        WHEN added IS NOT NULL
                         AND ({mismatch_predicate})
                        THEN 1 ELSE 0
                      END
                    ), 0) AS mismatch_edges,
                    coalesce(sum(
                      CASE
                        WHEN selected IS NOT NULL
                         AND size(keys(selected)) > 0
                        THEN 1 ELSE 0
                      END
                    ), 0) AS relationships_with_properties
                }}
                RETURN
                  count(owner) AS owner_rows,
                  coalesce(sum(
                    CASE WHEN degree = 1 THEN 0 ELSE 1 END
                  ), 0) AS invalid_degrees,
                  coalesce(sum(mismatch_edges), 0) AS mismatch_edges,
                  coalesce(sum(relationships_with_properties), 0)
                    AS relationships_with_properties
                """
            ),
            f"validate {case.case_id} recursive source stage {stage_number}",
        )
        invalid_degrees = int(record["invalid_degrees"])
        mismatches = int(record["mismatch_edges"])
        relationship_properties = int(record["relationships_with_properties"])
        endpoint_record, _ = one_record(
            session.run(
                f"""
                MATCH (source)-[
                  relationship:{quote_ident(relationship.relationship_type)}
                ]->(target)
                RETURN
                  count(relationship) AS typed_relationships,
                  coalesce(sum(
                    CASE
                      WHEN source:{quote_ident(relationship.source_label)}
                       AND target:{quote_ident(relationship.target_label)}
                      THEN 1 ELSE 0
                    END
                  ), 0) AS exact_endpoint_relationships
                """
            ),
            f"validate endpoints for {relationship_id(relationship)}",
        )
        typed_relationships = int(endpoint_record["typed_relationships"])
        exact_endpoint_relationships = int(
            endpoint_record["exact_endpoint_relationships"]
        )
        unsupported_endpoints = typed_relationships - exact_endpoint_relationships
        if (
            invalid_degrees
            or mismatches
            or relationship_properties
            or unsupported_endpoints
        ):
            raise ExperimentValidationError(
                f"{case.case_id}: recursive stage {stage_number} requires "
                f"exactly one FK-matching, property-free "
                f"{relationship.relationship_type} edge per "
                f"{owner.table.label}; owner_rows={int(record['owner_rows'])}, "
                f"invalid_degrees={invalid_degrees}, "
                f"mismatch_edges={mismatches}, relationship_properties="
                f"{relationship_properties}, unsupported_direction_or_"
                f"endpoints={unsupported_endpoints}"
            )
        relationship_cache.add(relationship.relationship_type)


def build_recursive_baseline(
    session: Any,
    case: RecursiveCaseSpec,
    source_sha256: str,
    *,
    deep_stage_fingerprints: bool,
    source_shape_prevalidated: bool = False,
) -> RecursiveBaseline:
    if not source_shape_prevalidated:
        validate_recursive_source_shape(session, case)
    grain_rows = scalar_count(
        session,
        f"MATCH (node:{quote_ident(case.grain.table.label)}) "
        "RETURN count(node) AS count",
        "count",
        f"count {case.grain.table.label} grain rows",
    )
    final_table_rows = scalar_count(
        session,
        f"MATCH (node:{quote_ident(case.final_role.table.label)}) "
        "RETURN count(node) AS count",
        "count",
        f"count {case.final_role.table.label} final-role rows",
    )
    prefix_hashes: list[str | None] = []
    role_counts = (
        range(2, len(case.roles) + 1) if deep_stage_fingerprints else (len(case.roles),)
    )
    chain_rows = grain_rows
    if not deep_stage_fingerprints:
        prefix_hashes.extend([None] * (len(case.roles) - 2))
    for role_count in role_counts:
        fingerprint, rows = original_recursive_join_sha256(
            session,
            case,
            role_count,
        )
        if rows != grain_rows:
            raise ExperimentValidationError(
                f"{case.case_id}: source prefix with {role_count} roles has "
                f"{rows} rows, expected one row for each of the "
                f"{grain_rows} grain rows; recursive v10 requires total "
                "outgoing FK-to-PK participation"
            )
        prefix_hashes.append(fingerprint)
        chain_rows = rows
    final_key_expression = ", ".join(
        f"role_{len(case.roles) - 1}.{quote_ident(prop)}"
        for prop in case.final_role.table.key_fields
    )
    participating_final_rows = scalar_count(
        session,
        f"""
        MATCH {recursive_chain_pattern(case)}
        RETURN count(DISTINCT [{final_key_expression}]) AS count
        """,
        "count",
        f"count participating final keys for {case.case_id}",
    )
    return RecursiveBaseline(
        grain_rows=grain_rows,
        final_table_rows=final_table_rows,
        chain_rows=chain_rows,
        participating_final_rows=participating_final_rows,
        prefix_join_sha256=tuple(prefix_hashes),
        source_sha256=source_sha256,
    )


def recursive_update_pair(case: RecursiveCaseSpec) -> PairSpec:
    """Adapt the last recursive layer to the existing update benchmark API."""

    owner = case.final_owner
    referenced = case.final_role.table
    dependent = case.final_dependent_properties
    fk_mapping = tuple(
        (owner_property, referenced_property)
        for referenced_property, owner_property in case.final_join.target_keys
    )
    return PairSpec(
        case_id=case.case_id,
        referencing=owner.table,
        referenced=referenced,
        join=case.final_join,
        referencing_is_source=True,
        fk_mapping=fk_mapping,
        dependent_properties=dependent,
        referencing_alias=TABLE_ALIASES[owner.table.label],
        referenced_alias=TABLE_ALIASES[referenced.label],
        referencing_property_bindings=owner.property_bindings,
        referenced_property_bindings=tuple(
            binding
            for binding in case.final_role.property_bindings
            if binding.logical_name in dependent
        ),
        topology_fk_properties=topology_fk_properties_for(referenced),
        default_update_property=case.default_update_property,
        boundaries=(),
        names=PairNames(
            folded_label=case.names.final_folded_label,
            referencing_label=case.names.normalized_prefix_label,
            referenced_label=case.names.normalized_factor_label,
            normalized_join_type=case.names.normalized_join_type,
        ),
    )


def recursive_pair_baseline(
    baseline: RecursiveBaseline,
    boundary_expectations: Sequence[RecursiveBoundaryExpectation] = (),
) -> CaseBaseline:
    return CaseBaseline(
        referencing_rows=baseline.grain_rows,
        referenced_rows=baseline.final_table_rows,
        join_rows=baseline.chain_rows,
        participating_referenced_rows=baseline.participating_final_rows,
        join_sha256=baseline.join_sha256,
        source_sha256=baseline.source_sha256,
        boundary_expectations=tuple(
            BoundaryExpectation(
                boundary_id=item.boundary_id,
                owner=item.boundary.owner_role_symbol,
                copy_type=item.boundary.copy_type,
                folded_count=item.folded_count,
                normalized_count=item.normalized_count,
            )
            for item in boundary_expectations
        ),
    )


def recursive_grouped_fanout_query(case: RecursiveCaseSpec) -> str:
    final_alias = f"role_{len(case.roles) - 1}"
    return f"""
    MATCH {recursive_chain_pattern(case)}
    WITH {final_alias} AS referenced, count(*) AS fanout
    WHERE fanout > 0
    RETURN
      {node_projection("referenced", case.final_role.table.columns)}
        AS referenced_values,
      fanout
    """


def configuration_entity_dependent_property_count(
    entity: ConfigurationEntity,
) -> int:
    table = TABLES[entity]
    return len(table.columns) - len(table.key_fields)


def configuration_key_fanout(
    session: Any,
    *,
    match_body: str,
    entity_alias: str,
    entity: ConfigurationEntity,
    description: str,
) -> Counter[tuple[Any, ...]]:
    """Return physical occurrences grouped by one source-entity key."""

    table = TABLES[entity]
    key_expressions = ", ".join(
        f"configuration_entity.{quote_ident(prop)}" for prop in table.key_fields
    )
    result = session.run(
        f"""
        MATCH {match_body}
        WITH {entity_alias} AS configuration_entity, count(*) AS fanout
        WHERE fanout > 0
        RETURN [{key_expressions}] AS entity_key, fanout
        """
    )
    occurrences: Counter[tuple[Any, ...]] = Counter()
    for record in result:
        key = canonical_key(record["entity_key"])
        fanout = int(record["fanout"])
        if not key or fanout <= 0:
            result.consume()
            raise ExperimentValidationError(
                f"{description}: invalid {entity} key/fanout {key!r}/{fanout}"
            )
        occurrences[key] += fanout
    result.consume()
    if not occurrences:
        raise ExperimentValidationError(
            f"{description}: configuration observation is empty"
        )
    return occurrences


def merge_configuration_key_fanout(
    target: dict[
        tuple[str, ConfigurationEntity],
        Counter[tuple[Any, ...]],
    ],
    *,
    component_id: str,
    entity: ConfigurationEntity,
    occurrences: Counter[tuple[Any, ...]],
) -> None:
    target.setdefault((component_id, entity), Counter()).update(occurrences)


def configuration_observations_from_fanout(
    grouped: dict[
        tuple[str, ConfigurationEntity],
        Counter[tuple[Any, ...]],
    ],
) -> tuple[ConfigurationComponentObservation, ...]:
    observations: list[ConfigurationComponentObservation] = []
    for (component_id, entity), occurrences in sorted(
        grouped.items(),
        key=lambda item: (
            item[0][0],
            CONFIGURATION_REDUNDANCY_ENTITY_ORDER.index(item[0][1]),
        ),
    ):
        observations.append(
            ConfigurationComponentObservation(
                component_id=component_id,
                entity=entity,
                occurrence_count=sum(occurrences.values()),
                distinct_keys=frozenset(occurrences),
                dependent_property_count=(
                    configuration_entity_dependent_property_count(entity)
                ),
            )
        )
    return tuple(observations)


def configuration_redundancy_report(
    summary: ConfigurationRedundancySummary,
) -> dict[str, Any]:
    """Serialize a supplemental configuration snapshot without timing claims."""

    component_entities: dict[str, dict[str, Any]] = {}
    by_source_entity: dict[str, dict[str, Any]] = {}
    total_slots = 0
    for entity in summary.entities:
        components: list[dict[str, Any]] = []
        for component in entity.components:
            component_metric = {
                "physical_occurrences_J": component.occurrence_count,
                "distinct_keys_D": component.distinct_count,
                "within_component_redundant_tuple_copies": (
                    component.within_component_redundant_tuple_copies
                ),
                "within_component_redundant_property_slots": (
                    component.within_component_redundant_property_slots
                ),
            }
            components.append(
                {"component_id": component.component_id} | component_metric
            )
            component_entities.setdefault(
                component.component_id,
                {},
            )[entity.entity] = component_metric

        by_source_entity[entity.entity] = {
            "dependent_property_count": entity.dependent_property_count,
            "physical_occurrences_P": entity.occurrence_count,
            "summed_component_distinct_keys": (entity.summed_component_distinct_count),
            "union_distinct_keys_U": entity.union_distinct_count,
            "within_component_redundant_tuple_copies": (
                entity.within_component_redundant_tuple_copies
            ),
            "cross_component_redundant_tuple_copies": (
                entity.cross_component_redundant_tuple_copies
            ),
            "total_redundant_tuple_copies": (entity.total_redundant_tuple_copies),
            "within_component_redundant_property_slots": (
                entity.within_component_redundant_property_slots
            ),
            "cross_component_redundant_property_slots": (
                entity.cross_component_redundant_property_slots
            ),
            "total_redundant_property_slots": (entity.total_redundant_property_slots),
            "components": components,
        }
        total_slots += entity.total_redundant_property_slots

    components_report = [
        {
            "component_id": component_id,
            "kind": (
                "primary"
                if component_id == "primary"
                else (
                    "nation_region_closure"
                    if component_id.endswith("region_closure")
                    else "retained_normalized"
                )
            ),
            "entities": entities,
        }
        for component_id, entities in sorted(component_entities.items())
    ]
    return {
        "schema_version": summary.schema_version,
        "status": "ready",
        "scope": "logical_materialized_configuration_snapshot",
        "identity_policy": (
            "logical N1/N2 and R1/R2 roles are reconciled by physical "
            "source label plus primary key"
        ),
        "formula": {
            "total": "P - U",
            "within_component": "sum(P_i - D_i)",
            "cross_component": "sum(D_i) - U",
        },
        "timing_scope": (
            "supplemental read-only metric; paired full-layout update timing "
            "is reported separately under full_8table_replacement_update"
        ),
        "topology_redundancy_measurement": "not_measured",
        "configuration_update_amplification": (
            "not measured by this NATION/REGION subreport; see the complete "
            "physical target manifest in full_8table_replacement_update"
        ),
        "value_payload_measurement": "not_measured",
        "non_null_value_redundancy": "not_measured",
        "components": components_report,
        "by_source_entity": by_source_entity,
        "totals": {
            "total_redundant_property_slots": total_slots,
            "tuple_copies_are_reported_by_entity": True,
        },
    }


def retained_nation_fanout(
    session: Any,
    *,
    description: str,
) -> Counter[tuple[Any, ...]]:
    return configuration_key_fanout(
        session,
        match_body=f"(configuration_nation:{quote_ident('NATION')})",
        entity_alias="configuration_nation",
        entity="NATION",
        description=description,
    )


def nation_region_component_fanout(
    session: Any,
    *,
    entity: ConfigurationEntity,
    description: str,
) -> Counter[tuple[Any, ...]]:
    alias = "configuration_nation" if entity == "NATION" else "configuration_region"
    return configuration_key_fanout(
        session,
        match_body=(
            f"(configuration_nation:{quote_ident('NATION')})"
            f"-[:{quote_ident('NATION_REGION')}]->"
            f"(configuration_region:{quote_ident('REGION')})"
        ),
        entity_alias=alias,
        entity=entity,
        description=description,
    )


def build_direct_configuration_redundancy(
    session: Any,
    pair: PairSpec,
) -> dict[str, Any] | None:
    """Measure the shared Nation/Region configuration for direct folds."""

    if pair.case_id not in {
        "supplier_nation",
        "customer_nation",
        "nation_region",
    }:
        return None

    grouped: dict[
        tuple[str, ConfigurationEntity],
        Counter[tuple[Any, ...]],
    ] = {}
    match_body = join_pattern(pair)
    if pair.case_id == "nation_region":
        merge_configuration_key_fanout(
            grouped,
            component_id="primary",
            entity="NATION",
            occurrences=configuration_key_fanout(
                session,
                match_body=match_body,
                entity_alias="referencing",
                entity="NATION",
                description=f"{pair.case_id} primary Nation",
            ),
        )
        merge_configuration_key_fanout(
            grouped,
            component_id="primary",
            entity="REGION",
            occurrences=configuration_key_fanout(
                session,
                match_body=match_body,
                entity_alias="referenced",
                entity="REGION",
                description=f"{pair.case_id} primary Region",
            ),
        )
    else:
        merge_configuration_key_fanout(
            grouped,
            component_id="primary",
            entity="NATION",
            occurrences=configuration_key_fanout(
                session,
                match_body=match_body,
                entity_alias="referenced",
                entity="NATION",
                description=f"{pair.case_id} primary Nation",
            ),
        )
        retained_component = (
            "customer_nation_retained"
            if pair.case_id == "supplier_nation"
            else "supplier_nation_retained"
        )
        merge_configuration_key_fanout(
            grouped,
            component_id=retained_component,
            entity="NATION",
            occurrences=retained_nation_fanout(
                session,
                description=f"{pair.case_id} retained Nation",
            ),
        )

    return configuration_redundancy_report(
        calculate_configuration_global_redundancy(
            configuration_observations_from_fanout(grouped)
        )
    )


def recursive_folded_role_edges(
    case: RecursiveCaseSpec,
) -> frozenset[tuple[str, str]]:
    return frozenset(
        (
            case.roles[stage.owner_role_index].symbol,
            case.roles[stage.added_role_index].symbol,
        )
        for stage in case.stages
    )


def build_recursive_configuration_redundancy(
    session: Any,
    case: RecursiveCaseSpec,
) -> dict[str, Any] | None:
    """Measure the final N/R snapshot implied by the physical fold set."""

    edges = recursive_folded_role_edges(case)
    has_customer_nation = ("C", "N1") in edges
    has_supplier_nation = ("S", "N2") in edges
    if not has_customer_nation and not has_supplier_nation:
        return None

    has_customer_region = ("N1", "R1") in edges
    has_supplier_region = ("N2", "R2") in edges
    region_edge_count = int(has_customer_region) + int(has_supplier_region)
    grouped: dict[
        tuple[str, ConfigurationEntity],
        Counter[tuple[Any, ...]],
    ] = {}
    role_by_symbol = {role.symbol: role for role in case.roles}
    selected_nation_symbols = tuple(
        symbol
        for symbol, selected in (
            ("N1", has_customer_nation),
            ("N2", has_supplier_nation),
        )
        if selected
    )
    match_body = recursive_chain_pattern(case)
    for symbol in selected_nation_symbols:
        role = role_by_symbol[symbol]
        merge_configuration_key_fanout(
            grouped,
            component_id="primary",
            entity="NATION",
            occurrences=configuration_key_fanout(
                session,
                match_body=match_body,
                entity_alias=f"role_{role.role_index}",
                entity="NATION",
                description=f"{case.case_id} primary {symbol}",
            ),
        )

    folds_nation_region = region_edge_count > 0
    declared_region_edge_by_nation = {
        "N1": has_customer_region,
        "N2": has_supplier_region,
    }
    implicit_region_roles: list[str] = []
    if folds_nation_region:
        for nation_symbol in selected_nation_symbols:
            if not declared_region_edge_by_nation[nation_symbol]:
                implicit_region_roles.append("R1" if nation_symbol == "N1" else "R2")
            nation_role = role_by_symbol[nation_symbol]
            region_alias = f"configuration_region_{nation_symbol.lower()}"
            region_match = (
                f"{match_body}\n        MATCH "
                f"(role_{nation_role.role_index})"
                f"-[:{quote_ident('NATION_REGION')}]->"
                f"({region_alias}:{quote_ident('REGION')})"
            )
            merge_configuration_key_fanout(
                grouped,
                component_id="primary",
                entity="REGION",
                occurrences=configuration_key_fanout(
                    session,
                    match_body=region_match,
                    entity_alias=region_alias,
                    entity="REGION",
                    description=(f"{case.case_id} primary Region for {nation_symbol}"),
                ),
            )

    if has_customer_nation != has_supplier_nation:
        supplier_side = has_supplier_nation
        component_id = (
            "nation_region_closure"
            if supplier_side and folds_nation_region
            else (
                "supplier_nation_region_closure"
                if folds_nation_region
                else (
                    "customer_nation_retained"
                    if supplier_side
                    else "supplier_nation_retained"
                )
            )
        )
        if folds_nation_region:
            for entity in CONFIGURATION_REDUNDANCY_ENTITY_ORDER:
                merge_configuration_key_fanout(
                    grouped,
                    component_id=component_id,
                    entity=entity,
                    occurrences=nation_region_component_fanout(
                        session,
                        entity=entity,
                        description=f"{case.case_id} {component_id} {entity}",
                    ),
                )
        else:
            merge_configuration_key_fanout(
                grouped,
                component_id=component_id,
                entity="NATION",
                occurrences=retained_nation_fanout(
                    session,
                    description=f"{case.case_id} {component_id}",
                ),
            )

    report = configuration_redundancy_report(
        calculate_configuration_global_redundancy(
            configuration_observations_from_fanout(grouped)
        )
    )
    report["folded_role_edges"] = [list(edge) for edge in sorted(edges)]
    report["implicit_physical_edge_completions"] = [
        {
            "edge": ["N1" if role == "R1" else "N2", role],
            "physical_relationship_type": "NATION_REGION",
            "component_id": "primary",
            "reason": "all physical NATION_REGION occurrences fold together",
        }
        for role in implicit_region_roles
    ]
    return report


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
        accumulator["sum"] = (accumulator["sum"] + record_digest) % modulus
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
                        "properties": dict(record["relationship_properties"]),
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
        f"MATCH (node:{quote_ident(ARTIFACT_LABEL)}) " "RETURN count(node) AS count",
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


def ensure_no_existing_artifacts_for_labels(
    session: Any,
    labels: Sequence[str],
) -> None:
    nodes, relationships = artifact_counts(session)
    if nodes or relationships:
        run_ids = existing_artifact_run_ids(session)
        raise ExperimentStateError(
            "existing MV experiment artifacts must be cleaned first: "
            f"nodes={nodes}, relationships={relationships}, runs={run_ids!r}"
        )
    reserved_labels = sorted(set(labels))
    record, _ = one_record(
        session.run(
            """
            MATCH (node)
            WHERE any(label IN labels(node) WHERE label IN $reserved_labels)
            RETURN count(node) AS count
            """,
            reserved_labels=reserved_labels,
        ),
        "check reserved MV labels",
    )
    if int(record["count"]):
        raise ExperimentStateError(
            "nodes already use one or more reserved MV labels: " f"{reserved_labels!r}"
        )


def ensure_no_existing_artifacts(
    session: Any,
    pairs: Sequence[PairSpec],
) -> None:
    ensure_no_existing_artifacts_for_labels(
        session,
        tuple(
            label
            for pair in pairs
            for label in (
                pair.names.folded_label,
                pair.names.referencing_label,
                pair.names.referenced_label,
            )
        ),
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
        owner_alias = "referencing" if boundary.owner == "referencing" else "referenced"
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
            missing.append(f"{table.label}{tuple(table.key_fields)!r}")
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
        if (relationship.source_label in labels or relationship.target_label in labels)
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


def recursive_schema_name(
    case: RecursiveCaseSpec,
    run_id: str,
    kind: str,
) -> str:
    return safe_schema_token(
        f"{SCHEMA_PREFIX}_recursive_{case.case_id}_{run_id}_{kind}"
    )


def recursive_schema_names(
    case: RecursiveCaseSpec,
    run_id: str,
) -> dict[str, str]:
    return {
        kind: recursive_schema_name(case, run_id, kind)
        for kind in (
            "folded_key",
            "prefix_key",
            "factor_key",
            "folded_final_fk",
            "prefix_final_fk",
        )
    }


def composite_property_expression(
    alias: str,
    properties: Sequence[str],
) -> str:
    expressions = ", ".join(f"{alias}.{quote_ident(prop)}" for prop in properties)
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


def create_recursive_schema(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    wait_seconds: int,
) -> tuple[list[ConstraintLease], list[IndexLease]]:
    names = recursive_schema_names(case, run_id)
    grain_keys = tuple(
        case.grain.folded_property(prop) for prop in case.grain.table.key_fields
    )
    final_keys = case.final_role.table.key_fields
    folded_final_keys = case.final_folded_key_properties
    constraint_definitions = (
        (
            names["folded_key"],
            case.names.final_folded_label,
            (RUN_ID_PROPERTY, *grain_keys),
        ),
        (
            names["prefix_key"],
            case.names.normalized_prefix_label,
            (RUN_ID_PROPERTY, *grain_keys),
        ),
        (
            names["factor_key"],
            case.names.normalized_factor_label,
            (RUN_ID_PROPERTY, *final_keys),
        ),
    )
    index_definitions = (
        (
            names["folded_final_fk"],
            case.names.final_folded_label,
            (RUN_ID_PROPERTY, *folded_final_keys),
        ),
        (
            names["prefix_final_fk"],
            case.names.normalized_prefix_label,
            (RUN_ID_PROPERTY, *folded_final_keys),
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
            constraints.append(ConstraintLease(name=name, created_by_experiment=True))
        for name, label, properties in index_definitions:
            session.run(
                f"""
                CREATE INDEX {quote_ident(name)} IF NOT EXISTS
                FOR (node:{quote_ident(label)})
                ON {composite_property_expression("node", properties)}
                """
            ).consume()
            indexes.append(IndexLease(name=name, created_by_experiment=True))
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
            session.run(f"DROP INDEX {quote_ident(index.name)} IF EXISTS").consume()
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
        details = "; ".join(f"{type(error).__name__}: {error}" for error in errors)
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
    expected.update(
        name
        for case in EXTENDED_CASES.values()
        for name in recursive_schema_names(case, run_id).values()
    )
    dropped_constraints: list[str] = []
    dropped_indexes: list[str] = []

    result = session.run("SHOW CONSTRAINTS YIELD name RETURN name")
    records, _ = result_records_and_summary(result)
    full_schema_prefix = safe_schema_token(f"{SCHEMA_PREFIX}_full_")
    safe_run_id = safe_schema_token(run_id)
    full_schema_pattern = re.compile(
        rf"^{re.escape(full_schema_prefix)}.+_"
        rf"{re.escape(safe_run_id)}_[0-9]+$"
    )
    for record in records:
        name = str(record["name"])
        if name in expected or full_schema_pattern.fullmatch(name) is not None:
            session.run(f"DROP CONSTRAINT {quote_ident(name)} IF EXISTS").consume()
            dropped_constraints.append(name)

    result = session.run("SHOW INDEXES YIELD name RETURN name")
    records, _ = result_records_and_summary(result)
    for record in records:
        name = str(record["name"])
        if name in expected or full_schema_pattern.fullmatch(name) is not None:
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
                "referencing" if boundary.owner == "referencing" else "referenced"
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


def recursive_base_property_map(
    case: RecursiveCaseSpec,
    alias: str,
) -> str:
    return cypher_map(
        tuple(
            (
                binding.folded_name,
                f"{alias}.{quote_ident(binding.logical_name)}",
            )
            for binding in case.grain.property_bindings
        )
    )


def recursive_added_property_map(
    case: RecursiveCaseSpec,
    role_index: int,
    alias: str,
) -> str:
    role = case.roles[role_index]
    keys = set(role.table.key_fields)
    return cypher_map(
        tuple(
            (
                binding.folded_name,
                f"{alias}.{quote_ident(binding.logical_name)}",
            )
            for binding in role.property_bindings
            if binding.logical_name not in keys
        )
    )


def recursive_factor_property_map_from_folded(
    case: RecursiveCaseSpec,
    folded_alias: str,
) -> str:
    return cypher_map(
        tuple(
            (
                binding.logical_name,
                f"{folded_alias}.{quote_ident(binding.folded_name)}",
            )
            for binding in case.final_role.property_bindings
        )
    )


def recursive_final_property_map_from_factor(
    case: RecursiveCaseSpec,
    factor_alias: str,
) -> str:
    keys = set(case.final_role.table.key_fields)
    return cypher_map(
        tuple(
            (
                binding.folded_name,
                f"{factor_alias}.{quote_ident(binding.logical_name)}",
            )
            for binding in case.final_role.property_bindings
            if binding.logical_name not in keys
        )
    )


def recursive_boundary_pattern(
    boundary: RecursiveBoundarySpec,
    owner_alias: str,
    external_alias: str,
    relationship_alias: str,
    *,
    copied: bool,
    owner_label: str | None = None,
    include_external_label: bool = True,
) -> str:
    """Render one role-scoped source or copied recursive boundary."""

    owner = (
        f"{owner_alias}:{quote_ident(owner_label)}"
        if owner_label is not None
        else owner_alias
    )
    external = (
        f"{external_alias}:{quote_ident(boundary.external_label)}"
        if include_external_label
        else external_alias
    )
    relationship_type = (
        boundary.copy_type if copied else boundary.relationship.relationship_type
    )
    relationship = f"{relationship_alias}:{quote_ident(relationship_type)}"
    if boundary.direction == "outgoing":
        return f"({owner})-[{relationship}]->({external})"
    if boundary.direction == "incoming":
        return f"({external})-[{relationship}]->({owner})"
    raise ExperimentValidationError(
        f"unknown recursive boundary direction {boundary.direction!r}"
    )


def recursive_grain_key_match_predicate(
    case: RecursiveCaseSpec,
    shadow_alias: str,
    source_alias: str,
) -> str:
    return strict_node_key_predicates(
        shadow_alias,
        tuple(case.grain.folded_property(prop) for prop in case.grain.table.key_fields),
        tuple(
            f"{source_alias}.{quote_ident(prop)}"
            for prop in case.grain.table.key_fields
        ),
    )


def recursive_final_boundary_expectations(
    session: Any,
    case: RecursiveCaseSpec,
    topology_mode: str,
) -> tuple[RecursiveBoundaryExpectation, ...]:
    """Count final-cut relationships in folded and normalized layouts."""

    if topology_mode == "property-fd":
        return ()
    if topology_mode != "complete":
        raise ValueError(f"unknown topology mode {topology_mode!r}")

    expectations: list[RecursiveBoundaryExpectation] = []
    final_role_index = len(case.roles) - 1
    for boundary in case.final_boundaries:
        owner_alias = f"role_{boundary.owner_role_index}"
        source_pattern = recursive_boundary_pattern(
            boundary,
            owner_alias,
            "external",
            "source_rel",
            copied=False,
        )
        record, _ = one_record(
            session.run(
                f"""
                MATCH {recursive_chain_pattern(case)}
                MATCH {source_pattern}
                RETURN
                  count(source_rel) AS folded_count,
                  count(DISTINCT source_rel) AS distinct_relationship_count
                """
            ),
            f"count recursive final boundary {boundary.boundary_id}",
        )
        folded_count = int(record["folded_count"])
        distinct_relationship_count = int(record["distinct_relationship_count"])

        parallel_record, _ = one_record(
            session.run(
                f"""
                MATCH {recursive_chain_pattern(case)}
                MATCH {source_pattern}
                WITH DISTINCT {owner_alias} AS owner, external, source_rel
                WITH owner, external, count(source_rel) AS relationship_count
                WHERE relationship_count > 1
                RETURN count(*) AS parallel_endpoint_pairs
                """
            ),
            f"validate recursive final boundary {boundary.boundary_id}",
        )
        parallel_endpoint_pairs = int(parallel_record["parallel_endpoint_pairs"])
        if parallel_endpoint_pairs:
            raise ExperimentValidationError(
                f"{case.case_id}: complete recursive topology cannot "
                f"normalize boundary {boundary.boundary_id} with "
                f"{parallel_endpoint_pairs} parallel endpoint pair(s)"
            )

        normalized_count = (
            distinct_relationship_count
            if boundary.owner_role_index == final_role_index
            else folded_count
        )
        expectations.append(
            RecursiveBoundaryExpectation(
                boundary=boundary,
                folded_count=folded_count,
                normalized_count=normalized_count,
            )
        )
    return tuple(expectations)


def recursive_boundary_descriptor(
    expectation: RecursiveBoundaryExpectation,
) -> dict[str, Any]:
    boundary = expectation.boundary
    return {
        "boundary_id": boundary.boundary_id,
        "owner_role": boundary.owner_role_symbol,
        "external_role": boundary.external_role_symbol,
        "direction": boundary.direction,
        "relationship_type": boundary.relationship.relationship_type,
        "copy_type": boundary.copy_type,
        "folded_relationships": expectation.folded_count,
        "normalized_relationships": expectation.normalized_count,
    }


def create_recursive_final_boundary_copies(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    topology_mode: str,
    batch_size: int,
    expectations: Sequence[RecursiveBoundaryExpectation],
) -> dict[str, Any]:
    """Materialize the final role-tree cut on the recursive primary MV."""

    if topology_mode == "property-fd":
        if expectations:
            raise ExperimentValidationError(
                f"{case.case_id}: property-fd mode received final boundary "
                "expectations"
            )
        return {
            "mode": topology_mode,
            "status": "skipped",
            "creation_timing": None,
            "boundaries": [],
            "boundary_relationships": {},
            "boundary_relationship_total": 0,
        }
    if topology_mode != "complete":
        raise ValueError(f"unknown topology mode {topology_mode!r}")
    if tuple(item.boundary for item in expectations) != case.final_boundaries:
        raise ExperimentValidationError(
            f"{case.case_id}: final boundary expectations do not match the "
            "logical role-tree cut"
        )

    parameters = recursive_parameters(case, run_id)
    grain_index_properties = ", ".join(
        quote_ident(prop)
        for prop in (
            RUN_ID_PROPERTY,
            *tuple(
                case.grain.folded_property(prop) for prop in case.grain.table.key_fields
            ),
        )
    )
    totals = MutationTotals()
    created_by_boundary: dict[str, int] = {}
    started_ns = perf_counter_ns()
    for expectation in expectations:
        boundary = expectation.boundary
        owner_alias = f"role_{boundary.owner_role_index}"
        source_pattern = recursive_boundary_pattern(
            boundary,
            owner_alias,
            "external",
            "source_rel",
            copied=False,
        )
        copy_pattern = recursive_boundary_pattern(
            boundary,
            "shadow",
            "external",
            "copy_rel",
            copied=True,
            include_external_label=False,
        )
        result = session.run(
            f"""
            MATCH {recursive_chain_pattern(case)}
            MATCH {source_pattern}
            MATCH (
              shadow:{quote_ident(ARTIFACT_LABEL)}
                    :{quote_ident(case.names.final_folded_label)}
              {artifact_identity_map()}
            )
            USING INDEX shadow:{quote_ident(case.names.final_folded_label)}(
              {grain_index_properties}
            )
            WHERE {
                recursive_grain_key_match_predicate(
                    case,
                    "shadow",
                    "role_0",
                )
            }
            CALL (shadow, external, source_rel) {{
              CREATE {copy_pattern}
              SET copy_rel = properties(source_rel)
            }} IN TRANSACTIONS OF {batch_size} ROWS
            """,
            parameters,
        )
        summary = result.consume()
        totals.add_summary(summary)
        created = int(summary.counters.relationships_created)
        if created != expectation.folded_count:
            raise ExperimentValidationError(
                f"{case.case_id}: final boundary {boundary.boundary_id} "
                f"created {created} relationships, expected "
                f"{expectation.folded_count}"
            )
        created_by_boundary[boundary.boundary_id] = created

    wall_ms = (perf_counter_ns() - started_ns) / 1_000_000
    timing = totals_timing_sample(
        0,
        wall_ms,
        totals,
        affected_nodes=0,
    )
    return {
        "mode": topology_mode,
        "status": "ready",
        "creation_timing": asdict(timing),
        "boundaries": [
            recursive_boundary_descriptor(expectation) for expectation in expectations
        ],
        "boundary_relationships": created_by_boundary,
        "boundary_relationship_total": sum(created_by_boundary.values()),
    }


def recursive_carried_boundaries_clause(
    case: RecursiveCaseSpec,
    stage: RecursiveStageSpec,
) -> str:
    if not stage.inherited_boundaries:
        return ""
    previous_boundaries = (
        ()
        if stage.input_label is None
        else case.stages[stage.stage_number - 2].inherited_boundaries
    )
    available_aliases = (
        ("owner", "added") if stage.input_label is None else ("previous", "added")
    )
    clauses: list[str] = []
    for boundary_index, boundary in enumerate(stage.inherited_boundaries):
        relationship = boundary.relationship
        if boundary.owner_role_index == stage.added_role_index:
            source_alias = "added"
            source_type = relationship.relationship_type
        elif (
            stage.input_label is None
            and boundary.owner_role_index == stage.owner_role_index
        ):
            source_alias = "owner"
            source_type = relationship.relationship_type
        elif stage.input_label is not None and boundary in previous_boundaries:
            source_alias = "previous"
            source_type = boundary.copy_type
        else:
            raise ExperimentValidationError(
                f"{case.case_id}: stage {stage.stage_number} cannot copy "
                f"boundary {boundary.boundary_id} from an unavailable "
                "source role"
            )
        source_relationship = f"source_future_{boundary_index}"
        future_node = f"future_{boundary_index}"
        inherited_relationship = f"inherited_{boundary_index}"
        clauses.extend(
            (
                f"      WITH output, {', '.join(available_aliases)}",
                f"      MATCH ({source_alias})-[{source_relationship}:"
                f"{quote_ident(source_type)}]->(",
                f"        {future_node}:" f"{quote_ident(boundary.external_label)}",
                "      )",
                f"      CREATE (output)-[{inherited_relationship}:"
                f"{quote_ident(boundary.copy_type)}]->({future_node})",
                f"      SET {inherited_relationship} = "
                f"properties({source_relationship})",
            )
        )
    return "\n".join(clauses)


def validate_recursive_selected_boundary(
    session: Any,
    case: RecursiveCaseSpec,
    stage: RecursiveStageSpec,
    run_id: str,
    expected_rows: int,
) -> None:
    if stage.input_label is None or stage.selected_copy_type is None:
        return
    record, _ = one_record(
        session.run(
            f"""
            MATCH (
              previous:{quote_ident(ARTIFACT_LABEL)}
                       :{quote_ident(stage.input_label)}
              {artifact_identity_map()}
            )
            CALL (previous) {{
              OPTIONAL MATCH (previous)-[
                selected:{quote_ident(stage.selected_copy_type)}
              ]->(:{quote_ident(case.roles[stage.added_role_index].table.label)})
              RETURN count(selected) AS degree
            }}
            RETURN
              count(previous) AS previous_nodes,
              coalesce(sum(degree), 0) AS selected_edges,
              coalesce(sum(CASE WHEN degree = 1 THEN 0 ELSE 1 END), 0)
                AS invalid_degrees
            """,
            recursive_parameters(case, run_id),
        ),
        f"validate inherited edge before {case.case_id} stage " f"{stage.stage_number}",
    )
    actual = {
        "previous_nodes": int(record["previous_nodes"]),
        "selected_edges": int(record["selected_edges"]),
        "invalid_degrees": int(record["invalid_degrees"]),
    }
    expected = {
        "previous_nodes": expected_rows,
        "selected_edges": expected_rows,
        "invalid_degrees": 0,
    }
    if actual != expected:
        raise ExperimentValidationError(
            f"{case.case_id}: stage {stage.stage_number} must consume "
            "exactly one physically inherited outgoing edge per preceding "
            f"MV row; actual={actual!r}, expected={expected!r}"
        )


def recursive_stage_creation_query(
    case: RecursiveCaseSpec,
    stage: RecursiveStageSpec,
    batch_size: int,
) -> str:
    """Build the exact mutating Cypher used for one recursive stage."""

    carried_clause = recursive_carried_boundaries_clause(case, stage)
    if stage.input_label is None:
        owner = case.roles[stage.owner_role_index]
        added = case.roles[stage.added_role_index]
        match_clause = (
            f"MATCH (owner:{quote_ident(owner.table.label)})"
            f"-[selected:{quote_ident(stage.join.relationship_type)}]->"
            f"(added:{quote_ident(added.table.label)})"
        )
        initial_properties = (
            f"SET output = {recursive_base_property_map(case, 'owner')}\n"
            f"      SET output += "
            f"{recursive_added_property_map(case, stage.added_role_index, 'added')}"
        )
        subquery_scope = "owner, added"
    else:
        match_clause = f"""
        MATCH (
          previous:{quote_ident(ARTIFACT_LABEL)}
                  :{quote_ident(stage.input_label)}
          {artifact_identity_map()}
        )-[selected:{quote_ident(stage.selected_copy_type or '')}]->(
          added:{quote_ident(case.roles[stage.added_role_index].table.label)}
        )
        """
        initial_properties = (
            "SET output = properties(previous)\n"
            f"      SET output += "
            f"{recursive_added_property_map(case, stage.added_role_index, 'added')}"
        )
        subquery_scope = "previous, added"

    return f"""
        {match_clause}
        CALL ({subquery_scope}) {{
          CREATE (
            output:{quote_ident(ARTIFACT_LABEL)}
                  :{quote_ident(stage.output_label)}
          )
          {initial_properties}
          SET output.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
              output.{quote_ident(CASE_ID_PROPERTY)} = $case_id,
              output.{quote_ident(RECURSIVE_STAGE_PROPERTY)} = $stage_number
          {carried_clause}
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """


def create_recursive_stage(
    session: Any,
    case: RecursiveCaseSpec,
    stage: RecursiveStageSpec,
    run_id: str,
    baseline: RecursiveBaseline,
    batch_size: int,
    *,
    deep_stage_fingerprints: bool,
) -> dict[str, Any]:
    parameters = recursive_parameters(case, run_id)
    expected_rows = baseline.grain_rows
    validate_recursive_selected_boundary(
        session,
        case,
        stage,
        run_id,
        expected_rows,
    )
    creation_totals = MutationTotals()
    creation_started_ns = perf_counter_ns()
    result = session.run(
        recursive_stage_creation_query(case, stage, batch_size),
        parameters | {"stage_number": stage.stage_number},
    )
    summary = result.consume()
    creation_wall_ms = (perf_counter_ns() - creation_started_ns) / 1_000_000
    creation_totals.add_summary(summary)
    created_nodes = int(summary.counters.nodes_created)
    created_boundaries = int(summary.counters.relationships_created)
    expected_boundaries = expected_rows * len(stage.inherited_boundaries)
    if created_nodes != expected_rows or created_boundaries != expected_boundaries:
        raise ExperimentValidationError(
            f"{case.case_id}: stage {stage.stage_number} materialization "
            f"created nodes={created_nodes}, boundaries={created_boundaries}; "
            f"expected nodes={expected_rows}, boundaries={expected_boundaries}"
        )

    expected_fingerprint = baseline.prefix_join_sha256[stage.stage_number - 1]
    validate_fingerprint = deep_stage_fingerprints or stage.stage_number == len(
        case.stages
    )
    fingerprint: str | None = None
    if validate_fingerprint:
        if expected_fingerprint is None:
            raise ExperimentStateError(
                f"{case.case_id}: source fingerprint missing for stage "
                f"{stage.stage_number}"
            )
        fingerprint, fingerprint_rows = folded_recursive_join_sha256(
            session,
            case,
            run_id,
            stage.added_role_index + 1,
            stage.output_label,
        )
        if fingerprint_rows != expected_rows or fingerprint != expected_fingerprint:
            raise ExperimentValidationError(
                f"{case.case_id}: stage {stage.stage_number} folded values "
                "differ from the corresponding source-chain prefix"
            )

    deleted_previous_nodes = 0
    deleted_previous_relationships = 0
    deletion_timing: dict[str, Any] | None = None
    if stage.input_label is not None:
        previous_stage = case.stages[stage.stage_number - 2]
        expected_deleted_relationships = expected_rows * len(
            previous_stage.inherited_boundaries
        )
        deletion_started_ns = perf_counter_ns()
        delete_result = session.run(
            f"""
            MATCH (
              previous:{quote_ident(ARTIFACT_LABEL)}
                      :{quote_ident(stage.input_label)}
              {artifact_identity_map()}
            )
            CALL (previous) {{
              DETACH DELETE previous
            }} IN TRANSACTIONS OF {batch_size} ROWS
            """,
            parameters,
        )
        delete_summary = delete_result.consume()
        deletion_wall_ms = (perf_counter_ns() - deletion_started_ns) / 1_000_000
        deleted_previous_nodes = int(delete_summary.counters.nodes_deleted)
        deleted_previous_relationships = int(
            delete_summary.counters.relationships_deleted
        )
        if (
            deleted_previous_nodes != expected_rows
            or deleted_previous_relationships != expected_deleted_relationships
        ):
            raise ExperimentValidationError(
                f"{case.case_id}: stage {stage.stage_number} deleted "
                f"{deleted_previous_nodes} preceding MV nodes and "
                f"{deleted_previous_relationships} selected/carried "
                f"boundaries; expected nodes={expected_rows}, "
                "relationships="
                f"{expected_deleted_relationships}"
            )
        deletion_totals = MutationTotals()
        deletion_totals.add_summary(delete_summary)
        deletion_timing = asdict(
            totals_timing_sample(
                stage.stage_number,
                deletion_wall_ms,
                deletion_totals,
                affected_nodes=deleted_previous_nodes,
            )
        )

    creation_timing = totals_timing_sample(
        stage.stage_number,
        creation_wall_ms,
        creation_totals,
        affected_nodes=created_nodes,
    )
    return {
        "stage_number": stage.stage_number,
        "input_label": stage.input_label,
        "output_label": stage.output_label,
        "owner_role": case.roles[stage.owner_role_index].symbol,
        "owner_table": case.roles[stage.owner_role_index].table.label,
        "added_role": case.roles[stage.added_role_index].symbol,
        "added_table": case.roles[stage.added_role_index].table.label,
        "join_relationship_type": stage.join.relationship_type,
        "selected_inherited_relationship_type": stage.selected_copy_type,
        "created_nodes": created_nodes,
        "created_carried_boundary_relationships": created_boundaries,
        "created_next_boundary_relationships": (
            expected_rows if stage.next_copy_type is not None else 0
        ),
        "deleted_previous_stage_nodes": deleted_previous_nodes,
        "deleted_previous_stage_relationships": (deleted_previous_relationships),
        "source_prefix_join_sha256": expected_fingerprint,
        "folded_prefix_join_sha256": fingerprint,
        "value_validation": (
            "full_source_vs_folded_fingerprint"
            if validate_fingerprint
            else "cardinality_and_source_shape_preflight; final-stage_"
            "fingerprint_deferred"
        ),
        "structurally_eligible_boundaries_after": [
            {
                "boundary_id": boundary.boundary_id,
                "relationship_type": boundary.relationship.relationship_type,
                "direction": boundary.direction,
                "owner_role": boundary.owner_role_symbol,
                "external_role": boundary.external_role_symbol,
                "external_label": boundary.external_label,
                "copy_type": boundary.copy_type,
            }
            for boundary in stage.effective_boundaries_after
        ],
        "actually_inherited_for_next_stage": (
            [stage.next_copy_type] if stage.next_copy_type is not None else []
        ),
        "carried_for_future_stages": [
            boundary.copy_type for boundary in stage.inherited_boundaries
        ],
        "creation_timing": asdict(creation_timing),
        "previous_stage_deletion_timing": deletion_timing,
    }


def create_recursive_shadow_projection(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    baseline: RecursiveBaseline,
    batch_size: int,
    *,
    deep_stage_fingerprints: bool,
) -> list[dict[str, Any]]:
    stage_reports: list[dict[str, Any]] = []
    for stage in case.stages:
        if stage.input_label is None:
            input_name = case.roles[stage.owner_role_index].display_label
        else:
            folded_roles = " + ".join(
                role.symbol for role in case.roles[: stage.added_role_index]
            )
            input_name = f"MV[{folded_roles}]"
        print(
            f"  [{stage.stage_number}/{len(case.stages)}] "
            f"{input_name} + "
            f"{case.roles[stage.added_role_index].display_label} via "
            f"{stage.join.relationship_type}",
            flush=True,
        )
        report = create_recursive_stage(
            session,
            case,
            stage,
            run_id,
            baseline,
            batch_size,
            deep_stage_fingerprints=deep_stage_fingerprints,
        )
        stage_reports.append(report)
        print(
            f"      nodes={report['created_nodes']:,} | "
            "carried boundaries="
            f"{report['created_carried_boundary_relationships']:,} | "
            "create="
            f"{report['creation_timing']['client_wall_ms']:.3f} ms",
            flush=True,
        )
    total_creation_ms = sum(
        float(report["creation_timing"]["client_wall_ms"]) for report in stage_reports
    )
    print(f"  Total MV creation: {total_creation_ms:.3f} ms", flush=True)
    return stage_reports


def build_recursive_fold_lifecycle(
    stage_reports: Sequence[Mapping[str, Any]],
    final_boundary_materialization: Mapping[str, Any],
) -> dict[str, Any]:
    """Summarize construction churn before updates and normalization.

    Counts include every recursive stage plus the optional final-boundary
    overlay.  They deliberately exclude benchmark updates, normalization,
    refolding, final cleanup, and all unchanged source-graph objects.
    """

    if not stage_reports:
        raise ExperimentValidationError(
            "recursive fold lifecycle requires at least one stage report"
        )

    def count(
        record: Mapping[str, Any],
        field: str,
        context: str,
    ) -> int:
        value = record.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ExperimentValidationError(
                f"{context}.{field} must be a non-negative integer"
            )
        return value

    created_nodes = sum(
        count(report, "created_nodes", f"stage_reports[{index}]")
        for index, report in enumerate(stage_reports)
    )
    deleted_nodes = sum(
        count(
            report,
            "deleted_previous_stage_nodes",
            f"stage_reports[{index}]",
        )
        for index, report in enumerate(stage_reports)
    )
    transit_relationships = sum(
        count(
            report,
            "created_carried_boundary_relationships",
            f"stage_reports[{index}]",
        )
        for index, report in enumerate(stage_reports)
    )
    deleted_relationships = sum(
        count(
            report,
            "deleted_previous_stage_relationships",
            f"stage_reports[{index}]",
        )
        for index, report in enumerate(stage_reports)
    )
    terminal_relationships = count(
        final_boundary_materialization,
        "boundary_relationship_total",
        "final_boundary_materialization",
    )
    raw_terminal_by_boundary = final_boundary_materialization.get(
        "boundary_relationships"
    )
    if not isinstance(raw_terminal_by_boundary, Mapping):
        raise ExperimentValidationError(
            "final_boundary_materialization.boundary_relationships must " "be an object"
        )
    terminal_by_boundary = sum(
        count(
            raw_terminal_by_boundary,
            str(boundary_id),
            "final_boundary_materialization.boundary_relationships",
        )
        for boundary_id in raw_terminal_by_boundary
    )
    if terminal_by_boundary != terminal_relationships:
        raise ExperimentValidationError(
            "final boundary relationship total differs from its per-boundary " "counts"
        )

    created_relationships = transit_relationships + terminal_relationships
    retained_nodes = created_nodes - deleted_nodes
    retained_relationships = created_relationships - deleted_relationships
    if retained_nodes < 0 or retained_relationships < 0:
        raise ExperimentValidationError(
            "recursive fold lifecycle deleted more artifacts than it created"
        )

    return {
        "scope": "recursive_fold_construction_before_benchmark",
        "excludes": [
            "source_graph_objects",
            "benchmark_updates_and_restores",
            "normalization",
            "refold",
            "final_cleanup",
        ],
        "stage_count": len(stage_reports),
        "source_graph": {
            "nodes_deleted": 0,
            "relationships_deleted": 0,
        },
        "nodes": {
            "created": created_nodes,
            "deleted_during_stage_replacement": deleted_nodes,
            "retained": retained_nodes,
        },
        "relationships": {
            "created": created_relationships,
            "deleted_during_stage_replacement": deleted_relationships,
            "retained": retained_relationships,
            "transit_created": transit_relationships,
            "terminal_boundary_created": terminal_relationships,
        },
    }


def tpch_node_property_cell_summary(
    nodes_by_label: Mapping[str, Any],
    *,
    context: str,
) -> dict[str, Any]:
    """Count logical TPC-H business-property cells for physical nodes.

    A cell is one declared TPC-H column stored on one logical node.  Primary
    keys and foreign keys are included; experiment labels and bookkeeping
    properties such as ``run_id`` are not.  Source relationships are required
    to be property-free elsewhere in the recursive source validation and
    therefore contribute no cells here.
    """

    by_physical_label: dict[str, int] = {}
    columns_per_node: dict[str, int] = {}
    for raw_label, raw_count in nodes_by_label.items():
        label = str(raw_label)
        table = TABLES.get(label)
        if table is None:
            raise ExperimentValidationError(
                f"{context} contains unknown TPC-H node label {label!r}"
            )
        if (
            isinstance(raw_count, bool)
            or not isinstance(raw_count, int)
            or raw_count < 0
        ):
            raise ExperimentValidationError(
                f"{context} {label} node count must be a non-negative integer"
            )
        column_count = len(table.columns)
        columns_per_node[label] = column_count
        by_physical_label[label] = raw_count * column_count

    return {
        "definition": (
            "logical TPC-H node-property cells including declared primary and "
            "foreign-key columns; relationship properties and experiment "
            "bookkeeping properties are excluded"
        ),
        "columns_per_node": columns_per_node,
        "by_physical_label": by_physical_label,
        "total": sum(by_physical_label.values()),
    }


def measure_recursive_fold_source_footprint(
    session: Any,
    case: RecursiveCaseSpec,
) -> dict[str, Any]:
    """Count the source objects touched by the whole logical fold.

    Nodes are the unique physical tables represented by selected roles.  A
    source relationship belongs to the replacement surface when at least one
    endpoint has one of those labels.  This deliberately includes the other
    live Nation parent (for example ``SUPPLIER_NATION`` in a C-N fold), even
    though that relationship is not a primary-role-tree boundary.
    """

    physical_labels = tuple(dict.fromkeys(role.table.label for role in case.roles))
    nodes_by_label = {
        label: scalar_count(
            session,
            f"""
            MATCH (node:{quote_ident(label)})
            WHERE NOT $artifact_label IN labels(node)
            RETURN count(node) AS count
            """,
            "count",
            f"count source {label} nodes for {case.case_id} replacement",
            artifact_label=ARTIFACT_LABEL,
        )
        for label in physical_labels
    }

    selected_labels = set(physical_labels)
    relationship_by_identity: dict[str, RelationshipSpec] = {}
    for relationship in RELATIONSHIPS:
        if (
            relationship.source_label not in selected_labels
            and relationship.target_label not in selected_labels
        ):
            continue
        relationship_by_identity.setdefault(
            relationship_id(relationship),
            relationship,
        )
    internal_relationship_ids = {
        relationship_id(relationship) for relationship in case.joins
    }
    relationships_by_type: dict[str, int] = {}
    internal_relationships_by_type: dict[str, int] = {}
    external_relationships_by_type: dict[str, int] = {}
    for relationship in relationship_by_identity.values():
        count = scalar_count(
            session,
            f"""
            MATCH
              (source:{quote_ident(relationship.source_label)})
              -[relationship:{quote_ident(relationship.relationship_type)}]->
              (target:{quote_ident(relationship.target_label)})
            WHERE NOT $artifact_label IN labels(source)
              AND NOT $artifact_label IN labels(target)
            RETURN count(relationship) AS count
            """,
            "count",
            (
                f"count source {relationship.relationship_type} relationships "
                f"for {case.case_id} replacement"
            ),
            artifact_label=ARTIFACT_LABEL,
        )
        existing = relationships_by_type.get(relationship.relationship_type)
        if existing is not None and existing != count:
            raise ExperimentValidationError(
                f"{case.case_id}: physical relationship type "
                f"{relationship.relationship_type!r} produced inconsistent "
                "replacement counts"
            )
        relationships_by_type[relationship.relationship_type] = count
        destination = (
            internal_relationships_by_type
            if relationship_id(relationship) in internal_relationship_ids
            else external_relationships_by_type
        )
        destination[relationship.relationship_type] = count

    return {
        "scope": "selected_fold_physical_objects_and_incident_schema_edges",
        "identity_policy": (
            "physical source labels and relationship identities are counted "
            "once across logical N1/N2 and R1/R2 aliases"
        ),
        "selected_roles": [role.symbol for role in case.roles],
        "nodes": {
            "by_physical_label": nodes_by_label,
            "total": sum(nodes_by_label.values()),
        },
        "property_cells": tpch_node_property_cell_summary(
            nodes_by_label,
            context=f"{case.case_id} replacement source",
        ),
        "relationships": {
            "by_physical_type": relationships_by_type,
            "total": sum(relationships_by_type.values()),
            "internal_fold": {
                "by_physical_type": internal_relationships_by_type,
                "total": sum(internal_relationships_by_type.values()),
            },
            "external_boundary": {
                "by_physical_type": external_relationships_by_type,
                "total": sum(external_relationships_by_type.values()),
            },
        },
    }


def measure_tpch_source_graph_footprint(session: Any) -> dict[str, Any]:
    """Count the complete declared TPC-H source graph.

    Only the eight source labels in ``TABLES`` and the eight source
    relationship identities in ``RELATIONSHIPS`` belong to this total.  Run
    scoped experiment artifacts are explicitly excluded.
    """

    nodes_by_label: dict[str, int] = {}
    for table in TABLES.values():
        nodes_by_label[table.label] = scalar_count(
            session,
            f"""
            MATCH (node:{quote_ident(table.label)})
            WHERE NOT $artifact_label IN labels(node)
            RETURN count(node) AS count
            """,
            "count",
            f"count complete source graph {table.label} nodes",
            artifact_label=ARTIFACT_LABEL,
        )

    relationships_by_type: dict[str, int] = {}
    relationship_by_identity: dict[str, RelationshipSpec] = {}
    for relationship in RELATIONSHIPS:
        relationship_by_identity.setdefault(
            relationship_id(relationship),
            relationship,
        )
    for relationship in relationship_by_identity.values():
        count = scalar_count(
            session,
            f"""
            MATCH
              (source:{quote_ident(relationship.source_label)})
              -[relationship:{quote_ident(relationship.relationship_type)}]->
              (target:{quote_ident(relationship.target_label)})
            WHERE NOT $artifact_label IN labels(source)
              AND NOT $artifact_label IN labels(target)
            RETURN count(relationship) AS count
            """,
            "count",
            (
                "count complete source graph "
                f"{relationship.relationship_type} relationships"
            ),
            artifact_label=ARTIFACT_LABEL,
        )
        existing = relationships_by_type.get(relationship.relationship_type)
        if existing is not None and existing != count:
            raise ExperimentValidationError(
                "complete source graph relationship type "
                f"{relationship.relationship_type!r} produced inconsistent counts"
            )
        relationships_by_type[relationship.relationship_type] = count

    return {
        "scope": "entire_tpch_source_graph",
        "identity_policy": (
            "all declared TPC-H physical source labels and relationship "
            "identities are counted once; experiment artifacts are excluded"
        ),
        "nodes": {
            "by_physical_label": nodes_by_label,
            "total": sum(nodes_by_label.values()),
        },
        "property_cells": tpch_node_property_cell_summary(
            nodes_by_label,
            context="complete TPC-H source graph",
        ),
        "relationships": {
            "by_physical_type": relationships_by_type,
            "total": sum(relationships_by_type.values()),
        },
    }


def recursive_fold_source_footprint_from_whole_graph(
    case: RecursiveCaseSpec,
    whole_source_graph_footprint: Mapping[str, Any],
) -> dict[str, Any]:
    """Derive one fold's replacement surface from complete source counts."""

    raw_nodes = whole_source_graph_footprint.get("nodes")
    raw_relationships = whole_source_graph_footprint.get("relationships")
    if not isinstance(raw_nodes, Mapping) or not isinstance(
        raw_relationships,
        Mapping,
    ):
        raise ExperimentValidationError(
            "complete TPC-H source graph footprint is malformed"
        )
    all_nodes_by_label = raw_nodes.get("by_physical_label")
    all_relationships_by_type = raw_relationships.get("by_physical_type")
    if not isinstance(all_nodes_by_label, Mapping) or not isinstance(
        all_relationships_by_type,
        Mapping,
    ):
        raise ExperimentValidationError(
            "complete TPC-H source graph count maps are malformed"
        )

    expected_labels = set(TABLES)
    expected_relationship_types = {
        relationship.relationship_type for relationship in RELATIONSHIPS
    }
    if set(all_nodes_by_label) != expected_labels:
        raise ExperimentValidationError(
            "complete TPC-H source node counts must cover exactly the declared "
            "source labels"
        )
    if set(all_relationships_by_type) != expected_relationship_types:
        raise ExperimentValidationError(
            "complete TPC-H source relationship counts must cover exactly the "
            "declared source relationship types"
        )

    def source_count(value: Any, context: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ExperimentValidationError(
                f"complete TPC-H source {context} must be a non-negative integer"
            )
        return value

    nodes_by_label = {
        label: source_count(all_nodes_by_label[label], f"{label} node count")
        for label in dict.fromkeys(role.table.label for role in case.roles)
    }
    selected_labels = set(nodes_by_label)
    internal_relationship_ids = {
        relationship_id(relationship) for relationship in case.joins
    }
    relationship_by_identity: dict[str, RelationshipSpec] = {}
    for relationship in RELATIONSHIPS:
        if (
            relationship.source_label in selected_labels
            or relationship.target_label in selected_labels
        ):
            relationship_by_identity.setdefault(
                relationship_id(relationship),
                relationship,
            )

    relationships_by_type: dict[str, int] = {}
    internal_relationships_by_type: dict[str, int] = {}
    external_relationships_by_type: dict[str, int] = {}
    for identity, relationship in relationship_by_identity.items():
        relationship_type = relationship.relationship_type
        count = source_count(
            all_relationships_by_type[relationship_type],
            f"{relationship_type} relationship count",
        )
        relationships_by_type[relationship_type] = count
        destination = (
            internal_relationships_by_type
            if identity in internal_relationship_ids
            else external_relationships_by_type
        )
        destination[relationship_type] = count

    return {
        "scope": "selected_fold_physical_objects_and_incident_schema_edges",
        "identity_policy": (
            "physical source labels and relationship identities are counted "
            "once across logical N1/N2 and R1/R2 aliases"
        ),
        "selected_roles": [role.symbol for role in case.roles],
        "nodes": {
            "by_physical_label": nodes_by_label,
            "total": sum(nodes_by_label.values()),
        },
        "property_cells": tpch_node_property_cell_summary(
            nodes_by_label,
            context=f"{case.case_id} replacement source",
        ),
        "relationships": {
            "by_physical_type": relationships_by_type,
            "total": sum(relationships_by_type.values()),
            "internal_fold": {
                "by_physical_type": internal_relationships_by_type,
                "total": sum(internal_relationships_by_type.values()),
            },
            "external_boundary": {
                "by_physical_type": external_relationships_by_type,
                "total": sum(external_relationships_by_type.values()),
            },
        },
    }


def direct_fold_source_footprint_from_whole_graph(
    pair: PairSpec,
    whole_source_graph_footprint: Mapping[str, Any],
) -> dict[str, Any]:
    """Derive one direct pair's complete logical replacement surface.

    The surface contains both folded source tables and every declared TPC-H
    relationship incident to either table.  In particular, folding S-N or C-N
    still includes the other live parent of NATION.  That edge is represented
    by a retained-Nation companion rather than copied onto the primary MV.
    """

    raw_nodes = whole_source_graph_footprint.get("nodes")
    raw_relationships = whole_source_graph_footprint.get("relationships")
    if not isinstance(raw_nodes, Mapping) or not isinstance(
        raw_relationships,
        Mapping,
    ):
        raise ExperimentValidationError(
            "complete TPC-H source graph footprint is malformed"
        )
    all_nodes_by_label = raw_nodes.get("by_physical_label")
    all_relationships_by_type = raw_relationships.get("by_physical_type")
    if not isinstance(all_nodes_by_label, Mapping) or not isinstance(
        all_relationships_by_type,
        Mapping,
    ):
        raise ExperimentValidationError(
            "complete TPC-H source graph count maps are malformed"
        )

    expected_labels = set(TABLES)
    expected_relationship_types = {
        relationship.relationship_type for relationship in RELATIONSHIPS
    }
    if set(all_nodes_by_label) != expected_labels:
        raise ExperimentValidationError(
            "complete TPC-H source node counts must cover exactly the declared "
            "source labels"
        )
    if set(all_relationships_by_type) != expected_relationship_types:
        raise ExperimentValidationError(
            "complete TPC-H source relationship counts must cover exactly the "
            "declared source relationship types"
        )

    def source_count(value: Any, context: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ExperimentValidationError(
                f"complete TPC-H source {context} must be a non-negative integer"
            )
        return value

    selected_role_labels = (
        pair.referencing.label,
        pair.referenced.label,
    )
    nodes_by_label = {
        label: source_count(all_nodes_by_label[label], f"{label} node count")
        for label in dict.fromkeys(selected_role_labels)
    }
    selected_labels = set(nodes_by_label)
    internal_relationship_identity = relationship_id(pair.join)
    relationship_by_identity: dict[str, RelationshipSpec] = {}
    for relationship in RELATIONSHIPS:
        if (
            relationship.source_label in selected_labels
            or relationship.target_label in selected_labels
        ):
            relationship_by_identity.setdefault(
                relationship_id(relationship),
                relationship,
            )

    relationships_by_type: dict[str, int] = {}
    internal_relationships_by_type: dict[str, int] = {}
    external_relationships_by_type: dict[str, int] = {}
    for identity, relationship in relationship_by_identity.items():
        relationship_type = relationship.relationship_type
        count = source_count(
            all_relationships_by_type[relationship_type],
            f"{relationship_type} relationship count",
        )
        relationships_by_type[relationship_type] = count
        destination = (
            internal_relationships_by_type
            if identity == internal_relationship_identity
            else external_relationships_by_type
        )
        destination[relationship_type] = count

    return {
        "scope": "selected_fold_physical_objects_and_incident_schema_edges",
        "identity_policy": (
            "the two direct-fold physical source labels and every incident "
            "declared TPC-H relationship identity are counted once"
        ),
        # Keep the v10 replacement shape shared with recursive cases.  Direct
        # role identity is unambiguous, so the physical labels are the roles.
        "selected_roles": list(selected_role_labels),
        "nodes": {
            "by_physical_label": nodes_by_label,
            "total": sum(nodes_by_label.values()),
        },
        "property_cells": tpch_node_property_cell_summary(
            nodes_by_label,
            context=f"{pair.case_id} replacement source",
        ),
        "relationships": {
            "by_physical_type": relationships_by_type,
            "total": sum(relationships_by_type.values()),
            "internal_fold": {
                "by_physical_type": internal_relationships_by_type,
                "total": sum(internal_relationships_by_type.values()),
            },
            "external_boundary": {
                "by_physical_type": external_relationships_by_type,
                "total": sum(external_relationships_by_type.values()),
            },
        },
    }


def build_direct_fold_replacement_footprint(
    pair: PairSpec,
    whole_source_graph_footprint: Mapping[str, Any],
    source_footprint: Mapping[str, Any],
    step0_artifacts: Mapping[str, Any],
    configuration_redundancy: Mapping[str, Any] | None,
    logical_complete_boundary_expectations: Sequence[BoundaryExpectation],
    topology_mode: str,
) -> dict[str, Any]:
    """Compare the complete TPC-H graph before and after one direct fold.

    Logical replacement totals always use complete boundary semantics.  The
    benchmark may still run in ``property-fd`` mode; its physically created
    shadow relationships are reported separately under
    ``actual_materialization``.
    """

    def count(value: Any, context: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ExperimentValidationError(
                f"direct replacement {context} must be a non-negative integer"
            )
        return value

    if topology_mode not in {"complete", "property-fd"}:
        raise ExperimentValidationError(
            f"direct replacement received invalid topology mode {topology_mode!r}"
        )

    whole_nodes = whole_source_graph_footprint.get("nodes")
    whole_relationships = whole_source_graph_footprint.get("relationships")
    whole_property_cells = whole_source_graph_footprint.get("property_cells")
    if (
        whole_source_graph_footprint.get("scope") != "entire_tpch_source_graph"
        or not isinstance(whole_nodes, Mapping)
        or not isinstance(whole_relationships, Mapping)
        or not isinstance(whole_property_cells, Mapping)
    ):
        raise ExperimentValidationError(
            "complete TPC-H source graph replacement footprint is malformed"
        )
    whole_nodes_by_label = whole_nodes.get("by_physical_label")
    whole_relationships_by_type = whole_relationships.get("by_physical_type")
    if not isinstance(whole_nodes_by_label, Mapping) or not isinstance(
        whole_relationships_by_type,
        Mapping,
    ):
        raise ExperimentValidationError(
            "complete TPC-H source graph count maps are malformed"
        )
    if set(whole_nodes_by_label) != set(TABLES):
        raise ExperimentValidationError(
            "complete TPC-H source graph must contain exactly the eight "
            "declared source node labels"
        )
    expected_relationship_types = {
        relationship.relationship_type for relationship in RELATIONSHIPS
    }
    if set(whole_relationships_by_type) != expected_relationship_types:
        raise ExperimentValidationError(
            "complete TPC-H source graph must contain exactly the eight "
            "declared source relationship types"
        )
    validated_whole_nodes_by_label = {
        str(label): count(value, f"whole {label} node count")
        for label, value in whole_nodes_by_label.items()
    }
    validated_whole_relationships_by_type = {
        str(relationship_type): count(
            value,
            f"whole {relationship_type} relationship count",
        )
        for relationship_type, value in whole_relationships_by_type.items()
    }
    whole_node_total = count(whole_nodes.get("total"), "whole source node total")
    whole_relationship_total = count(
        whole_relationships.get("total"),
        "whole source relationship total",
    )
    if whole_node_total != sum(validated_whole_nodes_by_label.values()):
        raise ExperimentValidationError(
            "complete TPC-H source node total differs from its label counts"
        )
    if whole_relationship_total != sum(validated_whole_relationships_by_type.values()):
        raise ExperimentValidationError(
            "complete TPC-H source relationship total differs from its type counts"
        )

    source_nodes = source_footprint.get("nodes")
    source_relationships = source_footprint.get("relationships")
    source_property_cells = source_footprint.get("property_cells")
    if (
        source_footprint.get("scope")
        != "selected_fold_physical_objects_and_incident_schema_edges"
        or source_footprint.get("selected_roles")
        != [pair.referencing.label, pair.referenced.label]
        or not isinstance(source_nodes, Mapping)
        or not isinstance(source_relationships, Mapping)
        or not isinstance(source_property_cells, Mapping)
    ):
        raise ExperimentValidationError(
            "direct source replacement footprint is malformed"
        )
    source_nodes_by_label = source_nodes.get("by_physical_label")
    source_relationships_by_type = source_relationships.get("by_physical_type")
    source_internal = source_relationships.get("internal_fold")
    source_external = source_relationships.get("external_boundary")
    if (
        not isinstance(source_nodes_by_label, Mapping)
        or not isinstance(source_relationships_by_type, Mapping)
        or not isinstance(source_internal, Mapping)
        or not isinstance(source_external, Mapping)
    ):
        raise ExperimentValidationError(
            "direct source replacement count maps are malformed"
        )
    expected_source_labels = {pair.referencing.label, pair.referenced.label}
    if set(source_nodes_by_label) != expected_source_labels:
        raise ExperimentValidationError(
            "direct source replacement labels differ from the folded pair"
        )
    source_node_counts = {
        str(label): count(value, f"source {label} node count")
        for label, value in source_nodes_by_label.items()
    }
    source_relationship_counts = {
        str(relationship_type): count(
            value,
            f"source {relationship_type} relationship count",
        )
        for relationship_type, value in source_relationships_by_type.items()
    }
    expected_incident_types = {
        relationship.relationship_type
        for relationship in RELATIONSHIPS
        if (
            relationship.source_label in expected_source_labels
            or relationship.target_label in expected_source_labels
        )
    }
    if set(source_relationship_counts) != expected_incident_types:
        raise ExperimentValidationError(
            "direct source relationships differ from the complete incident cut"
        )
    for label, value in source_node_counts.items():
        if validated_whole_nodes_by_label[label] != value:
            raise ExperimentValidationError(
                f"direct source {label} count differs from the whole graph"
            )
    for relationship_type, value in source_relationship_counts.items():
        if validated_whole_relationships_by_type[relationship_type] != value:
            raise ExperimentValidationError(
                f"direct source {relationship_type} count differs from the "
                "whole graph"
            )
    source_node_total = count(source_nodes.get("total"), "source node total")
    source_relationship_total = count(
        source_relationships.get("total"),
        "source relationship total",
    )
    if source_node_total != sum(source_node_counts.values()):
        raise ExperimentValidationError(
            "direct source node total differs from its label counts"
        )
    if source_relationship_total != sum(source_relationship_counts.values()):
        raise ExperimentValidationError(
            "direct source relationship total differs from its type counts"
        )

    internal_by_type = source_internal.get("by_physical_type")
    external_by_type = source_external.get("by_physical_type")
    if not isinstance(internal_by_type, Mapping) or not isinstance(
        external_by_type,
        Mapping,
    ):
        raise ExperimentValidationError(
            "direct source internal/external relationship maps are malformed"
        )
    validated_internal = {
        str(relationship_type): count(value, "source internal relationship count")
        for relationship_type, value in internal_by_type.items()
    }
    validated_external = {
        str(relationship_type): count(value, "source external relationship count")
        for relationship_type, value in external_by_type.items()
    }
    source_internal_total = count(
        source_internal.get("total"),
        "source internal relationship total",
    )
    source_external_total = count(
        source_external.get("total"),
        "source external relationship total",
    )
    if (
        validated_internal != {pair.join.relationship_type: source_internal_total}
        or source_internal_total != sum(validated_internal.values())
        or source_external_total != sum(validated_external.values())
        or set(validated_internal) & set(validated_external)
        or validated_internal | validated_external != source_relationship_counts
        or source_internal_total + source_external_total != source_relationship_total
    ):
        raise ExperimentValidationError(
            "direct source internal/external relationship breakdown is inconsistent"
        )

    def validated_property_cell_total(
        summary: Mapping[str, Any],
        node_counts: Mapping[str, Any],
        context: str,
    ) -> int:
        expected = tpch_node_property_cell_summary(node_counts, context=context)
        if (
            summary.get("definition") != expected["definition"]
            or summary.get("columns_per_node") != expected["columns_per_node"]
            or summary.get("by_physical_label") != expected["by_physical_label"]
            or summary.get("total") != expected["total"]
        ):
            raise ExperimentValidationError(
                f"{context} property-cell summary differs from its node counts"
            )
        return expected["total"]

    whole_property_cell_total = validated_property_cell_total(
        whole_property_cells,
        validated_whole_nodes_by_label,
        "complete TPC-H source graph",
    )
    source_property_cell_total = validated_property_cell_total(
        source_property_cells,
        source_node_counts,
        "direct replacement source graph",
    )
    unaffected_source_nodes = whole_node_total - source_node_total
    unaffected_source_relationships = (
        whole_relationship_total - source_relationship_total
    )
    unaffected_source_property_cells = (
        whole_property_cell_total - source_property_cell_total
    )
    if (
        min(
            unaffected_source_nodes,
            unaffected_source_relationships,
            unaffected_source_property_cells,
        )
        < 0
    ):
        raise ExperimentValidationError(
            "direct replacement surface is larger than the whole source graph"
        )

    actual_primary_nodes = count(
        step0_artifacts.get("folded_nodes"),
        "actual primary nodes",
    )
    if actual_primary_nodes != source_internal_total:
        raise ExperimentValidationError(
            "direct folded node count differs from the source join cardinality"
        )
    raw_actual_boundaries = step0_artifacts.get("boundary_relationships")
    if not isinstance(raw_actual_boundaries, Mapping):
        raise ExperimentValidationError(
            "direct actual boundary relationship map is malformed"
        )
    actual_boundary_counts = {
        str(boundary_id): count(value, "actual boundary relationship count")
        for boundary_id, value in raw_actual_boundaries.items()
    }
    actual_primary_relationships = count(
        step0_artifacts.get("boundary_relationship_total"),
        "actual primary relationships",
    )
    if actual_primary_relationships != sum(actual_boundary_counts.values()):
        raise ExperimentValidationError(
            "direct actual boundary relationship total differs from its map"
        )

    boundary_by_id = {boundary.boundary_id: boundary for boundary in pair.boundaries}
    logical_expectations = tuple(logical_complete_boundary_expectations)
    if {expectation.boundary_id for expectation in logical_expectations} != set(
        boundary_by_id
    ):
        raise ExperimentValidationError(
            "direct logical complete boundary expectations differ from the pair"
        )
    primary_by_boundary: dict[str, int] = {}
    primary_by_relationship_type: dict[str, int] = {}
    for expectation in logical_expectations:
        boundary = boundary_by_id[expectation.boundary_id]
        if (
            expectation.owner != boundary.owner
            or expectation.copy_type != boundary.copy_type
        ):
            raise ExperimentValidationError(
                "direct logical boundary metadata differs from the pair"
            )
        folded_count = count(
            expectation.folded_count,
            f"logical boundary {expectation.boundary_id}",
        )
        primary_by_boundary[expectation.boundary_id] = folded_count
        relationship_type = boundary.relationship.relationship_type
        primary_by_relationship_type[relationship_type] = (
            primary_by_relationship_type.get(relationship_type, 0) + folded_count
        )
    primary_relationship_total = sum(primary_by_boundary.values())
    if topology_mode == "complete":
        if actual_boundary_counts != primary_by_boundary:
            raise ExperimentValidationError(
                "complete direct materialization differs from its logical cut"
            )
    elif actual_boundary_counts or actual_primary_relationships:
        raise ExperimentValidationError(
            "property-fd direct materialization unexpectedly created boundaries"
        )

    shared_dimensions: dict[str, dict[str, int]] = {}
    configuration_components: list[Mapping[str, Any]] = []
    if configuration_redundancy is not None:
        if configuration_redundancy.get("status") != "ready":
            raise ExperimentValidationError(
                "direct replacement received non-ready configuration redundancy"
            )
        raw_by_source_entity = configuration_redundancy.get("by_source_entity")
        raw_components = configuration_redundancy.get("components")
        if not isinstance(raw_by_source_entity, Mapping) or not isinstance(
            raw_components,
            list,
        ):
            raise ExperimentValidationError(
                "direct configuration redundancy is malformed"
            )
        shared_field_names = (
            "physical_occurrences_P",
            "union_distinct_keys_U",
            "within_component_redundant_tuple_copies",
            "cross_component_redundant_tuple_copies",
            "total_redundant_tuple_copies",
            "total_redundant_property_slots",
        )
        for raw_entity, raw_metrics in raw_by_source_entity.items():
            entity = str(raw_entity)
            if entity not in CONFIGURATION_REDUNDANCY_ENTITY_ORDER or not isinstance(
                raw_metrics,
                Mapping,
            ):
                raise ExperimentValidationError(
                    "direct configuration contains an invalid shared dimension"
                )
            shared_dimensions[entity] = {
                field_name: count(
                    raw_metrics.get(field_name),
                    f"configuration {entity}.{field_name}",
                )
                for field_name in shared_field_names
            }
        for raw_component in raw_components:
            if not isinstance(raw_component, Mapping):
                raise ExperimentValidationError(
                    "direct configuration contains a malformed component"
                )
            configuration_components.append(raw_component)

    configuration_cases = {"supplier_nation", "customer_nation", "nation_region"}
    if (pair.case_id in configuration_cases) != (configuration_redundancy is not None):
        raise ExperimentValidationError(
            "direct shared-dimension configuration coverage is inconsistent"
        )

    companion_nodes: dict[str, int] = {}
    companion_property_cells: dict[str, dict[str, Any]] = {}
    companion_relationships: dict[str, dict[str, Any]] = {}
    component_ids: set[str] = set()
    for component in configuration_components:
        component_id = component.get("component_id")
        kind = component.get("kind")
        entities = component.get("entities")
        if (
            not isinstance(component_id, str)
            or not component_id
            or not isinstance(kind, str)
            or not isinstance(entities, Mapping)
            or component_id in component_ids
        ):
            raise ExperimentValidationError(
                "direct configuration contains invalid component metadata"
            )
        component_ids.add(component_id)
        if component_id == "primary":
            if kind != "primary":
                raise ExperimentValidationError(
                    "direct primary configuration component has invalid kind"
                )
            continue
        if kind != "retained_normalized":
            raise ExperimentValidationError(
                f"direct companion {component_id!r} must be retained_normalized"
            )
        nation_metrics = entities.get("NATION")
        if not isinstance(nation_metrics, Mapping) or set(entities) != {"NATION"}:
            raise ExperimentValidationError(
                f"direct companion {component_id!r} must contain only NATION"
            )
        component_node_count = count(
            nation_metrics.get("physical_occurrences_J"),
            f"configuration companion {component_id} nodes",
        )
        companion_nodes[component_id] = component_node_count
        properties_per_node = len(TABLES["NATION"].columns)
        companion_property_cells[component_id] = {
            "kind": kind,
            "nodes": component_node_count,
            "properties_per_node": properties_per_node,
            "total": component_node_count * properties_per_node,
        }
        expected_companion = {
            "supplier_nation": (
                "customer_nation_retained",
                "CUSTOMER_NATION",
            ),
            "customer_nation": (
                "supplier_nation_retained",
                "SUPPLIER_NATION",
            ),
        }.get(pair.case_id)
        if expected_companion is None or component_id != expected_companion[0]:
            raise ExperimentValidationError(
                f"unexpected direct configuration companion {component_id!r}"
            )
        parent_relationship_type = expected_companion[1]
        relationships_by_type = {
            parent_relationship_type: count(
                source_relationship_counts.get(parent_relationship_type),
                f"source {parent_relationship_type} count",
            ),
            "NATION_REGION": count(
                source_relationship_counts.get("NATION_REGION"),
                "source NATION_REGION count",
            ),
        }
        companion_relationships[component_id] = {
            "kind": kind,
            "by_source_relationship_type": relationships_by_type,
            "total": sum(relationships_by_type.values()),
        }

    if configuration_components and "primary" not in component_ids:
        raise ExperimentValidationError(
            "direct configuration redundancy has no primary component"
        )
    expected_companion_ids = {
        "supplier_nation": {"customer_nation_retained"},
        "customer_nation": {"supplier_nation_retained"},
    }.get(pair.case_id, set())
    if set(companion_nodes) != expected_companion_ids:
        raise ExperimentValidationError(
            "direct retained-Nation companion set is inconsistent"
        )

    primary_properties_per_node = len(pair.referencing.columns) + (
        len(pair.referenced.columns) - len(pair.referenced.key_fields)
    )
    primary_property_cell_total = actual_primary_nodes * primary_properties_per_node
    folded_property_cell_total = primary_property_cell_total + sum(
        component["total"] for component in companion_property_cells.values()
    )
    folded_node_total = actual_primary_nodes + sum(companion_nodes.values())
    companion_relationship_total = sum(
        component["total"] for component in companion_relationships.values()
    )
    folded_relationship_total = (
        primary_relationship_total + companion_relationship_total
    )
    node_change = folded_node_total - source_node_total
    relationship_change = folded_relationship_total - source_relationship_total
    property_cell_change = folded_property_cell_total - source_property_cell_total
    boundary_connection_change = folded_relationship_total - source_external_total

    whole_folded_node_total = unaffected_source_nodes + folded_node_total
    whole_folded_relationship_total = (
        unaffected_source_relationships + folded_relationship_total
    )
    whole_folded_property_cell_total = (
        unaffected_source_property_cells + folded_property_cell_total
    )
    whole_change = {
        "nodes": whole_folded_node_total - whole_node_total,
        "relationships": (whole_folded_relationship_total - whole_relationship_total),
        "property_cells": (
            whole_folded_property_cell_total - whole_property_cell_total
        ),
    }
    if whole_change != {
        "nodes": node_change,
        "relationships": relationship_change,
        "property_cells": property_cell_change,
    }:
        raise AssertionError(
            "whole-graph and direct replacement-scope changes must be identical"
        )

    return {
        "scope": "entire_tpch_graph_after_logical_fold_replacement",
        "measurement_basis": "whole_tpch_source_graph_counterfactual_replacement",
        "note": (
            "counterfactual replacement comparison; the experiment keeps the "
            "physical source graph unchanged"
        ),
        "whole_graph": {
            "scope": "entire_tpch_graph_after_logical_fold_replacement",
            "source": whole_source_graph_footprint,
            "folded": {
                "nodes": {
                    "unaffected_source": unaffected_source_nodes,
                    "folded_configuration": folded_node_total,
                    "total": whole_folded_node_total,
                },
                "relationships": {
                    "unaffected_source": unaffected_source_relationships,
                    "folded_configuration": folded_relationship_total,
                    "total": whole_folded_relationship_total,
                },
                "property_cells": {
                    "unaffected_source": unaffected_source_property_cells,
                    "folded_configuration": folded_property_cell_total,
                    "total": whole_folded_property_cell_total,
                },
            },
            "change": whole_change,
        },
        "source": source_footprint,
        "folded_configuration": {
            "topology_semantics": "complete",
            "nodes": {
                "primary": actual_primary_nodes,
                "companions": companion_nodes,
                "total": folded_node_total,
            },
            "relationships": {
                "primary_final_cut": {
                    "by_role_scoped_boundary": primary_by_boundary,
                    "by_source_relationship_type": primary_by_relationship_type,
                    "total": primary_relationship_total,
                },
                "companions": companion_relationships,
                "total": folded_relationship_total,
            },
            "property_cells": {
                "definition": (
                    "logical TPC-H business-property cells; declared source "
                    "primary/foreign keys are included and experiment "
                    "bookkeeping properties are excluded"
                ),
                "primary": {
                    "nodes": actual_primary_nodes,
                    "properties_per_node": primary_properties_per_node,
                    "implicit_region_roles": [],
                    "total": primary_property_cell_total,
                },
                "companions": companion_property_cells,
                "total": folded_property_cell_total,
            },
        },
        "change": {
            "nodes": node_change,
            "relationships": relationship_change,
            "property_cells": property_cell_change,
            "boundary_connections": {
                "source": source_external_total,
                "folded": folded_relationship_total,
                "added": boundary_connection_change,
            },
        },
        "actual_materialization": {
            "topology_mode": topology_mode,
            "primary_nodes": actual_primary_nodes,
            "primary_relationships": actual_primary_relationships,
            "companions_materialized": False,
        },
        "shared_dimensions": shared_dimensions,
        "shared_dimension_note": (
            "P is a property occurrence count, not a Neo4j node count; "
            "NATION/REGION configuration metrics are reported separately "
            "from the graph-object replacement totals"
        ),
    }


def build_recursive_fold_replacement_footprint(
    case: RecursiveCaseSpec,
    whole_source_graph_footprint: Mapping[str, Any],
    source_footprint: Mapping[str, Any],
    fold_construction_lifecycle: Mapping[str, Any],
    final_boundary_materialization: Mapping[str, Any],
    configuration_redundancy: Mapping[str, Any] | None,
    logical_complete_boundary_expectations: Sequence[RecursiveBoundaryExpectation],
) -> dict[str, Any]:
    """Compare the complete TPC-H source graph with its folded counterpart.

    The selected replacement surface is removed from the complete source
    graph, then the primary MV plus every retained-Nation or Nation/Region
    closure companion is inserted.  Logical totals always use complete
    boundary semantics.  The separately reported ``actual_materialization``
    keeps the benchmark's ``property-fd``/``complete`` choice observable.
    """

    source_nodes = source_footprint.get("nodes")
    source_relationships = source_footprint.get("relationships")
    source_property_cells = source_footprint.get("property_cells")
    if (
        not isinstance(source_nodes, Mapping)
        or not isinstance(
            source_relationships,
            Mapping,
        )
        or not isinstance(source_property_cells, Mapping)
    ):
        raise ExperimentValidationError(
            "recursive source replacement footprint is malformed"
        )
    source_node_total = source_nodes.get("total")
    source_relationship_total = source_relationships.get("total")
    if (
        isinstance(source_node_total, bool)
        or not isinstance(source_node_total, int)
        or source_node_total < 0
        or isinstance(source_relationship_total, bool)
        or not isinstance(source_relationship_total, int)
        or source_relationship_total < 0
    ):
        raise ExperimentValidationError(
            "recursive source replacement totals must be non-negative integers"
        )

    whole_nodes = whole_source_graph_footprint.get("nodes")
    whole_relationships = whole_source_graph_footprint.get("relationships")
    whole_property_cells = whole_source_graph_footprint.get("property_cells")
    if (
        whole_source_graph_footprint.get("scope") != "entire_tpch_source_graph"
        or not isinstance(whole_nodes, Mapping)
        or not isinstance(whole_relationships, Mapping)
        or not isinstance(whole_property_cells, Mapping)
    ):
        raise ExperimentValidationError(
            "complete TPC-H source graph replacement footprint is malformed"
        )
    whole_nodes_by_label = whole_nodes.get("by_physical_label")
    whole_relationships_by_type = whole_relationships.get("by_physical_type")
    local_nodes_by_label = source_nodes.get("by_physical_label")
    local_relationships_by_type = source_relationships.get("by_physical_type")
    if (
        not isinstance(whole_nodes_by_label, Mapping)
        or not isinstance(whole_relationships_by_type, Mapping)
        or not isinstance(local_nodes_by_label, Mapping)
        or not isinstance(local_relationships_by_type, Mapping)
    ):
        raise ExperimentValidationError(
            "complete or replacement-scope source count maps are malformed"
        )

    def validated_source_count(value: Any, context: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ExperimentValidationError(
                f"recursive replacement {context} must be a non-negative integer"
            )
        return value

    expected_source_labels = set(TABLES)
    expected_source_relationship_types = {
        relationship.relationship_type for relationship in RELATIONSHIPS
    }
    if set(whole_nodes_by_label) != expected_source_labels:
        raise ExperimentValidationError(
            "complete TPC-H source graph must contain exactly the eight "
            "declared source node labels"
        )
    if set(whole_relationships_by_type) != expected_source_relationship_types:
        raise ExperimentValidationError(
            "complete TPC-H source graph must contain exactly the eight "
            "declared source relationship types"
        )
    validated_whole_nodes_by_label = {
        str(label): validated_source_count(count, f"whole {label} node count")
        for label, count in whole_nodes_by_label.items()
    }
    validated_whole_relationships_by_type = {
        str(relationship_type): validated_source_count(
            count,
            f"whole {relationship_type} relationship count",
        )
        for relationship_type, count in whole_relationships_by_type.items()
    }
    whole_node_total = validated_source_count(
        whole_nodes.get("total"),
        "whole source node total",
    )
    whole_relationship_total = validated_source_count(
        whole_relationships.get("total"),
        "whole source relationship total",
    )
    if whole_node_total != sum(validated_whole_nodes_by_label.values()):
        raise ExperimentValidationError(
            "complete TPC-H source node total differs from its label counts"
        )
    if whole_relationship_total != sum(validated_whole_relationships_by_type.values()):
        raise ExperimentValidationError(
            "complete TPC-H source relationship total differs from its type counts"
        )
    if source_node_total != sum(
        validated_source_count(count, f"local {label} node count")
        for label, count in local_nodes_by_label.items()
    ):
        raise ExperimentValidationError(
            "replacement-scope source node total differs from its label counts"
        )
    if source_relationship_total != sum(
        validated_source_count(
            count,
            f"local {relationship_type} relationship count",
        )
        for relationship_type, count in local_relationships_by_type.items()
    ):
        raise ExperimentValidationError(
            "replacement-scope source relationship total differs from its type "
            "counts"
        )
    for label, count in local_nodes_by_label.items():
        if validated_whole_nodes_by_label.get(str(label)) != count:
            raise ExperimentValidationError(
                f"replacement-scope {label} node count differs from the "
                "complete source graph"
            )
    for relationship_type, count in local_relationships_by_type.items():
        if validated_whole_relationships_by_type.get(str(relationship_type)) != count:
            raise ExperimentValidationError(
                f"replacement-scope {relationship_type} relationship count "
                "differs from the complete source graph"
            )

    def validate_source_property_cells(
        raw_summary: Mapping[str, Any],
        node_counts: Mapping[str, Any],
        context: str,
    ) -> int:
        expected = tpch_node_property_cell_summary(
            node_counts,
            context=context,
        )
        raw_columns = raw_summary.get("columns_per_node")
        raw_by_label = raw_summary.get("by_physical_label")
        raw_total = raw_summary.get("total")
        if (
            not isinstance(raw_columns, Mapping)
            or dict(raw_columns) != expected["columns_per_node"]
            or not isinstance(raw_by_label, Mapping)
            or dict(raw_by_label) != expected["by_physical_label"]
            or isinstance(raw_total, bool)
            or not isinstance(raw_total, int)
            or raw_total != expected["total"]
        ):
            raise ExperimentValidationError(
                f"{context} property-cell summary does not match its TPC-H "
                "node counts and declared columns"
            )
        definition = raw_summary.get("definition")
        if not isinstance(definition, str) or not definition:
            raise ExperimentValidationError(
                f"{context} property-cell definition is missing"
            )
        return raw_total

    whole_property_cell_total = validate_source_property_cells(
        whole_property_cells,
        validated_whole_nodes_by_label,
        "complete TPC-H source graph",
    )
    source_property_cell_total = validate_source_property_cells(
        source_property_cells,
        local_nodes_by_label,
        "replacement-scope source graph",
    )
    unaffected_source_nodes = whole_node_total - source_node_total
    unaffected_source_relationships = (
        whole_relationship_total - source_relationship_total
    )
    unaffected_source_property_cells = (
        whole_property_cell_total - source_property_cell_total
    )
    if (
        unaffected_source_nodes < 0
        or unaffected_source_relationships < 0
        or unaffected_source_property_cells < 0
    ):
        raise ExperimentValidationError(
            "replacement scope is larger than the complete TPC-H source graph"
        )

    lifecycle_nodes = fold_construction_lifecycle.get("nodes")
    lifecycle_relationships = fold_construction_lifecycle.get("relationships")
    if not isinstance(lifecycle_nodes, Mapping) or not isinstance(
        lifecycle_relationships,
        Mapping,
    ):
        raise ExperimentValidationError(
            "recursive fold construction lifecycle is malformed"
        )
    actual_primary_nodes = lifecycle_nodes.get("retained")
    actual_primary_relationships = lifecycle_relationships.get("retained")
    materialized_terminal_relationships = final_boundary_materialization.get(
        "boundary_relationship_total"
    )
    for field_name, value in (
        ("actual primary nodes", actual_primary_nodes),
        ("actual primary relationships", actual_primary_relationships),
        (
            "materialized terminal boundary relationships",
            materialized_terminal_relationships,
        ),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ExperimentValidationError(
                f"recursive replacement {field_name} must be a non-negative " "integer"
            )
    if actual_primary_relationships != materialized_terminal_relationships:
        raise ExperimentValidationError(
            "recursive replacement actual primary relationships differ from the "
            "materialized final boundary total"
        )

    shared_dimensions: dict[str, dict[str, int]] = {}
    configuration_components: list[Mapping[str, Any]] = []
    if configuration_redundancy is not None:
        if configuration_redundancy.get("status") != "ready":
            raise ExperimentValidationError(
                "recursive replacement received non-ready configuration " "redundancy"
            )
        by_source_entity = configuration_redundancy.get("by_source_entity")
        if not isinstance(by_source_entity, Mapping):
            raise ExperimentValidationError(
                "configuration redundancy source-entity metrics are malformed"
            )
        field_map = (
            ("physical_occurrences_P", "physical_occurrences_P"),
            ("union_distinct_keys_U", "union_distinct_keys_U"),
            (
                "within_component_redundant_tuple_copies",
                "within_component_redundant_tuple_copies",
            ),
            (
                "cross_component_redundant_tuple_copies",
                "cross_component_redundant_tuple_copies",
            ),
            ("total_redundant_tuple_copies", "total_redundant_tuple_copies"),
            (
                "total_redundant_property_slots",
                "total_redundant_property_slots",
            ),
        )
        for entity, raw_metrics in by_source_entity.items():
            if entity not in CONFIGURATION_REDUNDANCY_ENTITY_ORDER or not isinstance(
                raw_metrics,
                Mapping,
            ):
                raise ExperimentValidationError(
                    "configuration redundancy contains an invalid shared " "dimension"
                )
            metrics: dict[str, int] = {}
            for output_name, source_name in field_map:
                value = raw_metrics.get(source_name)
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ExperimentValidationError(
                        f"configuration {entity}.{source_name} must be a "
                        "non-negative integer"
                    )
                metrics[output_name] = value
            shared_dimensions[str(entity)] = metrics

        raw_components = configuration_redundancy.get("components")
        if not isinstance(raw_components, list):
            raise ExperimentValidationError(
                "configuration redundancy components are malformed"
            )
        for raw_component in raw_components:
            if not isinstance(raw_component, Mapping):
                raise ExperimentValidationError(
                    "configuration redundancy contains a malformed component"
                )
            configuration_components.append(raw_component)

    source_relationship_types = source_relationships.get("by_physical_type")
    source_internal = source_relationships.get("internal_fold")
    source_external = source_relationships.get("external_boundary")
    if (
        not isinstance(source_relationship_types, Mapping)
        or not isinstance(source_internal, Mapping)
        or not isinstance(source_external, Mapping)
    ):
        raise ExperimentValidationError(
            "recursive source replacement relationship breakdown is malformed"
        )

    def non_negative_count(value: Any, context: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ExperimentValidationError(f"{context} must be a non-negative integer")
        return value

    source_internal_total = non_negative_count(
        source_internal.get("total"),
        "recursive source internal relationship total",
    )
    source_external_total = non_negative_count(
        source_external.get("total"),
        "recursive source external relationship total",
    )
    if source_internal_total + source_external_total != source_relationship_total:
        raise ExperimentValidationError(
            "recursive source internal and external relationship totals do not "
            "reconcile"
        )

    folded_edges = recursive_folded_role_edges(case)
    folds_nation_region = bool({("N1", "R1"), ("N2", "R2")} & folded_edges)
    implicit_region_roles = tuple(
        region_symbol
        for nation_edge, region_edge, region_symbol in (
            (("C", "N1"), ("N1", "R1"), "R1"),
            (("S", "N2"), ("N2", "R2"), "R2"),
        )
        if nation_edge in folded_edges
        and folds_nation_region
        and region_edge not in folded_edges
    )
    reported_implicit_completions: tuple[str, ...] = ()
    if configuration_redundancy is not None:
        raw_implicit_completions = configuration_redundancy.get(
            "implicit_physical_edge_completions",
            [],
        )
        if not isinstance(raw_implicit_completions, list):
            raise ExperimentValidationError(
                "configuration implicit physical edge completions are malformed"
            )
        completion_roles: list[str] = []
        for completion in raw_implicit_completions:
            if not isinstance(completion, Mapping):
                raise ExperimentValidationError(
                    "configuration contains a malformed implicit edge completion"
                )
            raw_edge = completion.get("edge")
            if (
                not isinstance(raw_edge, list)
                or len(raw_edge) != 2
                or raw_edge[1] not in {"R1", "R2"}
                or completion.get("physical_relationship_type") != "NATION_REGION"
                or completion.get("component_id") != "primary"
            ):
                raise ExperimentValidationError(
                    "configuration contains an invalid implicit NATION_REGION "
                    "completion"
                )
            completion_roles.append(str(raw_edge[1]))
        reported_implicit_completions = tuple(completion_roles)
        if reported_implicit_completions != implicit_region_roles:
            raise ExperimentValidationError(
                "configuration implicit NATION_REGION completions differ from "
                "the folded role edges"
            )
    primary_properties_per_node = len(case.grain.property_bindings) + sum(
        sum(
            binding.logical_name not in set(role.table.key_fields)
            for binding in role.property_bindings
        )
        for role in case.roles[1:]
    )
    region_dependent_property_count = len(TABLES["REGION"].columns) - len(
        TABLES["REGION"].key_fields
    )
    primary_properties_per_node += (
        len(implicit_region_roles) * region_dependent_property_count
    )
    logical_expectations = tuple(logical_complete_boundary_expectations)
    if tuple(item.boundary for item in logical_expectations) != case.final_boundaries:
        raise ExperimentValidationError(
            f"{case.case_id}: logical complete boundary expectations do not "
            "match the role-tree cut"
        )

    primary_by_boundary: dict[str, int] = {}
    primary_by_relationship_type: dict[str, int] = {}
    for expectation in logical_expectations:
        boundary = expectation.boundary
        if (
            folds_nation_region
            and boundary.relationship.relationship_type == "NATION_REGION"
        ):
            # Selecting either logical N-R role folds every physical
            # NATION_REGION occurrence, including the other Nation role.
            continue
        count = non_negative_count(
            expectation.folded_count,
            f"logical boundary {boundary.boundary_id}",
        )
        primary_by_boundary[boundary.boundary_id] = count
        relationship_type = boundary.relationship.relationship_type
        primary_by_relationship_type[relationship_type] = (
            primary_by_relationship_type.get(relationship_type, 0) + count
        )
    primary_relationship_total = sum(primary_by_boundary.values())

    selected_symbols = {role.symbol for role in case.roles}
    companion_nodes: dict[str, int] = {}
    companion_property_cells: dict[str, dict[str, Any]] = {}
    companion_relationships: dict[str, dict[str, Any]] = {}
    companion_relationship_total = 0
    component_ids: set[str] = set()
    for component in configuration_components:
        component_id = component.get("component_id")
        kind = component.get("kind")
        entities = component.get("entities")
        if (
            not isinstance(component_id, str)
            or not component_id
            or not isinstance(kind, str)
            or not isinstance(entities, Mapping)
        ):
            raise ExperimentValidationError(
                "configuration redundancy contains invalid component metadata"
            )
        if component_id in component_ids:
            raise ExperimentValidationError(
                f"duplicate configuration component {component_id!r}"
            )
        component_ids.add(component_id)
        if component_id == "primary":
            continue
        nation_metrics = entities.get("NATION")
        if not isinstance(nation_metrics, Mapping):
            raise ExperimentValidationError(
                f"configuration companion {component_id!r} has no NATION grain"
            )
        component_nodes = non_negative_count(
            nation_metrics.get("physical_occurrences_J"),
            f"configuration companion {component_id} nodes",
        )
        companion_nodes[component_id] = component_nodes

        if kind == "retained_normalized":
            component_properties_per_node = len(TABLES["NATION"].columns)
        elif kind == "nation_region_closure":
            region_metrics = entities.get("REGION")
            if not isinstance(region_metrics, Mapping):
                raise ExperimentValidationError(
                    f"configuration closure {component_id!r} has no REGION data"
                )
            region_occurrences = non_negative_count(
                region_metrics.get("physical_occurrences_J"),
                f"configuration closure {component_id} REGION occurrences",
            )
            if region_occurrences != component_nodes:
                raise ExperimentValidationError(
                    f"configuration closure {component_id!r} has different "
                    "NATION and REGION occurrence counts"
                )
            component_properties_per_node = (
                len(TABLES["NATION"].columns) + region_dependent_property_count
            )
        else:
            raise ExperimentValidationError(
                f"unknown configuration companion kind {kind!r}"
            )
        companion_property_cells[component_id] = {
            "kind": kind,
            "nodes": component_nodes,
            "properties_per_node": component_properties_per_node,
            "total": component_nodes * component_properties_per_node,
        }

        if component_id.startswith("customer_nation") or component_id == (
            "nation_region_closure"
        ):
            parent_symbol = "C"
            parent_relationship_type = "CUSTOMER_NATION"
        elif component_id.startswith("supplier_nation"):
            parent_symbol = "S"
            parent_relationship_type = "SUPPLIER_NATION"
        else:
            raise ExperimentValidationError(
                f"unknown configuration companion {component_id!r}"
            )

        relationships_by_type: dict[str, int] = {}
        if parent_symbol not in selected_symbols:
            parent_count = non_negative_count(
                source_relationship_types.get(parent_relationship_type),
                (
                    f"source {parent_relationship_type} count for companion "
                    f"{component_id}"
                ),
            )
            relationships_by_type[parent_relationship_type] = parent_count
        if kind == "retained_normalized":
            nation_region_count = non_negative_count(
                source_relationship_types.get("NATION_REGION"),
                f"source NATION_REGION count for companion {component_id}",
            )
            relationships_by_type["NATION_REGION"] = nation_region_count
        elif kind != "nation_region_closure":
            raise AssertionError("companion kind was validated above")
        component_relationship_total = sum(relationships_by_type.values())
        companion_relationship_total += component_relationship_total
        companion_relationships[component_id] = {
            "kind": kind,
            "by_source_relationship_type": relationships_by_type,
            "total": component_relationship_total,
        }

    if configuration_components and "primary" not in component_ids:
        raise ExperimentValidationError(
            "configuration redundancy has no primary component"
        )

    primary_nodes = actual_primary_nodes
    primary_property_cell_total = primary_nodes * primary_properties_per_node
    folded_property_cell_total = primary_property_cell_total + sum(
        component["total"] for component in companion_property_cells.values()
    )
    folded_node_total = primary_nodes + sum(companion_nodes.values())
    folded_relationship_total = (
        primary_relationship_total + companion_relationship_total
    )
    node_change = folded_node_total - source_node_total
    relationship_change = folded_relationship_total - source_relationship_total
    property_cell_change = folded_property_cell_total - source_property_cell_total
    boundary_connection_change = folded_relationship_total - source_external_total

    whole_folded_node_total = unaffected_source_nodes + folded_node_total
    whole_folded_relationship_total = (
        unaffected_source_relationships + folded_relationship_total
    )
    whole_folded_property_cell_total = (
        unaffected_source_property_cells + folded_property_cell_total
    )
    whole_node_change = whole_folded_node_total - whole_node_total
    whole_relationship_change = (
        whole_folded_relationship_total - whole_relationship_total
    )
    whole_property_cell_change = (
        whole_folded_property_cell_total - whole_property_cell_total
    )
    if (
        whole_node_change != node_change
        or (whole_relationship_change != relationship_change)
        or whole_property_cell_change != property_cell_change
    ):
        raise AssertionError(
            "whole-graph and replacement-scope changes must be identical"
        )

    return {
        "scope": "entire_tpch_graph_after_logical_fold_replacement",
        "measurement_basis": ("whole_tpch_source_graph_counterfactual_replacement"),
        "note": (
            "counterfactual replacement comparison; the experiment keeps the "
            "physical source graph unchanged"
        ),
        "whole_graph": {
            "scope": "entire_tpch_graph_after_logical_fold_replacement",
            "source": whole_source_graph_footprint,
            "folded": {
                "nodes": {
                    "unaffected_source": unaffected_source_nodes,
                    "folded_configuration": folded_node_total,
                    "total": whole_folded_node_total,
                },
                "relationships": {
                    "unaffected_source": unaffected_source_relationships,
                    "folded_configuration": folded_relationship_total,
                    "total": whole_folded_relationship_total,
                },
                "property_cells": {
                    "unaffected_source": unaffected_source_property_cells,
                    "folded_configuration": folded_property_cell_total,
                    "total": whole_folded_property_cell_total,
                },
            },
            "change": {
                "nodes": whole_node_change,
                "relationships": whole_relationship_change,
                "property_cells": whole_property_cell_change,
            },
        },
        "source": source_footprint,
        "folded_configuration": {
            "topology_semantics": "complete",
            "nodes": {
                "primary": primary_nodes,
                "companions": companion_nodes,
                "total": folded_node_total,
            },
            "relationships": {
                "primary_final_cut": {
                    "by_role_scoped_boundary": primary_by_boundary,
                    "by_source_relationship_type": (primary_by_relationship_type),
                    "total": primary_relationship_total,
                },
                "companions": companion_relationships,
                "total": folded_relationship_total,
            },
            "property_cells": {
                "definition": (
                    "logical TPC-H business-property cells; declared source "
                    "primary/foreign keys are included and experiment "
                    "bookkeeping properties are excluded"
                ),
                "primary": {
                    "nodes": primary_nodes,
                    "properties_per_node": primary_properties_per_node,
                    "implicit_region_roles": list(implicit_region_roles),
                    "total": primary_property_cell_total,
                },
                "companions": companion_property_cells,
                "total": folded_property_cell_total,
            },
        },
        "change": {
            "nodes": node_change,
            "relationships": relationship_change,
            "property_cells": property_cell_change,
            "boundary_connections": {
                "source": source_external_total,
                "folded": folded_relationship_total,
                "added": boundary_connection_change,
            },
        },
        "actual_materialization": {
            "topology_mode": final_boundary_materialization.get("mode"),
            "primary_nodes": actual_primary_nodes,
            "primary_relationships": actual_primary_relationships,
            "companions_materialized": False,
        },
        "shared_dimensions": shared_dimensions,
        "shared_dimension_note": (
            "P is a property occurrence count, not a Neo4j node count; "
            "NATION/REGION configuration metrics are reported separately "
            "from the graph-object replacement totals"
        ),
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
    table = pair.referencing if boundary.owner == "referencing" else pair.referenced
    return strict_node_key_predicates(
        shadow_alias,
        table.key_fields,
        tuple(f"{original_alias}.{quote_ident(prop)}" for prop in table.key_fields),
    )


def validate_boundary_copy(
    session: Any,
    pair: PairSpec,
    run_id: str,
    boundary: BoundaryEdgeSpec,
    *,
    phase: Literal["folded", "normalized"],
) -> None:
    owner_alias = "referencing" if boundary.owner == "referencing" else "referenced"
    owner_table = (
        pair.referencing if boundary.owner == "referencing" else pair.referenced
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
            key: value for key, value in state.boundary_relationships.items() if value
        }
    )
    if (
        state.phase != "folded"
        or state.folded_join_nodes != baseline.join_rows
        or (topology_mode == "complete" and actual_boundaries != expected_boundaries)
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
        or state.normalized_referenced_nodes != baseline.participating_referenced_rows
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
        expected_total=(baseline.join_rows + baseline.participating_referenced_rows),
    )
    validate_artifact_node_properties(
        session,
        pair,
        run_id,
        phase="normalized",
    )
    expected_relationship_total = baseline.join_rows + sum(expected_boundaries.values())
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


def count_recursive_label_nodes(
    session: Any,
    case: RecursiveCaseSpec,
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
        f"count {case.case_id} {label} nodes",
        **recursive_parameters(case, run_id),
    )


def inspect_recursive_shadow_state(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    topology_mode: str = "property-fd",
) -> RecursiveShadowState:
    parameters = recursive_parameters(case, run_id)
    final_folded = count_recursive_label_nodes(
        session,
        case,
        run_id,
        case.names.final_folded_label,
    )
    prefix = count_recursive_label_nodes(
        session,
        case,
        run_id,
        case.names.normalized_prefix_label,
    )
    factor = count_recursive_label_nodes(
        session,
        case,
        run_id,
        case.names.normalized_factor_label,
    )
    normalized_joins = scalar_count(
        session,
        f"""
        MATCH (
          prefix:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.normalized_prefix_label)}
          {artifact_identity_map()}
        )-[relationship]->(
          factor:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.normalized_factor_label)}
          {artifact_identity_map()}
        )
        WHERE type(relationship) = $normalized_join_type
        RETURN count(relationship) AS count
        """,
        "count",
        f"count normalized joins for {case.case_id}",
        normalized_join_type=case.names.normalized_join_type,
        **parameters,
    )
    all_nodes = scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )
        RETURN count(node) AS count
        """,
        "count",
        f"count all recursive nodes for {case.case_id}",
        **parameters,
    )
    all_relationships = scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )-[relationship]-()
        RETURN count(DISTINCT relationship) AS count
        """,
        "count",
        f"count all recursive relationships for {case.case_id}",
        **parameters,
    )
    if topology_mode not in {"property-fd", "complete"}:
        raise ValueError(f"unknown topology mode {topology_mode!r}")
    boundary_relationships = (
        {
            boundary.boundary_id: scalar_count(
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
                f"count recursive boundary {boundary.boundary_id}",
                relationship_type=boundary.copy_type,
                **parameters,
            )
            for boundary in case.final_boundaries
        }
        if topology_mode == "complete"
        else {}
    )
    return RecursiveShadowState(
        final_folded_nodes=final_folded,
        normalized_prefix_nodes=prefix,
        normalized_factor_nodes=factor,
        normalized_join_relationships=normalized_joins,
        boundary_relationships=boundary_relationships,
        all_case_nodes=all_nodes,
        all_case_relationships=all_relationships,
    )


def validate_recursive_folded_state(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    baseline: RecursiveBaseline,
    topology_mode: str = "property-fd",
    boundary_expectations: Sequence[RecursiveBoundaryExpectation] = (),
    *,
    deep: bool = True,
) -> RecursiveShadowState:
    state = inspect_recursive_shadow_state(
        session,
        case,
        run_id,
        topology_mode,
    )
    expected_boundaries = (
        {item.boundary_id: item.folded_count for item in boundary_expectations}
        if topology_mode == "complete"
        else {}
    )
    expected_relationships = sum(expected_boundaries.values())
    if (
        state.phase != "folded"
        or state.final_folded_nodes != baseline.chain_rows
        or state.boundary_relationships != expected_boundaries
        or state.all_case_relationships != expected_relationships
    ):
        raise ExperimentValidationError(
            f"{case.case_id}: invalid recursive folded state {state!r}; "
            f"expected {baseline.chain_rows} final folded nodes and "
            f"boundaries={expected_boundaries!r}"
        )
    if deep:
        fingerprint, rows = folded_recursive_join_sha256(
            session,
            case,
            run_id,
            len(case.roles),
            case.names.final_folded_label,
        )
        if rows != baseline.chain_rows or fingerprint != baseline.join_sha256:
            raise ExperimentValidationError(
                f"{case.case_id}: final folded fingerprint differs from "
                "the source recursive join"
            )
    return state


def validate_recursive_normalized_state(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    baseline: RecursiveBaseline,
    topology_mode: str = "property-fd",
    boundary_expectations: Sequence[RecursiveBoundaryExpectation] = (),
    *,
    deep: bool = True,
) -> RecursiveShadowState:
    state = inspect_recursive_shadow_state(
        session,
        case,
        run_id,
        topology_mode,
    )
    expected_boundaries = (
        {item.boundary_id: item.normalized_count for item in boundary_expectations}
        if topology_mode == "complete"
        else {}
    )
    expected_relationships = baseline.chain_rows + sum(expected_boundaries.values())
    if (
        state.phase != "normalized"
        or state.normalized_prefix_nodes != baseline.chain_rows
        or state.normalized_factor_nodes != baseline.participating_final_rows
        or state.normalized_join_relationships != baseline.chain_rows
        or state.boundary_relationships != expected_boundaries
        or state.all_case_relationships != expected_relationships
    ):
        raise ExperimentValidationError(
            f"{case.case_id}: invalid recursive normalized state {state!r}; "
            f"expected prefix={baseline.chain_rows}, "
            f"factor={baseline.participating_final_rows}, "
            f"joins={baseline.chain_rows}, boundaries={expected_boundaries!r}"
        )
    if deep:
        fingerprint, rows = normalized_recursive_join_sha256(
            session,
            case,
            run_id,
        )
        if rows != baseline.chain_rows or fingerprint != baseline.join_sha256:
            raise ExperimentValidationError(
                f"{case.case_id}: normalized recursive fingerprint differs "
                "from the source recursive join"
            )
    return state


def recursive_factor_identity_map(
    case: RecursiveCaseSpec,
    folded_alias: str,
) -> str:
    entries: list[tuple[str, str]] = [(RUN_ID_PROPERTY, "$run_id")]
    entries.extend(
        (
            binding.logical_name,
            f"{folded_alias}.{quote_ident(binding.folded_name)}",
        )
        for binding in case.final_role.property_bindings
        if binding.logical_name in case.final_role.table.key_fields
    )
    return cypher_map(entries)


def normalize_recursive_final_boundaries(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    batch_size: int,
    boundary_expectations: Sequence[RecursiveBoundaryExpectation],
    totals: MutationTotals,
) -> dict[str, dict[str, int]]:
    """Move final-role-owned copies from J prefixes onto D factors."""

    parameters = recursive_parameters(case, run_id)
    results: dict[str, dict[str, int]] = {}
    for expectation in boundary_expectations:
        boundary = expectation.boundary
        if boundary.owner_role_index != len(case.roles) - 1:
            continue
        old_pattern = recursive_boundary_pattern(
            boundary,
            "prefix",
            "external",
            "old_copy",
            copied=True,
        )
        new_pattern = recursive_boundary_pattern(
            boundary,
            "factor",
            "external",
            "new_copy",
            copied=True,
            include_external_label=False,
        )
        create_result = session.run(
            f"""
            MATCH (
              prefix:{quote_ident(ARTIFACT_LABEL)}
                    :{quote_ident(case.names.normalized_prefix_label)}
              {artifact_identity_map()}
            )-[:{quote_ident(case.names.normalized_join_type)}]->(
              factor:{quote_ident(ARTIFACT_LABEL)}
                    :{quote_ident(case.names.normalized_factor_label)}
              {artifact_identity_map()}
            )
            MATCH {old_pattern}
            CALL (factor, external, old_copy) {{
              MERGE {new_pattern}
              ON CREATE SET new_copy = properties(old_copy)
            }} IN TRANSACTIONS OF {batch_size} ROWS
            """,
            parameters,
        )
        summary = create_result.consume()
        totals.add_summary(summary)
        created = int(summary.counters.relationships_created)

        delete_result = session.run(
            f"""
            MATCH (
              prefix:{quote_ident(ARTIFACT_LABEL)}
                    :{quote_ident(case.names.normalized_prefix_label)}
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
        deleted = int(summary.counters.relationships_deleted)
        actual = {"created": created, "deleted": deleted}
        expected = {
            "created": expectation.normalized_count,
            "deleted": expectation.folded_count,
        }
        if actual != expected:
            raise ExperimentValidationError(
                f"{case.case_id}: normalize boundary "
                f"{boundary.boundary_id} counters={actual!r}, expected "
                f"{expected!r}"
            )
        results[boundary.boundary_id] = actual
    return results


def refold_recursive_final_boundaries(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    batch_size: int,
    boundary_expectations: Sequence[RecursiveBoundaryExpectation],
) -> dict[str, dict[str, int]]:
    """Expand final-role factor copies back onto every folded prefix row."""

    parameters = recursive_parameters(case, run_id)
    normalized_join = f"""
    (
      prefix:{quote_ident(ARTIFACT_LABEL)}
            :{quote_ident(case.names.normalized_prefix_label)}
      {artifact_identity_map()}
    )-[:{quote_ident(case.names.normalized_join_type)}]->(
      factor:{quote_ident(ARTIFACT_LABEL)}
            :{quote_ident(case.names.normalized_factor_label)}
      {artifact_identity_map()}
    )
    """
    results: dict[str, dict[str, int]] = {}
    for expectation in boundary_expectations:
        boundary = expectation.boundary
        if boundary.owner_role_index != len(case.roles) - 1:
            continue
        normalized_pattern = recursive_boundary_pattern(
            boundary,
            "factor",
            "external",
            "normalized_copy",
            copied=True,
        )
        folded_pattern = recursive_boundary_pattern(
            boundary,
            "prefix",
            "external",
            "folded_copy",
            copied=True,
            include_external_label=False,
        )
        create_result = session.run(
            f"""
            MATCH {normalized_join}
            MATCH {normalized_pattern}
            CALL (prefix, external, normalized_copy) {{
              CREATE {folded_pattern}
              SET folded_copy = properties(normalized_copy)
            }} IN TRANSACTIONS OF {batch_size} ROWS
            """,
            parameters,
        )
        created = int(create_result.consume().counters.relationships_created)
        delete_result = session.run(
            f"""
            MATCH (
              factor:{quote_ident(ARTIFACT_LABEL)}
                    :{quote_ident(case.names.normalized_factor_label)}
              {artifact_identity_map()}
            )
            MATCH {normalized_pattern}
            CALL (normalized_copy) {{
              DELETE normalized_copy
            }} IN TRANSACTIONS OF {batch_size} ROWS
            """,
            parameters,
        )
        deleted = int(delete_result.consume().counters.relationships_deleted)
        actual = {"created": created, "deleted": deleted}
        expected = {
            "created": expectation.folded_count,
            "deleted": expectation.normalized_count,
        }
        if actual != expected:
            raise ExperimentValidationError(
                f"{case.case_id}: refold boundary {boundary.boundary_id} "
                f"counters={actual!r}, expected {expected!r}"
            )
        results[boundary.boundary_id] = actual
    return results


def normalize_recursive_once(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    baseline: RecursiveBaseline,
    run: int,
    batch_size: int,
    *,
    topology_mode: str = "property-fd",
    boundary_expectations: Sequence[RecursiveBoundaryExpectation] = (),
) -> TimingSample:
    parameters = recursive_parameters(case, run_id)
    totals = MutationTotals()
    started_ns = perf_counter_ns()

    factor_result = session.run(
        f"""
        MATCH (
          folded:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.final_folded_label)}
          {artifact_identity_map()}
        )
        CALL (folded) {{
          MERGE (
            factor:{quote_ident(ARTIFACT_LABEL)}
                  :{quote_ident(case.names.normalized_factor_label)}
            {recursive_factor_identity_map(case, "folded")}
          )
          ON CREATE SET factor +=
            {recursive_factor_property_map_from_folded(case, "folded")},
            factor.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
            factor.{quote_ident(CASE_ID_PROPERTY)} = $case_id
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    factor_summary = factor_result.consume()
    totals.add_summary(factor_summary)
    created_factors = int(factor_summary.counters.nodes_created)

    final_nonkey_physical = tuple(
        binding.folded_name
        for binding in case.final_role.property_bindings
        if binding.logical_name not in case.final_role.table.key_fields
    )
    prefix_result = session.run(
        f"""
        MATCH (
          prefix:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.final_folded_label)}
          {artifact_identity_map()}
        )
        CALL (prefix) {{
          SET prefix:{quote_ident(case.names.normalized_prefix_label)}
          REMOVE {remove_properties_clause(final_nonkey_physical, "prefix")}
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    prefix_summary = prefix_result.consume()
    totals.add_summary(prefix_summary)
    added_prefix_labels = int(prefix_summary.counters.labels_added)

    key_predicate = strict_node_key_predicates(
        "factor",
        case.final_role.table.key_fields,
        tuple(
            f"prefix.{quote_ident(prop)}" for prop in case.final_folded_key_properties
        ),
    )
    factor_index_properties = ", ".join(
        quote_ident(prop)
        for prop in (RUN_ID_PROPERTY, *case.final_role.table.key_fields)
    )
    join_result = session.run(
        f"""
        MATCH (
          prefix:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.normalized_prefix_label)}
          {artifact_identity_map()}
        )
        MATCH (
          factor:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.normalized_factor_label)}
          {artifact_identity_map()}
        )
        USING INDEX factor:{quote_ident(case.names.normalized_factor_label)}(
          {factor_index_properties}
        )
        WHERE {key_predicate}
        CALL (prefix, factor) {{
          CREATE (prefix)-[
            relationship:{quote_ident(case.names.normalized_join_type)}
          ]->(factor)
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    join_summary = join_result.consume()
    totals.add_summary(join_summary)
    created_joins = int(join_summary.counters.relationships_created)

    if topology_mode == "complete":
        normalize_recursive_final_boundaries(
            session,
            case,
            run_id,
            batch_size,
            boundary_expectations,
            totals,
        )
    elif topology_mode != "property-fd":
        raise ValueError(f"unknown topology mode {topology_mode!r}")

    label_result = session.run(
        f"""
        MATCH (
          prefix:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.normalized_prefix_label)}
                :{quote_ident(case.names.final_folded_label)}
          {artifact_identity_map()}
        )
        CALL (prefix) {{
          REMOVE prefix:{quote_ident(case.names.final_folded_label)}
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    label_summary = label_result.consume()
    totals.add_summary(label_summary)
    removed_folded_labels = int(label_summary.counters.labels_removed)

    actual = {
        "created_factors": created_factors,
        "added_prefix_labels": added_prefix_labels,
        "created_joins": created_joins,
        "removed_folded_labels": removed_folded_labels,
    }
    expected = {
        "created_factors": baseline.participating_final_rows,
        "added_prefix_labels": baseline.chain_rows,
        "created_joins": baseline.chain_rows,
        "removed_folded_labels": baseline.chain_rows,
    }
    if actual != expected:
        raise ExperimentValidationError(
            f"{case.case_id}: recursive normalization counters failed: "
            f"actual={actual!r}, expected={expected!r}"
        )
    wall_ms = (perf_counter_ns() - started_ns) / 1_000_000
    sample = totals_timing_sample(
        run,
        wall_ms,
        totals,
        affected_nodes=baseline.chain_rows,
    )
    validate_recursive_normalized_state(
        session,
        case,
        run_id,
        baseline,
        topology_mode,
        boundary_expectations,
        deep=False,
    )
    return sample


def refold_recursive_projection(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    baseline: RecursiveBaseline,
    batch_size: int,
    *,
    topology_mode: str = "property-fd",
    boundary_expectations: Sequence[RecursiveBoundaryExpectation] = (),
) -> None:
    state = inspect_recursive_shadow_state(
        session,
        case,
        run_id,
        topology_mode,
    )
    if state.phase != "normalized":
        raise ExperimentStateError(
            f"{case.case_id}: cannot refold recursive phase {state.phase!r}"
        )
    parameters = recursive_parameters(case, run_id)
    pattern = f"""
    (
      prefix:{quote_ident(ARTIFACT_LABEL)}
            :{quote_ident(case.names.normalized_prefix_label)}
      {artifact_identity_map()}
    )-[join_rel:{quote_ident(case.names.normalized_join_type)}]->(
      factor:{quote_ident(ARTIFACT_LABEL)}
            :{quote_ident(case.names.normalized_factor_label)}
      {artifact_identity_map()}
    )
    """
    refold_result = session.run(
        f"""
        MATCH {pattern}
        CALL (prefix, factor) {{
          SET prefix += {recursive_final_property_map_from_factor(case, "factor")}
          SET prefix:{quote_ident(case.names.final_folded_label)}
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    refold_summary = refold_result.consume()
    restored_folded_labels = int(refold_summary.counters.labels_added)

    if topology_mode == "complete":
        refold_recursive_final_boundaries(
            session,
            case,
            run_id,
            batch_size,
            boundary_expectations,
        )
    elif topology_mode != "property-fd":
        raise ValueError(f"unknown topology mode {topology_mode!r}")

    join_result = session.run(
        f"""
        MATCH {pattern}
        CALL (join_rel) {{
          DELETE join_rel
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    deleted_joins = int(join_result.consume().counters.relationships_deleted)

    factor_result = session.run(
        f"""
        MATCH (
          factor:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.normalized_factor_label)}
          {artifact_identity_map()}
        )
        CALL (factor) {{
          DELETE factor
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    deleted_factors = int(factor_result.consume().counters.nodes_deleted)

    prefix_result = session.run(
        f"""
        MATCH (
          prefix:{quote_ident(ARTIFACT_LABEL)}
                :{quote_ident(case.names.normalized_prefix_label)}
                :{quote_ident(case.names.final_folded_label)}
          {artifact_identity_map()}
        )
        CALL (prefix) {{
          REMOVE prefix:{quote_ident(case.names.normalized_prefix_label)}
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        parameters,
    )
    removed_prefix_labels = int(prefix_result.consume().counters.labels_removed)
    actual = {
        "restored_folded_labels": restored_folded_labels,
        "deleted_joins": deleted_joins,
        "deleted_factors": deleted_factors,
        "removed_prefix_labels": removed_prefix_labels,
    }
    expected = {
        "restored_folded_labels": baseline.chain_rows,
        "deleted_joins": baseline.chain_rows,
        "deleted_factors": baseline.participating_final_rows,
        "removed_prefix_labels": baseline.chain_rows,
    }
    if actual != expected:
        raise ExperimentValidationError(
            f"{case.case_id}: recursive refold counters failed: "
            f"actual={actual!r}, expected={expected!r}"
        )
    validate_recursive_folded_state(
        session,
        case,
        run_id,
        baseline,
        topology_mode,
        boundary_expectations,
        deep=False,
    )


def remove_properties_clause(
    properties: Sequence[str],
    alias: str,
) -> str:
    return ", ".join(
        f"{alias}.{quote_ident(property_name)}" for property_name in properties
    )


def remove_folded_referenced_properties_clause(
    pair: PairSpec,
    alias: str,
) -> str:
    return remove_properties_clause(
        tuple(binding.folded_name for binding in pair.referenced_property_bindings),
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
        neo4j_reported_total_ms=(totals.available_after_ms + totals.consumed_after_ms),
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
            created_relationships = int(summary.counters.relationships_created)

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
            f"{pair.case_id} normalization counters failed: " + "; ".join(problems)
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
                "created": int(create_summary.counters.relationships_created),
                "deleted": int(delete_summary.counters.relationships_deleted),
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
        deleted_relationships += int(summary.counters.relationships_deleted)
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


def cleanup_recursive_artifacts(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    batch_size: int,
) -> CleanupResult:
    if ARTIFACT_LABEL not in database_node_labels(session):
        return CleanupResult(deleted_nodes=0, deleted_relationships=0)
    parameters = recursive_parameters(case, run_id)
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
        parameters | {"allowed_types": list(case.all_copy_relationship_types)},
    )
    unexpected = unexpected_result.single()
    unexpected_result.consume()
    if unexpected is not None:
        raise ExperimentStateError(
            f"{case.case_id}: cleanup refused to delete unexpected "
            f"{unexpected['relationship_type']!r} relationship(s) attached "
            "to recursive run-scoped nodes"
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
        f"count cleanup nodes for {case.case_id}",
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
            f"delete recursive cleanup batch for {case.case_id}",
        )
        selected = int(record["selected_nodes"])
        batch_nodes = int(summary.counters.nodes_deleted)
        if selected <= 0 or batch_nodes != selected:
            raise ExperimentValidationError(
                f"{case.case_id}: recursive cleanup selected {selected} "
                f"node(s) but deleted {batch_nodes}"
            )
        deleted_nodes += batch_nodes
        deleted_relationships += int(summary.counters.relationships_deleted)
    remaining = scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )
        RETURN count(node) AS count
        """,
        "count",
        f"verify recursive cleanup for {case.case_id}",
        **parameters,
    )
    if deleted_nodes != expected_nodes or remaining:
        raise ExperimentValidationError(
            f"{case.case_id}: recursive cleanup deleted {deleted_nodes} of "
            f"{expected_nodes} nodes; remaining={remaining}"
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
    *,
    grouped_fanout_query: str | None = None,
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
            value_types.add(f"{type(value).__module__}.{type(value).__qualname__}")
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
        for (token,) in connection.execute("SELECT token FROM domain ORDER BY token"):
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
            grouped_fanout_query
            if grouped_fanout_query is not None
            else f"""
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
            key = canonical_key(values[prop] for prop in pair.referenced.key_fields)
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

    boundary_folded = sum(item.folded_count for item in baseline.boundary_expectations)
    boundary_normalized = sum(
        item.normalized_count for item in baseline.boundary_expectations
    )
    metrics: dict[str, Any] = {
        "definition": {
            "referencing_table": pair.referencing.label,
            "referenced_table": pair.referenced.label,
            "join_tuple_count_J": occurrences,
            "participating_distinct_referenced_keys_D": (distinct_referenced_rows),
            "full_referenced_relation_rows": baseline.referenced_rows,
            "redundant_referenced_tuple_copies": "J - D",
            "redundant_referenced_property_slots": (
                "(J - D) * (column_count(referenced) - key_length(referenced))"
            ),
        },
        "join_rows_J": occurrences,
        "full_referenced_rows": baseline.referenced_rows,
        "participating_referenced_rows_D": distinct_referenced_rows,
        "unused_referenced_rows": (baseline.referenced_rows - distinct_referenced_rows),
        "redundant_referenced_tuple_copies": redundant_rows,
        "redundant_referenced_tuple_percentage_of_join": (
            100.0 * redundant_rows / occurrences if occurrences else 0.0
        ),
        "referenced_column_count": len(pair.referenced.columns),
        "referenced_key_length": len(pair.referenced.key_fields),
        "dependent_property_count": dependent_count,
        "theoretical_redundant_dependent_property_slots": theoretical_slots,
        "redundant_non_null_dependent_values": redundant_non_null_values,
        "total_non_null_dependent_values_in_folded_join": (total_non_null_values),
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
            str(fanout): count for fanout, count in sorted(fanout_histogram.items())
        },
        "topology": {
            "folded_boundary_relationships": boundary_folded,
            "normalized_boundary_relationships": boundary_normalized,
            "redundant_folded_boundary_relationship_copies": (
                boundary_folded - boundary_normalized
            ),
            "by_boundary": [
                asdict(expectation)
                | {"redundant_folded_copies": (expectation.redundant_folded_copies)}
                for expectation in baseline.boundary_expectations
            ],
        },
    }
    workload = UpdateWorkload(
        property_name=property_name,
        folded_property_name=(pair.referenced_binding_by_logical_name[property_name]),
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


def final_fold_dependent_property_cells(
    metrics: Mapping[str, Any],
) -> dict[str, int]:
    """Return participating dependent-property cells before/after one fold."""

    values: dict[str, int] = {}
    for field in (
        "join_rows_J",
        "participating_referenced_rows_D",
        "dependent_property_count",
        "theoretical_redundant_dependent_property_slots",
    ):
        value = metrics.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ExperimentValidationError(
                f"final-fold {field} must be a non-negative integer"
            )
        values[field] = value

    original = (
        values["participating_referenced_rows_D"] * values["dependent_property_count"]
    )
    folded = values["join_rows_J"] * values["dependent_property_count"]
    added = folded - original
    if added != values["theoretical_redundant_dependent_property_slots"]:
        raise ExperimentValidationError(
            "final-fold dependent-property cells do not reconcile with "
            "redundant slots"
        )
    return {"original": original, "folded": folded, "added": added}


def print_final_fold_dependent_property_cells(metrics: Mapping[str, Any]) -> None:
    cells = final_fold_dependent_property_cells(metrics)
    added = "0" if cells["added"] == 0 else f"{cells['added']:+,}"
    print(
        "  Final-fold dependent-property cells: original="
        f"{cells['original']:,} -> folded={cells['folded']:,} | added={added}"
    )


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
    print_final_fold_dependent_property_cells(metrics)


def print_recursive_redundancy(metrics: dict[str, Any]) -> None:
    """Print a compact recursive summary; the complete histogram stays JSON."""
    histogram = metrics["referenced_fanout_histogram"]
    fanouts = sorted(int(fanout) for fanout in histogram)
    redundant_copies = metrics["redundant_referenced_tuple_copies"]
    print(
        f"  J={metrics['join_rows_J']:,} | "
        f"D={metrics['participating_referenced_rows_D']:,} | "
        f"J-D={redundant_copies:,} "
        "("
        f"{metrics['redundant_referenced_tuple_percentage_of_join']:.3f}%"
        ")"
    )
    print(
        "  Redundant slots="
        f"{metrics['theoretical_redundant_dependent_property_slots']:,} | "
        "non-NULL values="
        f"{metrics['redundant_non_null_dependent_values']:,} | "
        f"payload={metrics['redundant_value_payload_bytes']:,} bytes"
    )
    if fanouts:
        print(
            f"  Final-key fanout={fanouts[0]:,}..{fanouts[-1]:,} "
            "rows/key | "
            f"keys={metrics['participating_referenced_rows_D']:,} | "
            f"levels={len(fanouts):,} (full histogram in JSON)"
        )
    else:
        print("  Final-key fanout: none (full histogram in JSON)")
    print_final_fold_dependent_property_cells(metrics)


def print_recursive_fold_lifecycle(metrics: Mapping[str, Any]) -> None:
    """Print construction churn separately from normalization and cleanup."""

    nodes = metrics["nodes"]
    relationships = metrics["relationships"]
    print("  Whole recursive fold lifecycle (construction only)")
    print(
        "    Nodes: cumulative created="
        f"{nodes['created']:,} | deleted during stage replacement="
        f"{nodes['deleted_during_stage_replacement']:,} | retained="
        f"{nodes['retained']:,}"
    )
    print(
        "    Relationships: cumulative created="
        f"{relationships['created']:,} | deleted during stage replacement="
        f"{relationships['deleted_during_stage_replacement']:,} | retained="
        f"{relationships['retained']:,}"
    )
    source_graph = metrics["source_graph"]
    print(
        "    Source graph deletions: nodes="
        f"{source_graph['nodes_deleted']:,} | relationships="
        f"{source_graph['relationships_deleted']:,}"
    )


def print_fold_replacement_footprint(
    metrics: Mapping[str, Any],
) -> None:
    """Print the counterfactual complete TPC-H graph after any Fold."""

    whole_graph = metrics["whole_graph"]
    whole_source = whole_graph["source"]
    whole_folded = whole_graph["folded"]
    whole_change = whole_graph["change"]
    source = metrics["source"]
    folded = metrics["folded_configuration"]
    folded_nodes = folded["nodes"]
    folded_relationships = folded["relationships"]
    change = metrics["change"]

    def signed_count(value: int) -> str:
        return "0" if value == 0 else f"{value:+,}"

    companion_nodes = sum(folded_nodes["companions"].values())
    companion_relationships = sum(
        component["total"] for component in folded_relationships["companions"].values()
    )
    print("  Whole TPC-H graph after logical fold replacement")
    print(
        "    Nodes: original graph="
        f"{whole_source['nodes']['total']:,} -> folded graph="
        f"{whole_folded['nodes']['total']:,} | "
        f"change={signed_count(whole_change['nodes'])}"
    )
    print(
        "    Relationships: original graph="
        f"{whole_source['relationships']['total']:,} -> folded graph="
        f"{whole_folded['relationships']['total']:,} | "
        f"change={signed_count(whole_change['relationships'])}"
    )
    print(
        "    Redundancy value (TPC-H node-property cells): original graph="
        f"{whole_source['property_cells']['total']:,} -> folded graph="
        f"{whole_folded['property_cells']['total']:,} | "
        f"change={signed_count(whole_change['property_cells'])}"
    )
    print(
        "    Replacement surface removed: nodes="
        f"{source['nodes']['total']:,} | relationships="
        f"{source['relationships']['total']:,}"
    )
    print(
        "    Fold configuration inserted: nodes="
        f"{folded_nodes['total']:,} (primary={folded_nodes['primary']:,} | "
        f"companions={companion_nodes:,}) | relationships="
        f"{folded_relationships['total']:,} (primary="
        f"{folded_relationships['primary_final_cut']['total']:,} | "
        f"companions={companion_relationships:,})"
    )
    print(
        "    Unaffected source retained: nodes="
        f"{whole_folded['nodes']['unaffected_source']:,} | relationships="
        f"{whole_folded['relationships']['unaffected_source']:,}"
    )
    boundary_change = change["boundary_connections"]
    print(
        "    Fold boundary connections: source="
        f"{boundary_change['source']:,} -> folded={boundary_change['folded']:,} "
        f"| added={signed_count(boundary_change['added'])}"
    )


def print_configuration_redundancy(
    metrics: dict[str, Any] | None,
) -> None:
    if metrics is None:
        return
    if metrics["status"] != "ready":
        print(
            "  Configuration redundancy: unsupported ("
            f"{metrics.get('reason', 'unknown reason')})"
        )
        return
    print("  Configuration-wide shared-dimension redundancy:")
    original_cells = 0
    folded_cells = 0
    added_cells = 0
    for entity, entity_metrics in metrics["by_source_entity"].items():
        print(
            f"    {entity}: P={entity_metrics['physical_occurrences_P']:,}, "
            f"U={entity_metrics['union_distinct_keys_U']:,}, "
            "within="
            f"{entity_metrics['within_component_redundant_tuple_copies']:,}, "
            "cross="
            f"{entity_metrics['cross_component_redundant_tuple_copies']:,}, "
            "total="
            f"{entity_metrics['total_redundant_tuple_copies']:,}, "
            "slots="
            f"{entity_metrics['total_redundant_property_slots']:,}"
        )
        dependent_property_count = entity_metrics.get("dependent_property_count")
        occurrences = entity_metrics.get("physical_occurrences_P")
        union_keys = entity_metrics.get("union_distinct_keys_U")
        redundant_slots = entity_metrics.get("total_redundant_property_slots")
        for field_name, value in (
            ("dependent_property_count", dependent_property_count),
            ("physical_occurrences_P", occurrences),
            ("union_distinct_keys_U", union_keys),
            ("total_redundant_property_slots", redundant_slots),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ExperimentValidationError(
                    f"configuration {entity}.{field_name} must be a "
                    "non-negative integer"
                )
        entity_original = union_keys * dependent_property_count
        entity_folded = occurrences * dependent_property_count
        entity_added = entity_folded - entity_original
        if entity_added != redundant_slots:
            raise ExperimentValidationError(
                f"configuration {entity} dependent-property cells do not "
                "reconcile with redundant slots"
            )
        original_cells += entity_original
        folded_cells += entity_folded
        added_cells += entity_added
    formatted_added = "0" if added_cells == 0 else f"{added_cells:+,}"
    print(
        "    Shared-dimension dependent-property cells: original="
        f"{original_cells:,} -> folded={folded_cells:,} | "
        f"added={formatted_added}"
    )


def build_full_table_update_workload(
    session: Any,
    table: NodeSpec,
    property_name: str,
    batch_size: int,
) -> UpdateWorkload:
    """Build one all-row logical workload for one physical TPC-H table."""

    descriptor, spool_name = tempfile.mkstemp(
        prefix=f"tpch_full_update_{snake_case(table.label)}_",
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
            CREATE TABLE physical_target_key (
              target_id TEXT NOT NULL,
              key_token TEXT NOT NULL REFERENCES logical_update(key_token),
              PRIMARY KEY (target_id, key_token)
            ) WITHOUT ROWID;
            """
        )

        key_projection = ", ".join(
            f"source.{quote_ident(key)}" for key in table.key_fields
        )
        source_result = session.run(
            f"""
            MATCH (source:{quote_ident(table.label)})
            RETURN
              [{key_projection}] AS key,
              source.{quote_ident(property_name)} AS property_value
            """
        )
        source_count = 0
        null_count = 0
        value_types: set[str] = set()
        domain_rows: list[tuple[str, bytes, int]] = []
        logical_rows: list[tuple[str, bytes, str, int]] = []
        for record in source_result:
            key = canonical_key(record["key"])
            value = record["property_value"]
            source_count += 1
            if value is None:
                null_count += 1
                continue
            value_types.add(f"{type(value).__module__}.{type(value).__qualname__}")
            value_token = domain_value_token(value)
            domain_rows.append(
                (
                    value_token,
                    pickle.dumps(value, protocol=5),
                    value_payload_bytes(value),
                )
            )
            key_token = json.dumps(
                [domain_value_token(item) for item in key],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            logical_rows.append(
                (
                    key_token,
                    pickle.dumps(key, protocol=5),
                    value_token,
                    1,
                )
            )
            if len(domain_rows) >= batch_size:
                connection.executemany(
                    "INSERT OR IGNORE INTO domain VALUES (?, ?, ?, NULL)",
                    domain_rows,
                )
                domain_rows = []
            if len(logical_rows) >= batch_size:
                connection.executemany(
                    "INSERT INTO logical_update VALUES (?, ?, ?, ?)",
                    logical_rows,
                )
                logical_rows = []
        source_result.consume()
        if domain_rows:
            connection.executemany(
                "INSERT OR IGNORE INTO domain VALUES (?, ?, ?, NULL)",
                domain_rows,
            )
        if logical_rows:
            connection.executemany(
                "INSERT INTO logical_update VALUES (?, ?, ?, ?)",
                logical_rows,
            )
        connection.commit()

        if source_count <= 0:
            raise ExperimentValidationError(
                f"full update source relation {table.label} is empty"
            )
        if null_count:
            raise ExperimentValidationError(
                f"full update property {table.label}.{property_name} contains "
                f"NULL in {null_count} of {source_count} rows"
            )
        if len(value_types) != 1:
            raise ExperimentValidationError(
                f"full update property {table.label}.{property_name} has mixed "
                f"types: {sorted(value_types)!r}"
            )
        value_type = next(iter(value_types))
        active_domain_size = int(
            connection.execute("SELECT count(*) FROM domain").fetchone()[0]
        )
        if active_domain_size < 2:
            raise ExperimentValidationError(
                f"full update property {table.label}.{property_name} has only "
                f"{active_domain_size} distinct value(s)"
            )

        active_domain_hasher = hashlib.sha256()
        active_domain_hasher.update(b"[")
        first_token: str | None = None
        previous_token: str | None = None
        successor_rows: list[tuple[str, str]] = []
        for (raw_token,) in connection.execute(
            "SELECT token FROM domain ORDER BY token"
        ):
            token = str(raw_token)
            if first_token is None:
                first_token = token
            if previous_token is not None:
                successor_rows.append((token, previous_token))
                active_domain_hasher.update(b",")
            active_domain_hasher.update(
                json.dumps(
                    token,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            previous_token = token
        assert first_token is not None and previous_token is not None
        successor_rows.append((first_token, previous_token))
        connection.executemany(
            "UPDATE domain SET successor_token=? WHERE token=?",
            successor_rows,
        )
        connection.commit()
        active_domain_hasher.update(b"]")

        payload_row = connection.execute(
            """
            SELECT
              count(*),
              coalesce(sum(original.payload_bytes), 0),
              coalesce(sum(successor.payload_bytes), 0),
              coalesce(sum(
                CASE WHEN original.token = successor.token THEN 1 ELSE 0 END
              ), 0)
            FROM logical_update AS workload
            JOIN domain AS original
              ON original.token = workload.original_token
            JOIN domain AS successor
              ON successor.token = original.successor_token
            """
        ).fetchone()
        logical_count = int(payload_row[0])
        if logical_count != source_count:
            raise ExperimentValidationError(
                f"full update spool for {table.label} contains "
                f"{logical_count} rows, expected {source_count}"
            )
        if int(payload_row[3]):
            raise ExperimentValidationError(
                f"full update rotation left {payload_row[3]} "
                f"{table.label} value(s) unchanged"
            )

        mapping_count = 0
        mapping_sum = 0
        mapping_xor = 0
        mapping_modulus = 1 << 256
        for key_token, original_token, successor_token in connection.execute(
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
            digest = int.from_bytes(
                hashlib.sha256(
                    json.dumps(
                        {
                            "table": table.label,
                            "property": property_name,
                            "key": json.loads(key_token),
                            "original": original_token,
                            "updated": successor_token,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).digest(),
                "big",
            )
            mapping_count += 1
            mapping_sum = (mapping_sum + digest) % mapping_modulus
            mapping_xor ^= digest

        return UpdateWorkload(
            property_name=property_name,
            folded_property_name=property_name,
            logical_tuple_count=logical_count,
            active_domain_size=active_domain_size,
            active_domain_source_tuple_count=source_count,
            active_domain_sha256=active_domain_hasher.hexdigest(),
            mapping_sha256=sha256_json(
                {
                    "algorithm": "commutative_full_update_mapping_v1",
                    "count": mapping_count,
                    "sum_sha256": f"{mapping_sum:064x}",
                    "xor_sha256": f"{mapping_xor:064x}",
                }
            ),
            value_type=value_type,
            logical_original_payload_bytes=int(payload_row[1]),
            logical_updated_payload_bytes=int(payload_row[2]),
            _database_path=spool_path,
            _connection=connection,
        )
    except BaseException:
        connection.close()
        spool_path.unlink(missing_ok=True)
        raise


def build_full_update_workloads(
    session: Any,
    batch_size: int,
) -> tuple[dict[str, UpdateWorkload], dict[str, Any]]:
    workloads: dict[str, UpdateWorkload] = {}
    try:
        for table_spec in TPC_H_TBL_TABLES:
            label = table_spec.label
            if label not in FULL_UPDATE_PROPERTIES:
                continue
            workloads[label] = build_full_table_update_workload(
                session,
                TABLES[label],
                FULL_UPDATE_PROPERTIES[label],
                batch_size,
            )
    except BaseException:
        for workload in workloads.values():
            workload.close()
        raise

    table_reports = {
        label: {
            "property": workload.property_name,
            "primary_key": list(TABLES[label].key_fields),
            "logical_facts": workload.logical_tuple_count,
            "active_domain_size": workload.active_domain_size,
            "active_domain_source_tuple_count": (
                workload.active_domain_source_tuple_count
            ),
            "active_domain_sha256": workload.active_domain_sha256,
            "mapping_sha256": workload.mapping_sha256,
            "value_type": workload.value_type,
            "logical_original_payload_bytes": (
                workload.logical_original_payload_bytes
            ),
            "logical_updated_payload_bytes": (
                workload.logical_updated_payload_bytes
            ),
        }
        for label, workload in workloads.items()
    }
    logical_fact_count = sum(
        workload.logical_tuple_count for workload in workloads.values()
    )
    workload_sha256 = sha256_json(
        {
            "semantics": FULL_REPLACEMENT_SEMANTICS_ID,
            "logical_identity": "physical_table+primary_key+property",
            "tables": {
                label: {
                    "property": item["property"],
                    "logical_facts": item["logical_facts"],
                    "mapping_sha256": item["mapping_sha256"],
                }
                for label, item in table_reports.items()
            },
        }
    )
    return workloads, {
        "strategy_id": FULL_REPLACEMENT_UPDATE_STRATEGY_ID,
        "logical_identity": "(physical table, primary key, property)",
        "role_identity_policy": (
            "N1/N2 and R1/R2 are physical occurrences of NATION/REGION, "
            "not additional logical facts"
        ),
        "property_scope": (
            "one non-key, non-FK scalar property on every row of the seven "
            "updated relations; LINEITEM is excluded"
        ),
        "updated_physical_tables": list(workloads),
        "excluded_physical_tables": list(FULL_UPDATE_EXCLUDED_TABLES),
        "exclusion_reason": (
            "LINEITEM.l_shipmode is not duplicated by the evaluated folds; "
            "neither layout updates it"
        ),
        "logical_fact_count": logical_fact_count,
        "workload_sha256": workload_sha256,
        "tables": table_reports,
    }


def full_update_artifact_label(
    case_id: str,
    layout: str,
    component_id: str,
    table_label: str,
) -> str:
    return safe_schema_token(
        "mv_full_"
        f"{snake_case(case_id)}_{snake_case(layout)}_"
        f"{snake_case(component_id)}_{snake_case(table_label)}"
    )


def full_update_identity_parameters(case_id: str, run_id: str) -> dict[str, str]:
    return {"case_id": case_id, "run_id": run_id}


def full_topology_relationship_type(source_relationship_type: str) -> str:
    return f"{FULL_TOPOLOGY_RELATIONSHIP_PREFIX}{source_relationship_type}".upper()


def full_topology_anchor_table_label(table_label: str) -> str:
    return safe_schema_token(
        f"{FULL_TOPOLOGY_ANCHOR_LABEL}_{snake_case(table_label)}"
    )


def direct_primary_full_update_targets(
    pair: PairSpec,
    primary_label: str,
) -> list[FullUpdateTarget]:
    return [
        FullUpdateTarget(
            layout="replacement",
            component_id="primary:referencing",
            component_kind="primary",
            table_label=pair.referencing.label,
            label=primary_label,
            key_properties=pair.folded_referencing_key_fields,
            physical_property=pair.referencing_binding_by_logical_name[
                FULL_LAYOUT_OCCURRENCE_PROPERTIES[pair.referencing.label]
            ],
        ),
        FullUpdateTarget(
            layout="replacement",
            component_id="primary:referenced",
            component_kind="primary",
            table_label=pair.referenced.label,
            label=primary_label,
            key_properties=pair.folded_referencing_fk_fields,
            physical_property=pair.referenced_binding_by_logical_name[
                FULL_LAYOUT_OCCURRENCE_PROPERTIES[pair.referenced.label]
            ],
        ),
    ]


def recursive_primary_full_update_targets(
    case: RecursiveCaseSpec,
    configuration_redundancy: Mapping[str, Any] | None,
    primary_label: str,
) -> list[FullUpdateTarget]:
    targets = [
        FullUpdateTarget(
            layout="replacement",
            component_id=f"primary:{role.symbol}",
            component_kind="primary",
            table_label=role.table.label,
            label=primary_label,
            key_properties=tuple(
                role.folded_property(prop) for prop in role.table.key_fields
            ),
            physical_property=role.folded_property(
                FULL_LAYOUT_OCCURRENCE_PROPERTIES[role.table.label]
            ),
        )
        for role in case.roles
    ]
    if configuration_redundancy is None:
        return targets

    role_by_symbol = {role.symbol: role for role in case.roles}
    for completion in configuration_redundancy.get(
        "implicit_physical_edge_completions",
        (),
    ):
        nation_symbol, region_symbol = tuple(completion["edge"])
        nation_role = role_by_symbol[nation_symbol]
        targets.append(
            FullUpdateTarget(
                layout="replacement",
                component_id=f"primary:{region_symbol}:implicit",
                component_kind="primary",
                table_label="REGION",
                label=primary_label,
                key_properties=(nation_role.folded_property("n_regionkey"),),
                physical_property=recursive_role_folded_property(
                    region_symbol,
                    FULL_LAYOUT_OCCURRENCE_PROPERTIES["REGION"],
                ),
            )
        )
    return targets


def materialize_full_update_primary_clone(
    session: Any,
    *,
    case: PairSpec | RecursiveCaseSpec,
    run_id: str,
    label: str,
    batch_size: int,
) -> dict[str, Any]:
    """Clone the final Fold into the isolated full-layout namespace."""

    source_label = (
        case.names.folded_label
        if isinstance(case, PairSpec)
        else case.names.final_folded_label
    )
    started_ns = perf_counter_ns()
    result = session.run(
        f"""
        MATCH (
          source:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(source_label)}
          {artifact_identity_map()}
        )
        CALL (source) {{
          CREATE (
            primary:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
          )
          SET primary = properties(source)
          SET primary.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
              primary.{quote_ident(CASE_ID_PROPERTY)} = $case_id,
              primary.mv_full_layout = 'replacement',
              primary.mv_full_component = 'primary',
              primary.mv_source_table = 'FOLDED_PRIMARY'
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        full_update_identity_parameters(case.case_id, run_id),
    )
    summary = result.consume()
    created = int(summary.counters.nodes_created)
    expected = scalar_count(
        session,
        f"""
        MATCH (
          source:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(source_label)}
          {artifact_identity_map()}
        )
        RETURN count(source) AS count
        """,
        "count",
        f"count {case.case_id} final primary rows for full-layout clone",
        **full_update_identity_parameters(case.case_id, run_id),
    )
    if created != expected:
        raise ExperimentValidationError(
            f"{case.case_id}: full-layout primary clone created {created} "
            f"nodes, expected {expected}"
        )
    return {
        "layout": "replacement",
        "component_id": "primary",
        "component_kind": "primary",
        "table": "FOLDED_PRIMARY",
        "label": label,
        "nodes_created": created,
        "client_wall_ms": (perf_counter_ns() - started_ns) / 1_000_000,
        "source_label": source_label,
    }


def materialize_full_update_clone(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    table: NodeSpec,
    label: str,
    layout: Literal["normalized", "replacement"],
    component_id: str,
    batch_size: int,
) -> dict[str, Any]:
    started_ns = perf_counter_ns()
    result = session.run(
        f"""
        MATCH (source:{quote_ident(table.label)})
        CALL (source) {{
          CREATE (
            shadow:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
          )
          SET shadow = properties(source)
          SET shadow.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
              shadow.{quote_ident(CASE_ID_PROPERTY)} = $case_id,
              shadow.mv_full_layout = $layout,
              shadow.mv_full_component = $component_id,
              shadow.mv_source_table = $table_label
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        full_update_identity_parameters(case_id, run_id)
        | {
            "layout": layout,
            "component_id": component_id,
            "table_label": table.label,
        },
    )
    summary = result.consume()
    elapsed_ms = (perf_counter_ns() - started_ns) / 1_000_000
    created = int(summary.counters.nodes_created)
    expected = scalar_count(
        session,
        f"MATCH (source:{quote_ident(table.label)}) RETURN count(source) AS count",
        "count",
        f"count {table.label} source rows for full update clone",
    )
    if created != expected:
        raise ExperimentValidationError(
            f"{case_id}: {layout}/{component_id} created {created} "
            f"{table.label} nodes, expected {expected}"
        )
    return {
        "layout": layout,
        "component_id": component_id,
        "component_kind": (
            "normalized" if layout == "normalized" else "unaffected"
        ),
        "table": table.label,
        "label": label,
        "nodes_created": created,
        "client_wall_ms": elapsed_ms,
    }


def materialize_nation_region_closure(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    component_id: str,
    label: str,
    batch_size: int,
) -> dict[str, Any]:
    region = TABLES["REGION"]
    region_nonkeys = tuple(
        prop for prop in region.columns if prop not in region.key_fields
    )
    region_assignments = ",\n              ".join(
        f"shadow.{quote_ident(prop)} = source_region.{quote_ident(prop)}"
        for prop in region_nonkeys
    )
    started_ns = perf_counter_ns()
    result = session.run(
        f"""
        MATCH
          (source_nation:{quote_ident('NATION')})
          -[:{quote_ident('NATION_REGION')}]->
          (source_region:{quote_ident('REGION')})
        CALL (source_nation, source_region) {{
          CREATE (
            shadow:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
          )
          SET shadow = properties(source_nation)
          SET {region_assignments}
          SET shadow.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
              shadow.{quote_ident(CASE_ID_PROPERTY)} = $case_id,
              shadow.mv_full_layout = 'replacement',
              shadow.mv_full_component = $component_id,
              shadow.mv_source_table = 'NATION+REGION'
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        full_update_identity_parameters(case_id, run_id)
        | {"component_id": component_id},
    )
    summary = result.consume()
    elapsed_ms = (perf_counter_ns() - started_ns) / 1_000_000
    created = int(summary.counters.nodes_created)
    expected = scalar_count(
        session,
        f"""
        MATCH
          (nation:{quote_ident('NATION')})
          -[:{quote_ident('NATION_REGION')}]->
          (:{quote_ident('REGION')})
        RETURN count(nation) AS count
        """,
        "count",
        f"count {case_id} Nation/Region closure rows",
    )
    if created != expected:
        raise ExperimentValidationError(
            f"{case_id}: closure {component_id} created {created} nodes, "
            f"expected {expected}"
        )
    return {
        "layout": "replacement",
        "component_id": component_id,
        "component_kind": "shared_companion",
        "table": "NATION+REGION",
        "label": label,
        "nodes_created": created,
        "client_wall_ms": elapsed_ms,
    }


def augment_recursive_implicit_region_roles(
    session: Any,
    case: RecursiveCaseSpec,
    run_id: str,
    configuration_redundancy: Mapping[str, Any] | None,
    primary_label: str,
    batch_size: int,
) -> list[dict[str, Any]]:
    if configuration_redundancy is None:
        return []
    role_by_symbol = {role.symbol: role for role in case.roles}
    reports: list[dict[str, Any]] = []
    for completion in configuration_redundancy.get(
        "implicit_physical_edge_completions",
        (),
    ):
        nation_symbol, region_symbol = tuple(completion["edge"])
        nation_role = role_by_symbol[nation_symbol]
        nation_region_key = nation_role.folded_property("n_regionkey")
        assignments = ",\n              ".join(
            f"primary.{quote_ident(recursive_role_folded_property(region_symbol, prop))} "
            f"= source_region.{quote_ident(prop)}"
            for prop in TABLES["REGION"].columns
            if prop not in TABLES["REGION"].key_fields
        )
        started_ns = perf_counter_ns()
        record, summary = one_record(
            session.run(
                f"""
                MATCH (
                  primary:{quote_ident(ARTIFACT_LABEL)}
                         :{quote_ident(primary_label)}
                  {artifact_identity_map()}
                )
                MATCH (source_region:{quote_ident('REGION')})
                WHERE source_region.{quote_ident('r_regionkey')} =
                      primary.{quote_ident(nation_region_key)}
                CALL (primary, source_region) {{
                  SET {assignments}
                }} IN TRANSACTIONS OF {batch_size} ROWS
                RETURN count(primary) AS augmented_nodes
                """,
                recursive_parameters(case, run_id),
            ),
            f"{case.case_id} augment implicit {region_symbol}",
        )
        augmented = int(record["augmented_nodes"])
        expected = scalar_count(
            session,
            f"""
            MATCH (
              primary:{quote_ident(ARTIFACT_LABEL)}
                     :{quote_ident(primary_label)}
              {artifact_identity_map()}
            )
            RETURN count(primary) AS count
            """,
            "count",
            f"count {case.case_id} primary nodes for implicit Region",
            **recursive_parameters(case, run_id),
        )
        if augmented != expected:
            raise ExperimentValidationError(
                f"{case.case_id}: implicit {region_symbol} augmented "
                f"{augmented} primary nodes, expected {expected}"
            )
        reports.append(
            {
                "layout": "replacement",
                "component_id": f"primary:{region_symbol}:implicit",
                "component_kind": "primary",
                "table": "REGION",
                "label": primary_label,
                "nodes_augmented": augmented,
                "properties_set": int(summary.counters.properties_set),
                "client_wall_ms": (perf_counter_ns() - started_ns) / 1_000_000,
            }
        )
    return reports


def full_update_component_targets(
    *,
    case_id: str,
    component_id: str,
    component_kind: Literal["unaffected", "residual", "shared_companion"],
    label: str,
    tables: Sequence[str],
    closure: bool = False,
) -> list[FullUpdateTarget]:
    targets: list[FullUpdateTarget] = []
    for table_label in tables:
        table = TABLES[table_label]
        key_properties = table.key_fields
        if closure and table_label == "REGION":
            key_properties = ("n_regionkey",)
        targets.append(
            FullUpdateTarget(
                layout="replacement",
                component_id=component_id,
                component_kind=component_kind,
                table_label=table_label,
                label=label,
                key_properties=key_properties,
                physical_property=FULL_LAYOUT_OCCURRENCE_PROPERTIES[table_label],
            )
        )
    return targets


def materialize_full_update_residual(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    table: NodeSpec,
    label: str,
    represented_targets: Sequence[FullUpdateTarget],
    batch_size: int,
) -> dict[str, Any]:
    identity = artifact_identity_map()
    existence_clauses: list[str] = []
    seen_lookup: set[tuple[str, tuple[str, ...]]] = set()
    for target in represented_targets:
        lookup = (target.label, target.key_properties)
        if lookup in seen_lookup:
            continue
        seen_lookup.add(lookup)
        predicate = " AND ".join(
            f"represented.{quote_ident(target_property)} = "
            f"source.{quote_ident(source_property)}"
            for target_property, source_property in zip(
                target.key_properties,
                table.key_fields,
                strict=True,
            )
        )
        existence_clauses.append(
            "EXISTS {\n"
            f"  MATCH (represented:{quote_ident(ARTIFACT_LABEL)}:"
            f"{quote_ident(target.label)} {identity})\n"
            f"  WHERE {predicate}\n"
            "}"
        )
    if not existence_clauses:
        raise ExperimentValidationError(
            f"{case_id}: no primary/companion representation exists before "
            f"building {table.label} residuals"
        )
    represented_predicate = " OR ".join(existence_clauses)
    started_ns = perf_counter_ns()
    result = session.run(
        f"""
        MATCH (source:{quote_ident(table.label)})
        WHERE NOT ({represented_predicate})
        CALL (source) {{
          CREATE (
            residual:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(label)}
          )
          SET residual = properties(source)
          SET residual.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
              residual.{quote_ident(CASE_ID_PROPERTY)} = $case_id,
              residual.mv_full_layout = 'replacement',
              residual.mv_full_component = $component_id,
              residual.mv_source_table = $table_label
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        full_update_identity_parameters(case_id, run_id)
        | {
            "component_id": f"residual:{table.label}",
            "table_label": table.label,
        },
    )
    summary = result.consume()
    return {
        "layout": "replacement",
        "component_id": f"residual:{table.label}",
        "component_kind": "residual",
        "table": table.label,
        "label": label,
        "nodes_created": int(summary.counters.nodes_created),
        "client_wall_ms": (perf_counter_ns() - started_ns) / 1_000_000,
    }


def materialize_full_update_layouts(
    session: Any,
    *,
    case: PairSpec | RecursiveCaseSpec,
    run_id: str,
    configuration_redundancy: Mapping[str, Any] | None,
    batch_size: int,
    index_wait_seconds: int,
    indexes: list[IndexLease],
) -> tuple[list[FullUpdateTarget], list[dict[str, Any]]]:
    case_id = case.case_id
    targets: list[FullUpdateTarget] = []
    construction: list[dict[str, Any]] = []

    # Complete normalized property representation: exactly one clone per
    # source row in each of the eight physical relations.
    for table_spec in TPC_H_TBL_TABLES:
        table = TABLES[table_spec.label]
        label = full_update_artifact_label(
            case_id,
            "normalized",
            "source",
            table.label,
        )
        construction.append(
            materialize_full_update_clone(
                session,
                case_id=case_id,
                run_id=run_id,
                table=table,
                label=label,
                layout="normalized",
                component_id=f"normalized:{table.label}",
                batch_size=batch_size,
            )
        )
        targets.append(
            FullUpdateTarget(
                layout="normalized",
                component_id=f"normalized:{table.label}",
                component_kind="normalized",
                table_label=table.label,
                label=label,
                key_properties=table.key_fields,
                physical_property=FULL_LAYOUT_OCCURRENCE_PROPERTIES[table.label],
            )
        )

    primary_label = full_update_artifact_label(
        case_id,
        "replacement",
        "primary",
        "folded",
    )
    construction.append(
        materialize_full_update_primary_clone(
            session,
            case=case,
            run_id=run_id,
            label=primary_label,
            batch_size=batch_size,
        )
    )

    if isinstance(case, PairSpec):
        primary_targets = direct_primary_full_update_targets(case, primary_label)
    else:
        primary_targets = recursive_primary_full_update_targets(
            case,
            configuration_redundancy,
            primary_label,
        )
        construction.extend(
            augment_recursive_implicit_region_roles(
                session,
                case,
                run_id,
                configuration_redundancy,
                primary_label,
                batch_size,
            )
        )
    targets.extend(primary_targets)
    selected_tables = {target.table_label for target in primary_targets}

    # Relations outside the selected physical replacement footprint are
    # cloned into the replacement namespace.  Updating the imported source in
    # addition to the folded primary would be coexistence, not replacement.
    for table_spec in TPC_H_TBL_TABLES:
        table = TABLES[table_spec.label]
        if table.label in selected_tables:
            continue
        component_id = f"unaffected:{table.label}"
        label = full_update_artifact_label(
            case_id,
            "replacement",
            "unaffected",
            table.label,
        )
        construction.append(
            materialize_full_update_clone(
                session,
                case_id=case_id,
                run_id=run_id,
                table=table,
                label=label,
                layout="replacement",
                component_id=component_id,
                batch_size=batch_size,
            )
        )
        targets.extend(
            full_update_component_targets(
                case_id=case_id,
                component_id=component_id,
                component_kind="unaffected",
                label=label,
                tables=(table.label,),
            )
        )

    # Shared Nation/Region components preserve the branch not represented by
    # the selected primary fold.  They are physical occurrences and therefore
    # receive the same logical Nation/Region updates as the primary copies.
    if configuration_redundancy is not None:
        for component in configuration_redundancy.get("components", ()):
            if component["component_id"] == "primary":
                continue
            component_id = str(component["component_id"])
            kind = str(component["kind"])
            entity_tables = tuple(str(item) for item in component["entities"])
            label = full_update_artifact_label(
                case_id,
                "replacement",
                component_id,
                "nation_region" if len(entity_tables) > 1 else entity_tables[0],
            )
            if kind == "nation_region_closure":
                construction.append(
                    materialize_nation_region_closure(
                        session,
                        case_id=case_id,
                        run_id=run_id,
                        component_id=component_id,
                        label=label,
                        batch_size=batch_size,
                    )
                )
                targets.extend(
                    full_update_component_targets(
                        case_id=case_id,
                        component_id=component_id,
                        component_kind="shared_companion",
                        label=label,
                        tables=entity_tables,
                        closure=True,
                    )
                )
            elif kind == "retained_normalized":
                if entity_tables != ("NATION",):
                    raise ExperimentValidationError(
                        f"{case_id}: unsupported retained component "
                        f"{component_id} entities={entity_tables!r}"
                    )
                construction.append(
                    materialize_full_update_clone(
                        session,
                        case_id=case_id,
                        run_id=run_id,
                        table=TABLES["NATION"],
                        label=label,
                        layout="replacement",
                        component_id=component_id,
                        batch_size=batch_size,
                    )
                )
                construction[-1]["component_kind"] = "shared_companion"
                targets.extend(
                    full_update_component_targets(
                        case_id=case_id,
                        component_id=component_id,
                        component_kind="shared_companion",
                        label=label,
                        tables=("NATION",),
                    )
                )
            else:
                raise ExperimentValidationError(
                    f"{case_id}: unsupported replacement component kind {kind!r}"
                )

    # Residual anti-joins must not repeatedly scan a large primary MV.  Build
    # lookup indexes for all already-materialized target families first; this
    # DDL remains outside update timing.
    indexes.extend(
        create_full_update_indexes(
            session,
            case_id=case_id,
            run_id=run_id,
            targets=targets,
            wait_seconds=index_wait_seconds,
        )
    )

    # Any selected source row absent from every primary/shared occurrence is a
    # residual row.  This closes the inner-join data-loss hole (for example,
    # 5,000 CUSTOMER rows in L-O-C and 57 PARTSUPP rows in L-PS at SF 0.1).
    for table_label in sorted(selected_tables):
        represented = [
            target
            for target in targets
            if target.layout == "replacement" and target.table_label == table_label
        ]
        residual_label = full_update_artifact_label(
            case_id,
            "replacement",
            "residual",
            table_label,
        )
        construction.append(
            materialize_full_update_residual(
                session,
                case_id=case_id,
                run_id=run_id,
                table=TABLES[table_label],
                label=residual_label,
                represented_targets=represented,
                batch_size=batch_size,
            )
        )
        targets.extend(
            full_update_component_targets(
                case_id=case_id,
                component_id=f"residual:{table_label}",
                component_kind="residual",
                label=residual_label,
                tables=(table_label,),
            )
        )

    residual_targets = [
        target for target in targets if target.component_kind == "residual"
    ]
    if residual_targets:
        indexes.extend(
            create_full_update_indexes(
                session,
                case_id=case_id,
                run_id=run_id,
                targets=residual_targets,
                wait_seconds=index_wait_seconds,
                name_offset=len(indexes),
            )
        )

    physical_families: set[tuple[str, str]] = set()
    for target in targets:
        if target.physical_cell_family in physical_families:
            raise ExperimentValidationError(
                f"{case_id}: duplicate physical target family "
                f"{target.physical_cell_family!r}"
            )
        physical_families.add(target.physical_cell_family)
    return targets, construction


def create_full_update_indexes(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    targets: Sequence[FullUpdateTarget],
    wait_seconds: int,
    name_offset: int = 0,
) -> list[IndexLease]:
    indexes: list[IndexLease] = []
    definitions: set[tuple[str, tuple[str, ...]]] = set()
    try:
        for target in targets:
            definition = (
                target.label,
                (RUN_ID_PROPERTY, *target.key_properties),
            )
            if definition in definitions:
                continue
            definitions.add(definition)
            name = safe_schema_token(
                f"{SCHEMA_PREFIX}_full_{case_id}_{run_id}_"
                f"{name_offset + len(definitions)}"
            )
            session.run(
                f"""
                CREATE INDEX {quote_ident(name)}
                FOR (node:{quote_ident(target.label)})
                ON {composite_property_expression('node', definition[1])}
                """
            ).consume()
            indexes.append(IndexLease(name=name, created_by_experiment=True))
        session.run(
            "CALL db.awaitIndexes($timeout_seconds)",
            timeout_seconds=wait_seconds,
        ).consume()
        existing_result = session.run(
            "SHOW INDEXES YIELD name WHERE name IN $names RETURN name",
            names=[index.name for index in indexes if index.name is not None],
        )
        existing_records, _ = result_records_and_summary(existing_result)
        existing_names = {str(record["name"]) for record in existing_records}
        expected_names = {
            str(index.name) for index in indexes if index.name is not None
        }
        if existing_names != expected_names:
            raise ExperimentValidationError(
                f"{case_id}: full-update indexes do not reconcile; "
                f"expected={sorted(expected_names)!r}, "
                f"actual={sorted(existing_names)!r}"
            )
    except BaseException:
        drop_case_schema(session, (), indexes)
        raise
    return indexes


def full_topology_role_edges(
) -> tuple[tuple[str, RelationshipSpec, str, str], ...]:
    relationship_by_type = {
        relationship.relationship_type: relationship
        for relationship in RELATIONSHIPS
    }
    definitions = (
        ("N1_R1", "NATION_REGION", "N1", "R1"),
        ("N2_R2", "NATION_REGION", "N2", "R2"),
        ("S_N2", "SUPPLIER_NATION", "S", "N2"),
        ("C_N1", "CUSTOMER_NATION", "C", "N1"),
        ("O_C", "ORDERS_CUSTOMER", "O", "C"),
        ("PS_P", "PARTSUPP_PART", "PS", "P"),
        ("PS_S", "PARTSUPP_SUPPLIER", "PS", "S"),
        ("L_O", "LINEITEM_ORDERS", "L", "O"),
        ("L_PS", "LINEITEM_PARTSUPP", "L", "PS"),
    )
    return tuple(
        (
            logical_edge_id,
            relationship_by_type[relationship_type],
            source_role,
            target_role,
        )
        for logical_edge_id, relationship_type, source_role, target_role in definitions
    )


def physical_roles_for_table(table_label: str) -> tuple[str, ...]:
    roles = tuple(
        role.symbol
        for role in RECURSIVE_ROLE_DEFINITION_ORDER
        if role.table_label == table_label
    )
    if not roles:
        raise ExperimentValidationError(
            f"full topology has no logical role for {table_label}"
        )
    return roles


def direct_primary_role_symbols(
    pair: PairSpec,
    table_label: str,
) -> tuple[str, ...]:
    if table_label == "NATION":
        if pair.case_id == "supplier_nation":
            return ("N2",)
        if pair.case_id == "customer_nation":
            return ("N1",)
        if pair.case_id == "nation_region":
            return ("N1", "N2")
    if table_label == "REGION" and pair.case_id == "nation_region":
        return ("R1", "R2")
    return physical_roles_for_table(table_label)


def companion_role_symbols(
    component_id: str,
    table_label: str,
) -> tuple[str, ...]:
    roles_by_component = {
        "customer_nation_retained": {"NATION": ("N1",)},
        "supplier_nation_retained": {"NATION": ("N2",)},
        "nation_region_closure": {
            "NATION": ("N1",),
            "REGION": ("R1",),
        },
        "supplier_nation_region_closure": {
            "NATION": ("N2",),
            "REGION": ("R2",),
        },
    }
    try:
        return roles_by_component[component_id][table_label]
    except KeyError as exc:
        raise ExperimentValidationError(
            f"unknown full-layout companion role for "
            f"{component_id}/{table_label}"
        ) from exc


def direct_primary_property_bindings(
    pair: PairSpec,
    table_label: str,
) -> tuple[PropertyBinding, ...]:
    if table_label == pair.referencing.label:
        return pair.referencing_property_bindings
    if table_label != pair.referenced.label:
        raise ExperimentValidationError(
            f"{pair.case_id}: {table_label} is not a direct primary role"
        )
    fk_by_referenced_key = {
        referenced_key: referencing_fk
        for referencing_fk, referenced_key in pair.fk_mapping
    }
    bindings = [
        PropertyBinding(
            logical_name=referenced_key,
            folded_name=pair.referencing_binding_by_logical_name[
                fk_by_referenced_key[referenced_key]
            ],
        )
        for referenced_key in pair.referenced.key_fields
    ]
    bindings.extend(pair.referenced_property_bindings)
    return tuple(bindings)


def implicit_region_property_bindings(
    case: RecursiveCaseSpec,
    region_symbol: str,
) -> tuple[PropertyBinding, ...]:
    nation_symbol = "N1" if region_symbol == "R1" else "N2"
    nation_role = next(
        role for role in case.roles if role.symbol == nation_symbol
    )
    return tuple(
        PropertyBinding(
            logical_name=property_name,
            folded_name=(
                nation_role.folded_property("n_regionkey")
                if property_name == "r_regionkey"
                else recursive_role_folded_property(region_symbol, property_name)
            ),
        )
        for property_name in TABLES["REGION"].columns
    )


def full_target_property_bindings(
    case: PairSpec | RecursiveCaseSpec,
    target: FullUpdateTarget,
) -> tuple[PropertyBinding, ...]:
    table = TABLES[target.table_label]
    if target.component_kind == "primary":
        if isinstance(case, PairSpec):
            bindings = direct_primary_property_bindings(case, target.table_label)
        else:
            parts = target.component_id.split(":")
            if len(parts) < 2:
                raise ExperimentValidationError(
                    f"{case.case_id}: malformed primary component "
                    f"{target.component_id!r}"
                )
            role_symbol = parts[1]
            role = next(
                (item for item in case.roles if item.symbol == role_symbol),
                None,
            )
            bindings = (
                role.property_bindings
                if role is not None
                else implicit_region_property_bindings(case, role_symbol)
            )
    elif (
        target.component_kind == "shared_companion"
        and target.table_label == "REGION"
        and target.key_properties != table.key_fields
    ):
        bindings = tuple(
            PropertyBinding(
                logical_name=property_name,
                folded_name=(
                    "n_regionkey"
                    if property_name == "r_regionkey"
                    else property_name
                ),
            )
            for property_name in table.columns
        )
    else:
        bindings = make_folded_property_bindings(table, table.columns)

    if {binding.logical_name for binding in bindings} != set(table.columns):
        raise ExperimentValidationError(
            f"{case.case_id}: occurrence {target.target_id} does not bind "
            f"all {target.table_label} columns"
        )
    binding_map = {
        binding.logical_name: binding.folded_name for binding in bindings
    }
    bound_keys = tuple(binding_map[key] for key in table.key_fields)
    if bound_keys != target.key_properties:
        raise ExperimentValidationError(
            f"{case.case_id}: occurrence {target.target_id} key bindings "
            f"{bound_keys!r} differ from target keys {target.key_properties!r}"
        )
    return bindings


def full_target_role_symbols(
    case: PairSpec | RecursiveCaseSpec,
    target: FullUpdateTarget,
) -> tuple[str, ...]:
    if target.layout == "normalized" or target.component_kind in {
        "unaffected",
        "residual",
    }:
        return physical_roles_for_table(target.table_label)
    if target.component_kind == "shared_companion":
        return companion_role_symbols(target.component_id, target.table_label)
    if isinstance(case, PairSpec):
        return direct_primary_role_symbols(case, target.table_label)
    parts = target.component_id.split(":")
    if len(parts) < 2:
        raise ExperimentValidationError(
            f"{case.case_id}: malformed primary target {target.target_id}"
        )
    return (parts[1],)


def build_full_topology_occurrences(
    case: PairSpec | RecursiveCaseSpec,
    targets: Sequence[FullUpdateTarget],
) -> list[FullTopologyOccurrence]:
    occurrences = [
        FullTopologyOccurrence(
            layout=target.layout,
            occurrence_id=target.target_id,
            component_id=target.component_id,
            component_kind=target.component_kind,
            table_label=target.table_label,
            label=target.label,
            property_bindings=full_target_property_bindings(case, target),
            role_symbols=full_target_role_symbols(case, target),
        )
        for target in targets
    ]
    occurrence_ids = [occurrence.occurrence_id for occurrence in occurrences]
    if len(occurrence_ids) != len(set(occurrence_ids)):
        raise ExperimentValidationError(
            f"{case.case_id}: duplicate full topology occurrence IDs"
        )
    return occurrences


def build_full_topology_routes(
    case: PairSpec | RecursiveCaseSpec,
    occurrences: Sequence[FullTopologyOccurrence],
) -> list[FullTopologyRoute]:
    """Build a bounded source-occurrence to canonical-target route plan.

    The previous implementation took the Cartesian product of every source
    and target occurrence family.  That multiplied recursive layouts (for
    example L-O-C-N1) even before repeated keys inside a primary Fold were
    considered.  Here each source occurrence has at most one route for a
    logical role edge.  Its target is a *candidate set* from which the live
    materializer selects one canonical node per role/key.

    Logical aliases share a physical route only when the source occurrence and
    complete target candidate set are identical.  Their IDs remain attached
    to that edge and are validated independently (N1/R1 versus N2/R2).
    """

    occurrence_by_id = {
        occurrence.occurrence_id: occurrence for occurrence in occurrences
    }
    occurrence_ids = set(occurrence_by_id)
    merged: dict[
        tuple[str, str, tuple[str, ...]],
        dict[str, str],
    ] = {}
    for layout in ("normalized", "replacement"):
        layout_occurrences = [
            occurrence
            for occurrence in occurrences
            if occurrence.layout == layout
        ]
        for logical_edge_id, relationship, source_role, target_role in (
            full_topology_role_edges()
        ):
            sources = [
                occurrence
                for occurrence in layout_occurrences
                if occurrence.table_label == relationship.source_label
                and source_role in occurrence.role_symbols
            ]
            targets = [
                occurrence
                for occurrence in layout_occurrences
                if occurrence.table_label == relationship.target_label
                and target_role in occurrence.role_symbols
            ]
            if not sources or not targets:
                raise ExperimentValidationError(
                    f"{case.case_id}: {layout} topology edge "
                    f"{logical_edge_id} has no occurrence route"
                )
            target_ids = tuple(
                sorted(target.occurrence_id for target in targets)
            )
            for source in sources:
                merged.setdefault(
                    (
                        relationship.relationship_type,
                        source.occurrence_id,
                        target_ids,
                    ),
                    {},
                )[logical_edge_id] = target_role
    routes = [
        FullTopologyRoute(
            relationship_type=relationship_type,
            full_relationship_type=(
                full_topology_relationship_type(relationship_type)
            ),
            source_occurrence_id=source_occurrence_id,
            target_occurrence_ids=target_ids,
            logical_edge_ids=tuple(sorted(logical_edges)),
            target_role_symbols=tuple(
                logical_edges[logical_edge_id]
                for logical_edge_id in sorted(logical_edges)
            ),
        )
        for (
            relationship_type,
            source_occurrence_id,
            target_ids,
        ), logical_edges in sorted(merged.items())
    ]
    planned_source_edges: set[tuple[str, str, str]] = set()
    for route in routes:
        if route.source_occurrence_id not in occurrence_ids:
            raise ExperimentValidationError(
                f"{case.case_id}: route references unknown source occurrence "
                f"{route.source_occurrence_id!r}"
            )
        if not route.target_occurrence_ids or any(
            occurrence_id not in occurrence_ids
            for occurrence_id in route.target_occurrence_ids
        ):
            raise ExperimentValidationError(
                f"{case.case_id}: route has an empty or unknown canonical "
                "target candidate"
            )
        if len(route.logical_edge_ids) != len(route.target_role_symbols):
            raise ExperimentValidationError(
                f"{case.case_id}: route logical-edge/target-role arity differs"
            )
        for logical_edge_id in route.logical_edge_ids:
            identity = (
                occurrence_by_id[route.source_occurrence_id].layout,
                logical_edge_id,
                route.source_occurrence_id,
            )
            if identity in planned_source_edges:
                raise ExperimentValidationError(
                    f"{case.case_id}: {identity[0]} {logical_edge_id} has "
                    "more than one route for a source occurrence"
                )
            planned_source_edges.add(identity)
    return routes


def validate_full_tpch_source_topology(session: Any) -> dict[str, Any]:
    """Validate all eight declared TPC-H FK edges before full materialization."""

    by_type: dict[str, int] = {}
    for relationship in RELATIONSHIPS:
        relationship_type = relationship.relationship_type
        row_table = TABLES[relationship.row_label]
        typed_count = scalar_count(
            session,
            f"""
            MATCH ()-[relationship:{quote_ident(relationship_type)}]->()
            RETURN count(relationship) AS count
            """,
            "count",
            f"count full source topology {relationship_type}",
        )
        declared_count = scalar_count(
            session,
            f"""
            MATCH
              (source:{quote_ident(relationship.source_label)})
              -[relationship:{quote_ident(relationship_type)}]->
              (target:{quote_ident(relationship.target_label)})
            RETURN count(relationship) AS count
            """,
            "count",
            f"count declared full source topology {relationship_type}",
        )
        expected_count = scalar_count(
            session,
            f"MATCH (row:{quote_ident(row_table.label)}) "
            "RETURN count(row) AS count",
            "count",
            f"count source rows for {relationship_type}",
        )
        if typed_count != declared_count or declared_count != expected_count:
            raise ExperimentValidationError(
                f"full source topology {relationship_type} has typed="
                f"{typed_count}, declared={declared_count}, "
                f"expected={expected_count}"
            )
        relationship_properties = scalar_count(
            session,
            f"""
            MATCH ()-[relationship:{quote_ident(relationship_type)}]->()
            WHERE size(keys(relationship)) > 0
            RETURN count(relationship) AS count
            """,
            "count",
            f"validate property-free {relationship_type}",
        )
        if relationship_properties:
            raise ExperimentValidationError(
                f"full source topology {relationship_type} has "
                f"{relationship_properties} relationship(s) with properties"
            )

        row_alias = (
            "source"
            if relationship.row_label == relationship.source_label
            else "target"
        )
        endpoint_predicates = [
            f"source.{quote_ident(endpoint_property)} = "
            f"{row_alias}.{quote_ident(row_property)}"
            for endpoint_property, row_property in relationship.source_keys
        ]
        endpoint_predicates.extend(
            f"target.{quote_ident(endpoint_property)} = "
            f"{row_alias}.{quote_ident(row_property)}"
            for endpoint_property, row_property in relationship.target_keys
        )
        invalid_keys = scalar_count(
            session,
            f"""
            MATCH
              (source:{quote_ident(relationship.source_label)})
              -[relationship:{quote_ident(relationship_type)}]->
              (target:{quote_ident(relationship.target_label)})
            WHERE NOT ({' AND '.join(endpoint_predicates)})
            RETURN count(relationship) AS count
            """,
            "count",
            f"validate endpoint keys for {relationship_type}",
        )
        if invalid_keys:
            raise ExperimentValidationError(
                f"full source topology {relationship_type} has "
                f"{invalid_keys} invalid endpoint mapping(s)"
            )
        by_type[relationship_type] = declared_count
    return {
        "status": "valid",
        "relationship_properties_required": False,
        "by_physical_type": by_type,
        "total": sum(by_type.values()),
    }


def occurrence_key_predicate(
    occurrence: FullTopologyOccurrence,
    physical_alias: str,
    source_alias: str,
) -> str:
    table = TABLES[occurrence.table_label]
    bindings = occurrence.binding_by_logical_name
    return " AND ".join(
        f"{physical_alias}.{quote_ident(bindings[key])} = "
        f"{source_alias}.{quote_ident(key)}"
        for key in table.key_fields
    )


def occurrence_index_hint(
    occurrence: FullTopologyOccurrence,
    alias: str,
) -> str:
    properties = ", ".join(
        quote_ident(prop)
        for prop in (RUN_ID_PROPERTY, *occurrence.key_properties)
    )
    return f"USING INDEX {alias}:{quote_ident(occurrence.label)}({properties})"


def full_topology_anchor_plans(
    occurrences: Sequence[FullTopologyOccurrence],
) -> list[FullTopologyAnchorPlan]:
    candidates: dict[tuple[str, str, str], set[str]] = {}
    for occurrence in occurrences:
        for role_symbol in occurrence.role_symbols:
            candidates.setdefault(
                (occurrence.layout, occurrence.table_label, role_symbol),
                set(),
            ).add(occurrence.occurrence_id)
    return [
        FullTopologyAnchorPlan(
            layout=layout,
            table_label=table_label,
            role_symbol=role_symbol,
            candidate_occurrence_ids=tuple(sorted(occurrence_ids)),
        )
        for (
            layout,
            table_label,
            role_symbol,
        ), occurrence_ids in sorted(candidates.items())
    ]


def full_topology_candidate_union_subquery(
    *,
    target_occurrences: Sequence[FullTopologyOccurrence],
    target_alias: str,
    passthrough: Sequence[str],
) -> str:
    """Select one occurrence without multiplying it by other query rows."""

    if not target_occurrences:
        raise ExperimentValidationError("empty canonical target candidate set")
    branches: list[str] = []
    for occurrence in target_occurrences:
        branches.append(
            "MATCH (candidate:"
            f"{quote_ident(ARTIFACT_LABEL)}:{quote_ident(occurrence.label)} "
            f"{artifact_identity_map()})\n"
            f"{occurrence_index_hint(occurrence, 'candidate')}\n"
            f"WHERE {occurrence_key_predicate(occurrence, 'candidate', target_alias)}\n"
            "RETURN candidate AS physical_target, "
            f"{1 if occurrence.component_kind == 'primary' else 0} "
            "AS primary_rank, "
            "coalesce(candidate.mv_full_component, '') AS component_rank"
        )
    post_union_scope = ", ".join(
        (*passthrough, "physical_target", "primary_rank", "component_rank")
    )
    collect_scope = ", ".join(passthrough)
    collect_prefix = f"{collect_scope}, " if collect_scope else ""
    return (
        f"CALL ({target_alias}) {{\n  "
        + "\n  UNION ALL\n  ".join(branches).replace("\n", "\n  ")
        + "\n}\n"
        f"WITH {post_union_scope}\n"
        "ORDER BY primary_rank ASC, component_rank ASC, "
        "elementId(physical_target) ASC\n"
        f"WITH {collect_prefix}head(collect(physical_target)) AS physical_target"
    )


def materialize_full_topology_anchors(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    occurrences: Sequence[FullTopologyOccurrence],
    batch_size: int,
    index_wait_seconds: int,
    indexes: list[IndexLease],
) -> dict[str, Any]:
    """Materialize one indexed role/key anchor before any topology routing."""

    occurrence_by_id = {
        occurrence.occurrence_id: occurrence for occurrence in occurrences
    }
    reports: list[dict[str, Any]] = []
    for plan in full_topology_anchor_plans(occurrences):
        target_occurrences = [
            occurrence_by_id[occurrence_id]
            for occurrence_id in plan.candidate_occurrence_ids
        ]
        table = TABLES[plan.table_label]
        target_union = full_topology_candidate_union_subquery(
            target_occurrences=target_occurrences,
            target_alias="source",
            passthrough=("source",),
        )
        key_assignments = ",\n                  ".join(
            f"anchor.{quote_ident(key)} = source.{quote_ident(key)}"
            for key in table.key_fields
        )
        result = session.run(
            f"""
            MATCH (source:{quote_ident(plan.table_label)})
            CALL (source) {{
              WITH source
              {target_union}
              CREATE (
                anchor:{quote_ident(ARTIFACT_LABEL)}
                       :{quote_ident(FULL_TOPOLOGY_ANCHOR_LABEL)}
                       :{quote_ident(full_topology_anchor_table_label(plan.table_label))}
              )
              SET anchor.{quote_ident(RUN_ID_PROPERTY)} = $run_id,
                  anchor.{quote_ident(CASE_ID_PROPERTY)} = $case_id,
                  anchor.mv_full_layout = $layout,
                  anchor.mv_full_component = 'topology_anchor',
                  anchor.mv_full_anchor_table = $table_label,
                  anchor.mv_full_anchor_role = $role_symbol,
                  {key_assignments}
              CREATE
                (anchor)
                -[:{quote_ident(FULL_TOPOLOGY_ANCHOR_RELATIONSHIP)}]->
                (physical_target)
            }} IN TRANSACTIONS OF {batch_size} ROWS
            """,
            full_update_identity_parameters(case_id, run_id)
            | {
                "layout": plan.layout,
                "table_label": plan.table_label,
                "role_symbol": plan.role_symbol,
            },
        )
        summary = result.consume()
        expected = scalar_count(
            session,
            f"MATCH (source:{quote_ident(plan.table_label)}) "
            "RETURN count(source) AS count",
            "count",
            f"count {plan.table_label} anchor source rows",
        )
        created = int(summary.counters.nodes_created)
        if created != expected or int(summary.counters.relationships_created) != expected:
            raise ExperimentValidationError(
                f"{case_id}: {plan.layout}/{plan.role_symbol} created "
                f"{created} anchors and "
                f"{int(summary.counters.relationships_created)} links, "
                f"expected {expected}"
            )
        reports.append(
            {
                "layout": plan.layout,
                "table": plan.table_label,
                "role_symbol": plan.role_symbol,
                "candidate_occurrence_ids": list(
                    plan.candidate_occurrence_ids
                ),
                "anchors_created": created,
            }
        )
    anchor_index_leases: list[IndexLease] = []
    try:
        for table_spec in TPC_H_TBL_TABLES:
            table = TABLES[table_spec.label]
            name = safe_schema_token(
                f"{SCHEMA_PREFIX}_full_anchor_{case_id}_{run_id}_"
                f"{snake_case(table.label)}"
            )
            properties = (
                RUN_ID_PROPERTY,
                "mv_full_layout",
                "mv_full_anchor_role",
                *table.key_fields,
            )
            session.run(
                f"""
                CREATE INDEX {quote_ident(name)}
                FOR (
                  anchor:{quote_ident(full_topology_anchor_table_label(table.label))}
                )
                ON {composite_property_expression('anchor', properties)}
                """
            ).consume()
            anchor_index_leases.append(
                IndexLease(name=name, created_by_experiment=True)
            )
        session.run(
            "CALL db.awaitIndexes($timeout_seconds)",
            timeout_seconds=index_wait_seconds,
        ).consume()
    except BaseException:
        drop_case_schema(session, (), anchor_index_leases)
        raise
    indexes.extend(anchor_index_leases)
    return {
        "plans": reports,
        "anchors_created": sum(report["anchors_created"] for report in reports),
        "anchor_links_created": sum(
            report["anchors_created"] for report in reports
        ),
    }


def full_topology_anchor_lookup(
    *,
    target_occurrence: FullTopologyOccurrence,
    target_role_symbol: str,
    target_alias: str,
) -> str:
    table = TABLES[target_occurrence.table_label]
    key_predicate = " AND ".join(
        f"anchor.{quote_ident(key)} = {target_alias}.{quote_ident(key)}"
        for key in table.key_fields
    )
    return f"""
    MATCH (
      anchor:{quote_ident(ARTIFACT_LABEL)}
             :{quote_ident(FULL_TOPOLOGY_ANCHOR_LABEL)}
             :{quote_ident(full_topology_anchor_table_label(target_occurrence.table_label))}
      {artifact_identity_map()}
    )-[:{quote_ident(FULL_TOPOLOGY_ANCHOR_RELATIONSHIP)}]->(physical_target)
    USING INDEX anchor:{quote_ident(full_topology_anchor_table_label(target_occurrence.table_label))}(
      {', '.join(quote_ident(prop) for prop in (
          RUN_ID_PROPERTY,
          'mv_full_layout',
          'mv_full_anchor_role',
          *table.key_fields,
      ))}
    )
    WHERE anchor.mv_full_layout = $layout
      AND anchor.mv_full_anchor_table = $target_table
      AND anchor.mv_full_anchor_role = $target_role
      AND {key_predicate}
    """


def materialize_full_layout_topology(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    occurrences: Sequence[FullTopologyOccurrence],
    routes: Sequence[FullTopologyRoute],
    batch_size: int,
) -> dict[str, Any]:
    occurrence_by_id = {
        occurrence.occurrence_id: occurrence for occurrence in occurrences
    }
    relationship_by_type = {
        relationship.relationship_type: relationship
        for relationship in RELATIONSHIPS
    }
    route_reports: list[dict[str, Any]] = []
    for route in routes:
        source_occurrence = occurrence_by_id[route.source_occurrence_id]
        target_occurrences = [
            occurrence_by_id[occurrence_id]
            for occurrence_id in route.target_occurrence_ids
        ]
        target_occurrence = target_occurrences[0]
        target_role_symbol = route.target_role_symbols[0]
        relationship = relationship_by_type[route.relationship_type]
        started_ns = perf_counter_ns()
        co_location_occurrence = next(
            (
                occurrence
                for occurrence in target_occurrences
                if occurrence.label == source_occurrence.label
            ),
            None,
        )
        co_location_predicate = co_location_occurrence is not None
        same_node_target_predicate = (
            occurrence_key_predicate(
                co_location_occurrence,
                "physical_source",
                "target",
            )
            if co_location_occurrence is not None
            else "false"
        )
        anchor_lookup = full_topology_anchor_lookup(
            target_occurrence=target_occurrence,
            target_role_symbol=target_role_symbol,
            target_alias="target",
        )
        result = session.run(
            f"""
            MATCH
              (source:{quote_ident(relationship.source_label)})
              -[source_relationship:{quote_ident(relationship.relationship_type)}]->
              (target:{quote_ident(relationship.target_label)})
            MATCH (
              physical_source:{quote_ident(ARTIFACT_LABEL)}
                             :{quote_ident(source_occurrence.label)}
              {artifact_identity_map()}
            )
            {occurrence_index_hint(source_occurrence, 'physical_source')}
            WHERE {
                occurrence_key_predicate(
                    source_occurrence,
                    'physical_source',
                    'source',
                )
            }
            WITH source_relationship, target, physical_source,
                 CASE
                   WHEN $co_location_allowed
                    AND {same_node_target_predicate}
                   THEN physical_source
                   ELSE NULL
                 END AS co_located_target
            CALL (source_relationship, target, physical_source, co_located_target) {{
              WITH source_relationship, target, physical_source,
                   co_located_target
              WHERE co_located_target IS NOT NULL
              RETURN co_located_target AS physical_target
              UNION ALL
              WITH source_relationship, target, physical_source,
                   co_located_target
              WHERE co_located_target IS NULL
              {anchor_lookup}
              RETURN physical_target
            }}
            WITH physical_source, source_relationship, physical_target,
                 physical_source = physical_target AS implicit
            CALL (physical_source, physical_target, source_relationship, implicit) {{
              WITH physical_source, physical_target, source_relationship
              WHERE NOT implicit
              CREATE
                (physical_source)
                -[copy:{quote_ident(route.full_relationship_type)}]->
                (physical_target)
              SET copy = properties(source_relationship),
                  copy.{quote_ident(FULL_TOPOLOGY_LOGICAL_EDGE_IDS_PROPERTY)} =
                    $logical_edge_ids
              RETURN count(*) AS created
            }} IN TRANSACTIONS OF {batch_size} ROWS
            RETURN
              count(*) AS routed_source_occurrences,
              sum(CASE WHEN implicit THEN 1 ELSE 0 END) AS implicit_occurrences
            """,
            full_update_identity_parameters(case_id, run_id)
            | {
                "logical_edge_ids": list(route.logical_edge_ids),
                "layout": source_occurrence.layout,
                "target_table": target_occurrence.table_label,
                "target_role": target_role_symbol,
                "co_location_allowed": co_location_predicate,
            },
        )
        record, summary = one_record(
            result,
            f"materialize canonical route {route.logical_edge_ids}",
        )
        routed_source_occurrences = int(record["routed_source_occurrences"])
        relationships_created = int(summary.counters.relationships_created)
        implicit_occurrences = int(record["implicit_occurrences"])
        if routed_source_occurrences != relationships_created + implicit_occurrences:
            raise ExperimentValidationError(
                f"{case_id}: route {route.logical_edge_ids} routed "
                f"{routed_source_occurrences} source occurrences but created "
                f"{relationships_created} explicit plus {implicit_occurrences} "
                "implicit witnesses"
            )
        route_reports.append(
            {
                "layout": source_occurrence.layout,
                "logical_edge_ids": list(route.logical_edge_ids),
                "source_relationship_type": route.relationship_type,
                "physical_relationship_type": route.full_relationship_type,
                "source_occurrence_id": route.source_occurrence_id,
                "target_occurrence_ids": list(route.target_occurrence_ids),
                "target_role_symbols": list(route.target_role_symbols),
                "selection_policy": (
                    "non-primary target occurrence first, then component_id "
                    "and elementId; selected source=target is implicit"
                ),
                "routed_source_occurrences": routed_source_occurrences,
                "relationships_created": relationships_created,
                "implicit_physical_occurrences": implicit_occurrences,
                "client_wall_ms": (perf_counter_ns() - started_ns) / 1_000_000,
            }
        )
    return {
        "route_count": len(route_reports),
        "routes": route_reports,
        "explicit_relationships_created": sum(
            int(report["relationships_created"]) for report in route_reports
        ),
        "implicit_physical_occurrences": sum(
            int(report["implicit_physical_occurrences"])
            for report in route_reports
        ),
    }


def validate_canonical_topology_route(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    route: FullTopologyRoute,
    logical_edge_id: str,
    source_occurrence: FullTopologyOccurrence,
    target_occurrences: Sequence[FullTopologyOccurrence],
    relationship: RelationshipSpec,
) -> dict[str, int]:
    """Replay canonical selection and require exactly one route witness."""

    target_occurrence = target_occurrences[0]
    logical_edge_index = route.logical_edge_ids.index(logical_edge_id)
    target_role_symbol = route.target_role_symbols[logical_edge_index]
    co_location_occurrence = next(
        (
            occurrence
            for occurrence in target_occurrences
            if occurrence.label == source_occurrence.label
        ),
        None,
    )
    co_location_allowed = co_location_occurrence is not None
    same_node_target_predicate = (
        occurrence_key_predicate(
            co_location_occurrence,
            "physical_source",
            "target",
        )
        if co_location_occurrence is not None
        else "false"
    )
    anchor_lookup = full_topology_anchor_lookup(
        target_occurrence=target_occurrence,
        target_role_symbol=target_role_symbol,
        target_alias="target",
    )
    record, _ = one_record(
        session.run(
            f"""
            MATCH
              (source:{quote_ident(relationship.source_label)})
              -[source_relationship:{quote_ident(relationship.relationship_type)}]->
              (target:{quote_ident(relationship.target_label)})
            MATCH (
              physical_source:{quote_ident(ARTIFACT_LABEL)}
                             :{quote_ident(source_occurrence.label)}
              {artifact_identity_map()}
            )
            {occurrence_index_hint(source_occurrence, 'physical_source')}
            WHERE {
                occurrence_key_predicate(
                    source_occurrence,
                    'physical_source',
                    'source',
                )
            }
            WITH source_relationship, target, physical_source,
                 CASE
                   WHEN $co_location_allowed
                    AND {same_node_target_predicate}
                   THEN physical_source
                   ELSE NULL
                 END AS co_located_target
            CALL (source_relationship, target, physical_source, co_located_target) {{
              WITH source_relationship, target, physical_source,
                   co_located_target
              WHERE co_located_target IS NOT NULL
              RETURN co_located_target AS physical_target
              UNION ALL
              WITH source_relationship, target, physical_source,
                   co_located_target
              WHERE co_located_target IS NULL
              {anchor_lookup}
              RETURN physical_target
            }}
            WITH physical_source, source_relationship, physical_target,
                 physical_source = physical_target AS implicit
            OPTIONAL MATCH
              (physical_source)
              -[copy:{quote_ident(route.full_relationship_type)}]->
              (physical_target)
            WHERE $logical_edge_id IN coalesce(
              copy.{quote_ident(FULL_TOPOLOGY_LOGICAL_EDGE_IDS_PROPERTY)}, []
            )
            WITH physical_source, source_relationship, physical_target,
                 implicit, count(copy) AS explicit_witnesses
            RETURN
              count(*) AS expected_source_occurrences,
              sum(
                CASE
                  WHEN physical_target IS NOT NULL
                   AND implicit = true
                   AND explicit_witnesses = 0 THEN 1
                  WHEN physical_target IS NOT NULL
                   AND implicit = false
                   AND explicit_witnesses = 1 THEN 1
                  ELSE 0
                END
              ) AS valid_source_occurrences,
              sum(CASE WHEN implicit = true THEN 1 ELSE 0 END)
                AS implicit_source_occurrences,
              sum(explicit_witnesses) AS explicit_witnesses,
              sum(CASE WHEN explicit_witnesses > 1 THEN 1 ELSE 0 END)
                AS duplicate_source_occurrences
            """,
            full_update_identity_parameters(case_id, run_id)
            | {
                "logical_edge_id": logical_edge_id,
                "layout": source_occurrence.layout,
                "target_table": target_occurrence.table_label,
                "target_role": target_role_symbol,
                "co_location_allowed": co_location_allowed,
            },
        ),
        f"validate canonical topology {logical_edge_id} "
        f"from {source_occurrence.occurrence_id}",
    )
    return {
        key: int(record[key])
        for key in (
            "expected_source_occurrences",
            "valid_source_occurrences",
            "implicit_source_occurrences",
            "explicit_witnesses",
            "duplicate_source_occurrences",
        )
    }


def full_topology_signature(
    session: Any,
    *,
    case_id: str,
    run_id: str,
) -> dict[str, dict[str, int]]:
    result = session.run(
        f"""
        MATCH (
          source:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )-[relationship]->(
          target:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )
        WHERE source.mv_full_layout IS NOT NULL
          AND target.mv_full_layout = source.mv_full_layout
          AND type(relationship) IN $allowed_types
        RETURN
          source.mv_full_layout AS layout,
          type(relationship) AS relationship_type,
          count(relationship) AS relationship_count
        ORDER BY layout, relationship_type
        """,
        full_update_identity_parameters(case_id, run_id)
        | {
            "allowed_types": [
                full_topology_relationship_type(relationship.relationship_type)
                for relationship in RELATIONSHIPS
            ]
        },
    )
    records, _ = result_records_and_summary(result)
    signature: dict[str, dict[str, int]] = {
        "normalized": {},
        "replacement": {},
    }
    for record in records:
        signature[str(record["layout"])][str(record["relationship_type"])] = int(
            record["relationship_count"]
        )
    return signature


def validate_full_layout_topology(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    source_topology: Mapping[str, Any],
    occurrences: Sequence[FullTopologyOccurrence],
    routes: Sequence[FullTopologyRoute],
    materialization: Mapping[str, Any],
) -> dict[str, Any]:
    occurrence_by_id = {
        occurrence.occurrence_id: occurrence for occurrence in occurrences
    }
    invalid_scope = scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )-[relationship]-(other)
        WHERE node.mv_full_layout IS NOT NULL
          AND (
            NOT node.mv_full_layout IN $allowed_layouts
            OR NOT $artifact_label IN labels(other)
            OR other.{quote_ident(RUN_ID_PROPERTY)} IS NULL
            OR other.{quote_ident(RUN_ID_PROPERTY)} <> $run_id
            OR other.{quote_ident(CASE_ID_PROPERTY)} IS NULL
            OR other.{quote_ident(CASE_ID_PROPERTY)} <> $case_id
            OR other.mv_full_layout IS NULL
            OR other.mv_full_layout <> node.mv_full_layout
            OR NOT type(relationship) IN $allowed_types
          )
        RETURN count(relationship) AS count
        """,
        "count",
        f"validate {case_id} full topology scope",
        **full_update_identity_parameters(case_id, run_id),
        artifact_label=ARTIFACT_LABEL,
        allowed_layouts=["normalized", "replacement"],
        allowed_types=[
            full_topology_relationship_type(relationship.relationship_type)
            for relationship in RELATIONSHIPS
        ]
        + [FULL_TOPOLOGY_ANCHOR_RELATIONSHIP],
    )
    if invalid_scope:
        raise ExperimentValidationError(
            f"{case_id}: full topology has {invalid_scope} cross-scope or "
            "unexpected relationship(s)"
        )

    coverage: dict[str, dict[str, Any]] = {}
    relationship_by_type = {
        relationship.relationship_type: relationship
        for relationship in RELATIONSHIPS
    }
    for layout in ("normalized", "replacement"):
        layout_coverage: dict[str, Any] = {}
        for relationship_type, relationship in relationship_by_type.items():
            relationship_routes = [
                route
                for route in routes
                if route.relationship_type == relationship_type
                and occurrence_by_id[route.source_occurrence_id].layout == layout
            ]
            if not relationship_routes:
                raise ExperimentValidationError(
                    f"{case_id}: {layout} has no route for {relationship_type}"
                )
            by_logical_edge: dict[str, list[dict[str, Any]]] = {}
            for route in relationship_routes:
                source_occurrence = occurrence_by_id[
                    route.source_occurrence_id
                ]
                target_occurrences = [
                    occurrence_by_id[occurrence_id]
                    for occurrence_id in route.target_occurrence_ids
                ]
                for logical_edge_id in route.logical_edge_ids:
                    route_counts = validate_canonical_topology_route(
                        session,
                        case_id=case_id,
                        run_id=run_id,
                        route=route,
                        logical_edge_id=logical_edge_id,
                        source_occurrence=source_occurrence,
                        target_occurrences=target_occurrences,
                        relationship=relationship,
                    )
                    if (
                        route_counts["valid_source_occurrences"]
                        != route_counts["expected_source_occurrences"]
                        or route_counts["duplicate_source_occurrences"]
                    ):
                        raise ExperimentValidationError(
                            f"{case_id}: {layout}/{logical_edge_id}/"
                            f"{source_occurrence.occurrence_id} canonical "
                            f"coverage is invalid: {route_counts!r}"
                        )
                    by_logical_edge.setdefault(logical_edge_id, []).append(
                        {
                            "source_occurrence_id": (
                                source_occurrence.occurrence_id
                            ),
                            **route_counts,
                        }
                    )
            expected_logical_edges = {
                logical_edge_id
                for logical_edge_id, candidate_relationship, _, _ in (
                    full_topology_role_edges()
                )
                if candidate_relationship.relationship_type == relationship_type
            }
            if set(by_logical_edge) != expected_logical_edges:
                raise ExperimentValidationError(
                    f"{case_id}: {layout}/{relationship_type} covers logical "
                    f"edges {sorted(by_logical_edge)!r}, expected "
                    f"{sorted(expected_logical_edges)!r}"
                )
            layout_coverage[relationship_type] = {
                "logical_edges": by_logical_edge,
                "missing_source_occurrences": 0,
                "duplicate_source_occurrences": 0,
            }
        coverage[layout] = layout_coverage

    signature = full_topology_signature(
        session,
        case_id=case_id,
        run_id=run_id,
    )
    normalized_expected = {
        full_topology_relationship_type(source_type): int(count)
        for source_type, count in source_topology["by_physical_type"].items()
    }
    if signature["normalized"] != normalized_expected:
        raise ExperimentValidationError(
            f"{case_id}: normalized topology counts differ from source; "
            f"actual={signature['normalized']!r}, "
            f"expected={normalized_expected!r}"
        )
    actual_explicit = sum(
        count
        for by_type in signature.values()
        for count in by_type.values()
    )
    expected_explicit = int(materialization["explicit_relationships_created"])
    if actual_explicit != expected_explicit:
        raise ExperimentValidationError(
            f"{case_id}: full topology contains {actual_explicit} explicit "
            f"relationships, expected {expected_explicit}"
        )
    return {
        "status": "lossless_and_isolated",
        "logical_missing_edges": coverage,
        "canonical_anchor_policy": (
            "one target per source occurrence/logical role edge; non-primary "
            "component first, component_id then elementId tie-break"
        ),
        "canonical_multiplicity_validated_per_logical_edge": True,
        "explicit_relationship_signature": signature,
        "normalized_matches_source_relationship_counts": True,
        "cross_layout_or_source_relationships": 0,
        "implicit_relationship_representation": "same-node role co-location",
    }


def validate_full_layout_reconstruction(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    occurrences: Sequence[FullTopologyOccurrence],
    target_counts: Mapping[str, int],
) -> dict[str, Any]:
    """Prove both layouts reconstruct the same eight complete source tables."""

    by_layout: dict[str, Any] = {}
    identity = artifact_identity_map()
    for layout in ("normalized", "replacement"):
        layout_occurrences = [
            occurrence
            for occurrence in occurrences
            if occurrence.layout == layout
        ]
        table_reports: dict[str, Any] = {}
        for table_spec in TPC_H_TBL_TABLES:
            table = TABLES[table_spec.label]
            table_occurrences = [
                occurrence
                for occurrence in layout_occurrences
                if occurrence.table_label == table.label
            ]
            if not table_occurrences:
                raise ExperimentValidationError(
                    f"{case_id}: {layout} cannot reconstruct {table.label}"
                )
            occurrence_reports: dict[str, Any] = {}
            for occurrence in table_occurrences:
                bindings = occurrence.binding_by_logical_name
                equality = " AND ".join(
                    equality_predicate(
                        f"physical.{quote_ident(bindings[property_name])}",
                        f"source.{quote_ident(property_name)}",
                    )
                    for property_name in table.columns
                )
                record, _ = one_record(
                    session.run(
                        f"""
                        MATCH (
                          physical:{quote_ident(ARTIFACT_LABEL)}
                                  :{quote_ident(occurrence.label)}
                          {identity}
                        )
                        CALL (physical) {{
                          MATCH (source:{quote_ident(table.label)})
                          WHERE {
                              occurrence_key_predicate(
                                  occurrence,
                                  'physical',
                                  'source',
                              )
                          }
                          RETURN
                            count(source) AS source_matches,
                            head(collect(source)) AS source
                        }}
                        RETURN
                          count(physical) AS physical_occurrences,
                          coalesce(sum(
                            CASE WHEN source_matches = 1 THEN 0 ELSE 1 END
                          ), 0) AS invalid_source_keys,
                          coalesce(sum(
                            CASE
                              WHEN source_matches = 1 AND ({equality})
                              THEN 0 ELSE 1
                            END
                          ), 0) AS unequal_rows
                        """,
                        full_update_identity_parameters(case_id, run_id),
                    ),
                    f"validate full reconstruction {occurrence.occurrence_id}",
                )
                physical_count = int(record["physical_occurrences"])
                expected_count = int(target_counts[occurrence.occurrence_id])
                if physical_count != expected_count:
                    raise ExperimentValidationError(
                        f"{case_id}: reconstruction occurrence "
                        f"{occurrence.occurrence_id} has {physical_count} rows, "
                        f"expected {expected_count}"
                    )
                if int(record["invalid_source_keys"]) or int(record["unequal_rows"]):
                    raise ExperimentValidationError(
                        f"{case_id}: reconstruction occurrence "
                        f"{occurrence.occurrence_id} has invalid_keys="
                        f"{record['invalid_source_keys']}, unequal_rows="
                        f"{record['unequal_rows']}"
                    )
                occurrence_reports[occurrence.occurrence_id] = {
                    "physical_occurrences": physical_count,
                    "invalid_source_keys": 0,
                    "unequal_all_column_rows": 0,
                    "property_bindings": {
                        binding.logical_name: binding.folded_name
                        for binding in occurrence.property_bindings
                    },
                    "component_kind": occurrence.component_kind,
                }

            coverage_clauses = [
                "EXISTS {\n"
                f"  MATCH (physical:{quote_ident(ARTIFACT_LABEL)}:"
                f"{quote_ident(occurrence.label)} {identity})\n"
                f"  WHERE {occurrence_key_predicate(occurrence, 'physical', 'source')}\n"
                "}"
                for occurrence in table_occurrences
            ]
            missing = scalar_count(
                session,
                f"""
                MATCH (source:{quote_ident(table.label)})
                WHERE NOT ({' OR '.join(coverage_clauses)})
                RETURN count(source) AS count
                """,
                "count",
                f"validate {layout} all-column coverage for {table.label}",
                **full_update_identity_parameters(case_id, run_id),
            )
            if missing:
                raise ExperimentValidationError(
                    f"{case_id}: {layout} reconstruction is missing {missing} "
                    f"{table.label} row(s)"
                )
            logical_rows = scalar_count(
                session,
                f"MATCH (source:{quote_ident(table.label)}) "
                "RETURN count(source) AS count",
                "count",
                f"count logical {table.label} rows for reconstruction",
            )
            physical_occurrences = sum(
                int(report["physical_occurrences"])
                for report in occurrence_reports.values()
            )
            table_reports[table.label] = {
                "logical_rows": logical_rows,
                "physical_role_occurrences": physical_occurrences,
                "role_occurrence_amplification": (
                    physical_occurrences / logical_rows if logical_rows else None
                ),
                "missing_logical_rows": 0,
                "all_columns_equal_source": True,
                "occurrences": occurrence_reports,
            }

        family_properties: dict[str, set[str]] = {}
        family_counts: dict[str, set[int]] = {}
        family_components: dict[str, set[str]] = {}
        for occurrence in layout_occurrences:
            family_properties.setdefault(occurrence.label, set()).update(
                binding.folded_name for binding in occurrence.property_bindings
            )
            family_counts.setdefault(occurrence.label, set()).add(
                int(target_counts[occurrence.occurrence_id])
            )
            family_components.setdefault(occurrence.label, set()).add(
                occurrence.component_id
            )
        storage_families: dict[str, Any] = {}
        for label, property_names in family_properties.items():
            counts = family_counts[label]
            if len(counts) != 1:
                raise ExperimentValidationError(
                    f"{case_id}: physical family {label} has inconsistent "
                    f"role counts {sorted(counts)!r}"
                )
            nodes = next(iter(counts))
            storage_families[label] = {
                "nodes": nodes,
                "logical_business_properties_per_node": len(property_names),
                "logical_business_property_cells": nodes * len(property_names),
                "physical_properties": sorted(property_names),
                "components": sorted(family_components[label]),
            }
        expected_nodes = sum(
            int(family["nodes"]) for family in storage_families.values()
        )
        actual_nodes = scalar_count(
            session,
            f"""
            MATCH (
              node:{quote_ident(ARTIFACT_LABEL)}
              {artifact_identity_map()}
            )
            WHERE node.mv_full_layout = $layout
            RETURN count(node) AS count
            """,
            "count",
            f"count actual {layout} full-layout nodes",
            **full_update_identity_parameters(case_id, run_id),
            layout=layout,
        )
        if actual_nodes != expected_nodes:
            raise ExperimentValidationError(
                f"{case_id}: {layout} has {actual_nodes} physical nodes, "
                f"expected {expected_nodes} from occurrence families"
            )
        by_layout[layout] = {
            "status": "lossless_all_column_reconstruction",
            "proof": (
                "every physical role occurrence matches exactly one source "
                "primary key and all declared columns; every source primary "
                "key has at least one occurrence"
            ),
            "tables": table_reports,
            "physical_storage": {
                "nodes": actual_nodes,
                "logical_business_property_cells": sum(
                    int(family["logical_business_property_cells"])
                    for family in storage_families.values()
                ),
                "families": storage_families,
            },
        }
    return {
        "status": "normalized_and_replacement_logically_equivalent",
        "logical_identity": "physical table plus physical primary key",
        "role_alias_policy": (
            "N1/N2 and R1/R2 occurrences collapse to their physical "
            "NATION/REGION primary keys during logical reconstruction"
        ),
        "layouts": by_layout,
    }


def full_update_target_count(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    target: FullUpdateTarget,
) -> int:
    return scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(target.label)}
          {artifact_identity_map()}
        )
        RETURN count(node) AS count
        """,
        "count",
        f"count full update target {target.target_id}",
        **full_update_identity_parameters(case_id, run_id),
    )


def validate_full_update_target_plan(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    targets: Sequence[FullUpdateTarget],
    workloads: Mapping[str, UpdateWorkload],
) -> tuple[dict[str, int], dict[str, Any]]:
    counts: dict[str, int] = {}
    table_report: dict[str, Any] = {}
    identity = artifact_identity_map()
    for target in targets:
        table = TABLES[target.table_label]
        source_predicate = " AND ".join(
            f"source.{quote_ident(source_property)} = "
            f"target.{quote_ident(target_property)}"
            for source_property, target_property in zip(
                table.key_fields,
                target.key_properties,
                strict=True,
            )
        )
        record, _ = one_record(
            session.run(
                f"""
                MATCH (
                  target:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(target.label)}
                  {identity}
                )
                CALL (target) {{
                  MATCH (source:{quote_ident(table.label)})
                  WHERE {source_predicate}
                  RETURN
                    count(source) AS source_matches,
                    head(collect(
                      source.{quote_ident(FULL_LAYOUT_OCCURRENCE_PROPERTIES[target.table_label])}
                    )) AS source_value
                }}
                RETURN
                  count(target) AS target_nodes,
                  coalesce(sum(CASE WHEN source_matches = 1 THEN 0 ELSE 1 END), 0)
                    AS invalid_source_keys,
                  coalesce(sum(
                    CASE
                      WHEN target.{quote_ident(target.physical_property)} IS NULL
                      THEN 1 ELSE 0
                    END
                  ), 0) AS null_target_values,
                  coalesce(sum(
                    CASE
                      WHEN source_matches = 1
                       AND target.{quote_ident(target.physical_property)} =
                           source_value
                      THEN 0 ELSE 1
                    END
                  ), 0) AS unequal_source_values
                """,
                full_update_identity_parameters(case_id, run_id),
            ),
            f"validate full update target {target.target_id}",
        )
        count = int(record["target_nodes"])
        counts[target.target_id] = count
        if int(record["invalid_source_keys"]):
            raise ExperimentValidationError(
                f"{case_id}: target {target.target_id} contains "
                f"{record['invalid_source_keys']} invalid source key(s)"
            )
        if int(record["null_target_values"]):
            raise ExperimentValidationError(
                f"{case_id}: target {target.target_id} contains "
                f"{record['null_target_values']} NULL update value(s)"
            )
        if int(record["unequal_source_values"]):
            raise ExperimentValidationError(
                f"{case_id}: target {target.target_id} contains "
                f"{record['unequal_source_values']} value(s) unequal to source"
            )

    for table_label, workload in workloads.items():
        table = TABLES[table_label]
        normalized = [
            target
            for target in targets
            if target.layout == "normalized" and target.table_label == table_label
        ]
        replacement = [
            target
            for target in targets
            if target.layout == "replacement" and target.table_label == table_label
        ]
        if len(normalized) != 1:
            raise ExperimentValidationError(
                f"{case_id}: normalized {table_label} has {len(normalized)} targets"
            )
        normalized_count = counts[normalized[0].target_id]
        if normalized_count != workload.logical_tuple_count:
            raise ExperimentValidationError(
                f"{case_id}: normalized {table_label} target count "
                f"{normalized_count}, expected {workload.logical_tuple_count}"
            )

        missing_clauses: list[str] = []
        for target in replacement:
            predicate = " AND ".join(
                f"represented.{quote_ident(target_property)} = "
                f"source.{quote_ident(source_property)}"
                for target_property, source_property in zip(
                    target.key_properties,
                    table.key_fields,
                    strict=True,
                )
            )
            missing_clauses.append(
                "EXISTS {\n"
                f"  MATCH (represented:{quote_ident(ARTIFACT_LABEL)}:"
                f"{quote_ident(target.label)} {identity})\n"
                f"  WHERE {predicate}\n"
                "}"
            )
        if not missing_clauses:
            raise ExperimentValidationError(
                f"{case_id}: replacement has no {table_label} target"
            )
        missing = scalar_count(
            session,
            f"""
            MATCH (source:{quote_ident(table.label)})
            WHERE NOT ({' OR '.join(missing_clauses)})
            RETURN count(source) AS count
            """,
            "count",
            f"validate replacement coverage for {case_id}/{table_label}",
            **full_update_identity_parameters(case_id, run_id),
        )
        if missing:
            raise ExperimentValidationError(
                f"{case_id}: replacement is missing {missing} "
                f"{table_label} logical fact(s)"
            )
        replacement_count = sum(counts[target.target_id] for target in replacement)
        table_report[table_label] = {
            "logical_facts": workload.logical_tuple_count,
            "normalized_property_targets": normalized_count,
            "replacement_property_targets": replacement_count,
            "replacement_write_amplification": (
                replacement_count / workload.logical_tuple_count
            ),
            "replacement_target_breakdown": {
                target.target_id: counts[target.target_id] for target in replacement
            },
            "missing_logical_facts": 0,
        }

    normalized_total = sum(
        counts[target.target_id]
        for target in targets
        if target.layout == "normalized" and target.table_label in workloads
    )
    replacement_total = sum(
        counts[target.target_id]
        for target in targets
        if target.layout == "replacement" and target.table_label in workloads
    )
    logical_total = sum(
        workload.logical_tuple_count for workload in workloads.values()
    )
    if normalized_total != logical_total:
        raise ExperimentValidationError(
            f"{case_id}: normalized full-update targets={normalized_total}, "
            f"logical facts={logical_total}"
        )
    return counts, {
        "logical_facts": logical_total,
        "normalized_property_targets": normalized_total,
        "replacement_property_targets": replacement_total,
        "replacement_write_amplification": replacement_total / normalized_total,
        "missing_logical_facts": 0,
        "conflicting_physical_targets": 0,
        "updated_physical_tables": list(workloads),
        "excluded_physical_tables": list(FULL_UPDATE_EXCLUDED_TABLES),
        "by_table": table_report,
    }


def route_full_update_workloads(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    targets: Sequence[FullUpdateTarget],
    workloads: Mapping[str, UpdateWorkload],
    batch_size: int,
) -> dict[str, int]:
    """Route only represented logical keys to each physical target family."""

    routed_counts: dict[str, int] = {}
    for target in targets:
        if target.table_label not in workloads:
            continue
        workload = workloads[target.table_label]
        connection = workload._require_connection()
        connection.execute(
            "DELETE FROM physical_target_key WHERE target_id = ?",
            (target.target_id,),
        )
        projection = ", ".join(
            f"node.{quote_ident(prop)}" for prop in target.key_properties
        )
        result = session.run(
            f"""
            MATCH (
              node:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(target.label)}
              {artifact_identity_map()}
            )
            RETURN DISTINCT [{projection}] AS key
            """,
            full_update_identity_parameters(case_id, run_id),
        )
        rows: list[tuple[str, str]] = []
        routed = 0
        for record in result:
            key = canonical_key(record["key"])
            if any(value is None for value in key):
                raise ExperimentValidationError(
                    f"{case_id}: NULL key in target {target.target_id}"
                )
            key_token = json.dumps(
                [domain_value_token(value) for value in key],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            rows.append((target.target_id, key_token))
            routed += 1
            if len(rows) >= batch_size:
                try:
                    connection.executemany(
                        "INSERT INTO physical_target_key VALUES (?, ?)",
                        rows,
                    )
                except sqlite3.IntegrityError as exc:
                    raise ExperimentValidationError(
                        f"{case_id}: target {target.target_id} contains a key "
                        "outside the full logical workload"
                    ) from exc
                rows = []
        result.consume()
        if rows:
            try:
                connection.executemany(
                    "INSERT INTO physical_target_key VALUES (?, ?)",
                    rows,
                )
            except sqlite3.IntegrityError as exc:
                raise ExperimentValidationError(
                    f"{case_id}: target {target.target_id} contains a key "
                    "outside the full logical workload"
                ) from exc
        connection.commit()
        database_count = int(
            connection.execute(
                "SELECT count(*) FROM physical_target_key WHERE target_id = ?",
                (target.target_id,),
            ).fetchone()[0]
        )
        if database_count != routed:
            raise ExperimentValidationError(
                f"{case_id}: routed {routed} keys for {target.target_id}, "
                f"stored {database_count}"
            )
        routed_counts[target.target_id] = routed
    return routed_counts


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
        physical_property = pair.referenced_binding_by_logical_name[property_name]
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
                case_parameters(pair, run_id) | {"expected_values": expected_values},
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
                    context=(f"{pair.case_id} {phase} best-effort restore batch"),
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


def full_target_update_query(
    target: FullUpdateTarget,
) -> str:
    key_predicate = " AND ".join(
        f"node.{quote_ident(prop)} = update.key[{index}]"
        for index, prop in enumerate(target.key_properties)
    )
    index_properties = ", ".join(
        quote_ident(prop) for prop in (RUN_ID_PROPERTY, *target.key_properties)
    )
    return f"""
    UNWIND $updates AS update
    MATCH (
      node:{quote_ident(ARTIFACT_LABEL)}:{quote_ident(target.label)}
      {artifact_identity_map()}
    )
    USING INDEX node:{quote_ident(target.label)}({index_properties})
    WHERE {key_predicate}
    SET node.{quote_ident(target.physical_property)} = update.value
    RETURN count(node) AS updated_nodes
    """


def execute_full_update_arm(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    layout: Literal["normalized", "replacement"],
    targets: Sequence[FullUpdateTarget],
    workloads: Mapping[str, UpdateWorkload],
    target_counts: Mapping[str, int],
    restore: bool,
    batch_size: int,
    run: int,
    allow_idempotent_noop_writes: bool = False,
) -> tuple[TimingSample, dict[str, Any]]:
    totals = MutationTotals()
    physical_occurrences = 0
    arm_started_ns = perf_counter_ns()
    database_transaction_ns = 0
    database_query_ns = 0
    per_table: dict[str, dict[str, Any]] = {}
    per_target: dict[str, dict[str, Any]] = {}
    layout_targets = [target for target in targets if target.layout == layout]
    for table_spec in TPC_H_TBL_TABLES:
        table_label = table_spec.label
        if table_label not in workloads:
            continue
        workload = workloads[table_label]
        table_started_ns = perf_counter_ns()
        table_transaction_ns = 0
        table_query_ns = 0
        table_occurrences = 0
        table_properties = 0
        query_invocations = 0
        logical_batches = 0
        logical_facts_processed = 0
        logical_key_target_submissions = 0
        table_targets = [
            target
            for target in layout_targets
            if target.table_label == table_label
        ]
        target_by_id = {target.target_id: target for target in table_targets}
        for target in table_targets:
            per_target[target.target_id] = {
                "target_id": target.target_id,
                "table": table_label,
                "component_id": target.component_id,
                "component_kind": target.component_kind,
                "database_query_client_wall_ms": 0.0,
                "matched_property_occurrences": 0,
                "properties_set": 0,
                "query_invocations": 0,
                "logical_key_submissions": 0,
            }
        for logical_batch_size, routed_updates in (
            workload.routed_transaction_batches(
                batch_size,
                restore=restore,
                target_ids=tuple(target_by_id),
            )
        ):
            logical_batches += 1
            logical_facts_processed += logical_batch_size
            transaction_started_ns = perf_counter_ns()
            transaction = session.begin_transaction()
            try:
                for target_id, target in target_by_id.items():
                    updates = routed_updates.get(target_id, ())
                    if not updates:
                        continue
                    query_started_ns = perf_counter_ns()
                    record, summary = one_record(
                        transaction.run(
                            full_target_update_query(target),
                            full_update_identity_parameters(case_id, run_id)
                            | {"updates": updates},
                        ),
                        (
                            f"{case_id} {layout} {table_label} "
                            f"{'restore' if restore else 'update'} "
                            f"target {target.component_id}"
                        ),
                    )
                    query_elapsed_ns = perf_counter_ns() - query_started_ns
                    database_query_ns += query_elapsed_ns
                    table_query_ns += query_elapsed_ns
                    matched = int(record["updated_nodes"])
                    properties_set = int(summary.counters.properties_set)
                    physical_occurrences += matched
                    table_occurrences += matched
                    table_properties += properties_set
                    query_invocations += 1
                    logical_key_target_submissions += len(updates)
                    target_report = per_target[target_id]
                    target_report["database_query_client_wall_ms"] += (
                        query_elapsed_ns / 1_000_000
                    )
                    target_report["matched_property_occurrences"] += matched
                    target_report["properties_set"] += properties_set
                    target_report["query_invocations"] += 1
                    target_report["logical_key_submissions"] += len(updates)
                    totals.add_summary(summary)
                transaction.commit()
            except BaseException:
                active_error = sys.exc_info()[1]
                try:
                    transaction.rollback()
                except BaseException as rollback_error:
                    if active_error is not None:
                        active_error.add_note(
                            "explicit full-update transaction rollback also "
                            f"failed: {type(rollback_error).__name__}: "
                            f"{rollback_error}"
                        )
                raise
            transaction_elapsed_ns = perf_counter_ns() - transaction_started_ns
            database_transaction_ns += transaction_elapsed_ns
            table_transaction_ns += transaction_elapsed_ns
        if logical_facts_processed != workload.logical_tuple_count:
            raise ExperimentValidationError(
                f"{case_id} {layout} {table_label}: routed "
                f"{logical_facts_processed} logical facts, expected "
                f"{workload.logical_tuple_count}"
            )
        for target in table_targets:
            target_report = per_target[target.target_id]
            expected = int(target_counts[target.target_id])
            matched_target = int(
                target_report["matched_property_occurrences"]
            )
            properties_target = int(target_report["properties_set"])
            properties_reconcile = (
                0 <= properties_target <= expected
                if allow_idempotent_noop_writes
                else properties_target == expected
            )
            if matched_target != expected or not properties_reconcile:
                raise ExperimentValidationError(
                    f"{case_id} {layout}: target {target.target_id} wrote "
                    f"{target_report['matched_property_occurrences']}/"
                    f"{target_report['properties_set']} occurrences/properties, "
                    f"expected {expected}"
                )
        table_elapsed_ns = perf_counter_ns() - table_started_ns
        per_table[table_label] = {
            "end_to_end_client_wall_ms": table_elapsed_ns / 1_000_000,
            "database_transaction_client_wall_ms": (
                table_transaction_ns / 1_000_000
            ),
            "database_query_client_wall_ms": table_query_ns / 1_000_000,
            "orchestration_residual_client_wall_ms": max(
                0.0,
                (table_elapsed_ns - table_transaction_ns) / 1_000_000,
            ),
            "logical_facts": workload.logical_tuple_count,
            "logical_batches": logical_batches,
            "explicit_transactions": logical_batches,
            "matched_property_occurrences": table_occurrences,
            "properties_set": table_properties,
            "query_invocations": query_invocations,
            "logical_key_target_submissions": logical_key_target_submissions,
        }

    arm_elapsed_ns = perf_counter_ns() - arm_started_ns
    sample = totals_timing_sample(
        run,
        arm_elapsed_ns / 1_000_000,
        totals,
        physical_occurrences,
    )
    by_component_kind: dict[str, dict[str, int | float]] = {}
    for target_report in per_target.values():
        component = by_component_kind.setdefault(
            str(target_report["component_kind"]),
            {
                "database_query_client_wall_ms": 0.0,
                "matched_property_occurrences": 0,
                "properties_set": 0,
                "query_invocations": 0,
                "logical_key_submissions": 0,
            },
        )
        for field_name in (
            "database_query_client_wall_ms",
            "matched_property_occurrences",
            "properties_set",
            "query_invocations",
            "logical_key_submissions",
        ):
            component[field_name] += target_report[field_name]
    return sample, {
        "run": run,
        "layout": layout,
        "restore": restore,
        "end_to_end_client_wall_ms": arm_elapsed_ns / 1_000_000,
        "database_transaction_client_wall_ms": (
            database_transaction_ns / 1_000_000
        ),
        "database_query_client_wall_ms": database_query_ns / 1_000_000,
        "orchestration_residual_client_wall_ms": max(
            0.0,
            (arm_elapsed_ns - database_transaction_ns) / 1_000_000,
        ),
        "logical_facts": sum(
            workload.logical_tuple_count for workload in workloads.values()
        ),
        "logical_batches": sum(
            int(item["logical_batches"]) for item in per_table.values()
        ),
        "explicit_transactions": sum(
            int(item["explicit_transactions"]) for item in per_table.values()
        ),
        "matched_property_occurrences": physical_occurrences,
        "properties_set": totals.properties_set,
        "query_invocations": sum(
            int(item["query_invocations"]) for item in per_table.values()
        ),
        "logical_key_target_submissions": sum(
            int(item["logical_key_target_submissions"])
            for item in per_table.values()
        ),
        "by_table": per_table,
        "by_target": per_target,
        "by_component_kind": by_component_kind,
    }


def verify_full_update_values(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    layout: Literal["normalized", "replacement"],
    targets: Sequence[FullUpdateTarget],
    workloads: Mapping[str, UpdateWorkload],
    target_counts: Mapping[str, int],
    updated: bool,
    batch_size: int,
    context: str,
) -> None:
    for target in (
        item
        for item in targets
        if item.layout == layout and item.table_label in workloads
    ):
        workload = workloads[target.table_label]
        key_predicate = " AND ".join(
            f"node.{quote_ident(prop)} = expected.key[{index}]"
            for index, prop in enumerate(target.key_properties)
        )
        index_properties = ", ".join(
            quote_ident(prop) for prop in (RUN_ID_PROPERTY, *target.key_properties)
        )
        found_nodes = 0
        incorrect_nodes = 0
        for expected_values in workload.parameter_batches(
            batch_size,
            restore=not updated,
            target_id=target.target_id,
        ):
            record, _ = one_record(
                session.run(
                    f"""
                    UNWIND $expected_values AS expected
                    CALL (expected) {{
                      MATCH (
                        node:{quote_ident(ARTIFACT_LABEL)}
                            :{quote_ident(target.label)}
                        {artifact_identity_map()}
                      )
                      USING INDEX node:{quote_ident(target.label)}({index_properties})
                      WHERE {key_predicate}
                      RETURN
                        count(node) AS matched_nodes,
                        coalesce(sum(
                          CASE
                            WHEN node.{quote_ident(target.physical_property)} =
                                 expected.value
                            THEN 0 ELSE 1
                          END
                        ), 0) AS incorrect_nodes
                    }}
                    RETURN
                      coalesce(sum(matched_nodes), 0) AS matched_nodes,
                      coalesce(sum(incorrect_nodes), 0) AS incorrect_nodes
                    """,
                    full_update_identity_parameters(case_id, run_id)
                    | {"expected_values": expected_values},
                ),
                f"{case_id} {layout} {context} {target.target_id}",
            )
            found_nodes += int(record["matched_nodes"])
            incorrect_nodes += int(record["incorrect_nodes"])
        expected_count = int(target_counts[target.target_id])
        if found_nodes != expected_count or incorrect_nodes:
            raise ExperimentValidationError(
                f"{case_id} {layout} {context}: target {target.target_id} "
                f"matched={found_nodes}, expected={expected_count}, "
                f"incorrect={incorrect_nodes}"
            )


def benchmark_full_replacement_pair(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    targets: Sequence[FullUpdateTarget],
    workloads: Mapping[str, UpdateWorkload],
    target_counts: Mapping[str, int],
    expected_by_layout: Mapping[str, int],
    warmup_runs: int,
    measured_runs: int,
    batch_size: int,
) -> tuple[list[TimingSample], list[TimingSample], dict[str, Any]]:
    samples: dict[str, list[TimingSample]] = {
        "normalized": [],
        "replacement": [],
    }
    measured_details: dict[str, list[dict[str, Any]]] = {
        "normalized": [],
        "replacement": [],
    }
    schedule: list[dict[str, Any]] = []
    restore_recovery_events: list[dict[str, Any]] = []
    total_trials = warmup_runs + measured_runs
    validated_successor: set[str] = set()

    def restore_and_validate(
        layout: Literal["normalized", "replacement"],
        *,
        trial: int,
        context: str,
    ) -> TimingSample:
        """Idempotently restore one arm, retrying a partial failed restore."""

        first_failure: BaseException | None = None
        for attempt in (1, 2):
            try:
                restore_sample, _restore_detail = execute_full_update_arm(
                    session,
                    case_id=case_id,
                    run_id=run_id,
                    layout=layout,
                    targets=targets,
                    workloads=workloads,
                    target_counts=target_counts,
                    restore=True,
                    batch_size=batch_size,
                    run=0,
                    allow_idempotent_noop_writes=(attempt > 1),
                )
                expected = int(expected_by_layout[layout])
                if (
                    restore_sample.affected_nodes != expected
                    or (
                        restore_sample.properties_set != expected
                        if attempt == 1
                        else not 0 <= restore_sample.properties_set <= expected
                    )
                ):
                    raise ExperimentValidationError(
                        f"{case_id} {layout}: restore writes do not reconcile"
                    )
                verify_full_update_values(
                    session,
                    case_id=case_id,
                    run_id=run_id,
                    layout=layout,
                    targets=targets,
                    workloads=workloads,
                    target_counts=target_counts,
                    updated=False,
                    batch_size=batch_size,
                    context=f"{context} restore validation attempt {attempt}",
                )
                return restore_sample
            except BaseException as restore_error:
                if attempt == 1:
                    first_failure = restore_error
                    restore_recovery_events.append(
                        {
                            "trial": trial,
                            "layout": layout,
                            "context": context,
                            "failed_attempt": attempt,
                            "error_type": type(restore_error).__name__,
                            "error": str(restore_error),
                            "action": (
                                "rerun the complete idempotent restore and "
                                "validate every target family"
                            ),
                        }
                    )
                    continue
                if first_failure is not None:
                    restore_error.add_note(
                        "first idempotent restore attempt also failed: "
                        f"{type(first_failure).__name__}: {first_failure}"
                    )
                raise
        raise AssertionError("unreachable restore retry state")

    for ordinal in range(1, total_trials + 1):
        measured_number = ordinal - warmup_runs
        order: tuple[Literal["normalized", "replacement"], ...] = (
            ("normalized", "replacement")
            if ordinal % 2
            else ("replacement", "normalized")
        )
        schedule.append(
            {
                "trial": ordinal,
                "measured": ordinal > warmup_runs,
                "order": list(order),
            }
        )
        for layout in order:
            try:
                sample, detail = execute_full_update_arm(
                    session,
                    case_id=case_id,
                    run_id=run_id,
                    layout=layout,
                    targets=targets,
                    workloads=workloads,
                    target_counts=target_counts,
                    restore=False,
                    batch_size=batch_size,
                    run=max(measured_number, 0),
                )
                expected = int(expected_by_layout[layout])
                if sample.affected_nodes != expected:
                    raise ExperimentValidationError(
                        f"{case_id} {layout}: matched "
                        f"{sample.affected_nodes} property occurrences, "
                        f"expected {expected}"
                    )
                if sample.properties_set != expected:
                    raise ExperimentValidationError(
                        f"{case_id} {layout}: Neo4j reported "
                        f"{sample.properties_set} property writes, expected {expected}"
                    )
                if any(
                    (
                        sample.nodes_created,
                        sample.nodes_deleted,
                        sample.relationships_created,
                        sample.relationships_deleted,
                        sample.labels_added,
                        sample.labels_removed,
                    )
                ):
                    raise ExperimentValidationError(
                        f"{case_id} {layout}: scalar update changed graph topology"
                    )
                if layout not in validated_successor:
                    verify_full_update_values(
                        session,
                        case_id=case_id,
                        run_id=run_id,
                        layout=layout,
                        targets=targets,
                        workloads=workloads,
                        target_counts=target_counts,
                        updated=True,
                        batch_size=batch_size,
                        context="successor validation",
                    )
                    validated_successor.add(layout)
            except BaseException:
                active_error = sys.exc_info()[1]
                try:
                    restore_and_validate(
                        layout,
                        trial=ordinal,
                        context="failed update compensation",
                    )
                except BaseException as restore_error:
                    if active_error is not None:
                        active_error.add_note(
                            f"{case_id} {layout} compensating restore failed: "
                            f"{type(restore_error).__name__}: {restore_error}"
                        )
                raise
            else:
                restore_and_validate(
                    layout,
                    trial=ordinal,
                    context="post-update",
                )
                if ordinal > warmup_runs:
                    samples[layout].append(sample)
                    measured_details[layout].append(detail)

    for layout in ("normalized", "replacement"):
        verify_full_update_values(
            session,
            case_id=case_id,
            run_id=run_id,
            layout=layout,
            targets=targets,
            workloads=workloads,
            target_counts=target_counts,
            updated=False,
            batch_size=batch_size,
            context="final restore validation",
        )
    return (
        samples["normalized"],
        samples["replacement"],
        {
            "counterbalanced_trial_schedule": schedule,
            "measured_run_details": measured_details,
            "restore_recovery_events": restore_recovery_events,
            "restore_policy": (
                "every arm is restored and value-validated; a failed or "
                "partial restore is rerun once as a complete idempotent "
                "restore and validated again"
            ),
        },
    )


def remove_implicit_region_properties(
    session: Any,
    *,
    case: PairSpec | RecursiveCaseSpec,
    run_id: str,
    configuration_redundancy: Mapping[str, Any] | None,
    batch_size: int,
) -> int:
    if not isinstance(case, RecursiveCaseSpec) or configuration_redundancy is None:
        return 0
    completions = configuration_redundancy.get(
        "implicit_physical_edge_completions",
        (),
    )
    if not completions:
        return 0
    removed_total = 0
    for completion in completions:
        _nation_symbol, region_symbol = tuple(completion["edge"])
        physical_properties = tuple(
            recursive_role_folded_property(region_symbol, prop)
            for prop in TABLES["REGION"].columns
            if prop not in TABLES["REGION"].key_fields
        )
        remove_clause = ", ".join(
            f"primary.{quote_ident(prop)}" for prop in physical_properties
        )
        result = session.run(
            f"""
            MATCH (
              primary:{quote_ident(ARTIFACT_LABEL)}
                     :{quote_ident(case.names.final_folded_label)}
              {artifact_identity_map()}
            )
            CALL (primary) {{
              REMOVE {remove_clause}
            }} IN TRANSACTIONS OF {batch_size} ROWS
            """,
            recursive_parameters(case, run_id),
        )
        summary = result.consume()
        removed_total += int(summary.counters.properties_set)
    return removed_total


def delete_full_topology_anchors(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    batch_size: int,
) -> CleanupResult:
    """Remove routing-only anchors outside conversion/update timings."""

    parameters = full_update_identity_parameters(case_id, run_id)
    deleted_nodes = 0
    deleted_relationships = 0
    while True:
        record, summary = one_record(
            session.run(
                f"""
                MATCH (
                  anchor:{quote_ident(ARTIFACT_LABEL)}
                         :{quote_ident(FULL_TOPOLOGY_ANCHOR_LABEL)}
                  {artifact_identity_map()}
                )
                WITH anchor
                LIMIT $batch_size
                DETACH DELETE anchor
                RETURN count(*) AS selected_nodes
                """,
                parameters | {"batch_size": batch_size},
            ),
            f"delete {case_id} full-topology anchor batch",
        )
        selected = int(record["selected_nodes"])
        if selected == 0:
            break
        deleted_nodes += int(summary.counters.nodes_deleted)
        deleted_relationships += int(summary.counters.relationships_deleted)
    return CleanupResult(deleted_nodes, deleted_relationships)


def delete_full_layout_nodes(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    layout: Literal["normalized", "replacement"],
    batch_size: int,
    mutation_totals: MutationTotals | None = None,
) -> CleanupResult:
    """Delete one isolated full layout, including its explicit topology."""

    parameters = full_update_identity_parameters(case_id, run_id) | {
        "layout": layout,
        "batch_size": batch_size,
    }
    deleted_nodes = 0
    deleted_relationships = 0
    while True:
        record, summary = one_record(
            session.run(
                f"""
                MATCH (
                  node:{quote_ident(ARTIFACT_LABEL)}
                  {artifact_identity_map()}
                )
                WHERE node.mv_full_layout = $layout
                  AND NOT node:{quote_ident(FULL_TOPOLOGY_ANCHOR_LABEL)}
                WITH node
                LIMIT $batch_size
                DETACH DELETE node
                RETURN count(*) AS selected_nodes
                """,
                parameters,
            ),
            f"delete {case_id} {layout} full-layout batch",
        )
        selected = int(record["selected_nodes"])
        if mutation_totals is not None:
            mutation_totals.add_summary(summary)
        if selected == 0:
            break
        deleted_nodes += int(summary.counters.nodes_deleted)
        deleted_relationships += int(summary.counters.relationships_deleted)
    return CleanupResult(deleted_nodes, deleted_relationships)


def converted_normalized_targets_by_table(
    targets: Sequence[FullUpdateTarget],
) -> dict[str, FullUpdateTarget]:
    by_table: dict[str, FullUpdateTarget] = {}
    for target in targets:
        if target.layout != "normalized":
            continue
        if target.table_label in by_table:
            raise ExperimentValidationError(
                f"more than one normalized target for {target.table_label}"
            )
        by_table[target.table_label] = target
    if set(by_table) != set(TABLES):
        raise ExperimentValidationError(
            "normalized conversion targets do not cover all eight tables"
        )
    return by_table


def materialize_normalized_nodes_from_replacement(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    targets: Sequence[FullUpdateTarget],
    occurrences: Sequence[FullTopologyOccurrence],
    expected_rows_by_table: Mapping[str, int],
    batch_size: int,
    mutation_totals: MutationTotals,
) -> dict[str, Any]:
    """Reconstruct normalized rows exclusively from replacement occurrences."""

    normalized_by_table = converted_normalized_targets_by_table(targets)
    reports: dict[str, Any] = {}
    for table_spec in TPC_H_TBL_TABLES:
        table = TABLES[table_spec.label]
        normalized_target = normalized_by_table[table.label]
        replacement_occurrences = [
            occurrence
            for occurrence in occurrences
            if occurrence.layout == "replacement"
            and occurrence.table_label == table.label
        ]
        if not replacement_occurrences:
            raise ExperimentValidationError(
                f"{case_id}: replacement has no {table.label} occurrence"
            )

        table_source_occurrences = 0
        table_nodes_created = 0
        family_reports: list[dict[str, Any]] = []
        for occurrence in replacement_occurrences:
            bindings = occurrence.binding_by_logical_name
            merge_properties = ",\n                    ".join(
                [f"{quote_ident(RUN_ID_PROPERTY)}: $run_id"]
                + [
                    f"{quote_ident(key)}: "
                    f"physical.{quote_ident(bindings[key])}"
                    for key in table.key_fields
                ]
            )
            assignments = [
                f"normalized.{quote_ident(CASE_ID_PROPERTY)} = $case_id",
                "normalized.mv_full_layout = 'normalized'",
                f"normalized.mv_full_component = "
                f"'converted:{snake_case(table.label)}'",
                f"normalized.mv_source_table = $table_label",
            ]
            assignments.extend(
                f"normalized.{quote_ident(prop)} = "
                f"physical.{quote_ident(bindings[prop])}"
                for prop in table.columns
                if prop not in table.key_fields
            )
            record, summary = one_record(
                session.run(
                    f"""
                    MATCH (
                      physical:{quote_ident(ARTIFACT_LABEL)}
                              :{quote_ident(occurrence.label)}
                      {artifact_identity_map()}
                    )
                    WHERE physical.mv_full_layout = 'replacement'
                    CALL (physical) {{
                      MERGE (
                        normalized:{quote_ident(ARTIFACT_LABEL)}
                                  :{quote_ident(normalized_target.label)} {{
                          {merge_properties}
                        }}
                      )
                      ON CREATE SET {', '.join(assignments)}
                    }} IN TRANSACTIONS OF {batch_size} ROWS
                    RETURN count(physical) AS source_occurrences
                    """,
                    full_update_identity_parameters(case_id, run_id)
                    | {"table_label": table.label},
                ),
                f"convert replacement {occurrence.occurrence_id} to normalized",
            )
            mutation_totals.add_summary(summary)
            source_occurrences = int(record["source_occurrences"])
            nodes_created = int(summary.counters.nodes_created)
            table_source_occurrences += source_occurrences
            table_nodes_created += nodes_created
            family_reports.append(
                {
                    "source_occurrence_id": occurrence.occurrence_id,
                    "source_occurrences_scanned": source_occurrences,
                    "normalized_nodes_created": nodes_created,
                }
            )

        expected = int(expected_rows_by_table[table.label])
        if table_nodes_created != expected:
            raise ExperimentValidationError(
                f"{case_id}: converted normalized {table.label} has "
                f"{table_nodes_created} newly created nodes, expected {expected}"
            )
        reports[table.label] = {
            "logical_rows": expected,
            "replacement_occurrences_scanned": table_source_occurrences,
            "normalized_nodes_created": table_nodes_created,
            "normalized_nodes_after_merge": table_nodes_created,
            "families": family_reports,
        }
    return {
        "by_table": reports,
        "nodes_created": sum(
            int(report["normalized_nodes_created"])
            for report in reports.values()
        ),
        "replacement_occurrences_scanned": sum(
            int(report["replacement_occurrences_scanned"])
            for report in reports.values()
        ),
    }


def materialize_normalized_topology_from_converted_properties(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    targets: Sequence[FullUpdateTarget],
    source_topology: Mapping[str, Any],
    batch_size: int,
    mutation_totals: MutationTotals,
) -> dict[str, Any]:
    """Create normalized FK edges from converted scalar FK properties only."""

    normalized_by_table = converted_normalized_targets_by_table(targets)
    logical_edge_ids_by_type: dict[str, list[str]] = {}
    for logical_edge_id, relationship, _, _ in full_topology_role_edges():
        logical_edge_ids_by_type.setdefault(
            relationship.relationship_type,
            [],
        ).append(logical_edge_id)

    reports: list[dict[str, Any]] = []
    expected_by_type = source_topology["by_physical_type"]
    for relationship in RELATIONSHIPS:
        if relationship.row_label != relationship.source_label:
            raise ExperimentValidationError(
                "replacement conversion currently requires FK-owning rows "
                "to be relationship sources"
            )
        source_target = normalized_by_table[relationship.source_label]
        referenced_target = normalized_by_table[relationship.target_label]
        referenced_table = TABLES[relationship.target_label]
        target_predicate = " AND ".join(
            f"target.{quote_ident(endpoint_property)} = "
            f"source.{quote_ident(row_property)}"
            for endpoint_property, row_property in relationship.target_keys
        )
        target_index_properties = ", ".join(
            quote_ident(prop)
            for prop in (RUN_ID_PROPERTY, *referenced_table.key_fields)
        )
        logical_edge_ids = sorted(
            logical_edge_ids_by_type[relationship.relationship_type]
        )
        record, summary = one_record(
            session.run(
                f"""
                MATCH (
                  source:{quote_ident(ARTIFACT_LABEL)}
                         :{quote_ident(source_target.label)}
                  {artifact_identity_map()}
                )
                MATCH (
                  target:{quote_ident(ARTIFACT_LABEL)}
                         :{quote_ident(referenced_target.label)}
                  {artifact_identity_map()}
                )
                USING INDEX target:{quote_ident(referenced_target.label)}(
                  {target_index_properties}
                )
                WHERE source.mv_full_layout = 'normalized'
                  AND target.mv_full_layout = 'normalized'
                  AND {target_predicate}
                CALL (source, target) {{
                  CREATE
                    (source)
                    -[copy:{quote_ident(full_topology_relationship_type(relationship.relationship_type))}]->
                    (target)
                  SET copy.{quote_ident(FULL_TOPOLOGY_LOGICAL_EDGE_IDS_PROPERTY)} =
                    $logical_edge_ids
                }} IN TRANSACTIONS OF {batch_size} ROWS
                RETURN count(source) AS routed_rows
                """,
                full_update_identity_parameters(case_id, run_id)
                | {"logical_edge_ids": logical_edge_ids},
            ),
            f"convert normalized topology {relationship.relationship_type}",
        )
        mutation_totals.add_summary(summary)
        routed = int(record["routed_rows"])
        created = int(summary.counters.relationships_created)
        expected = int(expected_by_type[relationship.relationship_type])
        if routed != expected or created != expected:
            raise ExperimentValidationError(
                f"{case_id}: converted {relationship.relationship_type} "
                f"routed={routed}, created={created}, expected={expected}"
            )
        reports.append(
            {
                "source_relationship_type": relationship.relationship_type,
                "physical_relationship_type": (
                    full_topology_relationship_type(
                        relationship.relationship_type
                    )
                ),
                "logical_edge_ids": logical_edge_ids,
                "relationships_created": created,
            }
        )
    return {
        "by_type": reports,
        "relationships_created": sum(
            int(report["relationships_created"]) for report in reports
        ),
    }


def validate_converted_normalized_layout(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    targets: Sequence[FullUpdateTarget],
    expected_rows_by_table: Mapping[str, int],
    expected_normalized_topology: Mapping[str, int],
) -> dict[str, Any]:
    """Validate the destructive conversion after its timer has stopped."""

    normalized_by_table = converted_normalized_targets_by_table(targets)
    table_reports: dict[str, Any] = {}
    for table_spec in TPC_H_TBL_TABLES:
        table = TABLES[table_spec.label]
        target = normalized_by_table[table.label]
        key_predicate = " AND ".join(
            f"source.{quote_ident(key)} = normalized.{quote_ident(key)}"
            for key in table.key_fields
        )
        value_predicate = " AND ".join(
            f"source.{quote_ident(prop)} = normalized.{quote_ident(prop)}"
            for prop in table.columns
        )
        record, _ = one_record(
            session.run(
                f"""
                MATCH (
                  normalized:{quote_ident(ARTIFACT_LABEL)}
                            :{quote_ident(target.label)}
                  {artifact_identity_map()}
                )
                WHERE normalized.mv_full_layout = 'normalized'
                CALL (normalized) {{
                  MATCH (source:{quote_ident(table.label)})
                  WHERE {key_predicate}
                  RETURN
                    count(source) AS source_matches,
                    head(collect(source)) AS source
                }}
                RETURN
                  count(normalized) AS normalized_rows,
                  coalesce(sum(
                    CASE
                      WHEN source_matches = 1 AND {value_predicate}
                      THEN 0 ELSE 1
                    END
                  ), 0) AS invalid_rows
                """,
                full_update_identity_parameters(case_id, run_id),
            ),
            f"validate converted normalized {table.label}",
        )
        rows = int(record["normalized_rows"])
        invalid = int(record["invalid_rows"])
        expected = int(expected_rows_by_table[table.label])
        missing = scalar_count(
            session,
            f"""
            MATCH (source:{quote_ident(table.label)})
            WHERE NOT EXISTS {{
              MATCH (
                normalized:{quote_ident(ARTIFACT_LABEL)}
                          :{quote_ident(target.label)}
                {artifact_identity_map()}
              )
              WHERE normalized.mv_full_layout = 'normalized'
                AND {' AND '.join(
                    f'normalized.{quote_ident(key)} = source.{quote_ident(key)}'
                    for key in table.key_fields
                )}
            }}
            RETURN count(source) AS count
            """,
            "count",
            f"validate converted normalized coverage for {table.label}",
            **full_update_identity_parameters(case_id, run_id),
        )
        if rows != expected or invalid or missing:
            raise ExperimentValidationError(
                f"{case_id}: converted normalized {table.label} rows="
                f"{rows}/{expected}, invalid={invalid}, missing={missing}"
            )
        table_reports[table.label] = {
            "logical_rows": expected,
            "normalized_rows": rows,
            "invalid_rows": invalid,
            "missing_rows": missing,
            "all_columns_equal_source": True,
        }

    remaining_replacement = scalar_count(
        session,
        f"""
        MATCH (node:{quote_ident(ARTIFACT_LABEL)} {artifact_identity_map()})
        WHERE node.mv_full_layout = 'replacement'
        RETURN count(node) AS count
        """,
        "count",
        f"validate {case_id} replacement removed by conversion",
        **full_update_identity_parameters(case_id, run_id),
    )
    remaining_anchors = scalar_count(
        session,
        f"""
        MATCH (
          anchor:{quote_ident(ARTIFACT_LABEL)}
                 :{quote_ident(FULL_TOPOLOGY_ANCHOR_LABEL)}
          {artifact_identity_map()}
        )
        RETURN count(anchor) AS count
        """,
        "count",
        f"validate {case_id} conversion anchors removed",
        **full_update_identity_parameters(case_id, run_id),
    )
    topology_signature = full_topology_signature(
        session,
        case_id=case_id,
        run_id=run_id,
    )
    if (
        topology_signature["normalized"]
        != dict(expected_normalized_topology)
        or topology_signature["replacement"]
        or remaining_replacement
        or remaining_anchors
    ):
        raise ExperimentValidationError(
            f"{case_id}: converted layout state is invalid: topology="
            f"{topology_signature!r}, replacement_nodes="
            f"{remaining_replacement}, anchors={remaining_anchors}"
        )

    endpoint_reports: dict[str, Any] = {}
    for relationship in RELATIONSHIPS:
        source_target = normalized_by_table[relationship.source_label]
        target_target = normalized_by_table[relationship.target_label]
        endpoint_predicates = [
            f"source.{quote_ident(endpoint_property)} = "
            f"source.{quote_ident(row_property)}"
            for endpoint_property, row_property in relationship.source_keys
        ]
        endpoint_predicates.extend(
            f"target.{quote_ident(endpoint_property)} = "
            f"source.{quote_ident(row_property)}"
            for endpoint_property, row_property in relationship.target_keys
        )
        record, _ = one_record(
            session.run(
                f"""
                MATCH (
                  source:{quote_ident(ARTIFACT_LABEL)}
                         :{quote_ident(source_target.label)}
                  {artifact_identity_map()}
                )-[relationship:{quote_ident(full_topology_relationship_type(relationship.relationship_type))}]->(
                  target:{quote_ident(ARTIFACT_LABEL)}
                         :{quote_ident(target_target.label)}
                  {artifact_identity_map()}
                )
                WHERE source.mv_full_layout = 'normalized'
                  AND target.mv_full_layout = 'normalized'
                RETURN
                  count(relationship) AS relationships,
                  sum(CASE WHEN {' AND '.join(endpoint_predicates)} THEN 0 ELSE 1 END)
                    AS invalid_endpoints
                """,
                full_update_identity_parameters(case_id, run_id),
            ),
            f"validate converted endpoints {relationship.relationship_type}",
        )
        relationship_count = int(record["relationships"])
        invalid_endpoints = int(record["invalid_endpoints"] or 0)
        expected = int(
            expected_normalized_topology[
                full_topology_relationship_type(
                    relationship.relationship_type
                )
            ]
        )
        if relationship_count != expected or invalid_endpoints:
            raise ExperimentValidationError(
                f"{case_id}: converted {relationship.relationship_type} "
                f"relationships={relationship_count}/{expected}, "
                f"invalid endpoints={invalid_endpoints}"
            )
        endpoint_reports[relationship.relationship_type] = {
            "relationships": relationship_count,
            "invalid_endpoints": invalid_endpoints,
        }
    return {
        "status": "complete_normalized_layout_valid",
        "tables": table_reports,
        "topology_signature": topology_signature["normalized"],
        "topology_endpoints": endpoint_reports,
        "replacement_nodes_remaining": remaining_replacement,
        "routing_anchors_remaining": remaining_anchors,
        "source_used_only_for_untimed_validation": True,
    }


def measure_full_replacement_to_normalized_conversion(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    targets: Sequence[FullUpdateTarget],
    occurrences: Sequence[FullTopologyOccurrence],
    expected_rows_by_table: Mapping[str, int],
    source_topology: Mapping[str, Any],
    expected_topology_signature: Mapping[str, Mapping[str, int]],
    expected_normalized_nodes: int,
    expected_replacement_nodes: int,
    batch_size: int,
) -> tuple[TimingSample, dict[str, Any]]:
    """Measure one destructive replacement-to-normalized conversion."""

    anchors_removed = delete_full_topology_anchors(
        session,
        case_id=case_id,
        run_id=run_id,
        batch_size=batch_size,
    )
    old_normalized_removed = delete_full_layout_nodes(
        session,
        case_id=case_id,
        run_id=run_id,
        layout="normalized",
        batch_size=batch_size,
    )
    if old_normalized_removed.deleted_nodes != expected_normalized_nodes:
        raise ExperimentValidationError(
            f"{case_id}: conversion setup removed "
            f"{old_normalized_removed.deleted_nodes} normalized nodes, "
            f"expected {expected_normalized_nodes}"
        )

    mutation_totals = MutationTotals()
    started_ns = perf_counter_ns()
    node_report = materialize_normalized_nodes_from_replacement(
        session,
        case_id=case_id,
        run_id=run_id,
        targets=targets,
        occurrences=occurrences,
        expected_rows_by_table=expected_rows_by_table,
        batch_size=batch_size,
        mutation_totals=mutation_totals,
    )
    topology_report = materialize_normalized_topology_from_converted_properties(
        session,
        case_id=case_id,
        run_id=run_id,
        targets=targets,
        source_topology=source_topology,
        batch_size=batch_size,
        mutation_totals=mutation_totals,
    )
    replacement_removed = delete_full_layout_nodes(
        session,
        case_id=case_id,
        run_id=run_id,
        layout="replacement",
        batch_size=batch_size,
        mutation_totals=mutation_totals,
    )
    client_wall_ms = (perf_counter_ns() - started_ns) / 1_000_000
    if replacement_removed.deleted_nodes != expected_replacement_nodes:
        raise ExperimentValidationError(
            f"{case_id}: conversion removed "
            f"{replacement_removed.deleted_nodes} replacement nodes, "
            f"expected {expected_replacement_nodes}"
        )

    sample = TimingSample(
        run=1,
        client_wall_ms=client_wall_ms,
        available_after_ms=mutation_totals.available_after_ms,
        consumed_after_ms=mutation_totals.consumed_after_ms,
        neo4j_reported_total_ms=(
            mutation_totals.available_after_ms
            + mutation_totals.consumed_after_ms
        ),
        affected_nodes=(
            mutation_totals.nodes_created + mutation_totals.nodes_deleted
        ),
        properties_set=mutation_totals.properties_set,
        nodes_created=mutation_totals.nodes_created,
        nodes_deleted=mutation_totals.nodes_deleted,
        relationships_created=mutation_totals.relationships_created,
        relationships_deleted=mutation_totals.relationships_deleted,
        labels_added=mutation_totals.labels_added,
        labels_removed=mutation_totals.labels_removed,
    )
    validation = validate_converted_normalized_layout(
        session,
        case_id=case_id,
        run_id=run_id,
        targets=targets,
        expected_rows_by_table=expected_rows_by_table,
        expected_normalized_topology=(
            expected_topology_signature["normalized"]
        ),
    )
    return sample, {
        "status": "measured_once",
        "conversion": "full lossless replacement to full normalized layout",
        "destructive": True,
        "source_read_during_timed_conversion": False,
        "input_layout_removed_inside_timer": True,
        "output_layout": "normalized",
        "warmup_runs": 0,
        "measured_runs": 1,
        "cache_policy": (
            "warm cache; server page/query caches are not cleared before "
            "the single conversion"
        ),
        "index_policy": (
            "reuses lookup indexes created before update timing; index DDL "
            "is excluded"
        ),
        "timing_scope": (
            "client wall time from the first replacement scan through "
            "primary-key deduplication into eight normalized tables, creation "
            "of all eight FK relationship types from converted FK properties, "
            "and deletion of the replacement input; every result is consumed "
            "and every batch committed"
        ),
        "excluded_from_timer": (
            "removal of the prebuilt normalized comparison layout and routing "
            "anchors, index DDL, post-conversion validation, source "
            "fingerprinting, and final cleanup"
        ),
        "setup_cleanup": {
            "routing_anchor_nodes_removed": anchors_removed.deleted_nodes,
            "routing_anchor_relationships_removed": (
                anchors_removed.deleted_relationships
            ),
            "prebuilt_normalized_nodes_removed": (
                old_normalized_removed.deleted_nodes
            ),
            "prebuilt_normalized_relationships_removed": (
                old_normalized_removed.deleted_relationships
            ),
        },
        "normalized_node_materialization": node_report,
        "normalized_topology_materialization": topology_report,
        "replacement_input_deletion": asdict(replacement_removed),
        "sample": asdict(sample),
        "summary": timing_summary([sample]),
        "validation": validation,
    }


def cleanup_full_update_clones(
    session: Any,
    *,
    case_id: str,
    run_id: str,
    batch_size: int,
) -> CleanupResult:
    if ARTIFACT_LABEL not in database_node_labels(session):
        return CleanupResult(deleted_nodes=0, deleted_relationships=0)
    parameters = full_update_identity_parameters(case_id, run_id)
    invalid_relationships = scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )-[relationship]-(other)
        WHERE node.mv_full_layout IS NOT NULL
          AND (
            NOT node.mv_full_layout IN $allowed_layouts
            OR NOT $artifact_label IN labels(other)
            OR other.{quote_ident(RUN_ID_PROPERTY)} IS NULL
            OR other.{quote_ident(RUN_ID_PROPERTY)} <> $run_id
            OR other.{quote_ident(CASE_ID_PROPERTY)} IS NULL
            OR other.{quote_ident(CASE_ID_PROPERTY)} <> $case_id
            OR other.mv_full_layout IS NULL
            OR other.mv_full_layout <> node.mv_full_layout
            OR NOT type(relationship) IN $allowed_types
          )
        RETURN count(relationship) AS count
        """,
        "count",
        f"verify {case_id} full-update topology cleanup scope",
        **parameters,
        artifact_label=ARTIFACT_LABEL,
        allowed_layouts=["normalized", "replacement"],
        allowed_types=[
            full_topology_relationship_type(relationship.relationship_type)
            for relationship in RELATIONSHIPS
        ]
        + [FULL_TOPOLOGY_ANCHOR_RELATIONSHIP],
    )
    if invalid_relationships:
        raise ExperimentStateError(
            f"{case_id}: cleanup refused to delete full-update property clones "
            f"with {invalid_relationships} cross-scope or unexpected "
            "relationship(s)"
        )
    expected = scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )
        WHERE node.mv_full_layout IS NOT NULL
        RETURN count(node) AS count
        """,
        "count",
        f"count {case_id} full-update clones",
        **parameters,
    )
    expected_relationships = scalar_count(
        session,
        f"""
        MATCH (
          source:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )-[relationship]->(
          target:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )
        WHERE source.mv_full_layout IS NOT NULL
          AND target.mv_full_layout = source.mv_full_layout
          AND type(relationship) IN $allowed_types
        RETURN count(relationship) AS count
        """,
        "count",
        f"count {case_id} full-update topology relationships",
        **parameters,
        allowed_types=[
            full_topology_relationship_type(relationship.relationship_type)
            for relationship in RELATIONSHIPS
        ]
        + [FULL_TOPOLOGY_ANCHOR_RELATIONSHIP],
    )
    deleted_nodes = 0
    deleted_relationships = 0
    while deleted_nodes < expected:
        record, summary = one_record(
            session.run(
                f"""
                MATCH (
                  node:{quote_ident(ARTIFACT_LABEL)}
                  {artifact_identity_map()}
                )
                WHERE node.mv_full_layout IS NOT NULL
                WITH node
                LIMIT $cleanup_batch_size
                DETACH DELETE node
                RETURN count(*) AS selected_nodes
                """,
                parameters | {"cleanup_batch_size": batch_size},
            ),
            f"delete {case_id} full-update clone batch",
        )
        selected = int(record["selected_nodes"])
        if selected <= 0:
            raise ExperimentValidationError(
                f"{case_id}: full-update cleanup stopped before completion"
            )
        deleted_nodes += int(summary.counters.nodes_deleted)
        deleted_relationships += int(summary.counters.relationships_deleted)
    remaining = scalar_count(
        session,
        f"""
        MATCH (
          node:{quote_ident(ARTIFACT_LABEL)}
          {artifact_identity_map()}
        )
        WHERE node.mv_full_layout IS NOT NULL
        RETURN count(node) AS count
        """,
        "count",
        f"verify {case_id} full-update clone cleanup",
        **parameters,
    )
    if (
        deleted_nodes != expected
        or deleted_relationships != expected_relationships
        or remaining
    ):
        raise ExperimentValidationError(
            f"{case_id}: full-update cleanup deleted {deleted_nodes}/{expected}; "
            f"relationships={deleted_relationships}/{expected_relationships}; "
            f"remaining={remaining}"
        )
    return CleanupResult(deleted_nodes, deleted_relationships)


def full_replacement_update_derived(
    normalized_samples: Sequence[TimingSample],
    replacement_samples: Sequence[TimingSample],
    validation_report: Mapping[str, Any],
) -> dict[str, Any]:
    normalized_median = float(
        timing_summary(normalized_samples)["client_wall_ms"]["median"]
    )
    replacement_median = float(
        timing_summary(replacement_samples)["client_wall_ms"]["median"]
    )
    ratio_of_marginal_medians = (
        replacement_median / normalized_median
        if normalized_median > 0
        else None
    )
    return {
        "replacement_minus_normalized_median_ms": (
            replacement_median - normalized_median
        ),
        "replacement_over_normalized_ratio_of_marginal_medians": (
            ratio_of_marginal_medians
        ),
        "normalized_minus_replacement_median_ms": (
            normalized_median - replacement_median
        ),
        "normalized_over_replacement_median_speedup": (
            replacement_median / normalized_median
            if normalized_median > 0
            else None
        ),
        "ratio_formula": {
            "replacement_over_normalized_ratio_of_marginal_medians": (
                "median(t_R) / median(t_N)"
            ),
            "normalized_over_replacement_median_speedup": "t_R / t_N",
            "interpretation": (
                "a value greater than one means the normalized layout is faster"
            ),
        },
        "physical_property_write_amplification": validation_report[
            "replacement_write_amplification"
        ],
        "physical_write_amplification": validation_report[
            "replacement_write_amplification"
        ],
        "physical_write_amplification_legacy_alias_note": (
            "legacy alias; this ratio counts scalar property-cell writes, "
            "not nodes"
        ),
        "logical_fact_count_both_layouts": validation_report["logical_facts"],
        "normalized_property_targets": validation_report[
            "normalized_property_targets"
        ],
        "replacement_property_targets": validation_report[
            "replacement_property_targets"
        ],
        "layout_conversion_break_even": None,
        "layout_conversion_break_even_reason": (
            "assigned after the single full replacement-to-normalized "
            "conversion measurement; break-even is not calculated"
        ),
    }


def full_update_timing_analysis(
    measured_details: Mapping[str, Sequence[Mapping[str, Any]]],
    validation_report: Mapping[str, Any],
) -> dict[str, Any]:
    """Aggregate paired per-table, micro, macro, and throughput metrics."""

    by_run: dict[str, dict[int, Mapping[str, Any]]] = {}
    for layout in ("normalized", "replacement"):
        layout_by_run = {
            int(detail["run"]): detail
            for detail in measured_details[layout]
        }
        if len(layout_by_run) != len(measured_details[layout]):
            raise ExperimentValidationError(
                f"duplicate measured run number in {layout} full-update details"
            )
        by_run[layout] = layout_by_run
    run_ids = sorted(by_run["normalized"])
    if not run_ids or run_ids != sorted(by_run["replacement"]):
        raise ExperimentValidationError(
            "normalized/replacement measured full-update runs are not paired"
        )

    def rates(counts: Sequence[int], milliseconds: Sequence[float]) -> list[float]:
        return [
            count * 1_000 / duration
            for count, duration in zip(counts, milliseconds, strict=True)
            if duration > 0
        ]

    def timing_breakdown(
        rows: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        transaction_ms = [
            float(row["database_transaction_client_wall_ms"]) for row in rows
        ]
        query_ms = [
            float(row["database_query_client_wall_ms"]) for row in rows
        ]
        return {
            "database_transaction_client_wall_ms": numeric_summary(
                transaction_ms
            ),
            "database_query_client_wall_ms": numeric_summary(query_ms),
            "database_transaction_non_query_client_wall_ms": numeric_summary(
                [
                    max(0.0, transaction - query)
                    for transaction, query in zip(
                        transaction_ms,
                        query_ms,
                        strict=True,
                    )
                ]
            ),
            "orchestration_residual_client_wall_ms": numeric_summary(
                [
                    float(row["orchestration_residual_client_wall_ms"])
                    for row in rows
                ]
            ),
            "logical_batches": numeric_summary(
                [int(row["logical_batches"]) for row in rows]
            ),
            "explicit_transactions": numeric_summary(
                [int(row["explicit_transactions"]) for row in rows]
            ),
            "query_invocations": numeric_summary(
                [int(row["query_invocations"]) for row in rows]
            ),
            "logical_key_target_submissions": numeric_summary(
                [int(row["logical_key_target_submissions"]) for row in rows]
            ),
        }

    per_table: dict[str, Any] = {}
    macro_arithmetic_by_run: list[float] = []
    macro_geometric_by_run: list[float] = []
    ratios_by_run_and_table: dict[int, list[float]] = {
        run_id: [] for run_id in run_ids
    }
    for table_spec in TPC_H_TBL_TABLES:
        table_label = table_spec.label
        if table_label not in validation_report["by_table"]:
            continue
        normalized_table_rows = [
            by_run["normalized"][run_id]["by_table"][table_label]
            for run_id in run_ids
        ]
        replacement_table_rows = [
            by_run["replacement"][run_id]["by_table"][table_label]
            for run_id in run_ids
        ]
        normalized_ms = [
            float(row["end_to_end_client_wall_ms"])
            for row in normalized_table_rows
        ]
        replacement_ms = [
            float(row["end_to_end_client_wall_ms"])
            for row in replacement_table_rows
        ]
        paired_ratios = [
            replacement / normalized if normalized > 0 else math.nan
            for normalized, replacement in zip(
                normalized_ms,
                replacement_ms,
                strict=True,
            )
        ]
        if any(not math.isfinite(value) or value <= 0 for value in paired_ratios):
            raise ExperimentValidationError(
                f"invalid paired timing ratio for {table_label}"
            )
        for run_id, ratio in zip(run_ids, paired_ratios, strict=True):
            ratios_by_run_and_table[run_id].append(ratio)
        logical_facts = int(
            validation_report["by_table"][table_label]["logical_facts"]
        )
        normalized_writes = [
            int(
                by_run["normalized"][run_id]["by_table"][table_label][
                    "properties_set"
                ]
            )
            for run_id in run_ids
        ]
        replacement_writes = [
            int(
                by_run["replacement"][run_id]["by_table"][table_label][
                    "properties_set"
                ]
            )
            for run_id in run_ids
        ]
        per_table[table_label] = {
            "logical_facts": logical_facts,
            "normalized": {
                "end_to_end_client_wall_ms": numeric_summary(normalized_ms),
                **timing_breakdown(normalized_table_rows),
                "logical_facts_per_second": numeric_summary(
                    rates([logical_facts] * len(run_ids), normalized_ms)
                ),
                "physical_property_writes_per_second": numeric_summary(
                    rates(normalized_writes, normalized_ms)
                ),
                "physical_property_writes": sorted(set(normalized_writes)),
            },
            "replacement": {
                "end_to_end_client_wall_ms": numeric_summary(replacement_ms),
                **timing_breakdown(replacement_table_rows),
                "logical_facts_per_second": numeric_summary(
                    rates([logical_facts] * len(run_ids), replacement_ms)
                ),
                "physical_property_writes_per_second": numeric_summary(
                    rates(replacement_writes, replacement_ms)
                ),
                "physical_property_writes": sorted(set(replacement_writes)),
            },
            "paired": {
                "replacement_minus_normalized_ms": numeric_summary(
                    [
                        replacement - normalized
                        for normalized, replacement in zip(
                            normalized_ms,
                            replacement_ms,
                            strict=True,
                        )
                    ]
                ),
                "replacement_over_normalized_ratio": numeric_summary(
                    paired_ratios
                ),
            },
            "physical_property_write_amplification": validation_report[
                "by_table"
            ][table_label]["replacement_write_amplification"],
        }

    for run_id in run_ids:
        ratios = ratios_by_run_and_table[run_id]
        macro_arithmetic_by_run.append(fmean(ratios))
        macro_geometric_by_run.append(
            math.exp(fmean([math.log(value) for value in ratios]))
        )

    normalized_whole_ms = [
        float(by_run["normalized"][run_id]["end_to_end_client_wall_ms"])
        for run_id in run_ids
    ]
    replacement_whole_ms = [
        float(by_run["replacement"][run_id]["end_to_end_client_wall_ms"])
        for run_id in run_ids
    ]
    whole_ratios = [
        replacement / normalized if normalized > 0 else math.nan
        for normalized, replacement in zip(
            normalized_whole_ms,
            replacement_whole_ms,
            strict=True,
        )
    ]
    if any(not math.isfinite(value) or value <= 0 for value in whole_ratios):
        raise ExperimentValidationError(
            "invalid paired whole-database timing ratio"
        )
    logical_total = int(validation_report["logical_facts"])
    normalized_whole_rows = [
        by_run["normalized"][run_id] for run_id in run_ids
    ]
    replacement_whole_rows = [
        by_run["replacement"][run_id] for run_id in run_ids
    ]

    component_kinds = sorted(
        {
            component_kind
            for layout in ("normalized", "replacement")
            for detail in measured_details[layout]
            for component_kind in detail["by_component_kind"]
        }
    )
    component_breakdown: dict[str, Any] = {}
    for component_kind in component_kinds:
        component_breakdown[component_kind] = {}
        for layout in ("normalized", "replacement"):
            rows = [
                detail["by_component_kind"].get(component_kind)
                for detail in measured_details[layout]
            ]
            present = [row for row in rows if row is not None]
            if not present:
                continue
            component_breakdown[component_kind][layout] = {
                "database_query_client_wall_ms": numeric_summary(
                    [float(row["database_query_client_wall_ms"]) for row in present]
                ),
                "physical_property_writes": sorted(
                    {int(row["properties_set"]) for row in present}
                ),
                "logical_key_submissions": sorted(
                    {int(row["logical_key_submissions"]) for row in present}
                ),
                "query_invocations": sorted(
                    {int(row["query_invocations"]) for row in present}
                ),
            }

    return {
        "paired_run_ids": run_ids,
        "whole_database_row_weighted_micro": {
            "normalized_end_to_end_client_wall_ms": numeric_summary(
                normalized_whole_ms
            ),
            "replacement_end_to_end_client_wall_ms": numeric_summary(
                replacement_whole_ms
            ),
            "replacement_minus_normalized_ms": numeric_summary(
                [
                    replacement - normalized
                    for normalized, replacement in zip(
                        normalized_whole_ms,
                        replacement_whole_ms,
                        strict=True,
                    )
                ]
            ),
            "replacement_over_normalized_ratio": numeric_summary(whole_ratios),
            "timing_breakdown": {
                "normalized": timing_breakdown(normalized_whole_rows),
                "replacement": timing_breakdown(replacement_whole_rows),
            },
            "normalized_logical_facts_per_second": numeric_summary(
                rates([logical_total] * len(run_ids), normalized_whole_ms)
            ),
            "replacement_logical_facts_per_second": numeric_summary(
                rates([logical_total] * len(run_ids), replacement_whole_ms)
            ),
        },
        "updated_table_equal_weight_macro": {
            "arithmetic_mean_of_table_ratios": numeric_summary(
                macro_arithmetic_by_run
            ),
            "geometric_mean_of_table_ratios": numeric_summary(
                macro_geometric_by_run
            ),
            "ratio_definition": "replacement table time / normalized table time",
        },
        "by_table": per_table,
        "by_component_kind": component_breakdown,
    }


def reconcile_counterfactual_fold_footprint(
    counterfactual: Mapping[str, Any],
    actual: Mapping[str, Any],
) -> dict[str, Any]:
    """Link the legacy logical footprint to the timed physical layouts.

    The old footprint remains useful as a logical, complete-topology
    counterfactual, but it can omit residual rows which the lossless timed
    replacement must materialize.  Its unchanged source side must nevertheless
    reconcile exactly with the newly materialized normalized layout.
    """

    def mapping_field(
        parent: Mapping[str, Any],
        field: str,
        context: str,
    ) -> Mapping[str, Any]:
        value = parent.get(field)
        if not isinstance(value, Mapping):
            raise ExperimentValidationError(
                f"counterfactual footprint {context}.{field} is malformed"
            )
        return value

    def count_field(parent: Mapping[str, Any], field: str, context: str) -> int:
        value = parent.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ExperimentValidationError(
                f"counterfactual footprint {context}.{field} must be a "
                "non-negative integer"
            )
        return value

    whole_graph = mapping_field(counterfactual, "whole_graph", "root")
    source = mapping_field(whole_graph, "source", "whole_graph")
    folded = mapping_field(whole_graph, "folded", "whole_graph")
    actual_normalized = mapping_field(actual, "normalized", "actual")
    actual_replacement = mapping_field(actual, "replacement", "actual")

    legacy_source = {
        "nodes": count_field(
            mapping_field(source, "nodes", "whole_graph.source"),
            "total",
            "whole_graph.source.nodes",
        ),
        "logical_business_property_cells": count_field(
            mapping_field(source, "property_cells", "whole_graph.source"),
            "total",
            "whole_graph.source.property_cells",
        ),
        "explicit_relationships": count_field(
            mapping_field(source, "relationships", "whole_graph.source"),
            "total",
            "whole_graph.source.relationships",
        ),
    }
    materialized_normalized = {
        "nodes": count_field(actual_normalized, "nodes", "actual.normalized"),
        "logical_business_property_cells": count_field(
            actual_normalized,
            "logical_business_property_cells",
            "actual.normalized",
        ),
        "explicit_relationships": count_field(
            actual_normalized,
            "explicit_relationships",
            "actual.normalized",
        ),
    }
    if materialized_normalized != legacy_source:
        raise ExperimentValidationError(
            "counterfactual source footprint does not reconcile with the "
            "materialized normalized full-update layout: "
            f"counterfactual={legacy_source!r}, actual={materialized_normalized!r}"
        )

    legacy_folded = {
        "nodes": count_field(
            mapping_field(folded, "nodes", "whole_graph.folded"),
            "total",
            "whole_graph.folded.nodes",
        ),
        "logical_business_property_cells": count_field(
            mapping_field(folded, "property_cells", "whole_graph.folded"),
            "total",
            "whole_graph.folded.property_cells",
        ),
        "explicit_relationships": count_field(
            mapping_field(folded, "relationships", "whole_graph.folded"),
            "total",
            "whole_graph.folded.relationships",
        ),
    }
    materialized_replacement = {
        "nodes": count_field(actual_replacement, "nodes", "actual.replacement"),
        "logical_business_property_cells": count_field(
            actual_replacement,
            "logical_business_property_cells",
            "actual.replacement",
        ),
        "explicit_relationships": count_field(
            actual_replacement,
            "explicit_relationships",
            "actual.replacement",
        ),
    }
    return {
        "status": "source_baseline_exact_replacement_diagnostic_only",
        "counterfactual_scope": counterfactual.get("scope"),
        "normalized_source_baseline": {
            "counterfactual": legacy_source,
            "actually_materialized": materialized_normalized,
            "exact_match": True,
        },
        "replacement": {
            "legacy_logical_counterfactual": legacy_folded,
            "actually_materialized_lossless_layout": materialized_replacement,
            "actual_minus_counterfactual": {
                field: materialized_replacement[field] - legacy_folded[field]
                for field in legacy_folded
            },
            "equality_required": False,
            "reason": (
                "the timed layout is authoritative and lossless; unlike the "
                "legacy counterfactual it materializes unmatched residual rows "
                "and their topology, and its role-occurrence representation may "
                "therefore have a different physical footprint"
            ),
        },
    }


def run_full_8table_replacement_update(
    session: Any,
    *,
    case: PairSpec | RecursiveCaseSpec,
    run_id: str,
    configuration_redundancy: Mapping[str, Any] | None,
    fold_replacement_footprint: Mapping[str, Any],
    warmup_runs: int,
    measured_runs: int,
    batch_size: int,
    index_wait_seconds: int,
) -> FullReplacementBenchmarkResult:
    """Run paired seven-table updates on complete lossless graph layouts."""

    case_id = case.case_id
    workloads: dict[str, UpdateWorkload] = {}
    indexes: list[IndexLease] = []
    implicit_primary_roles_materialized = False
    clone_cleanup = CleanupResult(0, 0)
    construction: list[dict[str, Any]] = []
    report: dict[str, Any] | None = None
    try:
        source_topology = validate_full_tpch_source_topology(session)
        print(
            f"\n[{case_id}] Complete-layout replacement update: "
            "building one shared logical workload",
            flush=True,
        )
        workloads, workload_report = build_full_update_workloads(
            session,
            batch_size,
        )
        print(
            "  Logical facts: "
            f"{workload_report['logical_fact_count']:,} across seven updated "
            "tables; LINEITEM unchanged",
            flush=True,
        )
        implicit_primary_roles_materialized = (
            isinstance(case, RecursiveCaseSpec)
            and configuration_redundancy is not None
            and bool(
                configuration_redundancy.get(
                    "implicit_physical_edge_completions",
                    (),
                )
            )
        )
        targets, construction = materialize_full_update_layouts(
            session,
            case=case,
            run_id=run_id,
            configuration_redundancy=configuration_redundancy,
            batch_size=batch_size,
            index_wait_seconds=index_wait_seconds,
            indexes=indexes,
        )
        target_counts, validation_report = validate_full_update_target_plan(
            session,
            case_id=case_id,
            run_id=run_id,
            targets=targets,
            workloads=workloads,
        )
        occurrences = build_full_topology_occurrences(case, targets)
        reconstruction_validation = validate_full_layout_reconstruction(
            session,
            case_id=case_id,
            run_id=run_id,
            occurrences=occurrences,
            target_counts=target_counts,
        )
        topology_routes = build_full_topology_routes(case, occurrences)
        topology_anchors = materialize_full_topology_anchors(
            session,
            case_id=case_id,
            run_id=run_id,
            occurrences=occurrences,
            batch_size=batch_size,
            index_wait_seconds=index_wait_seconds,
            indexes=indexes,
        )
        topology_materialization = materialize_full_layout_topology(
            session,
            case_id=case_id,
            run_id=run_id,
            occurrences=occurrences,
            routes=topology_routes,
            batch_size=batch_size,
        )
        topology_validation = validate_full_layout_topology(
            session,
            case_id=case_id,
            run_id=run_id,
            source_topology=source_topology,
            occurrences=occurrences,
            routes=topology_routes,
            materialization=topology_materialization,
        )
        topology_signature_before = topology_validation[
            "explicit_relationship_signature"
        ]
        routed_key_counts = route_full_update_workloads(
            session,
            case_id=case_id,
            run_id=run_id,
            targets=targets,
            workloads=workloads,
            batch_size=batch_size,
        )
        validation_report["routed_distinct_logical_keys_by_target"] = (
            routed_key_counts
        )
        validation_report["negative_lookup_policy"] = (
            "target-specific routing: only keys represented by a target family "
            "are submitted to that family"
        )
        expected_by_layout = {
            "normalized": int(validation_report["normalized_property_targets"]),
            "replacement": int(validation_report["replacement_property_targets"]),
        }
        print(
            "  Validated targets: normalized="
            f"{expected_by_layout['normalized']:,}, replacement="
            f"{expected_by_layout['replacement']:,}; missing facts=0",
            flush=True,
        )
        normalized_samples, replacement_samples, schedule_report = (
            benchmark_full_replacement_pair(
                session,
                case_id=case_id,
                run_id=run_id,
                targets=targets,
                workloads=workloads,
                target_counts=target_counts,
                expected_by_layout=expected_by_layout,
                warmup_runs=warmup_runs,
                measured_runs=measured_runs,
                batch_size=batch_size,
            )
        )
        print_timing_summary("full normalized-layout update", normalized_samples)
        print_timing_summary("full replacement-layout update", replacement_samples)
        topology_signature_after = full_topology_signature(
            session,
            case_id=case_id,
            run_id=run_id,
        )
        if topology_signature_after != topology_signature_before:
            raise ExperimentValidationError(
                f"{case_id}: scalar benchmark changed full-layout topology"
            )
        topology_validation["stable_through_scalar_benchmark"] = True
        derived = full_replacement_update_derived(
            normalized_samples,
            replacement_samples,
            validation_report,
        )
        timing_analysis = full_update_timing_analysis(
            schedule_report["measured_run_details"],
            validation_report,
        )
        paired_whole_database_ratio = timing_analysis[
            "whole_database_row_weighted_micro"
        ]["replacement_over_normalized_ratio"]
        derived["replacement_over_normalized_median_ratio"] = float(
            paired_whole_database_ratio["median"]
        )
        derived["replacement_over_normalized_median_ratio_estimator"] = (
            "median of within-run replacement/normalized whole-arm ratios"
        )
        derived["ratio_formula"][
            "replacement_over_normalized_median_ratio"
        ] = "median_i(t_R,i / t_N,i)"
        derived["paired_whole_database_ratio"] = paired_whole_database_ratio
        derived["updated_table_equal_weight_macro_ratio"] = timing_analysis[
            "updated_table_equal_weight_macro"
        ]
        target_manifest = [
            asdict(target)
            | {
                "target_id": target.target_id,
                "expected_property_targets": target_counts[target.target_id],
            }
            for target in targets
            if target.table_label in workloads
        ]
        residual_rows_by_table = {
            str(item["table"]): int(item["nodes_created"])
            for item in construction
            if item.get("component_kind") == "residual"
            and int(item.get("nodes_created", 0)) > 0
        }
        normalized_storage = reconstruction_validation["layouts"]["normalized"][
            "physical_storage"
        ]
        replacement_storage = reconstruction_validation["layouts"]["replacement"][
            "physical_storage"
        ]
        normalized_relationships = sum(
            topology_signature_before["normalized"].values()
        )
        replacement_relationships = sum(
            topology_signature_before["replacement"].values()
        )
        actual_lossless_layout_footprint = {
            "scope": "complete_eight_table_lossless_graph_replacement",
            "measurement_basis": "actually_materialized_run_scoped_layouts",
            "normalized": {
                **normalized_storage,
                "explicit_relationships": normalized_relationships,
                "implicit_role_edge_occurrences": sum(
                    int(route["implicit_physical_occurrences"])
                    for route in topology_materialization["routes"]
                    if route["layout"] == "normalized"
                ),
            },
            "replacement": {
                **replacement_storage,
                "explicit_relationships": replacement_relationships,
                "implicit_role_edge_occurrences": sum(
                    int(route["implicit_physical_occurrences"])
                    for route in topology_materialization["routes"]
                    if route["layout"] == "replacement"
                ),
                "residual_rows_by_physical_table": residual_rows_by_table,
            },
            "change_replacement_minus_normalized": {
                "nodes": (
                    int(replacement_storage["nodes"])
                    - int(normalized_storage["nodes"])
                ),
                "logical_business_property_cells": (
                    int(replacement_storage["logical_business_property_cells"])
                    - int(normalized_storage["logical_business_property_cells"])
                ),
                "explicit_relationships": (
                    replacement_relationships - normalized_relationships
                ),
            },
            "property_cell_ratio": (
                int(replacement_storage["logical_business_property_cells"])
                / int(normalized_storage["logical_business_property_cells"])
            ),
        }
        actual_lossless_layout_footprint["counterfactual_footprint_audit"] = (
            reconcile_counterfactual_fold_footprint(
                fold_replacement_footprint,
                actual_lossless_layout_footprint,
            )
        )
        conversion_sample, conversion_report = (
            measure_full_replacement_to_normalized_conversion(
                session,
                case_id=case_id,
                run_id=run_id,
                targets=targets,
                occurrences=occurrences,
                expected_rows_by_table={
                    table_label: int(table_report["logical_rows"])
                    for table_label, table_report in (
                        reconstruction_validation["layouts"]["normalized"][
                            "tables"
                        ].items()
                    )
                },
                source_topology=source_topology,
                expected_topology_signature=topology_signature_after,
                expected_normalized_nodes=int(normalized_storage["nodes"]),
                expected_replacement_nodes=int(replacement_storage["nodes"]),
                batch_size=batch_size,
            )
        )
        print_timing_summary(
            "full replacement-to-normalized conversion",
            [conversion_sample],
        )
        derived["layout_conversion_client_wall_ms"] = (
            conversion_sample.client_wall_ms
        )
        derived["layout_conversion_break_even"] = None
        derived["layout_conversion_break_even_reason"] = (
            "the full destructive conversion is measured once, but "
            "break-even calculation is intentionally outside this experiment"
        )
        report = {
            "schema_version": FULL_REPLACEMENT_UPDATE_SCHEMA_VERSION,
            "semantics_id": FULL_REPLACEMENT_SEMANTICS_ID,
            "status": "complete",
            "research_question": (
                "steady-state seven-table scalar-update cost of a complete "
                "normalized eight-table graph layout versus a lossless "
                "fold-replacement graph layout, excluding LINEITEM updates, "
                "plus one full destructive replacement-to-normalized "
                "conversion time"
            ),
            "workload": workload_report,
            "layout_semantics": {
                "normalized": (
                    "one run-scoped property clone per source row in each of "
                    "the eight physical TPC-H relations"
                ),
                "replacement": (
                    "final folded primary plus unaffected-table clones, "
                    "unmatched selected-table residuals, and required shared "
                    "Nation/Region companions"
                ),
                "source_graph_updated": False,
                "coexistence_semantics": False,
                "topology_materialized_for_this_update_subsystem": True,
                "topology_isolated_to_run_scoped_layouts": True,
                "topology_updates_excluded": True,
                "scalar_update_excluded_tables": list(
                    FULL_UPDATE_EXCLUDED_TABLES
                ),
                "topology_scope_note": (
                    "the complete starting topology is materialized and held "
                    "constant; PK/FK changes and rewiring remain a separate "
                    "topology-update workload"
                ),
            },
            "construction_outside_update_timer": construction,
            "replacement_to_normalized_conversion": conversion_report,
            "actual_lossless_graph_layout_footprint": (
                actual_lossless_layout_footprint
            ),
            "source_topology_preflight": source_topology,
            "topology_materialization": topology_materialization,
            "topology_anchors": topology_anchors,
            "topology_validation": topology_validation,
            "logical_reconstruction_validation": reconstruction_validation,
            "dynamic_lookup_indexes": {
                "names": [
                    index.name for index in indexes if index.name is not None
                ],
                "count": len(indexes),
                "created_before_update_timing": True,
                "dropped_after_update_timing": True,
            },
            "target_manifest": target_manifest,
            "validation": validation_report,
            "timing_scope": (
                "whole-arm client wall time including SQLite route iteration, "
                "Neo4j result consumption, and explicit transaction commits; "
                "workload spooling, layout/index/topology build, validation, "
                "restoration, and cleanup are excluded"
            ),
            "neo4j_reported_total_ms_scope": (
                "sum of Neo4j query-result summary available_after_ms plus "
                "consumed_after_ms; it excludes explicit transaction begin/"
                "commit latency, client-side routing/orchestration, restore, "
                "and validation, so client wall time is the primary metric"
            ),
            "transaction_semantics": (
                "each table's deterministic logical-key batch is one explicit "
                "transaction containing all routed physical copies for that "
                "layout; the seven-table scalar workload is a deterministic "
                "sequence of these atomic logical batches, and LINEITEM is "
                "not submitted to either update arm"
            ),
            "order_policy": (
                "normalized/replacement order alternates AB/BA by trial; table "
                "order is the fixed physical TPC-H catalog order"
            ),
            **schedule_report,
            "timing_analysis": timing_analysis,
            "timings": {
                "normalized": {
                    "samples": samples_as_dicts(normalized_samples),
                    "summary": timing_summary(normalized_samples),
                },
                "replacement": {
                    "samples": samples_as_dicts(replacement_samples),
                    "summary": timing_summary(replacement_samples),
                },
                "replacement_to_normalized_conversion": {
                    "samples": [asdict(conversion_sample)],
                    "summary": timing_summary([conversion_sample]),
                },
            },
            "derived": derived,
        }
        return FullReplacementBenchmarkResult(
            normalized_samples=normalized_samples,
            replacement_samples=replacement_samples,
            conversion_sample=conversion_sample,
            report=report,
        )
    finally:
        active_error = sys.exc_info()[1]
        cleanup_errors: list[BaseException] = []
        try:
            clone_cleanup = cleanup_full_update_clones(
                session,
                case_id=case_id,
                run_id=run_id,
                batch_size=batch_size,
            )
        except BaseException as exc:
            cleanup_errors.append(exc)
        try:
            drop_case_schema(session, (), indexes)
        except BaseException as exc:
            cleanup_errors.append(exc)
        for workload in workloads.values():
            try:
                workload.close()
            except BaseException as exc:
                cleanup_errors.append(exc)
        if cleanup_errors:
            cleanup_failure = RuntimeError(
                f"{case_id}: full replacement subsystem cleanup failed: "
                + "; ".join(
                    f"{type(error).__name__}: {error}" for error in cleanup_errors
                )
            )
            if active_error is not None:
                raise cleanup_failure from active_error
            raise cleanup_failure
        if report is not None:
            report["cleanup"] = {
                **asdict(clone_cleanup),
                "implicit_primary_roles_removed_with_replacement_layout": (
                    implicit_primary_roles_materialized
                ),
                "dynamic_lookup_indexes_dropped": True,
                "workload_spools_closed": True,
                "verified_before_return": True,
            }
        if construction:
            print(
                "  Remaining full-update layout artifacts removed: nodes="
                f"{clone_cleanup.deleted_nodes:,}",
                flush=True,
            )


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
        "affected_nodes": sorted({sample.affected_nodes for sample in samples}),
        "properties_set": sorted({sample.properties_set for sample in samples}),
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
        else (f"median={server['median']:.3f} ms, mean={server['mean']:.3f} ms")
    )
    if client["n"] == 1:
        print(
            f"  {title}: client={client['median']:.3f} ms | "
            "neo4j=" + ("N/A" if server is None else f"{server['median']:.3f} ms")
        )
        return
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
    normalized_median = median(sample.client_wall_ms for sample in normalized_updates)
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
            folded_median / normalized_median if normalized_median > 0 else None
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
        raise FileNotFoundError(f"TPC-H data directory does not exist: {data_dir}")

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
    if key and all(isinstance(value, int) and 0 <= value < (1 << 64) for value in key):
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
            normalized_count = sum(degree_by_key[key] for key in referenced_occurrences)
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
            value_types.add(f"{type(value).__module__}.{type(value).__qualname__}")
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
                "referencing_is_relationship_source": (pair.referencing_is_source),
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
                        asdict(binding) for binding in pair.referenced_property_bindings
                    ],
                },
                "metrics": asdict(actual),
                "default_update_property": pair.default_update_property,
                "active_domain_size": active_domain_size,
                "update_value_type": value_type,
                "boundary_expectations": [asdict(boundary) for boundary in boundaries],
            }
        )

    composite_pair = CASES["lineitem_partsupp"]
    referenced_aliases = {
        binding.folded_name for binding in composite_pair.referenced_property_bindings
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
        "configuration_redundancy_schema_version": (
            CONFIGURATION_REDUNDANCY_SCHEMA_VERSION
        ),
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
        "Full paired-update property catalog: "
        + ", ".join(
            f"{table}.{FULL_UPDATE_PROPERTIES[table]}"
            for table in TABLES
            if table in FULL_UPDATE_PROPERTIES
        )
    )
    print(
        "LINEITEM.l_shipmode is excluded from both update arms.\n"
        "The per-case property shown below is only the final-layer FD payload "
        "diagnostic; it does not change the seven-table paired workload.\n"
    )
    print("Direct pair cases (default suite):")
    print(
        f"{'case_id':<31} "
        f"{'referencing table':<20} "
        f"{'referenced table':<20} "
        f"{'relationship':<18} "
        "FD diagnostic"
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
    print("\nRecursive FK-to-PK cases (--case or --all-extended-cases):")
    print("  Role tree: L->O->C->N1->R1; " "L->PS->S->N2->R2; PS->P")
    print("  Physical aliases: N1/N2=NATION; R1/R2=REGION")
    recursive_counts = Counter(len(case.roles) for case in EXTENDED_CASES.values())
    print(
        "  Counts: "
        + ", ".join(
            f"{role_count} roles={recursive_counts[role_count]}"
            for role_count in range(
                RECURSIVE_MIN_ROLE_COUNT,
                RECURSIVE_MAX_ROLE_COUNT + 1,
            )
        )
        + f"; total={len(EXTENDED_CASES)}"
    )
    for role_count in range(
        RECURSIVE_MIN_ROLE_COUNT,
        RECURSIVE_MAX_ROLE_COUNT + 1,
    ):
        print(f"\n  {role_count} roles ({recursive_counts[role_count]} cases)")
        for case_id in EXTENDED_CASE_ORDER:
            case = EXTENDED_CASES[case_id]
            if len(case.roles) != role_count:
                continue
            order = " -> ".join(role.symbol for role in case.roles)
            print(
                f"    {case_id}\n"
                f"      {order} | final={case.final_role.display_label}."
                f"{case.default_update_property}"
            )
    print(
        "\nRecursive v10 policy: rooted outgoing FK->PK role folds; every later "
        "stage consumes the preceding MV's physical inherited edge."
    )
    print(
        "A stage carries all currently exposed boundaries used by later "
        "declared stages, including branches owned by earlier roles."
    )
    print(
        "With --topology-mode complete, the final MV also preserves every "
        "still-exposed role-tree boundary; property-fd leaves it isolated."
    )
    print(
        "Timed shadow roles remain branch-scoped. Supplemental configuration "
        "redundancy reconciles N1/N2 and R1/R2 by physical key and includes "
        "the retained Nation or opposite-parent NR closure."
    )
    print(
        f"Legacy pre-v4 --case names remain accepted as "
        f"{len(LEGACY_RECURSIVE_CASE_ALIASES)} CLI aliases."
    )


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
            "direction": (f"{pair.join.source_label}->{pair.join.target_label}"),
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
                    asdict(binding) for binding in pair.referencing_property_bindings
                ],
                "referenced_non_key": [
                    asdict(binding) for binding in pair.referenced_property_bindings
                ],
            },
            "referenced_key_copied": False,
            "referenced_key_representation": [
                {
                    "referencing_fk": referencing_fk,
                    "folded_referencing_fk": (
                        pair.referencing_binding_by_logical_name[referencing_fk]
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


def recursive_case_descriptor(case: RecursiveCaseSpec) -> dict[str, Any]:
    return {
        "recursive_fold_schema_version": RECURSIVE_FOLD_SCHEMA_VERSION,
        "case_id": case.case_id,
        "policy": {
            "fold_direction": "outgoing_foreign_key_to_primary_key_only",
            "role_addition_model": (
                "rooted sequence; a stage owner may be an earlier role"
            ),
            "row_identity": ("grain primary key remains unchanged at every stage"),
            "inheritance_scope": (
                "all currently exposed physical boundaries required by any "
                "later declared stage are copied"
            ),
            "terminal_boundary_policy": ("materialize-final-effective-boundary-cut"),
            "terminal_boundary_mode": (
                "property-fd skips the final cut; complete preserves it"
            ),
            "legacy_final_layer_unfolding_boundary_policy": (
                "if the legacy helper is invoked separately, prefix-owned "
                "copies remain on the prefix; final-role-owned copies move to "
                "the deduplicated factor and return on refold"
            ),
            "current_update_protocol_executes_legacy_unfolding": False,
            "current_update_protocol_executes_full_conversion": True,
            "next_stage_source": (
                "preceding MV inherited relationship; source schema "
                "adjacency fallback is forbidden"
            ),
            "incoming_fanout_supported": False,
            "repeated_physical_labels_supported": (
                "explicit N1/N2 and R1/R2 aliases only"
            ),
            "nation_region_role_scoping": {
                "customer_branch": "C->N1->R1",
                "supplier_branch": "S->N2->R2",
                "forbidden_cross_branch_edges": ["C->N2", "S->N1"],
            },
        },
        "grain_role": case.grain.symbol,
        "grain_table": case.grain.table.label,
        "grain_key": list(case.grain.table.key_fields),
        "role_order": [role.symbol for role in case.roles],
        "role_addition_order": [role.symbol for role in case.roles],
        "physical_table_order": [role.table.label for role in case.roles],
        "roles": [
            {
                "role_index": role.role_index,
                "role_symbol": role.symbol,
                "display_name": role.display_name,
                "table": role.table.label,
                "columns": list(role.table.columns),
                "key": list(role.table.key_fields),
                "logical_to_folded_property": {
                    binding.logical_name: binding.folded_name
                    for binding in role.property_bindings
                },
            }
            for role in case.roles
        ],
        "stages": [
            {
                "stage_number": stage.stage_number,
                "input_label": stage.input_label,
                "output_label": stage.output_label,
                "owner_role_index": stage.owner_role_index,
                "added_role_index": stage.added_role_index,
                "owner_role": case.roles[stage.owner_role_index].symbol,
                "added_role": case.roles[stage.added_role_index].symbol,
                "join": asdict(stage.join),
                "selected_copy_type": stage.selected_copy_type,
                "next_copy_type": stage.next_copy_type,
                "structurally_eligible_boundaries_after": [
                    {
                        "boundary_id": boundary.boundary_id,
                        "relationship_type": (boundary.relationship.relationship_type),
                        "direction": boundary.direction,
                        "owner_role": boundary.owner_role_symbol,
                        "external_role": boundary.external_role_symbol,
                        "external_label": boundary.external_label,
                        "copy_type": boundary.copy_type,
                    }
                    for boundary in stage.effective_boundaries_after
                ],
                "actually_inherited_for_next_stage": (
                    [stage.next_copy_type] if stage.next_copy_type is not None else []
                ),
                "carried_for_future_stages": [
                    boundary.copy_type for boundary in stage.inherited_boundaries
                ],
            }
            for stage in case.stages
        ],
        "normalization": {
            "descriptor_status": (
                "legacy_helper_only_not_executed_by_full_replacement_protocol"
            ),
            "executed_by_current_update_protocol": False,
            "scope": "optional legacy helper peels only the final fold layer",
            "folded_label": case.names.final_folded_label,
            "prefix_label": case.names.normalized_prefix_label,
            "factor_label": case.names.normalized_factor_label,
            "join_type": case.names.normalized_join_type,
            "final_role": case.final_role.symbol,
            "final_table": case.final_role.table.label,
            "final_key_physical_sources": {
                prop: case.final_role.folded_property(prop)
                for prop in case.final_role.table.key_fields
            },
        },
        "default_update_property": case.default_update_property,
    }


def print_final_comparison(report: dict[str, Any]) -> None:
    derived = report["derived"]
    break_even = derived["data_rewrite_normalization_break_even_update_batches"]
    ratio = derived["folded_to_normalized_update_median_ratio"]
    amplification = derived["write_amplification_nodes"]
    conversion_ms = derived.get("layout_conversion_client_wall_ms")
    print(f"\nFinal comparison: {report['case_id']}")
    print(
        "  Physical property-cell write amplification: "
        + ("N/A" if amplification is None else f"{amplification:.3f}x")
    )
    print(
        "  Replacement/normalized median client-time ratio: "
        + ("N/A" if ratio is None else f"{ratio:.3f}x")
    )
    print(
        "  Full replacement-to-normalized conversion: "
        + ("N/A" if conversion_ms is None else f"{conversion_ms:.3f} ms")
    )
    print(
        "  Complete layout-conversion break-even: "
        + ("N/A" if break_even is None else f"{break_even:.3f} logical update batches")
    )
    print(
        "  Final database state: original graph unchanged; "
        "run-scoped MV nodes, edges, indexes, and constraints removed"
    )


def print_recursive_final_comparison(report: dict[str, Any]) -> None:
    derived = report["derived"]
    ratio = derived["folded_to_normalized_update_median_ratio"]
    break_even = derived["data_rewrite_normalization_break_even_update_batches"]
    conversion_ms = derived.get("layout_conversion_client_wall_ms")
    ratio_text = "N/A" if ratio is None else f"{ratio:.3f}x"
    break_even_text = "N/A" if break_even is None else f"{break_even:.3f} batches"
    conversion_text = (
        "N/A" if conversion_ms is None else f"{conversion_ms:.3f} ms"
    )
    print(
        f"\nSummary [{report['case_id']}]: "
        f"replacement/normalized={ratio_text} | "
        f"full conversion={conversion_text} | "
        f"break-even={break_even_text} | source unchanged | artifacts removed"
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
    *,
    whole_source_graph_footprint: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    from neo4j import WRITE_ACCESS, __version__ as neo4j_driver_version

    started_at = datetime.now(timezone.utc)
    run_id = uuid4().hex
    constraints: list[ConstraintLease] = []
    indexes: list[IndexLease] = []
    cleanup_completed = False
    workload: UpdateWorkload | None = None
    full_update_result: FullReplacementBenchmarkResult | None = None
    fold_replacement_footprint: dict[str, Any]

    try:
        with driver.session(
            database=database,
            default_access_mode=WRITE_ACCESS,
        ) as session:
            ensure_no_existing_artifacts(session, [pair])
            # The benchmark may omit physical boundary copies, but the logical
            # whole-graph replacement metric always uses the complete TPC-H
            # topology and therefore validates that topology in both modes.
            validate_complete_source_topology(session, pair)
            baseline = build_case_baseline(
                session,
                pair,
                args.topology_mode,
                source_sha256_before,
            )
            logical_complete_boundary_expectations = (
                baseline.boundary_expectations
                if args.topology_mode == "complete"
                else boundary_expected_counts(session, pair, "complete")
            )
            complete_source_graph_footprint = (
                whole_source_graph_footprint
                if whole_source_graph_footprint is not None
                else measure_tpch_source_graph_footprint(session)
            )
            source_fold_footprint = direct_fold_source_footprint_from_whole_graph(
                pair,
                complete_source_graph_footprint,
            )
            environment = collect_environment_metadata(
                session,
                driver,
                neo4j_driver_version,
            )
            update_property = args.update_property or pair.default_update_property
            redundancy, workload, payload_metrics = build_case_measurements(
                session,
                pair,
                baseline,
                update_property,
                args.batch_size,
            )
            configuration_redundancy = build_direct_configuration_redundancy(
                session,
                pair,
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
            fold_replacement_footprint = build_direct_fold_replacement_footprint(
                pair,
                complete_source_graph_footprint,
                source_fold_footprint,
                step0,
                configuration_redundancy,
                logical_complete_boundary_expectations,
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
                f"\n[{pair.case_id}] Step 1: measuring referenced-table FD redundancy"
            )
            print(
                "  Join fingerprint "
                f"(schema v{JOIN_FINGERPRINT_SCHEMA_VERSION}): "
                f"{baseline.join_sha256}"
            )
            print_redundancy(pair, redundancy)
            print_fold_replacement_footprint(fold_replacement_footprint)
            print_configuration_redundancy(configuration_redundancy)

            full_update_result = run_full_8table_replacement_update(
                session,
                case=pair,
                run_id=run_id,
                configuration_redundancy=configuration_redundancy,
                fold_replacement_footprint=fold_replacement_footprint,
                warmup_runs=args.warmup_runs,
                measured_runs=args.runs,
                batch_size=args.batch_size,
                index_wait_seconds=args.index_wait_seconds,
            )
            fold_replacement_footprint["relationship_to_update_layout"] = (
                "legacy complete-topology counterfactual may omit unmatched "
                "selected-table residual rows; the authoritative node/property "
                "footprint for update timing is full_8table_replacement_update."
                "actual_lossless_graph_layout_footprint"
            )
            fold_replacement_footprint["actual_materialization"][
                "full_update_supplements_materialized_separately"
            ] = True
            # The paired subsystem restores every primary property and removes
            # all supplemental layouts before returning.
            validate_folded_shadow_state(
                session,
                pair,
                run_id,
                baseline,
                args.topology_mode,
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
                "  Original graph fingerprint after cleanup: " f"{source_sha256_after}",
                flush=True,
            )
            if source_sha256_after != source_sha256_before:
                raise ExperimentValidationError(
                    f"{pair.case_id}: original graph fingerprint changed "
                    "after cleanup"
                )
            print(
                "  Original graph unchanged: cleanup fingerprint matches "
                "the suite baseline ✅",
                flush=True,
            )

        completed_at = datetime.now(timezone.utc)
        if full_update_result is None:
            raise ExperimentStateError(
                f"{pair.case_id}: full replacement update did not run"
            )
        full_derived = full_update_result.report["derived"]
        derived = {
            "comparison_scope": "full_8table_lossless_graph_replacement",
            "folded_to_normalized_update_median_ratio": full_derived[
                "replacement_over_normalized_median_ratio"
            ],
            "denormalized_to_normalized_update_median_ratio": full_derived[
                "replacement_over_normalized_median_ratio"
            ],
            "write_amplification_nodes": full_derived[
                "physical_write_amplification"
            ],
            "physical_property_write_amplification": full_derived[
                "physical_property_write_amplification"
            ],
            "write_amplification_nodes_legacy_alias_note": (
                "legacy name; value is physical scalar property writes per "
                "logical property fact"
            ),
            "data_rewrite_normalization_break_even_update_batches": None,
            "shadow_data_rewrite_normalization_break_even_update_batches": None,
            "break_even_reason": full_derived[
                "layout_conversion_break_even_reason"
            ],
            **full_derived,
        }
        folded_update_samples = full_update_result.replacement_samples
        normalized_update_samples = full_update_result.normalized_samples
        conversion_sample = full_update_result.conversion_sample
        folded_timing = {
            "samples": samples_as_dicts(folded_update_samples),
            "summary": timing_summary(folded_update_samples),
        }
        normalization_timing = {
            "status": "measured_full_replacement_to_normalized_once",
            "samples": [asdict(conversion_sample)],
            "summary": timing_summary([conversion_sample]),
            "semantics": (
                "destructive full lossless replacement-to-normalized "
                "conversion; this is not the former final-layer unfolding"
            ),
        }
        normalized_timing = {
            "samples": samples_as_dicts(normalized_update_samples),
            "summary": timing_summary(normalized_update_samples),
        }
        report = {
            "report_schema_version": REPORT_SCHEMA_VERSION,
            "configuration_redundancy_schema_version": (
                CONFIGURATION_REDUNDANCY_SCHEMA_VERSION
            ),
            "experiment": "tpch_direct_pair_full_replacement_update",
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
                "local_concurrency_policy": ("exclusive advisory process lock"),
                "mutation_atomicity": (
                    "each configured-size logical-key batch is one explicit "
                    "transaction containing every routed physical copy for "
                    "that table/layout; a later-batch failure is compensated "
                    "by restoration or run-scoped cleanup"
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
                "join_fingerprint_schema_version": (JOIN_FINGERPRINT_SCHEMA_VERSION),
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
                "logical_complete_boundary_expectations": [
                    asdict(expectation)
                    for expectation in logical_complete_boundary_expectations
                ],
            },
            "configuration": {
                "update_semantics": FULL_REPLACEMENT_SEMANTICS_ID,
                "update_properties_by_physical_table": FULL_UPDATE_PROPERTIES,
                "update_excluded_physical_tables": list(
                    FULL_UPDATE_EXCLUDED_TABLES
                ),
                "batch_size": args.batch_size,
                "batch_size_scope": (
                    "logical keys per explicit transaction; all routed "
                    "physical copies of those keys commit together"
                ),
                "warmup_runs": args.warmup_runs,
                "measured_update_runs": args.runs,
                "folded_property_naming_strategy_id": (
                    FOLDED_PROPERTY_NAMING_STRATEGY_ID
                ),
                "update_strategy_id": FULL_REPLACEMENT_UPDATE_STRATEGY_ID,
                "update_null_policy": UPDATE_NULL_POLICY,
                "update_workload_storage": (
                    "seven run-local per-table SQLite spools shared by both "
                    "layout arms and removed when the case finishes or fails"
                ),
                "primary_fold_schema_objects_created_and_dropped": (
                    case_schema_names(pair, run_id)
                ),
                "full_update_schema_objects": (
                    full_update_result.report["dynamic_lookup_indexes"]
                ),
                "update_timing_scope": (
                    "whole-arm wall time including route-spool iteration, "
                    "result consumption, and explicit commits; restore and "
                    "validation excluded"
                ),
                "transformation_validation_policy": (
                    "full logical-key coverage plus successor and restore value "
                    "checks for every physical target family"
                ),
                "cache_policy": (
                    "warm workload; server page/query caches are not cleared"
                ),
            },
            "final_fd_redundancy_payload": payload_metrics,
            "step0_artifacts": step0,
            "folded_shadow_state": asdict(folded_state),
            "normalized_shadow_state": None,
            "redundancy": redundancy,
            "configuration_redundancy": configuration_redundancy,
            "fold_replacement_footprint": fold_replacement_footprint,
            "full_8table_replacement_update": full_update_result.report,
            "timing_alias_note": (
                "legacy folded_update/denormalized_update keys below alias the "
                "complete replacement-layout update, not a primary-only update"
            ),
            "timings": {
                "replacement_update": folded_timing,
                "folded_update": folded_timing,
                "denormalized_update": folded_timing,
                "replacement_to_normalized_conversion": normalization_timing,
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
                    full_cleanup_result = cleanup_full_update_clones(
                        cleanup_session,
                        case_id=pair.case_id,
                        run_id=run_id,
                        batch_size=args.batch_size,
                    )
                    cleanup_result = cleanup_shadow_artifacts(
                        cleanup_session,
                        pair,
                        run_id,
                        args.batch_size,
                    )
                    print(
                        "  Failure cleanup removed "
                        f"{cleanup_result.deleted_nodes + full_cleanup_result.deleted_nodes:,} "
                        "nodes and "
                        f"{cleanup_result.deleted_relationships + full_cleanup_result.deleted_relationships:,} "
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
                except BaseException as cleanup_error:
                    cleanup_errors.append(cleanup_error)
                try:
                    drop_recovery_schema(cleanup_session, run_id)
                except BaseException as cleanup_error:
                    cleanup_errors.append(cleanup_error)
                try:
                    failure_source_sha256 = original_graph_sha256(cleanup_session)
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
                    f"{type(error).__name__}: {error}" for error in cleanup_errors
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


def run_recursive_case_experiment(
    driver: Any,
    database: str,
    args: argparse.Namespace,
    case: RecursiveCaseSpec,
    source_sha256_before: str,
    *,
    source_shape_prevalidated: bool = False,
    whole_source_graph_footprint: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    from neo4j import WRITE_ACCESS, __version__ as neo4j_driver_version

    started_at = datetime.now(timezone.utc)
    run_id = uuid4().hex
    constraints: list[ConstraintLease] = []
    indexes: list[IndexLease] = []
    cleanup_completed = False
    workload: UpdateWorkload | None = None
    full_update_result: FullReplacementBenchmarkResult | None = None
    stage_reports: list[dict[str, Any]] = []
    boundary_expectations: tuple[RecursiveBoundaryExpectation, ...] = ()
    logical_complete_boundary_expectations: tuple[
        RecursiveBoundaryExpectation, ...
    ] = ()
    final_boundary_materialization: dict[str, Any]
    fold_construction_lifecycle: dict[str, Any]
    fold_replacement_footprint: dict[str, Any]
    update_pair = recursive_update_pair(case)

    try:
        with driver.session(
            database=database,
            default_access_mode=WRITE_ACCESS,
        ) as session:
            ensure_no_existing_artifacts_for_labels(
                session,
                case.all_artifact_labels,
            )
            baseline = build_recursive_baseline(
                session,
                case,
                source_sha256_before,
                deep_stage_fingerprints=args.deep_stage_fingerprints,
                source_shape_prevalidated=source_shape_prevalidated,
            )
            logical_complete_boundary_expectations = (
                recursive_final_boundary_expectations(
                    session,
                    case,
                    "complete",
                )
            )
            boundary_expectations = (
                logical_complete_boundary_expectations
                if args.topology_mode == "complete"
                else ()
            )
            pair_baseline = recursive_pair_baseline(
                baseline,
                boundary_expectations,
            )
            environment = collect_environment_metadata(
                session,
                driver,
                neo4j_driver_version,
            )
            update_property = args.update_property or case.default_update_property
            redundancy, workload, payload_metrics = build_case_measurements(
                session,
                update_pair,
                pair_baseline,
                update_property,
                args.batch_size,
                grouped_fanout_query=recursive_grouped_fanout_query(case),
            )
            configuration_redundancy = build_recursive_configuration_redundancy(
                session, case
            )
            complete_source_graph_footprint = (
                whole_source_graph_footprint
                if whole_source_graph_footprint is not None
                else measure_tpch_source_graph_footprint(session)
            )
            source_fold_footprint = recursive_fold_source_footprint_from_whole_graph(
                case,
                complete_source_graph_footprint,
            )
            redundancy["definition"].update(
                {
                    "experiment_scope": "final recursive fold layer",
                    "folded_prefix_roles": [role.symbol for role in case.roles[:-1]],
                    "folded_prefix_physical_tables": [
                        role.table.label for role in case.roles[:-1]
                    ],
                    "added_referenced_role": case.final_role.symbol,
                    "added_referenced_table": case.final_role.table.label,
                }
            )
            redundancy["definition"].pop("referencing_table", None)
            redundancy["definition"]["referencing_side"] = {
                "kind": "folded_recursive_prefix",
                "role_order": [role.symbol for role in case.roles[:-1]],
                "physical_table_order": [role.table.label for role in case.roles[:-1]],
                "grain_role": case.grain.symbol,
                "grain_table": case.grain.table.label,
                "grain_key": list(case.grain.table.key_fields),
            }

            constraints, indexes = create_recursive_schema(
                session,
                case,
                run_id,
                args.index_wait_seconds,
            )

            print(f"\n[{case.case_id}] Recursive MV experiment")
            print(f"  Run ID: {run_id}", flush=True)
            print(
                "  Role addition order: "
                + " -> ".join(role.symbol for role in case.roles),
                flush=True,
            )
            print(
                "  Physical tables: "
                + " -> ".join(role.table.label for role in case.roles),
                flush=True,
            )
            print("\nMV creation (excluded from benchmark timings)")
            stage_reports = create_recursive_shadow_projection(
                session,
                case,
                run_id,
                baseline,
                args.batch_size,
                deep_stage_fingerprints=args.deep_stage_fingerprints,
            )
            final_boundary_materialization = create_recursive_final_boundary_copies(
                session,
                case,
                run_id,
                args.topology_mode,
                args.batch_size,
                boundary_expectations,
            )
            if args.topology_mode == "complete":
                print(
                    "  Final MV external boundaries: "
                    f"{final_boundary_materialization['boundary_relationship_total']:,} "
                    "relationships across "
                    f"{len(boundary_expectations):,} role-scoped types | "
                    "create="
                    f"{final_boundary_materialization['creation_timing']['client_wall_ms']:.3f} ms",
                    flush=True,
                )
            else:
                print(
                    "  Final MV external boundaries: skipped "
                    "(--topology-mode property-fd)",
                    flush=True,
                )
            fold_construction_lifecycle = build_recursive_fold_lifecycle(
                stage_reports,
                final_boundary_materialization,
            )
            fold_replacement_footprint = build_recursive_fold_replacement_footprint(
                case,
                complete_source_graph_footprint,
                source_fold_footprint,
                fold_construction_lifecycle,
                final_boundary_materialization,
                configuration_redundancy,
                logical_complete_boundary_expectations,
            )
            folded_state = validate_recursive_folded_state(
                session,
                case,
                run_id,
                baseline,
                args.topology_mode,
                boundary_expectations,
                deep=False,
            )
            if (
                fold_construction_lifecycle["nodes"]["retained"]
                != folded_state.final_folded_nodes
                or fold_construction_lifecycle["relationships"]["retained"]
                != folded_state.all_case_relationships
            ):
                raise ExperimentValidationError(
                    f"{case.case_id}: fold construction lifecycle does not "
                    "reconcile with the folded shadow state"
                )

            print(
                "\nRedundancy added by final fold " f"({case.final_role.display_label})"
            )
            print_recursive_redundancy(redundancy)
            print_recursive_fold_lifecycle(fold_construction_lifecycle)
            print_fold_replacement_footprint(fold_replacement_footprint)
            print_configuration_redundancy(configuration_redundancy)

            full_update_result = run_full_8table_replacement_update(
                session,
                case=case,
                run_id=run_id,
                configuration_redundancy=configuration_redundancy,
                fold_replacement_footprint=fold_replacement_footprint,
                warmup_runs=args.warmup_runs,
                measured_runs=args.runs,
                batch_size=args.batch_size,
                index_wait_seconds=args.index_wait_seconds,
            )
            fold_replacement_footprint["relationship_to_update_layout"] = (
                "legacy complete-topology counterfactual may omit unmatched "
                "selected-table residual rows; the authoritative node/property "
                "footprint for update timing is full_8table_replacement_update."
                "actual_lossless_graph_layout_footprint"
            )
            fold_replacement_footprint["actual_materialization"][
                "full_update_supplements_materialized_separately"
            ] = True
            validate_recursive_folded_state(
                session,
                case,
                run_id,
                baseline,
                args.topology_mode,
                boundary_expectations,
                deep=True,
            )

            print("\nCleanup (not timed)")
            cleanup_result = cleanup_recursive_artifacts(
                session,
                case,
                run_id,
                args.batch_size,
            )
            drop_case_schema(session, constraints, indexes)
            constraints = []
            indexes = []
            cleanup_completed = True
            print(
                f"  Removed nodes={cleanup_result.deleted_nodes:,} | "
                "relationships="
                f"{cleanup_result.deleted_relationships:,}"
            )
            print(
                "  Verifying original graph fingerprint...",
                flush=True,
            )
            source_sha256_after = original_graph_sha256(session)
            if source_sha256_after != source_sha256_before:
                raise ExperimentValidationError(
                    f"{case.case_id}: original graph fingerprint changed "
                    "after recursive cleanup"
                )
            print(
                f"  Source fingerprint after cleanup: {source_sha256_after} "
                "\u2705 unchanged",
                flush=True,
            )

        completed_at = datetime.now(timezone.utc)
        if full_update_result is None:
            raise ExperimentStateError(
                f"{case.case_id}: full replacement update did not run"
            )
        full_derived = full_update_result.report["derived"]
        derived = {
            "comparison_scope": "full_8table_lossless_graph_replacement",
            "folded_to_normalized_update_median_ratio": full_derived[
                "replacement_over_normalized_median_ratio"
            ],
            "denormalized_to_normalized_update_median_ratio": full_derived[
                "replacement_over_normalized_median_ratio"
            ],
            "write_amplification_nodes": full_derived[
                "physical_write_amplification"
            ],
            "physical_property_write_amplification": full_derived[
                "physical_property_write_amplification"
            ],
            "write_amplification_nodes_legacy_alias_note": (
                "legacy name; value is physical scalar property writes per "
                "logical property fact"
            ),
            "data_rewrite_normalization_break_even_update_batches": None,
            "shadow_data_rewrite_normalization_break_even_update_batches": None,
            "break_even_reason": full_derived[
                "layout_conversion_break_even_reason"
            ],
            **full_derived,
        }
        folded_update_samples = full_update_result.replacement_samples
        normalized_update_samples = full_update_result.normalized_samples
        conversion_sample = full_update_result.conversion_sample
        folded_timing = {
            "samples": samples_as_dicts(folded_update_samples),
            "summary": timing_summary(folded_update_samples),
        }
        normalization_timing = {
            "status": "measured_full_replacement_to_normalized_once",
            "samples": [asdict(conversion_sample)],
            "summary": timing_summary([conversion_sample]),
            "semantics": (
                "destructive full lossless replacement-to-normalized "
                "conversion; this is not the former final-layer unfolding"
            ),
        }
        normalized_timing = {
            "samples": samples_as_dicts(normalized_update_samples),
            "summary": timing_summary(normalized_update_samples),
        }
        return {
            "report_schema_version": RECURSIVE_REPORT_SCHEMA_VERSION,
            "configuration_redundancy_schema_version": (
                CONFIGURATION_REDUNDANCY_SCHEMA_VERSION
            ),
            "experiment": "tpch_recursive_fold_full_replacement_update",
            "experiment_family": "tpch_recursive_fold_fd",
            "recursive_fold_schema_version": RECURSIVE_FOLD_SCHEMA_VERSION,
            "case_id": case.case_id,
            "run_id": run_id,
            "started_at_utc": started_at.isoformat(),
            "completed_at_utc": completed_at.isoformat(),
            "database": database,
            "environment": environment,
            "fold_chain": recursive_case_descriptor(case),
            "execution": {
                "mode": "run_scoped_batched_recursive_shadow_projection",
                "topology_mode": args.topology_mode,
                "recursive_stage_boundary_policy": ("template-scoped-future-only"),
                "source_policy": (
                    "the original graph is read-only; all recursive nodes "
                    "and inherited edge copies are run-scoped and removed"
                ),
                "continuation_policy": (
                    "stage 1 reads the source join; every later stage must "
                    "consume the preceding MV's actual inherited boundary"
                ),
                "source_schema_fallback": False,
                "local_concurrency_policy": "exclusive advisory process lock",
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
                "source_graph_sha256_after": source_sha256_after,
                "join_fingerprint_schema_version": (
                    RECURSIVE_JOIN_FINGERPRINT_SCHEMA_VERSION
                ),
                "join_fingerprint_algorithm": (RECURSIVE_JOIN_FINGERPRINT_ALGORITHM),
                "join_sha256": baseline.join_sha256,
                "prefix_join_sha256": list(baseline.prefix_join_sha256),
                "grain_rows": baseline.grain_rows,
                "full_final_relation_rows": baseline.final_table_rows,
                "join_rows": baseline.chain_rows,
                "participating_final_rows": (baseline.participating_final_rows),
                "final_boundary_expectations": [
                    recursive_boundary_descriptor(expectation)
                    for expectation in boundary_expectations
                ],
            },
            "configuration": {
                "update_semantics": FULL_REPLACEMENT_SEMANTICS_ID,
                "update_properties_by_physical_table": FULL_UPDATE_PROPERTIES,
                "update_excluded_physical_tables": list(
                    FULL_UPDATE_EXCLUDED_TABLES
                ),
                "batch_size": args.batch_size,
                "warmup_runs": args.warmup_runs,
                "measured_update_runs": args.runs,
                "folded_property_naming_strategy_id": (
                    RECURSIVE_FOLDED_PROPERTY_NAMING_STRATEGY_ID
                ),
                "update_strategy_id": FULL_REPLACEMENT_UPDATE_STRATEGY_ID,
                "update_null_policy": UPDATE_NULL_POLICY,
                "primary_fold_schema_objects_created_and_dropped": (
                    recursive_schema_names(case, run_id)
                ),
                "full_update_schema_objects": (
                    full_update_result.report["dynamic_lookup_indexes"]
                ),
                "terminal_boundary_topology_mode": args.topology_mode,
                "full_update_layout_topology_materialized": True,
                "topology_updates_excluded": True,
                "deep_intermediate_stage_fingerprints": (args.deep_stage_fingerprints),
                "stage_validation_policy": (
                    "all intermediate and final stages use full value " "fingerprints"
                    if args.deep_stage_fingerprints
                    else "source shape and per-stage cardinality are checked; "
                    "the final folded stage uses a full value fingerprint"
                ),
            },
            "final_fd_redundancy_payload": payload_metrics,
            "stage_builds": stage_reports,
            "final_boundary_materialization": (final_boundary_materialization),
            "fold_construction_lifecycle": fold_construction_lifecycle,
            "fold_replacement_footprint": fold_replacement_footprint,
            "mv_creation_client_wall_ms_total": sum(
                float(stage["creation_timing"]["client_wall_ms"])
                for stage in stage_reports
            ),
            "mv_creation_including_final_boundaries_client_wall_ms_total": (
                sum(
                    float(stage["creation_timing"]["client_wall_ms"])
                    for stage in stage_reports
                )
                + (
                    float(
                        final_boundary_materialization["creation_timing"][
                            "client_wall_ms"
                        ]
                    )
                    if final_boundary_materialization["creation_timing"] is not None
                    else 0.0
                )
            ),
            "folded_shadow_state": asdict(folded_state),
            "normalized_shadow_state": None,
            "redundancy": redundancy,
            "configuration_redundancy": configuration_redundancy,
            "full_8table_replacement_update": full_update_result.report,
            "timing_alias_note": (
                "legacy folded_update/denormalized_update keys below alias the "
                "complete replacement-layout update, not a primary-only update"
            ),
            "timings": {
                "stage_builds": [
                    {
                        "stage_number": stage["stage_number"],
                        "creation_timing": stage["creation_timing"],
                        "previous_stage_deletion_timing": stage[
                            "previous_stage_deletion_timing"
                        ],
                    }
                    for stage in stage_reports
                ],
                "final_boundary_creation": (
                    final_boundary_materialization["creation_timing"]
                ),
                "replacement_update": folded_timing,
                "folded_update": folded_timing,
                "denormalized_update": folded_timing,
                "replacement_to_normalized_conversion": normalization_timing,
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
    except BaseException:
        print(
            f"\nWARNING: {case.case_id} stopped; attempting recursive "
            f"cleanup for run_id={run_id}",
            file=sys.stderr,
        )
        cleanup_errors: list[BaseException] = []
        try:
            with driver.session(
                database=database,
                default_access_mode=WRITE_ACCESS,
            ) as cleanup_session:
                try:
                    full_cleanup_result = cleanup_full_update_clones(
                        cleanup_session,
                        case_id=case.case_id,
                        run_id=run_id,
                        batch_size=args.batch_size,
                    )
                    result = cleanup_recursive_artifacts(
                        cleanup_session,
                        case,
                        run_id,
                        args.batch_size,
                    )
                    print(
                        "  Failure cleanup removed "
                        f"{result.deleted_nodes + full_cleanup_result.deleted_nodes:,} "
                        "nodes and "
                        f"{result.deleted_relationships + full_cleanup_result.deleted_relationships:,} "
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
                except BaseException as cleanup_error:
                    cleanup_errors.append(cleanup_error)
                try:
                    drop_recovery_schema(cleanup_session, run_id)
                except BaseException as cleanup_error:
                    cleanup_errors.append(cleanup_error)
                try:
                    failure_source_sha256 = original_graph_sha256(cleanup_session)
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
                except BaseException as cleanup_error:
                    cleanup_errors.append(cleanup_error)
        except BaseException as cleanup_session_error:
            cleanup_errors.append(cleanup_session_error)
        if cleanup_errors:
            print(
                "WARNING: automatic recursive cleanup checks failed: "
                + "; ".join(
                    f"{type(error).__name__}: {error}" for error in cleanup_errors
                ),
                file=sys.stderr,
            )
        raise
    finally:
        if workload is not None:
            workload.close()
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
        known_case_ids = set(CASES) | set(EXTENDED_CASES)
        unknown = sorted(set(case_ids) - known_case_ids)
        if unknown:
            raise ExperimentStateError(
                f"run {run_id!r} contains unknown case IDs: {unknown!r}"
            )
        cleanup_results: dict[str, dict[str, int]] = {}
        for case_id in case_ids:
            full_result = cleanup_full_update_clones(
                session,
                case_id=case_id,
                run_id=run_id,
                batch_size=batch_size,
            )
            if case_id in CASES:
                primary_result = cleanup_shadow_artifacts(
                    session,
                    CASES[case_id],
                    run_id,
                    batch_size,
                )
            else:
                primary_result = cleanup_recursive_artifacts(
                    session,
                    EXTENDED_CASES[case_id],
                    run_id,
                    batch_size,
                )
            cleanup_results[case_id] = {
                "deleted_nodes": (
                    full_result.deleted_nodes + primary_result.deleted_nodes
                ),
                "deleted_relationships": (
                    full_result.deleted_relationships
                    + primary_result.deleted_relationships
                ),
                "full_layout": asdict(full_result),
                "primary_fold": asdict(primary_result),
            }
        dropped_constraints, dropped_indexes = drop_recovery_schema(
            session,
            run_id,
        )
        remaining_nodes, remaining_relationships = artifact_counts(session)
        return {
            "report_schema_version": REPORT_SCHEMA_VERSION,
            "configuration_redundancy_schema_version": (
                CONFIGURATION_REDUNDANCY_SCHEMA_VERSION
            ),
            "mode": "cleanup_only",
            "run_id": run_id,
            "cleaned_cases": cleanup_results,
            "dropped_constraints": dropped_constraints,
            "dropped_indexes": dropped_indexes,
            "remaining_global_artifact_nodes": remaining_nodes,
            "remaining_global_artifact_relationships": (remaining_relationships),
            "source_graph_fingerprint_schema_version": (
                SOURCE_GRAPH_FINGERPRINT_SCHEMA_VERSION
            ),
            "source_graph_fingerprint_algorithm": (SOURCE_GRAPH_FINGERPRINT_ALGORITHM),
            "source_graph_sha256_after_cleanup": (original_graph_sha256(session)),
        }


def validate_main_args(args: argparse.Namespace) -> None:
    if args.case_id is None and args.update_property is not None:
        raise ExperimentValidationError(
            "--update-property requires --case because each referenced table "
            "has a different property domain"
        )
    if (args.list_cases or args.validate_cases_only) and (
        args.all_cases
        or args.all_extended_cases
        or args.case_id is not None
        or args.update_property is not None
    ):
        raise ExperimentValidationError(
            "--list-cases/--validate-cases-only cannot be combined with "
            "--case, --all-cases, --all-extended-cases, or "
            "--update-property"
        )
    recursive_selected = args.all_extended_cases or (
        args.case_id is not None and canonical_case_id(args.case_id) in EXTENDED_CASES
    )
    if args.deep_stage_fingerprints and not recursive_selected:
        raise ExperimentValidationError(
            "--deep-stage-fingerprints requires a recursive --case or "
            "--all-extended-cases"
        )
    if args.cleanup_run_id is not None:
        raw = args.cleanup_run_id.strip()
        if re.fullmatch(r"[0-9a-f]{32}", raw) is None:
            raise ExperimentValidationError(
                "--cleanup-run-id must be the exact 32-character lowercase "
                "hexadecimal UUID printed by this script"
            )
        if (
            args.all_cases
            or args.all_extended_cases
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
            args.cleanup_run_id.strip() if args.cleanup_run_id is not None else None
        )

        try:
            from neo4j import GraphDatabase
        except ImportError as exc:
            raise RuntimeError(
                'install the Neo4j driver with: python -m pip install "neo4j>=5.7"'
            ) from exc

        password = resolve_neo4j_password(args.user)

        print(f"Connecting to {args.uri} as {args.user!r}; database={database!r}")
        with single_process_experiment_lock():
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
                    if args.all_extended_cases:
                        selected_ids = ALL_CASE_ORDER
                    elif args.case_id is None:
                        selected_ids = CASE_ORDER
                    else:
                        selected_ids = (canonical_case_id(args.case_id),)
                    selected_labels: list[str] = []
                    for case_id in selected_ids:
                        if case_id in CASES:
                            pair = CASES[case_id]
                            selected_labels.extend(
                                (
                                    pair.names.folded_label,
                                    pair.names.referencing_label,
                                    pair.names.referenced_label,
                                )
                            )
                        else:
                            selected_labels.extend(
                                EXTENDED_CASES[case_id].all_artifact_labels
                            )
                    print("\nSource preflight", flush=True)
                    recursive_source_prevalidated = False
                    whole_source_graph_footprint: dict[str, Any] | None = None
                    with driver.session(database=database) as source_session:
                        ensure_no_existing_artifacts_for_labels(
                            source_session,
                            selected_labels,
                        )
                        selected_recursive_cases = [
                            EXTENDED_CASES[case_id]
                            for case_id in selected_ids
                            if case_id in EXTENDED_CASES
                        ]
                        if selected_recursive_cases:
                            print(
                                "  Validating recursive source shape...",
                                flush=True,
                            )
                            validated_tables: set[str] = set()
                            validated_relationships: set[str] = set()
                            for recursive_case in selected_recursive_cases:
                                validate_recursive_source_shape(
                                    source_session,
                                    recursive_case,
                                    validated_tables=validated_tables,
                                    validated_relationships=(validated_relationships),
                                )
                            recursive_source_prevalidated = True
                            print(
                                "  Recursive shape: OK | "
                                f"tables={len(validated_tables)}, "
                                "relationships="
                                f"{len(validated_relationships)}",
                                flush=True,
                            )
                        whole_source_graph_footprint = (
                            measure_tpch_source_graph_footprint(source_session)
                        )
                        print(
                            "  Complete TPC-H source graph: nodes="
                            f"{whole_source_graph_footprint['nodes']['total']:,}"
                            " | relationships="
                            f"{whole_source_graph_footprint['relationships']['total']:,}",
                            flush=True,
                        )
                        print(
                            "  Computing original graph fingerprint...",
                            flush=True,
                        )
                        suite_source_sha256_before = original_graph_sha256(
                            source_session
                        )
                    print(
                        "  Source fingerprint before suite: "
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
                        if case_id in CASES:
                            report = run_case_experiment(
                                driver,
                                database,
                                args,
                                CASES[case_id],
                                suite_source_sha256_before,
                                whole_source_graph_footprint=(
                                    whole_source_graph_footprint
                                ),
                            )
                        else:
                            report = run_recursive_case_experiment(
                                driver,
                                database,
                                args,
                                EXTENDED_CASES[case_id],
                                suite_source_sha256_before,
                                source_shape_prevalidated=(
                                    recursive_source_prevalidated
                                ),
                                whole_source_graph_footprint=(
                                    whole_source_graph_footprint
                                ),
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
                    if len(reports) > 1:
                        print(
                            "All per-case cleanup fingerprints match the "
                            "original TPC-H suite baseline.",
                            flush=True,
                        )
                    for report in reports:
                        if report["experiment_family"] == "tpch_direct_pair_fd":
                            print_final_comparison(report)
                        else:
                            print_recursive_final_comparison(report)

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
            recursive_suite = all(
                report["experiment_family"] == "tpch_recursive_fold_fd"
                for report in reports
            )
            direct_suite = all(
                report["experiment_family"] == "tpch_direct_pair_fd"
                for report in reports
            )
            if recursive_suite:
                output_report = {
                    "report_schema_version": RECURSIVE_REPORT_SCHEMA_VERSION,
                    "configuration_redundancy_schema_version": (
                        CONFIGURATION_REDUNDANCY_SCHEMA_VERSION
                    ),
                    "experiment": (
                        "tpch_recursive_fold_full_replacement_update_suite"
                    ),
                    "recursive_fold_schema_version": (RECURSIVE_FOLD_SCHEMA_VERSION),
                    "database": database,
                    "topology_mode": args.topology_mode,
                    "case_count": len(reports),
                    "case_order": list(selected_ids),
                    "source_graph_sha256_before": reports[0]["dataset"][
                        "source_graph_sha256_before"
                    ],
                    "source_graph_sha256_after": reports[-1]["dataset"][
                        "source_graph_sha256_after"
                    ],
                    "cases": reports,
                }
                suite_name = "recursive TPC-H fold"
            elif direct_suite:
                output_report = {
                    "report_schema_version": REPORT_SCHEMA_VERSION,
                    "configuration_redundancy_schema_version": (
                        CONFIGURATION_REDUNDANCY_SCHEMA_VERSION
                    ),
                    "experiment": "tpch_direct_pair_full_replacement_update_suite",
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
                suite_name = "direct TPC-H pair"
            else:
                output_report = {
                    "report_schema_version": REPORT_SCHEMA_VERSION,
                    "configuration_redundancy_schema_version": (
                        CONFIGURATION_REDUNDANCY_SCHEMA_VERSION
                    ),
                    "experiment": (
                        "tpch_direct_and_recursive_full_replacement_update_suite"
                    ),
                    "recursive_report_schema_version": (
                        RECURSIVE_REPORT_SCHEMA_VERSION
                    ),
                    "recursive_fold_schema_version": (RECURSIVE_FOLD_SCHEMA_VERSION),
                    "database": database,
                    "topology_mode": {
                        "direct_pairs": args.topology_mode,
                        "recursive_folds": args.topology_mode,
                    },
                    "case_count": len(reports),
                    "direct_pair_case_count": sum(
                        report["experiment_family"] == "tpch_direct_pair_fd"
                        for report in reports
                    ),
                    "recursive_case_count": sum(
                        report["experiment_family"] == "tpch_recursive_fold_fd"
                        for report in reports
                    ),
                    "case_order": list(selected_ids),
                    "source_graph_sha256_before": reports[0]["dataset"][
                        "source_graph_sha256_before"
                    ],
                    "source_graph_sha256_after": reports[-1]["dataset"][
                        "source_graph_sha256_after"
                    ],
                    "cases": reports,
                }
                suite_name = "TPC-H direct-pair and recursive fold"
            print(
                f"\nAll {len(reports)} {suite_name} cases completed; "
                "every shadow projection was removed"
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
