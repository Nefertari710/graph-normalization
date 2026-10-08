#!/usr/bin/env python3
"""Materialize Neo4j join-node candidates for the TPC-H query graphs.

This is the Neo4j counterpart of
``back/psql_back_scripts/join_table/auto_mv_table_schema.py``.  It preserves
the workload's query graphs, removes query-driven edges that are not present
in the Neo4j schema, and creates one flattened node per joined row for every
remaining connected induced subgraph.  Generated nodes receive:

* the common ``AUTO_JOIN_MV`` label, used to scope cleanup to generated data;
* a deterministic strategy label such as
  ``q3_mv_lineitem_orders_customer``; and
* copies of source relationships that remain external after applying the
  schema inheritance rule.

The eight source TPC-H node labels and their relationships are never modified.
No relationship is created from a materialized node back to any source node
whose properties were folded into that materialized node.
When one strategy folds Nation-Region together with exactly one of
Supplier-Nation or Customer-Nation, the selected Nation-Region edge is closed
over the other still-live Nation occurrence.  The strategy therefore owns its
primary component plus a role-tagged Nation-Region companion: ``SNR`` is
paired with ``C -> NR``, while ``CNR`` is paired with ``S -> NR``.  A parent
role already consumed by the primary component is structurally forbidden from
being inherited by its companion or by the opposite primary component.
For each selected schema edge, the endpoint farther from the candidate's
canonical anchor does not copy its join-back role.  Other external
relationships remain eligible for inheritance only when a cardinality-aware
preflight confirms that they do not exceed the configured directional fanout
limit.  The benchmark runner can skip that expensive preflight; in that mode,
the metadata is marked unchecked and every validated boundary relationship
type remains eligible after structural hard blocks are applied.
Each run removes existing generated artifacts in the selected query or exact
strategy scope, sizes every candidate and, when enabled, analyzes all
inheritance policies before creating the first MV, then rebuilds those
candidates.  Exact strategy selection is also used by the test benchmark
runner so it can create, benchmark, and delete one MV at a time.

Use ``--dry-run`` to enumerate candidates without connecting to Neo4j.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import sys
from dataclasses import dataclass
from getpass import getpass
from itertools import combinations
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Iterable, Sequence


# =========================
# Connection/configuration
# =========================

DATABASE_NAME = "tpch-sf-001"
DEBUG_NEO4J_PASSWORD: str | None = ""

DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_MAX_ESTIMATED_ROWS = 200_000_000
DEFAULT_MAX_CANDIDATE_ROWS = 200_000_000
DEFAULT_QUERY_TIMEOUT_SECONDS = 3_600.0
DEFAULT_INDEX_WAIT_SECONDS = 3_600
DEFAULT_MAX_INCOMING_FANOUT = 1
DEFAULT_MAX_OUTGOING_DEGREE = 1

GENERATED_ROW_LABEL = "AUTO_JOIN_MV"
STRATEGY_LABEL = "AUTO_JOIN_STRATEGY"
STRATEGY_CONSTRAINT = "auto_join_strategy_name"
GENERATED_INDEX_PREFIX = "auto_join_mv_q"
EXPERIMENT_NAME = "tpch_auto_join_mv"
MATERIALIZATION_SCHEMA_VERSION = 3
CARDINALITY_AWARE_INHERITANCE_MODE = "cardinality_aware_directional"
UNCHECKED_INHERITANCE_MODE = "unchecked_all_validated_boundary_types"
SUPPLIER_NATION_CUSTOMER_EXCLUSION_REASON = (
    "supplier_nation_join_excludes_customer_nation"
)
CUSTOMER_NATION_SUPPLIER_EXCLUSION_REASON = (
    "customer_nation_join_excludes_supplier_nation"
)
FOLD_CLOSURE_CONSUMED_PARENT_REASON = "fold_closure_consumed_parent"
FOLD_COMPONENT_SCOPE_REASON = "fold_component_relationship_scope"
FANOUT_LIMIT_EXCEEDED_REASON = "fanout_limit_exceeded"

COMPONENT_PROPERTY = "_auto_join_component"
PRIMARY_COMPONENT_ROLE = "primary"
NATION_REGION_CLOSURE_ROLE = "nation_region_closure"
SUPPLIER_NATION_REGION_CLOSURE_ROLE = "supplier_nation_region_closure"

EXCEED_DIR = Path(__file__).resolve().parent / "exceed_marks"


# =========================
# TPC-H query join graphs
# =========================

Edge = tuple[str, str]

QUERY_GRAPHS: tuple[tuple[int, tuple[Edge, ...]], ...] = (
    (2, (("P", "PS"), ("S", "PS"), ("S", "N"), ("N", "R"))),
    (3, (("L", "O"), ("O", "C"))),
    (4, (("L", "O"),)),
    (5, (("L", "O"), ("O", "C"), ("C", "S"), ("L", "S"), ("S", "N"), ("N", "R"),),),
    (7, (("L", "O"), ("O", "C"), ("C", "N2"), ("L", "S"), ("S", "N1"),),),
    (8, (("L", "O"), ("L", "P"), ("O", "C"), ("C", "N1"), ("L", "S"), ("S", "N2"), ("N1", "R"),),),
    (9, (("L", "O"), ("L", "P"), ("L", "PS"), ("L", "S"), ("S", "N"),),),
    (10, (("L", "O"), ("O", "C"), ("C", "N"))),
    (11, (("PS", "S"), ("S", "N"))),
    (12, (("L", "O"),)),
    (13, (("O", "C"),)),
    (14, (("L", "P"),)),
    (15, (("L", "S"),)),
    (16, (("PS", "P"), ("PS", "S"))),
    (17, (("L", "P"),)),
    (18, (("L", "O"), ("O", "C"))),
    (19, (("L", "P"),)),
    (20, (("L", "PS"), ("PS", "P"), ("PS", "S"), ("S", "N"))),
    (21, (("L", "S"), ("L", "O"), ("S", "N"))),
    (22, (("O", "C"),)),
)

# These edges are part of the workload query graphs but are not relationships
# in the normalized Neo4j schema.  The query-driven experiment keeps them.
QUERY_DRIVEN_EDGES = frozenset(
    {
        ("L", "S"),
        ("L", "P"),
        ("C", "S"),
    }
)

NODE_LABEL_BY_SYMBOL = {
    "L": "LINEITEM",
    "O": "ORDERS",
    "C": "CUSTOMER",
    "S": "SUPPLIER",
    "N": "NATION",
    "N1": "NATION",
    "N2": "NATION",
    "R": "REGION",
    "P": "PART",
    "PS": "PARTSUPP",
}

DISPLAY_NAME_BY_SYMBOL = {
    "L": "lineitem",
    "O": "orders",
    "C": "customer",
    "S": "supplier",
    "N": "nation",
    "N1": "nation1",
    "N2": "nation2",
    "R": "region",
    "P": "part",
    "PS": "partsupp",
}

CANONICAL_ORDER = ("L", "O", "C", "PS", "S", "P", "N", "N1", "N2", "R")
SYMBOL_RANK = {symbol: rank for rank, symbol in enumerate(CANONICAL_ORDER)}
NATION_SYMBOLS = frozenset({"N", "N1", "N2"})

# Each value contains (left_property, right_property) pairs.
JOIN_KEYS: dict[Edge, tuple[tuple[str, str], ...]] = {
    ("L", "O"): (("l_orderkey", "o_orderkey"),),
    ("O", "C"): (("o_custkey", "c_custkey"),),
    ("C", "N"): (("c_nationkey", "n_nationkey"),),
    ("S", "N"): (("s_nationkey", "n_nationkey"),),
    ("N", "R"): (("n_regionkey", "r_regionkey"),),
    ("C", "N1"): (("c_nationkey", "n_nationkey"),),
    ("C", "N2"): (("c_nationkey", "n_nationkey"),),
    ("S", "N1"): (("s_nationkey", "n_nationkey"),),
    ("S", "N2"): (("s_nationkey", "n_nationkey"),),
    ("N1", "R"): (("n_regionkey", "r_regionkey"),),
    ("L", "PS",): (("l_partkey", "ps_partkey"),
                   ("l_suppkey", "ps_suppkey"),),
    ("PS", "S"): (("ps_suppkey", "s_suppkey"),),
    ("PS", "P"): (("ps_partkey", "p_partkey"),),
}

PK_PROPERTIES_BY_SYMBOL: dict[str, tuple[str, ...]] = {
    "R": ("r_regionkey",),
    "N": ("n_nationkey",),
    "N1": ("n_nationkey",),
    "N2": ("n_nationkey",),
    "S": ("s_suppkey",),
    "C": ("c_custkey",),
    "P": ("p_partkey",),
    "PS": ("ps_partkey", "ps_suppkey"),
    "O": ("o_orderkey",),
    "L": ("l_orderkey", "l_linenumber"),
}

FK_PROPERTIES_BY_SYMBOL: dict[str, tuple[tuple[str, ...], ...]] = {
    "S": (("s_nationkey",),),
    "C": (("c_nationkey",),),
    "O": (("o_custkey",),),
    "L": (("l_orderkey",), ("l_partkey",), ("l_suppkey",)),
    "PS": (("ps_partkey",), ("ps_suppkey",)),
    "N": (("n_regionkey",),),
    "N1": (("n_regionkey",),),
    "N2": (("n_regionkey",),),
}

NATION_PROPERTIES = (
    "n_nationkey",
    "n_name",
    "n_regionkey",
    "n_comment",
)

SOURCE_RELATIONSHIP_RULES = (
    ("NATION", "NATION_REGION", "REGION"),
    ("SUPPLIER", "SUPPLIER_NATION", "NATION"),
    ("CUSTOMER", "CUSTOMER_NATION", "NATION"),
    ("ORDERS", "ORDERS_CUSTOMER", "CUSTOMER"),
    ("PARTSUPP", "PARTSUPP_PART", "PART"),
    ("PARTSUPP", "PARTSUPP_SUPPLIER", "SUPPLIER"),
    ("LINEITEM", "LINEITEM_ORDERS", "ORDERS"),
    ("LINEITEM", "LINEITEM_PARTSUPP", "PARTSUPP"),
)


# =========================
# Candidate enumeration
# =========================


@dataclass(frozen=True)
class JoinStep:
    child: str
    parent: str
    optional: bool
    extra_neighbors: tuple[str, ...]


@dataclass(frozen=True)
class BoundaryRole:
    direction: str
    relationship_type: str


@dataclass(frozen=True)
class JoinBackBlock:
    owner: str
    relationship_type: str


@dataclass(frozen=True)
class InheritanceHardBlock:
    direction: str
    relationship_type: str
    reason: str


@dataclass(frozen=True)
class RelationshipBlocks:
    outgoing: tuple[JoinBackBlock, ...]
    incoming: tuple[JoinBackBlock, ...]


@dataclass(frozen=True)
class InheritanceStat:
    direction: str
    relationship_type: str
    baseline_relationships: int
    projected_relationships: int
    max_fanout: int
    reached_nodes: int
    allowed: bool
    decision_reason: str | None = None

    @property
    def amplification(self) -> float | None:
        if self.baseline_relationships == 0:
            return None
        return self.projected_relationships / self.baseline_relationships

    @property
    def metadata_value(self) -> str:
        amplification = (
            "N/A" if self.amplification is None else f"{self.amplification:.6f}"
        )
        return (
            f"{self.direction}|{self.relationship_type}|"
            f"baseline={self.baseline_relationships}|"
            f"projected={self.projected_relationships}|"
            f"amplification={amplification}|"
            f"max_fanout={self.max_fanout}|"
            f"allowed={str(self.allowed).lower()}|"
            f"decision_reason={self.decision_reason or 'none'}"
        )


@dataclass(frozen=True)
class InheritancePolicy:
    max_incoming_fanout: int | None
    max_outgoing_degree: int | None
    stats: tuple[InheritanceStat, ...]
    mode: str = CARDINALITY_AWARE_INHERITANCE_MODE
    allowed_incoming_types_override: tuple[str, ...] | None = None
    allowed_outgoing_types_override: tuple[str, ...] | None = None

    @property
    def allowed_incoming_types(self) -> tuple[str, ...]:
        if self.allowed_incoming_types_override is not None:
            return self.allowed_incoming_types_override
        return tuple(
            stat.relationship_type
            for stat in self.stats
            if stat.direction == "incoming" and stat.allowed
        )

    @property
    def allowed_outgoing_types(self) -> tuple[str, ...]:
        if self.allowed_outgoing_types_override is not None:
            return self.allowed_outgoing_types_override
        return tuple(
            stat.relationship_type
            for stat in self.stats
            if stat.direction == "outgoing" and stat.allowed
        )


@dataclass(frozen=True)
class BoundedRowCount:
    """Result of counting candidate rows up to ``max_rows + 1``."""

    observed_rows: int
    max_rows: int

    @property
    def exceeded_limit(self) -> bool:
        return self.observed_rows > self.max_rows

    @property
    def exact_rows(self) -> int | None:
        return None if self.exceeded_limit else self.observed_rows


@dataclass(frozen=True)
class StrategySizing:
    """Sizing information collected before any candidate MV is created."""

    estimated_peak_rows: float | None
    estimate_message: str | None
    row_count: BoundedRowCount | None
    count_message: str | None


@dataclass(frozen=True)
class MaterializationSummary:
    """Counts and MV write time returned by the session-level materializer."""

    ready: int
    skipped: int
    materialized_nodes: int
    inherited_relationships: int
    indexes: int
    creation_wall_ms: float = 0.0
    creation_wall_ms_by_strategy: tuple[tuple[str, float], ...] = ()


@dataclass(frozen=True)
class GeneratedArtifactPresence:
    """Whether generated artifacts exist in one cleanup scope."""

    materialized_nodes: bool
    strategy_nodes: bool
    indexes: bool

    @property
    def any(self) -> bool:
        return self.materialized_nodes or self.strategy_nodes or self.indexes


@dataclass(frozen=True)
class Strategy:
    query_id: int
    symbols: tuple[str, ...]
    edges: tuple[Edge, ...]

    @property
    def name(self) -> str:
        tables = "_".join(DISPLAY_NAME_BY_SYMBOL[symbol] for symbol in self.symbols)
        return safe_ident(f"q{self.query_id}_mv_{tables}")

    @property
    def edge_names(self) -> tuple[str, ...]:
        return tuple(f"{left}-{right}" for left, right in self.edges)


@dataclass(frozen=True)
class FoldComponent:
    """One physical node role belonging to a logical fold strategy.

    The primary component is the requested connected-subgraph MV.  Closure
    components materialize other live occurrences of a selected schema edge.
    All components share the owner's strategy label so lifecycle operations
    continue to create and clean one configuration as a unit.
    """

    owner: Strategy
    strategy: Strategy
    role: str
    hard_blocks: tuple[InheritanceHardBlock, ...] = ()
    allowed_incoming_types: tuple[str, ...] | None = None
    allowed_outgoing_types: tuple[str, ...] | None = None

    @property
    def display_name(self) -> str:
        return f"{self.owner.name}[{self.role}]"

    @property
    def is_primary(self) -> bool:
        return self.role == PRIMARY_COMPONENT_ROLE


def strategy_fold_components(strategy: Strategy) -> tuple[FoldComponent, ...]:
    """Expand one strategy into its primary MV and required edge closures.

    Folding Supplier-Nation or Customer-Nation leaves the other parent on a
    still-live Nation occurrence.  If the same strategy also folds
    Nation-Region, that second edge must be folded on both occurrences.  The
    companion exposes only the unconsumed parent relationship and suppresses
    the parent already consumed by the primary component.  When both parent
    edges are already part of the primary strategy, neither companion is
    created.
    """

    primary_hard_blocks: list[InheritanceHardBlock] = []
    if joins_supplier_and_nation(strategy):
        primary_hard_blocks.append(
            InheritanceHardBlock(
                direction="incoming",
                relationship_type="CUSTOMER_NATION",
                reason=SUPPLIER_NATION_CUSTOMER_EXCLUSION_REASON,
            )
        )
    if joins_customer_and_nation(strategy):
        primary_hard_blocks.append(
            InheritanceHardBlock(
                direction="incoming",
                relationship_type="SUPPLIER_NATION",
                reason=CUSTOMER_NATION_SUPPLIER_EXCLUSION_REASON,
            )
        )
    components = [
        FoldComponent(
            owner=strategy,
            strategy=strategy,
            role=PRIMARY_COMPONENT_ROLE,
            hard_blocks=tuple(primary_hard_blocks),
        )
    ]
    edge_set = {canonical_edge(edge) for edge in strategy.edges}
    for nation_symbol in sorted(
        NATION_SYMBOLS,
        key=lambda value: (sym_rank(value), value),
    ):
        supplier_edge = canonical_edge(("S", nation_symbol))
        customer_edge = canonical_edge(("C", nation_symbol))
        region_edge = canonical_edge((nation_symbol, "R"))
        has_supplier_edge = supplier_edge in edge_set
        has_customer_edge = customer_edge in edge_set
        if (
            region_edge not in edge_set
            or has_supplier_edge == has_customer_edge
        ):
            continue

        closure_strategy = Strategy(
            query_id=strategy.query_id,
            symbols=canonical_symbols((nation_symbol, "R")),
            edges=canonical_edges((region_edge,)),
        )
        if has_supplier_edge:
            role = (
                NATION_REGION_CLOSURE_ROLE
                if nation_symbol == "N"
                else safe_ident(
                    f"{DISPLAY_NAME_BY_SYMBOL[nation_symbol]}_region_closure"
                )
            )
            consumed_relationship_type = "SUPPLIER_NATION"
            live_relationship_type = "CUSTOMER_NATION"
        else:
            role = SUPPLIER_NATION_REGION_CLOSURE_ROLE
            consumed_relationship_type = "CUSTOMER_NATION"
            live_relationship_type = "SUPPLIER_NATION"

        components.append(
            FoldComponent(
                owner=strategy,
                strategy=closure_strategy,
                role=role,
                hard_blocks=(
                    InheritanceHardBlock(
                        direction="incoming",
                        relationship_type=consumed_relationship_type,
                        reason=FOLD_CLOSURE_CONSUMED_PARENT_REASON,
                    ),
                ),
                allowed_incoming_types=(live_relationship_type,),
                allowed_outgoing_types=(),
            )
        )

    return tuple(components)


def joins_supplier_and_nation(strategy: Strategy) -> bool:
    """Whether the candidate absorbs a direct Supplier-Nation join."""

    return any(
        (left == "S" and right in NATION_SYMBOLS)
        or (right == "S" and left in NATION_SYMBOLS)
        for left, right in strategy.edges
    )


def joins_customer_and_nation(strategy: Strategy) -> bool:
    """Whether the candidate absorbs a direct Customer-Nation join."""

    return any(
        (left == "C" and right in NATION_SYMBOLS)
        or (right == "C" and left in NATION_SYMBOLS)
        for left, right in strategy.edges
    )


def inheritance_is_hard_blocked(
    strategy: Strategy,
    direction: str,
    relationship_type: str,
    additional_blocks: Sequence[InheritanceHardBlock] = (),
) -> bool:
    """Apply structural exclusions that do not require a fanout query."""

    return inheritance_hard_block_reason(
        strategy,
        direction,
        relationship_type,
        additional_blocks,
    ) is not None


def inheritance_hard_block_reason(
    strategy: Strategy,
    direction: str,
    relationship_type: str,
    additional_blocks: Sequence[InheritanceHardBlock] = (),
) -> str | None:
    """Return the structural exclusion reason for one boundary role."""

    if direction not in {"incoming", "outgoing"}:
        raise ValueError(f"unknown inheritance direction: {direction!r}")
    if (
        direction == "incoming"
        and relationship_type == "CUSTOMER_NATION"
        and joins_supplier_and_nation(strategy)
    ):
        return SUPPLIER_NATION_CUSTOMER_EXCLUSION_REASON
    if (
        direction == "incoming"
        and relationship_type == "SUPPLIER_NATION"
        and joins_customer_and_nation(strategy)
    ):
        return CUSTOMER_NATION_SUPPLIER_EXCLUSION_REASON
    for block in additional_blocks:
        if (
            block.direction == direction
            and block.relationship_type == relationship_type
        ):
            return block.reason
    return None


def safe_ident(name: str) -> str:
    identifier = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if identifier and identifier[0].isdigit():
        identifier = f"n_{identifier}"
    return identifier


def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def sym_rank(symbol: str) -> int:
    return SYMBOL_RANK.get(symbol, 999)


def canonical_edge(edge: Edge) -> Edge:
    left, right = edge
    if (sym_rank(left), left) <= (sym_rank(right), right):
        return left, right
    return right, left


def canonical_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(symbols), key=lambda value: (sym_rank(value), value)))


def canonical_edges(edges: Iterable[Edge]) -> tuple[Edge, ...]:
    return tuple(
        sorted(
            {canonical_edge(edge) for edge in edges},
            key=lambda edge: (
                sym_rank(edge[0]),
                sym_rank(edge[1]),
                edge[0],
                edge[1],
            ),
        )
    )


def induced_edges(full_edges: Sequence[Edge], node_set: set[str]) -> tuple[Edge, ...]:
    return canonical_edges(
        edge
        for edge in full_edges
        if edge[0] in node_set and edge[1] in node_set
    )


def graph_symbols(full_edges: Sequence[Edge]) -> tuple[str, ...]:
    return canonical_symbols(symbol for edge in full_edges for symbol in edge)


def build_adjacency(edges: Sequence[Edge]) -> dict[str, set[str]]:
    adjacency: dict[str, set[str]] = {}
    for left, right in edges:
        adjacency.setdefault(left, set()).add(right)
        adjacency.setdefault(right, set()).add(left)
    return adjacency


def is_connected(node_set: set[str], edges: Sequence[Edge]) -> bool:
    if len(node_set) < 2 or not edges:
        return False

    adjacency = {symbol: set() for symbol in node_set}
    for left, right in edges:
        adjacency[left].add(right)
        adjacency[right].add(left)

    start = next(iter(node_set))
    seen = {start}
    stack = [start]
    while stack:
        current = stack.pop()
        for neighbor in adjacency[current]:
            if neighbor not in seen:
                seen.add(neighbor)
                stack.append(neighbor)
    return seen == node_set


def enumerate_strategies(
    selected_query_ids: set[int] | None = None,
) -> list[Strategy]:
    strategies: list[Strategy] = []
    for query_id, raw_edges in QUERY_GRAPHS:
        if selected_query_ids is not None and query_id not in selected_query_ids:
            continue

        full_edges = canonical_edges(raw_edges)
        schema_edges = tuple(
            edge for edge in full_edges if edge not in QUERY_DRIVEN_EDGES
        )
        symbols = graph_symbols(full_edges)
        for size in range(2, len(symbols) + 1):
            for combination in combinations(symbols, size):
                node_set = set(combination)
                edges = induced_edges(schema_edges, node_set)
                if is_connected(node_set, edges):
                    strategies.append(
                        Strategy(
                            query_id=query_id,
                            symbols=canonical_symbols(node_set),
                            edges=edges,
                        )
                    )
    return strategies


def select_strategies(
    selected_query_ids: set[int] | None = None,
    selected_strategy_names: set[str] | None = None,
) -> list[Strategy]:
    """Select strategies with query/name filters using intersection semantics."""

    all_strategies = enumerate_strategies()
    known_names = {strategy.name for strategy in all_strategies}
    requested_names = selected_strategy_names or set()
    unknown_names = requested_names - known_names
    if unknown_names:
        raise RuntimeError(
            "unknown AUTO_JOIN_STRATEGY name(s): "
            + ", ".join(sorted(unknown_names))
        )

    selected = [
        strategy
        for strategy in all_strategies
        if (
            selected_query_ids is None
            or strategy.query_id in selected_query_ids
        )
        and (not requested_names or strategy.name in requested_names)
    ]
    if not selected and requested_names:
        raise RuntimeError("the query/strategy filters selected no MV strategies")
    return selected


def shortest_distances(
    start: str,
    adjacency: dict[str, set[str]],
    blocked: set[str] | None = None,
) -> dict[str, int]:
    blocked = blocked or set()
    if start in blocked:
        return {}

    distances = {start: 0}
    queue = [start]
    for current in queue:
        for neighbor in sorted(
            adjacency.get(current, set()),
            key=lambda value: (sym_rank(value), value),
        ):
            if neighbor in blocked or neighbor in distances:
                continue
            distances[neighbor] = distances[current] + 1
            queue.append(neighbor)
    return distances


def customer_preserving_symbols(strategy: Strategy) -> set[str]:
    if "C" not in strategy.symbols or "O" not in strategy.symbols:
        return set()

    adjacency = build_adjacency(strategy.edges)
    return set(shortest_distances("O", adjacency, blocked={"C"}))


def build_join_steps(strategy: Strategy) -> tuple[str, tuple[JoinStep, ...]]:
    preserve_customer = "C" in strategy.symbols and "O" in strategy.symbols
    nullable_symbols = (
        customer_preserving_symbols(strategy) if preserve_customer else set()
    )
    root = "C" if preserve_customer else strategy.symbols[0]

    adjacency = build_adjacency(strategy.edges)
    distance_from_root = shortest_distances(root, adjacency)
    distance_from_orders = (
        shortest_distances("O", adjacency, blocked={"C"}) if preserve_customer else {}
    )

    joined = {root}
    steps: list[JoinStep] = []
    while len(joined) < len(strategy.symbols):
        candidates = [
            symbol
            for symbol in strategy.symbols
            if symbol not in joined
            and any(neighbor in joined for neighbor in adjacency.get(symbol, set()))
        ]
        if not candidates:
            raise ValueError(
                f"Unable to build join tree for {strategy.name}: "
                f"symbols={strategy.symbols}, edges={strategy.edges}"
            )

        def candidate_key(symbol: str) -> tuple[int, int, int, str]:
            is_nullable = 0 if symbol in nullable_symbols else 1
            distance = (
                distance_from_orders.get(symbol, 10**9)
                if symbol in nullable_symbols
                else distance_from_root.get(symbol, 10**9)
            )
            return is_nullable, distance, sym_rank(symbol), symbol

        child = min(candidates, key=candidate_key)
        joined_neighbors = sorted(
            (
                neighbor
                for neighbor in adjacency.get(child, set())
                if neighbor in joined
            ),
            key=lambda value: (sym_rank(value), value),
        )

        if child in nullable_symbols:
            parent = min(
                joined_neighbors,
                key=lambda symbol: (
                    0 if symbol in nullable_symbols else 1,
                    distance_from_orders.get(symbol, 10**9),
                    sym_rank(symbol),
                    symbol,
                ),
            )
            optional = True
        else:
            parent = min(
                joined_neighbors,
                key=lambda symbol: (
                    distance_from_root.get(symbol, 10**9),
                    sym_rank(symbol),
                    symbol,
                ),
            )
            optional = False

        steps.append(
            JoinStep(
                child=child,
                parent=parent,
                optional=optional,
                extra_neighbors=tuple(
                    neighbor for neighbor in joined_neighbors if neighbor != parent
                ),
            )
        )
        joined.add(child)

    return root, tuple(steps)


def oriented_strategy_edges(strategy: Strategy) -> tuple[Edge, ...]:
    """Orient schema edges away from the candidate's canonical anchor."""

    anchor = strategy.symbols[0]
    distances = shortest_distances(anchor, build_adjacency(strategy.edges))
    if set(distances) != set(strategy.symbols):
        raise ValueError(
            f"Unable to orient disconnected strategy {strategy.name}: "
            f"symbols={strategy.symbols}, edges={strategy.edges}"
        )

    oriented: list[Edge] = []
    for raw_edge in strategy.edges:
        left, right = canonical_edge(raw_edge)
        left_key = (distances[left], sym_rank(left), left)
        right_key = (distances[right], sym_rank(right), right)
        oriented.append((left, right) if left_key < right_key else (right, left))
    return tuple(oriented)


# =========================
# Cypher generation
# =========================


def alias_for(symbol: str) -> str:
    return symbol.lower()


def node_pattern(symbol: str) -> str:
    return f"({alias_for(symbol)}:{quote_ident(NODE_LABEL_BY_SYMBOL[symbol])})"


def relationship_pattern(left: str, right: str) -> str:
    symbols = {left, right}
    lineitem = node_pattern("L")
    orders = node_pattern("O")
    customer = node_pattern("C")
    supplier = node_pattern("S")
    part = node_pattern("P")
    partsupp = node_pattern("PS")
    region = node_pattern("R")

    if symbols == {"L", "O"}:
        return f"{lineitem}-[:LINEITEM_ORDERS]->{orders}"
    if symbols == {"O", "C"}:
        return f"{orders}-[:ORDERS_CUSTOMER]->{customer}"
    if symbols == {"L", "PS"}:
        return f"{lineitem}-[:LINEITEM_PARTSUPP]->{partsupp}"
    if symbols == {"PS", "P"}:
        return f"{partsupp}-[:PARTSUPP_PART]->{part}"
    if symbols == {"PS", "S"}:
        return f"{partsupp}-[:PARTSUPP_SUPPLIER]->{supplier}"

    nation_roles = symbols & NATION_SYMBOLS
    if "C" in symbols and len(nation_roles) == 1:
        nation = node_pattern(next(iter(nation_roles)))
        return f"{customer}-[:CUSTOMER_NATION]->{nation}"
    if "S" in symbols and len(nation_roles) == 1:
        nation = node_pattern(next(iter(nation_roles)))
        return f"{supplier}-[:SUPPLIER_NATION]->{nation}"
    if "R" in symbols and len(nation_roles) == 1:
        nation = node_pattern(next(iter(nation_roles)))
        return f"{nation}-[:NATION_REGION]->{region}"

    raise KeyError(f"No Neo4j relationship path for edge ({left}, {right})")


def directed_boundary_roles(
    source: str,
    relationship_type: str,
    target: str,
) -> dict[str, BoundaryRole]:
    return {
        source: BoundaryRole("outgoing", relationship_type),
        target: BoundaryRole("incoming", relationship_type),
    }


def boundary_roles_for_edge(left: str, right: str) -> dict[str, BoundaryRole]:
    """Return the physical relationship role adjacent to each schema endpoint."""

    symbols = {left, right}

    if symbols == {"L", "O"}:
        return directed_boundary_roles("L", "LINEITEM_ORDERS", "O")
    if symbols == {"O", "C"}:
        return directed_boundary_roles("O", "ORDERS_CUSTOMER", "C")
    if symbols == {"L", "PS"}:
        return directed_boundary_roles("L", "LINEITEM_PARTSUPP", "PS")
    if symbols == {"PS", "P"}:
        return directed_boundary_roles("PS", "PARTSUPP_PART", "P")
    if symbols == {"PS", "S"}:
        return directed_boundary_roles("PS", "PARTSUPP_SUPPLIER", "S")

    nation_roles = symbols & NATION_SYMBOLS
    if "C" in symbols and len(nation_roles) == 1:
        nation = next(iter(nation_roles))
        return directed_boundary_roles("C", "CUSTOMER_NATION", nation)
    if "S" in symbols and len(nation_roles) == 1:
        nation = next(iter(nation_roles))
        return directed_boundary_roles("S", "SUPPLIER_NATION", nation)
    if "R" in symbols and len(nation_roles) == 1:
        nation = next(iter(nation_roles))
        return directed_boundary_roles(nation, "NATION_REGION", "R")

    raise KeyError(f"No Neo4j boundary roles for edge ({left}, {right})")


def relationship_inheritance_blocks(
    strategy: Strategy,
) -> dict[str, RelationshipBlocks]:
    """Block each join's far-side role while keeping unrelated boundary edges."""

    blocked: dict[str, dict[str, set[tuple[str, str]]]] = {
        symbol: {"outgoing": set(), "incoming": set()}
        for symbol in strategy.symbols
    }
    for owner, absorbed in oriented_strategy_edges(strategy):
        role = boundary_roles_for_edge(owner, absorbed)[absorbed]
        blocked[absorbed][role.direction].add(
            (owner, role.relationship_type)
        )

    return {
        symbol: RelationshipBlocks(
            outgoing=tuple(
                JoinBackBlock(owner, relationship_type)
                for owner, relationship_type in sorted(directions["outgoing"])
            ),
            incoming=tuple(
                JoinBackBlock(owner, relationship_type)
                for owner, relationship_type in sorted(directions["incoming"])
            ),
        )
        for symbol, directions in blocked.items()
    }


def blocked_types_expression(blocks: Sequence[JoinBackBlock]) -> str:
    """Render blocks that apply only while their owner-side node is present."""

    if not blocks:
        return "[]"

    candidates = ", ".join(
        f"CASE WHEN {alias_for(block.owner)} IS NOT NULL "
        f"THEN '{block.relationship_type}' END"
        for block in blocks
    )
    return (
        f"[_blocked_type IN [{candidates}] "
        "WHERE _blocked_type IS NOT NULL]"
    )


def join_key_pairs(left: str, right: str) -> tuple[tuple[str, str], ...]:
    if (left, right) in JOIN_KEYS:
        return JOIN_KEYS[(left, right)]
    if (right, left) in JOIN_KEYS:
        return tuple(
            (right_property, left_property)
            for left_property, right_property in JOIN_KEYS[(right, left)]
        )
    raise KeyError(f"No join-key rule for edge ({left}, {right})")


def join_predicate(left: str, right: str) -> str:
    left_alias = alias_for(left)
    right_alias = alias_for(right)
    predicates = [
        f"{left_alias}.{left_property} = {right_alias}.{right_property}"
        for left_property, right_property in join_key_pairs(left, right)
    ]
    return "(" + " AND ".join(predicates) + ")"


def build_match_clauses(strategy: Strategy) -> tuple[str, ...]:
    root, steps = build_join_steps(strategy)
    clauses = [f"MATCH {node_pattern(root)}"]

    for step in steps:
        keyword = "OPTIONAL MATCH" if step.optional else "MATCH"
        pattern = relationship_pattern(step.parent, step.child)
        clause = f"{keyword} {pattern}"
        if step.extra_neighbors:
            predicates = [
                join_predicate(neighbor, step.child)
                for neighbor in step.extra_neighbors
            ]
            clause += "\nWHERE " + "\n  AND ".join(predicates)
        clauses.append(clause)

    return tuple(clauses)


def build_explain_query(strategy: Strategy) -> str:
    return "\n".join((*build_match_clauses(strategy), "RETURN 1 AS candidate_row"))


def build_bounded_count_query(strategy: Strategy) -> str:
    """Count candidate rows, stopping after the supplied probe limit."""

    return "\n".join(
        (
            *build_match_clauses(strategy),
            "WITH 1 AS candidate_row",
            "LIMIT $probe_limit",
            "RETURN count(*) AS counted_rows",
        )
    )


def mapped_nation_property(symbol: str, property_name: str) -> str:
    suffix = property_name[2:] if property_name.startswith("n_") else property_name
    return f"{alias_for(symbol)}_{suffix}"


def property_copy_clauses(strategy: Strategy) -> tuple[str, ...]:
    nation_count = sum(symbol in NATION_SYMBOLS for symbol in strategy.symbols)
    clauses: list[str] = []

    for symbol in strategy.symbols:
        alias = alias_for(symbol)
        if symbol in NATION_SYMBOLS and nation_count > 1:
            entries = ", ".join(
                f"{mapped_nation_property(symbol, property_name)}: "
                f"{alias}.{property_name}"
                for property_name in NATION_PROPERTIES
            )
            clauses.append(f"SET mv += {{{entries}}}")
        else:
            clauses.append(f"SET mv += coalesce(properties({alias}), {{}})")

    return tuple(clauses)


def inheritance_context_expressions(strategy: Strategy) -> tuple[str, str]:
    """Build the selected-node and role expressions shared by all inheritance."""

    aliases = tuple(alias_for(symbol) for symbol in strategy.symbols)
    selected_nodes = "[" + ", ".join(aliases) + "]"
    blocks = relationship_inheritance_blocks(strategy)
    source_roles = "[" + ", ".join(
        (
            f"{{node: {alias_for(symbol)}, "
            "blocked_outgoing: "
            f"{blocked_types_expression(blocks[symbol].outgoing)}, "
            "blocked_incoming: "
            f"{blocked_types_expression(blocks[symbol].incoming)}}}"
        )
        for symbol in strategy.symbols
    ) + "]"
    return selected_nodes, source_roles


def relationship_inheritance_clauses(strategy: Strategy) -> tuple[str, ...]:
    """Copy policy-approved boundary relationships onto ``mv``.

    The first canonical symbol is the anchor.  Schema edges are oriented away
    from it, and the far endpoint's join-back relationship role is suppressed.
    A block applies only when that schema edge's owner-side node is present,
    so a null-extended outer-join row still inherits the surviving node's
    ordinary boundary relationships.

    Remaining roles are filtered through direction-specific relationship-type
    allowlists.  These come from the fanout preflight or, in unchecked mode,
    the validated source schema.  Outgoing and incoming relationships are
    copied in separate unit subqueries so their degrees do not multiply the
    joined row through a Cartesian product.
    """

    selected_nodes, source_roles = inheritance_context_expressions(strategy)
    return (
        "WITH mv, "
        f"[_node IN {selected_nodes} WHERE _node IS NOT NULL] "
        "AS _selected_nodes, "
        f"[_role IN {source_roles} WHERE _role.node IS NOT NULL] "
        "AS _source_roles",
        "CALL (mv, _selected_nodes, _source_roles) {",
        "  UNWIND _source_roles AS _source_role",
        "  WITH mv, _selected_nodes, _source_role.node AS _source_node,",
        "       collect(_source_role.blocked_outgoing) AS _blocked_type_lists",
        "  WITH mv, _selected_nodes, _source_node,",
        "       reduce(_blocked_types = [], _types IN _blocked_type_lists |",
        "              _blocked_types + _types) AS _blocked_types",
        "  MATCH (_source_node)-[_source_rel]->(_external_node)",
        "  WHERE NOT (_external_node IN _selected_nodes)",
        f"    AND NOT ('{GENERATED_ROW_LABEL}' IN labels(_external_node))",
        "    AND NOT (type(_source_rel) IN _blocked_types)",
        "    AND type(_source_rel) IN $allowed_outgoing_relationship_types",
        "  CREATE (mv)-[_inherited_rel:$(type(_source_rel))]->(_external_node)",
        "  SET _inherited_rel = properties(_source_rel)",
        "}",
        "CALL (mv, _selected_nodes, _source_roles) {",
        "  UNWIND _source_roles AS _source_role",
        "  WITH mv, _selected_nodes, _source_role.node AS _source_node,",
        "       collect(_source_role.blocked_incoming) AS _blocked_type_lists",
        "  WITH mv, _selected_nodes, _source_node,",
        "       reduce(_blocked_types = [], _types IN _blocked_type_lists |",
        "              _blocked_types + _types) AS _blocked_types",
        "  MATCH (_external_node)-[_source_rel]->(_source_node)",
        "  WHERE NOT (_external_node IN _selected_nodes)",
        f"    AND NOT ('{GENERATED_ROW_LABEL}' IN labels(_external_node))",
        "    AND NOT (type(_source_rel) IN _blocked_types)",
        "    AND type(_source_rel) IN $allowed_incoming_relationship_types",
        "  CREATE (_external_node)-[_inherited_rel:$(type(_source_rel))]->(mv)",
        "  SET _inherited_rel = properties(_source_rel)",
        "}",
    )


def build_incoming_fanout_query(strategy: Strategy) -> str:
    """Measure actual external-node fanout for each eligible incoming type."""

    scope = ", ".join(alias_for(symbol) for symbol in strategy.symbols)
    selected_nodes, source_roles = inheritance_context_expressions(strategy)
    return "\n".join(
        (
            *build_match_clauses(strategy),
            f"CALL ({scope}) {{",
            "  WITH "
            f"[_node IN {selected_nodes} WHERE _node IS NOT NULL] "
            "AS _selected_nodes,",
            f"       [_role IN {source_roles} WHERE _role.node IS NOT NULL] "
            "AS _source_roles",
            "  UNWIND _source_roles AS _source_role",
            "  WITH _selected_nodes, _source_role.node AS _source_node,",
            "       collect(_source_role.blocked_incoming) "
            "AS _blocked_type_lists",
            "  WITH _selected_nodes, _source_node,",
            "       reduce(_blocked_types = [], "
            "_types IN _blocked_type_lists |",
            "              _blocked_types + _types) AS _blocked_types",
            "  MATCH (_external_node)-[_source_rel]->(_source_node)",
            "  WHERE NOT (_external_node IN _selected_nodes)",
            f"    AND NOT ('{GENERATED_ROW_LABEL}' IN labels(_external_node))",
            "    AND NOT (type(_source_rel) IN _blocked_types)",
            "  RETURN _external_node AS external_node,",
            "         type(_source_rel) AS relationship_type,",
            "         count(*) AS edge_count",
            "}",
            "WITH external_node, relationship_type,",
            "     sum(edge_count) AS actual_fanout",
            "RETURN relationship_type,",
            "       sum(actual_fanout) AS projected_relationships,",
            "       max(actual_fanout) AS max_fanout,",
            "       count(*) AS reached_nodes",
            "ORDER BY relationship_type",
        )
    )


def build_outgoing_fanout_query(strategy: Strategy) -> str:
    """Measure actual per-MV degree for each eligible outgoing type."""

    scope = ", ".join(alias_for(symbol) for symbol in strategy.symbols)
    selected_nodes, source_roles = inheritance_context_expressions(strategy)
    return "\n".join(
        (
            *build_match_clauses(strategy),
            f"CALL ({scope}) {{",
            "  WITH "
            f"[_node IN {selected_nodes} WHERE _node IS NOT NULL] "
            "AS _selected_nodes,",
            f"       [_role IN {source_roles} WHERE _role.node IS NOT NULL] "
            "AS _source_roles",
            "  UNWIND _source_roles AS _source_role",
            "  WITH _selected_nodes, _source_role.node AS _source_node,",
            "       collect(_source_role.blocked_outgoing) "
            "AS _blocked_type_lists",
            "  WITH _selected_nodes, _source_node,",
            "       reduce(_blocked_types = [], "
            "_types IN _blocked_type_lists |",
            "              _blocked_types + _types) AS _blocked_types",
            "  MATCH (_source_node)-[_source_rel]->(_external_node)",
            "  WHERE NOT (_external_node IN _selected_nodes)",
            f"    AND NOT ('{GENERATED_ROW_LABEL}' IN labels(_external_node))",
            "    AND NOT (type(_source_rel) IN _blocked_types)",
            "  WITH type(_source_rel) AS relationship_type,",
            "       count(*) AS degree_in_this_mv",
            "  RETURN relationship_type, degree_in_this_mv",
            "}",
            "RETURN relationship_type,",
            "       sum(degree_in_this_mv) AS projected_relationships,",
            "       max(degree_in_this_mv) AS max_fanout,",
            "       count(*) AS reached_nodes",
            "ORDER BY relationship_type",
        )
    )


def build_materialize_query(
    strategy: Strategy,
    batch_size: int,
    *,
    output_label: str | None = None,
) -> str:
    scope = ", ".join(alias_for(symbol) for symbol in strategy.symbols)
    candidate_label = quote_ident(output_label or strategy.name)
    generated_label = quote_ident(GENERATED_ROW_LABEL)

    subquery_lines = [
        f"  CREATE (mv:{generated_label}:{candidate_label})",
        *(f"  {clause}" for clause in property_copy_clauses(strategy)),
        f"  SET mv.{quote_ident(COMPONENT_PROPERTY)} = $component_role",
        *(f"  {clause}" for clause in relationship_inheritance_clauses(strategy)),
    ]

    return "\n".join(
        (
            *build_match_clauses(strategy),
            f"CALL ({scope}) {{",
            *subquery_lines,
            f"}} IN TRANSACTIONS OF {batch_size} ROWS",
        )
    )


# =========================
# Index mapping
# =========================


def output_property_name(
    strategy: Strategy,
    symbol: str,
    property_name: str,
) -> str:
    nation_count = sum(item in NATION_SYMBOLS for item in strategy.symbols)
    if symbol in NATION_SYMBOLS and nation_count > 1:
        return mapped_nation_property(symbol, property_name)
    return property_name


def collect_index_properties(strategy: Strategy) -> tuple[tuple[str, ...], ...]:
    properties: list[tuple[str, ...]] = []
    for symbol in strategy.symbols:
        primary_key = PK_PROPERTIES_BY_SYMBOL.get(symbol)
        if primary_key:
            mapped_key = tuple(
                output_property_name(strategy, symbol, property_name)
                for property_name in primary_key
            )
            properties.append(mapped_key)
            if len(mapped_key) > 1:
                properties.extend((property_name,) for property_name in mapped_key)

        for foreign_key in FK_PROPERTIES_BY_SYMBOL.get(symbol, ()):
            properties.append(
                tuple(
                    output_property_name(strategy, symbol, property_name)
                    for property_name in foreign_key
                )
            )

    unique: list[tuple[str, ...]] = []
    seen: set[tuple[str, ...]] = set()
    for property_group in properties:
        if property_group not in seen:
            seen.add(property_group)
            unique.append(property_group)
    return tuple(unique)


def strategy_index_name(
    strategy: Strategy,
    property_group: tuple[str, ...],
) -> str:
    property_suffix = "_".join(property_group)
    return safe_ident(
        f"{GENERATED_INDEX_PREFIX}{strategy.query_id}_{strategy.name}_{property_suffix}"
    )


def strategy_index_names(strategy: Strategy) -> tuple[str, ...]:
    return tuple(
        strategy_index_name(strategy, property_group)
        for property_group in collect_index_properties(strategy)
    )


def build_index_statements(strategy: Strategy) -> tuple[str, ...]:
    statements: list[str] = []
    label = quote_ident(strategy.name)
    for property_group, index_name in zip(
        collect_index_properties(strategy),
        strategy_index_names(strategy),
        strict=True,
    ):
        indexed_properties = ", ".join(
            f"n.{quote_ident(property_name)}" for property_name in property_group
        )
        statements.append(
            f"CREATE INDEX {quote_ident(index_name)} IF NOT EXISTS "
            f"FOR (n:{label}) ON ({indexed_properties})"
        )
    return tuple(statements)


# =========================
# Neo4j execution helpers
# =========================


def positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def nonnegative_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError(
            "must be a finite number that is zero or greater"
        )
    return value


VALID_QUERY_IDS = frozenset(query_id for query_id, _ in QUERY_GRAPHS)


def query_id_argument(raw: str) -> int:
    value = positive_int(raw)
    if value not in VALID_QUERY_IDS:
        choices = ", ".join(str(query_id) for query_id in sorted(VALID_QUERY_IDS))
        raise argparse.ArgumentTypeError(
            f"must be one of the join-query IDs: {choices}"
        )
    return value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create flattened Neo4j join nodes for all connected TPC-H "
            "denormalization candidates."
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
        default=os.getenv("NEO4J_DATABASE") or DATABASE_NAME,
        help=(
            "existing Neo4j database containing the normalized TPC-H graph "
            f"(default: NEO4J_DATABASE or {DATABASE_NAME})"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=positive_int,
        default=DEFAULT_BATCH_SIZE,
        help=(
            "joined rows committed per inner transaction "
            f"(default: {DEFAULT_BATCH_SIZE})"
        ),
    )
    parser.add_argument(
        "--max-estimated-rows",
        type=positive_int,
        default=DEFAULT_MAX_ESTIMATED_ROWS,
        help=(
            "skip a candidate when any EXPLAIN operator reaches this estimate "
            f"(default: {DEFAULT_MAX_ESTIMATED_ROWS})"
        ),
    )
    parser.add_argument(
        "--max-candidate-rows",
        type=positive_int,
        default=DEFAULT_MAX_CANDIDATE_ROWS,
        help=(
            "skip a candidate when a bounded exact count proves it has more "
            "than this many rows "
            f"(default: {DEFAULT_MAX_CANDIDATE_ROWS})"
        ),
    )
    parser.add_argument(
        "--query-timeout-seconds",
        type=nonnegative_float,
        default=DEFAULT_QUERY_TIMEOUT_SECONDS,
        help=(
            "server-side timeout for each count, inheritance preflight, and "
            "materialization; use 0 to disable "
            f"(default: {DEFAULT_QUERY_TIMEOUT_SECONDS:g})"
        ),
    )
    parser.add_argument(
        "--allow-unestimated",
        action="store_true",
        help=(
            "materialize a candidate even when EXPLAIN fails or returns no "
            "cardinality estimate (unsafe for large joins)"
        ),
    )
    parser.add_argument(
        "--inheritance-preflight",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "measure directional relationship fanout before materialization "
            "(default: enabled for this standalone builder)"
        ),
    )
    parser.add_argument(
        "--max-incoming-fanout",
        type=positive_int,
        default=DEFAULT_MAX_INCOMING_FANOUT,
        help=(
            "inherit t->MV relationship types only when no external source "
            "node would reach more than this many candidate rows "
            f"(default: {DEFAULT_MAX_INCOMING_FANOUT}; used only when "
            "inheritance preflight is enabled)"
        ),
    )
    parser.add_argument(
        "--max-outgoing-degree",
        type=positive_int,
        default=DEFAULT_MAX_OUTGOING_DEGREE,
        help=(
            "inherit MV->t relationship types only when no candidate row "
            "would create more than this many outgoing relationships "
            f"(default: {DEFAULT_MAX_OUTGOING_DEGREE}; used only when "
            "inheritance preflight is enabled)"
        ),
    )
    parser.add_argument(
        "--query-id",
        type=query_id_argument,
        action="append",
        help=(
            "materialize and rebuild only this TPC-H query ID; repeat for multiple IDs"
        ),
    )
    parser.add_argument(
        "--strategy",
        action="append",
        help=(
            "materialize and rebuild only this exact AUTO_JOIN_STRATEGY name; "
            "repeat for multiple strategies"
        ),
    )
    parser.add_argument(
        "--create-indexes",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "create candidate-specific RANGE indexes after materialization "
            "(default: disabled; use --create-indexes to enable)"
        ),
    )
    parser.add_argument(
        "--index-wait-seconds",
        type=positive_int,
        default=DEFAULT_INDEX_WAIT_SECONDS,
        help=(
            "maximum time to wait for candidate indexes to become ONLINE "
            f"(default: {DEFAULT_INDEX_WAIT_SECONDS})"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="enumerate candidates without connecting to or modifying Neo4j",
    )
    parser.add_argument(
        "--show-cypher",
        action="store_true",
        help="print generated MATCH/materialization Cypher in dry-run mode",
    )
    return parser.parse_args(argv)


def max_estimated_rows(plan: dict[str, Any] | None) -> float | None:
    if not plan:
        return None

    estimates: list[float] = []

    def visit(node: dict[str, Any]) -> None:
        arguments = node.get("args") or {}
        for key, raw_value in arguments.items():
            normalized_key = re.sub(r"[^a-z]", "", str(key).lower())
            if normalized_key != "estimatedrows":
                continue
            try:
                estimates.append(float(raw_value))
            except (TypeError, ValueError):
                pass

        for child in node.get("children") or ():
            if isinstance(child, dict):
                visit(child)

    visit(plan)
    return max(estimates) if estimates else None


def estimate_strategy_rows(session: Any, strategy: Strategy) -> float | None:
    result = session.run(
        "CYPHER replan=force EXPLAIN " + build_explain_query(strategy)
    )
    summary = result.consume()
    return max_estimated_rows(summary.plan)


def count_strategy_rows(
    session: Any,
    strategy: Strategy,
    max_candidate_rows: int,
    query_timeout_seconds: float,
) -> BoundedRowCount:
    """Return an exact count, or ``max_candidate_rows + 1`` when over limit."""

    from neo4j import Query

    probe_limit = max_candidate_rows + 1
    query_text = "CYPHER replan=force\n" + build_bounded_count_query(strategy)
    query = (
        Query(query_text, timeout=query_timeout_seconds)
        if query_timeout_seconds
        else query_text
    )
    record = session.run(
        query,
        probe_limit=probe_limit,
    ).single(strict=True)
    observed_rows = int(record["counted_rows"])
    if not 0 <= observed_rows <= probe_limit:
        raise RuntimeError(
            f"bounded count for {strategy.name} returned {observed_rows:,} "
            f"rows with probe_limit={probe_limit:,}"
        )
    return BoundedRowCount(
        observed_rows=observed_rows,
        max_rows=max_candidate_rows,
    )


def inheritance_allowed(
    direction: str,
    max_fanout: int,
    max_incoming_fanout: int,
    max_outgoing_degree: int,
) -> bool:
    """Apply the direction-specific cardinality limit to one relationship type."""

    if direction == "incoming":
        return max_fanout <= max_incoming_fanout
    if direction == "outgoing":
        return max_fanout <= max_outgoing_degree
    raise ValueError(f"unknown inheritance direction: {direction!r}")


def analyze_inheritance_policy(
    session: Any,
    strategy: Strategy,
    relationship_counts: dict[str, int],
    max_incoming_fanout: int,
    max_outgoing_degree: int,
    query_timeout_seconds: float,
    additional_hard_blocks: Sequence[InheritanceHardBlock] = (),
    allowed_incoming_types: Sequence[str] | None = None,
    allowed_outgoing_types: Sequence[str] | None = None,
) -> InheritancePolicy:
    """Measure candidate fanout and return all-or-nothing type allowlists."""

    from neo4j import Query

    stats: list[InheritanceStat] = []
    analyses = (
        ("incoming", build_incoming_fanout_query(strategy)),
        ("outgoing", build_outgoing_fanout_query(strategy)),
    )
    for direction, query_text in analyses:
        component_scope = (
            allowed_incoming_types
            if direction == "incoming"
            else allowed_outgoing_types
        )
        scoped_types = (
            None if component_scope is None else frozenset(component_scope)
        )
        query_text = "CYPHER replan=force\n" + query_text
        query = (
            Query(query_text, timeout=query_timeout_seconds)
            if query_timeout_seconds
            else query_text
        )
        for record in session.run(query):
            relationship_type = str(record["relationship_type"])
            if relationship_type not in relationship_counts:
                raise RuntimeError(
                    "fanout preflight found an unvalidated source relationship "
                    f"type: {relationship_type!r}"
                )

            projected_relationships = int(record["projected_relationships"])
            max_fanout = int(record["max_fanout"])
            hard_block_reason = inheritance_hard_block_reason(
                strategy,
                direction,
                relationship_type,
                additional_hard_blocks,
            )
            fanout_allowed = inheritance_allowed(
                direction,
                max_fanout,
                max_incoming_fanout,
                max_outgoing_degree,
            )
            scope_allowed = (
                scoped_types is None or relationship_type in scoped_types
            )
            stats.append(
                InheritanceStat(
                    direction=direction,
                    relationship_type=relationship_type,
                    baseline_relationships=relationship_counts[relationship_type],
                    projected_relationships=projected_relationships,
                    max_fanout=max_fanout,
                    reached_nodes=int(record["reached_nodes"]),
                    allowed=(
                        hard_block_reason is None
                        and scope_allowed
                        and fanout_allowed
                    ),
                    decision_reason=(
                        hard_block_reason
                        if hard_block_reason is not None
                        else (
                            FOLD_COMPONENT_SCOPE_REASON
                            if not scope_allowed
                            else (
                                None
                                if fanout_allowed
                                else FANOUT_LIMIT_EXCEEDED_REASON
                            )
                        )
                    ),
                )
            )

    direction_rank = {"incoming": 0, "outgoing": 1}
    return InheritancePolicy(
        max_incoming_fanout=max_incoming_fanout,
        max_outgoing_degree=max_outgoing_degree,
        stats=tuple(
            sorted(
                stats,
                key=lambda stat: (
                    direction_rank[stat.direction],
                    stat.relationship_type,
                ),
            )
        ),
    )


def unchecked_inheritance_policy(
    strategy: Strategy,
    relationship_counts: dict[str, int],
    additional_hard_blocks: Sequence[InheritanceHardBlock] = (),
    allowed_incoming_types: Sequence[str] | None = None,
    allowed_outgoing_types: Sequence[str] | None = None,
) -> InheritancePolicy:
    """Build schema-derived allowlists without running fanout queries."""

    selected_labels = {
        NODE_LABEL_BY_SYMBOL[symbol] for symbol in strategy.symbols
    }
    incoming_scope = (
        None
        if allowed_incoming_types is None
        else frozenset(allowed_incoming_types)
    )
    outgoing_scope = (
        None
        if allowed_outgoing_types is None
        else frozenset(allowed_outgoing_types)
    )
    allowed_incoming_types = tuple(
        sorted(
            relationship_type
            for _, relationship_type, target_label in SOURCE_RELATIONSHIP_RULES
            if target_label in selected_labels
            and relationship_type in relationship_counts
            and (
                incoming_scope is None
                or relationship_type in incoming_scope
            )
            and not inheritance_is_hard_blocked(
                strategy,
                "incoming",
                relationship_type,
                additional_hard_blocks,
            )
        )
    )
    allowed_outgoing_types = tuple(
        sorted(
            relationship_type
            for source_label, relationship_type, _ in SOURCE_RELATIONSHIP_RULES
            if source_label in selected_labels
            and relationship_type in relationship_counts
            and (
                outgoing_scope is None
                or relationship_type in outgoing_scope
            )
            and not inheritance_is_hard_blocked(
                strategy,
                "outgoing",
                relationship_type,
                additional_hard_blocks,
            )
        )
    )
    return InheritancePolicy(
        max_incoming_fanout=None,
        max_outgoing_degree=None,
        stats=(),
        mode=UNCHECKED_INHERITANCE_MODE,
        allowed_incoming_types_override=allowed_incoming_types,
        allowed_outgoing_types_override=allowed_outgoing_types,
    )


def print_inheritance_policy(
    policy: InheritancePolicy,
    strategy: Strategy | None = None,
    additional_hard_blocks: Sequence[InheritanceHardBlock] = (),
) -> None:
    if policy.mode == UNCHECKED_INHERITANCE_MODE:
        print(
            "  inheritance preflight skipped: validated boundary types are "
            "eligible after structural hard blocks"
        )
        return
    if not policy.stats:
        print("  inheritance preflight: no external relationships")
        return

    print(
        "  inheritance preflight: "
        f"incoming_limit={policy.max_incoming_fanout}, "
        f"outgoing_limit={policy.max_outgoing_degree}"
    )
    for stat in policy.stats:
        arrow = "t->MV" if stat.direction == "incoming" else "MV->t"
        amplification = (
            "N/A" if stat.amplification is None else f"{stat.amplification:.2f}x"
        )
        decision = "inherit" if stat.allowed else "suppress"
        if (
            strategy is not None
            and inheritance_is_hard_blocked(
                strategy,
                stat.direction,
                stat.relationship_type,
                additional_hard_blocks,
            )
        ):
            decision += f" ({stat.decision_reason or 'structural rule'})"
        print(
            f"    {arrow:<5} {stat.relationship_type:<24} "
            f"baseline={stat.baseline_relationships:,}, "
            f"projected={stat.projected_relationships:,}, "
            f"amplification={amplification}, "
            f"max_fanout={stat.max_fanout:,} -> {decision}"
        )


def print_strategy_sizing(sizing: StrategySizing) -> None:
    if sizing.estimated_peak_rows is None:
        print("  peak estimated rows: N/A")
        if sizing.estimate_message:
            print(f"    {sizing.estimate_message}", file=sys.stderr)
    else:
        print(f"  peak estimated rows: {sizing.estimated_peak_rows:,.0f}")

    if sizing.row_count is None:
        print("  candidate rows: N/A")
        if sizing.count_message:
            print(f"    {sizing.count_message}", file=sys.stderr)
    elif sizing.row_count.exceeded_limit:
        print(
            "  candidate rows: "
            f">{sizing.row_count.max_rows:,} "
            f"(bounded probe stopped at {sizing.row_count.observed_rows:,})"
        )
    else:
        print(f"  candidate rows: {sizing.row_count.observed_rows:,} (exact)")


def sizing_skip_reason(
    sizing: StrategySizing,
    max_estimated_rows: int,
    allow_unestimated: bool,
) -> tuple[str, str | None] | None:
    """Return the skip message and optional explosion-marker reason."""

    if sizing.row_count is None:
        return sizing.count_message or "candidate row count unavailable", None
    if sizing.row_count.exceeded_limit:
        return (
            "candidate row count exceeded the configured limit",
            "candidate_rows",
        )
    if sizing.estimated_peak_rows is None and not allow_unestimated:
        return sizing.estimate_message or "EXPLAIN estimate unavailable", None
    if (
        sizing.estimated_peak_rows is not None
        and sizing.estimated_peak_rows >= max_estimated_rows
    ):
        return (
            "peak estimated row count reached the configured limit",
            "estimated_peak_rows",
        )
    return None


def ensure_metadata_schema(session: Any) -> None:
    session.run(
        f"""
        CREATE CONSTRAINT {quote_ident(STRATEGY_CONSTRAINT)} IF NOT EXISTS
        FOR (strategy:{quote_ident(STRATEGY_LABEL)})
        REQUIRE strategy.name IS UNIQUE
        """
    ).consume()


def set_strategy_status(
    session: Any,
    strategy: Strategy,
    status: str,
    *,
    estimated_rows: float | None = None,
    row_count: int | None = None,
    relationship_count: int | None = None,
    message: str | None = None,
    inheritance_policy: InheritancePolicy | None = None,
    fold_components: Sequence[str] | None = None,
) -> None:
    session.run(
        f"""
        MERGE (strategy:{quote_ident(STRATEGY_LABEL)} {{name: $name}})
        ON CREATE SET strategy.created_at = datetime()
        SET strategy.query_id = $query_id,
            strategy.symbols = $symbols,
            strategy.edges = $edges,
            strategy.status = $status,
            strategy.estimated_rows = $estimated_rows,
            strategy.row_count = $row_count,
            strategy.relationship_count = $relationship_count,
            strategy.message = $message,
            strategy.inheritance_mode = $inheritance_mode,
            strategy.max_incoming_fanout = $max_incoming_fanout,
            strategy.max_outgoing_degree = $max_outgoing_degree,
            strategy.allowed_incoming_types = $allowed_incoming_types,
            strategy.allowed_outgoing_types = $allowed_outgoing_types,
            strategy.inheritance_stats = $inheritance_stats,
            strategy.fold_components = $fold_components,
            strategy.materialization_schema_version =
              $materialization_schema_version,
            strategy.experiment = $experiment,
            strategy.updated_at = datetime()
        """,
        name=strategy.name,
        query_id=strategy.query_id,
        symbols=list(strategy.symbols),
        edges=list(strategy.edge_names),
        status=status,
        estimated_rows=estimated_rows,
        row_count=row_count,
        relationship_count=relationship_count,
        message=message,
        inheritance_mode=(
            inheritance_policy.mode if inheritance_policy is not None else None
        ),
        max_incoming_fanout=(
            inheritance_policy.max_incoming_fanout
            if inheritance_policy is not None
            else None
        ),
        max_outgoing_degree=(
            inheritance_policy.max_outgoing_degree
            if inheritance_policy is not None
            else None
        ),
        allowed_incoming_types=(
            list(inheritance_policy.allowed_incoming_types)
            if inheritance_policy is not None
            else None
        ),
        allowed_outgoing_types=(
            list(inheritance_policy.allowed_outgoing_types)
            if inheritance_policy is not None
            else None
        ),
        inheritance_stats=(
            [stat.metadata_value for stat in inheritance_policy.stats]
            if inheritance_policy is not None
            else None
        ),
        fold_components=(
            list(fold_components) if fold_components is not None else None
        ),
        materialization_schema_version=MATERIALIZATION_SCHEMA_VERSION,
        experiment=EXPERIMENT_NAME,
    ).consume()


def drop_generated_indexes(
    session: Any,
    selected_query_ids: set[int] | None = None,
) -> int:
    records = list(session.run("SHOW INDEXES YIELD name RETURN name"))

    def is_selected(name: str) -> bool:
        if not name.startswith(GENERATED_INDEX_PREFIX):
            return False
        if selected_query_ids is None:
            return True
        return any(
            name.startswith(f"{GENERATED_INDEX_PREFIX}{query_id}_")
            for query_id in selected_query_ids
        )

    names = sorted(
        str(record["name"]) for record in records if is_selected(str(record["name"]))
    )
    for name in names:
        session.run(f"DROP INDEX {quote_ident(name)} IF EXISTS").consume()
    return len(names)


def database_node_labels(session: Any) -> frozenset[str]:
    """Return current node-label tokens without scanning the source graph."""

    return frozenset(
        str(record["label"])
        for record in session.run(
            "CALL db.labels() YIELD label RETURN label"
        )
    )


def _query_has_match(
    session: Any,
    query: str,
    **parameters: Any,
) -> bool:
    """Return whether a ``LIMIT 1`` existence query produced a row."""

    return bool(list(session.run(query, **parameters)))


def inspect_generated_artifacts(
    session: Any,
    selected_query_ids: set[int] | None = None,
    selected_strategies: Sequence[Strategy] | None = None,
) -> GeneratedArtifactPresence:
    """Check for generated artifacts in a cleanup scope before deleting them.

    Label tokens are checked first, so a fresh or partially cleaned database
    never executes a statically labelled query for a label that does not yet
    exist.  Exact strategy and query ownership tests use dynamic labels and
    properties to avoid the same warning for optional schema tokens.
    """

    strategies = (
        tuple(selected_strategies)
        if selected_strategies is not None
        else None
    )
    strategy_names = (
        sorted({strategy.name for strategy in strategies})
        if strategies is not None
        else []
    )
    query_ids = (
        sorted(selected_query_ids)
        if selected_query_ids is not None
        else None
    )
    if strategies is not None and not strategy_names:
        return GeneratedArtifactPresence(False, False, False)
    if strategies is None and query_ids == []:
        return GeneratedArtifactPresence(False, False, False)

    available_labels = database_node_labels(session)

    materialized_nodes = False
    if GENERATED_ROW_LABEL in available_labels:
        row_match = (
            f"MATCH (node:{quote_ident(GENERATED_ROW_LABEL)})"
        )
        if strategies is not None:
            materialized_nodes = _query_has_match(
                session,
                f"{row_match} "
                "WHERE any(node_label IN labels(node) "
                "WHERE node_label IN $strategy_names) "
                "RETURN 1 AS found LIMIT 1",
                strategy_names=strategy_names,
            )
        elif query_ids is not None:
            materialized_nodes = _query_has_match(
                session,
                f"{row_match} "
                "WHERE any(node_label IN labels(node) WHERE any("
                "strategy_prefix IN $strategy_prefixes "
                "WHERE node_label STARTS WITH strategy_prefix)) "
                "RETURN 1 AS found LIMIT 1",
                strategy_prefixes=[
                    f"q{query_id}_mv_" for query_id in query_ids
                ],
            )
        else:
            materialized_nodes = _query_has_match(
                session,
                f"{row_match} RETURN 1 AS found LIMIT 1",
            )

    strategy_nodes = False
    if STRATEGY_LABEL in available_labels:
        strategy_match = (
            f"MATCH (strategy:{quote_ident(STRATEGY_LABEL)})"
        )
        if strategies is not None:
            strategy_nodes = _query_has_match(
                session,
                f"{strategy_match} "
                "WHERE strategy[$property_name] IN $strategy_names "
                "RETURN 1 AS found LIMIT 1",
                property_name="name",
                strategy_names=strategy_names,
            )
        elif query_ids is not None:
            strategy_nodes = _query_has_match(
                session,
                f"{strategy_match} "
                "WHERE strategy[$property_name] IN $query_ids "
                "RETURN 1 AS found LIMIT 1",
                property_name="query_id",
                query_ids=query_ids,
            )
        else:
            strategy_nodes = _query_has_match(
                session,
                f"{strategy_match} RETURN 1 AS found LIMIT 1",
            )

    strategy_label_set = set(strategy_names)
    index_records = list(
        session.run(
            "SHOW INDEXES YIELD name, labelsOrTypes "
            "RETURN name, labelsOrTypes"
        )
    )

    def index_is_selected(record: Any) -> bool:
        name = str(record["name"])
        if not name.startswith(GENERATED_INDEX_PREFIX):
            return False
        if strategies is not None:
            return bool(
                strategy_label_set.intersection(
                    record["labelsOrTypes"] or ()
                )
            )
        if query_ids is not None:
            return any(
                name.startswith(f"{GENERATED_INDEX_PREFIX}{query_id}_")
                for query_id in query_ids
            )
        return True

    indexes = any(index_is_selected(record) for record in index_records)
    return GeneratedArtifactPresence(
        materialized_nodes,
        strategy_nodes,
        indexes,
    )


def drop_strategy_indexes(session: Any, strategy: Strategy) -> int:
    names = strategy_index_names(strategy)
    for name in names:
        session.run(f"DROP INDEX {quote_ident(name)} IF EXISTS").consume()
    return len(names)


def drop_indexes_for_strategies(
    session: Any,
    strategies: Sequence[Strategy],
) -> int:
    """Drop every current or stale generated index for exact strategies."""

    strategy_labels = {strategy.name for strategy in strategies}
    if not strategy_labels:
        return 0

    records = list(
        session.run(
            "SHOW INDEXES YIELD name, labelsOrTypes "
            "RETURN name, labelsOrTypes"
        )
    )
    names = sorted(
        str(record["name"])
        for record in records
        if str(record["name"]).startswith(GENERATED_INDEX_PREFIX)
        and strategy_labels.intersection(record["labelsOrTypes"] or ())
    )
    for name in names:
        session.run(f"DROP INDEX {quote_ident(name)} IF EXISTS").consume()
    return len(names)


def delete_rows_with_label(session: Any, label: str, batch_size: int) -> int:
    result = session.run(
        f"""
        MATCH (node:{quote_ident(label)})
        CALL (node) {{
          DETACH DELETE node
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """
    )
    return result.consume().counters.nodes_deleted


def delete_generated_rows_for_strategy(
    session: Any,
    strategy_name: str,
    batch_size: int,
) -> int:
    """Delete exact-strategy rows only when they are generated MV nodes."""

    if GENERATED_ROW_LABEL not in database_node_labels(session):
        return 0
    return _delete_generated_rows_for_strategies(
        session,
        (strategy_name,),
        batch_size,
    )


def _delete_generated_rows_for_strategies(
    session: Any,
    strategy_names: Sequence[str],
    batch_size: int,
) -> int:
    """Delete exact generated rows after their common label was verified."""

    selected_names = sorted(set(strategy_names))
    if not selected_names:
        return 0
    result = session.run(
        f"""
        MATCH (node:{quote_ident(GENERATED_ROW_LABEL)})
        WHERE any(
          node_label IN labels(node)
          WHERE node_label IN $strategy_names
        )
        CALL (node) {{
          DETACH DELETE node
        }} IN TRANSACTIONS OF {batch_size} ROWS
        """,
        strategy_names=selected_names,
    )
    return result.consume().counters.nodes_deleted


def delete_rows_for_query_ids(
    session: Any,
    query_ids: set[int],
    batch_size: int,
) -> int:
    """Delete every generated candidate owned by the selected queries.

    Prefix matching also removes candidates produced by an older candidate
    definition.  This prevents a filtered query-driven strategy from
    surviving a partial schema rebuild.
    """

    deleted_rows = 0
    generated_label = quote_ident(GENERATED_ROW_LABEL)
    for query_id in sorted(query_ids):
        result = session.run(
            f"""
            MATCH (node:{generated_label})
            WHERE any(
              label IN labels(node)
              WHERE label STARTS WITH $strategy_label_prefix
            )
            CALL (node) {{
              DETACH DELETE node
            }} IN TRANSACTIONS OF {batch_size} ROWS
            """,
            strategy_label_prefix=f"q{query_id}_mv_",
        )
        deleted_rows += result.consume().counters.nodes_deleted
    return deleted_rows


def cleanup_generated_artifacts(
    session: Any,
    batch_size: int,
    selected_query_ids: set[int] | None,
    selected_strategies: Sequence[Strategy] | None = None,
    *,
    known_presence: GeneratedArtifactPresence | None = None,
) -> tuple[int, int, int]:
    """Remove generated indexes, MV rows, and metadata in one exact scope."""

    presence = known_presence or inspect_generated_artifacts(
        session,
        selected_query_ids,
        selected_strategies,
    )
    if not presence.any:
        return 0, 0, 0

    if selected_strategies is not None:
        strategies = tuple(selected_strategies)
        dropped_indexes = (
            drop_indexes_for_strategies(session, strategies)
            if presence.indexes
            else 0
        )
        strategy_names = sorted({strategy.name for strategy in strategies})
        deleted_rows = (
            _delete_generated_rows_for_strategies(
                session,
                strategy_names,
                batch_size,
            )
            if presence.materialized_nodes
            else 0
        )
        if presence.strategy_nodes:
            result = session.run(
                f"MATCH (strategy:{quote_ident(STRATEGY_LABEL)}) "
                "WHERE strategy[$property_name] IN $strategy_names "
                "DETACH DELETE strategy",
                property_name="name",
                strategy_names=strategy_names,
            )
            deleted_strategies = result.consume().counters.nodes_deleted
        else:
            deleted_strategies = 0
        return deleted_rows, deleted_strategies, dropped_indexes

    dropped_indexes = (
        drop_generated_indexes(session, selected_query_ids)
        if presence.indexes
        else 0
    )

    if not presence.materialized_nodes:
        deleted_rows = 0
    elif selected_query_ids is None:
        deleted_rows = delete_rows_with_label(
            session,
            GENERATED_ROW_LABEL,
            batch_size,
        )
    else:
        deleted_rows = delete_rows_for_query_ids(
            session,
            selected_query_ids,
            batch_size,
        )

    if not presence.strategy_nodes:
        deleted_strategies = 0
    else:
        metadata_match = f"MATCH (strategy:{quote_ident(STRATEGY_LABEL)})"
        if selected_query_ids is None:
            result = session.run(f"{metadata_match} DETACH DELETE strategy")
        else:
            result = session.run(
                f"{metadata_match} "
                "WHERE strategy[$property_name] IN $query_ids "
                "DETACH DELETE strategy",
                property_name="query_id",
                query_ids=sorted(selected_query_ids),
            )
        strategy_summary = result.consume()
        deleted_strategies = strategy_summary.counters.nodes_deleted

    return deleted_rows, deleted_strategies, dropped_indexes


def validate_source_graph(
    session: Any,
) -> tuple[dict[str, int], dict[str, int]]:
    node_counts: dict[str, int] = {}
    for label in dict.fromkeys(NODE_LABEL_BY_SYMBOL.values()):
        record = session.run(
            f"MATCH (node:{quote_ident(label)}) RETURN count(node) AS node_count"
        ).single(strict=True)
        count = int(record["node_count"])
        node_counts[label] = count
        if count == 0:
            raise RuntimeError(
                f"source label {label!r} has no nodes; "
                "import the normalized TPC-H graph before materializing joins"
            )

    relationship_counts: dict[str, int] = {}
    for source_label, relationship_type, target_label in SOURCE_RELATIONSHIP_RULES:
        record = session.run(
            f"""
            MATCH (source:{quote_ident(source_label)})
            OPTIONAL MATCH
              (source)-[relationship:{quote_ident(relationship_type)}]->
              (:{quote_ident(target_label)})
            WITH source, count(relationship) AS degree
            RETURN count(source) AS source_count,
                   coalesce(sum(degree), 0) AS relationship_count,
                   coalesce(
                     sum(CASE WHEN degree = 1 THEN 0 ELSE 1 END),
                     0
                   ) AS invalid_source_count
            """
        ).single(strict=True)
        source_count = int(record["source_count"])
        relationship_count = int(record["relationship_count"])
        invalid_source_count = int(record["invalid_source_count"])
        relationship_counts[relationship_type] = relationship_count

        if source_count != node_counts[source_label] or invalid_source_count:
            raise RuntimeError(
                f"source relationship {relationship_type!r} is incomplete or "
                f"non-unique: source_nodes={source_count}, "
                f"relationships={relationship_count}, "
                f"invalid_source_nodes={invalid_source_count}; rerun the "
                "TPC-H importer before materializing joins"
            )

    return node_counts, relationship_counts


def write_exceed_mark(
    strategy: Strategy,
    sizing: StrategySizing,
    reason: str,
) -> Path:
    EXCEED_DIR.mkdir(parents=True, exist_ok=True)
    symbols = "_".join(strategy.symbols)
    path = EXCEED_DIR / safe_ident(f"Q{strategy.query_id}_{symbols}_exceed")
    estimated_rows = (
        "N/A"
        if sizing.estimated_peak_rows is None
        else str(sizing.estimated_peak_rows)
    )
    counted_rows = (
        "N/A"
        if sizing.row_count is None
        else str(sizing.row_count.observed_rows)
    )
    count_is_exact = (
        "N/A"
        if sizing.row_count is None
        else str(not sizing.row_count.exceeded_limit).lower()
    )
    max_candidate_rows = (
        "N/A"
        if sizing.row_count is None
        else str(sizing.row_count.max_rows)
    )
    path.write_text(
        (
            f"strategy={strategy.name}\n"
            f"reason={reason}\n"
            f"estimated_peak_rows={estimated_rows}\n"
            f"counted_rows={counted_rows}\n"
            f"count_is_exact={count_is_exact}\n"
            f"max_candidate_rows={max_candidate_rows}\n"
        ),
        encoding="utf-8",
    )
    return path


def create_strategy_indexes(
    session: Any,
    strategy: Strategy,
    index_wait_seconds: int,
) -> int:
    statements = build_index_statements(strategy)
    for statement in statements:
        session.run(statement).consume()
    if statements:
        session.run(
            "CALL db.awaitIndexes($timeout_seconds)",
            timeout_seconds=index_wait_seconds,
        ).consume()
    return len(statements)


def materialize_strategy(
    session: Any,
    strategy: Strategy,
    inheritance_policy: InheritancePolicy,
    batch_size: int,
    query_timeout_seconds: float,
    *,
    output_label: str | None = None,
    component_role: str = PRIMARY_COMPONENT_ROLE,
) -> tuple[int, int]:
    from neo4j import Query

    query_text = build_materialize_query(
        strategy,
        batch_size,
        output_label=output_label,
    )
    query = (
        Query(query_text, timeout=query_timeout_seconds)
        if query_timeout_seconds
        else query_text
    )
    result = session.run(
        query,
        allowed_incoming_relationship_types=list(
            inheritance_policy.allowed_incoming_types
        ),
        allowed_outgoing_relationship_types=list(
            inheritance_policy.allowed_outgoing_types
        ),
        component_role=component_role,
    )
    summary = result.consume()
    return (
        summary.counters.nodes_created,
        summary.counters.relationships_created,
    )


def print_dry_run(
    strategies: Sequence[Strategy],
    batch_size: int,
    show_cypher: bool,
) -> None:
    counts: dict[int, int] = {}
    component_count = 0
    for strategy in strategies:
        counts[strategy.query_id] = counts.get(strategy.query_id, 0) + 1
        components = strategy_fold_components(strategy)
        component_count += len(components)
        print(
            f"{strategy.name:<75} symbols={strategy.symbols} "
            f"edges={strategy.edges} components={len(components)}"
        )
        for component in components:
            configured_incoming = (
                "all validated boundary types"
                if component.allowed_incoming_types is None
                else repr(component.allowed_incoming_types)
            )
            configured_outgoing = (
                "all validated boundary types"
                if component.allowed_outgoing_types is None
                else repr(component.allowed_outgoing_types)
            )
            hard_blocks = tuple(
                (
                    block.direction,
                    block.relationship_type,
                    block.reason,
                )
                for block in component.hard_blocks
            )
            if not component.is_primary:
                print(
                    f"  + {component.role}: symbols={component.strategy.symbols} "
                    f"edges={component.strategy.edges}"
                )
            print(
                f"    inheritance scope: incoming={configured_incoming}; "
                f"outgoing={configured_outgoing}; "
                f"hard_blocks={hard_blocks or 'none'}"
            )
            if show_cypher:
                print("-" * 100)
                print(f"component_role={component.role}")
                print(
                    build_materialize_query(
                        component.strategy,
                        batch_size,
                        output_label=strategy.name,
                    )
                )
                print("-" * 100)

    summary = ", ".join(
        f"Q{query_id}={count}" for query_id, count in sorted(counts.items())
    )
    print(
        f"\nCandidates: {len(strategies)} ({summary}); "
        f"physical components: {component_count}"
    )


def collect_strategy_sizing(
    session: Any,
    strategies: Sequence[Strategy],
    max_candidate_rows: int,
    query_timeout_seconds: float,
) -> dict[Strategy, StrategySizing]:
    """Collect every candidate's plan estimate and bounded count before writes."""

    sizing_by_strategy: dict[Strategy, StrategySizing] = {}
    print("\nPhase 1/3: sizing every candidate before creating any MV")
    for number, strategy in enumerate(strategies, start=1):
        print(
            f"\n[sizing {number}/{len(strategies)}] "
            f"{strategy.name} symbols={strategy.symbols}"
        )

        try:
            estimated_peak_rows = estimate_strategy_rows(session, strategy)
        except Exception as exc:
            estimated_peak_rows = None
            estimate_message = f"EXPLAIN failed ({type(exc).__name__}: {exc})"
        else:
            estimate_message = (
                "EXPLAIN returned no cardinality estimate"
                if estimated_peak_rows is None
                else None
            )

        try:
            row_count = count_strategy_rows(
                session,
                strategy,
                max_candidate_rows,
                query_timeout_seconds,
            )
        except Exception as exc:
            row_count = None
            count_message = (
                "bounded candidate count failed "
                f"({type(exc).__name__}: {exc})"
            )
        else:
            count_message = None

        sizing = StrategySizing(
            estimated_peak_rows=estimated_peak_rows,
            estimate_message=estimate_message,
            row_count=row_count,
            count_message=count_message,
        )
        sizing_by_strategy[strategy] = sizing
        print_strategy_sizing(sizing)

    return sizing_by_strategy


def collect_fold_component_sizing(
    session: Any,
    components: Sequence[FoldComponent],
    max_candidate_rows: int,
    query_timeout_seconds: float,
) -> dict[FoldComponent, StrategySizing]:
    """Size every physical component before any component is written."""

    sizing_by_component: dict[FoldComponent, StrategySizing] = {}
    print("\nPhase 1/3: sizing every fold component before creating any MV")
    for number, component in enumerate(components, start=1):
        component_strategy = component.strategy
        print(
            f"\n[sizing {number}/{len(components)}] "
            f"{component.display_name} symbols={component_strategy.symbols}"
        )

        try:
            estimated_peak_rows = estimate_strategy_rows(
                session,
                component_strategy,
            )
        except Exception as exc:
            estimated_peak_rows = None
            estimate_message = f"EXPLAIN failed ({type(exc).__name__}: {exc})"
        else:
            estimate_message = (
                "EXPLAIN returned no cardinality estimate"
                if estimated_peak_rows is None
                else None
            )

        try:
            row_count = count_strategy_rows(
                session,
                component_strategy,
                max_candidate_rows,
                query_timeout_seconds,
            )
        except Exception as exc:
            row_count = None
            count_message = (
                "bounded candidate count failed "
                f"({type(exc).__name__}: {exc})"
            )
        else:
            count_message = None

        sizing = StrategySizing(
            estimated_peak_rows=estimated_peak_rows,
            estimate_message=estimate_message,
            row_count=row_count,
            count_message=count_message,
        )
        sizing_by_component[component] = sizing
        print_strategy_sizing(sizing)

    return sizing_by_component


def fold_component_metadata_value(
    component: FoldComponent,
    sizing: StrategySizing,
    policy: InheritancePolicy | None = None,
    *,
    created_rows: int | None = None,
    created_relationships: int | None = None,
) -> str:
    """Serialize one component for strategy metadata without nested values."""

    row_count = sizing.row_count
    expected_rows = row_count.exact_rows if row_count is not None else None
    hard_blocks = ",".join(
        f"{block.direction}:{block.relationship_type}:{block.reason}"
        for block in component.hard_blocks
    ) or "none"
    configured_incoming = (
        "all"
        if component.allowed_incoming_types is None
        else ",".join(component.allowed_incoming_types) or "none"
    )
    configured_outgoing = (
        "all"
        if component.allowed_outgoing_types is None
        else ",".join(component.allowed_outgoing_types) or "none"
    )
    incoming = (
        ",".join(policy.allowed_incoming_types)
        if policy is not None
        else "not_preflighted"
    )
    outgoing = (
        ",".join(policy.allowed_outgoing_types)
        if policy is not None
        else "not_preflighted"
    )
    inheritance_decisions = (
        ";".join(
            (
                f"{stat.direction}:{stat.relationship_type}:"
                f"projected={stat.projected_relationships}:"
                f"max_fanout={stat.max_fanout}:"
                f"allowed={str(stat.allowed).lower()}:"
                f"reason={stat.decision_reason or 'allowed'}"
            )
            for stat in policy.stats
        )
        if policy is not None and policy.stats
        else "none"
    )
    estimated_peak_rows = (
        sizing.estimated_peak_rows
        if sizing.estimated_peak_rows is not None
        else "N/A"
    )
    return (
        f"role={component.role}|"
        f"symbols={','.join(component.strategy.symbols)}|"
        f"edges={','.join(component.strategy.edge_names)}|"
        f"estimated_peak_rows={estimated_peak_rows}|"
        f"expected_rows={expected_rows if expected_rows is not None else 'N/A'}|"
        f"created_rows={created_rows if created_rows is not None else 'N/A'}|"
        "created_relationships="
        f"{created_relationships if created_relationships is not None else 'N/A'}|"
        f"allowed_incoming={incoming or 'none'}|"
        f"allowed_outgoing={outgoing or 'none'}|"
        f"configured_incoming={configured_incoming}|"
        f"configured_outgoing={configured_outgoing}|"
        f"hard_blocks={hard_blocks}|"
        f"inheritance_decisions={inheritance_decisions}"
    )


def materialize_strategies_in_session(
    session: Any,
    args: argparse.Namespace,
    strategies: Sequence[Strategy],
    relationship_counts: dict[str, int],
) -> MaterializationSummary:
    """Size, preflight, and build complete fold configurations."""

    ensure_metadata_schema(session)
    ready = 0
    skipped = 0
    total_rows = 0
    total_relationships = 0
    total_indexes = 0
    total_creation_wall_ms = 0.0
    creation_wall_ms_by_strategy: list[tuple[str, float]] = []

    components_by_strategy = {
        strategy: strategy_fold_components(strategy) for strategy in strategies
    }
    all_components = tuple(
        component
        for strategy in strategies
        for component in components_by_strategy[strategy]
    )

    sizing_by_component = collect_fold_component_sizing(
        session,
        all_components,
        args.max_candidate_rows,
        args.query_timeout_seconds,
    )

    def estimated_peak_for(strategy: Strategy) -> float | None:
        estimates = [
            sizing_by_component[component].estimated_peak_rows
            for component in components_by_strategy[strategy]
            if sizing_by_component[component].estimated_peak_rows is not None
        ]
        return max(estimates) if estimates else None

    def component_metadata(
        strategy: Strategy,
        policies: dict[FoldComponent, InheritancePolicy] | None = None,
        created: dict[FoldComponent, tuple[int, int]] | None = None,
    ) -> list[str]:
        return [
            fold_component_metadata_value(
                component,
                sizing_by_component[component],
                policies.get(component) if policies is not None else None,
                created_rows=(
                    created[component][0]
                    if created is not None and component in created
                    else None
                ),
                created_relationships=(
                    created[component][1]
                    if created is not None and component in created
                    else None
                ),
            )
            for component in components_by_strategy[strategy]
        ]

    eligible_strategies: list[Strategy] = []
    for strategy in strategies:
        first_skip: tuple[FoldComponent, str, str | None] | None = None
        for component in components_by_strategy[strategy]:
            sizing = sizing_by_component[component]
            skip = sizing_skip_reason(
                sizing,
                args.max_estimated_rows,
                args.allow_unestimated,
            )
            if skip is None:
                if sizing.estimated_peak_rows is None:
                    print(
                        f"  WARNING: {component.display_name}: "
                        f"{sizing.estimate_message}; continuing because "
                        "--allow-unestimated was supplied",
                        file=sys.stderr,
                    )
                continue
            if first_skip is None:
                first_skip = (component, skip[0], skip[1])

        if first_skip is None:
            eligible_strategies.append(strategy)
            continue

        component, message, marker_reason = first_skip
        sizing = sizing_by_component[component]
        message = f"{component.display_name}: {message}"
        if marker_reason is not None:
            mark = write_exceed_mark(
                strategy,
                sizing,
                f"{marker_reason}_{component.role}",
            )
            message = f"{message}; marker={mark.name}"
        set_strategy_status(
            session,
            strategy,
            "skipped",
            estimated_rows=estimated_peak_for(strategy),
            message=message,
            fold_components=component_metadata(strategy),
        )
        skipped += 1
        print(f"  skipped {strategy.name}: {message}", file=sys.stderr)

    eligible_component_count = sum(
        len(components_by_strategy[item])
        for item in eligible_strategies
    )
    print(
        "\nSizing complete before MV creation: "
        f"eligible_strategies={len(eligible_strategies)}, "
        f"eligible_components={eligible_component_count}, "
        f"skipped={skipped}"
    )

    inheritance_by_component: dict[FoldComponent, InheritancePolicy] = {}
    eligible_components = tuple(
        component
        for strategy in eligible_strategies
        for component in components_by_strategy[strategy]
    )
    run_inheritance_preflight = getattr(args, "inheritance_preflight", True)
    if run_inheritance_preflight:
        print("\nPhase 2/3: inheritance preflight for every eligible component")
        for number, component in enumerate(eligible_components, start=1):
            sizing = sizing_by_component[component]
            row_count = sizing.row_count
            expected_rows = (
                row_count.exact_rows if row_count is not None else None
            )
            if expected_rows is None:
                raise AssertionError(
                    f"eligible component {component.display_name} has no exact "
                    "row count"
                )
            print(
                f"\n[inheritance {number}/{len(eligible_components)}] "
                f"{component.display_name} candidate_rows={expected_rows:,}"
            )
            try:
                inheritance_policy = analyze_inheritance_policy(
                    session,
                    component.strategy,
                    relationship_counts,
                    args.max_incoming_fanout,
                    args.max_outgoing_degree,
                    args.query_timeout_seconds,
                    component.hard_blocks,
                    component.allowed_incoming_types,
                    component.allowed_outgoing_types,
                )
            except (Exception, KeyboardInterrupt) as exc:
                status = (
                    "interrupted"
                    if isinstance(exc, KeyboardInterrupt)
                    else "failed"
                )
                try:
                    set_strategy_status(
                        session,
                        component.owner,
                        status,
                        estimated_rows=estimated_peak_for(component.owner),
                        message=(
                            f"{component.display_name} inheritance preflight "
                            "failed before MV creation "
                            f"({type(exc).__name__}: {exc})"
                        ),
                        fold_components=component_metadata(component.owner),
                    )
                except Exception as status_exc:
                    print(
                        "  WARNING: could not update preflight failure "
                        f"status ({type(status_exc).__name__}: {status_exc})",
                        file=sys.stderr,
                    )
                raise
            inheritance_by_component[component] = inheritance_policy
            print_inheritance_policy(
                inheritance_policy,
                component.strategy,
                component.hard_blocks,
            )

        for strategy in eligible_strategies:
            primary = components_by_strategy[strategy][0]
            set_strategy_status(
                session,
                strategy,
                "preflighted",
                estimated_rows=estimated_peak_for(strategy),
                inheritance_policy=inheritance_by_component[primary],
                fold_components=component_metadata(
                    strategy,
                    inheritance_by_component,
                ),
            )
    else:
        print("\nPhase 2/3: inheritance preflight skipped")
        print(
            "  Fanout limits are not applied; each component keeps only its "
            "validated boundary scope after structural hard blocks."
        )
        if any(len(components_by_strategy[item]) > 1 for item in eligible_strategies):
            print(
                "  Fold closure: Nation-Region companions inherit only the "
                "opposite still-live parent role; the parent's occurrence in "
                "the primary component is structurally suppressed."
            )
        for component in eligible_components:
            inheritance_policy = unchecked_inheritance_policy(
                component.strategy,
                relationship_counts,
                component.hard_blocks,
                component.allowed_incoming_types,
                component.allowed_outgoing_types,
            )
            inheritance_by_component[component] = inheritance_policy
            if not component.is_primary:
                print(f"\n[inheritance scope] {component.display_name}")
                print_inheritance_policy(
                    inheritance_policy,
                    component.strategy,
                    component.hard_blocks,
                )

        for strategy in eligible_strategies:
            primary = components_by_strategy[strategy][0]
            set_strategy_status(
                session,
                strategy,
                "preflight_skipped",
                estimated_rows=estimated_peak_for(strategy),
                inheritance_policy=inheritance_by_component[primary],
                fold_components=component_metadata(
                    strategy,
                    inheritance_by_component,
                ),
            )

    print("\nPhase 3/3: materializing complete fold configurations")
    for number, strategy in enumerate(eligible_strategies, start=1):
        components = components_by_strategy[strategy]
        expected_total_rows = 0
        for component in components:
            row_count = sizing_by_component[component].row_count
            expected_rows = (
                row_count.exact_rows if row_count is not None else None
            )
            if expected_rows is None:
                raise AssertionError(
                    f"eligible component {component.display_name} has no exact "
                    "row count"
                )
            expected_total_rows += expected_rows

        primary = components[0]
        primary_policy = inheritance_by_component[primary]
        print(
            f"\n[build {number}/{len(eligible_strategies)}] "
            f"{strategy.name} components={len(components)} "
            f"expected_nodes={expected_total_rows:,}"
        )
        set_strategy_status(
            session,
            strategy,
            "building",
            estimated_rows=estimated_peak_for(strategy),
            inheritance_policy=primary_policy,
            fold_components=component_metadata(
                strategy,
                inheritance_by_component,
            ),
        )

        creation_started_ns = perf_counter_ns()
        created_by_component: dict[FoldComponent, tuple[int, int]] = {}
        created_rows = 0
        created_relationships = 0
        try:
            for component in components:
                sizing = sizing_by_component[component]
                row_count = sizing.row_count
                expected_rows = (
                    row_count.exact_rows if row_count is not None else None
                )
                if expected_rows is None:
                    raise AssertionError(
                        f"component {component.display_name} lost its exact "
                        "row count"
                    )
                component_rows, component_relationships = materialize_strategy(
                    session,
                    component.strategy,
                    inheritance_by_component[component],
                    args.batch_size,
                    args.query_timeout_seconds,
                    output_label=strategy.name,
                    component_role=component.role,
                )
                if component_rows != expected_rows:
                    raise RuntimeError(
                        f"{component.display_name} created {component_rows:,} "
                        f"nodes, but its preflight count was {expected_rows:,}"
                    )
                created_by_component[component] = (
                    component_rows,
                    component_relationships,
                )
                created_rows += component_rows
                created_relationships += component_relationships
                print(
                    f"  {component.role}: created {component_rows:,} nodes, "
                    f"{component_relationships:,} inherited relationships"
                )

            creation_wall_ms = (
                perf_counter_ns() - creation_started_ns
            ) / 1_000_000
            if created_rows != expected_total_rows:
                raise RuntimeError(
                    f"{strategy.name} created {created_rows:,} component nodes, "
                    f"but the configuration count was {expected_total_rows:,}"
                )
            index_count = (
                create_strategy_indexes(
                    session,
                    strategy,
                    args.index_wait_seconds,
                )
                if args.create_indexes
                else 0
            )
            set_strategy_status(
                session,
                strategy,
                "ready",
                estimated_rows=estimated_peak_for(strategy),
                row_count=created_rows,
                relationship_count=created_relationships,
                inheritance_policy=primary_policy,
                fold_components=component_metadata(
                    strategy,
                    inheritance_by_component,
                    created_by_component,
                ),
            )
        except (Exception, KeyboardInterrupt) as exc:
            cleanup_notes: list[str] = []
            try:
                partial_rows = delete_generated_rows_for_strategy(
                    session,
                    strategy.name,
                    args.batch_size,
                )
                cleanup_notes.append(f"removed_partial_rows={partial_rows}")
            except Exception as cleanup_exc:
                cleanup_notes.append(
                    "partial_row_cleanup_failed="
                    f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                )

            try:
                drop_strategy_indexes(session, strategy)
                cleanup_notes.append("candidate_indexes_removed")
            except Exception as cleanup_exc:
                cleanup_notes.append(
                    "index_cleanup_failed="
                    f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                )

            status = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed"
            failure_message = f"{type(exc).__name__}: {exc}; " + "; ".join(
                cleanup_notes
            )
            try:
                set_strategy_status(
                    session,
                    strategy,
                    status,
                    estimated_rows=estimated_peak_for(strategy),
                    message=failure_message,
                    inheritance_policy=primary_policy,
                    fold_components=component_metadata(
                        strategy,
                        inheritance_by_component,
                    ),
                )
            except Exception as status_exc:
                print(
                    "  WARNING: could not update failure status "
                    f"({type(status_exc).__name__}: {status_exc})",
                    file=sys.stderr,
                )
            raise

        ready += 1
        total_rows += created_rows
        total_relationships += created_relationships
        total_indexes += index_count
        total_creation_wall_ms += creation_wall_ms
        creation_wall_ms_by_strategy.append(
            (strategy.name, creation_wall_ms)
        )
        print(
            f"  configuration total: {created_rows:,} nodes, "
            f"{created_relationships:,} inherited relationships; "
            f"indexes={index_count:,}; "
            f"creation_time={creation_wall_ms / 1_000:.3f} s"
        )

    return MaterializationSummary(
        ready=ready,
        skipped=skipped,
        materialized_nodes=total_rows,
        inherited_relationships=total_relationships,
        indexes=total_indexes,
        creation_wall_ms=total_creation_wall_ms,
        creation_wall_ms_by_strategy=tuple(
            creation_wall_ms_by_strategy
        ),
    )


def run(args: argparse.Namespace, password: str) -> None:
    try:
        from neo4j import GraphDatabase, WRITE_ACCESS
    except ImportError as exc:
        raise RuntimeError(
            "The Neo4j Python driver is not installed. "
            "Install it with: pip install neo4j"
        ) from exc

    selected_query_ids = set(args.query_id) if args.query_id else None
    selected_strategy_names = (
        set(args.strategy) if getattr(args, "strategy", None) else None
    )
    strategies = select_strategies(
        selected_query_ids,
        selected_strategy_names,
    )

    print(f"Connecting to {args.uri} as {args.user!r}; database={args.database!r}")
    print(
        f"Preparing {len(strategies)} candidate strategies; "
        f"batch_size={args.batch_size:,}; "
        f"max_estimated_rows={args.max_estimated_rows:,}; "
        f"max_candidate_rows={args.max_candidate_rows:,}; "
        f"max_incoming_fanout={args.max_incoming_fanout}; "
        f"max_outgoing_degree={args.max_outgoing_degree}; "
        f"query_timeout={args.query_timeout_seconds:g}s"
    )

    with GraphDatabase.driver(args.uri, auth=(args.user, password)) as driver:
        driver.verify_connectivity()
        with driver.session(
            database=args.database,
            default_access_mode=WRITE_ACCESS,
        ) as session:
            source_counts, relationship_counts = validate_source_graph(session)
            print(
                "Source nodes: "
                + ", ".join(
                    f"{label}={count:,}" for label, count in source_counts.items()
                )
            )
            print(
                "Source relationships: "
                + ", ".join(
                    f"{relationship_type}={count:,}"
                    for relationship_type, count in relationship_counts.items()
                )
            )

            exact_cleanup = strategies if selected_strategy_names is not None else None
            deleted_rows, deleted_strategies, dropped_indexes = (
                cleanup_generated_artifacts(
                    session,
                    args.batch_size,
                    selected_query_ids,
                    exact_cleanup,
                )
            )
            print(
                "Cleanup: "
                f"{deleted_rows:,} materialized nodes, "
                f"{deleted_strategies:,} strategy nodes, "
                f"{dropped_indexes:,} indexes"
            )
            if selected_query_ids is not None or selected_strategy_names is not None:
                print(
                    "WARNING: filtered cleanup retains unselected MVs; their "
                    "same-type inherited relationships may still inflate "
                    "EXPLAIN peak estimates. Bounded candidate counts remain "
                    "label-qualified.",
                    file=sys.stderr,
                )

            summary = materialize_strategies_in_session(
                session,
                args,
                strategies,
                relationship_counts,
            )

    print(
        "\nAll done: "
        f"ready={summary.ready}, skipped={summary.skipped}, "
        f"materialized_nodes={summary.materialized_nodes:,}, "
        f"inherited_relationships={summary.inherited_relationships:,}, "
        f"indexes={summary.indexes:,}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    selected_query_ids = set(args.query_id) if args.query_id else None
    selected_strategy_names = set(args.strategy) if args.strategy else None

    try:
        strategies = select_strategies(
            selected_query_ids,
            selected_strategy_names,
        )
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.dry_run:
        print_dry_run(strategies, args.batch_size, args.show_cypher)
        return 0

    password = os.getenv("NEO4J_PASSWORD") or DEBUG_NEO4J_PASSWORD
    if password is None:
        password = getpass(f"Neo4j password for {args.user}: ")

    try:
        run(args, password)
    except KeyboardInterrupt:
        print("\nMaterialization interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        cause = exc.__cause__
        if cause is not None:
            print(f"CAUSE: {cause}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
