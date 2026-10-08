#!/usr/bin/env python3
"""Run paired TPC-H updates for every subset of the eight foreign-key edges.

Temporary denormalized schemas are compared with the retained normalized graph;
normalization rebuilds nodes, relationships, and indexes before cleanup.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import statistics
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from getpass import getpass
from pathlib import Path
from typing import Any, Iterable
DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD: str | None = ""
DEFAULT_DATABASE = "tpch-sf-005"
DEFAULT_UPDATE_COUNT = 50       # 50
DEFAULT_UPDATE_REPEATS = 10     # 10
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_CLEANUP_BATCH_SIZE = 1_000
DEFAULT_INDEX_TIMEOUT = 3000
DEFAULT_RANDOM_SEED = 20260818
# None runs all 256 configurations; for example, (2, 12, 96) runs only three.
DEFAULT_MASKS: tuple[int, ...] | None = None
# None writes to results/new_redundancy/<database>/results.json.
DEFAULT_OUTPUT: Path | None = (
    Path(__file__).resolve().parents[1]
    / "results"
    / "test"
    / DEFAULT_DATABASE
    / "new_redundancy_results.json"
)
ARTIFACT_LABEL = "FOLD_ARTIFACT"
NORMALIZED_LABEL = "NORMALIZED_ARTIFACT"
EXPERIMENT_PROPERTIES = ("nr_run_id", "nr_config", "nr_recipe")
@dataclass(frozen=True)
class Table:
    code: str
    label: str
    key: tuple[str, ...]
    columns: tuple[str, ...]
    update_property: str | None
@dataclass(frozen=True)
class Edge:
    code: str
    source: str
    target: str
    relationship: str
    source_fields: tuple[str, ...]
TABLES = (
    Table("L", "LINEITEM", ("l_orderkey", "l_linenumber"),
          ("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
           "l_quantity", "l_extendedprice", "l_discount", "l_tax",
           "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
           "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"), None),
    Table("PS", "PARTSUPP", ("ps_partkey", "ps_suppkey"),
          ("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost",
           "ps_comment"), "ps_comment"),
    Table("P", "PART", ("p_partkey",),
          ("p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size",
           "p_container", "p_retailprice", "p_comment"), "p_comment"),
    Table("O", "ORDERS", ("o_orderkey",),
          ("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
           "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority",
           "o_comment"), "o_comment"),
    Table("C", "CUSTOMER", ("c_custkey",),
          ("c_custkey", "c_name", "c_address", "c_nationkey", "c_phone",
           "c_acctbal", "c_mktsegment", "c_comment"), "c_comment"),
    Table("S", "SUPPLIER", ("s_suppkey",),
          ("s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone",
           "s_acctbal", "s_comment"), "s_comment"),
    Table("N", "NATION", ("n_nationkey",),
          ("n_nationkey", "n_name", "n_regionkey", "n_comment"), "n_comment"),
    Table("R", "REGION", ("r_regionkey",),
          ("r_regionkey", "r_name", "r_comment"), "r_comment"),
)
# Bit positions are stable and are recorded in the result file.
EDGES = (
    Edge("L_PS", "L", "PS", "LINEITEM_PARTSUPP", ("l_partkey", "l_suppkey")),
    Edge("L_O", "L", "O", "LINEITEM_ORDERS", ("l_orderkey",)),
    Edge("PS_P", "PS", "P", "PARTSUPP_PART", ("ps_partkey",)),
    Edge("PS_S", "PS", "S", "PARTSUPP_SUPPLIER", ("ps_suppkey",)),
    Edge("O_C", "O", "C", "ORDERS_CUSTOMER", ("o_custkey",)),
    Edge("C_N", "C", "N", "CUSTOMER_NATION", ("c_nationkey",)),
    Edge("S_N", "S", "N", "SUPPLIER_NATION", ("s_nationkey",)),
    Edge("N_R", "N", "R", "NATION_REGION", ("n_regionkey",)),
)
TABLE_BY_CODE = {table.code: table for table in TABLES}
OUTGOING = {
    table.code: tuple(edge for edge in EDGES if edge.source == table.code)
    for table in TABLES
}
INCOMING = {
    table.code: tuple(edge for edge in EDGES if edge.target == table.code)
    for table in TABLES
}
@dataclass
class Occurrence:
    table: str
    path: tuple[str, ...]
    variable: str
    parent: "Occurrence | None" = None
    parent_edge: Edge | None = None
    alias: str = ""
    stored: dict[str, str] | None = None
    locator: dict[str, str] | None = None
    @property
    def token(self) -> str:
        return "__".join((self.table, *self.path))
    @property
    def copy_relationship(self) -> str:
        return safe_name(f"COPY_{self.token}")
@dataclass
class FoldPlan:
    root: str
    occurrences: list[Occurrence]
    name: str = ""
    label: str = ""
    @property
    def root_occurrence(self) -> Occurrence:
        return self.occurrences[0]
@dataclass(frozen=True)
class SchemaNode:
    root: str
    name: str
    folded: bool
    plan: FoldPlan | None
@dataclass(frozen=True)
class InheritedEdge:
    edge: Edge
    source_node: SchemaNode
    source_occurrence: Occurrence | None
    target_node: SchemaNode
@dataclass
class SchemaPlan:
    mask: int
    selected: frozenset[str]
    nodes: list[SchemaNode]
    inherited: list[InheritedEdge]
    terminals: tuple[str, ...]
    @property
    def folds(self) -> list[FoldPlan]:
        return [node.plan for node in self.nodes if node.plan is not None]
@dataclass(frozen=True)
class UpdateItem:
    ordinal: int
    table: str
    keys: tuple[Any, ...]
    property_name: str
    a0: Any
    a1: Any
@dataclass(frozen=True)
class UpdateBinding:
    plan: FoldPlan
    occurrence: Occurrence
    folded_property: str
    lookup_properties: tuple[str, ...]
@dataclass(frozen=True)
class IndexSpec:
    name: str
    label: str
    properties: tuple[str, ...]
def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", value).upper()
    if not cleaned or cleaned[0].isdigit():
        raise ValueError(f"unsafe generated identifier: {value!r}")
    return cleaned
def quoted(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"
def selected_edges(mask: int) -> frozenset[str]:
    if not 0 <= mask < 1 << len(EDGES):
        raise ValueError(f"mask must be between 0 and {(1 << len(EDGES)) - 1}")
    return frozenset(edge.code for index, edge in enumerate(EDGES) if mask & (1 << index))
def representation_roots(selected: frozenset[str]) -> list[str]:
    roots = []
    for table in TABLES:
        incoming = INCOMING[table.code]
        if not incoming or any(edge.code not in selected for edge in incoming):
            roots.append(table.code)
    return roots
def make_fold(root: str, selected: frozenset[str]) -> FoldPlan | None:
    if not any(edge.code in selected for edge in OUTGOING[root]):
        return None
    occurrences: list[Occurrence] = []
    def visit(code: str, path: tuple[str, ...], parent: Occurrence | None,
              parent_edge: Edge | None) -> Occurrence:
        occurrence = Occurrence(code, path, f"v{len(occurrences)}", parent, parent_edge)
        occurrences.append(occurrence)
        for edge in OUTGOING[code]:
            if edge.code in selected:
                visit(edge.target, (*path, edge.code), occurrence, edge)
        return occurrence

    visit(root, (), None, None)
    counts = {table.code: sum(o.table == table.code for o in occurrences) for table in TABLES}
    for occurrence in occurrences:
        occurrence.alias = (
            occurrence.table if counts[occurrence.table] == 1
            else safe_name(occurrence.token)
        )

    used_properties: set[str] = set()
    for occurrence in occurrences:
        table = TABLE_BY_CODE[occurrence.table]
        occurrence.stored = {}
        occurrence.locator = {}
        omitted = set(table.key) if occurrence.parent is not None else set()
        for column in table.columns:
            if column in omitted:
                assert occurrence.parent is not None and occurrence.parent_edge is not None
                index = table.key.index(column)
                source_column = occurrence.parent_edge.source_fields[index]
                occurrence.locator[column] = occurrence.parent.locator[source_column]  # type: ignore[index]
                continue
            output = column if counts[occurrence.table] == 1 else f"{column}__{occurrence.alias.lower()}"
            if output in used_properties:
                raise AssertionError(f"duplicate folded property {output}")
            used_properties.add(output)
            occurrence.stored[column] = output
            occurrence.locator[column] = output

    folded_edges = {
        occurrence.parent_edge.code
        for occurrence in occurrences
        if occurrence.parent_edge is not None
    }
    name = "__".join(edge.code for edge in EDGES if edge.code in folded_edges)
    return FoldPlan(root, occurrences, name, safe_name(f"NR_FOLD_{name}"))
def plan_schema(mask: int) -> SchemaPlan:
    selected = selected_edges(mask)
    nodes: list[SchemaNode] = []
    by_root: dict[str, SchemaNode] = {}
    for root in representation_roots(selected):
        fold = make_fold(root, selected)
        node = SchemaNode(root, fold.name if fold else root, fold is not None, fold)
        nodes.append(node)
        by_root[root] = node

    inherited: list[InheritedEdge] = []
    for edge in EDGES:
        if edge.code in selected:
            continue
        target = by_root[edge.target]
        for source_node in nodes:
            if source_node.plan:
                for occurrence in source_node.plan.occurrences:
                    if occurrence.table == edge.source:
                        inherited.append(InheritedEdge(edge, source_node, occurrence, target))
            elif source_node.root == edge.source:
                inherited.append(InheritedEdge(edge, source_node, None, target))

    terminals = tuple(
        table.code for table in TABLES
        if any(edge.code in selected and edge.target == table.code for edge in EDGES)
        and not any(edge.code in selected for edge in OUTGOING[table.code])
    )
    plan = SchemaPlan(mask, selected, nodes, inherited, terminals)
    validate_plan(plan)
    return plan
def validate_plan(plan: SchemaPlan) -> None:
    if len({node.root for node in plan.nodes}) != len(plan.nodes):
        raise AssertionError(f"mask {plan.mask}: duplicate representation root")
    for fold in plan.folds:
        if len(fold.occurrences) < 2:
            raise AssertionError(f"mask {plan.mask}: empty fold {fold.name}")
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", fold.label):
            raise AssertionError(f"mask {plan.mask}: unsafe label {fold.label}")
    represented = {occ.table for fold in plan.folds for occ in fold.occurrences}
    represented.update(node.root for node in plan.nodes if not node.folded)
    if represented != set(TABLE_BY_CODE):
        raise AssertionError(f"mask {plan.mask}: relations are not fully represented")
    if any(table not in represented for table in plan.terminals):
        raise AssertionError(f"mask {plan.mask}: missing terminal")
def plan_dict(plan: SchemaPlan) -> dict[str, Any]:
    return {
        "mask": plan.mask,
        "folded_edges": [edge.code for edge in EDGES if edge.code in plan.selected],
        "terminal_relations": list(plan.terminals),
    }
def create_fold_query(fold: FoldPlan, batch_size: int) -> str:
    root = fold.root_occurrence
    matches = [f"MATCH ({root.variable}:{TABLE_BY_CODE[root.table].label})"]
    for occurrence in fold.occurrences[1:]:
        assert occurrence.parent is not None and occurrence.parent_edge is not None
        matches.append(
            f"MATCH ({occurrence.parent.variable})-[:{occurrence.parent_edge.relationship}]->"
            f"({occurrence.variable}:{TABLE_BY_CODE[occurrence.table].label})"
        )
    variables = ", ".join(o.variable for o in fold.occurrences)
    properties = []
    for occurrence in fold.occurrences:
        assert occurrence.stored is not None
        properties.extend(
            f"{quoted(output)}: {occurrence.variable}.{quoted(source)}"
            for source, output in occurrence.stored.items()
        )
    lineage = "\n  ".join(
        f"CREATE (f)-[:{occurrence.copy_relationship}]->({occurrence.variable})"
        for occurrence in fold.occurrences
    )
    return "\n".join(
        [
            *matches,
            f"CALL ({variables}) {{",
            f"  CREATE (f:{ARTIFACT_LABEL}:{fold.label} "
            "{nr_run_id: $run_id, nr_config: $config, nr_recipe: $recipe})",
            "  SET f += {" + ", ".join(properties) + "}",
            f"  {lineage}",
            f"}} IN TRANSACTIONS OF {batch_size} ROWS",
        ]
    )
def create_inherited_query(relation: InheritedEdge, batch_size: int) -> str | None:
    source_fold = relation.source_node.plan
    target_fold = relation.target_node.plan
    if source_fold is None and target_fold is None:
        return None
    edge = relation.edge
    source_label = TABLE_BY_CODE[edge.source].label
    target_label = TABLE_BY_CODE[edge.target].label
    if source_fold:
        assert relation.source_occurrence is not None
        left = (
            f"(a:{source_fold.label} {{nr_run_id: $run_id}})"
            f"-[:{relation.source_occurrence.copy_relationship}]->(s:{source_label})"
        )
    else:
        left = f"(s:{source_label})"
    if target_fold:
        target_copy = target_fold.root_occurrence.copy_relationship
        right = (
            f"(t:{target_label})<-[:{target_copy}]-"
            f"(b:{target_fold.label} {{nr_run_id: $run_id}})"
        )
        target_variable = "b"
    else:
        right = f"(t:{target_label})"
        target_variable = "t"
    source_variable = "a" if source_fold else "s"
    return "\n".join(
        [
            f"MATCH {left}-[:{edge.relationship}]->{right}",
            f"CALL ({source_variable}, {target_variable}) {{",
            f"  CREATE ({source_variable})-[:{edge.relationship} "
            "{nr_run_id: $run_id}]->"
            f"({target_variable})",
            f"}} IN TRANSACTIONS OF {batch_size} ROWS",
        ]
    )
def run_autocommit(session: Any, query: str, parameters: dict[str, Any]) -> Any:
    return session.run(query, parameters).consume()
def materialize(session: Any, plan: SchemaPlan, run_id: str,
                batch_size: int) -> tuple[float, list[dict[str, Any]], dict[str, int]]:
    steps: list[dict[str, Any]] = []
    started = time.perf_counter_ns()
    for fold in plan.folds:
        step_started = time.perf_counter_ns()
        summary = run_autocommit(
            session,
            create_fold_query(fold, batch_size),
            {"run_id": run_id, "config": plan.mask, "recipe": fold.name},
        )
        steps.append({"kind": "fold", "name": fold.name,
                      "elapsed_ms": elapsed_ms(step_started),
                      "created_node_count": summary.counters.nodes_created})
    for relation in plan.inherited:
        query = create_inherited_query(relation, batch_size)
        if query is None:
            continue
        step_started = time.perf_counter_ns()
        summary = run_autocommit(session, query, {"run_id": run_id})
        steps.append({"kind": "inherit", "edge": relation.edge.code,
                      "type": relation.edge.relationship,
                      "source": relation.source_node.name,
                      "target": relation.target_node.name,
                      "elapsed_ms": elapsed_ms(step_started),
                      "created_relationship_count":
                          summary.counters.relationships_created})
    total_ms = elapsed_ms(started)
    if not plan.folds:
        return total_ms, steps, {"node_count": 0, "property_cell_count": 0}
    record = session.run(
        f"MATCH (n:{ARTIFACT_LABEL} {{nr_run_id: $run_id}}) "
        "RETURN count(n) AS nodes, "
        "coalesce(sum(size([k IN keys(n) WHERE NOT (k IN $metadata)])), 0) AS cells",
        run_id=run_id, metadata=list(EXPERIMENT_PROPERTIES),
    ).single(strict=True)
    return total_ms, steps, {
        "node_count": int(record["nodes"]),
        "property_cell_count": int(record["cells"]),
    }
def cleanup(session: Any, label: str, run_id: str, batch_size: int) -> float:
    """Delete incident relationships in bounded batches, then isolated artifacts."""
    if label not in (ARTIFACT_LABEL, NORMALIZED_LABEL) or not run_id:
        raise ValueError("cleanup requires an artifact label and a non-empty run_id")
    if batch_size <= 0:
        raise ValueError("cleanup batch size must be positive")
    started = time.perf_counter_ns()
    target = f"(n:{quoted(label)} {{nr_run_id: $run_id}})"
    # A node batch does not bound the number of relationships DETACH DELETE
    # removes. Delete outgoing, then remaining incoming relationships instead.
    # Separate directed passes avoid matching internal edges twice and include
    # COPY relationships, which have no nr_run_id property of their own.
    for pattern in (f"{target}-[r]->()", f"{target}<-[r]-()"):
        query = "\n".join(
            [
                f"MATCH {pattern}",
                "CALL (r) { DELETE r }",
                f"IN TRANSACTIONS OF {batch_size} ROWS",
            ]
        )
        run_autocommit(session, query, {"run_id": run_id})
    # Plain DELETE must fail if relationships remain; do not fall back to an
    # unbounded detach when another writer attaches new relationships.
    query = "\n".join(
        [
            f"MATCH {target}",
            "CALL (n) { DELETE n }",
            f"IN TRANSACTIONS OF {batch_size} ROWS",
        ]
    )
    run_autocommit(session, query, {"run_id": run_id})
    return elapsed_ms(started)
def source_state(session: Any) -> tuple[
    dict[str, int], dict[str, tuple[int, int]], dict[str, int]
]:
    tables: dict[str, tuple[int, int]] = {}
    for table in TABLES:
        record = session.run(
            f"MATCH (n:{quoted(table.label)}) "
            "RETURN count(n) AS nodes, "
            "coalesce(sum(size([k IN keys(n) WHERE k IN $columns])), 0) AS cells",
            columns=list(table.columns),
        ).single(strict=True)
        tables[table.code] = (int(record["nodes"]), int(record["cells"]))
    relationships = {}
    for edge in EDGES:
        record = session.run(
            f"MATCH (:{quoted(TABLE_BY_CODE[edge.source].label)})"
            f"-[r:{quoted(edge.relationship)}]->"
            f"(:{quoted(TABLE_BY_CODE[edge.target].label)}) "
            "RETURN count(r) AS relationships"
        ).single(strict=True)
        relationships[edge.code] = int(record["relationships"])
    return {
        "node_count": sum(value[0] for value in tables.values()),
        "relationship_count": sum(relationships.values()),
        "property_cell_count": sum(value[1] for value in tables.values()),
    }, tables, relationships
def folded_state(plan: SchemaPlan, artifacts: dict[str, int], inherited: int,
                 tables: dict[str, tuple[int, int]],
                 relationships: dict[str, int]) -> dict[str, int]:
    retained_tables = [node.root for node in plan.nodes if not node.folded]
    retained_edges = {
        relation.edge.code for relation in plan.inherited
        if not relation.source_node.folded and not relation.target_node.folded
    }
    return {
        "node_count": artifacts["node_count"]
        + sum(tables[code][0] for code in retained_tables),
        "relationship_count": inherited
        + sum(relationships[code] for code in retained_edges),
        "property_cell_count": artifacts["property_cell_count"]
        + sum(tables[code][1] for code in retained_tables),
    }
def state_delta(folded: dict[str, int], normalized: dict[str, int]) -> dict[str, int]:
    return {
        "delta_nodes": folded["node_count"] - normalized["node_count"],
        "delta_relationships": (
            folded["relationship_count"] - normalized["relationship_count"]
        ),
        "delta_property_cells": (
            folded["property_cell_count"] - normalized["property_cell_count"]
        ),
    }
def candidate_query(table: Table, limit: int) -> str:
    inbound = INCOMING[table.code]
    predicates = [
        f"EXISTS {{ MATCH ()-[:{edge.relationship}]->(n) }}" for edge in inbound
    ]
    where = "WHERE " + " AND ".join(predicates) if predicates else ""
    returns = ", ".join(
        [*(f"n.{quoted(key)} AS k{i}" for i, key in enumerate(table.key)),
         f"n.{quoted(table.update_property or '')} AS a0"]
    )
    order = ", ".join(f"k{i}" for i in range(len(table.key)))
    return (
        f"MATCH (n:{table.label})\n{where}\nRETURN {returns}\n"
        f"ORDER BY {order}\nLIMIT {limit}"
    )
def generate_workload(session: Any, count: int,
                      seed: int) -> dict[str, list[UpdateItem]]:
    workload: dict[str, list[UpdateItem]] = {}
    rng = random.Random(seed)
    for table in TABLES:
        if table.update_property is None:
            continue
        rows = [
            dict(record)
            for record in session.run(candidate_query(table, max(count, 2)))
        ]
        if not rows:
            raise RuntimeError(f"no update candidates found for {table.label}")
        domain = list(dict.fromkeys(row["a0"] for row in rows if row["a0"] is not None))
        if len(domain) < 2:
            raise RuntimeError(
                f"{table.label}.{table.update_property} needs at least two domain values"
            )
        items = []
        for ordinal in range(1, count + 1):
            row = rows[(ordinal - 1) % len(rows)]
            a0 = row["a0"]
            if a0 is None:
                raise RuntimeError(f"{table.label}.{table.update_property} contains null")
            a1 = rng.choice([value for value in domain if value != a0])
            items.append(
                UpdateItem(
                    ordinal,
                    table.code,
                    tuple(row[f"k{i}"] for i in range(len(table.key))),
                    table.update_property,
                    a0,
                    a1,
                )
            )
        workload[table.code] = items
    return workload
def update_bindings(plan: SchemaPlan, table_code: str) -> list[UpdateBinding]:
    table = TABLE_BY_CODE[table_code]
    assert table.update_property is not None
    bindings = []
    for fold in plan.folds:
        for occurrence in fold.occurrences:
            if occurrence.table != table_code:
                continue
            assert occurrence.stored is not None
            folded_property = occurrence.stored.get(table.update_property)
            if folded_property is None:
                raise AssertionError(f"update property was omitted in {fold.name}")
            assert occurrence.locator is not None
            lookup = tuple(occurrence.locator[key] for key in table.key)
            bindings.append(UpdateBinding(fold, occurrence, folded_property, lookup))
    return bindings
def index_specs(plan: SchemaPlan, run_id: str) -> list[IndexSpec]:
    specs = []
    seen: set[tuple[str, tuple[str, ...]]] = set()
    for table_code in plan.terminals:
        for binding in update_bindings(plan, table_code):
            identity = (binding.plan.label, binding.lookup_properties)
            if identity in seen:
                continue
            seen.add(identity)
            name = safe_name(
                f"NR_IDX_{run_id[:12]}_{binding.plan.root}_{binding.occurrence.token}"
            )
            specs.append(IndexSpec(name, binding.plan.label, binding.lookup_properties))
    return specs
def create_indexes(session: Any, specs: list[IndexSpec], wait_seconds: int) -> dict[str, Any]:
    started = time.perf_counter_ns()
    created: list[IndexSpec] = []
    try:
        for spec in specs:
            properties = ", ".join(f"n.{quoted(prop)}" for prop in spec.properties)
            session.run(
                f"CREATE RANGE INDEX {quoted(spec.name)} "
                f"FOR (n:{quoted(spec.label)}) ON ({properties})"
            ).consume()
            created.append(spec)
        if created:
            session.run(
                "CALL db.awaitIndexes($timeout_seconds)",
                timeout_seconds=wait_seconds,
            ).consume()
    except BaseException:
        drop_indexes(session, created)
        raise
    return {
        "elapsed_ms": elapsed_ms(started),
        "wait_timeout_seconds": wait_seconds,
        "indexes": [
            {"name": spec.name, "type": "RANGE", "label": spec.label,
             "properties": list(spec.properties), "unique": False}
            for spec in specs
        ],
    }
def drop_indexes(session: Any, specs: list[IndexSpec]) -> None:
    for spec in reversed(specs):
        session.run(f"DROP INDEX {quoted(spec.name)} IF EXISTS").consume()
def normalized_label(table_code: str) -> str:
    return safe_name(f"NR_NORMALIZED_{table_code}")
def normalization_tables(plan: SchemaPlan) -> set[str]:
    retained = {node.root for node in plan.nodes if not node.folded}
    return set(TABLE_BY_CODE) - retained
def normalization_index_specs(plan: SchemaPlan, run_id: str) -> list[IndexSpec]:
    return [
        IndexSpec(safe_name(f"NR_NORM_IDX_{run_id[:12]}_{code}"),
                  normalized_label(code), ("nr_run_id", *TABLE_BY_CODE[code].key))
        for code in sorted(normalization_tables(plan))
    ]
def normalization_node_query(fold: FoldPlan, occurrence: Occurrence,
                             batch_size: int) -> str:
    table = TABLE_BY_CODE[occurrence.table]
    assert occurrence.locator is not None
    keys = ", ".join(
        f"{quoted(key)}: f.{quoted(occurrence.locator[key])}" for key in table.key
    )
    properties = ", ".join(
        f"{quoted(column)}: f.{quoted(occurrence.locator[column])}"
        for column in table.columns
    )
    return "\n".join([
        f"MATCH (f:{quoted(fold.label)} {{nr_run_id: $fold_id}})",
        "CALL (f) {",
        f"  MERGE (n:{NORMALIZED_LABEL}:{normalized_label(table.code)} "
        f"{{nr_run_id: $norm_id, {keys}}})",
        f"  SET n += {{{properties}}}",
        f"}} IN TRANSACTIONS OF {batch_size} ROWS",
    ])
def normalization_relationship_query(edge: Edge, reconstructed: set[str],
                                     batch_size: int) -> str:
    source = (f"(s:{normalized_label(edge.source)} {{nr_run_id: $norm_id}})"
              if edge.source in reconstructed else
              f"(s:{quoted(TABLE_BY_CODE[edge.source].label)})")
    target_label = (normalized_label(edge.target) if edge.target in reconstructed
                    else quoted(TABLE_BY_CODE[edge.target].label))
    target_values = (["nr_run_id: $norm_id"] if edge.target in reconstructed else [])
    target_values += [
        f"{quoted(target)}: s.{quoted(source_field)}"
        for source_field, target in zip(
            edge.source_fields, TABLE_BY_CODE[edge.target].key
        )
    ]
    return "\n".join([
        f"MATCH {source}", "CALL (s) {",
        f"  MATCH (t:{target_label} {{{', '.join(target_values)}}})",
        f"  CREATE (s)-[:{quoted(edge.relationship)}]->(t)",
        f"}} IN TRANSACTIONS OF {batch_size} ROWS",
    ])
def normalize(session: Any, plan: SchemaPlan, fold_id: str, norm_id: str,
              specs: list[IndexSpec], batch_size: int, wait_seconds: int) -> dict[str, Any]:
    reconstructed = normalization_tables(plan)
    if not reconstructed:
        return {"method": "rebuild_from_folded_data", "sample_count": 1,
                "elapsed_ms": 0.0, "created_node_count": 0,
                "created_relationship_count": 0}
    started = time.perf_counter_ns()
    index_result = create_indexes(session, specs, wait_seconds)
    node_started = time.perf_counter_ns()
    nodes = 0
    for fold in plan.folds:
        for occurrence in fold.occurrences:
            if occurrence.table in reconstructed:
                summary = run_autocommit(
                    session, normalization_node_query(fold, occurrence, batch_size),
                    {"fold_id": fold_id, "norm_id": norm_id},
                )
                nodes += summary.counters.nodes_created
    node_ms = elapsed_ms(node_started)
    relationship_started = time.perf_counter_ns()
    relationships = 0
    for edge in EDGES:
        if edge.source in reconstructed or edge.target in reconstructed:
            summary = run_autocommit(
                session, normalization_relationship_query(edge, reconstructed, batch_size),
                {"norm_id": norm_id},
            )
            relationships += summary.counters.relationships_created
    relationship_ms = elapsed_ms(relationship_started)
    return {"method": "rebuild_from_folded_data", "scope": "participating tuples",
            "sample_count": 1, "elapsed_ms": elapsed_ms(started),
            "index_build_ms": index_result["elapsed_ms"],
            "node_rebuild_ms": node_ms, "relationship_rebuild_ms": relationship_ms,
            "created_node_count": nodes, "created_relationship_count": relationships}
def update_query(binding: UpdateBinding) -> str:
    key_map = ", ".join(
        f"{quoted(prop)}: $k{i}" for i, prop in enumerate(binding.lookup_properties)
    )
    return "\n".join(
        [
            f"MATCH (f:{quoted(binding.plan.label)} {{{key_map}}})",
            "WHERE f.nr_run_id = $run_id",
            f"SET f.{quoted(binding.folded_property)} = $value",
            "RETURN count(f) AS touched",
        ]
    )
def normalized_update_query(table: Table) -> str:
    key_map = ", ".join(
        f"{quoted(prop)}: $k{i}" for i, prop in enumerate(table.key)
    )
    return "\n".join(
        [f"MATCH (n:{quoted(table.label)} {{{key_map}}})",
         f"SET n.{quoted(table.update_property or '')} = $value",
         "RETURN count(n) AS touched"]
    )
def update_transaction(session: Any,
                       statements: Iterable[tuple[str, dict[str, Any]]]) -> int:
    touched = 0
    transaction = session.begin_transaction()
    try:
        for query, parameters in statements:
            record = transaction.run(query, parameters).single(strict=True)
            touched += int(record["touched"])
        transaction.commit()
    except BaseException:
        transaction.rollback()
        raise
    finally:
        transaction.close()
    return touched
def apply_update(session: Any, bindings: list[UpdateBinding], item: UpdateItem,
                 value: Any, run_id: str) -> int:
    parameters = {"run_id": run_id, "value": value}
    parameters.update({f"k{i}": key for i, key in enumerate(item.keys)})
    return update_transaction(
        session, ((update_query(binding), parameters) for binding in bindings)
    )
def apply_normalized_update(session: Any, item: UpdateItem, value: Any) -> int:
    table = TABLE_BY_CODE[item.table]
    parameters = {"value": value}
    parameters.update({f"k{i}": key for i, key in enumerate(item.keys)})
    return update_transaction(
        session, ((normalized_update_query(table), parameters),)
    )
def timing_summary(values: list[float]) -> dict[str, Any]:
    return {"sample_count": len(values), "min_ms": min(values),
            "median_ms": statistics.median(values), "max_ms": max(values)}
def repeated_update(set_value: Any, restore_value: Any, repeats: int
                    ) -> tuple[int, list[float]]:
    touched_counts, durations = [], []
    for _ in range(repeats):
        started = time.perf_counter_ns()
        try:
            touched_counts.append(set_value())
            durations.append(elapsed_ms(started))
        finally:
            restore_value()
    if len(set(touched_counts)) != 1:
        raise RuntimeError(f"repeated updates touched {touched_counts} nodes")
    return touched_counts[0], durations
def measure_normalized_updates(
    session: Any, workload: dict[str, list[UpdateItem]], repeats: int
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for table_code, items in workload.items():
        table = TABLE_BY_CODE[table_code]
        samples = []
        for item in items:
            touched, durations = repeated_update(
                lambda: apply_normalized_update(session, item, item.a1),
                lambda: apply_normalized_update(session, item, item.a0), repeats)
            if touched != 1:
                raise RuntimeError(f"normalized update {table_code} {item.keys} "
                                   f"touched {touched} nodes")
            samples.append(
                {"group": item.ordinal,
                 "key": dict(zip(table.key, json_values(item.keys))),
                 "a0": json_value(item.a0), "a1": json_value(item.a1),
                 "touched_nodes": touched, "repeat_elapsed_ms": durations,
                 "elapsed_ms": statistics.median(durations)})
        output[table_code] = {
            "property": table.update_property,
            "repeat_count": repeats,
            "indexed_lookup": {"label": table.label,
                               "properties": list(table.key)},
            **timing_summary([sample["elapsed_ms"] for sample in samples]),
            "samples": samples,
        }
    return output
def add_normalized_comparisons(denormalized: dict[str, Any],
                               normalized: dict[str, Any]) -> None:
    for table_code, result in denormalized.items():
        baseline = normalized[table_code]
        if len(result["samples"]) != len(baseline["samples"]):
            raise AssertionError(f"unpaired update count for {table_code}")
        deltas = []
        for folded, normal in zip(result["samples"], baseline["samples"]):
            identity = ("group", "key", "a0", "a1")
            if any(folded[field] != normal[field] for field in identity):
                raise AssertionError(f"unpaired normalized update for {table_code}")
            deltas.append(folded["elapsed_ms"] - normal["elapsed_ms"])
        result["normalized_update"] = {
            key: baseline[key]
            for key in ("sample_count", "repeat_count", "min_ms", "median_ms", "max_ms")
        }
        result["normalization_save"] = timing_summary(deltas)
def measure_updates(session: Any, plan: SchemaPlan, workload: dict[str, list[UpdateItem]],
                    run_id: str,
                    repeats: int) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for table_code in plan.terminals:
        bindings = update_bindings(plan, table_code)
        if not bindings:
            raise AssertionError(f"mask {plan.mask}: terminal {table_code} has no folded copy")
        samples = []
        for item in workload[table_code]:
            touched, durations = repeated_update(
                lambda: apply_update(session, bindings, item, item.a1, run_id),
                lambda: apply_update(session, bindings, item, item.a0, run_id), repeats)
            if touched == 0:
                raise RuntimeError(f"mask {plan.mask}: update {table_code} "
                                   f"{item.keys} touched no folded node")
            samples.append(
                {"group": item.ordinal, "key": dict(zip(TABLE_BY_CODE[table_code].key,
                                                         json_values(item.keys))),
                 "a0": json_value(item.a0), "a1": json_value(item.a1),
                 "touched_nodes": touched, "repeat_elapsed_ms": durations,
                 "elapsed_ms": statistics.median(durations)})
        values = [sample["elapsed_ms"] for sample in samples]
        output[table_code] = {
            "property": TABLE_BY_CODE[table_code].update_property,
            "repeat_count": repeats,
            "copy_locations": len(bindings),
            "indexed_lookup": [
                {"fold": binding.plan.name,
                 "properties": list(binding.lookup_properties)}
                for binding in bindings
            ],
            **timing_summary(values),
            "samples": samples,
        }
    return output
def json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value
def json_values(values: Iterable[Any]) -> list[Any]:
    return [json_value(value) for value in values]
def workload_dict(workload: dict[str, list[UpdateItem]], count: int) -> list[dict[str, Any]]:
    groups = []
    for index in range(count):
        updates = {}
        for table_code, items in workload.items():
            item = items[index]
            table = TABLE_BY_CODE[table_code]
            updates[table_code] = {
                "key": dict(zip(table.key, json_values(item.keys))),
                "property": item.property_name,
                "a0": json_value(item.a0),
                "a1": json_value(item.a1),
            }
        groups.append({"group": index + 1, "updates": updates})
    return groups
def elapsed_ms(start_ns: int) -> float:
    return (time.perf_counter_ns() - start_ns) / 1_000_000
def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n",
                         encoding="utf-8")
    temporary.replace(path)
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize all TPC-H fold schemas and time terminal FD updates.")
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI", DEFAULT_URI))
    parser.add_argument("--user", default=os.getenv("NEO4J_USER", DEFAULT_USER))
    parser.add_argument("--password",
                        default=os.getenv("NEO4J_PASSWORD", DEFAULT_PASSWORD))
    parser.add_argument("--database", default=os.getenv("NEO4J_DATABASE", DEFAULT_DATABASE))
    parser.add_argument("--updates", type=int, default=DEFAULT_UPDATE_COUNT,
                        help="number of seven-table update groups")
    parser.add_argument("--repeats", type=int, default=DEFAULT_UPDATE_REPEATS,
                        help="timed repetitions per update case")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--cleanup-batch-size", type=int,
                        default=DEFAULT_CLEANUP_BATCH_SIZE,
                        help="relationships or isolated nodes per cleanup transaction")
    parser.add_argument("--index-timeout", type=int, default=DEFAULT_INDEX_TIMEOUT,
                        help="seconds to wait for folded indexes to become online")
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED,
                        help="seed for choosing A1 from existing domain values")
    parser.add_argument("--mask", action="append", type=int,
                        help="run only this 0..255 mask; may be repeated")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--plan-only", action="store_true",
                        help="write/print the 256 schema plans without connecting to Neo4j")
    args = parser.parse_args(argv)
    if min(args.updates, args.repeats, args.batch_size,
           args.cleanup_batch_size, args.index_timeout) <= 0:
        parser.error("--updates, --repeats, --batch-size, --cleanup-batch-size "
                     "and --index-timeout "
                     "must be positive")
    if args.mask and any(not 0 <= mask < 256 for mask in args.mask):
        parser.error("each --mask must be between 0 and 255")
    return args
def experiment(args: argparse.Namespace) -> dict[str, Any]:
    configured_masks = args.mask if args.mask is not None else DEFAULT_MASKS
    masks = (
        sorted(set(configured_masks))
        if configured_masks is not None else list(range(256))
    )
    plans = [plan_schema(mask) for mask in masks]
    header: dict[str, Any] = {
        "experiment": "tpch_256_fold_terminal_update",
        "created_at": datetime.now().astimezone().isoformat(),
        "edge_bits": {str(index): edge.code for index, edge in enumerate(EDGES)},
        "configuration_count": len(plans),
        "configurations": [],
    }
    if args.plan_only:
        header["configurations"] = [plan_dict(plan) for plan in plans]
        return header

    try:
        import neo4j
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise SystemExit("neo4j driver is required: install package 'neo4j'") from exc

    password = args.password or getpass("Neo4j password: ")
    output = args.output or (
        Path(__file__).resolve().parent.parent / "results" / "new_redundancy"
        / args.database / "results.json"
    )
    header.update({"database": args.database, "neo4j_driver": neo4j.__version__,
                   "update_group_count": args.updates,
                   "update_repeat_count": args.repeats,
                   "cleanup_batch_size": args.cleanup_batch_size,
                   "update_value_policy": "random_existing_participating_domain_value",
                   "random_seed": args.seed, "results_file": str(output),
                   "measurement_definitions": {
                       "normalization_save_ms": "denormalized - normalized",
                       "update_case_time": "median of repeated A0-to-A1 updates; A1-to-A0 restore excluded",
                       "state_delta": "denormalized - normalized",
                       "property_cells": "non-null TPC-H properties; nr_* excluded",
                       "normalization_time": "rebuild normalized nodes, relationships and indexes from Fold data; cleanup excluded",
                   }})

    driver = GraphDatabase.driver(args.uri, auth=(args.user, password))
    try:
        driver.verify_connectivity()
        with driver.session(database=args.database) as session:
            workload = generate_workload(session, args.updates, args.seed)
            header["update_groups"] = workload_dict(workload, args.updates)
            normalized_state, table_counts, relationship_counts = source_state(session)
            normalized_updates = measure_normalized_updates(session, workload,
                                                            args.repeats)
            header["normalized_baseline"] = {
                "state": normalized_state,
                "updates": normalized_updates,
            }
            write_json(output, header)
            for position, plan in enumerate(plans, start=1):
                run_id = uuid.uuid4().hex
                norm_id = uuid.uuid4().hex
                indexes = index_specs(plan, run_id)
                norm_indexes = normalization_index_specs(plan, norm_id)
                print(f"[{position}/{len(plans)}] mask={plan.mask:03d} "
                      f"edges={','.join(sorted(plan.selected)) or '-'}", flush=True)
                config = plan_dict(plan)
                try:
                    fold_ms, fold_steps, artifact_state = materialize(
                        session, plan, run_id, args.batch_size
                    )
                    inherited_count = sum(
                        step.get("created_relationship_count", 0)
                        for step in fold_steps if step["kind"] == "inherit"
                    )
                    config["fold"] = {"elapsed_ms": fold_ms,
                                      "created_node_count": artifact_state["node_count"],
                                      "created_property_cell_count":
                                          artifact_state["property_cell_count"],
                                      "created_inherited_relationship_count":
                                          inherited_count,
                                      "steps": fold_steps}
                    denormalized_state = folded_state(
                        plan, artifact_state, inherited_count,
                        table_counts, relationship_counts,
                    )
                    config["denormalized_state"] = denormalized_state
                    config["structural_delta"] = state_delta(
                        denormalized_state, normalized_state
                    )
                    config["index_build"] = create_indexes(
                        session, indexes, args.index_timeout
                    )
                    config["updates"] = measure_updates(session, plan, workload,
                                                        run_id, args.repeats)
                    add_normalized_comparisons(config["updates"], normalized_updates)
                    config["normalization"] = normalize(
                        session, plan, run_id, norm_id, norm_indexes,
                        args.batch_size, args.index_timeout,
                    )
                finally:
                    drop_started = time.perf_counter_ns()
                    try:
                        drop_indexes(session, indexes)
                        drop_indexes(session, norm_indexes)
                    finally:
                        config["index_drop"] = {
                            "elapsed_ms": elapsed_ms(drop_started),
                            "index_count": len(indexes) + len(norm_indexes),
                        }
                        if norm_indexes:
                            cleanup(session, NORMALIZED_LABEL, norm_id,
                                    args.cleanup_batch_size)
                        if plan.folds:
                            cleanup(session, ARTIFACT_LABEL, run_id,
                                    args.cleanup_batch_size)
                header["configurations"].append(config)
                write_json(output, header)
    finally:
        driver.close()
    return header
def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = experiment(args)
    if args.plan_only:
        if args.output:
            write_json(args.output, result)
            print(args.output)
        else:
            print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(result["results_file"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
