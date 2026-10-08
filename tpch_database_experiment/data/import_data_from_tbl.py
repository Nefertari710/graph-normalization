#!/usr/bin/env python3
"""Load TPC-H ``.tbl`` files into Neo4j.

The graph keeps all eight TPC-H tables as nodes. Foreign keys are represented
as relationships, which preserves the relational schema without losing the
attributes or composite keys of PARTSUPP and LINEITEM.

For a new/empty graph, the import order is:

1. create all nodes;
2. add node key constraints;
3. create all foreign-key relationships;
4. validate the imported counts.

If TPC-H nodes already exist (for example after an interrupted import), the
script creates the node key constraints first and uses MERGE to make the retry
safe. Node key constraints require Neo4j Enterprise Edition.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import sys
from dataclasses import dataclass
from datetime import date
from getpass import getpass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence


DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "tbl_sf_01"

# Edit this value to select the target Neo4j database. If it does not exist,
# the importer creates it before loading data. Neo4j database names cannot
# contain underscores, so use a name such as "tpchsf1" or "tpch-sf-1".
DATABASE_NAME = "tpch-sf-01"

# Local debugging only. Temporarily replace None with your Neo4j password.
# Never commit or share this file while it contains a real password.
DEBUG_NEO4J_PASSWORD: str | None = ""

SYSTEM_DATABASE = "system"

class TblFormatError(ValueError):
    """Raised when a TPC-H .tbl row does not match its declared schema."""


class ImportValidationError(RuntimeError):
    """Raised when Neo4j contains fewer or more imported objects than expected."""


Converter = Callable[[str], Any]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    converter: Converter


@dataclass(frozen=True)
class TableSpec:
    filename: str
    label: str
    constraint_name: str
    columns: tuple[ColumnSpec, ...]
    key_fields: tuple[str, ...]


@dataclass(frozen=True)
class EndpointSpec:
    label: str
    # Each item is (node property, source .tbl row property).
    keys: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class EdgeSpec:
    relationship_type: str
    target: EndpointSpec


@dataclass(frozen=True)
class RelationshipGroup:
    table: TableSpec
    source: EndpointSpec
    edges: tuple[EdgeSpec, ...]


def as_text(raw: str) -> str:
    # TPC-H text can contain meaningful leading spaces; do not strip it.
    return raw


def as_int(raw: str) -> int:
    return int(raw)


def as_float(raw: str) -> float:
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("value must be finite")
    return value


def as_date(raw: str) -> date:
    return date.fromisoformat(raw)


REGION = TableSpec(
    filename="region.tbl",
    label="REGION",
    constraint_name="region_pk",
    columns=(
        ColumnSpec("r_regionkey", as_int),          # INTEGER PRIMARY KEY
        ColumnSpec("r_name", as_text),              # CHAR(25) NOT NULL
        ColumnSpec("r_comment", as_text),           # VARCHAR(152)
    ),
    key_fields=("r_regionkey",),
)

NATION = TableSpec(
    filename="nation.tbl",
    label="NATION",
    constraint_name="nation_pk",
    columns=(
        ColumnSpec("n_nationkey", as_int),          # INTEGER PRIMARY KEY
        ColumnSpec("n_name", as_text),              # CHAR(25) NOT NULL
        ColumnSpec("n_regionkey", as_int),          # INTEGER NOT NULL
        ColumnSpec("n_comment", as_text),           # VARCHAR(152)
    ),
    key_fields=("n_nationkey",),
)

SUPPLIER = TableSpec(
    filename="supplier.tbl",
    label="SUPPLIER",
    constraint_name="supplier_pk",
    columns=(
        ColumnSpec("s_suppkey", as_int),            # INTEGER PRIMARY KEY
        ColumnSpec("s_name", as_text),              # CHAR(25) NOT NULL
        ColumnSpec("s_address", as_text),           # VARCHAR(40) NOT NULL
        ColumnSpec("s_nationkey", as_int),          # INTEGER NOT NULL
        ColumnSpec("s_phone", as_text),             # CHAR(15) NOT NULL
        ColumnSpec("s_acctbal", as_float),          # NUMERIC(15,2) NOT NULL
        ColumnSpec("s_comment", as_text),           # VARCHAR(101) NOT NULL
    ),
    key_fields=("s_suppkey",),
)

CUSTOMER = TableSpec(
    filename="customer.tbl",
    label="CUSTOMER",
    constraint_name="customer_pk",
    columns=(
        ColumnSpec("c_custkey", as_int),            # INTEGER PRIMARY KEY
        ColumnSpec("c_name", as_text),              # VARCHAR(25) NOT NULL
        ColumnSpec("c_address", as_text),           # VARCHAR(40) NOT NULL
        ColumnSpec("c_nationkey", as_int),          # INTEGER NOT NULL
        ColumnSpec("c_phone", as_text),             # CHAR(15) NOT NULL
        ColumnSpec("c_acctbal", as_float),          # NUMERIC(15,2) NOT NULL
        ColumnSpec("c_mktsegment", as_text),        # CHAR(10) NOT NULL
        ColumnSpec("c_comment", as_text),           # VARCHAR(117) NOT NULL
    ),
    key_fields=("c_custkey",),
)

PART = TableSpec(
    filename="part.tbl",
    label="PART",
    constraint_name="part_pk",
    columns=(
        ColumnSpec("p_partkey", as_int),            # INTEGER PRIMARY KEY
        ColumnSpec("p_name", as_text),              # VARCHAR(55) NOT NULL
        ColumnSpec("p_mfgr", as_text),              # CHAR(25) NOT NULL
        ColumnSpec("p_brand", as_text),             # CHAR(10) NOT NULL
        ColumnSpec("p_type", as_text),              # VARCHAR(25) NOT NULL
        ColumnSpec("p_size", as_int),               # INTEGER NOT NULL
        ColumnSpec("p_container", as_text),         # CHAR(10) NOT NULL
        ColumnSpec("p_retailprice", as_float),      # NUMERIC(15,2) NOT NULL
        ColumnSpec("p_comment", as_text),           # VARCHAR(23) NOT NULL
    ),
    key_fields=("p_partkey",),
)

PARTSUPP = TableSpec(
    filename="partsupp.tbl",
    label="PARTSUPP",
    constraint_name="partsupp_pk",
    columns=(
        ColumnSpec("ps_partkey", as_int),           # INTEGER NOT NULL
        ColumnSpec("ps_suppkey", as_int),           # INTEGER NOT NULL
        ColumnSpec("ps_availqty", as_int),          # INTEGER NOT NULL
        ColumnSpec("ps_supplycost", as_float),      # NUMERIC(15,2) NOT NULL
        ColumnSpec("ps_comment", as_text),          # VARCHAR(199) NOT NULL
    ),
    key_fields=("ps_partkey", "ps_suppkey"),
)

ORDERS = TableSpec(
    filename="orders.tbl",
    label="ORDERS",
    constraint_name="order_pk",
    columns=(
        ColumnSpec("o_orderkey", as_int),           # INTEGER PRIMARY KEY
        ColumnSpec("o_custkey", as_int),            # INTEGER NOT NULL
        ColumnSpec("o_orderstatus", as_text),       # CHAR(1) NOT NULL
        ColumnSpec("o_totalprice", as_float),       # NUMERIC(15,2) NOT NULL
        ColumnSpec("o_orderdate", as_date),         # DATE NOT NULL
        ColumnSpec("o_orderpriority", as_text),     # CHAR(15) NOT NULL
        ColumnSpec("o_clerk", as_text),             # CHAR(15) NOT NULL
        ColumnSpec("o_shippriority", as_int),       # INTEGER NOT NULL
        ColumnSpec("o_comment", as_text),           # VARCHAR(79) NOT NULL
    ),
    key_fields=("o_orderkey",),
)

LINEITEM = TableSpec(
    filename="lineitem.tbl",
    label="LINEITEM",
    constraint_name="lineitem_pk",
    columns=(
        ColumnSpec("l_orderkey", as_int),           # INTEGER NOT NULL
        ColumnSpec("l_partkey", as_int),            # INTEGER NOT NULL
        ColumnSpec("l_suppkey", as_int),            # INTEGER NOT NULL
        ColumnSpec("l_linenumber", as_int),         # INTEGER NOT NULL
        ColumnSpec("l_quantity", as_float),         # NUMERIC(15,2) NOT NULL
        ColumnSpec("l_extendedprice", as_float),    # NUMERIC(15,2) NOT NULL
        ColumnSpec("l_discount", as_float),         # NUMERIC(15,2) NOT NULL
        ColumnSpec("l_tax", as_float),              # NUMERIC(15,2) NOT NULL
        ColumnSpec("l_returnflag", as_text),        # CHAR(1) NOT NULL
        ColumnSpec("l_linestatus", as_text),        # CHAR(1) NOT NULL
        ColumnSpec("l_shipdate", as_date),          # DATE NOT NULL
        ColumnSpec("l_commitdate", as_date),        # DATE NOT NULL
        ColumnSpec("l_receiptdate", as_date),       # DATE NOT NULL
        ColumnSpec("l_shipinstruct", as_text),      # CHAR(25) NOT NULL
        ColumnSpec("l_shipmode", as_text),          # CHAR(10) NOT NULL
        ColumnSpec("l_comment", as_text),           # VARCHAR(44) NOT NULL
    ),
    key_fields=("l_orderkey", "l_linenumber"),
)


NODE_TABLES = (
    REGION,
    NATION,
    SUPPLIER,
    CUSTOMER,
    PART,
    PARTSUPP,
    ORDERS,
    LINEITEM,
)


RELATIONSHIP_GROUPS = (
    RelationshipGroup(
        table=NATION,
        source=EndpointSpec(NATION.label, (("n_nationkey", "n_nationkey"),)),
        edges=(
            EdgeSpec(
                "NATION_REGION",
                EndpointSpec(REGION.label, (("r_regionkey", "n_regionkey"),)),
            ),
        ),
    ),
    RelationshipGroup(
        table=SUPPLIER,
        source=EndpointSpec(SUPPLIER.label, (("s_suppkey", "s_suppkey"),)),
        edges=(
            EdgeSpec(
                "SUPPLIER_NATION",
                EndpointSpec(NATION.label, (("n_nationkey", "s_nationkey"),)),
            ),
        ),
    ),
    RelationshipGroup(
        table=CUSTOMER,
        source=EndpointSpec(CUSTOMER.label, (("c_custkey", "c_custkey"),)),
        edges=(
            EdgeSpec(
                "CUSTOMER_NATION",
                EndpointSpec(NATION.label, (("n_nationkey", "c_nationkey"),)),
            ),
        ),
    ),
    RelationshipGroup(
        table=ORDERS,
        source=EndpointSpec(ORDERS.label, (("o_orderkey", "o_orderkey"),)),
        edges=(
            EdgeSpec(
                "ORDERS_CUSTOMER",
                EndpointSpec(CUSTOMER.label, (("c_custkey", "o_custkey"),)),
            ),
        ),
    ),
    RelationshipGroup(
        table=PARTSUPP,
        source=EndpointSpec(
            PARTSUPP.label,
            (
                ("ps_partkey", "ps_partkey"),
                ("ps_suppkey", "ps_suppkey"),
            ),
        ),
        edges=(
            EdgeSpec(
                "PARTSUPP_PART",
                EndpointSpec(PART.label, (("p_partkey", "ps_partkey"),)),
            ),
            EdgeSpec(
                "PARTSUPP_SUPPLIER",
                EndpointSpec(SUPPLIER.label, (("s_suppkey", "ps_suppkey"),)),
            ),
        ),
    ),
    RelationshipGroup(
        table=LINEITEM,
        source=EndpointSpec(
            LINEITEM.label,
            (
                ("l_orderkey", "l_orderkey"),
                ("l_linenumber", "l_linenumber"),
            ),
        ),
        edges=(
            EdgeSpec(
                "LINEITEM_ORDERS",
                EndpointSpec(ORDERS.label, (("o_orderkey", "l_orderkey"),)),
            ),
            EdgeSpec(
                "LINEITEM_PARTSUPP",
                EndpointSpec(
                    PARTSUPP.label,
                    (
                        ("ps_partkey", "l_partkey"),
                        ("ps_suppkey", "l_suppkey"),
                    ),
                ),
            ),
        ),
    ),
)


def iter_tbl_rows(data_dir: Path, table: TableSpec) -> Iterator[dict[str, Any]]:
    """Yield typed property dictionaries from one TPC-H .tbl file."""

    path = data_dir / table.filename
    expected_columns = len(table.columns) + 1  # dbgen adds a trailing "|".

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
                    f"{path}:{line_number}: expected {len(table.columns)} fields "
                    "followed by a trailing '|', "
                    f"but found {len(raw_row)} parsed fields"
                )

            typed_row: dict[str, Any] = {}
            for column, raw_value in zip(table.columns, raw_row[:-1], strict=True):
                try:
                    typed_row[column.name] = column.converter(raw_value)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise TblFormatError(
                        f"{path}:{line_number}: invalid value for "
                        f"{column.name}: {raw_value!r} ({exc})"
                    ) from exc
            yield typed_row


def iter_batches(
    rows: Iterable[dict[str, Any]], batch_size: int
) -> Iterator[list[dict[str, Any]]]:
    """Yield lists of at most batch_size rows (compatible with Python 3.10+)."""

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    batch: list[dict[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def property_map(keys: tuple[tuple[str, str], ...]) -> str:
    return (
        "{"
        + ", ".join(
            f"{node_property}: row.{row_property}"
            for node_property, row_property in keys
        )
        + "}"
    )


def node_query(table: TableSpec, merge: bool) -> str:
    if merge:
        keys = tuple((field, field) for field in table.key_fields)
        return (
            "UNWIND $rows AS row\n"
            f"MERGE (n:{table.label} {property_map(keys)})\n"
            "SET n += row"
        )
    return "UNWIND $rows AS row\n" f"CREATE (n:{table.label})\n" "SET n = row"


def constraint_query(table: TableSpec) -> str:
    if len(table.key_fields) == 1:
        key_expression = f"n.{table.key_fields[0]}"
    else:
        properties = ", ".join(f"n.{field}" for field in table.key_fields)
        key_expression = f"({properties})"
    return (
        f"CREATE CONSTRAINT {table.constraint_name} IF NOT EXISTS\n"
        f"FOR (n:{table.label})\n"
        f"REQUIRE {key_expression} IS NODE KEY"
    )


def relationship_query(group: RelationshipGroup) -> str:
    lines = [
        "UNWIND $rows AS row",
        f"MATCH (source:{group.source.label} {property_map(group.source.keys)})",
    ]
    for index, edge in enumerate(group.edges):
        target_name = f"target_{index}"
        lines.append(
            f"MATCH ({target_name}:{edge.target.label} "
            f"{property_map(edge.target.keys)})"
        )
    for index, edge in enumerate(group.edges):
        target_name = f"target_{index}"
        lines.append(f"MERGE (source)-[:{edge.relationship_type}]->({target_name})")
    lines.append("RETURN count(*) AS matched_rows")
    return "\n".join(lines)


def execute_query(
    driver: Any,
    database: str,
    query: str,
    parameters: dict[str, Any] | None = None,
) -> Any:
    return driver.execute_query(
        query,
        parameters_=parameters or {},
        database_=database,
    )


def get_database_status(
    driver: Any,
    database: str,
) -> tuple[str, str] | None:
    result = execute_query(
        driver,
        SYSTEM_DATABASE,
        """
        SHOW DATABASES
        YIELD name, currentStatus, statusMessage
        WHERE name = $database
        RETURN currentStatus, statusMessage
        """,
        {"database": database},
    )
    if not result.records:
        return None

    record = result.records[0]
    return str(record["currentStatus"]), str(record["statusMessage"] or "")


def ensure_database_exists(driver: Any, database: str) -> str:
    database = database.lower()
    status = get_database_status(driver, database)

    if status is None:
        print(f"Database {database!r} does not exist; creating it")
        execute_query(
            driver,
            SYSTEM_DATABASE,
            "CREATE DATABASE $database IF NOT EXISTS WAIT 30 SECONDS",
            {"database": database},
        )
        status = get_database_status(driver, database)
    else:
        print(f"Database {database!r} already exists")

    if status is None:
        raise RuntimeError(
            f"database {database!r} was not found after the create command"
        )

    current_status, status_message = status
    if current_status != "online":
        detail = f": {status_message}" if status_message else ""
        raise RuntimeError(
            f"database {database!r} is not online "
            f"(status={current_status!r}){detail}"
        )

    print(f"Database {database!r} is online")
    return database


def existing_tpch_node_count(driver: Any, database: str) -> int:
    labels = [table.label for table in NODE_TABLES]
    result = execute_query(
        driver,
        database,
        """
        MATCH (n)
        WHERE any(label IN labels(n) WHERE label IN $tpch_labels)
        RETURN count(n) AS node_count
        """,
        {"tpch_labels": labels},
    )
    return int(result.records[0]["node_count"])


def load_nodes(
    driver: Any,
    database: str,
    data_dir: Path,
    batch_size: int,
    merge: bool,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    action = "MERGE" if merge else "CREATE"

    for table in NODE_TABLES:
        count = 0
        query = node_query(table, merge=merge)
        for batch in iter_batches(iter_tbl_rows(data_dir, table), batch_size):
            execute_query(driver, database, query, {"rows": batch})
            count += len(batch)
        counts[table.label] = count
        print(f"  {action:<6} {table.label:<11} {count:>8,} rows")
    return counts


def create_constraints(driver: Any, database: str) -> None:
    for table in NODE_TABLES:
        execute_query(driver, database, constraint_query(table))
        print(f"  constraint {table.constraint_name}")


def edge_id(group: RelationshipGroup, edge: EdgeSpec) -> str:
    return f"{group.source.label}-[:{edge.relationship_type}]->" f"{edge.target.label}"


def load_relationships(
    driver: Any,
    database: str,
    data_dir: Path,
    batch_size: int,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    identifier_width = max(
        len(edge_id(group, edge))
        for group in RELATIONSHIP_GROUPS
        for edge in group.edges
    )

    for group in RELATIONSHIP_GROUPS:
        processed = 0
        query = relationship_query(group)
        for batch in iter_batches(iter_tbl_rows(data_dir, group.table), batch_size):
            result = execute_query(driver, database, query, {"rows": batch})
            matched = int(result.records[0]["matched_rows"])
            if matched != len(batch):
                raise ImportValidationError(
                    f"{group.table.filename}: only {matched} of {len(batch)} rows "
                    "matched every relationship endpoint. Check the source "
                    "foreign keys and previously imported nodes."
                )
            processed += len(batch)

        for edge in group.edges:
            identifier = edge_id(group, edge)
            counts[identifier] = processed
            print(f"  {identifier:<{identifier_width}} {processed:>8,} rows")
    return counts


def validate_import(
    driver: Any,
    database: str,
    expected_nodes: dict[str, int],
    expected_relationships: dict[str, int],
) -> tuple[int, int]:
    problems: list[str] = []

    for table in NODE_TABLES:
        result = execute_query(
            driver,
            database,
            f"MATCH (n:{table.label}) RETURN count(n) AS actual_count",
        )
        actual = int(result.records[0]["actual_count"])
        expected = expected_nodes[table.label]
        if actual != expected:
            problems.append(
                f"node {table.label}: expected {expected:,}, found {actual:,}"
            )

    for group in RELATIONSHIP_GROUPS:
        for edge in group.edges:
            identifier = edge_id(group, edge)
            result = execute_query(
                driver,
                database,
                f"""
                MATCH (:{group.source.label})
                      -[r:{edge.relationship_type}]->
                      (:{edge.target.label})
                RETURN count(r) AS actual_count
                """,
            )
            actual = int(result.records[0]["actual_count"])
            expected = expected_relationships[identifier]
            if actual != expected:
                problems.append(
                    f"relationship {identifier}: expected {expected:,}, "
                    f"found {actual:,}"
                )

    if problems:
        details = "\n  - ".join(problems)
        raise ImportValidationError(
            "Imported graph count validation failed:\n  - " + details
        )

    return sum(expected_nodes.values()), sum(expected_relationships.values())


def validate_source_files(data_dir: Path) -> None:
    if not data_dir.is_dir():
        raise FileNotFoundError(f"TPC-H data directory does not exist: {data_dir}")

    missing = [
        str(data_dir / table.filename)
        for table in NODE_TABLES
        if not (data_dir / table.filename).is_file()
    ]
    if missing:
        raise FileNotFoundError(
            "Missing required TPC-H files:\n  - " + "\n  - ".join(missing)
        )


def validate_source_only(data_dir: Path) -> int:
    total = 0
    print(f"Validating TPC-H files in {data_dir}")
    for table in NODE_TABLES:
        count = sum(1 for _ in iter_tbl_rows(data_dir, table))
        total += count
        print(f"  {table.filename:<14} {count:>8,} rows")
    print(f"Source validation succeeded: {total:,} rows")
    return total


def positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import TPC-H .tbl data into a Neo4j property graph."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"directory containing the eight .tbl files (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--uri",
        default=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        help="Neo4j URI (default: NEO4J_URI or bolt://localhost:7687)",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("NEO4J_USER", "neo4j"),
        help="Neo4j user (default: NEO4J_USER or neo4j)",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("NEO4J_DATABASE") or DATABASE_NAME,
        help=(
            "Neo4j database, created automatically when missing "
            f"(default: NEO4J_DATABASE or {DATABASE_NAME})"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=positive_int,
        default=1_000,
        help="rows sent in each transaction (default: 1000)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="parse and type-check the .tbl files without connecting to Neo4j",
    )
    return parser.parse_args(argv)


def run_import(args: argparse.Namespace, password: str) -> None:
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise RuntimeError(
            "The Neo4j Python driver is not installed. Activate the project "
            "environment and run: pip install neo4j"
        ) from exc

    data_dir = args.data_dir.expanduser().resolve()
    print(f"Connecting to {args.uri} as {args.user!r}; " f"database={args.database!r}")

    with GraphDatabase.driver(args.uri, auth=(args.user, password)) as driver:
        driver.verify_connectivity()
        database = ensure_database_exists(driver, args.database)
        existing_nodes = existing_tpch_node_count(driver, database)

        if existing_nodes == 0:
            print("[1/4] Creating nodes")
            node_counts = load_nodes(
                driver,
                database,
                data_dir,
                args.batch_size,
                merge=False,
            )

            print("[2/4] Adding node key constraints")
            create_constraints(driver, database)
        else:
            print(
                f"WARNING: Found {existing_nodes:,} existing TPC-H nodes. "
                "A clean import normally expects an empty or dedicated database. "
                "The loader is entering MERGE mode, so nodes with matching "
                "primary keys may be updated.",
                file=sys.stderr,
            )
            print("[1/4] Ensuring node key constraints")
            create_constraints(driver, database)

            print("[2/4] Merging nodes")
            node_counts = load_nodes(
                driver,
                database,
                data_dir,
                args.batch_size,
                merge=True,
            )

        print("[3/4] Creating relationships")
        relationship_counts = load_relationships(
            driver,
            database,
            data_dir,
            args.batch_size,
        )

        print("[4/4] Validating imported graph")
        node_total, relationship_total = validate_import(
            driver,
            database,
            node_counts,
            relationship_counts,
        )

    print(
        "  Import succeeded: "
        f"{node_total:,} nodes and {relationship_total:,} relationships"
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    data_dir = args.data_dir.expanduser().resolve()

    try:
        validate_source_files(data_dir)
        if args.validate_only:
            validate_source_only(data_dir)
            return 0

        password = DEBUG_NEO4J_PASSWORD or os.getenv("NEO4J_PASSWORD")
        if not password:
            password = getpass(f"Neo4j password for {args.user}: ")
        run_import(args, password)
        return 0
    except KeyboardInterrupt:
        print("\nImport cancelled.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
