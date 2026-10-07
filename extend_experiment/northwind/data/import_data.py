#!/usr/bin/env python3
"""Import the Northwind CSV data into Neo4j.

This script replaces the three standalone Cypher files previously used for
the case study:

* ``create_constraints.txt``;
* ``import_nodes.txt``; and
* ``create_edges.txt``.

The CSV files are read locally by Python and sent to Neo4j in parameterized
``UNWIND`` batches.  Consequently, the files do not need to be copied into
Neo4j's server-side import directory and no ``.txt`` file is read at runtime.

The imported graph deliberately matches the original model: 11 table-node
labels and 10 relationship types.  ``Employee.ReportsTo`` remains a property
and is not converted into an additional relationship.

Imports are retry-safe.  Nodes are merged by their primary keys, relationships
are merged by their endpoints, and the final graph is checked against the
source row counts.  Literal ``NULL`` values become absent Neo4j properties,
``Products.Discontinued`` is converted from 0/1 to Boolean, and date/time
strings are format-checked while remaining strings for compatibility with the
original Cypher importer.  Use a dedicated database: final validation rejects
extra Northwind nodes or relationships that are not present in these CSVs.

Examples:

    # Check all CSV files without connecting to Neo4j.
    python import_data.py --validate-only

    # Neo4j Enterprise (creates "northwind" when it does not exist).
    python import_data.py --database northwind

    # Neo4j Community (named databases cannot be created).
    python import_data.py --constraint-type unique --database neo4j
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from getpass import getpass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence


DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "dataset"
DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_DATABASE = "northwind"
DEFAULT_BATCH_SIZE = 1_000
SYSTEM_DATABASE = "system"
DATABASE_CREATE_WAIT_SECONDS = 30

# Local debugging only. Never commit a real password here.
DEBUG_NEO4J_PASSWORD: str | None = ""

NULL_VALUES = frozenset({"", "NULL"})


class CsvFormatError(ValueError):
    """Raised when a Northwind CSV file does not match its declared schema."""


class ImportValidationError(RuntimeError):
    """Raised when source or imported graph validation fails."""


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

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)


@dataclass(frozen=True)
class EndpointSpec:
    table: TableSpec
    # Each item is (node property, source CSV row property).
    keys: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class RelationshipSpec:
    # The CSV row table carries the foreign key and is always the relationship
    # source; the target is the referenced table.
    row_table: TableSpec
    source: EndpointSpec
    relationship_type: str
    target: EndpointSpec


@dataclass
class SourceData:
    rows_by_filename: dict[str, tuple[dict[str, Any], ...]]
    relationship_rows: dict[str, tuple[dict[str, Any], ...]]

    @property
    def node_counts(self) -> dict[str, int]:
        return {
            table.label: len(self.rows_by_filename[table.filename])
            for table in NODE_TABLES
        }

    @property
    def relationship_counts(self) -> dict[str, int]:
        return {
            relationship_id(relationship): len(
                self.relationship_rows[relationship_id(relationship)]
            )
            for relationship in RELATIONSHIPS
        }


def as_text(raw: str) -> str:
    """Preserve source text exactly, including meaningful surrounding spaces."""

    return raw


def as_int(raw: str) -> int:
    return int(raw)


def as_float(raw: str) -> float:
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("value must be finite")
    return value


def as_boolean(raw: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true"}:
        return True
    if normalized in {"0", "false"}:
        return False
    raise ValueError("expected 0, 1, true, or false")


def as_datetime_text(raw: str) -> str:
    """Validate an ISO-like timestamp while preserving the original string."""

    datetime.fromisoformat(raw)
    return raw


def column(name: str, converter: Converter = as_text) -> ColumnSpec:
    return ColumnSpec(name, converter)


CATEGORY = TableSpec(
    filename="categories.csv",
    label="Category",
    constraint_name="category_node_key",
    columns=(
        column("CategoryID", as_int),
        column("CategoryName"),
        column("Description"),
        column("Picture"),
    ),
    key_fields=("CategoryID",),
)

CUSTOMER = TableSpec(
    filename="customers.csv",
    label="Customer",
    constraint_name="customer_node_key",
    columns=(
        column("CustomerID"),
        column("CompanyName"),
        column("ContactName"),
        column("ContactTitle"),
        column("Address"),
        column("City"),
        column("Region"),
        column("PostalCode"),
        column("Country"),
        column("Phone"),
        column("Fax"),
    ),
    key_fields=("CustomerID",),
)

EMPLOYEE = TableSpec(
    filename="employees.csv",
    label="Employee",
    constraint_name="employee_node_key",
    columns=(
        column("EmployeeID", as_int),
        column("LastName"),
        column("FirstName"),
        column("Title"),
        column("TitleOfCourtesy"),
        column("BirthDate", as_datetime_text),
        column("HireDate", as_datetime_text),
        column("Address"),
        column("City"),
        column("Region"),
        column("PostalCode"),
        column("Country"),
        column("HomePhone"),
        column("Extension"),
        column("Photo"),
        column("Notes"),
        column("ReportsTo", as_int),
        column("PhotoPath"),
    ),
    key_fields=("EmployeeID",),
)

EMPLOYEE_TERRITORY = TableSpec(
    filename="employee_territories.csv",
    label="EmployeeTerritory",
    constraint_name="employee_territory_node_key",
    columns=(
        column("EmployeeID", as_int),
        # TerritoryID is text so leading zeroes such as "01581" survive.
        column("TerritoryID"),
    ),
    key_fields=("EmployeeID", "TerritoryID"),
)

ORDER_DETAIL = TableSpec(
    filename="order_details.csv",
    label="OrderDetail",
    constraint_name="order_detail_node_key",
    columns=(
        column("OrderID", as_int),
        column("ProductID", as_int),
        column("UnitPrice", as_float),
        column("Quantity", as_int),
        column("Discount", as_float),
    ),
    key_fields=("OrderID", "ProductID"),
)

ORDER = TableSpec(
    filename="orders.csv",
    label="Order",
    constraint_name="order_node_key",
    columns=(
        column("OrderID", as_int),
        column("CustomerID"),
        column("EmployeeID", as_int),
        column("OrderDate", as_datetime_text),
        column("RequiredDate", as_datetime_text),
        column("ShippedDate", as_datetime_text),
        column("ShipVia", as_int),
        column("Freight", as_float),
        column("ShipName"),
        column("ShipAddress"),
        column("ShipCity"),
        column("ShipRegion"),
        column("ShipPostalCode"),
        column("ShipCountry"),
    ),
    key_fields=("OrderID",),
)

PRODUCT = TableSpec(
    filename="products.csv",
    label="Product",
    constraint_name="product_node_key",
    columns=(
        column("ProductID", as_int),
        column("ProductName"),
        column("SupplierID", as_int),
        column("CategoryID", as_int),
        column("QuantityPerUnit"),
        column("UnitPrice", as_float),
        column("UnitsInStock", as_int),
        column("UnitsOnOrder", as_int),
        column("ReorderLevel", as_int),
        column("Discontinued", as_boolean),
    ),
    key_fields=("ProductID",),
)

REGION = TableSpec(
    filename="regions.csv",
    label="Region",
    constraint_name="region_node_key",
    columns=(
        column("RegionID", as_int),
        column("RegionDescription"),
    ),
    key_fields=("RegionID",),
)

SHIPPER = TableSpec(
    filename="shippers.csv",
    label="Shipper",
    constraint_name="shipper_node_key",
    columns=(
        column("ShipperID", as_int),
        column("CompanyName"),
        column("Phone"),
    ),
    key_fields=("ShipperID",),
)

SUPPLIER = TableSpec(
    filename="suppliers.csv",
    label="Supplier",
    constraint_name="supplier_node_key",
    columns=(
        column("SupplierID", as_int),
        column("CompanyName"),
        column("ContactName"),
        column("ContactTitle"),
        column("Address"),
        column("City"),
        column("Region"),
        column("PostalCode"),
        column("Country"),
        column("Phone"),
        column("Fax"),
        column("HomePage"),
    ),
    key_fields=("SupplierID",),
)

TERRITORY = TableSpec(
    filename="territories.csv",
    label="Territory",
    constraint_name="territory_node_key",
    columns=(
        column("TerritoryID"),
        column("TerritoryDescription"),
        column("RegionID", as_int),
    ),
    key_fields=("TerritoryID",),
)

NODE_TABLES = (
    CATEGORY,
    CUSTOMER,
    EMPLOYEE,
    EMPLOYEE_TERRITORY,
    ORDER_DETAIL,
    ORDER,
    PRODUCT,
    REGION,
    SHIPPER,
    SUPPLIER,
    TERRITORY,
)


def endpoint(
    table: TableSpec,
    *keys: tuple[str, str],
) -> EndpointSpec:
    return EndpointSpec(table, tuple(keys))


# Relationship order matches the legacy file. Every direction follows the
# foreign key: referencing row table -> referenced table.
RELATIONSHIPS = (
    RelationshipSpec(
        row_table=ORDER,
        source=endpoint(ORDER, ("OrderID", "OrderID")),
        relationship_type="PLACED_BY",
        target=endpoint(CUSTOMER, ("CustomerID", "CustomerID")),
    ),
    RelationshipSpec(
        row_table=ORDER_DETAIL,
        source=endpoint(
            ORDER_DETAIL,
            ("OrderID", "OrderID"),
            ("ProductID", "ProductID"),
        ),
        relationship_type="OF_ORDER",
        target=endpoint(ORDER, ("OrderID", "OrderID")),
    ),
    RelationshipSpec(
        row_table=ORDER_DETAIL,
        source=endpoint(
            ORDER_DETAIL,
            ("OrderID", "OrderID"),
            ("ProductID", "ProductID"),
        ),
        relationship_type="OF_PRODUCT",
        target=endpoint(PRODUCT, ("ProductID", "ProductID")),
    ),
    RelationshipSpec(
        row_table=PRODUCT,
        source=endpoint(PRODUCT, ("ProductID", "ProductID")),
        relationship_type="IN_CATEGORY",
        target=endpoint(CATEGORY, ("CategoryID", "CategoryID")),
    ),
    RelationshipSpec(
        row_table=PRODUCT,
        source=endpoint(PRODUCT, ("ProductID", "ProductID")),
        relationship_type="SUPPLIED_BY",
        target=endpoint(SUPPLIER, ("SupplierID", "SupplierID")),
    ),
    RelationshipSpec(
        row_table=ORDER,
        source=endpoint(ORDER, ("OrderID", "OrderID")),
        relationship_type="HANDLED_BY",
        target=endpoint(EMPLOYEE, ("EmployeeID", "EmployeeID")),
    ),
    RelationshipSpec(
        row_table=ORDER,
        source=endpoint(ORDER, ("OrderID", "OrderID")),
        relationship_type="SHIPPED_BY",
        target=endpoint(SHIPPER, ("ShipperID", "ShipVia")),
    ),
    RelationshipSpec(
        row_table=EMPLOYEE_TERRITORY,
        source=endpoint(
            EMPLOYEE_TERRITORY,
            ("EmployeeID", "EmployeeID"),
            ("TerritoryID", "TerritoryID"),
        ),
        relationship_type="HAS_TERRITORY",
        target=endpoint(EMPLOYEE, ("EmployeeID", "EmployeeID")),
    ),
    RelationshipSpec(
        row_table=EMPLOYEE_TERRITORY,
        source=endpoint(
            EMPLOYEE_TERRITORY,
            ("EmployeeID", "EmployeeID"),
            ("TerritoryID", "TerritoryID"),
        ),
        relationship_type="IN_TERRITORY",
        target=endpoint(TERRITORY, ("TerritoryID", "TerritoryID")),
    ),
    RelationshipSpec(
        row_table=TERRITORY,
        source=endpoint(TERRITORY, ("TerritoryID", "TerritoryID")),
        relationship_type="IN_REGION",
        target=endpoint(REGION, ("RegionID", "RegionID")),
    ),
)


def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def iter_csv_rows(
    data_dir: Path,
    table: TableSpec,
) -> Iterator[dict[str, Any]]:
    """Yield typed dictionaries from one pipe-delimited Northwind CSV file."""

    path = data_dir / table.filename
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(
            source,
            delimiter="|",
            quoting=csv.QUOTE_NONE,
            strict=True,
        )
        try:
            raw_header = next(reader)
        except StopIteration as exc:
            raise CsvFormatError(f"{path}: file is empty") from exc
        except csv.Error as exc:
            raise CsvFormatError(f"{path}: could not parse header ({exc})") from exc

        header = tuple(raw_header)
        if header != table.column_names:
            raise CsvFormatError(
                f"{path}: expected header {table.column_names!r}, "
                f"but found {header!r}"
            )

        try:
            for line_number, raw_row in enumerate(reader, start=2):
                if len(raw_row) != len(table.columns):
                    raise CsvFormatError(
                        f"{path}:{line_number}: expected {len(table.columns)} "
                        f"fields, but found {len(raw_row)}"
                    )

                typed_row: dict[str, Any] = {}
                for spec, raw_value in zip(table.columns, raw_row, strict=True):
                    if raw_value in NULL_VALUES:
                        value = None
                    else:
                        try:
                            value = spec.converter(raw_value)
                        except (TypeError, ValueError, OverflowError) as exc:
                            raise CsvFormatError(
                                f"{path}:{line_number}: invalid value for "
                                f"{spec.name}: {raw_value!r} ({exc})"
                            ) from exc
                    typed_row[spec.name] = value

                missing_keys = [
                    key for key in table.key_fields if typed_row[key] is None
                ]
                if missing_keys:
                    raise CsvFormatError(
                        f"{path}:{line_number}: primary-key fields cannot be "
                        f"NULL: {', '.join(missing_keys)}"
                    )
                yield typed_row
        except csv.Error as exc:
            raise CsvFormatError(f"{path}: CSV parsing failed ({exc})") from exc


def iter_batches(
    rows: Iterable[dict[str, Any]],
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
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


def validate_source_files(data_dir: Path) -> None:
    if not data_dir.is_dir():
        raise FileNotFoundError(f"Northwind data directory does not exist: {data_dir}")

    missing = [
        str(data_dir / table.filename)
        for table in NODE_TABLES
        if not (data_dir / table.filename).is_file()
    ]
    if missing:
        raise FileNotFoundError(
            "Missing required Northwind CSV files:\n  - " + "\n  - ".join(missing)
        )


def validate_configuration() -> None:
    """Fail early if a hard-coded relationship mapping is internally invalid."""

    known_tables = set(NODE_TABLES)
    relationship_types: set[str] = set()
    for relationship in RELATIONSHIPS:
        if relationship.row_table not in known_tables:
            raise AssertionError(
                f"unknown relationship row table: {relationship.row_table.label}"
            )
        if relationship.source.table is not relationship.row_table:
            raise AssertionError(
                f"{relationship.relationship_type} must point from its "
                f"referencing row table {relationship.row_table.label!r}; "
                f"found source {relationship.source.table.label!r}"
            )
        if relationship.relationship_type in relationship_types:
            raise AssertionError(
                f"duplicate relationship type: {relationship.relationship_type}"
            )
        relationship_types.add(relationship.relationship_type)

        row_columns = set(relationship.row_table.column_names)
        for role, endpoint_spec in (
            ("source", relationship.source),
            ("target", relationship.target),
        ):
            if endpoint_spec.table not in known_tables:
                raise AssertionError(
                    f"{relationship.relationship_type} has unknown {role} table"
                )
            endpoint_properties = tuple(
                node_property for node_property, _ in endpoint_spec.keys
            )
            if endpoint_properties != endpoint_spec.table.key_fields:
                raise AssertionError(
                    f"{relationship.relationship_type} {role} endpoint must use "
                    f"the complete key {endpoint_spec.table.key_fields!r}, "
                    f"not {endpoint_properties!r}"
                )
            unknown_row_properties = [
                row_property
                for _, row_property in endpoint_spec.keys
                if row_property not in row_columns
            ]
            if unknown_row_properties:
                raise AssertionError(
                    f"{relationship.relationship_type} references unknown "
                    f"{relationship.row_table.filename} fields: "
                    f"{unknown_row_properties!r}"
                )


def row_key(row: dict[str, Any], fields: Sequence[str]) -> tuple[Any, ...]:
    return tuple(row[field] for field in fields)


def endpoint_key(
    endpoint_spec: EndpointSpec,
    source_row: dict[str, Any],
) -> tuple[Any, ...]:
    return tuple(
        source_row[row_property]
        for _, row_property in endpoint_spec.keys
    )


def endpoint_keys_in_source(
    endpoint_spec: EndpointSpec,
    rows_by_filename: dict[str, tuple[dict[str, Any], ...]],
) -> set[tuple[Any, ...]]:
    node_properties = tuple(
        node_property for node_property, _ in endpoint_spec.keys
    )
    return {
        row_key(row, node_properties)
        for row in rows_by_filename[endpoint_spec.table.filename]
    }


def relationship_id(relationship: RelationshipSpec) -> str:
    return (
        f"{relationship.source.table.label}"
        f"-[:{relationship.relationship_type}]->"
        f"{relationship.target.table.label}"
    )


def relationship_payload_fields(
    relationship: RelationshipSpec,
) -> tuple[str, ...]:
    fields: list[str] = []
    for endpoint_spec in (relationship.source, relationship.target):
        for _, row_property in endpoint_spec.keys:
            if row_property not in fields:
                fields.append(row_property)
    return tuple(fields)


def prepare_source_data(data_dir: Path) -> SourceData:
    """Parse all files and validate primary and foreign keys before any writes."""

    validate_configuration()
    validate_source_files(data_dir)

    rows_by_filename: dict[str, tuple[dict[str, Any], ...]] = {}
    for table in NODE_TABLES:
        rows = tuple(iter_csv_rows(data_dir, table))
        seen_keys: set[tuple[Any, ...]] = set()
        for row in rows:
            key = row_key(row, table.key_fields)
            if key in seen_keys:
                raise ImportValidationError(
                    f"{table.filename}: duplicate primary key {key!r}"
                )
            seen_keys.add(key)
        rows_by_filename[table.filename] = rows

    relationship_rows: dict[str, tuple[dict[str, Any], ...]] = {}
    for relationship in RELATIONSHIPS:
        identifier = relationship_id(relationship)
        valid_source_keys = endpoint_keys_in_source(
            relationship.source,
            rows_by_filename,
        )
        valid_target_keys = endpoint_keys_in_source(
            relationship.target,
            rows_by_filename,
        )
        payload_fields = relationship_payload_fields(relationship)

        payloads: list[dict[str, Any]] = []
        seen_endpoint_pairs: set[
            tuple[tuple[Any, ...], tuple[Any, ...]]
        ] = set()
        for row in rows_by_filename[relationship.row_table.filename]:
            source_key = endpoint_key(relationship.source, row)
            target_key = endpoint_key(relationship.target, row)

            # This matches the old MATCH/WHERE behavior for nullable foreign
            # keys: no relationship is created when either endpoint is NULL.
            if any(value is None for value in (*source_key, *target_key)):
                continue
            if source_key not in valid_source_keys:
                raise ImportValidationError(
                    f"{identifier}: source row has an orphan key {source_key!r}"
                )
            if target_key not in valid_target_keys:
                raise ImportValidationError(
                    f"{identifier}: source row has an orphan target key "
                    f"{target_key!r}"
                )

            endpoint_pair = (source_key, target_key)
            if endpoint_pair in seen_endpoint_pairs:
                raise ImportValidationError(
                    f"{identifier}: duplicate relationship endpoints "
                    f"{endpoint_pair!r}"
                )
            seen_endpoint_pairs.add(endpoint_pair)
            payloads.append({field: row[field] for field in payload_fields})

        relationship_rows[identifier] = tuple(payloads)

    return SourceData(
        rows_by_filename=rows_by_filename,
        relationship_rows=relationship_rows,
    )


def print_source_summary(data_dir: Path, source_data: SourceData) -> None:
    print(f"Validated Northwind CSV files in {data_dir}")
    for table in NODE_TABLES:
        count = source_data.node_counts[table.label]
        print(f"  node {table.label:<19} {count:>8,} rows")

    node_total = sum(source_data.node_counts.values())
    relationship_total = sum(source_data.relationship_counts.values())
    print(
        f"Source validation succeeded: {node_total:,} nodes, "
        f"{relationship_total:,} relationships"
    )


def property_map(keys: tuple[tuple[str, str], ...]) -> str:
    return (
        "{"
        + ", ".join(
            f"{quote_ident(node_property)}: row.{quote_ident(row_property)}"
            for node_property, row_property in keys
        )
        + "}"
    )


def constraint_query(table: TableSpec, constraint_type: str) -> str:
    if len(table.key_fields) == 1:
        key_expression = f"n.{quote_ident(table.key_fields[0])}"
    else:
        properties = ", ".join(
            f"n.{quote_ident(field)}" for field in table.key_fields
        )
        key_expression = f"({properties})"

    constraint_predicate = {
        "node-key": "IS NODE KEY",
        "unique": "IS UNIQUE",
    }[constraint_type]
    return (
        f"CREATE CONSTRAINT {quote_ident(table.constraint_name)} IF NOT EXISTS\n"
        f"FOR (n:{quote_ident(table.label)})\n"
        f"REQUIRE {key_expression} {constraint_predicate}"
    )


def node_query(table: TableSpec) -> str:
    key_map = property_map(tuple((field, field) for field in table.key_fields))
    return "\n".join(
        (
            "UNWIND $rows AS row",
            f"MERGE (n:{quote_ident(table.label)} {key_map})",
            "SET n += row",
            "RETURN count(*) AS merged_rows",
        )
    )


def relationship_query(relationship: RelationshipSpec) -> str:
    return "\n".join(
        (
            "UNWIND $rows AS row",
            f"MATCH (source:{quote_ident(relationship.source.table.label)} "
            f"{property_map(relationship.source.keys)})",
            f"MATCH (target:{quote_ident(relationship.target.table.label)} "
            f"{property_map(relationship.target.keys)})",
            f"MERGE (source)-[:{quote_ident(relationship.relationship_type)}]"
            "->(target)",
            "RETURN count(*) AS matched_rows",
        )
    )


def execute_query(
    driver: Any,
    database: str,
    query: str,
    parameters: dict[str, Any] | None = None,
) -> list[Any]:
    result = driver.execute_query(
        query,
        parameters_=parameters or {},
        database_=database,
    )
    return list(result.records)


def get_database_status(
    driver: Any,
    database: str,
) -> tuple[str, str] | None:
    records = execute_query(
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
    if not records:
        return None

    record = records[0]
    return (
        str(record["currentStatus"]).lower(),
        str(record["statusMessage"] or ""),
    )


def ensure_database_exists(driver: Any, database: str) -> str:
    """Create a missing Enterprise database and require it to be online."""

    database = database.strip().lower()
    if not database:
        raise ValueError("database name cannot be empty")
    if database == SYSTEM_DATABASE:
        raise ValueError("the system database cannot contain Northwind data")

    status = get_database_status(driver, database)
    if status is None:
        print(f"Database {database!r} does not exist; creating it")
        try:
            execute_query(
                driver,
                SYSTEM_DATABASE,
                (
                    "CREATE DATABASE $database IF NOT EXISTS "
                    f"WAIT {DATABASE_CREATE_WAIT_SECONDS} SECONDS"
                ),
                {"database": database},
            )
        except Exception as exc:
            raise RuntimeError(
                f"database {database!r} does not exist and could not be "
                "created. Automatic named-database creation requires Neo4j "
                "Enterprise and a user with database-management privileges. "
                "For Neo4j Community, use --database neo4j."
            ) from exc
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


def single_count(records: Sequence[Any], field: str, context: str) -> int:
    if len(records) != 1:
        raise RuntimeError(
            f"{context}: expected one result record, found {len(records)}"
        )
    return int(records[0][field])


def create_constraints(
    driver: Any,
    database: str,
    constraint_type: str,
) -> None:
    for table in NODE_TABLES:
        try:
            execute_query(
                driver,
                database,
                constraint_query(table, constraint_type),
            )
        except Exception as exc:
            advice = (
                " NODE KEY constraints require Neo4j Enterprise; for "
                "Community use --constraint-type unique."
                if constraint_type == "node-key"
                else ""
            )
            raise RuntimeError(
                f"could not create constraint {table.constraint_name!r}.{advice}"
            ) from exc

    records = execute_query(
        driver,
        database,
        """
        SHOW CONSTRAINTS
        YIELD name, type, entityType, labelsOrTypes, properties
        RETURN name, type, entityType, labelsOrTypes, properties
        """,
    )
    constraints_by_name = {str(record["name"]): record for record in records}
    allowed_types = (
        {"NODE_KEY"}
        if constraint_type == "node-key"
        else {"UNIQUENESS", "NODE_PROPERTY_UNIQUENESS"}
    )
    problems: list[str] = []
    for table in NODE_TABLES:
        record = constraints_by_name.get(table.constraint_name)
        if record is None:
            problems.append(f"{table.constraint_name}: constraint is missing")
            continue

        actual_type = str(record["type"]).upper()
        actual_entity_type = str(record["entityType"]).upper()
        actual_labels = tuple(str(value) for value in record["labelsOrTypes"])
        actual_properties = tuple(str(value) for value in record["properties"])
        if actual_type not in allowed_types:
            problems.append(
                f"{table.constraint_name}: expected {constraint_type}, "
                f"found type {actual_type}"
            )
        if actual_entity_type != "NODE":
            problems.append(
                f"{table.constraint_name}: expected NODE, "
                f"found {actual_entity_type}"
            )
        if actual_labels != (table.label,):
            problems.append(
                f"{table.constraint_name}: expected label {table.label!r}, "
                f"found {actual_labels!r}"
            )
        if actual_properties != table.key_fields:
            problems.append(
                f"{table.constraint_name}: expected properties "
                f"{table.key_fields!r}, found {actual_properties!r}"
            )

    if problems:
        raise ImportValidationError(
            "Constraint validation failed. An existing same-name constraint "
            "may have a different type or schema; it was not modified:\n  - "
            + "\n  - ".join(problems)
        )

    for table in NODE_TABLES:
        print(f"  constraint {table.constraint_name} ({constraint_type})")


def load_nodes(
    driver: Any,
    database: str,
    source_data: SourceData,
    batch_size: int,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in NODE_TABLES:
        rows = source_data.rows_by_filename[table.filename]
        processed = 0
        query = node_query(table)
        for batch in iter_batches(rows, batch_size):
            records = execute_query(
                driver,
                database,
                query,
                {"rows": batch},
            )
            merged = single_count(
                records,
                "merged_rows",
                f"node batch for {table.label}",
            )
            if merged != len(batch):
                raise ImportValidationError(
                    f"{table.filename}: Neo4j processed {merged} of "
                    f"{len(batch)} node rows"
                )
            processed += len(batch)
        counts[table.label] = processed
        print(f"  MERGE {table.label:<19} {processed:>8,} rows")
    return counts


def load_relationships(
    driver: Any,
    database: str,
    source_data: SourceData,
    batch_size: int,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    identifier_width = max(
        len(relationship_id(relationship)) for relationship in RELATIONSHIPS
    )

    for relationship in RELATIONSHIPS:
        identifier = relationship_id(relationship)
        rows = source_data.relationship_rows[identifier]
        processed = 0
        query = relationship_query(relationship)
        for batch in iter_batches(rows, batch_size):
            records = execute_query(
                driver,
                database,
                query,
                {"rows": batch},
            )
            matched = single_count(
                records,
                "matched_rows",
                f"relationship batch for {identifier}",
            )
            if matched != len(batch):
                raise ImportValidationError(
                    f"{identifier}: only {matched} of {len(batch)} rows "
                    "matched both endpoints"
                )
            processed += len(batch)
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
        records = execute_query(
            driver,
            database,
            f"MATCH (n:{quote_ident(table.label)}) "
            "RETURN count(n) AS actual_count",
        )
        actual = single_count(records, "actual_count", f"validate {table.label}")
        expected = expected_nodes[table.label]
        if actual != expected:
            problems.append(
                f"node {table.label}: expected {expected:,}, found {actual:,}"
            )

    for relationship in RELATIONSHIPS:
        identifier = relationship_id(relationship)
        records = execute_query(
            driver,
            database,
            (
                f"MATCH (:{quote_ident(relationship.source.table.label)})"
                f"-[r:{quote_ident(relationship.relationship_type)}]->"
                f"(:{quote_ident(relationship.target.table.label)}) "
                "RETURN count(r) AS actual_count"
            ),
        )
        actual = single_count(
            records,
            "actual_count",
            f"validate {identifier}",
        )
        expected = expected_relationships[identifier]
        if actual != expected:
            problems.append(
                f"relationship {identifier}: expected {expected:,}, "
                f"found {actual:,}"
            )
        records = execute_query(
            driver,
            database,
            (
                f"MATCH ()-[r:{quote_ident(relationship.relationship_type)}]"
                "->() RETURN count(r) AS actual_count"
            ),
        )
        typed_total = single_count(
            records,
            "actual_count",
            f"validate all {relationship.relationship_type} relationships",
        )
        if typed_total != expected:
            problems.append(
                f"relationship type {relationship.relationship_type}: "
                f"expected {expected:,} total edge(s), found "
                f"{typed_total:,}; unexpected endpoints or direction may "
                "remain from an older import"
            )

    if problems:
        raise ImportValidationError(
            "Imported graph count validation failed:\n  - "
            + "\n  - ".join(problems)
        )

    return sum(expected_nodes.values()), sum(expected_relationships.values())


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
        description=(
            "Import the Northwind CSV files into Neo4j without external "
            "Cypher .txt files."
        )
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=(
            "directory containing the 11 Northwind CSV files "
            f"(default: {DEFAULT_DATA_DIR})"
        ),
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
            "Neo4j database, created automatically when missing "
            f"(default: NEO4J_DATABASE or {DEFAULT_DATABASE})"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=positive_int,
        default=DEFAULT_BATCH_SIZE,
        help=f"rows sent per transaction (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--constraint-type",
        choices=("node-key", "unique"),
        default=os.getenv("NEO4J_CONSTRAINT_TYPE", "node-key"),
        help=(
            "node-key preserves the original Enterprise constraints; "
            "use unique for Neo4j Community (default: node-key)"
        ),
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="validate CSV types and keys without connecting to Neo4j",
    )
    args = parser.parse_args(argv)
    if args.constraint_type not in {"node-key", "unique"}:
        parser.error(
            "NEO4J_CONSTRAINT_TYPE must be either 'node-key' or 'unique'"
        )
    return args


def run_import(
    args: argparse.Namespace,
    password: str,
    source_data: SourceData,
) -> None:
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise RuntimeError(
            "The Neo4j Python driver is not installed. Run: "
            'python -m pip install "neo4j>=5.7"'
        ) from exc

    print(
        f"Connecting to {args.uri} as {args.user!r}; "
        f"database={args.database!r}"
    )
    with GraphDatabase.driver(args.uri, auth=(args.user, password)) as driver:
        if not hasattr(driver, "execute_query"):
            raise RuntimeError(
                "Neo4j Python Driver 5.7 or newer is required. Upgrade with: "
                'python -m pip install --upgrade "neo4j>=5.7"'
            )
        driver.verify_connectivity()
        database = ensure_database_exists(driver, args.database)

        print(f"[1/4] Creating {args.constraint_type} constraints")
        create_constraints(driver, database, args.constraint_type)

        print("[2/4] Merging nodes")
        node_counts = load_nodes(
            driver,
            database,
            source_data,
            args.batch_size,
        )

        print("[3/4] Merging relationships")
        relationship_counts = load_relationships(
            driver,
            database,
            source_data,
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
        "Import completed successfully: "
        f"{node_total:,} nodes, {relationship_total:,} relationships"
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    data_dir = args.data_dir.expanduser().resolve()

    try:
        source_data = prepare_source_data(data_dir)
        print_source_summary(data_dir, source_data)

        if args.validate_only:
            return 0

        password = os.getenv("NEO4J_PASSWORD") or DEBUG_NEO4J_PASSWORD
        if password is None:
            password = getpass(f"Neo4j password for {args.user}: ")

        run_import(args, password, source_data)
    except KeyboardInterrupt:
        print("\nImport interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        if exc.__cause__ is not None:
            print(f"CAUSE: {exc.__cause__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
