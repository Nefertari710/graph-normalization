#!/usr/bin/env python3
"""Import one :HospitalRecord node for every hospital CSV row.

Data source: https://data.cms.gov/provider-data/dataset/yv7e-xc69
Last modified: July 22, 2026
Published: August 13, 2026
"""

import csv
import os
from getpass import getpass
from pathlib import Path

from neo4j import GraphDatabase

CSV_FILE = Path(__file__).with_name("Timely_and_Effective_Care-Hospital.csv")
URI = os.getenv("NEO4J_URI") or "bolt://localhost:7687"
USER = os.getenv("NEO4J_USER") or "neo4j"
DATABASE = os.getenv("NEO4J_DATABASE") or "hospitalcsv"
BATCH_SIZE = 2_000

IMPORT_QUERY = """
UNWIND $rows AS properties
CREATE (node:HospitalRecord)
SET node = properties
RETURN count(node) AS created
"""

NODE_KEY_QUERY = """
CREATE CONSTRAINT hospital_record_node_key IF NOT EXISTS
FOR (node:HospitalRecord)
REQUIRE (node.facility_id, node.measure_id) IS NODE KEY
"""


def property_name(header):
    return header.lower().replace(" ", "_").replace("/", "_")


def read_batches():
    with CSV_FILE.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        batch = []
        for row in reader:
            batch.append({property_name(key): value for key, value in row.items()})
            if len(batch) == BATCH_SIZE:
                yield batch
                batch = []
        if batch:
            yield batch


def main():
    password = os.getenv("NEO4J_PASSWORD") or getpass(f"Neo4j password for {USER}: ")
    with GraphDatabase.driver(URI, auth=(USER, password)) as driver:
        driver.verify_connectivity()

        with driver.session(database="system") as session:
            session.run(
                "CREATE DATABASE $name IF NOT EXISTS WAIT 60 SECONDS",
                name=DATABASE,
            ).consume()

        with driver.session(database=DATABASE) as session:
            existing = session.run(
                "MATCH (n) RETURN count(n) AS count"
            ).single(strict=True)["count"]
            if existing:
                raise RuntimeError(
                    f"Database {DATABASE!r} already contains {existing:,} nodes"
                )

            session.run(NODE_KEY_QUERY).consume()

            imported = 0
            for batch in read_batches():
                imported += session.run(
                    IMPORT_QUERY, rows=batch
                ).single(strict=True)["created"]

            final_count = session.run(
                "MATCH (n:HospitalRecord) RETURN count(n) AS count"
            ).single(strict=True)["count"]

    if imported != final_count:
        raise RuntimeError(
            f"Import count mismatch: created={imported}, database={final_count}"
        )
    print(f"Imported {final_count:,} HospitalRecord nodes into {DATABASE!r}")


if __name__ == "__main__":
    main()
