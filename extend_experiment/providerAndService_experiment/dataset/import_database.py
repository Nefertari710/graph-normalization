#!/usr/bin/env python3
"""Import one :ProviderServiceRecord node for every CSV row.

Data source:
https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service/data
"""

import csv
import os
from pathlib import Path

from neo4j import GraphDatabase

CSV_FILE = Path(__file__).with_name("PHY_R26_P05_V10_D24_Prov_Svc.csv")
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = os.getenv("NEO4J_PASSWORD", "")
DATABASE = "providerandservicecsv"
BATCH_SIZE = 2_000

IMPORT_QUERY = """
UNWIND $rows AS properties
CREATE (node:ProviderServiceRecord)
SET node = properties
RETURN count(node) AS created
"""

NODE_KEY_QUERY = """
CREATE CONSTRAINT provider_service_record_node_key IF NOT EXISTS
FOR (node:ProviderServiceRecord)
REQUIRE (node.rndrng_npi, node.hcpcs_cd, node.place_of_srvc) IS NODE KEY
"""


def property_name(header):
    return header.lower().replace(" ", "_").replace("/", "_")


def read_batches():
    with CSV_FILE.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        headers = [property_name(header) for header in next(reader)]
        batch = []
        for values in reader:
            batch.append(dict(zip(headers, values)))
            if len(batch) == BATCH_SIZE:
                yield batch
                batch = []
        if batch:
            yield batch


def main():
    with GraphDatabase.driver(URI, auth=(USER, PASSWORD)) as driver:
        driver.verify_connectivity()

        with driver.session(database="system") as session:
            session.run(
                f"CREATE DATABASE {DATABASE} IF NOT EXISTS WAIT 60 SECONDS"
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
                if imported % 100_000 == 0:
                    print(f"Imported {imported:,} nodes...", flush=True)

            final_count = session.run(
                "MATCH (n:ProviderServiceRecord) RETURN count(n) AS count"
            ).single(strict=True)["count"]

    if imported != final_count:
        raise RuntimeError(
            f"Import count mismatch: created={imported}, database={final_count}"
        )
    print(
        f"Imported {final_count:,} ProviderServiceRecord nodes "
        f"into {DATABASE!r}"
    )


if __name__ == "__main__":
    main()
