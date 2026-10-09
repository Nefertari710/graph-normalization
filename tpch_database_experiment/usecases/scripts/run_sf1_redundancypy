#!/usr/bin/env python3
"""Import TPC-H SF 1 and run the selected Enterprise update experiments."""

import os
import sys
from getpass import getpass
from pathlib import Path

from neo4j import GraphDatabase

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPOSITORY_ROOT))

from tpch_database_experiment.data import import_data_from_tbl_enterprise as importer
from tpch_database_experiment.redundancy.scripts import new_redundancy

# Connection and use-case settings.
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "")  # Empty: prompt when running.
DATABASE = "tpch-sf-1-usecase"
DATA_DIR = REPOSITORY_ROOT / "tpch_database_experiment" / "data" / "tbl_sf_1"
OUTPUT = (
    REPOSITORY_ROOT / "tpch_database_experiment" / "usecases" / "results"
    / "sf1" / "redundancy" / "new_redundancy_results.json"
)
MASKS = (2, 16, 18, 32, 48, 64, 72, 128, 160, 176)
UPDATES = 50        # 50
REPEATS = 10        # 10
SKIP_IMPORT = os.getenv("SKIP_IMPORT", "0") == "1"


def main() -> None:
    if not SKIP_IMPORT:
        importer.validate_source_files(DATA_DIR)
    password = PASSWORD or getpass("Neo4j password: ")

    if not SKIP_IMPORT:
        with GraphDatabase.driver(URI, auth=(USER, password)) as driver:
            driver.verify_connectivity()
            importer.ensure_database_exists(driver, DATABASE)
            print(f"Clearing database {DATABASE} ...", flush=True)
            with driver.session(database=DATABASE) as session:
                session.run(
                    "MATCH (n) CALL (n) { DETACH DELETE n } "
                    "IN TRANSACTIONS OF 10000 ROWS"
                ).consume()
                # Recreate TPC-H primary keys as Enterprise node-key constraints.
                for table in importer.NODE_TABLES:
                    session.run(
                        f"DROP CONSTRAINT {new_redundancy.quoted(table.constraint_name)} "
                        "IF EXISTS"
                    ).consume()
                indexes = list(session.run(
                    "SHOW INDEXES YIELD name, type, owningConstraint "
                    "WHERE type <> 'LOOKUP' AND owningConstraint IS NULL RETURN name"
                ))
                for index in indexes:
                    session.run(
                        f"DROP INDEX {new_redundancy.quoted(index['name'])} IF EXISTS"
                    ).consume()

        print("Importing TPC-H SF 1 ...", flush=True)
        import_args = importer.parse_args([
            "--uri", URI, "--user", USER, "--database", DATABASE,
            "--data-dir", str(DATA_DIR),
        ])
        importer.run_import(import_args, password)

    print("Running update experiment ...", flush=True)
    arguments = [
        "--uri", URI, "--user", USER, "--database", DATABASE,
        "--output", str(OUTPUT), "--updates", str(UPDATES),
        "--repeats", str(REPEATS),
    ]
    for mask in MASKS:
        arguments.extend(["--mask", str(mask)])
    experiment_args = new_redundancy.parse_args(arguments)
    experiment_args.password = password
    new_redundancy.experiment(experiment_args)
    print(f"Done: {OUTPUT}")


if __name__ == "__main__":
    main()
