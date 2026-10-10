#!/usr/bin/env python3
"""Import five Offshore node CSVs and one relationship CSV into Neo4j."""

import csv
import getpass
import os
import subprocess
import tempfile
import time
from pathlib import Path

DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "data"
DATABASE_NAME = "offshorecsv"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
DEBUG_NEO4J_PASSWORD: str | None = ""
SYSTEM_DATABASE = "system"

NODE_FILES = [
    ("Address", "nodes-addresses.csv"),
    ("Entity", "nodes-entities.csv"),
    ("Intermediary", "nodes-intermediaries.csv"),
    ("Officer", "nodes-officers.csv"),
    ("Other", "nodes-others.csv"),
]
RELATIONSHIP_FILE = "relationships.csv"


def cypher(query, database, password, capture=False):
    environment = os.environ.copy()
    environment["NEO4J_PASSWORD"] = password
    return subprocess.run(
        ["cypher-shell", "--non-interactive", "--format", "plain",
         "--wrap", "false", "-a", NEO4J_URI,
         "-u", NEO4J_USER, "-d", database],
        input=query, text=True, capture_output=capture, check=True,
        env=environment)


def ensure_neo4j_running(password):
    status = subprocess.run(["neo4j", "status"],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    if status.returncode:
        subprocess.run(["neo4j", "start"], check=True)
    for _ in range(90):
        try:
            cypher("SHOW DATABASES YIELD name RETURN count(name);",
                   SYSTEM_DATABASE, password, capture=True)
            print(f"Connected to Neo4j at {NEO4J_URI} as {NEO4J_USER}.")
            return
        except subprocess.CalledProcessError:
            time.sleep(1)
    raise RuntimeError("Neo4j did not become ready within 90 seconds")


def create_headers(directory):
    headers = {}
    for _, filename in NODE_FILES:
        with (DEFAULT_DATA_DIR / filename).open(
            "r", encoding="utf-8-sig", newline=""
        ) as source:
            columns = next(csv.reader(source))
        columns[0] = "node_id:ID(Offshore){id-type:long}"
        header = directory / filename
        with header.open("w", encoding="utf-8", newline="") as target:
            csv.writer(target, lineterminator="\n").writerow(columns)
        headers[filename] = header

    with (DEFAULT_DATA_DIR / RELATIONSHIP_FILE).open(
        "r", encoding="utf-8-sig", newline=""
    ) as source:
        columns = next(csv.reader(source))
    columns[:3] = ["node_id_start:START_ID(Offshore)",
                   "node_id_end:END_ID(Offshore)", "rel_type:TYPE"]
    header = directory / RELATIONSHIP_FILE
    with header.open("w", encoding="utf-8", newline="") as target:
        csv.writer(target, lineterminator="\n").writerow(columns)
    headers[RELATIONSHIP_FILE] = header
    return headers


def shared_intermediary_officer_ids():
    with (DEFAULT_DATA_DIR / "nodes-intermediaries.csv").open(
        "r", encoding="utf-8-sig", newline=""
    ) as source:
        intermediary_ids = {
            int(row["node_id"]) for row in csv.DictReader(source)
        }
    with (DEFAULT_DATA_DIR / "nodes-officers.csv").open(
        "r", encoding="utf-8-sig", newline=""
    ) as source:
        return [int(row["node_id"]) for row in csv.DictReader(source)
                if int(row["node_id"]) in intermediary_ids]


def import_csv(headers):
    command = [
        "neo4j-admin", "database", "import", "full",
        DATABASE_NAME, "--format=standard", "--id-type=integer",
        "--multiline-fields=true", "--auto-skip-subsequent-headers=true",
        "--skip-duplicate-nodes=true", "--bad-tolerance=1139",
    ]
    for label, filename in NODE_FILES:
        command.append(
            f"--nodes={label}={headers[filename]},{DEFAULT_DATA_DIR / filename}")
    command.append(f"--relationships={headers[RELATIONSHIP_FILE]},"
                   f"{DEFAULT_DATA_DIR / RELATIONSHIP_FILE}")

    print(f"Importing CSV data into {DATABASE_NAME}...", flush=True)
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode:
        details = "\n".join(
            output.strip() for output in (result.stdout, result.stderr)
            if output.strip())
        raise RuntimeError(f"CSV import failed:\n{details}")
    for number, (label, _) in enumerate(NODE_FILES, start=1):
        print(f"[{number}/6] {label} nodes loaded")
    print("[6/6] Relationships loaded")


def main():
    password = DEBUG_NEO4J_PASSWORD or os.environ.get("NEO4J_PASSWORD")
    if not password:
        password = getpass.getpass(f"Neo4j password for {NEO4J_USER}: ")

    ensure_neo4j_running(password)
    result = cypher(
        f"SHOW DATABASES YIELD name WHERE name = '{DATABASE_NAME}' RETURN count(*)",
        SYSTEM_DATABASE, password, capture=True)
    exists = int(result.stdout.splitlines()[-1]) > 0
    if exists:
        raise RuntimeError(f"{DATABASE_NAME} already exists")

    overlap_ids = shared_intermediary_officer_ids()
    with tempfile.TemporaryDirectory(prefix="offshorecsv_import_") as temp:
        headers = create_headers(Path(temp))
        import_csv(headers)

    cypher(f"CREATE DATABASE `{DATABASE_NAME}` WAIT 300 SECONDS;",
           SYSTEM_DATABASE, password)
    ids = ",".join(map(str, overlap_ids))
    cypher(f"MATCH (n) WHERE n.node_id IN [{ids}] "
           "SET n:Intermediary:Officer;", DATABASE_NAME, password)
    result = cypher(
        "CALL { MATCH (n) RETURN count(n) AS nodes } "
        "CALL { MATCH ()-[r]->() RETURN count(r) AS relationships } "
        "RETURN nodes, relationships;",
        DATABASE_NAME, password, capture=True)
    nodes, relationships = (
        int(value.strip()) for value in result.stdout.splitlines()[-1].split(","))
    print(f"Import complete: {nodes:,} nodes, "
          f"{relationships:,} relationships.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}")
        raise SystemExit(1)
