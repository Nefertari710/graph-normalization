#!/usr/bin/env python3
"""Import the Offshore CSVs into the default database of Neo4j Community.

Neo4j Community supports a single user database, so this wrapper reuses the
header rewriting and import options of ``dataset/import_database.py`` but
imports into ``neo4j`` (with the server stopped) instead of creating the
``offshorecsv`` database.

Usage (after ``source ~/neo4j/env.sh``)::

    python import_community.py --data-dir ~/neo4j/offshore_data
"""

import argparse
import importlib.util
import os
import subprocess
import tempfile
import time
from pathlib import Path

DATABASE = "neo4j"
HERE = Path(__file__).resolve().parent
ORIGINAL = HERE.parent / "dataset" / "import_database.py"


def load_original(data_dir: Path):
    spec = importlib.util.spec_from_file_location("import_database", ORIGINAL)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.DEFAULT_DATA_DIR = data_dir
    module.DATABASE_NAME = DATABASE
    return module


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path,
                        default=Path.home() / "neo4j" / "offshore_data")
    args = parser.parse_args()
    original = load_original(args.data_dir.expanduser().resolve())
    password = os.environ["NEO4J_PASSWORD"]

    subprocess.run(["neo4j", "stop"], check=False)
    overlap_ids = original.shared_intermediary_officer_ids()
    with tempfile.TemporaryDirectory(prefix="offshore_import_") as temp:
        headers = original.create_headers(Path(temp))
        command = [
            "neo4j-admin", "database", "import", "full", DATABASE,
            "--overwrite-destination=true", "--format=standard",
            "--id-type=integer", "--multiline-fields=true",
            "--auto-skip-subsequent-headers=true",
            "--skip-duplicate-nodes=true", "--bad-tolerance=1139",
        ]
        for label, filename in original.NODE_FILES:
            command.append(
                f"--nodes={label}={headers[filename]},"
                f"{original.DEFAULT_DATA_DIR / filename}")
        command.append(
            f"--relationships={headers[original.RELATIONSHIP_FILE]},"
            f"{original.DEFAULT_DATA_DIR / original.RELATIONSHIP_FILE}")
        print("Importing CSV data into neo4j...", flush=True)
        result = subprocess.run(command, text=True, capture_output=True)
        if result.returncode:
            raise RuntimeError(result.stdout[-4000:] + result.stderr[-4000:])

    subprocess.run(["neo4j", "start"], check=True)
    for _ in range(120):
        try:
            original.cypher("RETURN 1;", DATABASE, password, capture=True)
            break
        except subprocess.CalledProcessError:
            time.sleep(1)
    ids = ",".join(map(str, overlap_ids))
    original.cypher(f"MATCH (n) WHERE n.node_id IN [{ids}] "
                    "SET n:Intermediary:Officer;", DATABASE, password)
    result = original.cypher(
        "CALL { MATCH (n) RETURN count(n) AS nodes } "
        "CALL { MATCH ()-[r]->() RETURN count(r) AS relationships } "
        "RETURN nodes, relationships;", DATABASE, password, capture=True)
    print("Import complete (nodes, relationships):",
          result.stdout.splitlines()[-1])


if __name__ == "__main__":
    main()
