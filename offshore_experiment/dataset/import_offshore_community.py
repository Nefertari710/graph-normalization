#!/usr/bin/env python3
"""Import the Offshore CSVs into the default database of Neo4j Community.

Neo4j Community supports a single user database, so this wrapper reuses the
header rewriting and import options of ``import_offshore_enterprise.py`` but
imports into ``neo4j`` (with the server stopped) instead of creating the
``offshorecsv`` database.

``NEO4J_URI``, ``NEO4J_USER`` and ``NEO4J_PASSWORD`` configure the connection.
The import target is always ``neo4j``; ``NEO4J_DATABASE`` is ignored here.

Usage (after ``source ~/neo4j/env.sh``)::

    python import_offshore_community.py
    python import_offshore_community.py --data-dir /path/to/csv
"""

import argparse
import getpass
import importlib.util
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

DATABASE = "neo4j"
HERE = Path(__file__).resolve().parent
ORIGINAL = HERE / "import_offshore_enterprise.py"


class ImportFailure(RuntimeError):
    """A diagnostic containing only a safe operation name and error code."""


class PrivateArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(2, "ERROR: Invalid arguments. Use --help for usage.\n")


def run_command(command, stage, *, check=True):
    """Capture tool diagnostics so paths and data records are not echoed."""
    try:
        result = subprocess.run(command, text=True, capture_output=True)
    except OSError as error:
        raise ImportFailure(
            f"{stage} could not be started (OS error {error.errno})."
        ) from None
    if check and result.returncode:
        raise ImportFailure(
            f"{stage} failed (exit status {result.returncode})."
        )
    return result


def load_original(data_dir: Path):
    spec = importlib.util.spec_from_file_location("import_database", ORIGINAL)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.DEFAULT_DATA_DIR = data_dir
    module.DATABASE_NAME = DATABASE
    return module


def main() -> None:
    parser = PrivateArgumentParser()
    parser.add_argument("--data-dir", type=Path,
                        default=HERE / "data")
    args = parser.parse_args()
    original = load_original(args.data_dir.expanduser().resolve())
    password = os.environ.get("NEO4J_PASSWORD")
    if not password:
        password = getpass.getpass("Neo4j password: ")

    print("Stopping Neo4j...", flush=True)
    run_command(["neo4j", "stop"], "Stopping Neo4j", check=False)
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
        run_command(command, "CSV import")

    print("Starting Neo4j...", flush=True)
    run_command(["neo4j", "start"], "Starting Neo4j")
    for _ in range(120):
        try:
            original.cypher("RETURN 1;", DATABASE, password, capture=True)
            break
        except subprocess.CalledProcessError:
            time.sleep(1)
    ids = ",".join(map(str, overlap_ids))
    original.cypher(f"MATCH (n) WHERE n.node_id IN [{ids}] "
                    "SET n:Intermediary:Officer;", DATABASE, password,
                    capture=True)
    result = original.cypher(
        "CALL { MATCH (n) RETURN count(n) AS nodes } "
        "CALL { MATCH ()-[r]->() RETURN count(r) AS relationships } "
        "RETURN nodes, relationships;", DATABASE, password, capture=True)
    nodes, relationships = (
        int(value.strip()) for value in result.stdout.splitlines()[-1].split(",")
    )
    print("Import complete (nodes, relationships):", f"{nodes}, {relationships}")


def cli() -> int:
    try:
        main()
    except ImportFailure as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as error:
        print(f"ERROR: Database command failed (exit status {error.returncode}).",
              file=sys.stderr)
        return 1
    except OSError as error:
        print(f"ERROR: File or process operation failed (OS error {error.errno}).",
              file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("ERROR: Import interrupted.", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"ERROR: Import failed ({type(error).__name__}).", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(cli())
