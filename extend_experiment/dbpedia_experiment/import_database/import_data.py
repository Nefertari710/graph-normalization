#!/usr/bin/env python3
"""Stream the four DBpedia RDF files into Neo4j 5.26+ (including 2026.x).

Resources, type labels, property keys and relationship types use complete URIs,
following readDBpedia() in GraphNormalization-main/main.cpp.
Literal properties are lists of lexical strings; rdf_literals preserves each
value's predicate, datatype and language. No numeric coercion, inference,
filtering or external URI fetching is performed. A normal run creates the target
database if needed and imports the sources once, checking RDF structure during
parsing and batch write counts. No post-import triple-by-triple verification is
performed. Only the same source files can be retried.
"""

from __future__ import annotations

import argparse
import bz2
import hashlib
import json
import os
import sys
from getpass import getpass
from itertools import islice
from pathlib import Path
from typing import Iterator



NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""
NEO4J_DATABASE = "dbpedia1"


DEFAULT_DATA_DIR = Path(__file__).resolve().parents[1] / "dataset"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
FILES = {
    "instance-types_lang=en_specific.ttl.bz2": "types",
    "labels_lang=en.ttl.bz2": "labels",
    "mappingbased-literals_lang=en.ttl.bz2": "literals",
    "mappingbased-objects_lang=en.ttl.bz2": "objects",
}
WRITE_QUERIES = {
    "types": """
        UNWIND $rows AS row
        MERGE (n:DBpediaResource {uri: row.subject})
        SET n:$(row.object)
        SET n.rdf_types = CASE
            WHEN row.object IN coalesce(n.rdf_types, []) THEN n.rdf_types
            ELSE coalesce(n.rdf_types, []) + [row.object] END
        RETURN count(*) AS processed
    """,
    "literals": """
        UNWIND $rows AS row
        MERGE (n:DBpediaResource {uri: row.subject})
        SET n[row.predicate] = CASE
            WHEN row.object IN coalesce(n[row.predicate], []) THEN n[row.predicate]
            ELSE coalesce(n[row.predicate], []) + [row.object] END
        SET n.rdf_literals = CASE
            WHEN row.literal IN coalesce(n.rdf_literals, []) THEN n.rdf_literals
            ELSE coalesce(n.rdf_literals, []) + [row.literal] END
        RETURN count(*) AS processed
    """,
    "objects": """
        UNWIND $rows AS row
        MERGE (s:DBpediaResource {uri: row.subject})
        MERGE (o:DBpediaResource {uri: row.object})
        MERGE (s)-[r:$(row.predicate)]->(o)
        SET r.predicate = row.predicate
        RETURN count(*) AS processed
    """,
}


class ImportErrorBase(ValueError):
    """Invalid source, incompatible destination, or failed write."""


def source_manifest(data_dir: Path) -> str:
    checksums = {}
    for filename in FILES:
        with (data_dir / filename).open("rb") as stream:
            checksums[filename] = hashlib.file_digest(stream, "sha256").hexdigest()
    # Version 2 uses full URI names; do not merge into a version 1 prefix graph.
    return json.dumps({"model_version": 2, "files": checksums}, sort_keys=True)


def iter_rows(path: Path, kind: str) -> Iterator[dict[str, str]]:
    # Import lazily so --help also works before dependencies are installed.
    from pyoxigraph import Literal, NamedNode, RdfFormat, parse

    position = 0
    try:
        with bz2.open(path, "rb") as stream:
            for position, quad in enumerate(
                # The supplied labels dump contains nonstandard IRI characters
                # (e.g. U+FFF9). Preserve them as identifiers rather than change
                # or discard entities. RDF structure and term roles are checked.
                parse(stream, format=RdfFormat.TURTLE, without_named_graphs=True, lenient=True), 1
            ):
                if not isinstance(quad.subject, NamedNode):
                    raise ValueError("Expected a resource URI as subject; blank nodes are unsupported")
                predicate, obj = quad.predicate.value, quad.object
                if kind == "types":
                    if predicate != RDF_TYPE or not isinstance(obj, NamedNode):
                        raise ValueError("Expected an rdf:type statement with a class URI")
                elif kind == "objects":
                    if not isinstance(obj, NamedNode) or predicate == RDF_TYPE:
                        raise ValueError("Expected a resource relationship, excluding rdf:type")
                elif not isinstance(obj, Literal):
                    raise ValueError("Expected a literal object")
                if kind == "labels" and predicate != RDFS_LABEL:
                    raise ValueError("Expected rdfs:label in the labels file")

                row = {
                    "subject": quad.subject.value,
                    "predicate": predicate,
                    "object": obj.value,
                }
                if isinstance(obj, Literal):
                    # One JSON string per literal keeps its metadata attached to
                    # its value, even for mixed datatypes or multiple languages.
                    row["literal"] = json.dumps(
                        [predicate, obj.value, obj.datatype.value, obj.language],
                        ensure_ascii=False, separators=(",", ":"),
                    )
                yield row
    except (ValueError, SyntaxError, OSError, EOFError) as exc:
        raise ImportErrorBase(
            f"{path.name}: at/after triple {position:,}: {exc}"
        ) from exc
    if position == 0:
        raise ImportErrorBase(f"{path.name}: no RDF statements found")


def write_batch(tx, kind: str, rows: list[dict[str, str]]) -> None:
    result = tx.run(WRITE_QUERIES[kind], rows=rows).single(strict=True)
    if result["processed"] != len(rows):
        raise ImportErrorBase("Unexpected write count; check for duplicate relationships")


def process_files(data_dir: Path, batch_size: int, *, session) -> dict[str, int]:
    counts = {}
    for filename, file_kind in FILES.items():
        kind = "literals" if file_kind == "labels" else file_kind
        total = 0
        rows_iter = iter_rows(data_dir / filename, file_kind)
        print(f"[import] {filename}", flush=True)
        while rows := list(islice(rows_iter, batch_size)):
            session.execute_write(write_batch, kind, rows)
            previous = total
            total += len(rows)
            if total // 100_000 != previous // 100_000:
                print(f"  {total:,} source triples", flush=True)
        counts[filename] = total
        print(f"  OK: {total:,} source triples", flush=True)
    return counts


def create_database_if_missing(driver, database: str) -> None:
    with driver.session(database="system") as session:
        query = "SHOW DATABASE $name YIELD currentStatus, statusMessage RETURN currentStatus, statusMessage"
        states = session.run(query, name=database).data()
        if not states:
            print(f"[database] Creating {database}; waiting for startup...", flush=True)
            session.run(
                "CREATE DATABASE $name IF NOT EXISTS WAIT 60 SECONDS", name=database,
            ).consume()
            states = session.run(query, name=database).data()
        if not states or any(state["currentStatus"] != "online" for state in states):
            raise ImportErrorBase(f"Database {database!r} is not online: {states}")
        print(f"[database] {database} is online", flush=True)


def prepare_database(session, manifest: str) -> None:
    version = session.run(
        "CALL dbms.components() YIELD name, versions "
        "WHERE name = 'Neo4j Kernel' RETURN versions[0] AS version"
    ).single(strict=True)["version"]
    if tuple(int(part) for part in version.split(".")[:2]) < (5, 26):
        raise ImportErrorBase(f"Neo4j 5.26+ is required; found {version}")

    # An empty database has no import labels yet. Skip those lookups so Neo4j
    # does not emit missing-label/property notifications during normal setup.
    labels = {row["label"] for row in session.run("CALL db.labels() YIELD label RETURN label")}
    marker = None
    if "DBpediaImport" in labels:
        marker = session.run(
            "MATCH (m:DBpediaImport {id: 'dbpedia'}) RETURN m.manifest AS manifest"
        ).single()
    if marker is not None and marker["manifest"] != manifest:
        raise ImportErrorBase("This database contains a different DBpedia source/model; use a fresh database")
    if marker is None:
        if "DBpediaResource" in labels and session.run(
            "MATCH (n:DBpediaResource) RETURN n LIMIT 1"
        ).single() is not None:
            raise ImportErrorBase("Existing DBpediaResource nodes have no import manifest; use a fresh database")
    if marker is None:
        print("[database] First import: initializing import metadata and constraints.", flush=True)
    else:
        print("[database] Matching import record found; continuing.", flush=True)

    for label, prop in (("DBpediaResource", "uri"), ("DBpediaImport", "id")):
        session.run(
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE"
        ).consume()
    saved = session.run(
        "MERGE (m:DBpediaImport {id: 'dbpedia'}) "
        "ON CREATE SET m.manifest = $manifest "
        "RETURN m.manifest AS manifest", manifest=manifest,
    ).single(strict=True)
    if saved["manifest"] != manifest:
        raise ImportErrorBase("The DBpedia import manifest changed; stop other import processes")
    session.run(
        "MATCH (m:DBpediaImport {id: 'dbpedia'}) "
        "SET m.complete = false, m.verified = false "
        "REMOVE m.completed_at, m.source_counts"
    ).consume()


def run_import(args, manifest: str) -> None:
    from neo4j import GraphDatabase

    password = os.getenv("NEO4J_PASSWORD") or NEO4J_PASSWORD or getpass(f"Neo4j password for {args.user}: ")
    with GraphDatabase.driver(args.uri, auth=(args.user, password)) as driver:
        driver.verify_connectivity()
        create_database_if_missing(driver, args.database)
        with driver.session(database=args.database) as session:
            prepare_database(session, manifest)
            written = process_files(args.data_dir, args.batch_size, session=session)
            if source_manifest(args.data_dir) != manifest:
                raise ImportErrorBase("Source files changed during the run")
            session.run(
                "MATCH (m:DBpediaImport {id: 'dbpedia'}) "
                "SET m.complete = true, m.verified = false, m.completed_at = datetime(), "
                "m.source_counts = $counts", counts=json.dumps(written, sort_keys=True),
            ).consume()
    print(f"Imported {sum(written.values()):,} source triples.", flush=True)


def positive_int(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI", NEO4J_URI))
    parser.add_argument("--user", default=os.getenv("NEO4J_USER", NEO4J_USER))
    parser.add_argument("--database", default=os.getenv("NEO4J_DATABASE", NEO4J_DATABASE))
    parser.add_argument("--batch-size", type=positive_int, default=2000)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    args.data_dir = args.data_dir.expanduser().resolve()
    try:
        manifest = source_manifest(args.data_dir)
        run_import(args, manifest)
        return 0
    except KeyboardInterrupt:
        print("\nCancelled. Committed batches remain; rerun with the same files to resume.", file=sys.stderr)
        return 130
    except ModuleNotFoundError as exc:
        print(f"Missing dependency: {exc.name or str(exc)}. See import_database/README_CN.md.", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
