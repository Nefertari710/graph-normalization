#!/usr/bin/env python3
"""Import the DBLP RDF dump as a paper-author property graph."""

import argparse
import gzip
import os
import re
from collections import defaultdict
from getpass import getpass
from pathlib import Path

from neo4j import GraphDatabase
from pyoxigraph import Literal, NamedNode, RdfFormat, parse


SOURCE = Path(__file__).resolve().parents[1] / "dataset" / "dblp-2024-03-01.nt.gz"
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = ""
DATABASE = "dblp"
BATCH_SIZE = 5000
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
AUTHORED_BY = "https://dblp.org/rdf/schema#authoredBy"
DBLP_SCHEMA = "https://dblp.org/rdf/schema#"

QUERIES = {
    "type": """
        UNWIND $rows AS row
        MERGE (n:DBLPResource {uri: row.subject})
        SET n:$(row.label)
        SET n.rdf_types = CASE
            WHEN row.type_uri IN coalesce(n.rdf_types, []) THEN n.rdf_types
            ELSE coalesce(n.rdf_types, []) + [row.type_uri]
        END
    """,
    "property": """
        UNWIND $rows AS row
        MERGE (n:DBLPResource {uri: row.subject})
        SET n[row.predicate] = CASE
            WHEN row.value IN coalesce(n[row.predicate], []) THEN n[row.predicate]
            ELSE coalesce(n[row.predicate], []) + [row.value]
        END
    """,
    "author": """
        UNWIND $rows AS row
        MERGE (publication:DBLPResource {uri: row.publication})
        SET publication:DBLP_Publication
        MERGE (person:DBLPResource {uri: row.person})
        SET person:DBLP_Person
        MERGE (publication)-[:AUTHORED_BY]->(person)
    """,
}


def label_for(type_uri):
    """Turn a DBLP class URI into a readable Neo4j label."""
    prefix = "DBLP" if type_uri.startswith(DBLP_SCHEMA) else "RDF"
    local_name = type_uri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
    local_name = re.sub(r"[^A-Za-z0-9_]", "_", local_name).strip("_")
    return f"{prefix}_{local_name or 'Resource'}"


def write_batch(tx, query, rows):
    tx.run(query, rows=rows).consume()


def flush(session, batches, kind):
    rows = batches[kind]
    if rows:
        session.execute_write(write_batch, QUERIES[kind], rows)
        rows.clear()


def ensure_database(driver, database):
    """Create the target database when using Neo4j Enterprise."""
    with driver.session(database="system") as session:
        result = session.run(
            "SHOW DATABASE $database YIELD currentStatus RETURN currentStatus",
            database=database,
        ).single()
        if result is None:
            session.run(
                "CREATE DATABASE $database IF NOT EXISTS WAIT 120 SECONDS",
                database=database,
            ).consume()


def import_dblp(source, driver, database, batch_size, limit=None):
    batches = defaultdict(list)
    counts = defaultdict(int)

    with driver.session(database=database) as session:
        session.run(
            "CREATE CONSTRAINT dblp_resource_uri IF NOT EXISTS "
            "FOR (n:DBLPResource) REQUIRE n.uri IS UNIQUE"
        ).consume()

        with gzip.open(source, "rb") as stream:
            triples = parse(
                stream,
                format=RdfFormat.N_TRIPLES,
                without_named_graphs=True,
            )
            for number, triple in enumerate(triples, 1):
                if limit is not None and number > limit:
                    break
                counts["source"] = number
                if number % 1_000_000 == 0:
                    print(f"Imported {number:,} source triples", flush=True)
                subject = triple.subject
                predicate = triple.predicate.value
                obj = triple.object

                # The experiment uses named DBLP resources. Identifier and
                # signature blank-node structures are intentionally omitted.
                if not isinstance(subject, NamedNode):
                    counts["skipped"] += 1
                    continue

                if predicate == RDF_TYPE and isinstance(obj, NamedNode):
                    kind = "type"
                    row = {
                        "subject": subject.value,
                        "label": label_for(obj.value),
                        "type_uri": obj.value,
                    }
                elif predicate == AUTHORED_BY and isinstance(obj, NamedNode):
                    kind = "author"
                    row = {
                        "publication": subject.value,
                        "person": obj.value,
                    }
                elif isinstance(obj, (Literal, NamedNode)):
                    kind = "property"
                    row = {
                        "subject": subject.value,
                        "predicate": predicate,
                        "value": obj.value,
                    }
                else:
                    counts["skipped"] += 1
                    continue

                batches[kind].append(row)
                counts[kind] += 1
                if len(batches[kind]) >= batch_size:
                    flush(session, batches, kind)

        for kind in QUERIES:
            flush(session, batches, kind)

    return counts


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI", URI))
    parser.add_argument("--user", default=os.getenv("NEO4J_USER", USER))
    parser.add_argument("--database", default=os.getenv("NEO4J_DATABASE", DATABASE))
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--limit", type=int, help="import only the first N triples")
    return parser.parse_args()


def main():
    args = arguments()
    password = os.getenv("NEO4J_PASSWORD", PASSWORD)
    if not password:
        password = getpass(f"Neo4j password for {args.user}: ")
    source = args.source.expanduser().resolve()
    if args.batch_size < 1 or (args.limit is not None and args.limit < 1):
        raise SystemExit("batch size and limit must be positive")

    with GraphDatabase.driver(args.uri, auth=(args.user, password)) as driver:
        driver.verify_connectivity()
        ensure_database(driver, args.database)
        counts = import_dblp(
            source, driver, args.database, args.batch_size, args.limit
        )

    print(
        "Finished: " + ", ".join(f"{key}={value:,}" for key, value in counts.items())
    )


if __name__ == "__main__":
    main()
