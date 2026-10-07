#!/usr/bin/env python3
"""Compare Hospital updates before and after normalization.

Normalized temporary graph:
    (HospitalRecord)-[:FOR_HOSPITAL]->(Hospital)
"""

import json
import random
import time
from datetime import datetime
from pathlib import Path

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""
DATABASE = "hospitalcsv"

UPDATE_COUNT = 50
UPDATE_REPEATS = 10
BATCH_SIZE = 10_000
RANDOM_SEED = 20260919
OUTPUT = (
        Path(__file__).resolve().parents[1]
        / "results"
        / DATABASE
        / "redundancy_experiment.json"
)

ARTIFACT = "HOSPITAL_NR_ARTIFACT"
RECORD_COPY = "HOSPITAL_NR_RECORD"
HOSPITAL = "HOSPITAL_NR_HOSPITAL"
RELATIONSHIP = "FOR_HOSPITAL"
SOURCE_INDEX = "hospital_nr_source_facility"
HOSPITAL_KEY = "hospital_nr_hospital_key"

# HospitalRecord property -> Hospital property.
PROPERTY_MAP = (
    ("facility_name", "facility_name"),
    ("address", "address"),
    ("city_town", "city"),
    ("state", "state"),
    ("zip_code", "zip_code"),
    ("county_parish", "county"),
    ("telephone_number", "telephone"),
)
SOURCE_FIELDS = tuple(source for source, _ in PROPERTY_MAP)
HOSPITAL_FIELDS = tuple(target for _, target in PROPERTY_MAP)


def run(session, query, **parameters):
    return session.run(query, parameters).consume()


def elapsed_ms(started):
    return (time.perf_counter_ns() - started) / 1_000_000


def reset_experiment(session):
    run(
        session,
        f"""
MATCH (node:{ARTIFACT})
CALL (node) {{ DETACH DELETE node }} IN TRANSACTIONS OF {BATCH_SIZE} ROWS
""",
    )
    run(session, f"DROP INDEX {SOURCE_INDEX} IF EXISTS")
    run(session, f"DROP CONSTRAINT {HOSPITAL_KEY} IF EXISTS")


def read_groups(session):
    """Validate facility_id -> hospital properties."""

    present = " AND ".join(
        f"record.{field} IS NOT NULL AND record.{field} <> ''"
        for field in SOURCE_FIELDS
    )
    values = ", ".join(f"record.{field}" for field in SOURCE_FIELDS)
    records = session.run(
        f"""
MATCH (record:HospitalRecord)
WHERE record.facility_id IS NOT NULL AND record.facility_id <> ''
WITH record.facility_id AS facility_id,
     count(*) AS fanout,
     sum(CASE WHEN {present} THEN 1 ELSE 0 END) AS complete,
     collect(DISTINCT CASE WHEN {present} THEN [{values}] END) AS variants
RETURN facility_id, fanout, complete, variants
ORDER BY facility_id
"""
    )

    groups = []
    for record in records:
        variants = [tuple(value) for value in record["variants"]]
        if record["complete"] != record["fanout"] or len(variants) != 1:
            raise RuntimeError(
                f"Invalid Hospital dependency for {record['facility_id']}"
            )
        groups.append(
            {
                "facility_id": record["facility_id"],
                "fanout": int(record["fanout"]),
                "values": variants[0],
            }
        )
    if not groups:
        raise RuntimeError("No HospitalRecord nodes found")
    return groups


def make_workload(groups):
    groups = sorted(
        groups, key=lambda group: (-group["fanout"], group["facility_id"])
    )
    domain = list(dict.fromkeys(group["values"] for group in groups))
    if len(domain) < 2:
        raise RuntimeError("At least two hospitals are required")

    rng = random.Random(RANDOM_SEED)
    workload = []
    for index in range(UPDATE_COUNT):
        group = groups[index % len(groups)]
        new = group["values"]
        while new == group["values"]:
            new = rng.choice(domain)
        workload.append({**group, "old": group["values"], "new": new})
    return workload


def source_state(session):
    record = session.run(
        "MATCH (record:HospitalRecord) "
        "RETURN count(record) AS nodes, "
        "coalesce(sum(size(keys(record))), 0) AS cells"
    ).single(strict=True)
    return {
        "node_count": int(record["nodes"]),
        "relationship_count": 0,
        "property_cell_count": int(record["cells"]),
    }


def normalize(session):
    started = time.perf_counter_ns()
    run(
        session,
        f"""
MATCH (source:HospitalRecord)
CALL (source) {{
  CREATE (copy:{ARTIFACT}:{RECORD_COPY})
  SET copy = properties(source)
}} IN TRANSACTIONS OF {BATCH_SIZE} ROWS
""",
    )

    hospital_setters = ", ".join(
        f"hospital.{target} = example.{source}"
        for source, target in PROPERTY_MAP
    )
    run(
        session,
        f"""
MATCH (source:HospitalRecord)
WITH source.facility_id AS facility_id, head(collect(source)) AS example
CREATE (hospital:{ARTIFACT}:{HOSPITAL})
SET hospital.facility_id = facility_id, {hospital_setters}
""",
    )
    run(
        session,
        f"""
CREATE CONSTRAINT {HOSPITAL_KEY} IF NOT EXISTS
FOR (hospital:{HOSPITAL})
REQUIRE hospital.facility_id IS NODE KEY
""",
    )
    run(session, "CALL db.awaitIndexes(300)")

    run(
        session,
        f"""
MATCH (record:{RECORD_COPY})
CALL (record) {{
  MATCH (hospital:{HOSPITAL} {{facility_id: record.facility_id}})
  CREATE (record)-[:{RELATIONSHIP}]->(hospital)
}} IN TRANSACTIONS OF {BATCH_SIZE} ROWS
""",
    )
    removed = ", ".join(
        f"record.{field}" for field in ("facility_id", *SOURCE_FIELDS)
    )
    run(
        session,
        f"""
MATCH (record:{RECORD_COPY})-[:{RELATIONSHIP}]->()
CALL (record) {{ REMOVE {removed} }} IN TRANSACTIONS OF {BATCH_SIZE} ROWS
""",
    )

    nodes = session.run(
        f"MATCH (node:{ARTIFACT}) "
        "RETURN count(node) AS count, sum(size(keys(node))) AS cells"
    ).single(strict=True)
    relationships = session.run(
        f"MATCH (:{RECORD_COPY})-[r:{RELATIONSHIP}]->(:{HOSPITAL}) "
        "RETURN count(r) AS count"
    ).single(strict=True)
    return {
        "elapsed_ms": elapsed_ms(started),
        "state": {
            "node_count": int(nodes["count"]),
            "relationship_count": int(relationships["count"]),
            "property_cell_count": int(nodes["cells"]),
        },
    }


def set_values(session, update, values, normalized):
    fields = HOSPITAL_FIELDS if normalized else SOURCE_FIELDS
    label = HOSPITAL if normalized else "HospitalRecord"
    setters = ", ".join(
        f"node.{field} = $value{index}" for index, field in enumerate(fields)
    )
    parameters = {
        "facility_id": update["facility_id"],
        **{f"value{index}": value for index, value in enumerate(values)},
    }
    record = session.run(
        f"""
MATCH (node:{label} {{facility_id: $facility_id}})
SET {setters}
RETURN count(node) AS touched
""",
        parameters,
    ).single(strict=True)
    return int(record["touched"])


def benchmark(session, workload, normalized):
    results = []
    for update in workload:
        times = []
        touched = []
        for _ in range(UPDATE_REPEATS):
            started = time.perf_counter_ns()
            try:
                touched.append(set_values(session, update, update["new"], normalized))
                times.append(elapsed_ms(started))
            finally:
                set_values(session, update, update["old"], normalized)

        expected = 1 if normalized else update["fanout"]
        if set(touched) != {expected}:
            raise RuntimeError(f"Expected {expected} updates, observed {touched}")
        results.append(times)
    return results


def main():
    from neo4j import GraphDatabase

    print(f"Connecting to {DATABASE} at {NEO4J_URI}")
    with GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
    ) as driver:
        driver.verify_connectivity()
        with driver.session(database=DATABASE) as session:
            reset_experiment(session)
            groups = read_groups(session)
            workload = make_workload(groups)
            denormalized_state = source_state(session)

            print("[1/2] Denormalized HospitalRecord", flush=True)
            run(
                session,
                f"CREATE INDEX {SOURCE_INDEX} IF NOT EXISTS "
                "FOR (record:HospitalRecord) ON (record.facility_id)",
            )
            run(session, "CALL db.awaitIndexes(300)")
            try:
                denormalized_times = benchmark(session, workload, False)
            finally:
                run(session, f"DROP INDEX {SOURCE_INDEX} IF EXISTS")

            print("[2/2] Normalized Hospital", flush=True)
            try:
                normalization = normalize(session)
                normalized_times = benchmark(session, workload, True)
            finally:
                reset_experiment(session)

            if source_state(session) != denormalized_state:
                raise RuntimeError("Source HospitalRecord graph changed")

            normalized_state = normalization["state"]
            update_runs = []
            for update, old_times, new_times in zip(
                    workload, denormalized_times, normalized_times
            ):
                update_runs.append(
                    {
                        "facility_id": update["facility_id"],
                        "fanout": update["fanout"],
                        "old": list(update["old"]),
                        "new": list(update["new"]),
                        "denormalized_elapsed_ms": old_times,
                        "normalized_elapsed_ms": new_times,
                    }
                )

            report = {
                "experiment": "hospital_record_normalization",
                "created_at": datetime.now().astimezone().isoformat(),
                "database": DATABASE,
                "dependency": {
                    "determinant": "facility_id",
                    "source_dependents": list(SOURCE_FIELDS),
                    "hospital_properties": ["facility_id", *HOSPITAL_FIELDS],
                    "groups": len(groups),
                    "rows": sum(group["fanout"] for group in groups),
                },
                "settings": {
                    "update_count": UPDATE_COUNT,
                    "update_repeats": UPDATE_REPEATS,
                    "batch_size": BATCH_SIZE,
                    "random_seed": RANDOM_SEED,
                },
                "normalized_schema": {
                    "pattern": "(HospitalRecord)-[:FOR_HOSPITAL]->(Hospital)",
                    "record_removed_properties": [
                        "facility_id",
                        *SOURCE_FIELDS,
                    ],
                },
                "denormalized_state": denormalized_state,
                "normalized_state": normalized_state,
                "normalized_minus_denormalized": {
                    key: normalized_state[key] - denormalized_state[key]
                    for key in denormalized_state
                },
                "normalization_elapsed_ms": normalization["elapsed_ms"],
                "update_runs": update_runs,
            }
            OUTPUT.parent.mkdir(parents=True, exist_ok=True)
            OUTPUT.write_text(
                json.dumps(report, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

    print(f"Experiment complete: {OUTPUT}")


if __name__ == "__main__":
    main()
