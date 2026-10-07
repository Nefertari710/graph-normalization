#!/usr/bin/env python3
"""Compare Provider updates before and after normalization.

Normalized temporary graph:
    (ProviderServiceRecord)-[:FOR_PROVIDER]->(Provider)
"""

import json
import random
import time
from datetime import datetime
from pathlib import Path

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""
DATABASE = "providerandservicecsv"

UPDATE_COUNT = 50
UPDATE_REPEATS = 10
BATCH_SIZE = 10_000
RANDOM_SEED = 20260920
OUTPUT = (
        Path(__file__).resolve().parents[1]
        / "results"
        / DATABASE
        / "redundancy_experiment.json"
)

ARTIFACT = "PROVIDER_SERVICE_NR_ARTIFACT"
RECORD_COPY = "PROVIDER_SERVICE_NR_RECORD"
PROVIDER = "PROVIDER_SERVICE_NR_PROVIDER"
RELATIONSHIP = "FOR_PROVIDER"
SOURCE_INDEX = "provider_service_nr_source_npi"
PROVIDER_KEY = "provider_service_nr_provider_key"

DETERMINANT = "rndrng_npi"
DEPENDENTS = (
    "rndrng_prvdr_last_org_name",
    "rndrng_prvdr_first_name",
    "rndrng_prvdr_mi",
    "rndrng_prvdr_ent_cd",
    "rndrng_prvdr_st1",
    "rndrng_prvdr_st2",
    "rndrng_prvdr_city",
    "rndrng_prvdr_state_abrvtn",
    "rndrng_prvdr_zip5",
    "rndrng_prvdr_cntry",
)


def run(session, query, **parameters):
    return session.run(query, parameters).consume()


def elapsed_ms(started):
    return (time.perf_counter_ns() - started) / 1_000_000


def group_query():
    """Group records by NPI and collect distinct Provider values."""

    present = " AND ".join(
        f"record.{field} IS NOT NULL" for field in DEPENDENTS
    )
    values = ", ".join(f"record.{field}" for field in DEPENDENTS)
    return f"""
MATCH (record:ProviderServiceRecord)
WHERE record.{DETERMINANT} IS NOT NULL
  AND record.{DETERMINANT} <> ''
WITH record.{DETERMINANT} AS npi,
     count(*) AS fanout,
     sum(CASE WHEN {present} THEN 1 ELSE 0 END) AS complete,
     collect(DISTINCT CASE WHEN {present} THEN [{values}] END) AS variants
"""


def reset_experiment(session):
    run(
        session,
        f"""
MATCH (node:{ARTIFACT})
CALL (node) {{ DETACH DELETE node }} IN TRANSACTIONS OF {BATCH_SIZE} ROWS
""",
    )
    run(session, f"DROP INDEX {SOURCE_INDEX} IF EXISTS")
    run(session, f"DROP CONSTRAINT {PROVIDER_KEY} IF EXISTS")


def read_validation(session):
    record = session.run(
        group_query()
        + """
RETURN count(*) AS groups,
       coalesce(sum(fanout), 0) AS rows,
       sum(CASE WHEN complete = fanout AND size(variants) = 1
                THEN 1 ELSE 0 END) AS valid_groups,
       sum(CASE WHEN complete = fanout AND size(variants) = 1
                THEN fanout ELSE 0 END) AS valid_rows,
       sum(CASE WHEN size(variants) > 1 THEN 1 ELSE 0 END) AS conflicting_groups,
       sum(CASE WHEN complete < fanout THEN 1 ELSE 0 END) AS incomplete_groups
"""
    ).single(strict=True)
    return {key: int(record[key]) for key in record.keys()}


def read_workload_groups(session):
    records = session.run(
        group_query()
        + """
WHERE complete = fanout AND size(variants) = 1
RETURN npi, fanout, variants[0] AS values
ORDER BY fanout DESC, npi
LIMIT $limit
""",
        limit=UPDATE_COUNT,
    )
    return [
        {
            "rndrng_npi": record["npi"],
            "fanout": int(record["fanout"]),
            "values": tuple(record["values"]),
        }
        for record in records
    ]


def make_workload(groups):
    if not groups:
        raise RuntimeError("No valid Provider dependency groups found")
    domain = list(dict.fromkeys(group["values"] for group in groups))
    if len(domain) < 2:
        raise RuntimeError("At least two distinct Provider values are required")

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
        "MATCH (record:ProviderServiceRecord) "
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
MATCH (source:ProviderServiceRecord)
CALL (source) {{
  CREATE (copy:{ARTIFACT}:{RECORD_COPY})
  SET copy = properties(source)
}} IN TRANSACTIONS OF {BATCH_SIZE} ROWS
""",
    )

    provider_setters = ", ".join(
        f"provider.{field} = values[{index}]"
        for index, field in enumerate(DEPENDENTS)
    )
    run(
        session,
        group_query()
        + f"""
WHERE complete = fanout AND size(variants) = 1
WITH npi, variants[0] AS values
CALL (npi, values) {{
  CREATE (provider:{ARTIFACT}:{PROVIDER})
  SET provider.{DETERMINANT} = npi, {provider_setters}
}} IN TRANSACTIONS OF {BATCH_SIZE} ROWS
""",
    )
    run(
        session,
        f"""
CREATE CONSTRAINT {PROVIDER_KEY} IF NOT EXISTS
FOR (provider:{PROVIDER})
REQUIRE provider.{DETERMINANT} IS NODE KEY
""",
    )
    run(session, "CALL db.awaitIndexes(1800)")

    run(
        session,
        f"""
MATCH (record:{RECORD_COPY})
CALL (record) {{
  MATCH (provider:{PROVIDER} {{{DETERMINANT}: record.{DETERMINANT}}})
  CREATE (record)-[:{RELATIONSHIP}]->(provider)
}} IN TRANSACTIONS OF {BATCH_SIZE} ROWS
""",
    )
    removed = ", ".join(
        f"record.{field}" for field in (DETERMINANT, *DEPENDENTS)
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
        "RETURN count(node) AS count, "
        "coalesce(sum(size(keys(node))), 0) AS cells"
    ).single(strict=True)
    relationships = session.run(
        f"MATCH (:{RECORD_COPY})-[rel:{RELATIONSHIP}]->(:{PROVIDER}) "
        "RETURN count(rel) AS count"
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
    label = PROVIDER if normalized else "ProviderServiceRecord"
    setters = ", ".join(
        f"node.{field} = $value{index}"
        for index, field in enumerate(DEPENDENTS)
    )
    parameters = {
        "rndrng_npi": update["rndrng_npi"],
        **{f"value{index}": value for index, value in enumerate(values)},
    }
    record = session.run(
        f"""
MATCH (node:{label} {{{DETERMINANT}: $rndrng_npi}})
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
            validation = read_validation(session)
            workload = make_workload(read_workload_groups(session))
            denormalized_state = source_state(session)

            print("[1/2] Denormalized ProviderServiceRecord", flush=True)
            run(
                session,
                f"CREATE INDEX {SOURCE_INDEX} IF NOT EXISTS "
                f"FOR (record:ProviderServiceRecord) ON (record.{DETERMINANT})",
            )
            run(session, "CALL db.awaitIndexes(1800)")
            try:
                denormalized_times = benchmark(session, workload, False)
            finally:
                run(session, f"DROP INDEX {SOURCE_INDEX} IF EXISTS")

            print("[2/2] Normalized Provider", flush=True)
            try:
                normalization = normalize(session)
                expected_normalized_state = {
                    "node_count": (
                            denormalized_state["node_count"]
                            + validation["valid_groups"]
                    ),
                    "relationship_count": validation["valid_rows"],
                    "property_cell_count": (
                            denormalized_state["property_cell_count"]
                            - validation["valid_rows"]
                            * (len(DEPENDENTS) + 1)
                            + validation["valid_groups"]
                            * (len(DEPENDENTS) + 1)
                    ),
                }
                if normalization["state"] != expected_normalized_state:
                    raise RuntimeError("Normalized graph structure is inconsistent")
                normalized_times = benchmark(session, workload, True)
            finally:
                reset_experiment(session)

            if source_state(session) != denormalized_state:
                raise RuntimeError("Source ProviderServiceRecord graph changed")

            normalized_state = normalization["state"]
            update_runs = []
            for update, old_times, new_times in zip(
                    workload, denormalized_times, normalized_times
            ):
                update_runs.append(
                    {
                        "rndrng_npi": update["rndrng_npi"],
                        "fanout": update["fanout"],
                        "old": list(update["old"]),
                        "new": list(update["new"]),
                        "denormalized_elapsed_ms": old_times,
                        "normalized_elapsed_ms": new_times,
                    }
                )

            report = {
                "experiment": "provider_service_record_normalization",
                "created_at": datetime.now().astimezone().isoformat(),
                "database": DATABASE,
                "dependency": {
                    "determinant": DETERMINANT,
                    "source_dependents": list(DEPENDENTS),
                    "provider_properties": [DETERMINANT, *DEPENDENTS],
                    **validation,
                },
                "settings": {
                    "update_count": UPDATE_COUNT,
                    "update_repeats": UPDATE_REPEATS,
                    "providers_per_update": 1,
                    "batch_size": BATCH_SIZE,
                    "random_seed": RANDOM_SEED,
                },
                "scope": (
                    "Only complete, conflict-free NPI groups are normalized; "
                    "empty strings are treated as valid Provider values."
                ),
                "normalized_schema": {
                    "pattern": (
                        "(ProviderServiceRecord)-[:FOR_PROVIDER]->(Provider)"
                    ),
                    "record_removed_properties": [DETERMINANT, *DEPENDENTS],
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
