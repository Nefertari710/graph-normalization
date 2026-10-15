# Graph Normalization

Experiments on property-graph normalization: queries, redundancy, updates, and embeddings.

## Setup

Use **Python 3.13** and **Neo4j Enterprise 2026.06.0**. Start Neo4j locally, then run these Bash commands from the repository root:

```bash
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export NEO4J_URI='bolt://localhost:7687'
export NEO4J_USER='neo4j'
export NEO4J_PASSWORD='your-local-password'
```

Replace `your-local-password` with your Neo4j password. Leave `NEO4J_DATABASE` unset so each dataset uses its own default database.

## Run an experiment

Choose a dataset and follow its README in order. **Run all commands from the repository root.**

| Dataset | Data | Experiments |
| --- | --- | --- |
| [TPC-H](tpch_database_experiment/README.md) | SF 0.1 included | Queries, redundancy, updates |
| [Offshore Leaks](offshore_experiment/README.md) | Download script | Redundancy, updates, embeddings, use cases |
| [Northwind](extend_experiment/northwind/README.md) | Included | Embeddings |
| [Hospital](extend_experiment/hospital_experiment/README.md) | Included | Redundancy, updates |
| [Provider & Service](extend_experiment/providerAndService_experiment/README.md) | Download script | Redundancy, updates, embeddings |
| [DBpedia](extend_experiment/dbpedia_experiment/README.md) | Download script | Functional dependency discovery |

Run experiments sequentially; rerunning can overwrite saved results. Exact historical values require the same data snapshot and runtime environment.

## GDS setup (only where required)

Install **GDS 2026.06.0** using the [Neo4j installation guide](https://neo4j.com/docs/graph-data-science/current/installation/neo4j-server/). In `neo4j.conf`, add `gds.*` to `dbms.security.procedures.unrestricted` and any configured allowlist. On macOS, also add `server.jvm.additional=-Djol.skipHotspotSAAttach=true`. Restart Neo4j and check `RETURN gds.version();` in Neo4j Browser.

## Theory checks (optional)

These run without Neo4j. The last command needs the Northwind and Offshore CSVs.

```bash
python theory/check_kgnf_running_example.py
python theory/normalize_schemas.py
python theory/reference_fd_experiment.py
```
