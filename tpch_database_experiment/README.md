# TPC-H

Compare normalized and denormalized graphs for query performance, redundancy, and update cost.

Complete the shared [setup](../README.md#setup) first. Run all commands below from the **repository root**.

## 1. Data and import

The eight **SF 0.1** tables are included in [`data/tbl_sf_01/`](data/tbl_sf_01/). Use Neo4j Enterprise and a fresh, dedicated database; the importer creates `tpch-sf-01` by default.

```bash
python tpch_database_experiment/data/import_data_from_tbl_enterprise.py
```

### Imported schema

Each table is a node label; columns are node properties and foreign keys become directed relationships.

![TPC-H schema exported from Neo4j](figures/tpch_schema.svg)

Generated in Neo4j Browser on `tpch-sf-01` with `CALL db.schema.visualization();`. Composite keys: `PARTSUPP (ps_partkey, ps_suppkey)` and `LINEITEM (l_orderkey, l_linenumber)`.

## 2. Run experiments

Run sequentially; each experiment is followed by its analysis command:

```bash
python tpch_database_experiment/query/scripts/run_benchmark.py --output-json outputs/tpch/query.json
python tpch_database_experiment/query/analysis/query_analysis.py --input outputs/tpch/query.json

python tpch_database_experiment/redundancy/scripts/new_redundancy.py --database tpch-sf-01 --output outputs/tpch/redundancy.json
python tpch_database_experiment/redundancy/analysis/analysis_new.py --input outputs/tpch/redundancy.json
```

The query runner defaults to **SF 0.1**. If you set a custom `NEO4J_DATABASE`, update `--database tpch-sf-01` above to match.

Outputs: `outputs/tpch/query.json`, `outputs/tpch/redundancy.json`, and matching `.md` reports. Rerunning overwrites these files.

## Optional

For Community, run `python tpch_database_experiment/data/import_data_from_tbl_community.py --database neo4j`, and pass `--database neo4j` to both experiment commands.

SF 1 use cases require data generated with [TPC-H DBGEN](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp), for example `./dbgen -s 1`, placed in `tpch_database_experiment/data/tbl_sf_1/`.
The preparation **clears and rebuilds the target database**, defaulting to `tpch-sf-1-usecase` (or `NEO4J_DATABASE` if set):

```bash
python tpch_database_experiment/usecases/scripts/run_sf1.py
python tpch_database_experiment/usecases/analysis/analyze_sf1_queries.py
python tpch_database_experiment/usecases/analysis/analyze_sf1_redundancy.py
```

Outputs: [`usecases/results/sf1/`](usecases/results/sf1/), with Markdown reports in `report/`.
