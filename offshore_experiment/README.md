# Offshore Leaks

Compare redundancy, updates, embeddings, use cases, and entity resolution on ICIJ's Offshore Leaks graph.

Complete the shared [setup](../README.md#setup) and run all commands from the **repository root**.

## 1. Data and import

Download the [official ICIJ dataset](https://offshoreleaks.icij.org/pages/database):

```bash
python offshore_experiment/dataset/download_data.py
```

The download saves five node CSVs and `relationships.csv` in `offshore_experiment/dataset/data/`. `LATEST` can change; historical results require the same snapshot.

Use a local Neo4j Enterprise installation with `neo4j`, `neo4j-admin`, and `cypher-shell` available:

```bash
export NEO4J_HOME='/absolute/path/to/your/neo4j-installation'
export PATH="$NEO4J_HOME/bin:$PATH"
python offshore_experiment/dataset/import_offshore_enterprise.py
```

The importer creates `offshorecsv` by default and refuses to overwrite an existing database. Set `NEO4J_DATABASE` to use another new database for import and experiments.

![Offshore imported graph schema, selected relationships](../docs/offshore_schema.svg)

Run selected experiments sequentially; they are independent, and rerunning overwrites their outputs. Analysis commands read saved JSON without Neo4j.

Output paths below are relative to `offshore_experiment/`.

## 2. Embedding similarity

Compare embedding similarities for node pairs sharing an FD determinant before and after normalization. **GDS is required.**

```bash
python offshore_experiment/embedding/scripts/embedding_offshore.py
python offshore_experiment/embedding/analysis/analysis.py
python offshore_experiment/embedding/analysis/cos_similarity.py
```

Outputs: `embedding/results/embedding_offshore.json`, the report [`embedding/analysis/result.md`](embedding/analysis/result.md), and the figure [`embedding/analysis/cosine_similarity_ecdf.png`](embedding/analysis/cosine_similarity_ecdf.png).

## 3. Redundancy and update cost

Compare redundant copies with one shared node for 12 property-based functional dependencies (FDs), measuring redundancy, normalization cost, and update time.

```bash
python offshore_experiment/redundancy/scripts/updates_offshore.py
python offshore_experiment/redundancy/analysis/analysis_update.py
```

Outputs: `redundancy/results/updates_offshore.json` and [`redundancy/results/updates_offshore.md`](redundancy/results/updates_offshore.md).

## 4. Downstream use cases (U1–U3)

Evaluate determinant classification, similarity search, and embedding maintenance under complete or partial updates. **GDS is required.**

```bash
python offshore_experiment/usecases/scripts/usecases_offshore.py
python offshore_experiment/usecases/analysis/analyze_usecases.py
```

Outputs: `usecases/results/usecases_offshore.json`, plus [`usecases_offshore.md`](usecases/results/report/usecases_offshore.md) and [`usecase_partial_updates.png`](usecases/results/report/usecase_partial_updates.png) in `usecases/results/report/`.

## 5. Entity resolution (U4)

Compare duplicate-entity detection before and after normalization. **GDS is required.**

```bash
python offshore_experiment/usecases/scripts/er_offshore.py
python offshore_experiment/usecases/analysis/analyze_er.py
```

Outputs: `usecases/results/er_offshore.json` and [`usecases/results/report/er_offshore.md`](usecases/results/report/er_offshore.md).

## 6. Updates through shared address references

Measure normalization cost and compare repeated `sourceID` updates with one shared Address update for Officer, Intermediary, and Entity groups sharing `registered_address`.

```bash
python offshore_experiment/usecases/scripts/updates_reference_fd.py
python offshore_experiment/usecases/analysis/analyze_updates.py
```

Outputs: `usecases/results/updates_reference_fd.json` and [`usecases/results/report/updates_reference_fd.md`](usecases/results/report/updates_reference_fd.md).

## Community edition (optional)

For Community, run `python offshore_experiment/dataset/import_offshore_community.py`. It stops Neo4j and **replaces the entire `neo4j` database**, ignoring `NEO4J_DATABASE`. Set `NEO4J_DATABASE=neo4j` for the subsequent experiments.
