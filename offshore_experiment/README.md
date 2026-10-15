# Offshore Leaks

Compare redundancy, updates, embeddings, use cases, and entity resolution on ICIJ's Offshore Leaks graph.

Complete the shared [setup](../README.md#setup). Run all commands below from the repository root.

## 1. Data and import

Download the [official ICIJ dataset](https://offshoreleaks.icij.org/pages/database):

```bash
python offshore_experiment/dataset/download_data.py
```

The five node CSVs and `relationships.csv` are saved in `offshore_experiment/dataset/data/`. The `LATEST` download can change; reproducing historical results requires the same snapshot.

Use a local Neo4j Enterprise installation with `neo4j`, `neo4j-admin`, and `cypher-shell` available:

```bash
export NEO4J_HOME='/absolute/path/to/your/neo4j-installation'
export PATH="$NEO4J_HOME/bin:$PATH"
python offshore_experiment/dataset/import_offshore_enterprise.py
```

The importer creates a new database, `offshorecsv` by default, and refuses to overwrite an existing database. Set `NEO4J_DATABASE` to use another new database for import and experiments.

## 2. Run experiments

Run sequentially. Embeddings, use cases, and entity resolution require GDS from the shared setup.

```bash
# Embeddings
python offshore_experiment/embedding/scripts/embedding_offshore.py
python offshore_experiment/embedding/analysis/analysis.py
python offshore_experiment/embedding/analysis/cos_similarity.py

# Redundancy and updates
python offshore_experiment/redundancy/scripts/updates_offshore.py
python offshore_experiment/redundancy/analysis/analysis_update.py

# Use cases
python offshore_experiment/usecases/scripts/usecases_offshore.py
python offshore_experiment/usecases/analysis/analyze_usecases.py

# Entity resolution
python offshore_experiment/usecases/scripts/er_offshore.py
python offshore_experiment/usecases/analysis/analyze_er.py

# Reference-FD updates
python offshore_experiment/usecases/scripts/updates_reference_fd.py
python offshore_experiment/usecases/analysis/analyze_updates.py
```

Outputs under `offshore_experiment/`: raw JSON in `embedding/results/`, `redundancy/results/`, and `usecases/results/`; reports and figures in `embedding/analysis/`, `redundancy/results/`, and `usecases/results/report/`. Rerunning overwrites existing outputs.

## Optional

For Community, run `python offshore_experiment/dataset/import_offshore_community.py`. It stops Neo4j and **replaces the entire `neo4j` database**, ignoring `NEO4J_DATABASE`. Set `NEO4J_DATABASE=neo4j` for the subsequent experiments.
