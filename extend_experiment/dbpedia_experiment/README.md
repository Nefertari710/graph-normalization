# DBpedia

Discover functional dependencies among literal properties within English DBpedia entity classes.

Complete the [shared setup](../../README.md#setup). Run all commands from the repository root. GDS is not required.

## 1. Download and import

The downloader saves four compressed RDF files from the official **2022.12.01** release to `dataset/data/`. Keep them compressed. The importer creates `dbpedia1` by default (Neo4j Enterprise); FD discovery uses the same database.

```bash
python extend_experiment/dbpedia_experiment/dataset/download_data.py
python extend_experiment/dbpedia_experiment/dataset/import_data.py
```

After an interrupted import, rerun the import command with the same files.

## 2. Discover functional dependencies

Searches FDs among non-missing, single-valued literal properties, requiring **20 eligible instances, 20% class coverage, and 3 repeated determinant groups**.

```bash
python extend_experiment/dbpedia_experiment/find_functional_dependency/find_functional_dependency.py
```

After interruption, append `--resume` to the search command, keeping the same settings and graph.

Outputs: `functional_dependencies_all.json` and `.md` in [find_functional_dependency/](find_functional_dependency/).

For Community, add `--database neo4j` to both import and FD commands; the database must already exist.

The historical data release is unknown. Exact reproduction requires the file hashes recorded in the [saved results](find_functional_dependency/functional_dependencies_all.json), the same search settings, and the same runtime environment.
