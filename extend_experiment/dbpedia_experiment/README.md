# DBpedia

Discover functional dependencies among literal properties within English DBpedia entity classes.

Complete the [shared setup](../../README.md#setup). Run all commands from the repository root. GDS is not required.

## 1. Download and import

The downloader saves these official **2022.12.01** files to `dataset/data/` within this directory. Keep them compressed.

| File |
| --- |
| [instance-types_lang=en_specific.ttl.bz2](https://downloads.dbpedia.org/repo/dbpedia/mappings/instance-types/2022.12.01/instance-types_lang=en_specific.ttl.bz2) |
| [labels_lang=en.ttl.bz2](https://downloads.dbpedia.org/repo/dbpedia/generic/labels/2022.12.01/labels_lang=en.ttl.bz2) |
| [mappingbased-literals_lang=en.ttl.bz2](https://downloads.dbpedia.org/repo/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=en.ttl.bz2) |
| [mappingbased-objects_lang=en.ttl.bz2](https://downloads.dbpedia.org/repo/dbpedia/mappings/mappingbased-objects/2022.12.01/mappingbased-objects_lang=en.ttl.bz2) |

Neo4j Enterprise creates the default database `dbpedia1` automatically. Use the same database for import and FD discovery.

```bash
python extend_experiment/dbpedia_experiment/dataset/download_data.py
python extend_experiment/dbpedia_experiment/dataset/import_data.py
```

After an interrupted import, rerun the import command with the same files.

## 2. Discover functional dependencies

Searches `X → Y` and `X → {Y1, ..., Yn}` among single-valued literals, requiring **20 eligible instances, 20% class coverage, and 3 repeated X groups**; missing and multivalued properties are excluded.

```bash
python extend_experiment/dbpedia_experiment/find_functional_dependency/find_functional_dependency.py
```

After interruption, append `--resume` to the search command, keeping the same settings and graph.

Outputs: `functional_dependencies_all.json` and `.md` in [find_functional_dependency/](find_functional_dependency/).

For Community, add `--database neo4j` to both import and FD commands; the database must already exist.

The historical release is unknown; exact reproduction requires matching the hashes in the [saved results](find_functional_dependency/functional_dependencies_all.json) (`import_at_start.manifest`), search settings, and runtime environment.
