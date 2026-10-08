# Import DBLP into Neo4j

`dblp-2024-03-01.nt.gz` is an N-Triples RDF file. The script streams the gzip file
directly and writes to Neo4j in batches of 5000 triples. No decompression, APOC,
or n10s is required.

This version builds only the paper–author property graph needed by this project:

- DBLP URIs become `DBLPResource` nodes.
- `rdf:type` becomes labels such as `DBLP_Publication` and `DBLP_Person`.
- `authoredBy` becomes an `AUTHORED_BY` relationship.
- Literals or URIs for title, year, name, venue, and similar fields become node properties.
- Blank nodes used for identifiers and author signatures are skipped.

The model therefore preserves connections between papers and authors, but does
not preserve author order stored in blank nodes or the datatype/language of
literals. This matches the scope of the paper–author experiments in the current
repository.

The current project environment already has `neo4j` and `pyoxigraph` installed.
Run the following commands from the project root:

```bash
conda activate graph_database_denormalization
python extend_experiment/dblp/import_database/import_data.py
```

The password has been redacted. Provide it through the `NEO4J_PASSWORD`
environment variable or enter it when prompted at runtime.

By default, the script connects to `bolt://localhost:7687` and creates or uses the
`dblp` database. You can also specify options:

```bash
python extend_experiment/dblp/import_database/import_data.py \
  --database dblp \
  --batch-size 5000
```

Import the first 100,000 triples to check the results:

```bash
python extend_experiment/dblp/import_database/import_data.py \
  --database dblp-preview \
  --limit 100000
```

The script uses uniqueness constraints and `MERGE`, so repeated runs do not
create duplicate nodes, property values, or author relationships. Restarting
after an interruption scans the gzip file from the beginning. Do not run two
import processes at the same time or import DBLP snapshots from different dates
into the same database.

Example query:

```cypher
MATCH (paper:DBLP_Publication)-[:AUTHORED_BY]->(author:DBLP_Person)
RETURN head(paper.`https://dblp.org/rdf/schema#title`) AS title,
       head(author.`https://dblp.org/rdf/schema#primaryCreatorName`) AS author
LIMIT 20;
```

The file is estimated to contain approximately 400 million triples, so a full
import will still take considerable time. A measured import of the first
10 million triples using the current script produced a 346 MiB Neo4j store
after checkpointing, containing 474,627 nodes and 614,573 relationships. Allowing
for uncertainty in the distribution of the full dataset, the final database is
expected to occupy approximately 15–25 GiB. Including transaction logs and
import headroom, reserve at least 30 GiB, or 40 GiB for a more conservative
margin.
