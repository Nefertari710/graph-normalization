# Import DBpedia into Neo4j

`import_data.py` reads the four `.ttl.bz2` files in the sibling `dataset/` directory,
parses RDF as a stream, and writes batches using the official Neo4j Python Driver.
It requires Python **3.11+** and Neo4j **5.26+**, and supports the **2026.06.0** version
recorded for this project. Both Community and Enterprise are supported; APOC and n10s are not required.

## Usage

Install the dependencies in the project Python environment, working from the project root.
This project currently uses Conda:

```bash
conda activate graph_database_denormalization
conda install -c conda-forge neo4j-python-driver pyoxigraph
```

For a separate pip/venv environment, install the dependencies from this directory's `requirements.txt`.

Set the password and target database name in the connection configuration at the top of `import_data.py`:

```python
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""  # Enter your password here.
NEO4J_DATABASE = "dbpedia"
```

If the password is empty and the `NEO4J_PASSWORD` environment variable is unset,
the program prompts for a password at runtime. Start Neo4j, then run the import:

```bash
python extend_experiment/dbpedia_experiment/import_database/import_data.py
```

Specify the target database and batch size:

```bash
python extend_experiment/dbpedia_experiment/import_database/import_data.py --database dbpedia --batch-size 5000
```

The program also accepts the `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, and `NEO4J_DATABASE`
environment variables, along with the `--uri`, `--user`, and `--data-dir` options.
Connection settings take precedence in this order: explicit command-line arguments > environment variables > configuration at the top of the file.
During a normal import, the program first connects to the `system` database. If the target database is missing,
it runs `CREATE DATABASE $name IF NOT EXISTS WAIT 60 SECONDS` and confirms that its status is `online`
before importing. An existing database is used directly.
Automatic database creation requires Neo4j Enterprise and permission to create databases.
Community can import into an existing database, for example by setting `NEO4J_DATABASE` to `"neo4j"`.

The first import displays standard initialization messages:

```text
[database] dbpedia is online
[database] First import: initializing import metadata and constraints.
[import] instance-types_lang=en_specific.ttl.bz2
```

The preparation stage reads existing labels first, avoiding lengthy notifications caused by querying
import labels that do not yet exist in an empty database. Neo4j warning and error logs remain enabled.

The program exits after writing all four files. It no longer runs per-triple `verify` checks
or counts all nodes and relationships at the end. The completion message is:

```text
Imported 67,046,342 source triples.
```

`--verify-only` has been removed. A process already in the old `[verify]` stage can be stopped;
committed import data is retained, and removing verification does not require reimporting.
Changing the script does not automatically change the execution logic of an already running process.

## Data representation

| Source | Neo4j representation |
| --- | --- |
| Entity URIs in all four files, plus object URIs in the objects file | `(:DBpediaResource {uri: full_URI})`, with a uniqueness constraint on the URI |
| `rdf:type` | The full type URI is used as a label, such as `http://dbpedia.org/ontology/Band`, and is also retained in the `rdf_types` string array |
| `rdfs:label` | A string array stored under the property key `http://www.w3.org/2000/01/rdf-schema#label` |
| Literal properties | The full predicate URI is the property key, such as `http://dbpedia.org/ontology/alias`, with values such as `["Chk Chk Chk"]` |
| Resource relationships | The full predicate URI is the directed relationship type, such as `http://dbpedia.org/ontology/bandMember`; the relationship's `predicate` property also stores this URI |

Following [readDBpedia() in main.cpp](../GraphNormalization-main/main.cpp), type, property, and relationship names use full URIs directly.
There is no prefix table or abbreviation conversion. Full URIs distinguish identically named terms in different namespaces.
For example, `http://dbpedia.org/ontology/name` and `http://dbpedia.org/property/name` are separate properties.
In Cypher, enclose full URIs in backticks or access properties dynamically with `n[$property_uri]`;
the importer passes these names as parameters.
Original URIs are not truncated or rewritten. Resources with only a label, or appearing only as relationship targets, are retained.
The `specific` types do not expand parent classes; the program performs no inference or domain filtering.

The program reads the four files in the current dataset. The reference implementation also reads
commons-sameAs and infobox files. Those files are absent from this directory, so this importer does not
merge entity aliases or import the additional properties from them.

To keep the import simple and preserve original values, **all literal properties are string arrays, including numbers and dates**.
For example, `http://dbpedia.org/ontology/activeYearsStartYear` is stored as `["1996"]`;
use `toInteger()` in a query when numeric calculations are needed.
The program does not convert original years to full dates or high-precision decimals to floating-point numbers.

Each node also has an `rdf_literals` string array. Each element is a JSON record:

```text
[full predicate URI, original lexical value, full datatype URI, language or null]
```

For example, an alias is recorded as:

```json
["http://dbpedia.org/ontology/alias","Chk Chk Chk","http://www.w3.org/1999/02/22-rdf-syntax-ns#langString","en"]
```

Records with the same text but different languages or datatypes are retained separately.
Property arrays used for queries are deduplicated by text; `rdf_literals` preserves the complete RDF literal identity.
JSON records are stored as strings because Neo4j properties cannot directly store nested dictionaries.
This uses additional storage but preserves the association between each value, its language, and its datatype.

## Correctness and retries

A normal run performs these steps:

1. Compute SHA-256 hashes of the four compressed files to identify the current data snapshot. The full RDF `check` before import has been removed.
2. Create the target database if necessary and confirm that it is online, then create the URI uniqueness constraint and a `:DBpediaImport` record. While parsing, check predicate and object types for each file role, batch `MERGE` nodes, properties, and relationships, and count source triples.
3. Confirm that the compressed source file SHA-256 hashes have not changed, mark the write as complete, and report the number of source triples processed.
   This step does not parse RDF again or reread Neo4j to verify each triple.

Parsing or batch-write failures produce a nonzero exit code; erroneous records are not skipped.
Structures outside the conventions of these four file types, such as blank nodes, produce explicit errors.
The original labels file was found to contain IRIs with nonstandard characters such as U+FFF9,
so parsing uses PyOxigraph's `lenient=True`.
These identifiers are preserved exactly, without correction or removal. The program still checks RDF structure,
file roles, and object types. A normal run parses each RDF file once, and memory use is mainly determined by batch size.
If an empty file or invalid record is detected during import, the program stops; previously committed batches are retained.

Rerunning the same files does not add duplicate nodes, relationships, or array elements.
After interruption, rerun the same command to scan the files again and fill in missing data;
previously committed batches are not rolled back as a whole.
Exact duplicate RDF triples are merged according to set semantics, so the number of source records
may exceed the number of distinct RDF facts stored.

Different file checksums or mapping versions are rejected when writing to existing DBpedia data,
preventing another snapshot from being mixed in.
The current `model_version` is **2** and uses full URI names. If data was previously imported using
a version with prefixes such as `dbo__`, reimport into a new empty database or instance.
The program rejects mixed naming models and does not automatically modify the old graph.
It does not delete existing data; the `DBpediaResource` label separates this import process from other data.
Give this importer exclusive write access to the DBpedia data and keep the source files unchanged.
URI uniqueness constraints, `MERGE` deduplication, RDF structure checks, per-batch count checks,
and source-file matching safeguards remain in place.
These checks do not constitute a database reread to verify all source facts; per-triple verification has been removed.

In the import record, `complete=true` means all files were written successfully, and `verified=false`
means no post-import RDF verification was performed.
`complete=true` is set only after all batches and source-file checks succeed; failures leave an incomplete state.
At the start of a retry, the previous completion time and source counts are cleared.
The FD search program reports write completion and verification status separately.

## Query example

```cypher
MATCH (band:DBpediaResource:`http://dbpedia.org/ontology/Band` {
  uri: 'http://dbpedia.org/resource/!!!'
})-[r:`http://dbpedia.org/ontology/bandMember`]->(member:DBpediaResource)
RETURN band.`http://www.w3.org/2000/01/rdf-schema#label` AS band_name,
       band.`http://dbpedia.org/ontology/activeYearsStartYear` AS start_year,
       type(r), member.uri,
       member.`http://www.w3.org/2000/01/rdf-schema#label` AS member_name;
```

## Find functional dependencies within classes

Run `find_functional_dependency.py` in its separate directory. It neither imports nor depends on `import_data.py`.
Configure the connection URI, username, password, and database name separately at the top of the search script;
it also accepts `NEO4J_*` environment variables and command-line arguments.
An empty password triggers a runtime prompt. The program runs read-only queries and does not modify Neo4j.

```python
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""
NEO4J_DATABASE = "dbpedia1"
```

The search program does not read connection settings from `import_database/`.
Configuration precedence is: command-line `--uri`, `--user`, and `--database` > corresponding `NEO4J_*`
environment variables > configuration at the top of the search file.
The password comes from `NEO4J_PASSWORD` or the file configuration; an interactive prompt is used only when both are empty.

```bash
python extend_experiment/dbpedia_experiment/find_functional_dependency/find_functional_dependency.py
```

By default, the program starts with small classes and traverses **all explicit RDF classes** in the database.
It treats nodes of the same class as table rows and their RDF literal properties as columns, searching for:

- One determinant field and one dependent field: `X → Y`.
- One determinant field and multiple dependent fields: `X → {Y1, Y2, ..., Yn}`, with `N >= 2` by default.

Here, `1 → N` means one field determines N fields, not that one graph node connects to multiple nodes.
For example, `shippingAddress → {city, country}` means nodes with the same shipping address have
the same complete tuple of city and country values.
The search no longer stops at 10 dependencies, keeps only 2 per class, or skips classes with more than 100,000 instances.
Classes from other namespaces, such as `owl:Thing`, are included; internal labels `DBpediaResource` and `DBpediaImport` are excluded.
Both X and Y must be RDF literal properties on the current node.
Outgoing relationships and their target URIs are excluded, as are labels, node URIs, RDF types,
the aggregate `rdf_literals` storage property, and import metadata.
The program reads original values of individual properties from `rdf_literals` to retain datatypes and languages.
Property URIs in the report still correspond to the full URI property keys shown in the node's Properties panel.
Only imported explicit class labels are used; subclass instances are not automatically included.
Nodes without class labels are outside the scope of the within-class search.
This is not an exhaustive search of arbitrary FDs: dependencies with composite determinants, such as `(X1, X2) → Y`, are not searched yet.

For example, within the `Person` class, `birthDate → birthYear` means that two nodes with the same
`birthDate` must also have the same `birthYear`.
These repeated property combinations within nodes are the candidates this program provides for further normalization.

Validation conditions and default thresholds:

- **Read the entire class.** For a single RHS, X and Y must each have exactly one RDF literal; for multiple RHS fields, X and every Y must all be single-valued.
  No sampling or `LIMIT` is used.
- Reject a candidate if the same X has two different Y values. Different X values may share the same Y.
- Require at least 50 eligible instances, at least 50% class coverage, and at least 3 distinct X values that each occur in two or more instances.
- Exclude cases where X is entirely unique, Y is constant, or the two fields have identical values. Validate `X → Y` and `Y → X` separately and retain both if both hold.
- Instances with missing or multiple values do not participate in the FD check for that field pair, but are counted separately and included in coverage reporting.
  Consequently, `complete_single_valued_subset` **means an FD only within the complete single-valued subset**, not the entire class;
  `whole_class` means every class instance satisfies the conditions.
- Compare values by original lexical text, datatype, and language to avoid treating distinct RDF literals as equal.
- Multi-RHS candidates are formed from multiple qualifying `X → Y` dependencies with the same X. Support, coverage, repeated X groups, and complete Y tuples are then revalidated on **nodes participating in all fields**.
  The report retains the largest qualifying RHS sets. If adding a sparse field makes a set ineligible, smaller field combinations are still checked.
- Each listed Y must have at least two values among the jointly participating nodes and must not simply duplicate X.
  Only independently qualifying single-RHS dependencies are combined, making this a conservative search:
  it does not report multi-RHS combinations that hold only after excluding nodes missing another Y.

Outputs are saved alongside the search script:

- `functional_dependencies_all.md`: current progress, result tables, full property URIs, coverage, and actual node examples.
- `functional_dependencies_all.json`: `dependencies` stores single-RHS results, and `multi_dependencies` stores 1→N results;
  it also records the class inventory and completed, skipped, failed, and pending classes.

The files are updated atomically after each class is completed.
Classes larger than `MEMORY_CLASS_SIZE` (100000 by default) are streamed to temporary SQLite storage,
validated by grouping on disk, and removed from temporary storage afterward.
This threshold selects the processing method; it is **not a search size limit**.
Use `--temp-dir` to select the temporary disk location; the default is the system temporary directory.
No separate SQLite service or Python package is required.
By default, `--timeout 0` imposes no transaction timeout on full-class reads; set a duration in seconds if needed.

After interruption, resume with the same search conditions:

```bash
python extend_experiment/dbpedia_experiment/find_functional_dependency/find_functional_dependency.py --resume
```

Checkpoints are per class: the interrupted class is read again, while completed classes are not recomputed.
On resume, the program checks database identity, import status/manifest, the class inventory and node counts,
and search parameters. A mismatch requires starting again.
These checks cannot detect every content change; **resuming still assumes the graph data has not changed in the meantime**.
If data was imported or modified, search again without `--resume`.
A class-read failure is recorded and other classes continue; the final status is `incomplete`, with exit code 2.
Failed classes can be retried on resume.
Ctrl+C saves completed classes and exits with code 130. `running`, `interrupted`, and `limited` do not mean the full-database search is complete.

Search only selected classes, or require 100% class-instance coverage:

```bash
python extend_experiment/dbpedia_experiment/find_functional_dependency/find_functional_dependency.py --classes Archaea Plant PopulatedPlace
python extend_experiment/dbpedia_experiment/find_functional_dependency/find_functional_dependency.py --min-coverage 1
```

`--count`, `--max-class-size`, and `--max-per-class` all default to **0 (unlimited)**.
For a short run, set limits explicitly, for example `--count 10 --output /tmp/dbpedia_fd_preview.json`;
the report records the limits.
Classes with fewer nodes than `--min-support` cannot meet the support threshold and are recorded as `below_min_support` without further processing.
Finding no dependencies is not itself an error. The program succeeds when the entire selected scope has been processed without failures.
Use `--help` for additional thresholds; commonly used defaults are also listed at the top of the file.
`--min-dependents` controls the minimum number of RHS fields for 1→N results and defaults to 2.

To broaden the candidate scope, lower support and coverage thresholds, while distinguishing coincidental
patterns from semantic dependencies suitable for normalization:

```bash
python extend_experiment/dbpedia_experiment/find_functional_dependency/find_functional_dependency.py --min-support 10 --min-coverage 0.1 --output /tmp/dbpedia_fd_broader.json
```

These results are **dependencies in the current data**, not automatically permanent business rules.
For example, even a zero-conflict "birth date → highest ranking" result in a small class may simply reflect
the same athlete appearing in ranking records across multiple years; it does not establish that birth date determines ranking.
Review the reported examples and their meaning before using the results in normalization experiments.
The program records import `complete` and `verified` statuses separately, but does not prevent other programs from writing;
the entire search is not guaranteed to use a single isolated snapshot.
If an import is still writing, search again after it completes.
The current importer does not verify every triple, so `verified` remains `false` after a successful write.
For old records without `verified`, the value is inferred from `complete` under the old semantics.
An old `complete=false` does not indicate whether writing has finished and verification is in progress.

On 2026-09-11, a v5 full-database run searching only node literal properties completed on the `dbpedia1`
database in about 3 minutes 42 seconds.
The inventory contained 439 explicit classes and 7,707,325 class memberships (not distinct nodes).
395 classes were read and evaluated in full; 44 were excluded for having fewer than 50 nodes.
There were no failed or pending classes. 13 large classes used SQLite, including the largest class,
CareerStation, with 1,716,420 instances, all of which were read.

The results contain **7 single-RHS dependencies and 1 group of 1→N dependencies**. See
[`functional_dependencies_all.md`](../find_functional_dependency/functional_dependencies_all.md) and
[`functional_dependencies_all.json`](../find_functional_dependency/functional_dependencies_all.json):

- `SnookerWorldRanking.birthDate → activeYearsStartYear`
- `SnookerWorldRanking.birthDate → highestRank`
- `PopulatedPlace.areaCode → utcOffset`
- `Philosopher.birthDate → birthYear`
- `Actor.birthDate → birthYear`
- `Drug.fdaUniiCode → chEMBL`
- `Person.birthDate → birthYear`

The only multi-RHS result is
`SnookerWorldRanking.birthDate → {activeYearsStartYear, highestRank}`.
The old `Archaea.family/order` results came from outgoing RDF relationships rather than node properties,
so they were correctly excluded from this within-node FD search.
For the actual SnookerWorldRanking and Archaea classes, forcing both the in-memory and SQLite paths
produced identical results, statistics, and examples.
Three offline tests also passed, covering relationship exclusion, duplicate literals, multivalued properties,
and datatype/language distinctions.
These dependencies are candidate inputs for decomposition. A meaningful 3NF/BCNF decomposition still requires
checking the determinant's business meaning, a minimal dependency set, and lossless joins after decomposition.

At both the start and end of this full-database run, the import record was `complete=true`, `verified=false`:
all four files had been written, but the current import workflow had not performed the costly post-import verification of every RDF triple.
The FD search itself completed in full and does not replace import verification.

The implementation references [Neo4j read-only sessions, query timeouts, and identifier handling](https://neo4j.com/docs/python-manual/current/query-advanced/)
and [RDF literal identity](https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal).

## References

- [Source data description](../dataset/README.md): roles and examples for the four files.
- [GraphNormalization main.cpp](../GraphNormalization-main/main.cpp): `readDBpedia()` preserves full URIs as resource identifiers, types, property keys, and edge labels.
- [PyOxigraph RDF parsing](https://pyoxigraph.readthedocs.io/en/stable/io.html): streaming Turtle parsing from compressed input, yielding RDF records.
- [W3C RDF 1.1 Literals](https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal): lexical values, datatypes, and language information.
- [Neo4j Python Driver performance recommendations](https://neo4j.com/docs/python-manual/current/performance/#batch-data-creation): batch writes with `UNWIND`.
- [Neo4j MERGE](https://neo4j.com/docs/cypher-manual/current/clauses/merge/): dynamic relationship types and deduplication.
- [Neo4j COUNT subqueries](https://neo4j.com/docs/cypher-manual/current/subqueries/count/): checking per-triple match counts.
- [Neo4j database creation](https://neo4j.com/docs/operations-manual/current/database-administration/standard-databases/create-databases/): run `CREATE DATABASE ... IF NOT EXISTS ... WAIT` through the `system` database.

## Historical test records

The following includes tests performed before `verify` was removed.
The current program no longer provides `--verify-only` or automatic per-triple verification.

On 2026-09-10, the project's Python 3.13.14, PyOxigraph 0.5.6, and Neo4j Driver 6.2.0 environment
was used to check all four current compressed files:

| File | Source triples, including duplicate records |
| --- | ---: |
| instance-types | 7,707,325 |
| labels | 16,904,466 |
| mappingbased-literals | 19,643,380 |
| mappingbased-objects | 22,791,171 |
| Total | **67,046,342** |

Integration tests also passed in a temporary Neo4j Enterprise 2026.06.0 instance, covering multivalued properties
across batches, repeated imports, original high-precision numeric text, escaped characters, resources without
labels or types, and rejection of missing data, duplicate relationships, incorrect predicates, and different snapshots.
These tests validate the workflow; full writes and per-triple verification of the production database
are performed by actually running the import command.

The full-URI naming version also passed 3 temporary tests covering identically named terms in different namespaces,
Cypher queries using full URIs, repeated imports, data verification, and rejection of the old model.
Another 200 records from each of the four actual files were sampled, imported, and verified in temporary Neo4j:
**800 triples, 562 resource nodes, and 200 relationships**.

After removing the pre-import `check`, those 3 tests passed again.
A normal import was confirmed to parse each source file only twice, and `--verify-only` only once,
with no `[check]` stage in either mode.

After adding automatic database creation, 4 tests passed, covering first-time creation and import,
retention of an existing database and its data on repeated runs, no database creation or import-record modification
in `--verify-only`, and stopping when the target database was offline.
These tests used only databases in a temporary instance.

After removing `verify`, 7 new temporary tests passed, covering single-pass parsing of the four compressed files,
normal writes, batch errors, RDF errors, source changes, retry state, and removed CLI parameters.
They confirmed that no verification queries or final full-graph counts were executed.
Another 6 tests confirmed that FD reports distinguish write completion from verification status
and remain compatible with old status semantics.
The tests used real RDF parsing and a simulated Neo4j connection; production data was not reimported.
