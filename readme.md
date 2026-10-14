# TPC-H to Neo4j importer

## Environment prepare

### install Neo4j

1. Install Neo4j 2026.06.0 in `$HOME/neo4j`.

```bash
mkdir -p "$HOME/neo4j"
```

community version
```bash
tar -xzf "$HOME/Downloads/neo4j-community-2026.06.0-unix.tar.gz" -C "$HOME/neo4j/"
```

enterprise version
```bash
tar -xzf "$HOME/Downloads/neo4j-enterprise-2026.06.0-unix.tar.gz" -C "$HOME/neo4j/"
```

2. Write down in ~/.bash_profile

vim ~/.bash_profile
```bash
export NEO4J_HOME="$HOME/neo4j/neo4j-enterprise-2026.06.0"
export PATH="$NEO4J_HOME/bin:$PATH"
```

source ~/.bash_profile

Run `command -v neo4j` to verify that it resolves to `$NEO4J_HOME/bin/neo4j`.

3. Start the neo4j

```bash
neo4j start
```

### create conda env

1. available version for python

```bash
# requires Python >= 3.10

# Python 3.14 supported.
# Python 3.13 supported.
# Python 3.12 supported.
# Python 3.11 supported.
# Python 3.10 supported.

conda create --name graph_database_denormalization python=3.13

conda activate graph_database_denormalization
```

2. install python neo4j

```bash
pip install neo4j
```

## Import the TPC-H `.tbl` data

Run the following commands from the experiment directory:

```bash
cd tpch_database_experiment
```

The Enterprise loader reads the eight files in `data/tbl_sf_01` through the Neo4j
Python driver, so the files do not need to be copied into Neo4j's `import`
directory. Its default target database is `tpch-sf-01`.

First, check that every source row has the expected TPC-H columns and types:

```bash
conda activate graph_database_denormalization
python data/import_data_from_tbl_enterprise.py --validate-only
```

Then start Neo4j and run the import. If `NEO4J_PASSWORD` is not set, the
script asks for it without displaying it on screen:

```bash
neo4j start
python data/import_data_from_tbl_enterprise.py
```

Connection settings can be changed with command-line arguments or environment
variables:

```bash
export NEO4J_URI='bolt://localhost:7687'
export NEO4J_USER='neo4j'
export NEO4J_DATABASE='tpch-sf-01'

python data/import_data_from_tbl_enterprise.py --batch-size 1000

# Or load another dbgen output directory with the same eight file names.
python data/import_data_from_tbl_enterprise.py --data-dir /path/to/tbl_dir
```

Run `python data/import_data_from_tbl_enterprise.py --help` for all options.
The normal first import into an empty graph executes in this order:

1. Create all nodes.
2. Add the eight primary-key Node Key constraints and foreign-key indexes.
3. Create all foreign-key relationships.
4. Validate the final node and relationship counts.

The loader uses Neo4j Enterprise Node Key constraints, which require every
primary-key property to exist and make each key value or composite key unique.
The loader does not delete existing data. Use an empty or dedicated database.
When the same source data is retried after an interrupted or completed run,
the loader first ensures that the constraints and foreign-key indexes exist
and then uses `MERGE` to resume without duplicating nodes or relationships.
Loading a different data set into the same labels can leave old nodes behind
and will fail the final exact-count validation. Every `.tbl` row must retain
dbgen's trailing `|`.

### Graph model

Every TPC-H table is preserved as a node label. Labels use the uppercase
TPC-H table names, including the plural table name `ORDERS`.

| Source table | Neo4j label | Node key |
| --- | --- | --- |
| `region.tbl` | `REGION` | `r_regionkey` |
| `nation.tbl` | `NATION` | `n_nationkey` |
| `supplier.tbl` | `SUPPLIER` | `s_suppkey` |
| `customer.tbl` | `CUSTOMER` | `c_custkey` |
| `part.tbl` | `PART` | `p_partkey` |
| `partsupp.tbl` | `PARTSUPP` | `(ps_partkey, ps_suppkey)` |
| `orders.tbl` | `ORDERS` | `o_orderkey` |
| `lineitem.tbl` | `LINEITEM` | `(l_orderkey, l_linenumber)` |

Foreign keys become these directed relationships:

```text
(NATION)-[:NATION_REGION]->(REGION)
(SUPPLIER)-[:SUPPLIER_NATION]->(NATION)
(CUSTOMER)-[:CUSTOMER_NATION]->(NATION)
(ORDERS)-[:ORDERS_CUSTOMER]->(CUSTOMER)
(PARTSUPP)-[:PARTSUPP_PART]->(PART)
(PARTSUPP)-[:PARTSUPP_SUPPLIER]->(SUPPLIER)
(LINEITEM)-[:LINEITEM_ORDERS]->(ORDERS)
(LINEITEM)-[:LINEITEM_PARTSUPP]->(PARTSUPP)
```

For the current SF 0.1 files, expected validation totals are `866,602` nodes
and `1,527,169` relationships. Dates are stored as Neo4j `DATE` values; TPC-H
decimal values are stored as Neo4j `FLOAT` values.
