#!/usr/bin/env bash
# TPC-H SF 1 update experiment on Neo4j Community (single database "neo4j").
# 1. clears the current database (the Offshore graph can be re-imported with
#    offshore_experiment/usecases/import_community.py),
# 2. imports TPC-H SF 1 generated with DuckDB's dbgen into ~/neo4j/tpch_sf1,
# 3. runs new_redundancy.py for representative de-normalization masks.
# Masks (bits e1..e8 = 1,2,4,...,128): {e2}=2, {e5}=16, {e2,e5}=18,
# {e6}=32, {e5,e6}=48, {e7}=64, {e4,e7}=72, {e8}=128, {e6,e8}=160,
# {e5,e6,e8}=176.
set -euo pipefail
source ~/neo4j/env.sh
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HERE/back/results_sf1/new_redundancy_results.json"
mkdir -p "$(dirname "$OUT")"

if [ "${SKIP_IMPORT:-0}" != "1" ]; then
  echo "Clearing database neo4j ..."
  cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
    "MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS;"
  for idx in $(cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
      "SHOW INDEXES YIELD name, type, owningConstraint WHERE type <> 'LOOKUP' AND owningConstraint IS NULL RETURN name;" | tail -n +2 | tr -d '"'); do
    cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "DROP INDEX \`$idx\` IF EXISTS;"
  done
  echo "Importing TPC-H SF 1 ..."
  time python "$HERE/data/import_data_from_tbl_community.py" --data-dir ~/neo4j/tpch_sf1
fi

echo "Running update experiment ..."
time python "$HERE/back/redundancy_20_9/scripts/new_redundancy.py" \
  --database neo4j --output "$OUT" \
  --mask 2 --mask 16 --mask 18 --mask 32 --mask 48 \
  --mask 64 --mask 72 --mask 128 --mask 160 --mask 176
echo "Done: $OUT"
