#!/usr/bin/env bash
# TPC-H SF 1 query experiment for the 20 variants of the query table
# (12 that favour normalization, 8 that favour de-normalization at SF 0.1).
# Requires the SF 1 graph in database "tpch-sf-1-usecase" (see run_sf1.py).
# One JSON report per variant in usecases/results/sf1/query/; finished variants are
# skipped, so the script can simply be restarted after an interruption.
# A variant whose build fails with the default batch size is retried with
# --batch-size 1000 (transaction-memory limit on LINEITEM folds).
set -uo pipefail
source ~/neo4j/env.sh
HERE="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$HERE/usecases/results/sf1/query"
mkdir -p "$OUT"
cd "$HERE"

# query-id:strategy, cheap builds first, LINEITEM folds last
VARIANTS=(
  21:q21_mv_supplier_nation
  9:q9_mv_supplier_nation
  7:q7_mv_supplier_nation1
  10:q10_mv_customer_nation
  13:q13_mv_orders_customer
  22:q22_mv_orders_customer
  3:q3_mv_orders_customer
  5:q5_mv_orders_customer
  8:q8_mv_orders_customer
  16:q16_mv_partsupp_supplier_part
  20:q20_mv_partsupp_supplier_part
  11:q11_mv_partsupp_supplier_nation
  2:q2_mv_partsupp_supplier_nation_region
  5:q5_mv_lineitem_orders
  8:q8_mv_lineitem_orders
  12:q12_mv_lineitem_orders
  18:q18_mv_lineitem_orders
  7:q7_mv_lineitem_orders_customer
  9:q9_mv_lineitem_orders_partsupp
  20:q20_mv_lineitem_partsupp_supplier_part
)

ok() { python - "$1" <<'EOF'
import json, sys
try:
    r = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(1)
sys.exit(0 if isinstance(r, list) and r and all("template_result" in x for x in r) else 1)
EOF
}

n=0
for v in "${VARIANTS[@]}"; do
  n=$((n + 1)); q="${v%%:*}"; s="${v#*:}"; j="$OUT/$s.json"
  if [ -f "$j" ] && ok "$j"; then echo "[$n/20] $s: done, skipped"; continue; fi
  for bs in 10000 1000; do
    echo "[$n/20] $s (batch size $bs) $(date +%H:%M:%S)"
    if python query/scripts/run_benchmark.py --database tpch-sf-1-usecase --sf 1 \
        --query-id "$q" --strategy "$s" --batch-size "$bs" \
        --output-json "$j" > "$OUT/$s.log" 2>&1 && ok "$j"; then
      tail -n 3 "$OUT/$s.log" | head -n 1
      break
    fi
    echo "  failed (see $OUT/$s.log)"; rm -f "$j"
  done
done
echo "All variants finished $(date +%H:%M:%S)"
