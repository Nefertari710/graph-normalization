"""TPC-H Q8 using schema MV q8_mv_lineitem_orders (L, O)."""

TEMPLATE_Q8 = """
MATCH (lo:AUTO_JOIN_MV:`q8_mv_lineitem_orders`)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n1:NATION)
      -[:NATION_REGION]->(r:REGION)
MATCH (s:SUPPLIER)-[:SUPPLIER_NATION]->(n2:NATION)
MATCH (p:PART)
      
      
WHERE r.r_name = $region
  AND lo.l_partkey = p.p_partkey
  AND lo.l_suppkey = s.s_suppkey
  AND lo.o_orderdate >= date('1995-01-01')
  AND lo.o_orderdate <= date('1996-12-31')
  AND p.p_type = $ptype

WITH
    lo.o_orderdate.year AS o_year,
    n2.n_name AS nation,
    lo.l_extendedprice * (1.0 - lo.l_discount) AS volume

RETURN
    o_year,
    sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
        / sum(volume) AS mkt_share

ORDER BY
    o_year
"""

# TEMPLATE_Q8 = """
# MATCH (lo:AUTO_JOIN_MV:`q8_mv_lineitem_orders`)
#       -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
#       -[:CUSTOMER_NATION]->(n1:NATION)
#       -[:NATION_REGION]->(r:REGION)
# MATCH (lo)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
#       -[:PARTSUPP_PART]->(p:PART)
# MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
#       -[:SUPPLIER_NATION]->(n2:NATION)
#
#
# WHERE r.r_name = $region
#   AND lo.o_orderdate >= date('1995-01-01')
#   AND lo.o_orderdate <= date('1996-12-31')
#   AND p.p_type = $ptype
#
# WITH
#     lo.o_orderdate.year AS o_year,
#     n2.n_name AS nation,
#     lo.l_extendedprice * (1.0 - lo.l_discount) AS volume
#
# RETURN
#     o_year,
#     sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
#         / sum(volume) AS mkt_share
#
# ORDER BY
#     o_year
# """