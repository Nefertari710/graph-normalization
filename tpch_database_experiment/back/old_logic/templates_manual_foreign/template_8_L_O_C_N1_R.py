"""TPC-H Q8 using schema MV q8_mv_lineitem_orders_customer_nation1_region (L, O, C, N1, R)."""

TEMPLATE_Q8 = """
MATCH (locn1r:AUTO_JOIN_MV:`q8_mv_lineitem_orders_customer_nation1_region`)
MATCH (s:SUPPLIER)-[:SUPPLIER_NATION]->(n2:NATION)
MATCH (p:PART)   
WHERE locn1r.r_name = $region
  AND locn1r.l_partkey = p.p_partkey
  AND locn1r.l_suppkey = s.s_suppkey
  AND locn1r.o_orderdate >= date('1995-01-01')
  AND locn1r.o_orderdate <= date('1996-12-31')
  AND p.p_type = $ptype

WITH
    locn1r.o_orderdate.year AS o_year,
    n2.n_name AS nation,
    locn1r.l_extendedprice * (1.0 - locn1r.l_discount) AS volume

RETURN
    o_year,
    sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
        / sum(volume) AS mkt_share

ORDER BY
    o_year
"""

# TEMPLATE_Q8 = """
# MATCH (locn1r:AUTO_JOIN_MV:`q8_mv_lineitem_orders_customer_nation1_region`)
#       -[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
#       -[:PARTSUPP_PART]->(p:PART)
# MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
#       -[:SUPPLIER_NATION]->(n2:NATION)
# WHERE locn1r.r_name = $region
#   AND locn1r.o_orderdate >= date('1995-01-01')
#   AND locn1r.o_orderdate <= date('1996-12-31')
#   AND p.p_type = $ptype
#
# WITH
#     locn1r.o_orderdate.year AS o_year,
#     n2.n_name AS nation,
#     locn1r.l_extendedprice * (1.0 - locn1r.l_discount) AS volume
#
# RETURN
#     o_year,
#     sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
#         / sum(volume) AS mkt_share
#
# ORDER BY
#     o_year
# """
