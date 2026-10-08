"""TPC-H Q8 using schema MV q8_mv_supplier_nation2 (S, N2)."""

TEMPLATE_Q8 = """

MATCH (l)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n1:NATION)
      -[:NATION_REGION]->(r:REGION)
MATCH (p:PART)
MATCH (sn2:AUTO_JOIN_MV:`q8_mv_supplier_nation2`)
WHERE r.r_name = $region
  AND l.l_partkey = p.p_partkey
  AND l.l_suppkey = sn2.s_suppkey
  AND o.o_orderdate >= date('1995-01-01')
  AND o.o_orderdate <= date('1996-12-31')
  AND p.p_type = $ptype

WITH
    o.o_orderdate.year AS o_year,
    sn2.n_name AS nation,
    l.l_extendedprice * (1.0 - l.l_discount) AS volume

RETURN
    o_year,
    sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
        / sum(volume) AS mkt_share

ORDER BY
    o_year
"""


# TEMPLATE_Q8 = """
# MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
#       -[:PARTSUPP_PART]->(p:PART)
# MATCH (ps)-[:PARTSUPP_SUPPLIER]->
#       (sn2:AUTO_JOIN_MV:`q8_mv_supplier_nation2`)
# MATCH (l)-[:LINEITEM_ORDERS]->(o:ORDERS)
#       -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
#       -[:CUSTOMER_NATION]->(n1:NATION)
#       -[:NATION_REGION]->(r:REGION)
# WHERE r.r_name = $region
#   AND o.o_orderdate >= date('1995-01-01')
#   AND o.o_orderdate <= date('1996-12-31')
#   AND p.p_type = $ptype
#
# WITH
#     o.o_orderdate.year AS o_year,
#     sn2.n_name AS nation,
#     l.l_extendedprice * (1.0 - l.l_discount) AS volume
#
# RETURN
#     o_year,
#     sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
#         / sum(volume) AS mkt_share
#
# ORDER BY
#     o_year
# """
