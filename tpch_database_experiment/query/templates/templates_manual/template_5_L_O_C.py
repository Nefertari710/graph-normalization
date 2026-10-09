"""TPC-H Q5 using schema MV q5_mv_lineitem_orders_customer (L, O, C)."""

TEMPLATE_Q5 = """
MATCH (loc:AUTO_JOIN_MV:`q5_mv_lineitem_orders_customer`)
      -[:CUSTOMER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
MATCH (loc)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n)
WHERE r.r_name = $region
  AND loc.o_orderdate >= date($date)
  AND loc.o_orderdate < date($date) + duration({years: 1})

RETURN
    n.n_name AS n_name,
    sum(loc.l_extendedprice * (1.0 - loc.l_discount)) AS revenue

ORDER BY
    revenue DESC
"""
