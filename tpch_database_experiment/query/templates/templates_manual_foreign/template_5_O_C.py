"""TPC-H Q5 using schema MV q5_mv_orders_customer (O, C)."""

TEMPLATE_Q5 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->
      (oc:AUTO_JOIN_MV:`q5_mv_orders_customer`)
      -[:CUSTOMER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
MATCH (l)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n)
WHERE r.r_name = $region
  AND oc.o_orderdate >= date($date)
  AND oc.o_orderdate < date($date) + duration({years: 1})

RETURN
    n.n_name AS n_name,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue

ORDER BY
    revenue DESC
"""
