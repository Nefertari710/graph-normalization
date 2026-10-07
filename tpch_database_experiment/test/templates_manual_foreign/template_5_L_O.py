"""TPC-H Q5 using schema MV q5_mv_lineitem_orders (L, O)."""

TEMPLATE_Q5 = """
MATCH (lo:AUTO_JOIN_MV:`q5_mv_lineitem_orders`)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
MATCH (lo)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n)
WHERE r.r_name = $region
  AND lo.o_orderdate >= date($date)
  AND lo.o_orderdate < date($date) + duration({years: 1})

RETURN
    n.n_name AS n_name,
    sum(lo.l_extendedprice * (1.0 - lo.l_discount)) AS revenue

ORDER BY
    revenue DESC
"""
