"""TPC-H Q5 using schema MV q5_mv_supplier_nation (S, N)."""

TEMPLATE_Q5 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(customer_n:NATION)
MATCH (l)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(sn:AUTO_JOIN_MV:`q5_mv_supplier_nation`)
      -[:NATION_REGION]->(r:REGION)
WHERE customer_n.n_nationkey = sn.n_nationkey
  AND r.r_name = $region
  AND o.o_orderdate >= date($date)
  AND o.o_orderdate < date($date) + duration({years: 1})

RETURN
    sn.n_name AS n_name,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue

ORDER BY
    revenue DESC
"""
