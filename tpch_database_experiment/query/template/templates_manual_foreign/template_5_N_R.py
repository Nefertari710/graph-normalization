"""TPC-H Q5 using schema MV q5_mv_nation_region (N, R)."""

TEMPLATE_Q5 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(nr:AUTO_JOIN_MV:`q5_mv_nation_region`)
MATCH (l)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(nr)
WHERE nr.r_name = $region
  AND o.o_orderdate >= date($date)
  AND o.o_orderdate < date($date) + duration({years: 1})

RETURN
    nr.n_name AS n_name,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue

ORDER BY
    revenue DESC
"""
