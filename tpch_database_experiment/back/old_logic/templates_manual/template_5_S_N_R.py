"""TPC-H Q5 using schema MV q5_mv_supplier_nation_region (S, N, R)."""

TEMPLATE_Q5 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(customer_n:NATION)
MATCH (l)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->
      (snr:AUTO_JOIN_MV:`q5_mv_supplier_nation_region`)
WHERE customer_n.n_nationkey = snr.n_nationkey
  AND snr.r_name = $region
  AND o.o_orderdate >= date($date)
  AND o.o_orderdate < date($date) + duration({years: 1})

RETURN
    snr.n_name AS n_name,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue

ORDER BY
    revenue DESC
"""
