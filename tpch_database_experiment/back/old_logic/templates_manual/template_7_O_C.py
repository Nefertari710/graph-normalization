"""TPC-H Q7 using schema MV q7_mv_orders_customer (O, C)."""

TEMPLATE_Q7 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->
      (oc:AUTO_JOIN_MV:`q7_mv_orders_customer`)
      -[:CUSTOMER_NATION]->(n2:NATION)
MATCH (l)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n1:NATION)
WHERE (
        (n1.n_name = $nation1 AND n2.n_name = $nation2)
        OR
        (n1.n_name = $nation2 AND n2.n_name = $nation1)
      )
  AND l.l_shipdate >= date('1995-01-01')
  AND l.l_shipdate <= date('1996-12-31')

RETURN
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    l.l_shipdate.year AS l_year,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue

ORDER BY
    supp_nation,
    cust_nation,
    l_year
"""
