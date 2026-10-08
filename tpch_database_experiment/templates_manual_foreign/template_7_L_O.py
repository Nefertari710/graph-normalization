"""TPC-H Q7 using schema MV q7_mv_lineitem_orders (L, O)."""

TEMPLATE_Q7 = """

      
MATCH (lo:AUTO_JOIN_MV:`q7_mv_lineitem_orders`)-[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n2:NATION)
MATCH (s:SUPPLIER)-[:SUPPLIER_NATION]->(n1:NATION)
WHERE lo.l_suppkey = s.s_suppkey
  AND (
        (n1.n_name = $nation1 AND n2.n_name = $nation2)
        OR
        (n1.n_name = $nation2 AND n2.n_name = $nation1)
      )
  AND lo.l_shipdate >= date('1995-01-01')
  AND lo.l_shipdate <= date('1996-12-31')

RETURN
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    lo.l_shipdate.year AS l_year,
    sum(lo.l_extendedprice * (1.0 - lo.l_discount)) AS revenue

ORDER BY
    supp_nation,
    cust_nation,
    l_year
"""
