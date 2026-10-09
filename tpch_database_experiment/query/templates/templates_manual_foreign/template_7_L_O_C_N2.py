"""TPC-H Q7 using schema MV q7_mv_lineitem_orders_customer_nation2 (L, O, C, N2)."""
TEMPLATE_Q7 = """
MATCH (locn2:AUTO_JOIN_MV:`q7_mv_lineitem_orders_customer_nation2`)
MATCH (s:SUPPLIER)-[:SUPPLIER_NATION]->(n1:NATION)
WHERE locn2.l_suppkey = s.s_suppkey
  AND (
        (n1.n_name = $nation1 AND locn2.n_name = $nation2)
        OR
        (n1.n_name = $nation2 AND locn2.n_name = $nation1)
      )
  AND locn2.l_shipdate >= date('1995-01-01')
  AND locn2.l_shipdate <= date('1996-12-31')

RETURN
    n1.n_name AS supp_nation,
    locn2.n_name AS cust_nation,
    locn2.l_shipdate.year AS l_year,
    sum(locn2.l_extendedprice * (1.0 - locn2.l_discount)) AS revenue

ORDER BY
    supp_nation,
    cust_nation,
    l_year
"""

## Another model

# TEMPLATE_Q7 = """
# MATCH (locn2:AUTO_JOIN_MV:`q7_mv_lineitem_orders_customer_nation2`)
#       -[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
#       -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
#       -[:SUPPLIER_NATION]->(n1:NATION)
# WHERE (
#         (n1.n_name = $nation1 AND locn2.n_name = $nation2)
#         OR
#         (n1.n_name = $nation2 AND locn2.n_name = $nation1)
#       )
#   AND locn2.l_shipdate >= date('1995-01-01')
#   AND locn2.l_shipdate <= date('1996-12-31')
#
# RETURN
#     n1.n_name AS supp_nation,
#     locn2.n_name AS cust_nation,
#     locn2.l_shipdate.year AS l_year,
#     sum(locn2.l_extendedprice * (1.0 - locn2.l_discount)) AS revenue
#
# ORDER BY
#     supp_nation,
#     cust_nation,
#     l_year
# """
