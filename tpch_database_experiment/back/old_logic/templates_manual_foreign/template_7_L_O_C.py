"""TPC-H Q7 using schema MV q7_mv_lineitem_orders_customer (L, O, C)."""

TEMPLATE_Q7 = """
MATCH (loc:AUTO_JOIN_MV:`q7_mv_lineitem_orders_customer`)-[:CUSTOMER_NATION]->(n2:NATION)
MATCH (s:SUPPLIER)-[:SUPPLIER_NATION]->(n1:NATION)
WHERE loc.l_suppkey = s.s_suppkey
  AND (
        (n1.n_name = $nation1 AND n2.n_name = $nation2)
        OR
        (n1.n_name = $nation2 AND n2.n_name = $nation1)
      )
  AND loc.l_shipdate >= date('1995-01-01')
  AND loc.l_shipdate <= date('1996-12-31')

RETURN
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    loc.l_shipdate.year AS l_year,
    sum(loc.l_extendedprice * (1.0 - loc.l_discount)) AS revenue

ORDER BY
    supp_nation,
    cust_nation,
    l_year
"""


## Another model

# TEMPLATE_Q7 = """
# MATCH (loc:AUTO_JOIN_MV:`q7_mv_lineitem_orders_customer`)
#       -[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
#       -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
#       -[:SUPPLIER_NATION]->(n1:NATION)
# MATCH (loc)-[:CUSTOMER_NATION]->(n2:NATION)
# WHERE (
#         (n1.n_name = $nation1 AND n2.n_name = $nation2)
#         OR
#         (n1.n_name = $nation2 AND n2.n_name = $nation1)
#       )
#   AND loc.l_shipdate >= date('1995-01-01')
#   AND loc.l_shipdate <= date('1996-12-31')
#
# RETURN
#     n1.n_name AS supp_nation,
#     n2.n_name AS cust_nation,
#     loc.l_shipdate.year AS l_year,
#     sum(loc.l_extendedprice * (1.0 - loc.l_discount)) AS revenue
#
# ORDER BY
#     supp_nation,
#     cust_nation,
#     l_year
# """
