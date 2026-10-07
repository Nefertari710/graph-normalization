"""TPC-H Q5 using the SNR primary plus its Customer->NR closure via keys."""


TEMPLATE_Q5 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)

MATCH (snr:AUTO_JOIN_MV:`q5_mv_supplier_nation_region`
          {_auto_join_component: 'primary'})
MATCH (nr:AUTO_JOIN_MV:`q5_mv_supplier_nation_region`
          {_auto_join_component: 'nation_region_closure'})

WHERE l.l_suppkey = snr.s_suppkey
  AND c.c_nationkey = nr.n_nationkey
  AND nr.n_nationkey = snr.n_nationkey
  AND nr.r_name = $region
  AND o.o_orderdate >= date($date)
  AND o.o_orderdate < date($date) + duration({years: 1})

RETURN
    nr.n_name AS n_name,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue

ORDER BY
    revenue DESC
"""


## Legacy pre-closure model retained only for comparison; do not execute.


# TEMPLATE_Q5 = """
# MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
#       -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
#       -[:CUSTOMER_NATION]->(customer_n:NATION)
# MATCH (l)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
#       -[:PARTSUPP_SUPPLIER]->
#       (snr:AUTO_JOIN_MV:`q5_mv_supplier_nation_region`)
# WHERE customer_n.n_nationkey = snr.n_nationkey
#   AND snr.r_name = $region
#   AND o.o_orderdate >= date($date)
#   AND o.o_orderdate < date($date) + duration({years: 1})
#
# RETURN
#     snr.n_name AS n_name,
#     sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue
#
# ORDER BY
#     revenue DESC
# """
