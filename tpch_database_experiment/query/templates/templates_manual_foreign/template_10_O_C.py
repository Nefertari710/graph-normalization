"""TPC-H Q10 using schema MV q10_mv_orders_customer (O, C)."""

TEMPLATE_Q10 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->
      (oc:AUTO_JOIN_MV:`q10_mv_orders_customer`)
      -[:CUSTOMER_NATION]->(n:NATION)
WHERE oc.o_orderdate >= date($date)
  AND oc.o_orderdate < date($date) + duration({months: 3})
  AND l.l_returnflag = 'R'

WITH
    oc.c_custkey AS c_custkey,
    oc.c_name AS c_name,
    oc.c_acctbal AS c_acctbal,
    n.n_name AS n_name,
    oc.c_address AS c_address,
    oc.c_phone AS c_phone,
    oc.c_comment AS c_comment,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue

RETURN
    c_custkey,
    c_name,
    revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment

ORDER BY
    revenue DESC
LIMIT 20
"""
