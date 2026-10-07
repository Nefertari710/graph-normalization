"""TPC-H Q10 using schema MV q10_mv_orders_customer_nation (O, C, N)."""

TEMPLATE_Q10 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->
      (ocn:AUTO_JOIN_MV:`q10_mv_orders_customer_nation`)
WHERE ocn.o_orderdate >= date($date)
  AND ocn.o_orderdate < date($date) + duration({months: 3})
  AND l.l_returnflag = 'R'

WITH
    ocn.c_custkey AS c_custkey,
    ocn.c_name AS c_name,
    ocn.c_acctbal AS c_acctbal,
    ocn.n_name AS n_name,
    ocn.c_address AS c_address,
    ocn.c_phone AS c_phone,
    ocn.c_comment AS c_comment,
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
