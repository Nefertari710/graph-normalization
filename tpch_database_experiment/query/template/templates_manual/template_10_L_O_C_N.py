"""TPC-H Q10 using schema MV q10_mv_lineitem_orders_customer_nation (L, O, C, N)."""

TEMPLATE_Q10 = """
MATCH (locn:AUTO_JOIN_MV:`q10_mv_lineitem_orders_customer_nation`)
WHERE locn.o_orderdate >= date($date)
  AND locn.o_orderdate < date($date) + duration({months: 3})
  AND locn.l_returnflag = 'R'

WITH
    locn.c_custkey AS c_custkey,
    locn.c_name AS c_name,
    locn.c_acctbal AS c_acctbal,
    locn.n_name AS n_name,
    locn.c_address AS c_address,
    locn.c_phone AS c_phone,
    locn.c_comment AS c_comment,
    sum(locn.l_extendedprice * (1.0 - locn.l_discount)) AS revenue

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
