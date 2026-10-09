"""TPC-H Q10 using schema MV q10_mv_lineitem_orders (L, O)."""

TEMPLATE_Q10 = """
MATCH (lo:AUTO_JOIN_MV:`q10_mv_lineitem_orders`)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n:NATION)
WHERE lo.o_orderdate >= date($date)
  AND lo.o_orderdate < date($date) + duration({months: 3})
  AND lo.l_returnflag = 'R'

WITH
    c,
    n,
    sum(lo.l_extendedprice * (1.0 - lo.l_discount)) AS revenue

RETURN
    c.c_custkey AS c_custkey,
    c.c_name AS c_name,
    revenue,
    c.c_acctbal AS c_acctbal,
    n.n_name AS n_name,
    c.c_address AS c_address,
    c.c_phone AS c_phone,
    c.c_comment AS c_comment

ORDER BY
    revenue DESC
LIMIT 20
"""
