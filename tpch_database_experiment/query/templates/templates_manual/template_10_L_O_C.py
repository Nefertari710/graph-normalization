"""TPC-H Q10 using schema MV q10_mv_lineitem_orders_customer (L, O, C)."""

TEMPLATE_Q10 = """
MATCH (loc:AUTO_JOIN_MV:`q10_mv_lineitem_orders_customer`)
      -[:CUSTOMER_NATION]->(n:NATION)
WHERE loc.o_orderdate >= date($date)
  AND loc.o_orderdate < date($date) + duration({months: 3})
  AND loc.l_returnflag = 'R'

WITH
    loc.c_custkey AS c_custkey,
    loc.c_name AS c_name,
    loc.c_acctbal AS c_acctbal,
    n.n_name AS n_name,
    loc.c_address AS c_address,
    loc.c_phone AS c_phone,
    loc.c_comment AS c_comment,
    sum(loc.l_extendedprice * (1.0 - loc.l_discount)) AS revenue

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
