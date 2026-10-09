"""TPC-H Q10 using schema MV q10_mv_customer_nation (C, N)."""

TEMPLATE_Q10 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(cn:AUTO_JOIN_MV:`q10_mv_customer_nation`)
WHERE o.o_orderdate >= date($date)
  AND o.o_orderdate < date($date) + duration({months: 3})
  AND l.l_returnflag = 'R'

WITH
    cn,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue

RETURN
    cn.c_custkey AS c_custkey,
    cn.c_name AS c_name,
    revenue,
    cn.c_acctbal AS c_acctbal,
    cn.n_name AS n_name,
    cn.c_address AS c_address,
    cn.c_phone AS c_phone,
    cn.c_comment AS c_comment

ORDER BY
    revenue DESC
LIMIT 20
"""
