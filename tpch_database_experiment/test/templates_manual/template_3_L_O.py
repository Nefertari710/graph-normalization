"""TPC-H Q3 using schema MV q3_mv_lineitem_orders (L, O)."""

TEMPLATE_Q3 = """
MATCH (lo:AUTO_JOIN_MV:`q3_mv_lineitem_orders`)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
WHERE c.c_mktsegment = $segment
  AND lo.o_orderdate < date($date)
  AND lo.l_shipdate > date($date)

WITH
    lo.l_orderkey AS l_orderkey,
    lo.o_orderdate AS o_orderdate,
    lo.o_shippriority AS o_shippriority,
    sum(lo.l_extendedprice * (1.0 - lo.l_discount)) AS revenue

RETURN
    l_orderkey,
    revenue,
    o_orderdate,
    o_shippriority

ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
"""
