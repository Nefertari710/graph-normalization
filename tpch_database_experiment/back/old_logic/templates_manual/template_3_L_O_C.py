"""TPC-H Q3 using schema MV q3_mv_lineitem_orders_customer (L, O, C)."""

TEMPLATE_Q3 = """
MATCH (loc:AUTO_JOIN_MV:`q3_mv_lineitem_orders_customer`)
WHERE loc.c_mktsegment = $segment
  AND loc.o_orderdate < date($date)
  AND loc.l_shipdate > date($date)

WITH
    loc.l_orderkey AS l_orderkey,
    loc.o_orderdate AS o_orderdate,
    loc.o_shippriority AS o_shippriority,
    sum(loc.l_extendedprice * (1.0 - loc.l_discount)) AS revenue

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
