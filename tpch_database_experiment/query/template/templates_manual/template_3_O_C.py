"""TPC-H Q3 using schema MV q3_mv_orders_customer (O, C)."""

TEMPLATE_Q3 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->
      (oc:AUTO_JOIN_MV:`q3_mv_orders_customer`)
WHERE oc.c_mktsegment = $segment
  AND oc.o_orderdate < date($date)
  AND l.l_shipdate > date($date)

WITH
    l.l_orderkey AS l_orderkey,
    oc.o_orderdate AS o_orderdate,
    oc.o_shippriority AS o_shippriority,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue

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
