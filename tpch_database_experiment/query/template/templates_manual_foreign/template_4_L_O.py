"""TPC-H Q4 using schema MV q4_mv_lineitem_orders (L, O)."""

TEMPLATE_Q4 = """
MATCH (lo:AUTO_JOIN_MV:`q4_mv_lineitem_orders`)
WHERE lo.o_orderdate >= date($date)
  AND lo.o_orderdate < date($date) + duration({months: 3})
  AND lo.l_commitdate < lo.l_receiptdate

WITH DISTINCT
    lo.o_orderkey AS o_orderkey,
    lo.o_orderpriority AS o_orderpriority

RETURN
    o_orderpriority,
    count(*) AS order_count

ORDER BY
    o_orderpriority
"""
