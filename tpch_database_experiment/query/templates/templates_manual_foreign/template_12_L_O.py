"""TPC-H Q12 using schema MV q12_mv_lineitem_orders (L, O)."""

TEMPLATE_Q12 = """
MATCH (lo:AUTO_JOIN_MV:`q12_mv_lineitem_orders`)
WHERE lo.l_shipmode IN [$shipmode1, $shipmode2]
  AND lo.l_commitdate < lo.l_receiptdate
  AND lo.l_shipdate < lo.l_commitdate
  AND lo.l_receiptdate >= date($date)
  AND lo.l_receiptdate < date($date) + duration({years: 1})

RETURN
    lo.l_shipmode AS l_shipmode,
    sum(
        CASE
            WHEN lo.o_orderpriority = '1-URGENT'
              OR lo.o_orderpriority = '2-HIGH'
            THEN 1
            ELSE 0
        END
    ) AS high_line_count,
    sum(
        CASE
            WHEN lo.o_orderpriority <> '1-URGENT'
             AND lo.o_orderpriority <> '2-HIGH'
            THEN 1
            ELSE 0
        END
    ) AS low_line_count

ORDER BY
    l_shipmode
"""
