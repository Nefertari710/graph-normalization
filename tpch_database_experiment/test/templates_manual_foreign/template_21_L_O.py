"""TPC-H Q21 using schema MV q21_mv_lineitem_orders (L, O)."""

TEMPLATE_Q21 = """
MATCH (lo:AUTO_JOIN_MV:`q21_mv_lineitem_orders`)      
MATCH (s:SUPPLIER)-[:SUPPLIER_NATION]->(n:NATION)
WHERE lo.o_orderstatus = 'F'
  AND lo.l_suppkey = s.s_suppkey
WITH
    lo.o_orderkey AS o_orderkey,
    collect({
        s_suppkey: lo.l_suppkey,
        s_name: s.s_name,
        n_name: n.n_name,
        is_late: lo.l_receiptdate > lo.l_commitdate
    }) AS order_lines

UNWIND order_lines AS current

WITH
    current,
    order_lines
WHERE current.is_late
  AND current.n_name = $nation
  AND size([
      other IN order_lines
      WHERE other.s_suppkey <> current.s_suppkey
  ]) > 0
  AND size([
      late IN order_lines
      WHERE late.s_suppkey <> current.s_suppkey
        AND late.is_late
  ]) = 0

RETURN
    current.s_name AS s_name,
    count(*) AS numwait

ORDER BY
    numwait DESC,
    s_name
"""

