"""TPC-H Q21 using schema MV q21_mv_supplier_nation (S, N)."""

TEMPLATE_Q21 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
MATCH (sn:AUTO_JOIN_MV:`q21_mv_supplier_nation`)
WHERE o.o_orderstatus = 'F'
  AND l.l_suppkey = sn.s_suppkey
  AND l.l_receiptdate > l.l_commitdate
  AND sn.n_name = $nation
  AND EXISTS {
      MATCH (other_l:LINEITEM)-[:LINEITEM_ORDERS]->(o)
      WHERE other_l.l_suppkey <> l.l_suppkey
  }
  AND NOT EXISTS {
      MATCH (late_l:LINEITEM)-[:LINEITEM_ORDERS]->(o)
      WHERE late_l.l_suppkey <> l.l_suppkey
        AND late_l.l_receiptdate > late_l.l_commitdate
  }

RETURN
    sn.s_name AS s_name,
    count(*) AS numwait

ORDER BY
    numwait DESC,
    s_name
"""
