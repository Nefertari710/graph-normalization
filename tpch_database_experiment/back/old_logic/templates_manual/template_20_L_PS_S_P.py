"""TPC-H Q20 using schema MV q20_mv_lineitem_partsupp_supplier_part (L, PS, S, P)."""

TEMPLATE_Q20 = """
MATCH (lpssp:AUTO_JOIN_MV:`q20_mv_lineitem_partsupp_supplier_part`)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE lpssp.p_name STARTS WITH replace($p_name, '%', '')
  AND n.n_name = $nation
  AND lpssp.l_shipdate >= date($date)
  AND lpssp.l_shipdate < date($date) + duration({years: 1})

WITH
    lpssp.s_suppkey AS s_suppkey,
    lpssp.s_name AS s_name,
    lpssp.s_address AS s_address,
    lpssp.ps_partkey AS ps_partkey,
    lpssp.ps_suppkey AS ps_suppkey,
    max(lpssp.ps_availqty) AS ps_availqty,
    sum(lpssp.l_quantity) AS shipped_quantity
WHERE ps_availqty > 0.5 * shipped_quantity

WITH DISTINCT
    s_suppkey,
    s_name,
    s_address

RETURN
    s_name,
    s_address

ORDER BY
    s_name
"""
