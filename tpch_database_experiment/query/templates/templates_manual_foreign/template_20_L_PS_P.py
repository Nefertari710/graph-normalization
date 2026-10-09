"""TPC-H Q20 using schema MV q20_mv_lineitem_partsupp_part (L, PS, P)."""

TEMPLATE_Q20 = """
MATCH (lpsp:AUTO_JOIN_MV:`q20_mv_lineitem_partsupp_part`)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE lpsp.p_name STARTS WITH replace($p_name, '%', '')
  AND n.n_name = $nation
  AND lpsp.l_shipdate >= date($date)
  AND lpsp.l_shipdate < date($date) + duration({years: 1})

WITH
    s.s_suppkey AS s_suppkey,
    s.s_name AS s_name,
    s.s_address AS s_address,
    lpsp.ps_partkey AS ps_partkey,
    lpsp.ps_suppkey AS ps_suppkey,
    max(lpsp.ps_availqty) AS ps_availqty,
    sum(lpsp.l_quantity) AS shipped_quantity
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
