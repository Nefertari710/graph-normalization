"""TPC-H Q20 using schema MV q20_mv_supplier_nation (S, N)."""

TEMPLATE_Q20 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->
      (sn:AUTO_JOIN_MV:`q20_mv_supplier_nation`)
WHERE p.p_name STARTS WITH replace($p_name, '%', '')
  AND sn.n_name = $nation
  AND l.l_shipdate >= date($date)
  AND l.l_shipdate < date($date) + duration({years: 1})

WITH
    sn,
    ps,
    sum(l.l_quantity) AS shipped_quantity
WHERE ps.ps_availqty > 0.5 * shipped_quantity

WITH DISTINCT sn

RETURN
    sn.s_name AS s_name,
    sn.s_address AS s_address

ORDER BY
    s_name
"""
