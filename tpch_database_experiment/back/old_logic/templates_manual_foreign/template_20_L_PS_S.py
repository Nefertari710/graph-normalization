"""TPC-H Q20 using schema MV q20_mv_lineitem_partsupp_supplier (L, PS, S)."""

TEMPLATE_Q20 = """
MATCH (lpss:AUTO_JOIN_MV:`q20_mv_lineitem_partsupp_supplier`)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (lpss)-[:SUPPLIER_NATION]->(n:NATION)
WHERE p.p_name STARTS WITH replace($p_name, '%', '')
  AND n.n_name = $nation
  AND lpss.l_shipdate >= date($date)
  AND lpss.l_shipdate < date($date) + duration({years: 1})

WITH
    lpss.s_suppkey AS s_suppkey,
    lpss.s_name AS s_name,
    lpss.s_address AS s_address,
    lpss.ps_partkey AS ps_partkey,
    lpss.ps_suppkey AS ps_suppkey,
    max(lpss.ps_availqty) AS ps_availqty,
    sum(lpss.l_quantity) AS shipped_quantity
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
