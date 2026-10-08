"""TPC-H Q20 using schema MV q20_mv_partsupp_supplier (PS, S)."""

TEMPLATE_Q20 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->
      (pss:AUTO_JOIN_MV:`q20_mv_partsupp_supplier`)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (pss)-[:SUPPLIER_NATION]->(n:NATION)
WHERE p.p_name STARTS WITH replace($p_name, '%', '')
  AND n.n_name = $nation
  AND l.l_shipdate >= date($date)
  AND l.l_shipdate < date($date) + duration({years: 1})

WITH
    pss.s_suppkey AS s_suppkey,
    pss.s_name AS s_name,
    pss.s_address AS s_address,
    pss.ps_partkey AS ps_partkey,
    pss.ps_suppkey AS ps_suppkey,
    max(pss.ps_availqty) AS ps_availqty,
    sum(l.l_quantity) AS shipped_quantity
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
