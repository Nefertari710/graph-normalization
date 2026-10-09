"""TPC-H Q20 using schema MV q20_mv_partsupp_supplier_part_nation (PS, S, P, N)."""

TEMPLATE_Q20 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->
      (psspn:AUTO_JOIN_MV:`q20_mv_partsupp_supplier_part_nation`)
WHERE psspn.p_name STARTS WITH replace($p_name, '%', '')
  AND psspn.n_name = $nation
  AND l.l_shipdate >= date($date)
  AND l.l_shipdate < date($date) + duration({years: 1})

WITH
    psspn.s_suppkey AS s_suppkey,
    psspn.s_name AS s_name,
    psspn.s_address AS s_address,
    psspn.ps_partkey AS ps_partkey,
    psspn.ps_suppkey AS ps_suppkey,
    max(psspn.ps_availqty) AS ps_availqty,
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
