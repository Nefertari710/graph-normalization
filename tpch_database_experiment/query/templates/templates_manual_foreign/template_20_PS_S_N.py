"""TPC-H Q20 using schema MV q20_mv_partsupp_supplier_nation (PS, S, N)."""

TEMPLATE_Q20 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->
      (pssn:AUTO_JOIN_MV:`q20_mv_partsupp_supplier_nation`)
      -[:PARTSUPP_PART]->(p:PART)
WHERE p.p_name STARTS WITH replace($p_name, '%', '')
  AND pssn.n_name = $nation
  AND l.l_shipdate >= date($date)
  AND l.l_shipdate < date($date) + duration({years: 1})

WITH
    pssn.s_suppkey AS s_suppkey,
    pssn.s_name AS s_name,
    pssn.s_address AS s_address,
    pssn.ps_partkey AS ps_partkey,
    pssn.ps_suppkey AS ps_suppkey,
    max(pssn.ps_availqty) AS ps_availqty,
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
