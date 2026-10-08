"""TPC-H Q20 using schema MV q20_mv_lineitem_partsupp_supplier_nation (L, PS, S, N)."""

TEMPLATE_Q20 = """
MATCH (lpssn:AUTO_JOIN_MV:`q20_mv_lineitem_partsupp_supplier_nation`)
      -[:PARTSUPP_PART]->(p:PART)
WHERE p.p_name STARTS WITH replace($p_name, '%', '')
  AND lpssn.n_name = $nation
  AND lpssn.l_shipdate >= date($date)
  AND lpssn.l_shipdate < date($date) + duration({years: 1})

WITH
    lpssn.s_suppkey AS s_suppkey,
    lpssn.s_name AS s_name,
    lpssn.s_address AS s_address,
    lpssn.ps_partkey AS ps_partkey,
    lpssn.ps_suppkey AS ps_suppkey,
    max(lpssn.ps_availqty) AS ps_availqty,
    sum(lpssn.l_quantity) AS shipped_quantity
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
