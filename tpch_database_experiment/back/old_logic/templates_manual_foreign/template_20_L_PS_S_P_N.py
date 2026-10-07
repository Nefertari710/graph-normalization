"""TPC-H Q20 using schema MV q20_mv_lineitem_partsupp_supplier_part_nation (L, PS, S, P, N)."""

TEMPLATE_Q20 = """
MATCH (lpsspn:AUTO_JOIN_MV:`q20_mv_lineitem_partsupp_supplier_part_nation`)
WHERE lpsspn.p_name STARTS WITH replace($p_name, '%', '')
  AND lpsspn.n_name = $nation
  AND lpsspn.l_shipdate >= date($date)
  AND lpsspn.l_shipdate < date($date) + duration({years: 1})

WITH
    lpsspn.s_suppkey AS s_suppkey,
    lpsspn.s_name AS s_name,
    lpsspn.s_address AS s_address,
    lpsspn.ps_partkey AS ps_partkey,
    lpsspn.ps_suppkey AS ps_suppkey,
    max(lpsspn.ps_availqty) AS ps_availqty,
    sum(lpsspn.l_quantity) AS shipped_quantity
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
