"""TPC-H Q16 using schema MV q16_mv_partsupp_supplier_part (PS, S, P)."""

TEMPLATE_Q16 = """
MATCH (pssp:AUTO_JOIN_MV:`q16_mv_partsupp_supplier_part`)
WHERE pssp.p_brand <> $brand
  AND NOT (pssp.p_type STARTS WITH $type_prefix)
  AND pssp.p_size IN $sizes
  AND NOT (pssp.s_comment =~ '.*Customer.*Complaints.*')

RETURN
    pssp.p_brand AS p_brand,
    pssp.p_type AS p_type,
    pssp.p_size AS p_size,
    count(DISTINCT pssp.ps_suppkey) AS supplier_cnt

ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size
"""
