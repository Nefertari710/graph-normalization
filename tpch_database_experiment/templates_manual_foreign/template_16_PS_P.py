"""TPC-H Q16 using schema MV q16_mv_partsupp_part (PS, P)."""

TEMPLATE_Q16 = """
MATCH (psp:AUTO_JOIN_MV:`q16_mv_partsupp_part`)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
WHERE psp.p_brand <> $brand
  AND NOT (psp.p_type STARTS WITH $type_prefix)
  AND psp.p_size IN $sizes
  AND NOT (s.s_comment =~ '.*Customer.*Complaints.*')

RETURN
    psp.p_brand AS p_brand,
    psp.p_type AS p_type,
    psp.p_size AS p_size,
    count(DISTINCT psp.ps_suppkey) AS supplier_cnt

ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size
"""
