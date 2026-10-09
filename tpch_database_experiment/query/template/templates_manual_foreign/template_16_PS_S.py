"""TPC-H Q16 using schema MV q16_mv_partsupp_supplier (PS, S)."""

TEMPLATE_Q16 = """
MATCH (pss:AUTO_JOIN_MV:`q16_mv_partsupp_supplier`)
      -[:PARTSUPP_PART]->(p:PART)
WHERE p.p_brand <> $brand
  AND NOT (p.p_type STARTS WITH $type_prefix)
  AND p.p_size IN $sizes
  AND NOT (pss.s_comment =~ '.*Customer.*Complaints.*')

RETURN
    p.p_brand AS p_brand,
    p.p_type AS p_type,
    p.p_size AS p_size,
    count(DISTINCT pss.ps_suppkey) AS supplier_cnt

ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size
"""
