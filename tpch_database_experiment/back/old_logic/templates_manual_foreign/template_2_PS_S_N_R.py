"""TPC-H Q2 using schema MV q2_mv_partsupp_supplier_nation_region (PS, S, N, R)."""

TEMPLATE_Q2 = """
MATCH (p:PART)
WHERE p.p_size = $p_size
  AND p.p_type ENDS WITH $type_suffix

MATCH (pssnr:AUTO_JOIN_MV:`q2_mv_partsupp_supplier_nation_region`)
      -[:PARTSUPP_PART]->(p)
WHERE pssnr.r_name = $region

WITH p, min(pssnr.ps_supplycost) AS min_supplycost

MATCH (pssnr:AUTO_JOIN_MV:`q2_mv_partsupp_supplier_nation_region`)
      -[:PARTSUPP_PART]->(p)
WHERE pssnr.r_name = $region
  AND pssnr.ps_supplycost = min_supplycost

RETURN
    pssnr.s_acctbal AS s_acctbal,
    pssnr.s_name AS s_name,
    pssnr.n_name AS n_name,
    p.p_partkey AS p_partkey,
    p.p_mfgr AS p_mfgr,
    pssnr.s_address AS s_address,
    pssnr.s_phone AS s_phone,
    pssnr.s_comment AS s_comment

ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
"""
