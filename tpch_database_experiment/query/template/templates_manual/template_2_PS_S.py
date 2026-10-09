"""TPC-H Q2 using schema MV q2_mv_partsupp_supplier (PS, S)."""

TEMPLATE_Q2 = """
MATCH (pss:AUTO_JOIN_MV:`q2_mv_partsupp_supplier`)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (pss)-[:SUPPLIER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE p.p_size = $p_size
  AND p.p_type ENDS WITH $type_suffix
  AND r.r_name = $region

WITH p, min(pss.ps_supplycost) AS min_supplycost

MATCH (pss:AUTO_JOIN_MV:`q2_mv_partsupp_supplier`)
      -[:PARTSUPP_PART]->(p)
MATCH (pss)-[:SUPPLIER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE r.r_name = $region
  AND pss.ps_supplycost = min_supplycost

RETURN
    pss.s_acctbal AS s_acctbal,
    pss.s_name AS s_name,
    n.n_name AS n_name,
    p.p_partkey AS p_partkey,
    p.p_mfgr AS p_mfgr,
    pss.s_address AS s_address,
    pss.s_phone AS s_phone,
    pss.s_comment AS s_comment

ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
"""
