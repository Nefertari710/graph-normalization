"""TPC-H Q2 using schema MV q2_mv_partsupp_supplier_nation (PS, S, N)."""

TEMPLATE_Q2 = """
MATCH (p:PART)
WHERE p.p_size = $p_size
  AND p.p_type ENDS WITH $type_suffix

MATCH (pssn:AUTO_JOIN_MV:`q2_mv_partsupp_supplier_nation`)
      -[:PARTSUPP_PART]->(p)
MATCH (pssn)-[:NATION_REGION]->(r:REGION)
WHERE r.r_name = $region

WITH p, min(pssn.ps_supplycost) AS min_supplycost

MATCH (pssn:AUTO_JOIN_MV:`q2_mv_partsupp_supplier_nation`)
      -[:PARTSUPP_PART]->(p)
MATCH (pssn)-[:NATION_REGION]->(r:REGION)
WHERE r.r_name = $region
  AND pssn.ps_supplycost = min_supplycost

RETURN
    pssn.s_acctbal AS s_acctbal,
    pssn.s_name AS s_name,
    pssn.n_name AS n_name,
    p.p_partkey AS p_partkey,
    p.p_mfgr AS p_mfgr,
    pssn.s_address AS s_address,
    pssn.s_phone AS s_phone,
    pssn.s_comment AS s_comment

ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
"""
