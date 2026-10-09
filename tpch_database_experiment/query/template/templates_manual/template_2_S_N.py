"""TPC-H Q2 using schema MV q2_mv_supplier_nation (S, N)."""

TEMPLATE_Q2 = """
MATCH (p:PART)
WHERE p.p_size = $p_size
  AND p.p_type ENDS WITH $type_suffix

MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(sn:AUTO_JOIN_MV:`q2_mv_supplier_nation`)
      -[:NATION_REGION]->(r:REGION)
WHERE r.r_name = $region

WITH p, min(ps.ps_supplycost) AS min_supplycost

MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(sn:AUTO_JOIN_MV:`q2_mv_supplier_nation`)
      -[:NATION_REGION]->(r:REGION)
WHERE r.r_name = $region
  AND ps.ps_supplycost = min_supplycost

RETURN
    sn.s_acctbal AS s_acctbal,
    sn.s_name AS s_name,
    sn.n_name AS n_name,
    p.p_partkey AS p_partkey,
    p.p_mfgr AS p_mfgr,
    sn.s_address AS s_address,
    sn.s_phone AS s_phone,
    sn.s_comment AS s_comment

ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
"""
