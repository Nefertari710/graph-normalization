"""TPC-H Q2 using schema MV q2_mv_nation_region (N, R)."""

TEMPLATE_Q2 = """
MATCH (p:PART)
WHERE p.p_size = $p_size
  AND p.p_type ENDS WITH $type_suffix

MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(nr:AUTO_JOIN_MV:`q2_mv_nation_region`)
WHERE nr.r_name = $region

WITH p, min(ps.ps_supplycost) AS min_supplycost

MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(nr:AUTO_JOIN_MV:`q2_mv_nation_region`)
WHERE nr.r_name = $region
  AND ps.ps_supplycost = min_supplycost

RETURN
    s.s_acctbal AS s_acctbal,
    s.s_name AS s_name,
    nr.n_name AS n_name,
    p.p_partkey AS p_partkey,
    p.p_mfgr AS p_mfgr,
    s.s_address AS s_address,
    s.s_phone AS s_phone,
    s.s_comment AS s_comment

ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
"""
