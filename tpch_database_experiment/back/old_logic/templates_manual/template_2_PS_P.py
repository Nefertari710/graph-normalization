"""TPC-H Q2 using schema MV q2_mv_partsupp_part (PS, P)."""

TEMPLATE_Q2 = """
MATCH (psp:AUTO_JOIN_MV:`q2_mv_partsupp_part`)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE psp.p_size = $p_size
  AND psp.p_type ENDS WITH $type_suffix
  AND r.r_name = $region

WITH
    psp.p_partkey AS target_partkey,
    min(psp.ps_supplycost) AS min_supplycost

MATCH (psp:AUTO_JOIN_MV:`q2_mv_partsupp_part`)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE psp.p_partkey = target_partkey
  AND r.r_name = $region
  AND psp.ps_supplycost = min_supplycost

RETURN
    s.s_acctbal AS s_acctbal,
    s.s_name AS s_name,
    n.n_name AS n_name,
    psp.p_partkey AS p_partkey,
    psp.p_mfgr AS p_mfgr,
    s.s_address AS s_address,
    s.s_phone AS s_phone,
    s.s_comment AS s_comment

ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
"""