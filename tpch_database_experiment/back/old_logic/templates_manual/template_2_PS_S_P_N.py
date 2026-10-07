"""TPC-H Q2 using schema MV q2_mv_partsupp_supplier_part_nation (PS, S, P, N)."""

TEMPLATE_Q2 = """
MATCH (psspn:AUTO_JOIN_MV:`q2_mv_partsupp_supplier_part_nation`)
      -[:NATION_REGION]->(r:REGION)
WHERE psspn.p_size = $p_size
  AND psspn.p_type ENDS WITH $type_suffix
  AND r.r_name = $region

WITH
    psspn.p_partkey AS target_partkey,
    min(psspn.ps_supplycost) AS min_supplycost

MATCH (psspn:AUTO_JOIN_MV:`q2_mv_partsupp_supplier_part_nation`)
      -[:NATION_REGION]->(r:REGION)
WHERE psspn.p_partkey = target_partkey
  AND r.r_name = $region
  AND psspn.ps_supplycost = min_supplycost

RETURN
    psspn.s_acctbal AS s_acctbal,
    psspn.s_name AS s_name,
    psspn.n_name AS n_name,
    psspn.p_partkey AS p_partkey,
    psspn.p_mfgr AS p_mfgr,
    psspn.s_address AS s_address,
    psspn.s_phone AS s_phone,
    psspn.s_comment AS s_comment

ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
"""
