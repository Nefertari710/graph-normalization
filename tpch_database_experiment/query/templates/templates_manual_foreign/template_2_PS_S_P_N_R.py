"""TPC-H Q2 using schema MV q2_mv_partsupp_supplier_part_nation_region (PS, S, P, N, R)."""

TEMPLATE_Q2 = """
MATCH (psspnr:AUTO_JOIN_MV:`q2_mv_partsupp_supplier_part_nation_region`
          {_auto_join_component: 'primary'})
WHERE psspnr.p_size = $p_size
  AND psspnr.p_type ENDS WITH $type_suffix
  AND psspnr.r_name = $region

WITH
    psspnr.p_partkey AS target_partkey,
    min(psspnr.ps_supplycost) AS min_supplycost

MATCH (psspnr:AUTO_JOIN_MV:`q2_mv_partsupp_supplier_part_nation_region`
          {_auto_join_component: 'primary'})
  WHERE psspnr.p_partkey = target_partkey
  AND psspnr.r_name = $region
  AND psspnr.ps_supplycost = min_supplycost

RETURN
    psspnr.s_acctbal AS s_acctbal,
    psspnr.s_name AS s_name,
    psspnr.n_name AS n_name,
    psspnr.p_partkey AS p_partkey,
    psspnr.p_mfgr AS p_mfgr,
    psspnr.s_address AS s_address,
    psspnr.s_phone AS s_phone,
    psspnr.s_comment AS s_comment

ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
"""
