"""TPC-H Q2 using schema MV q2_mv_partsupp_supplier_part (PS, S, P)."""

TEMPLATE_Q2 = """
MATCH (pssp:AUTO_JOIN_MV:`q2_mv_partsupp_supplier_part`)
      -[:SUPPLIER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE pssp.p_size = $p_size
  AND pssp.p_type ENDS WITH $type_suffix
  AND r.r_name = $region

WITH
    pssp.p_partkey AS target_partkey,
    min(pssp.ps_supplycost) AS min_supplycost

MATCH (pssp:AUTO_JOIN_MV:`q2_mv_partsupp_supplier_part`)
      -[:SUPPLIER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE pssp.p_partkey = target_partkey
  AND r.r_name = $region
  AND pssp.ps_supplycost = min_supplycost

RETURN
    pssp.s_acctbal AS s_acctbal,
    pssp.s_name AS s_name,
    n.n_name AS n_name,
    pssp.p_partkey AS p_partkey,
    pssp.p_mfgr AS p_mfgr,
    pssp.s_address AS s_address,
    pssp.s_phone AS s_phone,
    pssp.s_comment AS s_comment

ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
"""
