"""TPC-H Q11 using schema MV q11_mv_supplier_nation (S, N)."""

TEMPLATE_Q11 = """
MATCH (all_ps:PARTSUPP)-[:PARTSUPP_SUPPLIER]->
      (all_sn:AUTO_JOIN_MV:`q11_mv_supplier_nation`)
WHERE all_sn.n_name = $nation

WITH
    sum(all_ps.ps_supplycost * all_ps.ps_availqty)
        * $fraction AS threshold

MATCH (ps:PARTSUPP)-[:PARTSUPP_SUPPLIER]->
      (sn:AUTO_JOIN_MV:`q11_mv_supplier_nation`)
WHERE sn.n_name = $nation

WITH
    ps.ps_partkey AS ps_partkey,
    sum(ps.ps_supplycost * ps.ps_availqty) AS value,
    threshold
WHERE value > threshold

RETURN
    ps_partkey,
    value

ORDER BY
    value DESC
"""
