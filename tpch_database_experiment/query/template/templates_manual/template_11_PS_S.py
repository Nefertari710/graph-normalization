"""TPC-H Q11 using schema MV q11_mv_partsupp_supplier (PS, S)."""

TEMPLATE_Q11 = """
MATCH (all_pss:AUTO_JOIN_MV:`q11_mv_partsupp_supplier`)
      -[:SUPPLIER_NATION]->(all_n:NATION)
WHERE all_n.n_name = $nation

WITH
    sum(all_pss.ps_supplycost * all_pss.ps_availqty)
        * $fraction AS threshold

MATCH (pss:AUTO_JOIN_MV:`q11_mv_partsupp_supplier`)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE n.n_name = $nation

WITH
    pss.ps_partkey AS ps_partkey,
    sum(pss.ps_supplycost * pss.ps_availqty) AS value,
    threshold
WHERE value > threshold

RETURN
    ps_partkey,
    value

ORDER BY
    value DESC
"""
