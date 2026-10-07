"""TPC-H Q11 using schema MV q11_mv_partsupp_supplier_nation (PS, S, N)."""

TEMPLATE_Q11 = """
MATCH (all_pssn:AUTO_JOIN_MV:`q11_mv_partsupp_supplier_nation`)
WHERE all_pssn.n_name = $nation

WITH
    sum(all_pssn.ps_supplycost * all_pssn.ps_availqty)
        * $fraction AS threshold

MATCH (pssn:AUTO_JOIN_MV:`q11_mv_partsupp_supplier_nation`)
WHERE pssn.n_name = $nation

WITH
    pssn.ps_partkey AS ps_partkey,
    sum(pssn.ps_supplycost * pssn.ps_availqty) AS value,
    threshold
WHERE value > threshold

RETURN
    ps_partkey,
    value

ORDER BY
    value DESC
"""
