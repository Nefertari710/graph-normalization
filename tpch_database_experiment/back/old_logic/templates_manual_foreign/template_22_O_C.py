"""TPC-H Q22 using schema MV q22_mv_orders_customer (O, C)."""

TEMPLATE_Q22 = """
MATCH (average_customer:CUSTOMER)
WHERE average_customer.c_acctbal > 0.0
  AND substring(average_customer.c_phone, 0, 2) IN $codes

WITH avg(average_customer.c_acctbal) AS average_balance

MATCH (oc:AUTO_JOIN_MV:`q22_mv_orders_customer`)
WHERE oc.o_orderkey IS NULL
  AND substring(oc.c_phone, 0, 2) IN $codes
  AND oc.c_acctbal > average_balance

WITH
    substring(oc.c_phone, 0, 2) AS cntrycode,
    oc.c_custkey AS c_custkey,
    oc.c_acctbal AS c_acctbal

RETURN
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal

ORDER BY
    cntrycode
"""
