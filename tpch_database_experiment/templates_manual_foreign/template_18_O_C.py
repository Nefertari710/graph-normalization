"""TPC-H Q18 using schema MV q18_mv_orders_customer (O, C)."""

TEMPLATE_Q18 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->
      (oc:AUTO_JOIN_MV:`q18_mv_orders_customer`)

WITH
    oc.c_name AS c_name,
    oc.c_custkey AS c_custkey,
    oc.o_orderkey AS o_orderkey,
    oc.o_orderdate AS o_orderdate,
    oc.o_totalprice AS o_totalprice,
    sum(l.l_quantity) AS sum_qty
WHERE sum_qty > $quantity

RETURN
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum_qty

ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100
"""
