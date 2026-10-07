"""TPC-H Q18 using schema MV q18_mv_lineitem_orders_customer (L, O, C)."""

TEMPLATE_Q18 = """
MATCH (loc:AUTO_JOIN_MV:`q18_mv_lineitem_orders_customer`)
WHERE loc.l_linenumber IS NOT NULL

WITH
    loc.c_name AS c_name,
    loc.c_custkey AS c_custkey,
    loc.o_orderkey AS o_orderkey,
    loc.o_orderdate AS o_orderdate,
    loc.o_totalprice AS o_totalprice,
    sum(loc.l_quantity) AS sum_qty
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
