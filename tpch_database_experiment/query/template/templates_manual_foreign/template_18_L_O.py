"""TPC-H Q18 using schema MV q18_mv_lineitem_orders (L, O)."""

TEMPLATE_Q18 = """
MATCH (lo:AUTO_JOIN_MV:`q18_mv_lineitem_orders`)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)

WITH
    c.c_name AS c_name,
    c.c_custkey AS c_custkey,
    lo.o_orderkey AS o_orderkey,
    lo.o_orderdate AS o_orderdate,
    lo.o_totalprice AS o_totalprice,
    sum(lo.l_quantity) AS sum_qty
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
