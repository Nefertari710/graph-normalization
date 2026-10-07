"""TPC-H Q9 using schema MV q9_mv_lineitem_orders_partsupp (L, O, PS)."""

TEMPLATE_Q9 = """
MATCH (lops:AUTO_JOIN_MV:`q9_mv_lineitem_orders_partsupp`)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (lops)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE p.p_name CONTAINS $color

RETURN
    n.n_name AS nation,
    lops.o_orderdate.year AS o_year,
    sum(
        lops.l_extendedprice * (1.0 - lops.l_discount)
        - lops.ps_supplycost * lops.l_quantity
    ) AS sum_profit

ORDER BY
    nation,
    o_year DESC
"""
