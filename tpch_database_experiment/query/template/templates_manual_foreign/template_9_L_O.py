"""TPC-H Q9 using schema MV q9_mv_lineitem_orders (L, O)."""

TEMPLATE_Q9 = """
MATCH (lo:AUTO_JOIN_MV:`q9_mv_lineitem_orders`)
      -[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE p.p_name CONTAINS $color

RETURN
    n.n_name AS nation,
    lo.o_orderdate.year AS o_year,
    sum(
        lo.l_extendedprice * (1.0 - lo.l_discount)
        - ps.ps_supplycost * lo.l_quantity
    ) AS sum_profit

ORDER BY
    nation,
    o_year DESC
"""
