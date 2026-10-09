"""TPC-H Q9 using schema MV q9_mv_lineitem_partsupp (L, PS)."""

TEMPLATE_Q9 = """
MATCH (lps:AUTO_JOIN_MV:`q9_mv_lineitem_partsupp`)
      -[:LINEITEM_ORDERS]->(o:ORDERS)
MATCH (lps)-[:PARTSUPP_PART]->(p:PART)
MATCH (lps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE p.p_name CONTAINS $color

RETURN
    n.n_name AS nation,
    o.o_orderdate.year AS o_year,
    sum(
        lps.l_extendedprice * (1.0 - lps.l_discount)
        - lps.ps_supplycost * lps.l_quantity
    ) AS sum_profit

ORDER BY
    nation,
    o_year DESC
"""
