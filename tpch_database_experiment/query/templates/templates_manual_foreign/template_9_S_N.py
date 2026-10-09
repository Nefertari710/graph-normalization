"""TPC-H Q9 using schema MV q9_mv_supplier_nation (S, N)."""

TEMPLATE_Q9 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(sn:AUTO_JOIN_MV:`q9_mv_supplier_nation`)
MATCH (l)-[:LINEITEM_ORDERS]->(o:ORDERS)
WHERE p.p_name CONTAINS $color

RETURN
    sn.n_name AS nation,
    o.o_orderdate.year AS o_year,
    sum(
        l.l_extendedprice * (1.0 - l.l_discount)
        - ps.ps_supplycost * l.l_quantity
    ) AS sum_profit

ORDER BY
    nation,
    o_year DESC
"""
