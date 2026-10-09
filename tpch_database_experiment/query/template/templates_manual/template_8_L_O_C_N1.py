"""TPC-H Q8 using schema MV q8_mv_lineitem_orders_customer_nation1 (L, O, C, N1)."""

TEMPLATE_Q8 = """
MATCH (locn1:AUTO_JOIN_MV:`q8_mv_lineitem_orders_customer_nation1`)
      -[:NATION_REGION]->(r:REGION)
MATCH (locn1)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n2:NATION)
WHERE r.r_name = $region
  AND locn1.o_orderdate >= date('1995-01-01')
  AND locn1.o_orderdate <= date('1996-12-31')
  AND p.p_type = $ptype

WITH
    locn1.o_orderdate.year AS o_year,
    n2.n_name AS nation,
    locn1.l_extendedprice * (1.0 - locn1.l_discount) AS volume

RETURN
    o_year,
    sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
        / sum(volume) AS mkt_share

ORDER BY
    o_year
"""