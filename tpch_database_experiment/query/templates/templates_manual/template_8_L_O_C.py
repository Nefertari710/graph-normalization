"""TPC-H Q8 using schema MV q8_mv_lineitem_orders_customer (L, O, C)."""


TEMPLATE_Q8 = """
MATCH (loc:AUTO_JOIN_MV:`q8_mv_lineitem_orders_customer`)
      -[:CUSTOMER_NATION]->(n1:NATION)
      -[:NATION_REGION]->(r:REGION)
MATCH (loc)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n2:NATION)
WHERE r.r_name = $region
  AND loc.o_orderdate >= date('1995-01-01')
  AND loc.o_orderdate <= date('1996-12-31')
  AND p.p_type = $ptype

WITH
    loc.o_orderdate.year AS o_year,
    n2.n_name AS nation,
    loc.l_extendedprice * (1.0 - loc.l_discount) AS volume

RETURN
    o_year,
    sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
        / sum(volume) AS mkt_share

ORDER BY
    o_year
"""
