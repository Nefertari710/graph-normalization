"""TPC-H Q8 using schema MV q8_mv_orders_customer_nation1_region (O, C, N1, R)."""


TEMPLATE_Q8 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->
      (ocn1r:AUTO_JOIN_MV:`q8_mv_orders_customer_nation1_region`)
MATCH (l)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n2:NATION)
WHERE ocn1r.r_name = $region
  AND ocn1r.o_orderdate >= date('1995-01-01')
  AND ocn1r.o_orderdate <= date('1996-12-31')
  AND p.p_type = $ptype

WITH
    ocn1r.o_orderdate.year AS o_year,
    n2.n_name AS nation,
    l.l_extendedprice * (1.0 - l.l_discount) AS volume

RETURN
    o_year,
    sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
        / sum(volume) AS mkt_share

ORDER BY
    o_year
"""
