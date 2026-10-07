"""TPC-H Q8 using schema MV q8_mv_orders_customer (O, C)."""


TEMPLATE_Q8 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->
      (oc:AUTO_JOIN_MV:`q8_mv_orders_customer`)
      -[:CUSTOMER_NATION]->(n1:NATION)
      -[:NATION_REGION]->(r:REGION)
MATCH (l)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n2:NATION)
WHERE r.r_name = $region
  AND oc.o_orderdate >= date('1995-01-01')
  AND oc.o_orderdate <= date('1996-12-31')
  AND p.p_type = $ptype

WITH
    oc.o_orderdate.year AS o_year,
    n2.n_name AS nation,
    l.l_extendedprice * (1.0 - l.l_discount) AS volume

RETURN
    o_year,
    sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
        / sum(volume) AS mkt_share

ORDER BY
    o_year
"""
