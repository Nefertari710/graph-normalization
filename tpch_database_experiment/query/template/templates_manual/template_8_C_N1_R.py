"""TPC-H Q8 using schema MV q8_mv_customer_nation1_region (C, N1, R)."""


TEMPLATE_Q8 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->
      (cn1r:AUTO_JOIN_MV:`q8_mv_customer_nation1_region`
          {_auto_join_component: 'primary'})
MATCH (l)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->
      (n2r:AUTO_JOIN_MV:`q8_mv_customer_nation1_region`
          {_auto_join_component: 'supplier_nation_region_closure'})
WHERE cn1r.r_name = $region
  AND o.o_orderdate >= date('1995-01-01')
  AND o.o_orderdate <= date('1996-12-31')
  AND p.p_type = $ptype

WITH
    o.o_orderdate.year AS o_year,
    n2r.n_name AS nation,
    l.l_extendedprice * (1.0 - l.l_discount) AS volume

RETURN
    o_year,
    sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
        / sum(volume) AS mkt_share

ORDER BY
    o_year
"""
