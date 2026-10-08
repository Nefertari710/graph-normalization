"""TPC-H Q5 using the SNR primary plus its Customer->NR fold closure."""

TEMPLATE_Q5 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->
      (nr:AUTO_JOIN_MV:`q5_mv_supplier_nation_region`
          {_auto_join_component: 'nation_region_closure'})
MATCH (l)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->
      (snr:AUTO_JOIN_MV:`q5_mv_supplier_nation_region`
          {_auto_join_component: 'primary'})
WHERE nr.n_nationkey = snr.n_nationkey
  AND nr.r_name = $region
  AND o.o_orderdate >= date($date)
  AND o.o_orderdate < date($date) + duration({years: 1})

RETURN
    nr.n_name AS n_name,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue

ORDER BY
    revenue DESC
"""
