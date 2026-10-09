"""TPC-H Q1-Q22 translated from PostgreSQL SQL to Neo4j Cypher.

The queries target the graph created by ``data/import_data_from_tbl.py``.
They deliberately follow the stored relationship directions:

    NATION   -[:NATION_REGION]-> REGION
    SUPPLIER -[:SUPPLIER_NATION]-> NATION
    CUSTOMER -[:CUSTOMER_NATION]-> NATION
    ORDERS   -[:ORDERS_CUSTOMER]-> CUSTOMER
    PARTSUPP -[:PARTSUPP_PART]-> PART
    PARTSUPP -[:PARTSUPP_SUPPLIER]-> SUPPLIER
    LINEITEM -[:LINEITEM_ORDERS]-> ORDERS
    LINEITEM -[:LINEITEM_PARTSUPP]-> PARTSUPP

All runtime values use Cypher parameters (``$name``). Date parameters are ISO
``YYYY-MM-DD`` strings. Unlike the PostgreSQL templates, ``$sizes`` is a list
of integers and ``$codes`` is a list of strings. Q13's ``$pattern`` and Q20's
``$p_name`` retain the standard TPC-H SQL-LIKE forms
``%word1%word2%`` and ``prefix%``, respectively.

This module only defines queries, matching the scope of ``baseline_psql.py``;
it does not connect to Neo4j or measure execution time.
"""

from __future__ import annotations


# TPC-H Q1: Pricing Summary Report
NEO4J_Q1 = """
MATCH (l:LINEITEM)
WHERE l.l_shipdate <= date('1998-12-01') - duration({days: $delta})
RETURN
    l.l_returnflag AS l_returnflag,
    l.l_linestatus AS l_linestatus,
    sum(l.l_quantity) AS sum_qty,
    sum(l.l_extendedprice) AS sum_base_price,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS sum_disc_price,
    sum(
        l.l_extendedprice * (1.0 - l.l_discount) * (1.0 + l.l_tax)
    ) AS sum_charge,
    avg(l.l_quantity) AS avg_qty,
    avg(l.l_extendedprice) AS avg_price,
    avg(l.l_discount) AS avg_disc,
    count(*) AS count_order
ORDER BY
    l_returnflag,
    l_linestatus
"""


# TPC-H Q2: Minimum Cost Supplier
NEO4J_Q2 = """
MATCH (p:PART)
WHERE p.p_size = $p_size
  AND p.p_type ENDS WITH $type_suffix
MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE r.r_name = $region
WITH p, min(ps.ps_supplycost) AS min_supplycost
MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE r.r_name = $region
  AND ps.ps_supplycost = min_supplycost
RETURN
    s.s_acctbal AS s_acctbal,
    s.s_name AS s_name,
    n.n_name AS n_name,
    p.p_partkey AS p_partkey,
    p.p_mfgr AS p_mfgr,
    s.s_address AS s_address,
    s.s_phone AS s_phone,
    s.s_comment AS s_comment
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
"""


# TPC-H Q3: Shipping Priority
NEO4J_Q3 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
WHERE c.c_mktsegment = $segment
  AND o.o_orderdate < date($date)
  AND l.l_shipdate > date($date)
WITH
    l.l_orderkey AS l_orderkey,
    o.o_orderdate AS o_orderdate,
    o.o_shippriority AS o_shippriority,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue
RETURN
    l_orderkey,
    revenue,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
"""


# TPC-H Q4: Order Priority Checking
NEO4J_Q4 = """
MATCH (o:ORDERS)
WHERE o.o_orderdate >= date($date)
  AND o.o_orderdate < date($date) + duration({months: 3})
  AND EXISTS {
      MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o)
      WHERE l.l_commitdate < l.l_receiptdate
  }
RETURN
    o.o_orderpriority AS o_orderpriority,
    count(*) AS order_count
ORDER BY
    o_orderpriority
"""


# TPC-H Q5: Local Supplier Volume
NEO4J_Q5 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
MATCH (l)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n)
WHERE r.r_name = $region
  AND o.o_orderdate >= date($date)
  AND o.o_orderdate < date($date) + duration({years: 1})
RETURN
    n.n_name AS n_name,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue
ORDER BY
    revenue DESC
"""


# TPC-H Q6: Forecasting Revenue Change
# TPC-H discounts have two decimal places. Compare integer percentage points
# so binary floating-point arithmetic cannot exclude either boundary value.
NEO4J_Q6 = """
WITH toInteger(round($discount * 100.0)) AS discount_percent
MATCH (l:LINEITEM)
WHERE l.l_shipdate >= date($date)
  AND l.l_shipdate < date($date) + duration({years: 1})
  AND abs(
      toInteger(round(l.l_discount * 100.0)) - discount_percent
  ) <= 1
  AND l.l_quantity < $quantity
RETURN
    CASE
        WHEN count(l) = 0 THEN null
        ELSE round(sum(l.l_extendedprice * l.l_discount), 4)
    END AS revenue
"""


# TPC-H Q7: Volume Shipping
NEO4J_Q7 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n1:NATION)
MATCH (l)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n2:NATION)
WHERE (
        (n1.n_name = $nation1 AND n2.n_name = $nation2)
        OR
        (n1.n_name = $nation2 AND n2.n_name = $nation1)
      )
  AND l.l_shipdate >= date('1995-01-01')
  AND l.l_shipdate <= date('1996-12-31')
RETURN
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    l.l_shipdate.year AS l_year,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue
ORDER BY
    supp_nation,
    cust_nation,
    l_year
"""


# TPC-H Q8: National Market Share
NEO4J_Q8 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
MATCH (ps)-[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(supplier_nation:NATION)
MATCH (l)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE r.r_name = $region
  AND o.o_orderdate >= date('1995-01-01')
  AND o.o_orderdate <= date('1996-12-31')
  AND p.p_type = $ptype
WITH
    o.o_orderdate.year AS o_year,
    supplier_nation.n_name AS nation,
    l.l_extendedprice * (1.0 - l.l_discount) AS volume
RETURN
    o_year,
    sum(CASE WHEN nation = $nation THEN volume ELSE 0.0 END)
        / sum(volume) AS mkt_share
ORDER BY
    o_year
"""


# TPC-H Q9: Product Type Profit Measure
NEO4J_Q9 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
MATCH (ps)-[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
MATCH (l)-[:LINEITEM_ORDERS]->(o:ORDERS)
WHERE p.p_name CONTAINS $color
RETURN
    n.n_name AS nation,
    o.o_orderdate.year AS o_year,
    sum(
        l.l_extendedprice * (1.0 - l.l_discount)
        - ps.ps_supplycost * l.l_quantity
    ) AS sum_profit
ORDER BY
    nation,
    o_year DESC
"""


# TPC-H Q10: Returned Item Reporting
NEO4J_Q10 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n:NATION)
WHERE o.o_orderdate >= date($date)
  AND o.o_orderdate < date($date) + duration({months: 3})
  AND l.l_returnflag = 'R'
WITH
    c,
    n,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue
RETURN
    c.c_custkey AS c_custkey,
    c.c_name AS c_name,
    revenue,
    c.c_acctbal AS c_acctbal,
    n.n_name AS n_name,
    c.c_address AS c_address,
    c.c_phone AS c_phone,
    c.c_comment AS c_comment
ORDER BY
    revenue DESC
LIMIT 20
"""


# TPC-H Q11: Important Stock Identification
NEO4J_Q11 = """
MATCH (all_ps:PARTSUPP)-[:PARTSUPP_SUPPLIER]->(:SUPPLIER)
      -[:SUPPLIER_NATION]->(all_n:NATION)
WHERE all_n.n_name = $nation
WITH sum(all_ps.ps_supplycost * all_ps.ps_availqty) * $fraction AS threshold
MATCH (ps:PARTSUPP)-[:PARTSUPP_SUPPLIER]->(:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE n.n_name = $nation
WITH
    ps.ps_partkey AS ps_partkey,
    sum(ps.ps_supplycost * ps.ps_availqty) AS value,
    threshold
WHERE value > threshold
RETURN
    ps_partkey,
    value
ORDER BY
    value DESC
"""


# TPC-H Q12: Shipping Modes and Order Priority
NEO4J_Q12 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
WHERE l.l_shipmode IN [$shipmode1, $shipmode2]
  AND l.l_commitdate < l.l_receiptdate
  AND l.l_shipdate < l.l_commitdate
  AND l.l_receiptdate >= date($date)
  AND l.l_receiptdate < date($date) + duration({years: 1})
RETURN
    l.l_shipmode AS l_shipmode,
    sum(
        CASE
            WHEN o.o_orderpriority = '1-URGENT'
              OR o.o_orderpriority = '2-HIGH'
            THEN 1
            ELSE 0
        END
    ) AS high_line_count,
    sum(
        CASE
            WHEN o.o_orderpriority <> '1-URGENT'
             AND o.o_orderpriority <> '2-HIGH'
            THEN 1
            ELSE 0
        END
    ) AS low_line_count
ORDER BY
    l_shipmode
"""


# TPC-H Q13: Customer Distribution
NEO4J_Q13 = """
MATCH (c:CUSTOMER)
WITH
    c,
    [term IN split($pattern, '%') WHERE term <> ''] AS pattern_terms
OPTIONAL MATCH (o:ORDERS)-[:ORDERS_CUSTOMER]->(c)
WHERE NOT (
    size(pattern_terms) = 2
    AND any(
        suffix IN tail(split(o.o_comment, pattern_terms[0]))
        WHERE suffix CONTAINS pattern_terms[1]
    )
)
WITH
    c.c_custkey AS c_custkey,
    count(o) AS c_count
WITH
    c_count,
    count(*) AS custdist
RETURN
    c_count,
    custdist
ORDER BY
    custdist DESC,
    c_count DESC
"""


# TPC-H Q14: Promotion Effect
NEO4J_Q14 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
WHERE l.l_shipdate >= date($date)
  AND l.l_shipdate < date($date) + duration({months: 1})
RETURN
    CASE
        WHEN count(l) = 0 THEN null
        ELSE 100.0
            * sum(
                CASE
                    WHEN p.p_type STARTS WITH 'PROMO'
                    THEN l.l_extendedprice * (1.0 - l.l_discount)
                    ELSE 0.0
                END
            )
            / sum(l.l_extendedprice * (1.0 - l.l_discount))
    END AS promo_revenue
"""


# TPC-H Q15: Top Supplier
# PostgreSQL's temporary revenue view is represented by one read-only pipeline.
NEO4J_Q15 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
WHERE l.l_shipdate >= date($date)
  AND l.l_shipdate < date($date) + duration({months: 3})
WITH
    s,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS total_revenue
WITH
    collect({
        s_suppkey: s.s_suppkey,
        s_name: s.s_name,
        s_address: s.s_address,
        s_phone: s.s_phone,
        total_revenue: total_revenue
    }) AS supplier_revenues,
    max(total_revenue) AS max_revenue
UNWIND supplier_revenues AS supplier_revenue
WITH supplier_revenue, max_revenue
WHERE supplier_revenue.total_revenue = max_revenue
RETURN
    supplier_revenue.s_suppkey AS s_suppkey,
    supplier_revenue.s_name AS s_name,
    supplier_revenue.s_address AS s_address,
    supplier_revenue.s_phone AS s_phone,
    supplier_revenue.total_revenue AS total_revenue
ORDER BY
    s_suppkey
"""


# TPC-H Q16: Parts/Supplier Relationship
NEO4J_Q16 = """
MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
WHERE p.p_brand <> $brand
  AND NOT (p.p_type STARTS WITH $type_prefix)
  AND p.p_size IN $sizes
  AND NOT (s.s_comment =~ '.*Customer.*Complaints.*')
RETURN
    p.p_brand AS p_brand,
    p.p_type AS p_type,
    p.p_size AS p_size,
    count(DISTINCT ps.ps_suppkey) AS supplier_cnt
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size
"""


# TPC-H Q17: Small-Quantity-Order Revenue
NEO4J_Q17 = """
MATCH (p:PART)
WHERE p.p_brand = $brand
  AND p.p_container = $container
MATCH (average_l:LINEITEM)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_PART]->(p)
WITH
    p,
    0.2 * avg(average_l.l_quantity) AS quantity_threshold
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_PART]->(p)
WHERE l.l_quantity < quantity_threshold
RETURN
    CASE
        WHEN count(l) = 0 THEN null
        ELSE sum(l.l_extendedprice) / 7.0
    END AS avg_yearly
"""


# TPC-H Q18: Large Volume Customer
NEO4J_Q18 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
WITH
    c,
    o,
    sum(l.l_quantity) AS sum_qty
WHERE sum_qty > $quantity
RETURN
    c.c_name AS c_name,
    c.c_custkey AS c_custkey,
    o.o_orderkey AS o_orderkey,
    o.o_orderdate AS o_orderdate,
    o.o_totalprice AS o_totalprice,
    sum_qty
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100
"""


# TPC-H Q19: Discounted Revenue
NEO4J_Q19 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
WHERE l.l_shipmode IN ['AIR', 'AIR REG']
  AND l.l_shipinstruct = 'DELIVER IN PERSON'
  AND (
      (
          p.p_brand = $brand1
          AND p.p_container IN ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']
          AND l.l_quantity >= $q1
          AND l.l_quantity <= $q1 + 10
          AND p.p_size >= 1
          AND p.p_size <= 5
      )
      OR
      (
          p.p_brand = $brand2
          AND p.p_container IN ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']
          AND l.l_quantity >= $q2
          AND l.l_quantity <= $q2 + 10
          AND p.p_size >= 1
          AND p.p_size <= 10
      )
      OR
      (
          p.p_brand = $brand3
          AND p.p_container IN ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']
          AND l.l_quantity >= $q3
          AND l.l_quantity <= $q3 + 10
          AND p.p_size >= 1
          AND p.p_size <= 15
      )
  )
RETURN
    CASE
        WHEN count(l) = 0 THEN null
        ELSE sum(l.l_extendedprice * (1.0 - l.l_discount))
    END AS revenue
"""


# TPC-H Q20: Potential Part Promotion
NEO4J_Q20 = """
MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE p.p_name STARTS WITH replace($p_name, '%', '')
  AND n.n_name = $nation
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps)
WHERE l.l_shipdate >= date($date)
  AND l.l_shipdate < date($date) + duration({years: 1})
WITH
    s,
    ps,
    sum(l.l_quantity) AS shipped_quantity
WHERE ps.ps_availqty > 0.5 * shipped_quantity
WITH DISTINCT s
RETURN
    s.s_name AS s_name,
    s.s_address AS s_address
ORDER BY
    s_name
"""


# TPC-H Q21: Suppliers Who Kept Orders Waiting
NEO4J_Q21 = """
MATCH (l1:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
MATCH (l1)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE o.o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND n.n_name = $nation
  AND EXISTS {
      MATCH (l2:LINEITEM)-[:LINEITEM_ORDERS]->(o)
      MATCH (l2)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
            -[:PARTSUPP_SUPPLIER]->(s2:SUPPLIER)
      WHERE s2.s_suppkey <> s.s_suppkey
  }
  AND NOT EXISTS {
      MATCH (l3:LINEITEM)-[:LINEITEM_ORDERS]->(o)
      MATCH (l3)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
            -[:PARTSUPP_SUPPLIER]->(s3:SUPPLIER)
      WHERE s3.s_suppkey <> s.s_suppkey
        AND l3.l_receiptdate > l3.l_commitdate
  }
RETURN
    s.s_name AS s_name,
    count(*) AS numwait
ORDER BY
    numwait DESC,
    s_name
"""


# TPC-H Q22: Global Sales Opportunity
NEO4J_Q22 = """
MATCH (average_customer:CUSTOMER)
WHERE average_customer.c_acctbal > 0.0
  AND substring(average_customer.c_phone, 0, 2) IN $codes
WITH avg(average_customer.c_acctbal) AS average_balance
MATCH (c:CUSTOMER)
WHERE substring(c.c_phone, 0, 2) IN $codes
  AND c.c_acctbal > average_balance
  AND NOT EXISTS {
      MATCH (:ORDERS)-[:ORDERS_CUSTOMER]->(c)
  }
WITH
    substring(c.c_phone, 0, 2) AS cntrycode,
    c
RETURN
    cntrycode,
    count(*) AS numcust,
    sum(c.c_acctbal) AS totacctbal
ORDER BY
    cntrycode
"""


BENCHMARK_QUERIES: dict[int, str] = {
    1: NEO4J_Q1,
    2: NEO4J_Q2,
    3: NEO4J_Q3,
    4: NEO4J_Q4,
    5: NEO4J_Q5,
    6: NEO4J_Q6,
    7: NEO4J_Q7,
    8: NEO4J_Q8,
    9: NEO4J_Q9,
    10: NEO4J_Q10,
    11: NEO4J_Q11,
    12: NEO4J_Q12,
    13: NEO4J_Q13,
    14: NEO4J_Q14,
    15: NEO4J_Q15,
    16: NEO4J_Q16,
    17: NEO4J_Q17,
    18: NEO4J_Q18,
    19: NEO4J_Q19,
    20: NEO4J_Q20,
    21: NEO4J_Q21,
    22: NEO4J_Q22,
}
