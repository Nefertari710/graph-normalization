

DELTA = 90

Q1 = """
MATCH (l:LINEITEM)
WHERE l.l_shipdate <= date('1998-12-01') - duration({days: 90})
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




SIZE = 15
TYPE = "BRASS"
REGION = "EUROPE"

Q2 = """
MATCH (p:PART)
WHERE p.p_size = 15
  AND p.p_type ENDS WITH 'BRASS'
MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE r.r_name = 'EUROPE'
WITH p, min(ps.ps_supplycost) AS min_supplycost
MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE r.r_name = 'EUROPE'
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



SEGMENT = "BUILDING"
DATE = "1995-03-15"

Q3 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
WHERE c.c_mktsegment = 'BUILDING'
  AND o.o_orderdate < date('1995-03-15')
  AND l.l_shipdate > date('1995-03-15')
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



DATE = "1993-07-01"

Q4 = """
MATCH (o:ORDERS)
WHERE o.o_orderdate >= date('1993-07-01')
  AND o.o_orderdate < date('1993-07-01') + duration({months: 3})
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


REGION = "ASIA"
DATE = "1994-01-01"

Q5 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n:NATION)
      -[:NATION_REGION]->(r:REGION)
MATCH (l)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n)
WHERE r.r_name = 'ASIA'
  AND o.o_orderdate >= date('1994-01-01')
  AND o.o_orderdate < date('1994-01-01') + duration({years: 1})
RETURN
    n.n_name AS n_name,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue
ORDER BY
    revenue DESC
"""




DATE = "1994-01-01"
DISCOUNT = 0.06
QUANTITY = 24

# It will be affected by accuracy.
Q6 = """
WITH toInteger(round(0.06 * 100.0)) AS discount_percent
MATCH (l:LINEITEM)
WHERE l.l_shipdate >= date('1994-01-01')
  AND l.l_shipdate < date('1994-01-01') + duration({years: 1})
  AND abs(
      toInteger(round(l.l_discount * 100.0)) - discount_percent
  ) <= 1
  AND l.l_quantity < 24
RETURN
    CASE
        WHEN count(l) = 0 THEN null
        ELSE round(sum(l.l_extendedprice * l.l_discount), 4)
    END AS revenue
"""



NATION1 = "FRANCE"
NATION2 = "GERMANY"

Q7 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n1:NATION)
MATCH (l)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n2:NATION)
WHERE (
        (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
        OR
        (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
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



NATION = "BRAZIL"
REGION = "AMERICA"
TYPE = "ECONOMY ANODIZED STEEL"

Q8 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
MATCH (ps)-[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(supplier_nation:NATION)
MATCH (l)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(:NATION)
      -[:NATION_REGION]->(r:REGION)
WHERE r.r_name = 'AMERICA'
  AND o.o_orderdate >= date('1995-01-01')
  AND o.o_orderdate <= date('1996-12-31')
  AND p.p_type = 'ECONOMY ANODIZED STEEL'
WITH
    o.o_orderdate.year AS o_year,
    supplier_nation.n_name AS nation,
    l.l_extendedprice * (1.0 - l.l_discount) AS volume
RETURN
    o_year,
    sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0.0 END)
        / sum(volume) AS mkt_share
ORDER BY
    o_year
"""


COLOR = "green"

# Original docs results have issue. True results is "ALGERIA|1998|27136900.1803" by psql
Q9 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)
MATCH (ps)-[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
MATCH (l)-[:LINEITEM_ORDERS]->(o:ORDERS)
WHERE p.p_name CONTAINS 'green'
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




DATE = "1993-10-01"

Q10 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
      -[:CUSTOMER_NATION]->(n:NATION)
WHERE o.o_orderdate >= date('1993-10-01')
  AND o.o_orderdate < date('1993-10-01') + duration({months: 3})
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



NATION = "GERMANY"
FRACTION = 0.0001

Q11 = """
MATCH (all_ps:PARTSUPP)-[:PARTSUPP_SUPPLIER]->(:SUPPLIER)
      -[:SUPPLIER_NATION]->(all_n:NATION)
WHERE all_n.n_name = 'GERMANY'
WITH sum(all_ps.ps_supplycost * all_ps.ps_availqty) * 0.0001 AS threshold
MATCH (ps:PARTSUPP)-[:PARTSUPP_SUPPLIER]->(:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE n.n_name = 'GERMANY'
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




SHIPMODE1 = "MAIL"
SHIPMODE2 = "SHIP"
DATE = "1994-01-01"

Q12 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
WHERE l.l_shipmode IN ['MAIL', 'SHIP']
  AND l.l_commitdate < l.l_receiptdate
  AND l.l_shipdate < l.l_commitdate
  AND l.l_receiptdate >= date('1994-01-01')
  AND l.l_receiptdate < date('1994-01-01') + duration({years: 1})
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



WORD1 = "special"
WORD2 = "requests"

# Original docs results have issue. True results is "0|50005" by psql
Q13 = """
MATCH (c:CUSTOMER)
WITH
    c,
    [term IN split('%special%requests%', '%') WHERE term <> ''] AS pattern_terms
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



DATE = "1995-09-01"

Q14 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
WHERE l.l_shipdate >= date('1995-09-01')
  AND l.l_shipdate < date('1995-09-01') + duration({months: 1})
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





DATE = "1996-01-01"


Q15 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
WHERE l.l_shipdate >= date('1996-01-01')
  AND l.l_shipdate < date('1996-01-01') + duration({months: 3})
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




BRAND = "Brand#45"
TYPE = "MEDIUM POLISHED"
SIZE1 = 49
SIZE2 = 14
SIZE3 = 23
SIZE4 = 45
SIZE5 = 19
SIZE6 = 3
SIZE7 = 36
SIZE8 = 9

Q16 = """
MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
WHERE p.p_brand <> 'Brand#45'
  AND NOT (p.p_type STARTS WITH 'MEDIUM POLISHED')
  AND p.p_size IN [49, 14, 23, 45, 19, 3, 36, 9]
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



BRAND = "Brand#23"
CONTAINER = "MED BOX"

Q17 = """
MATCH (p:PART)
WHERE p.p_brand = 'Brand#23'
  AND p.p_container = 'MED BOX'
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



QUANTITY = 300

Q18 = """
MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
      -[:ORDERS_CUSTOMER]->(c:CUSTOMER)
WITH
    c,
    o,
    sum(l.l_quantity) AS sum_qty
WHERE sum_qty > 300
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


QUANTITY1 = 1
QUANTITY2 = 10
QUANTITY3 = 20
BRAND1 = "Brand#12"
BRAND2 = "Brand#23"
BRAND3 = "Brand#34"

Q19 = """
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_PART]->(p:PART)
WHERE l.l_shipmode IN ['AIR', 'AIR REG']
  AND l.l_shipinstruct = 'DELIVER IN PERSON'
  AND (
      (
          p.p_brand = 'Brand#12'
          AND p.p_container IN ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']
          AND l.l_quantity >= 1
          AND l.l_quantity <= 1 + 10
          AND p.p_size >= 1
          AND p.p_size <= 5
      )
      OR
      (
          p.p_brand = 'Brand#23'
          AND p.p_container IN ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']
          AND l.l_quantity >= 10
          AND l.l_quantity <= 10 + 10
          AND p.p_size >= 1
          AND p.p_size <= 10
      )
      OR
      (
          p.p_brand = 'Brand#34'
          AND p.p_container IN ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']
          AND l.l_quantity >= 20
          AND l.l_quantity <= 20 + 10
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





COLOR = "forest"
DATE = "1994-01-01"
NATION = "CANADA"

Q20 = """
MATCH (ps:PARTSUPP)-[:PARTSUPP_PART]->(p:PART)
MATCH (ps)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE p.p_name STARTS WITH replace('forest%', '%', '')
  AND n.n_name = 'CANADA'
MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps)
WHERE l.l_shipdate >= date('1994-01-01')
  AND l.l_shipdate < date('1994-01-01') + duration({years: 1})
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


NATION = "SAUDI ARABIA"

Q21 = """
MATCH (l1:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
MATCH (l1)-[:LINEITEM_PARTSUPP]->(:PARTSUPP)
      -[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)
      -[:SUPPLIER_NATION]->(n:NATION)
WHERE o.o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND n.n_name = 'SAUDI ARABIA'
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


I1 = 13
I2 = 31
I3 = 23
I4 = 29
I5 = 30
I6 = 18
I7 = 17

Q22 = """
MATCH (average_customer:CUSTOMER)
WHERE average_customer.c_acctbal > 0.0
  AND substring(average_customer.c_phone, 0, 2) IN ['13', '31', '23', '29', '30', '18', '17']
WITH avg(average_customer.c_acctbal) AS average_balance
MATCH (c:CUSTOMER)
WHERE substring(c.c_phone, 0, 2) IN ['13', '31', '23', '29', '30', '18', '17']
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