BASELINE_Q1 = """
    SELECT
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        AVG(l_quantity) AS avg_qty,
        AVG(l_extendedprice) AS avg_price,
        AVG(l_discount) AS avg_disc,
        COUNT(*) AS count_order
    FROM
        lineitem
    WHERE
        l_shipdate <= DATE '1998-12-01' - INTERVAL '{delta} days'
    GROUP BY
        l_returnflag,
        l_linestatus
    ORDER BY
        l_returnflag,
        l_linestatus;
"""

BASELINE_Q2 = """
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part p,
    supplier s,
    partsupp ps,
    nation n,
    region r
WHERE
    p.p_partkey = ps.ps_partkey
    AND s.s_suppkey = ps.ps_suppkey
    AND p.p_size = {p_size}
    AND p.p_type LIKE '%{type_suffix}'
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND ps.ps_supplycost = (
        SELECT MIN(ps2.ps_supplycost)
        FROM partsupp ps2,
             supplier s2,
             nation n2,
             region r2
        WHERE ps2.ps_partkey = p.p_partkey
          AND ps2.ps_suppkey = s2.s_suppkey
          AND s2.s_nationkey = n2.n_nationkey
          AND n2.n_regionkey = r2.r_regionkey
          AND r2.r_name = '{region}'
    )
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey;
"""

BASELINE_Q3 = """
    SELECT
        l.l_orderkey,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
        o.o_orderdate,
        o.o_shippriority
    FROM
        customer c,
        orders o,
        lineitem l
    WHERE
        c.c_mktsegment = '{segment}'
        AND c.c_custkey = o.o_custkey
        AND l.l_orderkey = o.o_orderkey
        AND o.o_orderdate < '{date}'
        AND l.l_shipdate > '{date}'
    GROUP BY
        l.l_orderkey,
        o.o_orderdate,
        o.o_shippriority
    ORDER BY
        revenue DESC,
        o.o_orderdate
    LIMIT 10;
"""

BASELINE_Q4 = """
SELECT
    o.o_orderpriority,
    COUNT(*) AS order_count
FROM
    orders o
WHERE
    o.o_orderdate >= DATE '{date}'
    AND o.o_orderdate < DATE '{date}' + INTERVAL '3 month'
    AND EXISTS (
        SELECT 1
        FROM lineitem l
        WHERE l.l_orderkey = o.o_orderkey
          AND l.l_commitdate < l.l_receiptdate
    )
GROUP BY
    o.o_orderpriority
ORDER BY
    o.o_orderpriority;
"""

BASELINE_Q5 = """
SELECT
    n.n_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    customer c,
    orders o,
    lineitem l,
    supplier s,
    nation n,
    region r
WHERE
    c.c_custkey = o.o_custkey
    AND l.l_orderkey = o.o_orderkey
    AND l.l_suppkey = s.s_suppkey
    AND c.c_nationkey = s.s_nationkey
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND o.o_orderdate >= DATE '{date}'
    AND o.o_orderdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    n.n_name
ORDER BY
    revenue DESC;
"""

BASELINE_Q6 = """
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= DATE '{date}'
    AND l_shipdate < DATE '{date}' + INTERVAL '1 year'
    AND l_discount BETWEEN {discount} - 0.01 AND {discount} + 0.01
    AND l_quantity < {quantity};
"""

BASELINE_Q7 = """
SELECT
    shipping.supp_nation,
    shipping.cust_nation,
    shipping.l_year,
    SUM(shipping.volume) AS revenue
FROM
(
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        EXTRACT(YEAR FROM l.l_shipdate) AS l_year,
        l.l_extendedprice * (1 - l.l_discount) AS volume
    FROM
        supplier s,
        lineitem l,
        orders o,
        customer c,
        nation n1,
        nation n2
    WHERE
        s.s_suppkey = l.l_suppkey
        AND o.o_orderkey = l.l_orderkey
        AND c.c_custkey = o.o_custkey
        AND s.s_nationkey = n1.n_nationkey
        AND c.c_nationkey = n2.n_nationkey
        AND (
            (n1.n_name = '{nation1}' AND n2.n_name = '{nation2}')
            OR
            (n1.n_name = '{nation2}' AND n2.n_name = '{nation1}')
        )
        AND l.l_shipdate BETWEEN DATE '1995-01-01'
                             AND DATE '1996-12-31'
) AS shipping
GROUP BY
    shipping.supp_nation,
    shipping.cust_nation,
    shipping.l_year
ORDER BY
    shipping.supp_nation,
    shipping.cust_nation,
    shipping.l_year;
"""

BASELINE_Q8 = """
SELECT
    all_nations.o_year,
    SUM(
        CASE
            WHEN all_nations.nation = '{nation}' THEN all_nations.volume
            ELSE 0
        END
    ) / SUM(all_nations.volume) AS mkt_share
FROM
(
    SELECT
        EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
        l.l_extendedprice * (1 - l.l_discount) AS volume,
        n2.n_name AS nation
    FROM
        part p,
        supplier s,
        lineitem l,
        orders o,
        customer c,
        nation n1,
        nation n2,
        region r
    WHERE
        p.p_partkey = l.l_partkey
        AND s.s_suppkey = l.l_suppkey
        AND l.l_orderkey = o.o_orderkey
        AND o.o_custkey = c.c_custkey
        AND c.c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r.r_regionkey
        AND r.r_name = '{region}'
        AND s.s_nationkey = n2.n_nationkey
        AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND p.p_type = '{ptype}'
) AS all_nations
GROUP BY
    all_nations.o_year
ORDER BY
    all_nations.o_year;
"""

BASELINE_Q9 = """
SELECT
    profit.nation,
    profit.o_year,
    SUM(profit.amount) AS sum_profit
FROM
(
    SELECT
        n.n_name AS nation,
        EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
        (l.l_extendedprice * (1 - l.l_discount)
         - ps.ps_supplycost * l.l_quantity) AS amount
    FROM
        part p,
        supplier s,
        lineitem l,
        partsupp ps,
        orders o,
        nation n
    WHERE
        s.s_suppkey = l.l_suppkey
        AND ps.ps_suppkey = l.l_suppkey
        AND ps.ps_partkey = l.l_partkey
        AND p.p_partkey = l.l_partkey
        AND o.o_orderkey = l.l_orderkey
        AND s.s_nationkey = n.n_nationkey
        AND p.p_name LIKE '%{color}%'
) AS profit
GROUP BY
    profit.nation,
    profit.o_year
ORDER BY
    profit.nation,
    profit.o_year DESC;
"""

BASELINE_Q10 = """
SELECT
    c.c_custkey,
    c.c_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    c.c_acctbal,
    n.n_name,
    c.c_address,
    c.c_phone,
    c.c_comment
FROM
    customer c,
    orders o,
    lineitem l,
    nation n
WHERE
    c.c_custkey = o.o_custkey
    AND l.l_orderkey = o.o_orderkey
    AND o.o_orderdate >= DATE '{date}'
    AND o.o_orderdate < DATE '{date}' + INTERVAL '3 month'
    AND l.l_returnflag = 'R'
    AND c.c_nationkey = n.n_nationkey
GROUP BY
    c.c_custkey,
    c.c_name,
    c.c_acctbal,
    c.c_phone,
    n.n_name,
    c.c_address,
    c.c_comment
ORDER BY
    revenue DESC
LIMIT 20;
"""

BASELINE_Q11 = """
SELECT
    ps.ps_partkey,
    SUM(ps.ps_supplycost * ps.ps_availqty) AS value
FROM
    partsupp ps,
    supplier s,
    nation n
WHERE
    ps.ps_suppkey = s.s_suppkey
    AND s.s_nationkey = n.n_nationkey
    AND n.n_name = '{nation}'
GROUP BY
    ps.ps_partkey
HAVING
    SUM(ps.ps_supplycost * ps.ps_availqty) >
    (
        SELECT
            SUM(ps2.ps_supplycost * ps2.ps_availqty) * {fraction}
        FROM
            partsupp ps2,
            supplier s2,
            nation n2
        WHERE
            ps2.ps_suppkey = s2.s_suppkey
            AND s2.s_nationkey = n2.n_nationkey
            AND n2.n_name = '{nation}'
    )
ORDER BY
    value DESC;
"""

BASELINE_Q12 = """
SELECT
    l.l_shipmode,
    SUM(
        CASE
            WHEN o.o_orderpriority = '1-URGENT'
              OR o.o_orderpriority = '2-HIGH'
            THEN 1 ELSE 0
        END
    ) AS high_line_count,
    SUM(
        CASE
            WHEN o.o_orderpriority <> '1-URGENT'
             AND o.o_orderpriority <> '2-HIGH'
            THEN 1 ELSE 0
        END
    ) AS low_line_count
FROM
    orders o,
    lineitem l
WHERE
    o.o_orderkey = l.l_orderkey
    AND l.l_shipmode IN ('{shipmode1}', '{shipmode2}')
    AND l.l_commitdate < l.l_receiptdate
    AND l.l_shipdate < l.l_commitdate
    AND l.l_receiptdate >= DATE '{date}'
    AND l.l_receiptdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    l.l_shipmode
ORDER BY
    l.l_shipmode;
"""

BASELINE_Q13 = """
SELECT
    c_count,
    COUNT(*) AS custdist
FROM
(
    SELECT
        c.c_custkey,
        COUNT(o.o_orderkey) AS c_count
    FROM
        customer c
        LEFT OUTER JOIN orders o
          ON c.c_custkey = o.o_custkey
         AND o.o_comment NOT LIKE '{pattern}'
    GROUP BY
        c.c_custkey
) AS c_orders(c_custkey, c_count)
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;
"""

BASELINE_Q14 = """
SELECT
    100.00 *
    SUM(
        CASE
            WHEN p.p_type LIKE 'PROMO%%'
            THEN l.l_extendedprice * (1 - l.l_discount)
            ELSE 0
        END
    ) / SUM(l.l_extendedprice * (1 - l.l_discount))
    AS promo_revenue
FROM
    lineitem l,
    part p
WHERE
    l.l_partkey = p.p_partkey
    AND l.l_shipdate >= DATE '{date}'
    AND l.l_shipdate < DATE '{date}' + INTERVAL '1 month';
"""

BASELINE_Q15 = """
DROP VIEW IF EXISTS revenue_view;
CREATE VIEW revenue_view AS
        SELECT
            l_suppkey AS supplier_no,
            SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
        FROM
            lineitem
        WHERE
            l_shipdate >= DATE '{date}'
            AND l_shipdate < DATE '{date}' + INTERVAL '3 month'
        GROUP BY
            l_suppkey;
SELECT
    s.s_suppkey,
    s.s_name,
    s.s_address,
    s.s_phone,
    r.total_revenue
FROM
    supplier s,
    revenue_view r
WHERE
    s.s_suppkey = r.supplier_no
    AND r.total_revenue = (
        SELECT MAX(total_revenue) FROM revenue_view
    )
ORDER BY
    s.s_suppkey;

"""

BASELINE_Q16 = """
SELECT
    p.p_brand,
    p.p_type,
    p.p_size,
    COUNT(DISTINCT ps.ps_suppkey) AS supplier_cnt
FROM
    partsupp ps,
    part p
WHERE
    p.p_partkey = ps.ps_partkey
    AND p.p_brand <> '{brand}'
    AND p.p_type NOT LIKE '{type_prefix}%%'
    AND p.p_size IN ({sizes})
    AND ps.ps_suppkey NOT IN (
        SELECT s.s_suppkey
        FROM supplier s
        WHERE s.s_comment LIKE '%%Customer%%Complaints%%'
    )
GROUP BY
    p.p_brand,
    p.p_type,
    p.p_size
ORDER BY
    supplier_cnt DESC,
    p.p_brand,
    p.p_type,
    p.p_size;
"""

BASELINE_Q17 = """
SELECT
    SUM(l.l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem l,
    part p
WHERE
    p.p_partkey = l.l_partkey
    AND p.p_brand = '{brand}'
    AND p.p_container = '{container}'
    AND l.l_quantity < (
        SELECT
            0.2 * AVG(l2.l_quantity)
        FROM
            lineitem l2
        WHERE
            l2.l_partkey = p.p_partkey
    );
"""

BASELINE_Q18 = """
SELECT
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    SUM(l.l_quantity) AS sum_qty
FROM
    customer c,
    orders o,
    lineitem l
WHERE
    c.c_custkey = o.o_custkey
    AND o.o_orderkey = l.l_orderkey
GROUP BY
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice
HAVING
    SUM(l.l_quantity) > {quantity}
ORDER BY
    o.o_totalprice DESC,
    o.o_orderdate
LIMIT 100;
"""

BASELINE_Q19 = """
SELECT
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    lineitem l,
    part p
WHERE
    (
        p.p_partkey = l.l_partkey
        AND p.p_brand = '{brand1}'
        AND p.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l.l_quantity BETWEEN {q1} AND {q1} + 10
        AND p.p_size BETWEEN 1 AND 5
        AND l.l_shipmode IN ('AIR', 'AIR REG')
        AND l.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p.p_partkey = l.l_partkey
        AND p.p_brand = '{brand2}'
        AND p.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l.l_quantity BETWEEN {q2} AND {q2} + 10
        AND p.p_size BETWEEN 1 AND 10
        AND l.l_shipmode IN ('AIR', 'AIR REG')
        AND l.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p.p_partkey = l.l_partkey
        AND p.p_brand = '{brand3}'
        AND p.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l.l_quantity BETWEEN {q3} AND {q3} + 10
        AND p.p_size BETWEEN 1 AND 15
        AND l.l_shipmode IN ('AIR', 'AIR REG')
        AND l.l_shipinstruct = 'DELIVER IN PERSON'
    );
"""

BASELINE_Q20 = """
SELECT
    s.s_name,
    s.s_address
FROM
    supplier s,
    nation n
WHERE
    s.s_suppkey IN (
        SELECT
            ps.ps_suppkey
        FROM
            partsupp ps
        WHERE
            ps.ps_partkey IN (
                SELECT
                    p.p_partkey
                FROM
                    part p
                WHERE
                    p.p_name LIKE '{p_name}'
            )
            AND ps.ps_availqty > (
                SELECT
                    0.5 * SUM(l.l_quantity)
                FROM
                    lineitem l
                WHERE
                    l.l_partkey = ps.ps_partkey
                    AND l.l_suppkey = ps.ps_suppkey
                    AND l.l_shipdate >= DATE '{date}'
                    AND l.l_shipdate < DATE '{date}' + INTERVAL '1 year'
            )
    )
    AND s.s_nationkey = n.n_nationkey
    AND n.n_name = '{nation}'
ORDER BY
    s.s_name;
"""

BASELINE_Q21 = """
SELECT
    s.s_name,
    COUNT(*) AS numwait
FROM
    supplier s,
    lineitem l1,
    orders o,
    nation n
WHERE
    s.s_suppkey = l1.l_suppkey
    AND o.o_orderkey = l1.l_orderkey
    AND o.o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT 1
        FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
          AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT 1
        FROM lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey
          AND l3.l_suppkey <> l1.l_suppkey
          AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s.s_nationkey = n.n_nationkey
    AND n.n_name = '{nation}'
GROUP BY
    s.s_name
ORDER BY
    numwait DESC,
    s.s_name;
"""

BASELINE_Q22 = """
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM
(
    SELECT
        SUBSTRING(c.c_phone FROM 1 FOR 2) AS cntrycode,
        c.c_acctbal
    FROM
        customer c
    WHERE
        SUBSTRING(c.c_phone FROM 1 FOR 2) IN ({codes})
        AND c.c_acctbal > (
            SELECT
                AVG(c2.c_acctbal)
            FROM
                customer c2
            WHERE
                c2.c_acctbal > 0.00
                AND SUBSTRING(c2.c_phone FROM 1 FOR 2) IN ({codes})
        )
        AND NOT EXISTS (
            SELECT 1
            FROM orders o
            WHERE o.o_custkey = c.c_custkey
        )
) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
"""