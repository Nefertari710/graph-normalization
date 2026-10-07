TEMPLATE_Q1 = """
SELECT
    t.l_returnflag,
    t.l_linestatus,
    SUM(t.l_quantity) AS sum_qty,
    SUM(t.l_extendedprice) AS sum_base_price,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS sum_disc_price,
    SUM(t.l_extendedprice * (1 - t.l_discount) * (1 + t.l_tax)) AS sum_charge,
    AVG(t.l_quantity) AS avg_qty,
    AVG(t.l_extendedprice) AS avg_price,
    AVG(t.l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    {source} t
WHERE
    t.l_shipdate <= DATE '1998-12-01' - INTERVAL '{delta} days'
GROUP BY
    t.l_returnflag,
    t.l_linestatus
ORDER BY
    t.l_returnflag,
    t.l_linestatus;
"""


TEMPLATE_Q2 = """
WITH supplier_keys AS NOT MATERIALIZED (
    SELECT DISTINCT
        t.s_suppkey,
        t.s_nationkey
    FROM
        {source} t
    WHERE
        t.s_suppkey IS NOT NULL
        AND t.s_nationkey IS NOT NULL
),
min_cost AS (
    SELECT
        ps.ps_partkey,
        MIN(ps.ps_supplycost) AS min_supplycost
    FROM
        partsupp ps,
        supplier_keys sk,
        nation n,
        region r
    WHERE
        ps.ps_suppkey = sk.s_suppkey
        AND sk.s_nationkey = n.n_nationkey
        AND n.n_regionkey = r.r_regionkey
        AND r.r_name = '{region}'
    GROUP BY
        ps.ps_partkey
),
supplier_attr AS NOT MATERIALIZED (
    SELECT DISTINCT
        t.s_suppkey,
        t.s_acctbal,
        t.s_name,
        t.s_address,
        t.s_phone,
        t.s_comment
    FROM
        {source} t
    WHERE
        t.s_suppkey IS NOT NULL
)
SELECT
    sa.s_acctbal,
    sa.s_name,
    n.n_name,
    p.p_partkey,
    p.p_mfgr,
    sa.s_address,
    sa.s_phone,
    sa.s_comment
FROM
    part p,
    partsupp ps,
    min_cost mc,
    supplier_keys sk,
    supplier_attr sa,
    nation n,
    region r
WHERE
    p.p_partkey = ps.ps_partkey
    AND ps.ps_partkey = mc.ps_partkey
    AND ps.ps_supplycost = mc.min_supplycost
    AND ps.ps_suppkey = sk.s_suppkey
    AND sa.s_suppkey = sk.s_suppkey
    AND sk.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND p.p_size = {p_size}
    AND p.p_type LIKE '%{type_suffix}'
ORDER BY
    sa.s_acctbal DESC,
    n.n_name,
    sa.s_name,
    p.p_partkey;

"""



TEMPLATE_Q3 = """
SELECT
    t.l_orderkey,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue,
    o.o_orderdate,
    o.o_shippriority
FROM
    customer c,
    orders o,
    {source} t
WHERE
    c.c_mktsegment = '{segment}'
    AND c.c_custkey = o.o_custkey
    AND t.l_orderkey = o.o_orderkey
    AND o.o_orderdate < '{date}'
    AND t.l_shipdate > '{date}'
GROUP BY
    t.l_orderkey,
    o.o_orderdate,
    o.o_shippriority
ORDER BY
    revenue DESC,
    o.o_orderdate
LIMIT 10;
"""


TEMPLATE_Q4 = """
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
        FROM {source} t
        WHERE t.l_orderkey = o.o_orderkey
          AND t.l_commitdate < t.l_receiptdate
    )
GROUP BY
    o.o_orderpriority
ORDER BY
    o.o_orderpriority;
"""


TEMPLATE_Q5 = """
SELECT
    n.n_name,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue
FROM
    customer c,
    orders o,
    {source} t,
    nation n,
    region r
WHERE
    c.c_custkey = o.o_custkey
    AND t.l_orderkey = o.o_orderkey
    AND c.c_nationkey = t.s_nationkey
    AND t.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND o.o_orderdate >= DATE '{date}'
    AND o.o_orderdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    n.n_name
ORDER BY
    revenue DESC;
"""


TEMPLATE_Q6 = """
SELECT
    SUM(t.l_extendedprice * t.l_discount) AS revenue
FROM
    {source} t
WHERE
    t.l_shipdate >= DATE '{date}'
    AND t.l_shipdate < DATE '{date}' + INTERVAL '1 year'
    AND t.l_discount BETWEEN {discount} - 0.01 AND {discount} + 0.01
    AND t.l_quantity < {quantity};
"""


TEMPLATE_Q7 = """
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
        EXTRACT(YEAR FROM t.l_shipdate) AS l_year,
        t.l_extendedprice * (1 - t.l_discount) AS volume
    FROM
        {source} t,
        orders o,
        customer c,
        nation n1,
        nation n2
    WHERE
        t.l_orderkey = o.o_orderkey
        AND c.c_custkey = o.o_custkey
        AND t.s_nationkey = n1.n_nationkey
        AND c.c_nationkey = n2.n_nationkey
        AND (
            (n1.n_name = '{nation1}' AND n2.n_name = '{nation2}')
            OR
            (n1.n_name = '{nation2}' AND n2.n_name = '{nation1}')
        )
        AND t.l_shipdate BETWEEN DATE '1995-01-01'
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


TEMPLATE_Q8 = """
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
        t.l_extendedprice * (1 - t.l_discount) AS volume,
        n2.n_name AS nation
    FROM
        part p,
        orders o,
        customer c,
        nation n1,
        nation n2,
        region r,
        {source} t
    WHERE
        p.p_partkey = t.l_partkey
        AND t.l_orderkey = o.o_orderkey
        AND o.o_custkey = c.c_custkey
        AND c.c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r.r_regionkey
        AND r.r_name = '{region}'
        AND t.s_nationkey = n2.n_nationkey
        AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND p.p_type = '{ptype}'
) AS all_nations
GROUP BY
    all_nations.o_year
ORDER BY
    all_nations.o_year;
"""


TEMPLATE_Q9 = """
SELECT
    profit.nation,
    profit.o_year,
    SUM(profit.amount) AS sum_profit
FROM
(
    SELECT
        n.n_name AS nation,
        EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
        (t.l_extendedprice * (1 - t.l_discount)
         - ps.ps_supplycost * t.l_quantity) AS amount
    FROM
        part p,
        partsupp ps,
        orders o,
        nation n,
        {source} t
    WHERE
        t.l_orderkey = o.o_orderkey
        AND ps.ps_suppkey = t.s_suppkey
        AND ps.ps_partkey = t.l_partkey
        AND p.p_partkey = t.l_partkey
        AND t.s_nationkey = n.n_nationkey
        AND p.p_name LIKE '%{color}%'
) AS profit
GROUP BY
    profit.nation,
    profit.o_year
ORDER BY
    profit.nation,
    profit.o_year DESC;
"""


TEMPLATE_Q10 = """
SELECT
    c.c_custkey,
    c.c_name,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue,
    c.c_acctbal,
    n.n_name,
    c.c_address,
    c.c_phone,
    c.c_comment
FROM
    customer c,
    orders o,
    nation n,
    {source} t
WHERE
    c.c_custkey = o.o_custkey
    AND t.l_orderkey = o.o_orderkey
    AND c.c_nationkey = n.n_nationkey
    AND o.o_orderdate >= DATE '{date}'
    AND o.o_orderdate < DATE '{date}' + INTERVAL '3 month'
    AND t.l_returnflag = 'R'
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


TEMPLATE_Q11 = """
WITH supplier_only AS NOT MATERIALIZED (
    SELECT DISTINCT
        t.s_suppkey,
        t.s_nationkey
    FROM
        {source} t
    WHERE
        t.s_suppkey IS NOT NULL
        AND t.s_nationkey IS NOT NULL
),
nation_suppliers AS (
    SELECT
        so.s_suppkey
    FROM
        supplier_only so,
        nation n
    WHERE
        so.s_nationkey = n.n_nationkey
        AND n.n_name = '{nation}'
),
total_value AS (
    SELECT
        SUM(ps.ps_supplycost * ps.ps_availqty) AS total_val
    FROM
        partsupp ps,
        nation_suppliers ns
    WHERE
        ps.ps_suppkey = ns.s_suppkey
)
SELECT
    ps.ps_partkey,
    SUM(ps.ps_supplycost * ps.ps_availqty) AS value
FROM
    partsupp ps,
    nation_suppliers ns,
    total_value tv
WHERE
    ps.ps_suppkey = ns.s_suppkey
GROUP BY
    ps.ps_partkey,
    tv.total_val
HAVING
    SUM(ps.ps_supplycost * ps.ps_availqty) > tv.total_val * {fraction}
ORDER BY
    value DESC;
"""


TEMPLATE_Q12 = """
SELECT
    t.l_shipmode,
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
    {source} t
WHERE
    o.o_orderkey = t.l_orderkey
    AND t.l_shipmode IN ('{shipmode1}', '{shipmode2}')
    AND t.l_commitdate < t.l_receiptdate
    AND t.l_shipdate < t.l_commitdate
    AND t.l_receiptdate >= DATE '{date}'
    AND t.l_receiptdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    t.l_shipmode
ORDER BY
    t.l_shipmode;
"""



TEMPLATE_Q13 = """
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


TEMPLATE_Q14 = """
SELECT
    100.00 *
    SUM(
        CASE
            WHEN p.p_type LIKE 'PROMO%%'
            THEN t.l_extendedprice * (1 - t.l_discount)
            ELSE 0
        END
    ) / SUM(t.l_extendedprice * (1 - t.l_discount))
    AS promo_revenue
FROM
    {source} t,
    part p
WHERE
    t.l_partkey = p.p_partkey
    AND t.l_shipdate >= DATE '{date}'
    AND t.l_shipdate < DATE '{date}' + INTERVAL '1 month';
"""


TEMPLATE_Q15 = """
WITH revenue_view AS NOT MATERIALIZED (
    SELECT
        t.l_suppkey AS supplier_no,
        SUM(t.l_extendedprice * (1 - t.l_discount)) AS total_revenue
    FROM
        {source} t
    WHERE
        t.l_suppkey IS NOT NULL
        AND t.l_shipdate >= DATE '{date}'
        AND t.l_shipdate < DATE '{date}' + INTERVAL '3 month'
    GROUP BY
        t.l_suppkey
),
max_revenue AS (
    SELECT
        MAX(total_revenue) AS max_total_revenue
    FROM
        revenue_view
),
supplier_attr AS NOT MATERIALIZED (
    SELECT DISTINCT
        t.s_suppkey,
        t.s_name,
        t.s_address,
        t.s_phone
    FROM
        {source} t
    WHERE
        t.s_suppkey IS NOT NULL
)
SELECT
    sa.s_suppkey,
    sa.s_name,
    sa.s_address,
    sa.s_phone,
    r.total_revenue
FROM
    revenue_view r,
    max_revenue m,
    supplier_attr sa
WHERE
    r.total_revenue = m.max_total_revenue
    AND sa.s_suppkey = r.supplier_no
ORDER BY
    sa.s_suppkey;
"""



TEMPLATE_Q16 = """
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
        SELECT
            t.s_suppkey
        FROM
            {source} t
        WHERE
            t.s_comment LIKE '%%Customer%%Complaints%%'
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


TEMPLATE_Q17 = """
SELECT
    SUM(t.l_extendedprice) / 7.0 AS avg_yearly
FROM
    {source} t,
    part p
WHERE
    p.p_partkey = t.l_partkey
    AND p.p_brand = '{brand}'
    AND p.p_container = '{container}'
    AND t.l_quantity < (
        SELECT
            0.2 * AVG(t2.l_quantity)
        FROM
            {source} t2
        WHERE
            t2.l_partkey = p.p_partkey
    );
"""


TEMPLATE_Q18 = """
SELECT
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    SUM(t.l_quantity) AS sum_qty
FROM
    customer c,
    orders o,
    {source} t
WHERE
    c.c_custkey = o.o_custkey
    AND t.l_orderkey = o.o_orderkey
GROUP BY
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice
HAVING
    SUM(t.l_quantity) > {quantity}
ORDER BY
    o.o_totalprice DESC,
    o.o_orderdate
LIMIT 100;
"""


TEMPLATE_Q19 = """
SELECT
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue
FROM
    {source} t,
    part p
WHERE
    (
        p.p_partkey = t.l_partkey
        AND p.p_brand = '{brand1}'
        AND p.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND t.l_quantity BETWEEN {q1} AND {q1} + 10
        AND p.p_size BETWEEN 1 AND 5
        AND t.l_shipmode IN ('AIR', 'AIR REG')
        AND t.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p.p_partkey = t.l_partkey
        AND p.p_brand = '{brand2}'
        AND p.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND t.l_quantity BETWEEN {q2} AND {q2} + 10
        AND p.p_size BETWEEN 1 AND 10
        AND t.l_shipmode IN ('AIR', 'AIR REG')
        AND t.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p.p_partkey = t.l_partkey
        AND p.p_brand = '{brand3}'
        AND p.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND t.l_quantity BETWEEN {q3} AND {q3} + 10
        AND p.p_size BETWEEN 1 AND 15
        AND t.l_shipmode IN ('AIR', 'AIR REG')
        AND t.l_shipinstruct = 'DELIVER IN PERSON'
    );
"""


TEMPLATE_Q20 = """
WITH supplier_attr AS NOT MATERIALIZED (
    SELECT DISTINCT
        t.s_suppkey,
        t.s_name,
        t.s_address,
        t.s_nationkey
    FROM
        {source} t
    WHERE
        t.s_suppkey IS NOT NULL
),
nation_suppliers AS (
    SELECT
        sa.s_suppkey,
        sa.s_name,
        sa.s_address
    FROM
        supplier_attr sa,
        nation n
    WHERE
        sa.s_nationkey = n.n_nationkey
        AND n.n_name = '{nation}'
),
target_parts AS (
    SELECT
        p.p_partkey
    FROM
        part p
    WHERE
        p.p_name LIKE '{p_name}'
),
shipment_sum AS (
    SELECT
        t.l_suppkey,
        t.l_partkey,
        SUM(t.l_quantity) AS sum_qty
    FROM
        {source} t
    WHERE
        t.l_shipdate >= DATE '{date}'
        AND t.l_shipdate < DATE '{date}' + INTERVAL '1 year'
        AND t.l_suppkey IS NOT NULL
        AND t.l_partkey IS NOT NULL
    GROUP BY
        t.l_suppkey,
        t.l_partkey
),
qualified_partsupp AS (
    SELECT
        ps.ps_suppkey
    FROM
        partsupp ps,
        target_parts tp,
        shipment_sum ss
    WHERE
        ps.ps_partkey = tp.p_partkey
        AND ss.l_partkey = ps.ps_partkey
        AND ss.l_suppkey = ps.ps_suppkey
        AND ps.ps_availqty > 0.5 * ss.sum_qty
)
SELECT
    ns.s_name,
    ns.s_address
FROM
    nation_suppliers ns
WHERE
    ns.s_suppkey IN (
        SELECT DISTINCT ps_suppkey FROM qualified_partsupp
    )
ORDER BY
    ns.s_name;
"""



TEMPLATE_Q21 = """
SELECT
    t.s_name,
    COUNT(*) AS numwait
FROM
    {source} t,
    orders o,
    nation n
WHERE
    t.l_orderkey = o.o_orderkey
    AND o.o_orderstatus = 'F'
    AND t.l_receiptdate > t.l_commitdate
    AND EXISTS (
        SELECT 1
        FROM {source} t2
        WHERE
            t2.l_orderkey = t.l_orderkey
            AND t2.s_suppkey <> t.s_suppkey
    )
    AND NOT EXISTS (
        SELECT 1
        FROM {source} t3
        WHERE
            t3.l_orderkey = t.l_orderkey
            AND t3.s_suppkey <> t.s_suppkey
            AND t3.l_receiptdate > t3.l_commitdate
    )
    AND t.s_nationkey = n.n_nationkey
    AND n.n_name = '{nation}'
GROUP BY
    t.s_name
ORDER BY
    numwait DESC,
    t.s_name;
"""

TEMPLATE_Q22 = """
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