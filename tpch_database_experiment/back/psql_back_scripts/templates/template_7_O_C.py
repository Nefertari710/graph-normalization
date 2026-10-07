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
        EXTRACT(YEAR FROM l.l_shipdate) AS l_year,
        l.l_extendedprice * (1 - l.l_discount) AS volume
    FROM
        supplier s,
        lineitem l,
        {source} t,
        nation n1,
        nation n2
    WHERE
        s.s_suppkey = l.l_suppkey
        AND t.o_orderkey = l.l_orderkey
        AND s.s_nationkey = n1.n_nationkey
        AND t.c_nationkey = n2.n_nationkey
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