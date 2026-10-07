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
        t.n_name AS cust_nation,
        EXTRACT(YEAR FROM t.l_shipdate) AS l_year,
        t.l_extendedprice * (1 - t.l_discount) AS volume
    FROM
        supplier s,
        {source} t, -- L,O,C,N2
        (SELECT DISTINCT n_nationkey, n_name FROM {source}) n1
    WHERE
        s.s_suppkey = t.l_suppkey
        AND s.s_nationkey = n1.n_nationkey
        AND (
            (n1.n_name = '{nation1}' AND t.n_name = '{nation2}')
            OR
            (n1.n_name = '{nation2}' AND t.n_name = '{nation1}')
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