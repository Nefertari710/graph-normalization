TEMPLATE_Q7 = """
SELECT
    shipping.supp_nation,
    shipping.cust_nation,
    shipping.l_year,
    SUM(shipping.volume) AS revenue
FROM
(
     SELECT
        t.n1_name AS supp_nation,
        t.n2_name AS cust_nation,
        EXTRACT(YEAR FROM t.l_shipdate) AS l_year,
        t.l_extendedprice * (1 - t.l_discount) AS volume
    FROM
        {source} t     -- L,O,C,S,N1,N2
    WHERE
        (
            (t.n1_name = '{nation1}' AND t.n2_name = '{nation2}')
            OR
            (t.n1_name = '{nation2}' AND t.n2_name = '{nation1}')
        )
        AND t.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
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