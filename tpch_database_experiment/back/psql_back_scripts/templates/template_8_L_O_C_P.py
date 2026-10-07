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
        EXTRACT(YEAR FROM t.o_orderdate) AS o_year,
        t.l_extendedprice * (1 - t.l_discount) AS volume,
        n2.n_name AS nation
    FROM
        {source} t,  -- l,o,c,p,n1,
        supplier s,
        region r,
        nation n1,
        nation n2
    WHERE
        s.s_suppkey = t.l_suppkey
        AND r.r_name = '{region}'
        AND s.s_nationkey = n2.n_nationkey
        AND t.c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r.r_regionkey
        AND t.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND t.p_type = '{ptype}'
) AS all_nations
GROUP BY
    all_nations.o_year
ORDER BY
    all_nations.o_year;
"""

