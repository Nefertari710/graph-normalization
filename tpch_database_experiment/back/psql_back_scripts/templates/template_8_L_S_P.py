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
        {source} t,  -- l,o,s,p
        orders o,
        customer c,
        nation n1,
        nation n2,
        region r
    WHERE
        c.c_nationkey = n1.n_nationkey
        AND t.l_orderkey = o.o_orderkey
        AND o.o_custkey = c.c_custkey
        AND r.r_name = '{region}'
        AND n1.n_regionkey = r.r_regionkey
        AND t.s_nationkey = n2.n_nationkey
        AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND t.p_type = '{ptype}'
) AS all_nations
GROUP BY
    all_nations.o_year
ORDER BY
    all_nations.o_year;
"""