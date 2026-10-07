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
        t.n2_name AS nation
    FROM
        {source} t  -- L,O,C,S,P,N1,N2,R
    WHERE
        t.r_name = '{region}'
        AND t.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND t.p_type = '{ptype}'
) AS all_nations
GROUP BY
    all_nations.o_year
ORDER BY
    all_nations.o_year;
"""