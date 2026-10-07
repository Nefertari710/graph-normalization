TEMPLATE_Q9 = """
SELECT
    profit.nation,
    profit.o_year,
    SUM(profit.amount) AS sum_profit
FROM
(
    SELECT
        t.n_name AS nation,
        EXTRACT(YEAR FROM t.o_orderdate) AS o_year,
        (t.l_extendedprice * (1 - t.l_discount)
         - t.ps_supplycost * t.l_quantity) AS amount
    FROM
        {source} t, --l,s,ps,s,n
        part p
    WHERE
        p.p_partkey = t.l_partkey
        AND p.p_name LIKE '%{color}%'
) AS profit
GROUP BY
    profit.nation,
    profit.o_year
ORDER BY
    profit.nation,
    profit.o_year DESC;
"""