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
         - t.ps_supplycost * t.l_quantity) AS amount
    FROM
        {source} t, --l,ps,s,p
        orders o,
        nation n
    WHERE
        t.s_nationkey = n.n_nationkey
        AND o.o_orderkey = t.l_orderkey
        AND t.p_name LIKE '%{color}%'
) AS profit
GROUP BY
    profit.nation,
    profit.o_year
ORDER BY
    profit.nation,
    profit.o_year DESC;
"""