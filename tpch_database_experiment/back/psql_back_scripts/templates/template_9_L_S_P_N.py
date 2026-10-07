TEMPLATE_Q9 = """
SELECT
    profit.nation,
    profit.o_year,
    SUM(profit.amount) AS sum_profit
FROM
(
    SELECT
        t.n_name AS nation,
        EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
        (t.l_extendedprice * (1 - t.l_discount)
         - ps.ps_supplycost * t.l_quantity) AS amount
    FROM
        {source} t, --l,s,p,n
        orders o,
        partsupp ps
    WHERE
        ps.ps_suppkey = t.l_suppkey
        AND t.l_orderkey = o.o_orderkey
        AND ps.ps_partkey = t.l_partkey
        AND t.p_name LIKE '%{color}%'
) AS profit
GROUP BY
    profit.nation,
    profit.o_year
ORDER BY
    profit.nation,
    profit.o_year DESC;
"""