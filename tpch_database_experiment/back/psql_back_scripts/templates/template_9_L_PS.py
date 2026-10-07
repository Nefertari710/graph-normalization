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
        {source} t, --l,ps
        part p,
        supplier s,
        orders o,
        nation n
    WHERE
        s.s_suppkey = t.l_suppkey
        AND p.p_partkey = t.l_partkey
        AND o.o_orderkey = t.l_orderkey
        AND s.s_nationkey = n.n_nationkey
        AND p.p_name LIKE '%{color}%'
) AS profit
GROUP BY
    profit.nation,
    profit.o_year
ORDER BY
    profit.nation,
    profit.o_year DESC;
"""