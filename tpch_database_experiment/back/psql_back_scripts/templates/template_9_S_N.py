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
        (l.l_extendedprice * (1 - l.l_discount)
         - ps.ps_supplycost * l.l_quantity) AS amount
    FROM
        {source} t, --s,n
        part p,
        lineitem l,
        orders o,
        partsupp ps
    WHERE
        t.s_suppkey = l.l_suppkey
        AND ps.ps_suppkey = l.l_suppkey
        AND ps.ps_partkey = l.l_partkey
        AND p.p_partkey = l.l_partkey
        AND l.l_orderkey = o.o_orderkey
        AND p.p_name LIKE '%{color}%'
) AS profit
GROUP BY
    profit.nation,
    profit.o_year
ORDER BY
    profit.nation,
    profit.o_year DESC;
"""