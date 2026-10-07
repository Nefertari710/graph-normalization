TEMPLATE_Q5 = """
SELECT
    t.n_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    lineitem l,
    {source} t
WHERE
    l.l_orderkey = t.o_orderkey
    AND l.l_suppkey = t.s_suppkey
    AND t.r_name = '{region}'
    AND t.o_orderdate >= DATE '{date}'
    AND t.o_orderdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    t.n_name
ORDER BY
    revenue DESC;
"""