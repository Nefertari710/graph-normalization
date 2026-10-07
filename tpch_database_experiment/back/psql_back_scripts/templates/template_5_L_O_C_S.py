TEMPLATE_Q5 = """
SELECT
    n.n_name,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue
FROM
    {source} t,
    nation n,
    region r
WHERE
    t.s_nationkey = n.n_nationkey
    AND t.c_nationkey = t.s_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND t.o_orderdate >= DATE '{date}'
    AND t.o_orderdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    n.n_name
ORDER BY
    revenue DESC;
"""