TEMPLATE_Q5 = """
SELECT
    t.n_name,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue
FROM
    customer c,
    {source} t,
    region r
WHERE
    c.c_custkey = t.o_custkey
    AND c.c_nationkey = t.s_nationkey
    AND t.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND t.o_orderdate >= DATE '{date}'
    AND t.o_orderdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    t.n_name
ORDER BY
    revenue DESC;
"""