TEMPLATE_Q5 = """
SELECT
    t.n_name,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue
FROM
    {source} t,
    region r
WHERE
    t.n_regionkey = r.r_regionkey
    AND t.s_nationkey = t.c_nationkey
    AND r.r_name = '{region}'
    AND t.o_orderdate >= DATE '{date}'
    AND t.o_orderdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    t.n_name
ORDER BY
    revenue DESC;
"""