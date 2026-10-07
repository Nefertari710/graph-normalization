TEMPLATE_Q5 = """
SELECT
    t.n_name,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue
FROM
    {source} t
WHERE
    t.r_name = '{region}'
    AND t.s_nationkey = t.c_nationkey
    AND t.o_orderdate >= DATE '{date}'
    AND t.o_orderdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    t.n_name
ORDER BY
    revenue DESC;
"""