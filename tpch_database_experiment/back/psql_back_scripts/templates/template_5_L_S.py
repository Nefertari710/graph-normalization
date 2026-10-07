TEMPLATE_Q5 = """
SELECT
    n.n_name,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue
FROM
    customer c,
    orders o,
    {source} t,
    nation n,
    region r
WHERE
    c.c_custkey = o.o_custkey
    AND t.l_orderkey = o.o_orderkey
    AND c.c_nationkey = t.s_nationkey
    AND t.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND o.o_orderdate >= DATE '{date}'
    AND o.o_orderdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    n.n_name
ORDER BY
    revenue DESC;
"""