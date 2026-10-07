TEMPLATE_Q5 = """
SELECT
    n.n_name,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue
FROM
    customer c,
    {source} t,
    supplier s,
    nation n,
    region r
WHERE
    c.c_custkey = t.o_custkey
    AND t.l_suppkey = s.s_suppkey
    AND c.c_nationkey = s.s_nationkey
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND t.o_orderdate >= DATE '{date}'
    AND t.o_orderdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    n.n_name
ORDER BY
    revenue DESC;
"""