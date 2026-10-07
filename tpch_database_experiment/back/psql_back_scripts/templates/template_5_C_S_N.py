TEMPLATE_Q5 = """
SELECT
    t.n_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    orders o,
    lineitem l,
    {source} t,
    region r
WHERE
    t.c_custkey = o.o_custkey
    AND l.l_orderkey = o.o_orderkey
    AND l.l_suppkey = t.s_suppkey
    AND t.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND o.o_orderdate >= DATE '{date}'
    AND o.o_orderdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    t.n_name
ORDER BY
    revenue DESC;
"""