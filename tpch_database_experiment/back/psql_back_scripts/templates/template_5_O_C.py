TEMPLATE_Q5 = """
SELECT
    n.n_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    {source} t,
    lineitem l,
    supplier s,
    nation n,
    region r
WHERE
    l.l_orderkey = t.o_orderkey
    AND l.l_suppkey = s.s_suppkey
    AND t.c_nationkey = s.s_nationkey
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