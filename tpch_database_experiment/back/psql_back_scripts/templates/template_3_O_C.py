TEMPLATE_Q3 = """
SELECT
    l.l_orderkey,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    t.o_orderdate,
    t.o_shippriority
FROM
    {source} t,
    lineitem l
WHERE
    t.c_mktsegment = '{segment}'
    AND l.l_orderkey = t.o_orderkey
    AND t.o_orderdate < DATE '{date}'
    AND l.l_shipdate > DATE '{date}'
GROUP BY
    l.l_orderkey,
    t.o_orderdate,
    t.o_shippriority
ORDER BY
    revenue DESC,
    t.o_orderdate
LIMIT 10;
"""