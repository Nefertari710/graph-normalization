TEMPLATE_Q3 = """
SELECT
    t.l_orderkey,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue,
    t.o_orderdate,
    t.o_shippriority
FROM
    customer c,
    {source} t
WHERE
    c.c_mktsegment = '{segment}'
    AND c.c_custkey = t.o_custkey
    AND t.o_orderdate < DATE '{date}'
    AND t.l_shipdate > DATE '{date}'
GROUP BY
    t.l_orderkey,
    t.o_orderdate,
    t.o_shippriority
ORDER BY
    revenue DESC,
    t.o_orderdate
LIMIT 10;
"""