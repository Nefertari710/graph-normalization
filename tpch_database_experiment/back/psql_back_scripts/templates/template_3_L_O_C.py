TEMPLATE_Q3 = """
SELECT
    t.l_orderkey,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue,
    t.o_orderdate,
    t.o_shippriority
FROM
    {source} t
WHERE
    t.c_mktsegment = '{segment}'
    AND t.o_orderkey IS NOT NULL
    -- AND t.o_orderkey IS NOT NULL
    -- AND t.l_orderkey IS NOT NULL
    AND t.o_orderdate < '{date}'
    AND t.l_shipdate > '{date}'
GROUP BY
    t.l_orderkey,
    t.o_orderdate,
    t.o_shippriority
ORDER BY
    revenue DESC,
    t.o_orderdate
LIMIT 10;
"""