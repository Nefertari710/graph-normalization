TEMPLATE_Q4 = """
SELECT
    t.o_orderpriority,
    COUNT(DISTINCT t.o_orderkey) AS order_count
FROM
    {source} t
WHERE
    t.o_orderdate >= DATE '{date}'
    AND t.o_orderdate < DATE '{date}' + INTERVAL '3 month'
    AND t.l_commitdate < t.l_receiptdate
GROUP BY
    t.o_orderpriority
ORDER BY
    t.o_orderpriority;
"""