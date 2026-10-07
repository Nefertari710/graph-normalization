TEMPLATE_Q21 = """
SELECT
    t.s_name,
    COUNT(*) AS numwait
FROM
    {source} t,  -- l1,o,s,n
    orders o
WHERE
    o.o_orderkey = t.l_orderkey
    AND o.o_orderstatus = 'F'
    AND t.l_receiptdate > t.l_commitdate
    AND EXISTS (
        SELECT 1
        FROM {source} t2
        WHERE t2.l_orderkey = t.l_orderkey
          AND t2.l_suppkey <> t.l_suppkey
    )
    AND NOT EXISTS (
        SELECT 1
        FROM {source} t3
        WHERE t3.l_orderkey = t.l_orderkey
          AND t3.l_suppkey <> t.l_suppkey
          AND t3.l_receiptdate > t3.l_commitdate
    )
    AND t.n_name = '{nation}'
GROUP BY
    t.s_name
ORDER BY
    numwait DESC,
    t.s_name;
"""