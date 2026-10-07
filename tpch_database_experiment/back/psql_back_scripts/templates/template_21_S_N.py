TEMPLATE_Q21 = """
SELECT
    t.s_name,
    COUNT(*) AS numwait
FROM
    {source} t,  -- l1,o,s,n
    orders o,
    lineitem l
WHERE
    t.s_suppkey = l.l_suppkey
    AND o.o_orderkey = l.l_orderkey
    AND o.o_orderstatus = 'F'
    AND l.l_receiptdate > l.l_commitdate
    AND EXISTS (
        SELECT 1
        FROM 
            {source} t2,
            lineitem l2
        WHERE l2.l_orderkey = l.l_orderkey
          AND l2.l_suppkey <> l.l_suppkey
    )
    AND NOT EXISTS (
        SELECT 1
        FROM 
            {source} t3,
            lineitem l3 
        WHERE l3.l_orderkey = l.l_orderkey
          AND l3.l_suppkey <> l.l_suppkey
          AND l3.l_receiptdate > l3.l_commitdate
    )
    AND t.n_name = '{nation}'
GROUP BY
    t.s_name
ORDER BY
    numwait DESC,
    t.s_name;
"""