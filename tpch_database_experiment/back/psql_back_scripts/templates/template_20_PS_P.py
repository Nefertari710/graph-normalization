TEMPLATE_Q20 = """
SELECT DISTINCT
    s.s_name,
    s.s_address
FROM
    {source} t,
    supplier s,
    nation n
WHERE
    s.s_nationkey = n.n_nationkey
    AND s.s_suppkey = t.ps_suppkey
    AND t.p_name LIKE '{p_name}'
    AND n.n_name = '{nation}'
    AND EXISTS (
        SELECT 1
        FROM 
            {source} t1,
            lineitem l
        WHERE
            l.l_partkey = t1.ps_partkey
            AND l.l_suppkey = t1.ps_suppkey
            AND t1.ps_suppkey = t.ps_suppkey
            AND t1.ps_partkey = t.ps_partkey
        GROUP BY
            t1.ps_suppkey,
            t1.ps_partkey
        HAVING
            COUNT(*) FILTER (
                WHERE l.l_shipdate >= DATE '{date}'
                  AND l.l_shipdate <  DATE '{date}' + INTERVAL '1 year'
            ) > 0
            AND MAX(t1.ps_availqty) >
                0.5 * SUM(l.l_quantity) FILTER (
                    WHERE l.l_shipdate >= DATE '{date}'
                      AND l.l_shipdate <  DATE '{date}' + INTERVAL '1 year'
                )
    )
ORDER BY
    s.s_name;
"""