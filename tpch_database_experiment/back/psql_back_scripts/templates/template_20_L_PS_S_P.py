TEMPLATE_Q20 = """
SELECT DISTINCT
    t.s_name,
    t.s_address
FROM
    {source} t,
    nation n
WHERE
    t.s_nationkey = n.n_nationkey
    AND t.p_name LIKE '{p_name}'
    AND n.n_name = '{nation}'
    AND EXISTS (
        SELECT 1
        FROM {source} t1
        WHERE
            t1.ps_suppkey = t.ps_suppkey
            AND t1.ps_partkey = t.ps_partkey
        GROUP BY
            t1.ps_suppkey,
            t1.ps_partkey
        HAVING
            COUNT(*) FILTER (
                WHERE t1.l_shipdate >= DATE '{date}'
                  AND t1.l_shipdate <  DATE '{date}' + INTERVAL '1 year'
            ) > 0
            AND MAX(t1.ps_availqty) >
                0.5 * SUM(t1.l_quantity) FILTER (
                    WHERE t1.l_shipdate >= DATE '{date}'
                      AND t1.l_shipdate <  DATE '{date}' + INTERVAL '1 year'
                )
    )
ORDER BY
    t.s_name;
"""