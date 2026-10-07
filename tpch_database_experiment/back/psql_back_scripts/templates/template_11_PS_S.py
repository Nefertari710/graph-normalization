TEMPLATE_Q11 = """
SELECT
    t.ps_partkey,
    SUM(t.ps_supplycost * t.ps_availqty) AS value
FROM
    {source} t, -- ps,s
    nation n
WHERE
    t.s_nationkey = n.n_nationkey
    AND n.n_name = '{nation}'
GROUP BY
    t.ps_partkey
HAVING
    SUM(t.ps_supplycost * t.ps_availqty) >
    (
        SELECT
            SUM(t2.ps_supplycost * t2.ps_availqty) * {fraction}
        FROM
            {source} t2, -- ps2,s2,n2
            nation n2
        WHERE
            t2.s_nationkey = n2.n_nationkey
            AND n2.n_name = '{nation}'
    )
ORDER BY
    value DESC;
"""