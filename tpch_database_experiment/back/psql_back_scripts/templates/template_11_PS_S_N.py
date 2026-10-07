TEMPLATE_Q11 = """
SELECT
    t.ps_partkey,
    SUM(t.ps_supplycost * t.ps_availqty) AS value
FROM
    {source} t -- ps,s,n
WHERE
    t.n_name = '{nation}'
GROUP BY
    t.ps_partkey
HAVING
    SUM(t.ps_supplycost * t.ps_availqty) >
    (
        SELECT
            SUM(t2.ps_supplycost * t2.ps_availqty) * {fraction}
        FROM
            {source} t2 -- ps2,s2,n2
        WHERE
            t2.n_name = '{nation}'
    )
ORDER BY
    value DESC;
"""