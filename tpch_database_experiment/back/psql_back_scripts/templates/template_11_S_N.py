TEMPLATE_Q11 = """
SELECT
    ps.ps_partkey,
    SUM(ps.ps_supplycost * ps.ps_availqty) AS value
FROM
    partsupp ps,
    {source} t
WHERE
    ps.ps_suppkey = t.s_suppkey
    AND t.n_name = '{nation}'
GROUP BY
    ps.ps_partkey
HAVING
    SUM(ps.ps_supplycost * ps.ps_availqty) >
    (
        SELECT
            SUM(ps2.ps_supplycost * ps2.ps_availqty) * {fraction}
        FROM
            partsupp ps2,
            {source} t2
        WHERE
            ps2.ps_suppkey = t2.s_suppkey
            AND t2.n_name = '{nation}'
    )
ORDER BY
    value DESC;
"""