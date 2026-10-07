TEMPLATE_Q20 = """
SELECT DISTINCT
    t.s_name,
    t.s_address
FROM
    {source} t
    JOIN part p
      ON p.p_partkey = t.ps_partkey,
    nation n
WHERE
    n.n_name = '{nation}'
    AND p.p_name LIKE '{p_name}'
    AND t.ps_availqty > (
        SELECT
            0.5 * SUM(l.l_quantity)
        FROM
            lineitem l
        WHERE
            l.l_partkey = t.ps_partkey
            AND l.l_suppkey = t.ps_suppkey
            AND l.l_shipdate >= DATE '{date}'
            AND l.l_shipdate <  DATE '{date}' + INTERVAL '1 year'
    )
    AND t.s_nationkey = n.n_nationkey
ORDER BY
    t.s_name;
"""