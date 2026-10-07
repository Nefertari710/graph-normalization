TEMPLATE_Q20 = """
SELECT DISTINCT
    t.s_name,
    t.s_address
FROM
    partsupp ps,
    {source} t,
    part p
WHERE
    t.n_name = '{nation}'
    AND t.s_suppkey = ps.ps_suppkey
    AND p.p_partkey = ps.ps_partkey
    AND p.p_name LIKE '{p_name}'
    AND ps.ps_availqty > (
        SELECT
            0.5 * SUM(l.l_quantity)
        FROM
            lineitem l
        WHERE
            l.l_partkey = ps.ps_partkey
            AND l.l_suppkey = ps.ps_suppkey
            AND l.l_shipdate >= DATE '{date}'
            AND l.l_shipdate <  DATE '{date}' + INTERVAL '1 year'
    )
ORDER BY
    t.s_name;
"""