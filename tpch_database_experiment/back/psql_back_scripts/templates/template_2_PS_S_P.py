TEMPLATE_Q2 = """
SELECT
    t.s_acctbal,
    t.s_name,
    n.n_name,
    t.p_partkey,
    t.p_mfgr,
    t.s_address,
    t.s_phone,
    t.s_comment
FROM
    {source} t,
    nation n,
    region r
WHERE
    t.p_size = {p_size}
    AND t.p_type LIKE '%{type_suffix}'
    AND t.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND t.ps_supplycost = (
        SELECT
            MIN(t2.ps_supplycost)
        FROM
            {source} t2,
            nation n2,
            region r2
        WHERE
            t2.ps_partkey = t.p_partkey
            AND t2.s_nationkey = n2.n_nationkey
            AND n2.n_regionkey = r2.r_regionkey
            AND r2.r_name = '{region}'
    )
ORDER BY
    t.s_acctbal DESC,
    n.n_name,
    t.s_name,
    t.p_partkey;
"""

