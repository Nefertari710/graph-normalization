TEMPLATE_Q2 = """
SELECT
    t.s_acctbal,
    t.s_name,
    n.n_name,
    t.ps_partkey AS p_partkey,
    p.p_mfgr,
    t.s_address,
    t.s_phone,
    t.s_comment
FROM
    {source} t,
    part p,
    nation n,
    region r
WHERE
    p.p_partkey = t.ps_partkey
    AND p.p_size = {p_size}
    AND p.p_type LIKE '%{type_suffix}'
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
            t2.ps_partkey = p.p_partkey
            AND t2.s_nationkey = n2.n_nationkey
            AND n2.n_regionkey = r2.r_regionkey
            AND r2.r_name = '{region}'
    )
ORDER BY
    t.s_acctbal DESC,
    n.n_name,
    t.s_name,
    p.p_partkey;
"""