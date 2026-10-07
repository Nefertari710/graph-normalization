TEMPLATE_Q2 = """
SELECT
    s.s_acctbal,
    s.s_name,
    n.n_name,
    t.p_partkey,
    t.p_mfgr,
    s.s_address,
    s.s_phone,
    s.s_comment
FROM
    {source} t,
    supplier s,
    nation n,
    region r
WHERE
    s.s_suppkey = t.ps_suppkey
    AND t.p_size = {p_size}
    AND t.p_type LIKE '%{type_suffix}'
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = '{region}'
    AND t.ps_supplycost = (
        SELECT
            MIN(t2.ps_supplycost)
        FROM
            {source} t2,
            supplier s2,
            nation n2,
            region r2
        WHERE
            t2.ps_partkey = t.p_partkey
            AND s2.s_suppkey = t2.ps_suppkey
            AND s2.s_nationkey = n2.n_nationkey
            AND n2.n_regionkey = r2.r_regionkey
            AND r2.r_name = '{region}'
    )
ORDER BY
    s.s_acctbal DESC,
    n.n_name,
    s.s_name,
    t.p_partkey;
"""