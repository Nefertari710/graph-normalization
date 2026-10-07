TEMPLATE_Q2 = """
SELECT
    s.s_acctbal,
    s.s_name,
    t.n_name,
    p.p_partkey,
    p.p_mfgr,
    s.s_address,
    s.s_phone,
    s.s_comment
FROM
    part p,
    supplier s,
    partsupp ps,
    {source} t
WHERE
    p.p_partkey = ps.ps_partkey
    AND s.s_suppkey = ps.ps_suppkey
    AND p.p_size = {p_size}
    AND p.p_type LIKE '%{type_suffix}'
    AND s.s_nationkey = t.n_nationkey
    AND t.r_name = '{region}'
    AND ps.ps_supplycost = (
        SELECT
            MIN(ps2.ps_supplycost)
        FROM
            partsupp ps2,
            supplier s2,
            {source} t2
        WHERE
            ps2.ps_partkey = p.p_partkey
            AND ps2.ps_suppkey = s2.s_suppkey
            AND s2.s_nationkey = t2.n_nationkey
            AND t2.r_name = '{region}'
    )
ORDER BY
    s.s_acctbal DESC,
    t.n_name,
    s.s_name,
    p.p_partkey;
"""