TEMPLATE_Q2 = """
SELECT
    t.s_acctbal,
    t.s_name,
    t.n_name,
    p.p_partkey,
    p.p_mfgr,
    t.s_address,
    t.s_phone,
    t.s_comment
FROM
    part p,
    partsupp ps,
    {source} t
WHERE
    p.p_partkey = ps.ps_partkey
    AND t.s_suppkey = ps.ps_suppkey
    AND p.p_size = {p_size}
    AND p.p_type LIKE '%{type_suffix}'
    AND t.r_name = '{region}'
    AND ps.ps_supplycost = (
        SELECT
            MIN(ps2.ps_supplycost)
        FROM
            partsupp ps2,
            {source} t2
        WHERE
            ps2.ps_partkey = p.p_partkey
            AND t2.s_suppkey = ps2.ps_suppkey
            AND t2.r_name = '{region}'
    )
ORDER BY
    t.s_acctbal DESC,
    t.n_name,
    t.s_name,
    p.p_partkey;
"""