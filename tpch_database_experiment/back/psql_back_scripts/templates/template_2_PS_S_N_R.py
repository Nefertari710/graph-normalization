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
    {source} t
WHERE
    p.p_partkey = t.ps_partkey
    AND p.p_size = {p_size}
    AND p.p_type LIKE '%{type_suffix}'
    AND t.r_name = '{region}'
    AND t.ps_supplycost = (
        SELECT
            MIN(t2.ps_supplycost)
        FROM
            {source} t2
        WHERE
            p.p_partkey = t2.ps_partkey
            AND t2.r_name = '{region}'
    )
ORDER BY
    t.s_acctbal DESC,
    t.n_name,
    t.s_name,
    p.p_partkey;
"""
