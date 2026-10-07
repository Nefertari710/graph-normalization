TEMPLATE_Q16 = """
SELECT
    t.p_brand,
    t.p_type,
    t.p_size,
    COUNT(DISTINCT t.ps_suppkey) AS supplier_cnt
FROM
    {source} t
WHERE
    t.p_brand <> '{brand}'
    AND t.p_type NOT LIKE '{type_prefix}%%'
    AND t.p_size IN ({sizes})
    AND t.s_comment NOT LIKE '%%Customer%%Complaints%%'
GROUP BY
    t.p_brand,
    t.p_type,
    t.p_size
ORDER BY
    supplier_cnt DESC,
    t.p_brand,
    t.p_type,
    t.p_size;
"""