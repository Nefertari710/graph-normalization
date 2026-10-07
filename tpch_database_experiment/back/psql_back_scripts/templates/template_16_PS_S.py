TEMPLATE_Q16 = """
SELECT
    p.p_brand,
    p.p_type,
    p.p_size,
    COUNT(DISTINCT t.ps_suppkey) AS supplier_cnt
FROM 
    {source} t,
    part p
WHERE
    p.p_partkey = t.ps_partkey
    AND p.p_brand <> '{brand}'
    AND p.p_type NOT LIKE '{type_prefix}%%'
    AND p.p_size IN ({sizes})
    AND t.s_comment NOT LIKE '%%Customer%%Complaints%%'
GROUP BY
    p.p_brand,
    p.p_type,
    p.p_size
ORDER BY
    supplier_cnt DESC,
    p.p_brand,
    p.p_type,
    p.p_size;
"""