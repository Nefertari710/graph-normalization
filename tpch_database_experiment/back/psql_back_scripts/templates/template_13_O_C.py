TEMPLATE_Q13 = """
SELECT
    c_count,
    COUNT(*) AS custdist
FROM (
    SELECT
        t.c_custkey,
        SUM(
            CASE
                WHEN t.o_orderkey IS NOT NULL
                 AND t.o_comment NOT LIKE '{pattern}'
                THEN 1 ELSE 0
            END
        ) AS c_count
    FROM 
        {source} t
    WHERE
        t.c_custkey IS NOT NULL
    GROUP BY
        t.c_custkey
) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;
"""