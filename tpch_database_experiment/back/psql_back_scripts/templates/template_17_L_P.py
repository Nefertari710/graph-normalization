TEMPLATE_Q17 = """
SELECT
    SUM(t.l_extendedprice) / 7.0 AS avg_yearly
FROM 
    {source} t
WHERE
    t.p_brand = '{brand}'
    AND t.p_container = '{container}'
    AND t.l_quantity < (
        SELECT
            0.2 * AVG(t2.l_quantity)
        FROM
            {source} t2
        WHERE
            t2.l_partkey = t.p_partkey
    );
"""