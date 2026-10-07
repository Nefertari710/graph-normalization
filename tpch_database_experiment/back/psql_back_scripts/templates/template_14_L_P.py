TEMPLATE_Q14 = """
SELECT
    100.00 *
    SUM(
        CASE
            WHEN t.p_type LIKE 'PROMO%%'
            THEN t.l_extendedprice * (1 - t.l_discount)
            ELSE 0
        END
    ) / SUM(t.l_extendedprice * (1 - t.l_discount))
    AS promo_revenue
FROM
    {source} t
WHERE
    t.l_shipdate >= DATE '{date}'
    AND t.l_shipdate < DATE '{date}' + INTERVAL '1 month';
"""