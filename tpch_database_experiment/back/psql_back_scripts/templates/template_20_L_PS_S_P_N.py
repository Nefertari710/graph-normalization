TEMPLATE_Q20 = """
SELECT DISTINCT
    q.s_name,
    q.s_address
FROM
(
    SELECT
        t.s_name,
        t.s_address,
        t.ps_partkey,
        t.ps_suppkey,
        MAX(t.ps_availqty) AS ps_availqty,
        SUM(
            CASE
                WHEN t.l_shipdate >= DATE '{date}'
                 AND t.l_shipdate < DATE '{date}' + INTERVAL '1 year'
                THEN t.l_quantity
            END
        ) AS total_quantity
    FROM
        {source} t
    WHERE
        t.n_name = '{nation}'
        AND t.p_name LIKE '{p_name}'
    GROUP BY
        t.s_name,
        t.s_address,
        t.ps_partkey,
        t.ps_suppkey
    HAVING
        MAX(t.ps_availqty)
        >
        0.5 * SUM(
            CASE
                WHEN t.l_shipdate >= DATE '{date}'
                 AND t.l_shipdate < DATE '{date}' + INTERVAL '1 year'
                THEN t.l_quantity
            END
        )
) q
ORDER BY
    q.s_name;
"""

# TEMPLATE_Q20 = """
# SELECT
#     t.s_name,
#     t.s_address
# FROM
#     {source} t
# WHERE
#     t.n_name = '{nation}'
# GROUP BY
#     t.s_name,
#     t.s_address,
#     t.ps_suppkey
# HAVING
#     SUM(
#         CASE
#             WHEN t.p_name LIKE '{p_name}'
#             THEN t.ps_availqty
#         END
#     )
#     >
#     0.5 * SUM(
#         CASE
#             WHEN t.p_name LIKE '{p_name}'
#              AND t.l_shipdate >= DATE '{date}'
#              AND t.l_shipdate <  DATE '{date}' + INTERVAL '1 year'
#             THEN t.l_quantity
#         END
#     )
# ORDER BY
#     t.s_name;
# """


