TEMPLATE_Q20 = """
SELECT DISTINCT
    q.s_name,
    q.s_address
FROM
(
    SELECT
        t.s_name,
        t.s_address,
        t.ps_suppkey,
        t.ps_partkey,
        t.ps_availqty
    FROM
        {source} t
    WHERE
        t.n_name = '{nation}'
        AND t.p_name LIKE '{p_name}'
        AND t.ps_availqty > (
            SELECT
                0.5 * SUM(l.l_quantity)
            FROM
                lineitem l
            WHERE
                l.l_partkey = t.ps_partkey
                AND l.l_suppkey = t.ps_suppkey
                AND l.l_shipdate >= DATE '{date}'
                AND l.l_shipdate < DATE '{date}' + INTERVAL '1 year'
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
#     {source} t,
#     lineitem l
# WHERE
#     t.n_name = '{nation}'
#     AND l.l_partkey = t.ps_partkey
#     AND l.l_suppkey = t.ps_suppkey
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
#              AND l.l_shipdate >= DATE '{date}'
#              AND l.l_shipdate <  DATE '{date}' + INTERVAL '1 year'
#             THEN l.l_quantity
#         END
#     )
# ORDER BY
#     t.s_name;
# """