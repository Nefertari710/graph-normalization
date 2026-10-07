TEMPLATE_Q15 = """
WITH revenue_view AS NOT MATERIALIZED (
    SELECT
        t.l_suppkey AS supplier_no,
        SUM(t.l_extendedprice * (1 - t.l_discount)) AS total_revenue
    FROM
        {source} t
    WHERE
        t.l_suppkey IS NOT NULL
        AND t.l_shipdate >= DATE '{date}'
        AND t.l_shipdate < DATE '{date}' + INTERVAL '3 month'
    GROUP BY
        t.l_suppkey
),
max_revenue AS (
    SELECT
        MAX(total_revenue) AS max_total_revenue
    FROM
        revenue_view
),
supplier_attr AS NOT MATERIALIZED (
    SELECT DISTINCT
        t.s_suppkey,
        t.s_name,
        t.s_address,
        t.s_phone
    FROM
        {source} t
    WHERE
        t.s_suppkey IS NOT NULL
)
SELECT
    sa.s_suppkey,
    sa.s_name,
    sa.s_address,
    sa.s_phone,
    r.total_revenue
FROM
    revenue_view r,
    max_revenue m,
    supplier_attr sa
WHERE
    r.total_revenue = m.max_total_revenue
    AND sa.s_suppkey = r.supplier_no
ORDER BY
    sa.s_suppkey;
"""