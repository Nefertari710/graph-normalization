TEMPLATE_Q12 = """
SELECT
    t.l_shipmode,
    SUM(
        CASE
            WHEN t.o_orderpriority = '1-URGENT'
              OR t.o_orderpriority = '2-HIGH'
            THEN 1 ELSE 0
        END
    ) AS high_line_count,
    SUM(
        CASE
            WHEN t.o_orderpriority <> '1-URGENT'
             AND t.o_orderpriority <> '2-HIGH'
            THEN 1 ELSE 0
        END
    ) AS low_line_count
FROM
    {source} t
WHERE
    t.l_shipmode IN ('{shipmode1}', '{shipmode2}')
    AND t.l_commitdate < t.l_receiptdate
    AND t.l_shipdate < t.l_commitdate
    AND t.l_receiptdate >= DATE '{date}'
    AND t.l_receiptdate < DATE '{date}' + INTERVAL '1 year'
GROUP BY
    t.l_shipmode
ORDER BY
    t.l_shipmode;
"""