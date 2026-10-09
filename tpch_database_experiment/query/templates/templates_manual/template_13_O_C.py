"""TPC-H Q13 using schema MV q13_mv_orders_customer (O, C)."""

TEMPLATE_Q13 = """
MATCH (oc:AUTO_JOIN_MV:`q13_mv_orders_customer`)
WITH
    oc,
    [term IN split($pattern, '%') WHERE term <> ''] AS pattern_terms

WITH
    oc.c_custkey AS c_custkey,
    sum(
        CASE
            WHEN oc.o_orderkey IS NOT NULL
             AND NOT (
                size(pattern_terms) = 2
                AND any(
                    suffix IN tail(
                        split(oc.o_comment, pattern_terms[0])
                    )
                    WHERE suffix CONTAINS pattern_terms[1]
                )
             )
            THEN 1
            ELSE 0
        END
    ) AS c_count

WITH
    c_count,
    count(*) AS custdist

RETURN
    c_count,
    custdist

ORDER BY
    custdist DESC,
    c_count DESC
"""
