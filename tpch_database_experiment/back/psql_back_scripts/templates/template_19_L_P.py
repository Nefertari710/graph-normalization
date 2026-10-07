TEMPLATE_Q19 = """
SELECT
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue
FROM
    {source} t
WHERE
    (
        t.p_brand = '{brand1}'
        AND t.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND t.l_quantity BETWEEN {q1} AND {q1} + 10
        AND t.p_size BETWEEN 1 AND 5
        AND t.l_shipmode IN ('AIR', 'AIR REG')
        AND t.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        t.p_brand = '{brand2}'
        AND t.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND t.l_quantity BETWEEN {q2} AND {q2} + 10
        AND t.p_size BETWEEN 1 AND 10
        AND t.l_shipmode IN ('AIR', 'AIR REG')
        AND t.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        t.p_brand = '{brand3}'
        AND t.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND t.l_quantity BETWEEN {q3} AND {q3} + 10
        AND t.p_size BETWEEN 1 AND 15
        AND t.l_shipmode IN ('AIR', 'AIR REG')
        AND t.l_shipinstruct = 'DELIVER IN PERSON'
    );
"""