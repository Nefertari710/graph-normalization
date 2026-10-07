TEMPLATE_Q18 = """
SELECT
    t.c_name,
    t.c_custkey,
    t.o_orderkey,
    t.o_orderdate,
    t.o_totalprice,
    SUM(t.l_quantity) AS sum_qty
FROM
    {source} t
GROUP BY
    t.c_name,
    t.c_custkey,
    t.o_orderkey,
    t.o_orderdate,
    t.o_totalprice
HAVING
    SUM(t.l_quantity) > {quantity}
ORDER BY
    t.o_totalprice DESC,
    t.o_orderdate
LIMIT 100;
"""