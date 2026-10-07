TEMPLATE_Q18 = """
SELECT
    c.c_name,
    c.c_custkey,
    t.o_orderkey,
    t.o_orderdate,
    t.o_totalprice,
    SUM(t.l_quantity) AS sum_qty
FROM
    {source} t,
    customer c
WHERE
    c.c_custkey = t.o_custkey
GROUP BY
    c.c_name,
    c.c_custkey,
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