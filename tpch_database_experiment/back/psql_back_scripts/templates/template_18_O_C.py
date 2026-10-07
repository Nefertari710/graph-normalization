TEMPLATE_Q18 = """
SELECT
    t.c_name,
    t.c_custkey,
    t.o_orderkey,
    t.o_orderdate,
    t.o_totalprice,
    SUM(l.l_quantity) AS sum_qty
FROM
    {source} t,
    lineitem l
WHERE
    t.o_orderkey = l.l_orderkey
GROUP BY
    t.c_name,
    t.c_custkey,
    t.o_orderkey,
    t.o_orderdate,
    t.o_totalprice
HAVING
    SUM(l.l_quantity) > {quantity}
ORDER BY
    t.o_totalprice DESC,
    t.o_orderdate
LIMIT 100;
"""