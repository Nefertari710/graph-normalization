TEMPLATE_Q10 = """
SELECT
    c.c_custkey,
    c.c_name,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue,
    c.c_acctbal,
    n.n_name,
    c.c_address,
    c.c_phone,
    c.c_comment
FROM
    {source} t, -- l,o
    customer c,
    nation n
WHERE
    c.c_custkey = t.o_custkey
    AND t.o_orderdate >= DATE '{date}'
    AND t.o_orderdate < DATE '{date}' + INTERVAL '3 month'
    AND t.l_returnflag = 'R'
    AND c.c_nationkey = n.n_nationkey
GROUP BY
    c.c_custkey,
    c.c_name,
    c.c_acctbal,
    c.c_phone,
    n.n_name,
    c.c_address,
    c.c_comment
ORDER BY
    revenue DESC
LIMIT 20;
"""