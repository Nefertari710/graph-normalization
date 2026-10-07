TEMPLATE_Q10 = """
SELECT
    t.c_custkey,
    t.c_name,
    SUM(t.l_extendedprice * (1 - t.l_discount)) AS revenue,
    t.c_acctbal,
    n.n_name,
    t.c_address,
    t.c_phone,
    t.c_comment
FROM
    {source} t, -- l,o,c
    nation n
WHERE
    t.o_orderdate >= DATE '{date}'
    AND t.o_orderdate < DATE '{date}' + INTERVAL '3 month'
    AND t.l_returnflag = 'R'
    AND t.c_nationkey = n.n_nationkey
GROUP BY
    t.c_custkey,
    t.c_name,
    t.c_acctbal,
    t.c_phone,
    n.n_name,
    t.c_address,
    t.c_comment
ORDER BY
    revenue DESC
LIMIT 20;
"""