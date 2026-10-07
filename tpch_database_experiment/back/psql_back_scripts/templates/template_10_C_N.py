TEMPLATE_Q10 = """
SELECT
    t.c_custkey,
    t.c_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    t.c_acctbal,
    t.n_name,
    t.c_address,
    t.c_phone,
    t.c_comment
FROM
    {source} t,
    orders o,
    lineitem l
WHERE
    t.c_custkey = o.o_custkey
    AND l.l_orderkey = o.o_orderkey
    AND o.o_orderdate >= DATE '{date}'
    AND o.o_orderdate < DATE '{date}' + INTERVAL '3 month'
    AND l.l_returnflag = 'R'
GROUP BY
    t.c_custkey,
    t.c_name,
    t.c_acctbal,
    t.c_phone,
    t.n_name,
    t.c_address,
    t.c_comment
ORDER BY
    revenue DESC
LIMIT 20;
"""