TEMPLATE_Q10 = """
SELECT
    t.c_custkey,
    t.c_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    t.c_acctbal,
    n.n_name,
    t.c_address,
    t.c_phone,
    t.c_comment
FROM
    {source} t, -- o,c
    lineitem l,
    nation n
WHERE
    l.l_orderkey = t.o_orderkey
    AND t.o_orderdate >= DATE '{date}'
    AND t.o_orderdate < DATE '{date}' + INTERVAL '3 month'
    AND l.l_returnflag = 'R'
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