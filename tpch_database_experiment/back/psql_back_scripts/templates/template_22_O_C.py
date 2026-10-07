TEMPLATE_Q22 = """
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM (
    SELECT
        SUBSTRING(t.c_phone FROM 1 FOR 2) AS cntrycode,
        t.c_custkey,
        t.c_acctbal
    FROM
        {source} t
    WHERE
        t.c_custkey IS NOT NULL
        AND SUBSTRING(t.c_phone FROM 1 FOR 2) IN ({codes})
        AND t.c_acctbal > (
            SELECT
                AVG(c2.c_acctbal)
            FROM (
                SELECT DISTINCT
                    c_custkey,
                    c_acctbal,
                    c_phone
                FROM
                    {source}
                WHERE
                    c_custkey IS NOT NULL
            ) c2
            WHERE
                c2.c_acctbal > 0.00
                AND SUBSTRING(c2.c_phone FROM 1 FOR 2) IN ({codes})
        )
    GROUP BY
        t.c_custkey,
        t.c_phone,
        t.c_acctbal
    HAVING
        COUNT(t.o_orderkey) = 0
) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
"""