# TPC-H SF1 Query Performance Report

Data source: `../query/`, containing 20 strategy result files.

- Query times are client wall-clock timings in milliseconds (ms); MV build times are in seconds (s).
- TN: baseline query times on the original graph. TD: measured runtimes of completed template queries, including those completed over the limit.
- capped: runs stopped at the time limit; over: runs that completed beyond the time-limit threshold.
- TDlb: the mean of completed runtimes and lower bounds for capped runs, using runs with recorded timings. When capped runs are present, this is only a lower bound on the overall mean runtime.
- Errors: runs in other execution states; — indicates missing timing data.

| Strategy | Runs | TN mean (ms) | TN median (ms) | TD mean (ms) | TD median (ms) | capped | over | TDlb mean (ms) | MV build (s) | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| [q10_mv_customer_nation](../query/q10_mv_customer_nation.json) | 50 | 1051.9 | 851.0 | 930.2 | 840.7 | 0 | 0 | 930.2 | 162.3 | 0 |
| [q11_mv_partsupp_supplier_nation](../query/q11_mv_partsupp_supplier_nation.json) | 50 | 40.3 | 37.7 | 283.5 | 274.5 | 5 | 45 | 262.9 | 989.0 | 0 |
| [q12_mv_lineitem_orders](../query/q12_mv_lineitem_orders.json) | 50 | 1423.8 | 1152.0 | 1478.4 | 1361.0 | 0 | 0 | 1478.4 | 239.4 | 0 |
| [q13_mv_orders_customer](../query/q13_mv_orders_customer.json) | 50 | 1346.9 | 1263.6 | 1107.3 | 1075.8 | 0 | 0 | 1107.3 | 80.0 | 0 |
| [q16_mv_partsupp_supplier_part](../query/q16_mv_partsupp_supplier_part.json) | 50 | 408.0 | 389.1 | 948.8 | 925.4 | 12 | 35 | 907.5 | 70.3 | 0 |
| [q18_mv_lineitem_orders](../query/q18_mv_lineitem_orders.json) | 50 | 3865.3 | 3231.6 | 5214.2 | 4754.5 | 3 | 2 | 5220.1 | 252.1 | 0 |
| [q20_mv_lineitem_partsupp_supplier_part](../query/q20_mv_lineitem_partsupp_supplier_part.json) | 50 | 287.5 | 253.6 | 393.1 | 334.4 | 1 | 12 | 398.0 | 516.2 | 0 |
| [q20_mv_partsupp_supplier_part](../query/q20_mv_partsupp_supplier_part.json) | 50 | 106.5 | 100.7 | 36.7 | 30.2 | 0 | 0 | 36.7 | 74.7 | 0 |
| [q21_mv_supplier_nation](../query/q21_mv_supplier_nation.json) | 50 | 1010.7 | 521.0 | 518.9 | 402.7 | 0 | 1 | 518.9 | 18.0 | 0 |
| [q22_mv_orders_customer](../query/q22_mv_orders_customer.json) | 50 | 147.2 | 135.3 | 282.3 | 272.4 | 0 | 32 | 282.3 | 81.5 | 0 |
| [q2_mv_partsupp_supplier_nation_region](../query/q2_mv_partsupp_supplier_nation_region.json) | 50 | 126.7 | 108.1 | 48.7 | 42.5 | 0 | 0 | 48.7 | 1026.4 | 0 |
| [q3_mv_orders_customer](../query/q3_mv_orders_customer.json) | 50 | 572.9 | 431.3 | 543.4 | 475.8 | 1 | 2 | 546.4 | 80.8 | 0 |
| [q5_mv_lineitem_orders](../query/q5_mv_lineitem_orders.json) | 50 | 917.8 | 677.1 | 1196.6 | 800.0 | 1 | 9 | 1192.1 | 262.5 | 0 |
| [q5_mv_orders_customer](../query/q5_mv_orders_customer.json) | 50 | 418.3 | 379.2 | 309.1 | 251.9 | 0 | 0 | 309.1 | 81.1 | 0 |
| [q7_mv_lineitem_orders_customer](../query/q7_mv_lineitem_orders_customer.json) | 50 | 884.9 | 371.7 | 901.7 | 580.0 | 4 | 16 | 888.6 | 376.7 | 0 |
| [q7_mv_supplier_nation1](../query/q7_mv_supplier_nation1.json) | 50 | 483.9 | 404.7 | 565.2 | 447.7 | 0 | 5 | 565.2 | 17.3 | 0 |
| [q8_mv_lineitem_orders](../query/q8_mv_lineitem_orders.json) | 50 | 828.4 | 744.6 | 1157.8 | 1019.7 | 8 | 5 | 1261.1 | 250.5 | 0 |
| [q8_mv_orders_customer](../query/q8_mv_orders_customer.json) | 50 | 803.5 | 711.8 | 545.9 | 493.5 | 0 | 0 | 545.9 | 80.7 | 0 |
| [q9_mv_lineitem_orders_partsupp](../query/q9_mv_lineitem_orders_partsupp.json) | 50 | 1033.8 | 501.5 | 894.2 | 626.7 | 4 | 11 | 941.3 | 437.4 | 0 |
| [q9_mv_supplier_nation](../query/q9_mv_supplier_nation.json) | 50 | 661.9 | 564.4 | 1254.1 | 1123.8 | 7 | 22 | 1234.2 | 16.7 | 0 |
