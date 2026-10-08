# TPC-H Fast Benchmark Results

- Source: `run_benchmark_results.json`
- Benchmark runs analyzed: 3700
- TPC-H queries: 16
- Strategies: 74
- Runs per strategy: 50
- Aggregation: arithmetic mean by strategy; capped rewritten client times contribute their recorded lower bounds.
- Strategy means using rewritten client-time lower bounds: 16; `>` marks a conservative bound.
- Unavailable rewritten means: client=0, Neo4j=16; these are shown as `N/A`.

| query_id | strategy | mv_create_time_ms | baseline.client_time_ms | baseline.neo4j_time_ms | template_result.client_time_ms | template_result.neo4j_time_ms |
|---:|---|---:|---:|---:|---:|---:|
| 2 | q2_mv_nation_region | 660.539625 | 22.02169504 | 19.36 | 14.1358401 | 12.08 |
| 2 | q2_mv_partsupp_part | 8148.042042 | 22.02169504 | 19.36 | >243.25543006 | N/A |
| 2 | q2_mv_partsupp_supplier | 10573.491334 | 22.02169504 | 19.36 | 20.8642975 | 17.56 |
| 2 | q2_mv_partsupp_supplier_nation | 18562.975208 | 22.02169504 | 19.36 | 12.03277006 | 9.32 |
| 2 | q2_mv_partsupp_supplier_nation_region | 19710.01675 | 22.02169504 | 19.36 | 7.70247336 | 5.64 |
| 2 | q2_mv_partsupp_supplier_part | 7154.17275 | 22.02169504 | 19.36 | >146.63621752 | N/A |
| 2 | q2_mv_partsupp_supplier_part_nation | 20989.16775 | 22.02169504 | 19.36 | >129.52078242 | N/A |
| 2 | q2_mv_partsupp_supplier_part_nation_region | 22809.5175 | 22.02169504 | 19.36 | >348.28679746 | N/A |
| 2 | q2_mv_supplier_nation | 671.673917 | 22.02169504 | 19.36 | 13.29587246 | 9.78 |
| 2 | q2_mv_supplier_nation_region | 771.333958 | 22.02169504 | 19.36 | 13.510120 | 10.28 |
| 3 | q3_mv_lineitem_orders | 23142.076667 | 75.78360906 | 73.66 | 60.18959504 | 58.34 |
| 3 | q3_mv_lineitem_orders_customer | 29897.270875 | 75.78360906 | 73.66 | >122.88038412 | N/A |
| 3 | q3_mv_orders_customer | 8239.173916 | 75.78360906 | 73.66 | 48.1794642 | 46.3 |
| 4 | q4_mv_lineitem_orders | 20417.515667 | 29.10404244 | 27.68 | >89.95878662 | N/A |
| 5 | q5_mv_lineitem_orders | 21056.83225 | 34.28425416 | 33 | 58.70877338 | 57.06 |
| 5 | q5_mv_lineitem_orders_customer | 27804.397834 | 34.28425416 | 33 | 46.67785918 | 45.08 |
| 5 | q5_mv_nation_region | 212.299917 | 34.28425416 | 33 | 38.99116922 | 37.76 |
| 5 | q5_mv_orders_customer | 7990.043334 | 34.28425416 | 33 | 32.15867418 | 30.92 |
| 5 | q5_mv_supplier_nation | 674.316375 | 34.28425416 | 33 | >105.8750092 | N/A |
| 5 | q5_mv_supplier_nation_region | 803.983 | 34.28425416 | 33 | >318.62839262 | N/A |
| 7 | q7_mv_customer_nation2 | 2937.699833 | 34.35520662 | 32.72 | 31.85675676 | 30.38 |
| 7 | q7_mv_lineitem_orders | 21180.898709 | 34.35520662 | 32.72 | 37.52230924 | 36.48 |
| 7 | q7_mv_lineitem_orders_customer | 27311.7645 | 34.35520662 | 32.72 | 26.41930662 | 24.92 |
| 7 | q7_mv_lineitem_orders_customer_nation2 | 105359.35175 | 34.35520662 | 32.72 | 41.92529242 | 40.68 |
| 7 | q7_mv_orders_customer | 7985.048 | 34.35520662 | 32.72 | 28.46698324 | 27.24 |
| 7 | q7_mv_orders_customer_nation2 | 26875.335667 | 34.35520662 | 32.72 | 37.73310074 | 36.16 |
| 7 | q7_mv_supplier_nation1 | 589.868458 | 34.35520662 | 32.72 | 44.3183375 | 42.96 |
| 8 | q8_mv_customer_nation1 | 3117.666542 | 62.96053258 | 61.56 | 51.61525754 | 50.28 |
| 8 | q8_mv_customer_nation1_region | 3321.888417 | 62.96053258 | 61.56 | 57.73713002 | 56.5 |
| 8 | q8_mv_lineitem_orders | 21645.205958 | 62.96053258 | 61.56 | 79.75229916 | 78.54 |
| 8 | q8_mv_lineitem_orders_customer | 27772.419375 | 62.96053258 | 61.56 | 61.17183334 | 59.8 |
| 8 | q8_mv_lineitem_orders_customer_nation1 | 103476.46075 | 62.96053258 | 61.56 | 62.90722742 | 61.48 |
| 8 | q8_mv_lineitem_orders_customer_nation1_region | 108216.269375 | 62.96053258 | 61.56 | 51.80481244 | 50.66 |
| 8 | q8_mv_nation1_region | 163.822542 | 62.96053258 | 61.56 | 50.8879684 | 49.5 |
| 8 | q8_mv_orders_customer | 7114.294708 | 62.96053258 | 61.56 | 46.56308498 | 45.2 |
| 8 | q8_mv_orders_customer_nation1 | 27439.901041 | 62.96053258 | 61.56 | 49.64233582 | 48.44 |
| 8 | q8_mv_orders_customer_nation1_region | 27830.0245 | 62.96053258 | 61.56 | 52.19643668 | 50.96 |
| 8 | q8_mv_supplier_nation2 | 765.100416 | 62.96053258 | 61.56 | 58.24822498 | 56.94 |
| 9 | q9_mv_lineitem_orders | 20946.713875 | 55.80728602 | 49.92 | 51.79064672 | 45.54 |
| 9 | q9_mv_lineitem_orders_partsupp | 27492.518208 | 55.80728602 | 49.92 | 44.58919002 | 37.56 |
| 9 | q9_mv_lineitem_partsupp | 23568.818875 | 55.80728602 | 49.92 | 54.08627082 | 47.42 |
| 9 | q9_mv_supplier_nation | 652.142167 | 55.80728602 | 49.92 | 93.80579748 | 86.74 |
| 10 | q10_mv_customer_nation | 2992.613 | 40.41688088 | 38.1 | 65.69209922 | 62.96 |
| 10 | q10_mv_lineitem_orders | 21610.883792 | 40.41688088 | 38.1 | >122.08151932 | N/A |
| 10 | q10_mv_lineitem_orders_customer | 27731.430166 | 40.41688088 | 38.1 | >150.90878914 | N/A |
| 10 | q10_mv_lineitem_orders_customer_nation | 103164.162959 | 40.41688088 | 38.1 | >151.2354583 | N/A |
| 10 | q10_mv_orders_customer | 7577.804125 | 40.41688088 | 38.1 | 55.98394414 | 53.48 |
| 10 | q10_mv_orders_customer_nation | 26021.93 | 40.41688088 | 38.1 | 51.00965336 | 48.62 |
| 11 | q11_mv_partsupp_supplier | 6921.054417 | 4.68043078 | 4.06 | 4.94101086 | 4.16 |
| 11 | q11_mv_partsupp_supplier_nation | 19338.161333 | 4.68043078 | 4.06 | 27.9977642 | 26.78 |
| 11 | q11_mv_supplier_nation | 705.604375 | 4.68043078 | 4.06 | 5.34361756 | 4.62 |
| 12 | q12_mv_lineitem_orders | 21087.382459 | 131.11956342 | 129.62 | 152.58482578 | 151.08 |
| 13 | q13_mv_orders_customer | 7682.16125 | 108.41924504 | 105.32 | 120.58070246 | 118.24 |
| 16 | q16_mv_partsupp_part | 5523.883 | 54.0417275 | 28.92 | 88.90465164 | 61.4 |
| 16 | q16_mv_partsupp_supplier | 7876.338167 | 54.0417275 | 28.92 | 60.73702172 | 34.92 |
| 16 | q16_mv_partsupp_supplier_part | 7826.103042 | 54.0417275 | 28.92 | 116.61514164 | 86.7 |
| 18 | q18_mv_lineitem_orders | 20960.568625 | 223.5648533 | 222.26 | 402.40073996 | 400.96 |
| 18 | q18_mv_lineitem_orders_customer | 27124.967167 | 223.5648533 | 222.26 | 389.16201244 | 387.7 |
| 18 | q18_mv_orders_customer | 7604.089625 | 223.5648533 | 222.26 | 280.54369664 | 279.04 |
| 20 | q20_mv_lineitem_partsupp | 22276.158666 | 9.1286233 | 7.82 | >15.62527156 | N/A |
| 20 | q20_mv_lineitem_partsupp_part | 25506.508875 | 9.1286233 | 7.82 | >17.65009996 | N/A |
| 20 | q20_mv_lineitem_partsupp_supplier | 35823.655875 | 9.1286233 | 7.82 | 13.3424191 | 11.68 |
| 20 | q20_mv_lineitem_partsupp_supplier_nation | 127451.34975 | 9.1286233 | 7.82 | 11.9569308 | 10.86 |
| 20 | q20_mv_lineitem_partsupp_supplier_part | 37810.280083 | 9.1286233 | 7.82 | 14.05913166 | 12.92 |
| 20 | q20_mv_lineitem_partsupp_supplier_part_nation | 140261.100291 | 9.1286233 | 7.82 | >157.04331246 | N/A |
| 20 | q20_mv_partsupp_part | 6264.924292 | 9.1286233 | 7.82 | 8.93597494 | 6.72 |
| 20 | q20_mv_partsupp_supplier | 8209.527666 | 9.1286233 | 7.82 | 7.76316586 | 6.48 |
| 20 | q20_mv_partsupp_supplier_nation | 19228.172833 | 9.1286233 | 7.82 | 11.10164998 | 9.82 |
| 20 | q20_mv_partsupp_supplier_part | 7135.333958 | 9.1286233 | 7.82 | 5.79565832 | 4.66 |
| 20 | q20_mv_partsupp_supplier_part_nation | 19670.925 | 9.1286233 | 7.82 | >28.12386684 | N/A |
| 20 | q20_mv_supplier_nation | 688.225416 | 9.1286233 | 7.82 | 7.67189334 | 5.84 |
| 21 | q21_mv_lineitem_orders | 21111.579 | 52.07081668 | 49.04 | >396.85636162 | N/A |
| 21 | q21_mv_supplier_nation | 610.370208 | 52.07081668 | 49.04 | 42.24876008 | 39.76 |
| 22 | q22_mv_orders_customer | 7487.921583 | 19.01337916 | 17.46 | 36.1485475 | 34.58 |

## De-normalization Faster Than Baseline

- Includes exact strategy means where the de-normalized client time is lower than the matching baseline; capped lower bounds are excluded.
- Faster strategies: 30.

| Query | De-normalization strategy | De-normalized client (ms) | Baseline client (ms) | Saved by de-normalization (ms) | De-normalization creation (ms) |
|---:|---|---:|---:|---:|---:|
| Q2 | q2_mv_nation_region | 14.136 | 22.022 | 7.886 | 660.540 |
| Q2 | q2_mv_partsupp_supplier | 20.864 | 22.022 | 1.157 | 10573.491 |
| Q2 | q2_mv_partsupp_supplier_nation | 12.033 | 22.022 | 9.989 | 18562.975 |
| Q2 | q2_mv_partsupp_supplier_nation_region | 7.702 | 22.022 | 14.319 | 19710.017 |
| Q2 | q2_mv_supplier_nation | 13.296 | 22.022 | 8.726 | 671.674 |
| Q2 | q2_mv_supplier_nation_region | 13.510 | 22.022 | 8.512 | 771.334 |
| Q3 | q3_mv_lineitem_orders | 60.190 | 75.784 | 15.594 | 23142.077 |
| Q3 | q3_mv_orders_customer | 48.179 | 75.784 | 27.604 | 8239.174 |
| Q5 | q5_mv_orders_customer | 32.159 | 34.284 | 2.126 | 7990.043 |
| Q7 | q7_mv_customer_nation2 | 31.857 | 34.355 | 2.498 | 2937.700 |
| Q7 | q7_mv_lineitem_orders_customer | 26.419 | 34.355 | 7.936 | 27311.764 |
| Q7 | q7_mv_orders_customer | 28.467 | 34.355 | 5.888 | 7985.048 |
| Q8 | q8_mv_customer_nation1 | 51.615 | 62.961 | 11.345 | 3117.667 |
| Q8 | q8_mv_customer_nation1_region | 57.737 | 62.961 | 5.223 | 3321.888 |
| Q8 | q8_mv_lineitem_orders_customer | 61.172 | 62.961 | 1.789 | 27772.419 |
| Q8 | q8_mv_lineitem_orders_customer_nation1 | 62.907 | 62.961 | 0.053 | 103476.461 |
| Q8 | q8_mv_lineitem_orders_customer_nation1_region | 51.805 | 62.961 | 11.156 | 108216.269 |
| Q8 | q8_mv_nation1_region | 50.888 | 62.961 | 12.073 | 163.823 |
| Q8 | q8_mv_orders_customer | 46.563 | 62.961 | 16.397 | 7114.295 |
| Q8 | q8_mv_orders_customer_nation1 | 49.642 | 62.961 | 13.318 | 27439.901 |
| Q8 | q8_mv_orders_customer_nation1_region | 52.196 | 62.961 | 10.764 | 27830.024 |
| Q8 | q8_mv_supplier_nation2 | 58.248 | 62.961 | 4.712 | 765.100 |
| Q9 | q9_mv_lineitem_orders | 51.791 | 55.807 | 4.017 | 20946.714 |
| Q9 | q9_mv_lineitem_orders_partsupp | 44.589 | 55.807 | 11.218 | 27492.518 |
| Q9 | q9_mv_lineitem_partsupp | 54.086 | 55.807 | 1.721 | 23568.819 |
| Q20 | q20_mv_partsupp_part | 8.936 | 9.129 | 0.193 | 6264.924 |
| Q20 | q20_mv_partsupp_supplier | 7.763 | 9.129 | 1.365 | 8209.528 |
| Q20 | q20_mv_partsupp_supplier_part | 5.796 | 9.129 | 3.333 | 7135.334 |
| Q20 | q20_mv_supplier_nation | 7.672 | 9.129 | 1.457 | 688.225 |
| Q21 | q21_mv_supplier_nation | 42.249 | 52.071 | 9.822 | 610.370 |

## Baseline Faster Than De-normalization

- Includes exact strategy means where the baseline client time is lower than the de-normalized client time; capped lower bounds are excluded.
- Baseline-faster strategies: 28.

| Query | De-normalization strategy | De-normalized client (ms) | Baseline client (ms) | Saved by baseline (ms) | De-normalization creation (ms) |
|---:|---|---:|---:|---:|---:|
| Q5 | q5_mv_lineitem_orders | 58.709 | 34.284 | 24.425 | 21056.832 |
| Q5 | q5_mv_lineitem_orders_customer | 46.678 | 34.284 | 12.394 | 27804.398 |
| Q5 | q5_mv_nation_region | 38.991 | 34.284 | 4.707 | 212.300 |
| Q7 | q7_mv_lineitem_orders | 37.522 | 34.355 | 3.167 | 21180.899 |
| Q7 | q7_mv_lineitem_orders_customer_nation2 | 41.925 | 34.355 | 7.570 | 105359.352 |
| Q7 | q7_mv_orders_customer_nation2 | 37.733 | 34.355 | 3.378 | 26875.336 |
| Q7 | q7_mv_supplier_nation1 | 44.318 | 34.355 | 9.963 | 589.868 |
| Q8 | q8_mv_lineitem_orders | 79.752 | 62.961 | 16.792 | 21645.206 |
| Q9 | q9_mv_supplier_nation | 93.806 | 55.807 | 37.999 | 652.142 |
| Q10 | q10_mv_customer_nation | 65.692 | 40.417 | 25.275 | 2992.613 |
| Q10 | q10_mv_orders_customer | 55.984 | 40.417 | 15.567 | 7577.804 |
| Q10 | q10_mv_orders_customer_nation | 51.010 | 40.417 | 10.593 | 26021.930 |
| Q11 | q11_mv_partsupp_supplier | 4.941 | 4.680 | 0.261 | 6921.054 |
| Q11 | q11_mv_partsupp_supplier_nation | 27.998 | 4.680 | 23.317 | 19338.161 |
| Q11 | q11_mv_supplier_nation | 5.344 | 4.680 | 0.663 | 705.604 |
| Q12 | q12_mv_lineitem_orders | 152.585 | 131.120 | 21.465 | 21087.382 |
| Q13 | q13_mv_orders_customer | 120.581 | 108.419 | 12.161 | 7682.161 |
| Q16 | q16_mv_partsupp_part | 88.905 | 54.042 | 34.863 | 5523.883 |
| Q16 | q16_mv_partsupp_supplier | 60.737 | 54.042 | 6.695 | 7876.338 |
| Q16 | q16_mv_partsupp_supplier_part | 116.615 | 54.042 | 62.573 | 7826.103 |
| Q18 | q18_mv_lineitem_orders | 402.401 | 223.565 | 178.836 | 20960.569 |
| Q18 | q18_mv_lineitem_orders_customer | 389.162 | 223.565 | 165.597 | 27124.967 |
| Q18 | q18_mv_orders_customer | 280.544 | 223.565 | 56.979 | 7604.090 |
| Q20 | q20_mv_lineitem_partsupp_supplier | 13.342 | 9.129 | 4.214 | 35823.656 |
| Q20 | q20_mv_lineitem_partsupp_supplier_nation | 11.957 | 9.129 | 2.828 | 127451.350 |
| Q20 | q20_mv_lineitem_partsupp_supplier_part | 14.059 | 9.129 | 4.931 | 37810.280 |
| Q20 | q20_mv_partsupp_supplier_nation | 11.102 | 9.129 | 1.973 | 19228.173 |
| Q22 | q22_mv_orders_customer | 36.149 | 19.013 | 17.135 | 7487.922 |

## Paper-ready Query Summary

- `Usable/tested` counts strategies with complete MV and baseline times plus either exact rewritten times or capped lower bounds.
- No fastest run or strategy is selected. Each usable strategy is first summarized by the arithmetic mean across all parameterized runs; those strategy means are then averaged with equal weight for the query (a macro-average).
- Times are displayed as `rewritten → baseline`. Baseline saving is `mean rewritten client time - mean baseline client time` and is shown only when an exact value is positive; `—` means the exact result shows no saving.
- Baseline speedup is `mean rewritten client time / mean baseline client time`; values above `1×` mean baseline is faster. A `>` marks a conservative lower bound caused by capped runs.
- A saving bound at or below `0`, or a speedup bound at or below `1×`, is inconclusive rather than evidence against a benefit.

| Query | Usable/tested | Mean client time: rewritten → baseline (ms) | Baseline saving (ms) | Baseline speedup | Mean MV build (s) |
|---:|---:|---:|---:|---:|---:|
| Q2 | 10/10 | >94.9 → 22.0 | >72.9 | >4.31× | 11.01 |
| Q3 | 3/3 | >77.0 → 75.8 | >1.2 | >1.01× | 20.43 |
| Q4 | 1/1 | >89.9 → 29.1 | >60.8 | >3.09× | 20.42 |
| Q5 | 6/6 | >100.1 → 34.3 | >65.8 | >2.92× | 9.76 |
| Q7 | 7/7 | 35.5 → 34.4 | 1.1 | 1.03× | 27.46 |
| Q8 | 11/11 | 56.6 → 63.0 | — | 0.90× | 30.08 |
| Q9 | 4/4 | 61.1 → 55.8 | 5.3 | 1.09× | 18.17 |
| Q10 | 6/6 | >99.4 → 40.4 | >59.0 | >2.46× | 31.52 |
| Q11 | 3/3 | 12.8 → 4.7 | 8.1 | 2.73× | 8.99 |
| Q12 | 1/1 | 152.6 → 131.1 | 21.5 | 1.16× | 21.09 |
| Q13 | 1/1 | 120.6 → 108.4 | 12.2 | 1.11× | 7.68 |
| Q16 | 3/3 | 88.8 → 54.0 | 34.7 | 1.64× | 7.08 |
| Q18 | 3/3 | 357.4 → 223.6 | 133.8 | 1.60× | 18.56 |
| Q20 | 12/12 | >24.9 → 9.1 | >15.7 | >2.73× | 37.53 |
| Q21 | 2/2 | >219.5 → 52.1 | >167.4 | >4.21× | 10.86 |
| Q22 | 1/1 | 36.1 → 19.0 | 17.1 | 1.90× | 7.49 |
