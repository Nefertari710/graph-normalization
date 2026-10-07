# TPC-H Fast Benchmark Results

- Source: `run_benchmark_fast_results.json`
- Benchmark runs analyzed: 370
- TPC-H queries: 16
- Strategies: 74
- Runs per strategy: 5
- Aggregation: arithmetic mean of each timing field by strategy.
- Strategies with incomplete rewritten timings: 5; if any run is `null`/capped, the complete-group rewritten mean is shown as `N/A`.

| query_id | strategy | mv_create_time_ms | baseline.client_time_ms | baseline.neo4j_time_ms | template_result.client_time_ms | template_result.neo4j_time_ms |
|---:|---|---:|---:|---:|---:|---:|
| 2 | q2_mv_nation_region | 153.423792 | 86.6661334 | 83.8 | 25.924325 | 24.4 |
| 2 | q2_mv_partsupp_part | 2571.653375 | 17.0485834 | 14.4 | 291.5479668 | 289.6 |
| 2 | q2_mv_partsupp_supplier | 2150.43625 | 23.3052752 | 19.6 | 25.486675 | 22.6 |
| 2 | q2_mv_partsupp_supplier_nation | 2003.906625 | 32.5864332 | 28.6 | 21.0606168 | 18.4 |
| 2 | q2_mv_partsupp_supplier_nation_region | 2110.303208 | 14.0745336 | 9.6 | 16.997825 | 13.6 |
| 2 | q2_mv_partsupp_supplier_part | 1893.499708 | 11.4283914 | 9.4 | N/A | N/A |
| 2 | q2_mv_partsupp_supplier_part_nation | 2258.216958 | 11.9382752 | 8.8 | 128.6916748 | 126.2 |
| 2 | q2_mv_partsupp_supplier_part_nation_region | 1691.13725 | 30.5658498 | 27 | N/A | N/A |
| 2 | q2_mv_supplier_nation | 548.291167 | 12.981717 | 9.8 | 27.430075 | 25.4 |
| 2 | q2_mv_supplier_nation_region | 520.752709 | 11.0726416 | 8 | 23.4421252 | 20.6 |
| 3 | q3_mv_lineitem_orders | 13248.777709 | 213.0920084 | 209 | 111.240708 | 109.2 |
| 3 | q3_mv_lineitem_orders_customer | 10118.972125 | 51.5450832 | 50.4 | N/A | N/A |
| 3 | q3_mv_orders_customer | 5041.854041 | 34.3189748 | 32.2 | 62.1182248 | 60 |
| 4 | q4_mv_lineitem_orders | 6242.94175 | 48.7672916 | 47.4 | 108.831817 | 107.2 |
| 5 | q5_mv_lineitem_orders | 15600.842875 | 114.9862416 | 113.4 | 106.7473166 | 106 |
| 5 | q5_mv_lineitem_orders_customer | 18135.310958 | 84.7118916 | 83.2 | 68.569042 | 67.8 |
| 5 | q5_mv_nation_region | 204.156375 | 40.3857998 | 39.8 | 49.8497336 | 49.2 |
| 5 | q5_mv_orders_customer | 6095.159667 | 33.8563914 | 32.4 | 51.2862752 | 49.6 |
| 5 | q5_mv_supplier_nation | 510.416125 | 33.3364914 | 32 | 109.093725 | 108.6 |
| 5 | q5_mv_supplier_nation_region | 459.7615 | 35.3231416 | 33.8 | 94.5292914 | 93.6 |
| 7 | q7_mv_customer_nation2 | 1443.047417 | 46.7698752 | 45.6 | 45.091175 | 43.8 |
| 7 | q7_mv_lineitem_orders | 14063.02375 | 57.1971584 | 56 | 53.7005082 | 52.2 |
| 7 | q7_mv_lineitem_orders_customer | 17993.768375 | 44.7458086 | 43.4 | 40.031258 | 38.8 |
| 7 | q7_mv_lineitem_orders_customer_nation2 | 19161.6515 | 39.472525 | 38.2 | 60.3542416 | 59.2 |
| 7 | q7_mv_orders_customer | 6224.287625 | 51.1198168 | 49.2 | 44.9947086 | 43.8 |
| 7 | q7_mv_orders_customer_nation2 | 10264.427625 | 45.6689834 | 44.2 | 48.8687334 | 47.4 |
| 7 | q7_mv_supplier_nation1 | 410.575833 | 19.0806084 | 18.6 | 48.5792416 | 47.2 |
| 8 | q8_mv_customer_nation1 | 1526.320375 | 118.608025 | 116.8 | 91.3637748 | 88.6 |
| 8 | q8_mv_customer_nation1_region | 1357.238333 | 44.733850 | 43.8 | 85.9048748 | 84.8 |
| 8 | q8_mv_lineitem_orders | 14131.914125 | 131.924650 | 130.6 | 124.6756416 | 123.6 |
| 8 | q8_mv_lineitem_orders_customer | 17845.132958 | 144.180550 | 143.4 | 115.4205502 | 113.8 |
| 8 | q8_mv_lineitem_orders_customer_nation1 | 21247.615958 | 113.5271916 | 112.2 | 91.112975 | 89.8 |
| 8 | q8_mv_lineitem_orders_customer_nation1_region | 22859.852875 | 124.4001334 | 123.6 | 40.632900 | 39.6 |
| 8 | q8_mv_nation1_region | 108.038209 | 55.1655998 | 54.2 | 90.2156418 | 89 |
| 8 | q8_mv_orders_customer | 6039.980708 | 125.4914086 | 124.4 | 88.7270414 | 87.8 |
| 8 | q8_mv_orders_customer_nation1 | 11625.644084 | 116.921175 | 115.8 | 69.7896832 | 68.6 |
| 8 | q8_mv_orders_customer_nation1_region | 11091.170541 | 116.0822416 | 115.4 | 33.3657916 | 32.2 |
| 8 | q8_mv_supplier_nation2 | 417.636334 | 37.8314666 | 37 | 94.073425 | 93.6 |
| 9 | q9_mv_lineitem_orders | 12512.412458 | 64.8853834 | 59.2 | 53.8291418 | 50.4 |
| 9 | q9_mv_lineitem_orders_partsupp | 16693.744334 | 70.9870502 | 68.4 | 52.6363836 | 50.4 |
| 9 | q9_mv_lineitem_partsupp | 16571.107916 | 45.8974168 | 40.4 | 67.162142 | 62.8 |
| 9 | q9_mv_supplier_nation | 436.543708 | 50.5073416 | 46.2 | 97.2403416 | 92.6 |
| 10 | q10_mv_customer_nation | 1443.4775 | 103.4672916 | 101.8 | 75.2595668 | 73.8 |
| 10 | q10_mv_lineitem_orders | 11665.889917 | 42.1972084 | 41 | 124.6593084 | 123.4 |
| 10 | q10_mv_lineitem_orders_customer | 14959.45925 | 42.8259498 | 41 | 159.9055832 | 158.8 |
| 10 | q10_mv_lineitem_orders_customer_nation | 11103.504458 | 43.2049168 | 40.6 | 158.588950 | 157 |
| 10 | q10_mv_orders_customer | 5983.004958 | 39.2678082 | 38.4 | 65.6964832 | 64.6 |
| 10 | q10_mv_orders_customer_nation | 10550.355625 | 39.2012586 | 37.6 | 58.3186166 | 57 |
| 11 | q11_mv_partsupp_supplier | 1381.291958 | 10.2837248 | 9.6 | 9.111700 | 8.8 |
| 11 | q11_mv_partsupp_supplier_nation | 843.740375 | 3.1407586 | 2.8 | N/A | N/A |
| 11 | q11_mv_supplier_nation | 438.949541 | 5.8385586 | 5.4 | 11.7087916 | 10.8 |
| 12 | q12_mv_lineitem_orders | 6113.987375 | 122.0522584 | 120.8 | 146.9480168 | 146.2 |
| 13 | q13_mv_orders_customer | 1421.017625 | 115.0415834 | 113.2 | 125.583175 | 123.6 |
| 16 | q16_mv_partsupp_part | 1774.234041 | 73.2003668 | 43 | 103.7298334 | 78.8 |
| 16 | q16_mv_partsupp_supplier | 1569.144834 | 50.434100 | 25.6 | 55.077000 | 31.4 |
| 16 | q16_mv_partsupp_supplier_part | 949.625709 | 51.3434502 | 26 | 112.8609166 | 89.4 |
| 18 | q18_mv_lineitem_orders | 11653.133708 | 160.4283834 | 159.6 | 437.8712834 | 437.2 |
| 18 | q18_mv_lineitem_orders_customer | 9530.663708 | 183.3206666 | 182.6 | 404.4904584 | 403.2 |
| 18 | q18_mv_orders_customer | 4510.306583 | 191.3855502 | 190.2 | 270.012333 | 269 |
| 20 | q20_mv_lineitem_partsupp | 13655.772834 | 24.0219416 | 23 | 25.8659834 | 24.8 |
| 20 | q20_mv_lineitem_partsupp_part | 14585.656416 | 14.2174668 | 13 | 20.8203416 | 19.4 |
| 20 | q20_mv_lineitem_partsupp_supplier | 15654.570833 | 13.003375 | 11.6 | 16.907375 | 15.8 |
| 20 | q20_mv_lineitem_partsupp_supplier_nation | 16969.97475 | 11.1542502 | 9.6 | 14.2431166 | 13 |
| 20 | q20_mv_lineitem_partsupp_supplier_part | 19946.660542 | 15.409483 | 14 | 18.6804168 | 17.4 |
| 20 | q20_mv_lineitem_partsupp_supplier_part_nation | 12955.0545 | 14.5930834 | 13.2 | 176.226650 | 174.8 |
| 20 | q20_mv_partsupp_part | 4740.489625 | 8.334075 | 6.2 | 15.1913914 | 12.8 |
| 20 | q20_mv_partsupp_supplier | 5721.458333 | 18.6726414 | 16 | 13.7560496 | 12.4 |
| 20 | q20_mv_partsupp_supplier_nation | 10288.933916 | 85.851700 | 83.6 | 41.9856584 | 40.8 |
| 20 | q20_mv_partsupp_supplier_part | 6406.900125 | 22.3853918 | 21.2 | 15.847875 | 15 |
| 20 | q20_mv_partsupp_supplier_part_nation | 8618.41425 | 20.479950 | 19.4 | 41.7332834 | 40.6 |
| 20 | q20_mv_supplier_nation | 497.590416 | 4.9722166 | 4.4 | 14.1430832 | 13 |
| 21 | q21_mv_lineitem_orders | 13622.281209 | 85.0038082 | 83.6 | N/A | N/A |
| 21 | q21_mv_supplier_nation | 492.081375 | 52.6422836 | 51.8 | 54.6548582 | 53.6 |
| 22 | q22_mv_orders_customer | 1672.333375 | 23.682108 | 22.8 | 45.0489002 | 44 |

## Paper-ready Query Summary

- `Valid/tested` counts strategies whose MV creation, baseline client time, and rewritten client time are present in every run; incomplete/capped strategies are excluded from the averages.
- No fastest run or strategy is selected. Each valid strategy is first summarized by the arithmetic mean across all parameterized runs; those strategy means are then averaged with equal weight for the query (a macro-average).
- Times are displayed as `rewritten → baseline`. Baseline saving is `mean rewritten client time - mean baseline client time` and is shown only when positive; `—` means baseline does not save time for that query.
- Baseline speedup is `mean rewritten client time / mean baseline client time`; values above `1×` mean baseline is faster.

| Query | Valid/tested | Mean client time: rewritten → baseline (ms) | Baseline saving (ms) | Baseline speedup | Mean MV build (s) |
|---:|---:|---:|---:|---:|---:|
| Q2 | 8/10 | 70.1 → 26.2 | 43.9 | 2.67× | 1.54 |
| Q3 | 2/3 | 86.7 → 123.7 | — | 0.70× | 9.15 |
| Q4 | 1/1 | 108.8 → 48.8 | 60.1 | 2.23× | 6.24 |
| Q5 | 6/6 | 80.0 → 57.1 | 22.9 | 1.40× | 6.83 |
| Q7 | 7/7 | 48.8 → 43.4 | 5.4 | 1.12× | 9.94 |
| Q8 | 11/11 | 84.1 → 102.6 | — | 0.82× | 9.84 |
| Q9 | 4/4 | 67.7 → 58.1 | 9.6 | 1.17× | 11.55 |
| Q10 | 6/6 | 107.1 → 51.7 | 55.4 | 2.07× | 9.28 |
| Q11 | 2/3 | 10.4 → 8.1 | 2.3 | 1.29× | 0.91 |
| Q12 | 1/1 | 146.9 → 122.1 | 24.9 | 1.20× | 6.11 |
| Q13 | 1/1 | 125.6 → 115.0 | 10.5 | 1.09× | 1.42 |
| Q16 | 3/3 | 90.6 → 58.3 | 32.2 | 1.55× | 1.43 |
| Q18 | 3/3 | 370.8 → 178.4 | 192.4 | 2.08× | 8.56 |
| Q20 | 12/12 | 34.6 → 21.1 | 13.5 | 1.64× | 10.84 |
| Q21 | 1/2 | 54.7 → 52.6 | 2.0 | 1.04× | 0.49 |
| Q22 | 1/1 | 45.0 → 23.7 | 21.4 | 1.90× | 1.67 |
