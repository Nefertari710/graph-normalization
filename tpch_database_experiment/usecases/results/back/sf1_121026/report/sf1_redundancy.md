# TPC-H SF1 Redundancy and Update Experiment Report

Sources:

- `new_redundancy_results.json`: 10 configurations; update groups per target table: 50; repeats per group: 10.

UD is the denormalized graph update time and UN is the normalized source graph update time, in milliseconds (ms). Each update group uses the median time of repeated runs before computing the table's minimum, median and maximum; restoring original values is excluded.

dU is the median of paired-group UD - UN differences, in ms; a positive value means updating the normalized source graph is faster. B_N is the time to rebuild normalized nodes, relationships and indexes from the denormalized graph, in seconds (s), excluding cleanup.

dP is the increase in non-null TPC-H property values relative to the normalized source graph (excluding `nr_*` properties). K = 1,000, M = 1,000,000. copies is the median number of nodes touched per update group.

Edge mapping: `e_1` = `L_PS`, `e_2` = `L_O`, `e_3` = `PS_P`, `e_4` = `PS_S`, `e_5` = `O_C`, `e_6` = `C_N`, `e_7` = `S_N`, `e_8` = `N_R`.

| target | edges | UD min (ms) | UD median (ms) | UD max (ms) | UN median (ms) | dU (ms) | B_N (s) | dP | copies |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| O | e_2 | 4.0 | 6.6 | 7.8 | 6.5 | +0.2 | 296.8 | +34.51M | 4 |
| C | e_5 | 4.1 | 6.8 | 7.1 | 6.4 | +0.3 | 82.6 | +9.30M | 14 |
| C | e_2,e_5 | 4.1 | 5.9 | 7.4 | 6.4 | -0.4 | 400.8 | +75.32M | 54 |
| N | e_6 | 16.3 | 24.1 | 33.5 | 5.3 | +18.6 | 15.4 | +450.0K | 5995 |
| N | e_5,e_6 | 149.2 | 207.3 | 236.3 | 5.3 | +202.9 | 89.0 | +13.80M | 59724 |
| N | e_7 | 5.5 | 6.1 | 6.9 | 5.3 | +1.0 | 6.8 | +30.0K | 401 |
| N | e_4,e_7 | 64.8 | 106.0 | 120.3 | 5.3 | +100.9 | 102.5 | +7.13M | 32080 |
| R | e_8 | 4.0 | 6.2 | 7.2 | 6.1 | +0.3 | 1.6 | +35 | 5 |
| R | e_6,e_8 | 59.8 | 74.9 | 123.6 | 6.1 | +69.1 | 18.4 | +750.0K | 29957 |
| R | e_5,e_6,e_8 | 798.3 | 1182.6 | 1863.7 | 6.1 | +1177.2 | 128.7 | +16.80M | 299108 |
