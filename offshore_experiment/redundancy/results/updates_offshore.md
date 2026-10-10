# Offshore Update Experiment Analysis

Input: /Users/yma391/Desktop/M5/M5_home/PhD_research/research/research_2_graph_denormalization/github_final_code/graph-normalization/offshore_experiment/redundancy/results/updates_offshore.json
Experiment timestamp: 2026-10-11T02:44:01.025128+13:00
FDs: 12; maximum sampled groups per FD: 50; repetitions: 5; random seed: 20261006.

## Paper Table: FD Updates on the Full Graph

D denotes the denormalized representation; N denotes the normalized representation. B_N is in seconds; U_D/U_N are in milliseconds.
Groups and Redund. cover all valid groups; timing statistics cover sampled groups.
The Median columns take the medians of the groups' d_median_ms and n_median_ms, respectively.
The Largest group columns use the sampled group with the most copies. K = 1,000; values use the paper's display precision.

| Type | Determinant X | Groups | Redund. | B_N (s) | Median U_D | Median U_N | Largest copies | Largest U_D | Largest U_N |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Address | countries | 323 | 276.7K | 2.7 | 4.2 | 4.0 | 32.5K | 71.9 | 4.4 |
| Entity | service_provider | 4 | 688.2K | 2.0 | 55.6 | 4.3 | 213.6K | 242.3 | 5.1 |
| Entity | country_codes | 682 | 499.2K | 5.6 | 4.2 | 4.0 | 54.9K | 281.5 | 4.6 |
| Entity | countries | 686 | 503.9K | 5.4 | 4.8 | 4.1 | 84.3K | 319.6 | 4.6 |
| Intermediary | country_codes | 166 | 22.9K | 1.0 | 4.1 | 3.9 | 1.5K | 13.6 | 4.5 |
| Intermediary | countries | 166 | 22.9K | 1.5 | 4.4 | 4.1 | 522 | 7.0 | 4.8 |
| Intermediary | valid_until | 10 | 26.8K | 0.3 | 5.6 | 4.0 | 14.1K | 58.5 | 4.5 |
| Intermediary | sourceID | 8 | 25.7K | 0.2 | 6.0 | 4.5 | 14.1K | 67.3 | 4.5 |
| Other | jurisdiction | 5 | 952 | 0.2 | 4.2 | 4.0 | 888 | 8.0 | 4.1 |
| Other | jurisdiction_description | 5 | 952 | 0.2 | 4.2 | 4.1 | 888 | 7.8 | 4.2 |
| Other | country_codes | 23 | 323 | 0.2 | 4.1 | 4.0 | 105 | 4.0 | 3.9 |
| Other | countries | 23 | 323 | 0.2 | 4.1 | 4.0 | 105 | 6.1 | 5.0 |

## Speedup by Copy Count

Compute S = d_median_ms / n_median_ms for each group, then take the median S within each interval. S > 1 favors normalization.
Intervals: copies < 100; 100 ≤ copies < 1,000; 1,000 ≤ copies < 10,000; copies ≥ 10,000.

| Copies | Sampled groups | Median speedup (paper precision) | Median speedup (6 decimals) |
| --- | --- | --- | --- |
| <100 | 247 | 1.0× | 1.026587 |
| 100–999 | 58 | 1.3× | 1.310617 |
| 1,000–9,999 | 12 | 2.0× | 1.993857 |
| ≥10,000 | 11 | 15.0× | 14.984024 |

## Analysis Corresponding to the Paper

Across 12 FDs, 328 sampled groups were analyzed. The per-group median normalized update time ranges from 1.1 to 8.7 ms (before display rounding: 1.119438–8.733312 ms).

Median group speedups by copy-count interval: <100: 1.0× (247 groups); 100–999: 1.3× (58 groups); 1,000–9,999: 2.0× (12 groups); ≥10,000: 15.0× (11 groups).

The maximum observed speedup is 70× (before display rounding: 69.854756×), from group Malta of FD entity_country_reverse (Entity, 84,331 copies). Its D/N median update times are 319.603/4.575 ms, respectively.

The maximum one-time normalization cost per FD is 5.6 s (before display rounding: 5.555877 s; FD: country).

All values are computed from the input JSON using the paper's aggregation rules. The reported range covers per-group median times; raw repeated timings are not stored in this JSON.
