# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-01`
- Selected sample: group `8` of `50` eligible paired R samples (selected by --sample)
- Update key: `r_regionkey = 2`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses one paired update case. Each reported update time is the median of its `10` repeated measurements; the `50` update cases are not aggregated. `Δ Update Time = De-normalized median − Normalized median`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 4.995375 | 6.019938 | -1.024563 | 322.201875 | 5 | -5 | -25 | +35 |
| 2 | SNR | 5.904063 | 6.019938 | -0.115875 | 974.088375 | 230 | -5 | -1,025 | +5,035 |
| 3 | CNR | 13.018271 | 6.019938 | +6.998333 | 1,665.729625 | 3,019 | -5 | -15,025 | +75,035 |
| 4 | CNR + SNR | 13.504959 | 6.019938 | +7.485021 | 2,383.688167 | 3,239 | -30 | -16,025 | +79,885 |
| 5 | PSSNR | 40.566396 | 6.019938 | +34.546458 | 8,594.458584 | 18,005 | -1,005 | -81,025 | +873,035 |
| 6 | CNR + PSSNR | 50.757896 | 6.019938 | +44.737958 | 10,802.044416 | 21,014 | -1,030 | -96,025 | +947,885 |
| 7 | OCNR | 85.125062 | 6.019938 | +79.105125 | 9,933.541959 | 30,178 | -15,005 | -165,025 | +1,680,035 |
| 8 | OCNR + SNR | 76.767542 | 6.019938 | +70.747605 | 10,418.280459 | 30,398 | -15,030 | -166,025 | +1,684,885 |
| 9 | OCNR + PSSNR | 121.612105 | 6.019938 | +115.592167 | 19,132.083708 | 48,173 | -16,030 | -246,025 | +2,552,885 |
| 10 | LOCNR | 364.277479 | 6.019938 | +358.257542 | 39,453.933375 | 120,744 | -165,005 | -765,597 | +10,541,475 |
| 11 | LOCNR + SNR | 382.827021 | 6.019938 | +376.807083 | 40,950.559541 | 120,964 | -165,030 | -766,597 | +10,546,325 |
| 12 | LPSSNR | 362.113000 | 6.019938 | +356.093062 | 38,041.750250 | 134,379 | -81,005 | -161,025 | +8,001,043 |
| 13 | CNR + LPSSNR | 403.879437 | 6.019938 | +397.859500 | 39,849.537750 | 137,388 | -81,030 | -176,025 | +8,075,893 |
| 14 | LOCNR + PSSNR | 434.785521 | 6.019938 | +428.765583 | 42,839.031916 | 138,739 | -166,030 | -846,597 | +11,414,325 |
| 15 | OCNR + LPSSNR | 476.743000 | 6.019938 | +470.723062 | 44,646.740084 | 164,547 | -96,030 | -326,025 | +9,680,893 |
| 16 | LOCNR + LPSSNR | 910.167062 | 6.019938 | +904.147125 | 65,744.367167 | 255,113 | -246,030 | -926,597 | +18,542,333 |
