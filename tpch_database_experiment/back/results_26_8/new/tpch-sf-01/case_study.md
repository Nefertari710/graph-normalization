# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-01`
- Selected sample: group `27` of `50` eligible paired R samples (selected by --sample)
- Update key: `r_regionkey = 1`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses this single paired sample; no minimum, maximum, or median is calculated. `Δ Update Time = De-normalized − Normalized`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 4.998541 | 3.949333 | +1.049208 | 270.241292 | 5 | -5 | -25 | +35 |
| 2 | SNR | 7.173625 | 3.949333 | +3.224292 | 991.564916 | 199 | -5 | -1,025 | +5,035 |
| 3 | CNR | 9.736917 | 3.949333 | +5.787584 | 1,607.343333 | 2,982 | -5 | -15,025 | +75,035 |
| 4 | CNR + SNR | 13.557208 | 3.949333 | +9.607875 | 2,097.798916 | 3,171 | -30 | -16,025 | +79,885 |
| 5 | PSSNR | 39.058458 | 3.949333 | +35.109125 | 8,995.291875 | 15,525 | -1,005 | -81,025 | +873,035 |
| 6 | CNR + PSSNR | 71.675125 | 3.949333 | +67.725792 | 10,638.717875 | 18,497 | -1,030 | -96,025 | +947,885 |
| 7 | OCNR | 80.358167 | 3.949333 | +76.408834 | 10,105.523542 | 29,595 | -15,005 | -165,025 | +1,680,035 |
| 8 | OCNR + SNR | 75.703666 | 3.949333 | +71.754333 | 10,548.908458 | 29,784 | -15,030 | -166,025 | +1,684,885 |
| 9 | OCNR + PSSNR | 106.876292 | 3.949333 | +102.926959 | 18,210.542167 | 45,110 | -16,030 | -246,025 | +2,552,885 |
| 10 | LPSSNR | 347.440833 | 3.949333 | +343.491500 | 37,981.037750 | 117,028 | -81,005 | -161,025 | +8,001,043 |
| 11 | LOCNR | 374.947042 | 3.949333 | +370.997709 | 38,804.615375 | 118,852 | -165,005 | -765,597 | +10,541,475 |
| 12 | LOCNR + SNR | 393.095667 | 3.949333 | +389.146334 | 39,365.179375 | 119,041 | -165,030 | -766,597 | +10,546,325 |
| 13 | CNR + LPSSNR | 336.249250 | 3.949333 | +332.299917 | 39,310.155375 | 120,000 | -81,030 | -176,025 | +8,075,893 |
| 14 | LOCNR + PSSNR | 545.535542 | 3.949333 | +541.586209 | 41,786.645291 | 134,367 | -166,030 | -846,597 | +11,414,325 |
| 15 | OCNR + LPSSNR | 430.617083 | 3.949333 | +426.667750 | 44,197.381583 | 146,613 | -96,030 | -326,025 | +9,680,893 |
| 16 | LOCNR + LPSSNR | 1,454.188166 | 3.949333 | +1,450.238833 | 63,975.774542 | 235,870 | -246,030 | -926,597 | +18,542,333 |
