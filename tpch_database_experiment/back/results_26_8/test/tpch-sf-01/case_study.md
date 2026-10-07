# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-01`
- Selected sample: group `36` of `50` eligible paired R samples (system random)
- Update key: `r_regionkey = 0`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses this single paired sample; no minimum, maximum, or median is calculated. `Δ Update Time = De-normalized − Normalized`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 4.011833 | 4.732625 | -0.720792 | 270.241292 | 5 | -5 | -25 | +35 |
| 2 | SNR | 7.215542 | 4.732625 | +2.482917 | 991.564916 | 184 | -5 | -1,025 | +5,035 |
| 3 | CNR | 11.196167 | 4.732625 | +6.463542 | 1,607.343333 | 3,041 | -5 | -15,025 | +75,035 |
| 4 | CNR + SNR | 13.324334 | 4.732625 | +8.591709 | 2,097.798916 | 3,215 | -30 | -16,025 | +79,885 |
| 5 | PSSNR | 39.415708 | 4.732625 | +34.683083 | 8,995.291875 | 14,325 | -1,005 | -81,025 | +873,035 |
| 6 | CNR + PSSNR | 50.557083 | 4.732625 | +45.824458 | 10,638.717875 | 17,356 | -1,030 | -96,025 | +947,885 |
| 7 | OCNR | 107.462334 | 4.732625 | +102.729709 | 10,105.523542 | 30,010 | -15,005 | -165,025 | +1,680,035 |
| 8 | OCNR + SNR | 104.032500 | 4.732625 | +99.299875 | 10,548.908458 | 30,184 | -15,030 | -166,025 | +1,684,885 |
| 9 | OCNR + PSSNR | 136.647750 | 4.732625 | +131.915125 | 18,210.542167 | 44,325 | -16,030 | -246,025 | +2,552,885 |
| 10 | LPSSNR | 357.553584 | 4.732625 | +352.820959 | 37,981.037750 | 107,822 | -81,005 | -161,025 | +8,001,043 |
| 11 | CNR + LPSSNR | 327.860875 | 4.732625 | +323.128250 | 39,310.155375 | 110,853 | -81,030 | -176,025 | +8,075,893 |
| 12 | LOCNR | 558.225542 | 4.732625 | +553.492917 | 38,804.615375 | 120,038 | -165,005 | -765,597 | +10,541,475 |
| 13 | LOCNR + SNR | 478.245166 | 4.732625 | +473.512541 | 39,365.179375 | 120,212 | -165,030 | -766,597 | +10,546,325 |
| 14 | LOCNR + PSSNR | 658.410583 | 4.732625 | +653.677958 | 41,786.645291 | 134,353 | -166,030 | -846,597 | +11,414,325 |
| 15 | OCNR + LPSSNR | 779.266750 | 4.732625 | +774.534125 | 44,197.381583 | 137,822 | -96,030 | -326,025 | +9,680,893 |
| 16 | LOCNR + LPSSNR | 1,042.285833 | 4.732625 | +1,037.553208 | 63,975.774542 | 227,850 | -246,030 | -926,597 | +18,542,333 |
