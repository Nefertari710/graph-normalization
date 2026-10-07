# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-01`
- Selected sample: group `39` of `50` eligible paired R samples (system random)
- Update key: `r_regionkey = 3`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses this single paired sample; no minimum, maximum, or median is calculated. `Δ Update Time = De-normalized − Normalized`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 6.018250 | 4.405792 | +1.612458 | 281.460083 | 5 | -5 | -25 | +35 |
| 2 | SNR | 4.947417 | 4.405792 | +0.541625 | 745.415584 | 209 | -5 | -1,025 | +5,035 |
| 3 | CNR | 11.124625 | 4.405792 | +6.718833 | 1,693.370875 | 2,973 | -5 | -15,025 | +75,035 |
| 4 | CNR + SNR | 16.326792 | 4.405792 | +11.921000 | 2,110.366042 | 3,172 | -30 | -16,025 | +79,885 |
| 5 | PSSNR | 41.663166 | 4.405792 | +37.257374 | 8,474.917042 | 16,325 | -1,005 | -81,025 | +873,035 |
| 6 | CNR + PSSNR | 48.729583 | 4.405792 | +44.323791 | 10,557.072833 | 19,288 | -1,030 | -96,025 | +947,885 |
| 7 | OCNR | 92.409000 | 4.405792 | +88.003208 | 10,561.394666 | 29,868 | -15,005 | -165,025 | +1,680,035 |
| 8 | OCNR + SNR | 91.195083 | 4.405792 | +86.789291 | 10,450.351083 | 30,067 | -15,030 | -166,025 | +1,684,885 |
| 9 | OCNR + PSSNR | 146.228541 | 4.405792 | +141.822749 | 18,603.067333 | 46,183 | -16,030 | -246,025 | +2,552,885 |
| 10 | LOCNR | 464.311792 | 4.405792 | +459.906000 | 38,903.807250 | 119,404 | -165,005 | -765,597 | +10,541,475 |
| 11 | LOCNR + SNR | 532.754125 | 4.405792 | +528.348333 | 39,850.912000 | 119,603 | -165,030 | -766,597 | +10,546,325 |
| 12 | LPSSNR | 466.606459 | 4.405792 | +462.200667 | 38,231.818583 | 122,542 | -81,005 | -161,025 | +8,001,043 |
| 13 | CNR + LPSSNR | 412.880833 | 4.405792 | +408.475041 | 39,020.590750 | 125,505 | -81,030 | -176,025 | +8,075,893 |
| 14 | LOCNR + PSSNR | 561.940708 | 4.405792 | +557.534916 | 44,288.575042 | 135,719 | -166,030 | -846,597 | +11,414,325 |
| 15 | OCNR + LPSSNR | 580.753583 | 4.405792 | +576.347791 | 43,599.779209 | 152,400 | -96,030 | -326,025 | +9,680,893 |
| 16 | LOCNR + LPSSNR | 920.451875 | 4.405792 | +916.046083 | 63,057.610250 | 241,936 | -246,030 | -926,597 | +18,542,333 |
