# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-001`
- Selected sample: group `39` of `50` eligible paired R samples (system random)
- Update key: `r_regionkey = 3`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses this single paired sample; no minimum, maximum, or median is calculated. `Δ Update Time = De-normalized − Normalized`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 6.475855 | 5.300145 | +1.175709 | 273.141667 | 5 | -5 | -25 | +35 |
| 2 | SNR | 8.026125 | 5.300145 | +2.725980 | 454.308584 | 25 | -5 | -125 | +535 |
| 3 | CNR | 11.049751 | 5.300145 | +5.749605 | 414.416667 | 277 | -5 | -1,525 | +7,535 |
| 4 | CNR + SNR | 11.090688 | 5.300145 | +5.790542 | 525.183125 | 292 | -30 | -1,625 | +7,885 |
| 5 | PSSNR | 13.084146 | 5.300145 | +7.784001 | 1,093.880250 | 1,605 | -105 | -8,125 | +87,335 |
| 6 | CNR + PSSNR | 8.890979 | 5.300145 | +3.590834 | 1,155.590000 | 1,872 | -130 | -9,625 | +94,685 |
| 7 | OCNR | 14.979437 | 5.300145 | +9.679292 | 1,247.747834 | 2,728 | -1,505 | -16,525 | +168,035 |
| 8 | OCNR + SNR | 15.938062 | 5.300145 | +10.637917 | 1,341.259042 | 2,743 | -1,530 | -16,625 | +168,385 |
| 9 | OCNR + PSSNR | 16.471480 | 5.300145 | +11.171334 | 2,033.526250 | 4,323 | -1,630 | -24,625 | +255,185 |
| 10 | LOCNR | 38.267416 | 5.300145 | +32.967271 | 3,794.932625 | 10,846 | -16,505 | -76,700 | +1,056,535 |
| 11 | LOCNR + SNR | 38.832000 | 5.300145 | +33.531855 | 6,404.566625 | 10,861 | -16,530 | -76,800 | +1,056,885 |
| 12 | LPSSNR | 38.688375 | 5.300145 | +33.388230 | 3,752.521208 | 11,986 | -8,105 | -16,125 | +801,785 |
| 13 | CNR + LPSSNR | 37.633271 | 5.300145 | +32.333126 | 3,780.576917 | 12,253 | -8,130 | -17,625 | +809,135 |
| 14 | LOCNR + PSSNR | 44.617792 | 5.300145 | +39.317646 | 4,213.469458 | 12,441 | -16,630 | -84,800 | +1,143,685 |
| 15 | OCNR + LPSSNR | 44.986542 | 5.300145 | +39.686396 | 4,444.538959 | 14,704 | -9,630 | -32,625 | +969,635 |
| 16 | LOCNR + LPSSNR | 82.701375 | 5.300145 | +77.401229 | 5,998.227959 | 22,822 | -24,630 | -92,800 | +1,858,135 |
