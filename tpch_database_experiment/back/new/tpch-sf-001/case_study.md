# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-001`
- Selected sample: group `2` of `50` eligible paired R samples (system random)
- Update key: `r_regionkey = 1`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses one paired update case. Each reported update time is the median of its `10` repeated measurements; the `50` update cases are not aggregated. `Δ Update Time = De-normalized median − Normalized median`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 5.108854 | 4.922292 | +0.186562 | 273.141667 | 5 | -5 | -25 | +35 |
| 2 | SNR | 6.029479 | 4.922292 | +1.107187 | 454.308584 | 25 | -5 | -125 | +535 |
| 3 | CNR | 7.502562 | 4.922292 | +2.580270 | 414.416667 | 305 | -5 | -1,525 | +7,535 |
| 4 | CNR + SNR | 6.163833 | 4.922292 | +1.241541 | 525.183125 | 320 | -30 | -1,625 | +7,885 |
| 5 | PSSNR | 11.372583 | 4.922292 | +6.450291 | 1,093.880250 | 1,605 | -105 | -8,125 | +87,335 |
| 6 | CNR + PSSNR | 15.955583 | 4.922292 | +11.033291 | 1,155.590000 | 1,900 | -130 | -9,625 | +94,685 |
| 7 | OCNR | 14.873750 | 4.922292 | +9.951458 | 1,247.747834 | 2,927 | -1,505 | -16,525 | +168,035 |
| 8 | OCNR + SNR | 16.026292 | 4.922292 | +11.104000 | 1,341.259042 | 2,942 | -1,530 | -16,625 | +168,385 |
| 9 | OCNR + PSSNR | 17.157542 | 4.922292 | +12.235250 | 2,033.526250 | 4,522 | -1,630 | -24,625 | +255,185 |
| 10 | LOCNR | 37.448645 | 4.922292 | +32.526353 | 3,794.932625 | 11,787 | -16,505 | -76,700 | +1,056,535 |
| 11 | LOCNR + SNR | 36.413249 | 4.922292 | +31.490958 | 3,944.847125 | 11,802 | -16,530 | -76,800 | +1,056,885 |
| 12 | LPSSNR | 35.313688 | 4.922292 | +30.391396 | 3,752.521208 | 12,000 | -8,105 | -16,125 | +801,785 |
| 13 | CNR + LPSSNR | 33.848897 | 4.922292 | +28.926605 | 3,780.576917 | 12,295 | -8,130 | -17,625 | +809,135 |
| 14 | LOCNR + PSSNR | 40.778146 | 4.922292 | +35.855854 | 4,213.469458 | 13,382 | -16,630 | -84,800 | +1,143,685 |
| 15 | OCNR + LPSSNR | 39.948229 | 4.922292 | +35.025937 | 4,444.538959 | 14,917 | -9,630 | -32,625 | +969,635 |
| 16 | LOCNR + LPSSNR | 81.104604 | 4.922292 | +76.182312 | 5,998.227959 | 23,777 | -24,630 | -92,800 | +1,858,135 |
