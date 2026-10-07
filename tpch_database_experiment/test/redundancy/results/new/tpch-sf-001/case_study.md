# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-001`
- Selected sample: group `8` of `50` eligible paired R/N samples (fixed sample)
- Update key: `r_regionkey = 2`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses one paired update case. Each reported update time is the median of its `10` repeated measurements; the `50` update cases are not aggregated. `Δ Update Time = De-normalized median − Normalized median`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 6.074063 | 7.042729 | -0.968666 | 273.141667 | 5 | -5 | -25 | +35 |
| 2 | SNR | 7.034709 | 7.042729 | -0.008020 | 454.308584 | 32 | -5 | -125 | +535 |
| 3 | CNR | 11.370062 | 7.042729 | +4.327333 | 414.416667 | 314 | -5 | -1,525 | +7,535 |
| 4 | CNR + SNR | 11.037792 | 7.042729 | +3.995063 | 525.183125 | 336 | -30 | -1,625 | +7,885 |
| 5 | PSSNR | 10.952750 | 7.042729 | +3.910021 | 1,093.880250 | 2,165 | -105 | -8,125 | +87,335 |
| 6 | CNR + PSSNR | 15.889208 | 7.042729 | +8.846479 | 1,155.590000 | 2,469 | -130 | -9,625 | +94,685 |
| 7 | OCNR | 10.529708 | 7.042729 | +3.486979 | 1,247.747834 | 2,964 | -1,505 | -16,525 | +168,035 |
| 8 | OCNR + SNR | 14.185646 | 7.042729 | +7.142917 | 1,341.259042 | 2,986 | -1,530 | -16,625 | +168,385 |
| 9 | OCNR + PSSNR | 16.638000 | 7.042729 | +9.595271 | 2,033.526250 | 5,119 | -1,630 | -24,625 | +255,185 |
| 10 | LOCNR | 35.473354 | 7.042729 | +28.430625 | 3,794.932625 | 11,713 | -16,505 | -76,700 | +1,056,535 |
| 11 | LOCNR + SNR | 35.960521 | 7.042729 | +28.917791 | 3,944.847125 | 11,735 | -16,530 | -76,800 | +1,056,885 |
| 12 | LOCNR + PSSNR | 42.142209 | 7.042729 | +35.099480 | 4,213.469458 | 13,868 | -16,630 | -84,800 | +1,143,685 |
| 13 | LPSSNR | 43.107875 | 7.042729 | +36.065145 | 3,752.521208 | 16,469 | -8,105 | -16,125 | +801,785 |
| 14 | CNR + LPSSNR | 43.468645 | 7.042729 | +36.425916 | 3,780.576917 | 16,773 | -8,130 | -17,625 | +809,135 |
| 15 | OCNR + LPSSNR | 50.695125 | 7.042729 | +43.652396 | 4,444.538959 | 19,423 | -9,630 | -32,625 | +969,635 |
| 16 | LOCNR + LPSSNR | 91.016792 | 7.042729 | +83.974063 | 5,998.227959 | 28,172 | -24,630 | -92,800 | +1,858,135 |
