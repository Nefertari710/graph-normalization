# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-001`
- Selected sample: group `10` of `10` eligible paired R samples (system random)
- Update key: `r_regionkey = 4`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses this single paired sample; no minimum, maximum, or median is calculated. `Δ Update Time = De-normalized − Normalized`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 3.797125 | 4.109958 | -0.312833 | 216.831875 | 5 | -5 | -25 | +35 |
| 2 | SNR | 5.937458 | 4.109958 | +1.827500 | 353.462042 | 17 | -5 | -125 | +535 |
| 3 | CNR | 7.005042 | 4.109958 | +2.895084 | 446.542875 | 322 | -5 | -1,525 | +7,535 |
| 4 | CNR + SNR | 8.106667 | 4.109958 | +3.996709 | 495.887708 | 329 | -30 | -1,625 | +7,885 |
| 5 | PSSNR | 10.594917 | 4.109958 | +6.484959 | 1,066.894583 | 965 | -105 | -8,125 | +87,335 |
| 6 | CNR + PSSNR | 12.228292 | 4.109958 | +8.118334 | 1,197.973875 | 1,277 | -130 | -9,625 | +94,685 |
| 7 | OCNR | 17.320792 | 4.109958 | +13.210834 | 1,190.920833 | 3,286 | -1,505 | -16,525 | +168,035 |
| 8 | OCNR + SNR | 15.602750 | 4.109958 | +11.492792 | 1,302.869541 | 3,293 | -1,530 | -16,625 | +168,385 |
| 9 | OCNR + PSSNR | 17.747333 | 4.109958 | +13.637375 | 2,118.232292 | 4,241 | -1,630 | -24,625 | +255,185 |
| 10 | LPSSNR | 48.835541 | 4.109958 | +44.725583 | 4,581.371791 | 7,219 | -8,105 | -16,125 | +801,785 |
| 11 | CNR + LPSSNR | 29.772041 | 4.109958 | +25.662083 | 4,020.727042 | 7,531 | -8,130 | -17,625 | +809,135 |
| 12 | OCNR + LPSSNR | 34.080667 | 4.109958 | +29.970709 | 4,795.381500 | 10,495 | -9,630 | -32,625 | +969,635 |
| 13 | LOCNR | 51.348042 | 4.109958 | +47.238084 | 4,158.265000 | 13,201 | -16,505 | -76,700 | +1,056,535 |
| 14 | LOCNR + SNR | 64.446750 | 4.109958 | +60.336792 | 4,159.844334 | 13,208 | -16,530 | -76,800 | +1,056,885 |
| 15 | LOCNR + PSSNR | 57.147375 | 4.109958 | +53.037417 | 4,710.674834 | 14,156 | -16,630 | -84,800 | +1,143,685 |
| 16 | LOCNR + LPSSNR | 80.388083 | 4.109958 | +76.278125 | 6,464.180542 | 20,410 | -24,630 | -92,800 | +1,858,135 |
