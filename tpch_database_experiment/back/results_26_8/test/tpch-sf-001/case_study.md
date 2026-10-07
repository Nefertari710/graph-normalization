# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-001`
- Selected sample: group `2` of `10` eligible paired R samples (system random)
- Update key: `r_regionkey = 1`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses this single paired sample; no minimum, maximum, or median is calculated. `Δ Update Time = De-normalized − Normalized`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 5.889917 | 4.270041 | +1.619876 | 216.831875 | 5 | -5 | -25 | +35 |
| 2 | SNR | 3.812625 | 4.270041 | -0.457416 | 353.462042 | 25 | -5 | -125 | +535 |
| 3 | CNR | 6.974583 | 4.270041 | +2.704542 | 446.542875 | 305 | -5 | -1,525 | +7,535 |
| 4 | CNR + SNR | 6.780542 | 4.270041 | +2.510501 | 495.887708 | 320 | -30 | -1,625 | +7,885 |
| 5 | PSSNR | 11.862917 | 4.270041 | +7.592876 | 1,066.894583 | 1,605 | -105 | -8,125 | +87,335 |
| 6 | CNR + PSSNR | 11.781834 | 4.270041 | +7.511793 | 1,197.973875 | 1,900 | -130 | -9,625 | +94,685 |
| 7 | OCNR | 14.633458 | 4.270041 | +10.363417 | 1,190.920833 | 2,927 | -1,505 | -16,525 | +168,035 |
| 8 | OCNR + SNR | 15.080666 | 4.270041 | +10.810625 | 1,302.869541 | 2,942 | -1,530 | -16,625 | +168,385 |
| 9 | OCNR + PSSNR | 19.545875 | 4.270041 | +15.275834 | 2,118.232292 | 4,522 | -1,630 | -24,625 | +255,185 |
| 10 | LOCNR | 42.614750 | 4.270041 | +38.344709 | 4,158.265000 | 11,787 | -16,505 | -76,700 | +1,056,535 |
| 11 | LOCNR + SNR | 43.656417 | 4.270041 | +39.386376 | 4,159.844334 | 11,802 | -16,530 | -76,800 | +1,056,885 |
| 12 | LPSSNR | 53.142458 | 4.270041 | +48.872417 | 4,581.371791 | 12,000 | -8,105 | -16,125 | +801,785 |
| 13 | CNR + LPSSNR | 41.765208 | 4.270041 | +37.495167 | 4,020.727042 | 12,295 | -8,130 | -17,625 | +809,135 |
| 14 | LOCNR + PSSNR | 52.755083 | 4.270041 | +48.485042 | 4,710.674834 | 13,382 | -16,630 | -84,800 | +1,143,685 |
| 15 | OCNR + LPSSNR | 50.189916 | 4.270041 | +45.919875 | 4,795.381500 | 14,917 | -9,630 | -32,625 | +969,635 |
| 16 | LOCNR + LPSSNR | 97.094708 | 4.270041 | +92.824667 | 6,464.180542 | 23,777 | -24,630 | -92,800 | +1,858,135 |
