# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-005`
- Selected sample: group `8` of `50` eligible paired R/N samples (fixed sample)
- Update key: `r_regionkey = 2`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses one paired update case. Each reported update time is the median of its `10` repeated measurements; the `50` update cases are not aggregated. `Δ Update Time = De-normalized median − Normalized median`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 6.002854 | 5.971334 | +0.031521 | 456.700875 | 5 | -5 | -25 | +35 |
| 2 | SNR | 7.941229 | 5.971334 | +1.969895 | 601.363541 | 122 | -5 | -525 | +2,535 |
| 3 | CNR | 11.619813 | 5.971334 | +5.648479 | 988.636917 | 1,498 | -5 | -7,525 | +37,535 |
| 4 | CNR + SNR | 6.980854 | 5.971334 | +1.009521 | 1,262.831458 | 1,610 | -30 | -8,025 | +39,885 |
| 5 | PSSNR | 24.017520 | 5.971334 | +18.046187 | 3,931.698125 | 9,365 | -505 | -40,525 | +436,535 |
| 6 | CNR + PSSNR | 28.104813 | 5.971334 | +22.133479 | 4,747.538458 | 10,853 | -530 | -48,025 | +473,885 |
| 7 | OCNR | 37.985250 | 5.971334 | +32.013917 | 4,942.150667 | 14,524 | -7,505 | -82,525 | +840,035 |
| 8 | OCNR + SNR | 41.656292 | 5.971334 | +35.684959 | 5,153.217333 | 14,636 | -7,530 | -83,025 | +842,385 |
| 9 | OCNR + PSSNR | 68.406229 | 5.971334 | +62.434895 | 9,459.086291 | 23,879 | -8,030 | -123,025 | +1,276,385 |
| 10 | LOCNR | 182.438979 | 5.971334 | +176.467646 | 17,404.078875 | 58,123 | -82,505 | -382,339 | +5,261,315 |
| 11 | LOCNR + SNR | 181.407624 | 5.971334 | +175.436291 | 20,840.431083 | 58,235 | -82,530 | -382,839 | +5,263,665 |
| 12 | LOCNR + PSSNR | 219.245375 | 5.971334 | +213.274042 | 20,918.035959 | 67,478 | -83,030 | -422,839 | +5,697,665 |
| 13 | LPSSNR | 207.695958 | 5.971334 | +201.724625 | 19,670.406333 | 70,562 | -40,505 | -80,525 | +3,993,931 |
| 14 | CNR + LPSSNR | 201.472312 | 5.971334 | +195.500979 | 20,225.559167 | 72,050 | -40,530 | -88,025 | +4,031,281 |
| 15 | OCNR + LPSSNR | 239.420124 | 5.971334 | +233.448791 | 23,690.434500 | 85,076 | -48,030 | -163,025 | +4,833,781 |
| 16 | LOCNR + LPSSNR | 466.547625 | 5.971334 | +460.576292 | 32,413.611250 | 128,675 | -123,030 | -462,839 | +9,255,061 |
