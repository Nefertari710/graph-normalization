# R Redundancy Update Case Study

- Paths: `L → O → C → N → R` and `L → PS → S → N → R`
- Database: `tpch-sf-02`
- Selected sample: group `8` of `50` eligible paired R/N samples (fixed sample)
- Update key: `r_regionkey = 2`; property: `r_comment`
- Completed target strategies: `16/16`

Every row uses one paired update case. Each reported update time is the median of its `10` repeated measurements; the `50` update cases are not aggregated. `Δ Update Time = De-normalized median − Normalized median`, so a positive value is time saved by normalization.

`R Copies Updated` is the de-normalized `touched_nodes` value. Rows are sorted by this count. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 5.089709 | 7.229104 | -2.139395 | 416.633792 | 5 | -5 | -25 | +35 |
| 2 | SNR | 9.829979 | 7.229104 | +2.600875 | 2,075.139208 | 454 | -5 | -2,025 | +10,035 |
| 3 | CNR | 16.231459 | 7.229104 | +9.002355 | 3,496.412708 | 6,056 | -5 | -30,025 | +150,035 |
| 4 | CNR + SNR | 16.985188 | 7.229104 | +9.756084 | 4,757.966541 | 6,500 | -30 | -32,025 | +159,885 |
| 5 | PSSNR | 86.387875 | 7.229104 | +79.158771 | 19,673.931083 | 35,925 | -2,005 | -162,025 | +1,746,035 |
| 6 | CNR + PSSNR | 100.674729 | 7.229104 | +93.445625 | 21,415.332000 | 41,971 | -2,030 | -192,025 | +1,895,885 |
| 7 | OCNR | 153.304062 | 7.229104 | +146.074958 | 21,195.015125 | 60,474 | -30,005 | -330,025 | +3,360,035 |
| 8 | OCNR + SNR | 147.293771 | 7.229104 | +140.064667 | 20,621.570583 | 60,918 | -30,030 | -332,025 | +3,369,885 |
| 9 | OCNR + PSSNR | 292.018958 | 7.229104 | +284.789854 | 41,262.574041 | 96,389 | -32,030 | -492,025 | +5,105,885 |
| 10 | LOCNR | 964.874188 | 7.229104 | +957.645084 | 82,171.233875 | 240,961 | -330,005 | -1,529,994 | +21,059,415 |
| 11 | LOCNR + SNR | 876.255000 | 7.229104 | +869.025896 | 83,110.203708 | 241,405 | -330,030 | -1,531,994 | +21,069,265 |
| 12 | LPSSNR | 779.059749 | 7.229104 | +771.830645 | 82,805.234833 | 269,255 | -162,005 | -322,025 | +15,985,601 |
| 13 | CNR + LPSSNR | 816.863500 | 7.229104 | +809.634396 | 82,550.614625 | 275,301 | -162,030 | -352,025 | +16,135,451 |
| 14 | LOCNR + PSSNR | 899.859563 | 7.229104 | +892.630459 | 80,982.806292 | 276,876 | -332,030 | -1,691,994 | +22,805,265 |
| 15 | OCNR + LPSSNR | 1,034.486854 | 7.229104 | +1,027.257750 | 85,692.714208 | 329,719 | -192,030 | -652,025 | +19,345,451 |
| 16 | LOCNR + LPSSNR | 1,768.630042 | 7.229104 | +1,761.400938 | 121,974.715208 | 510,206 | -492,030 | -1,851,994 | +37,044,831 |
