# R Redundancy Update Case Study

- Chain: `L → PS → S → N → R`
- Database: `tpch-sf-001`
- Selected sample: group `8` of `10` eligible paired R samples (system random)
- Update key: `r_regionkey = 2`; property: `r_comment`
- Completed target strategies: `4/4`

Every row uses this single paired sample; no minimum, maximum, or median is calculated. `Δ Update Time = De-normalized − Normalized`, so a positive value is time saved by normalization.

`R Copies Updated` is `touched_nodes`. `ΔN`, `ΔE`, and `ΔC` are De-normalized − Normalized.

| No. | De-normalization Strategy | Update on De-normalized Schema (ms) | Update on Normalized Schema (ms) | Δ Update Time / Normalization Save (ms) | Normalization Time (ms) | R Copies Updated (Normalized → De-normalized) | ΔN | ΔE | ΔC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NR | 4.003792 | 4.042667 | -0.038875 | 216.831875 | 1 → 5 | -5 | -25 | +35 |
| 2 | SNR | 5.077292 | 4.042667 | +1.034625 | 353.462042 | 1 → 32 | -5 | -125 | +535 |
| 3 | PSSNR | 12.959542 | 4.042667 | +8.916875 | 1,066.894583 | 1 → 2,165 | -105 | -8,125 | +87,335 |
| 4 | LPSSNR | 249.918042 | 4.042667 | +245.875375 | 4,581.371791 | 1 → 16,469 | -8,105 | -16,125 | +801,785 |
