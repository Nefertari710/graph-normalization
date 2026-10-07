# Provider Service Record Normalization Analysis

The dependency uses `50` update cases. Each case updates `1` Provider, is repeated `10` times, and its median latency is used. `Δ Update Time = Denormalized - Normalized`.

## Functional-dependency validation

| Functional dependency | Groups | Rows | Valid groups | Valid rows | Conflicting groups | Incomplete groups |
|---|---:|---:|---:|---:|---:|---:|
| rndrng_npi → (rndrng_prvdr_last_org_name, rndrng_prvdr_first_name, rndrng_prvdr_mi, rndrng_prvdr_ent_cd, rndrng_prvdr_st1, rndrng_prvdr_st2, rndrng_prvdr_city, rndrng_prvdr_state_abrvtn, rndrng_prvdr_zip5, rndrng_prvdr_cntry) | 1,207,473 | 9,781,673 | 1,207,473 | 9,781,673 | 0 | 0 |

Only complete, conflict-free NPI groups are normalized. Empty strings are treated as valid Provider values.

## Denormalized ProviderServiceRecord baseline

| ProviderServiceRecord nodes | Relationships in experiment scope | Property cells |
|---:|---:|---:|
| 9,781,673 | 0 | 273,886,844 |

## Independent FD normalization results (1 row)

| No. | Normalization strategy | Functional dependency | Update (Denormalized) Min (ms) | Update (Denormalized) Median (ms) | Update (Denormalized) Max (ms) | Update (Normalized) Median (ms) | Δ Update Time Median (ms) | Normalization Time (ms) | Kappa | ΔN | ΔE | ΔC |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | Provider | rndrng_npi → (rndrng_prvdr_last_org_name, rndrng_prvdr_first_name, rndrng_prvdr_mi, rndrng_prvdr_ent_cd, rndrng_prvdr_st1, rndrng_prvdr_st2, rndrng_prvdr_city, rndrng_prvdr_state_abrvtn, rndrng_prvdr_zip5, rndrng_prvdr_cntry) | 6.009 | 7.605 | 25.108 | 4.864 | +2.909 | 336,686.975 | 115,736 | +1,207,473 | +9,781,673 | -94,316,200 |

## Metric definitions

- **Normalization Time** includes copying ProviderServiceRecord nodes, creating Provider nodes and relationships, creating the Provider Node Key, removing repeated properties, and counting the resulting structure.
- **Kappa** is `ceil(Normalization Time / median update saving)`; it estimates how many logical one-Provider updates are needed to recover the one-off normalization cost.
- **ΔN, ΔE, ΔC** are Normalized minus Denormalized node, relationship, and property-cell counts.
