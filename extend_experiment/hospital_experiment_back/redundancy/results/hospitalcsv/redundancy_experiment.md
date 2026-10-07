# Hospital Record Normalization Analysis

Each dependency uses `50` update cases. Each case updates one hospital, is repeated `10` times, and its median latency is used. `Δ Update Time = Denormalized - Normalized`.

## Functional-dependency validation

| Functional dependency | Groups | Rows | Valid groups | Valid rows | Conflicting groups | Incomplete groups | Conflicting keys |
|---|---:|---:|---:|---:|---:|---:|---|
| facility_id → (facility_name, address, city, state, zip_code, county, telephone) | 4,658 | 138,084 | 4,658 | 138,084 | 0 | 0 | — |

Only complete, conflict-free groups are normalized.

## Denormalized HospitalRecord baseline

| HospitalRecord nodes | Relationships in experiment scope | Property cells |
|---:|---:|---:|
| 138,084 | 0 | 2,209,344 |

## Independent FD normalization results (1 row)

| No. | Normalization strategy | Functional dependency | Update (Denormalized) Min (ms) | Update (Denormalized) Median (ms) | Update (Denormalized) Max (ms) | Update (Normalized) Median (ms) | Δ Update Time Median (ms) | Normalization Time (ms) | Kappa | ΔN | ΔE | ΔC |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | Hospital | facility_id → (facility_name, address, city, state, zip_code, county, telephone) | 3.043 | 4.118 | 7.030 | 4.009 | +0.079 | 5,040.100 | 63,415 | +4,658 | +138,084 | -1,067,408 |

## Metric definitions

- **Normalization Time** includes copying HospitalRecord nodes, creating Hospital nodes and relationships, creating the Hospital Node Key, removing repeated properties, and counting the resulting structure.
- **Kappa** is `ceil(Normalization Time / median update saving)`; it estimates how many logical one-hospital updates are needed to recover the one-off normalization cost.
- **ΔN, ΔE, ΔC** are Normalized minus Denormalized node, relationship, and property-cell counts.
