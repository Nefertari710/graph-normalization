# Offshore Reference-FD Updates

Source: [updates_reference_fd.json](<../updates_reference_fd.json>).

FD: `{registered_address} -> {sourceID}`.

Sampled groups — Officer: 50; Intermediary: 50; Entity: 50. Repetitions per group: 5.

| Type | FD (determinant) | Groups | Redund. | B_N (s) | Median U_D (ms) | Median U_N (ms) | Largest copies | Largest U_D (ms) | Largest U_N (ms) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Officer | `registered_address` | 50,774 | 128.5K | 2.1 | 4.1 | 4.1 | 9 | 5.0 | 5.0 |
| Intermediary | `registered_address` | 389 | 0.5K | 0.1 | 4.2 | 4.0 | 14 | 4.3 | 3.8 |
| Entity | `registered_address` | 17,738 | 223.4K | 0.3 | 4.2 | 4.5 | 494 | 5.0 | 3.9 |

B_N = normalization cost, including index setup. D = denormalized; N = normalized. Median U_D and U_N summarize the sampled groups' median times. Largest means the sampled group with the most copies, not the largest group in the full graph. K = 1,000; display precision matches the experiment's LaTeX rows.

Counts refer to eligible groups and their logical redundant copies. The D update query does not repeat the eligibility filters. Empty samples appear as —.
