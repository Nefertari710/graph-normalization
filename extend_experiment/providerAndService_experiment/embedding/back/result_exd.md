## Sequential normalization

`N0 Denormalized --FD1--> N1 Service-normalized --FD2--> N2 Service+Provider-normalized`

- `FD1`: `hcpcs_cd → (hcpcs_desc, hcpcs_drug_ind)`
- `FD2`: `rndrng_npi → Provider attributes`

### Mean ± sample SD

Values are summarized across 10 random-seed runs; each run contributes its median over the fixed same-NPI record pairs.

Both rows use exactly the same record pairs. N1 is computed once per seed and is shared by the two comparisons.
Within each row, Denormal is the less-normalized baseline and Normal is the next normalized state.

| Step | Comparison | Property Hash Denormal | Property Hash Normal | FastRP Denormal | FastRP Normal | Node2Vec Denormal | Node2Vec Normal | HashGNN Denormal | HashGNN Normal | GraphSAGE Denormal | GraphSAGE Normal |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `FD1 Service` | N0 Denormalized → N1 Service-normalized | 0.676148 ± 0.000000 | 0.759737 ± 0.000000 | 0.407849 ± 0.024395 | 0.470536 ± 0.029455 | 0.581573 ± 0.006471 | 0.522708 ± 0.004899 | 0.845974 ± 0.017822 | 0.702759 ± 0.017004 | 0.933099 ± 0.022044 | 0.961982 ± 0.009215 |
| `FD2 Provider` | N1 Service-normalized → N2 Service+Provider-normalized | 0.759737 ± 0.000000 | 0.640077 ± 0.000000 | 0.470536 ± 0.029455 | 0.643562 ± 0.024775 | 0.522708 ± 0.004899 | 0.407049 ± 0.003137 | 0.702759 ± 0.017004 | 0.719682 ± 0.030111 | 0.961982 ± 0.009215 | 0.985074 ± 0.008077 |

### Mean only

| Step | Comparison | Property Hash Denormal | Property Hash Normal | FastRP Denormal | FastRP Normal | Node2Vec Denormal | Node2Vec Normal | HashGNN Denormal | HashGNN Normal | GraphSAGE Denormal | GraphSAGE Normal |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `FD1 Service` | N0 Denormalized → N1 Service-normalized | 0.676148 | 0.759737 | 0.407849 | 0.470536 | 0.581573 | 0.522708 | 0.845974 | 0.702759 | 0.933099 | 0.961982 |
| `FD2 Provider` | N1 Service-normalized → N2 Service+Provider-normalized | 0.759737 | 0.640077 | 0.470536 | 0.643562 | 0.522708 | 0.407049 | 0.702759 | 0.719682 | 0.961982 | 0.985074 |
