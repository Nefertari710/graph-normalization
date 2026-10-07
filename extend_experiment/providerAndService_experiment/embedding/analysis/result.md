- Provider (same NPI): N0 Denormalized → N1 Provider-normalized
- Service (same HCPCS): N0 Denormalized → N2 Service-normalized
- N0 contains only Record nodes; all projections keep one non-semantic self-loop per Record for topology-only methods.

### Mean ± sample SD

Values are mean ± sample SD across 10 random-seed runs; each run independently resamples record pairs and contributes its median.

| Case | Property Hash Denorm | Property Hash Norm | FastRP Denorm | FastRP Norm | Node2Vec Denorm | Node2Vec Norm | HashGNN Denorm | HashGNN Norm | GraphSAGE Denorm | GraphSAGE Norm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Provider (same NPI) | 0.685790 ± 0.002250 | 0.560069 ± 0.003885 | 0.334448 ± 0.022540 | 0.791528 ± 0.011936 | 0.551276 ± 0.001998 | 0.997063 ± 0.000060 | 0.604916 ± 0.020460 | 0.640522 ± 0.025868 | 0.873711 ± 0.040389 | 0.928683 ± 0.028043 |
| Service (same HCPCS) | 0.556800 ± 0.003813 | 0.454913 ± 0.005051 | 0.271557 ± 0.021796 | 0.774909 ± 0.010054 | 0.572207 ± 0.002500 | 0.499973 ± 0.003411 | 0.561067 ± 0.037459 | 0.615761 ± 0.020213 | 0.831173 ± 0.050175 | 0.933985 ± 0.030949 |

### Mean only

| Case | Property Hash Denorm | Property Hash Norm | FastRP Denorm | FastRP Norm | Node2Vec Denorm | Node2Vec Norm | HashGNN Denorm | HashGNN Norm | GraphSAGE Denorm | GraphSAGE Norm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Provider (same NPI) | 0.685790 | 0.560069 | 0.334448 | 0.791528 | 0.551276 | 0.997063 | 0.604916 | 0.640522 | 0.873711 | 0.928683 |
| Service (same HCPCS) | 0.556800 | 0.454913 | 0.271557 | 0.774909 | 0.572207 | 0.499973 | 0.561067 | 0.615761 | 0.831173 | 0.933985 |
