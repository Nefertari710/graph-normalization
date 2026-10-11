# Offshore Use Case Analysis

Input: [usecases_offshore.json](../usecases_offshore.json)
Experiment timestamp: 2026-10-11T03:25:41.696889+13:00
Cases: 120; FDs: 12.

N denotes the normalized representation, D the denormalized representation, and R the random-hub control.

| FD | Cases / seeds |
| --- | --- |
| address_country | 10 |
| service_provider | 10 |
| country | 10 |
| entity_country_reverse | 10 |
| intermediary_country | 10 |
| intermediary_country_reverse | 10 |
| intermediary_valid_until | 10 |
| intermediary_source | 10 |
| other_jurisdiction | 10 |
| other_jurisdiction_reverse | 10 |
| other_country | 10 |
| other_country_reverse | 10 |

## Table 5 Reproduction Check

Mean over seeds of the same-determinant pair similarity medians; reference values are the constants from Table 5.

| FD | Method | Measured N/D | Table 5 N/D |
| --- | --- | --- | --- |
| address_country | Property Hash | 0.478/0.559 | 0.478/0.559 |
| address_country | FastRP | 0.723/0.295 | 0.723/0.294 |
| address_country | Node2Vec | 0.998/0.511 | 0.998/0.511 |
| address_country | HashGNN | 0.646/0.586 | 0.639/0.593 |
| address_country | GraphSAGE | 0.978/0.974 | 0.975/0.959 |
| service_provider | Property Hash | 0.204/0.419 | 0.204/0.419 |
| service_provider | FastRP | 0.488/0.388 | 0.488/0.388 |
| service_provider | Node2Vec | 0.554/0.549 | 0.553/0.551 |
| service_provider | HashGNN | 0.561/0.563 | 0.558/0.552 |
| service_provider | GraphSAGE | 0.983/0.987 | 0.975/0.990 |
| other_jurisdiction | Property Hash | 0.689/0.744 | 0.689/0.744 |
| other_jurisdiction | FastRP | 0.544/0.424 | 0.543/0.421 |
| other_jurisdiction | Node2Vec | 0.998/0.401 | 0.998/0.397 |
| other_jurisdiction | HashGNN | 0.691/0.652 | 0.691/0.652 |
| other_jurisdiction | GraphSAGE | 0.962/0.946 | 0.967/0.939 |

## U1: Same-Determinant Pair Classification

ROC-AUC means over FD/seed cases. Wins count FDs with paired Wilcoxon p < 0.05 after Holm correction across FDs for each method; the median paired difference determines the winner.

| Method | AUC N | AUC D | AUC R | Wins N | Wins D |
| --- | --- | --- | --- | --- | --- |
| Property Hash | 0.761 | 0.945 | 0.761 | 0 | 12 |
| FastRP | 0.968 | 0.856 | 0.736 | 12 | 0 |
| Node2Vec | 0.835 | 0.533 | 0.506 | 9 | 0 |
| HashGNN | 0.858 | 0.832 | 0.715 | 7 | 2 |
| GraphSAGE | 0.740 | 0.769 | 0.697 | 1 | 4 |

## U2: Retrieval Among Sampled Nodes

Means over FD/seed cases. Relevant nodes share the query's determinant value; each cell shows N/D. Lift is relative to random retrieval.

| Method | R-precision N/D | MRR N/D | MAP N/D | Lift N/D |
| --- | --- | --- | --- | --- |
| Property Hash | 0.369/0.755 | 0.576/0.908 | 0.387/0.785 | 49.0/130.9 |
| FastRP | 0.792/0.491 | 0.922/0.691 | 0.831/0.518 | 163.5/58.1 |
| Node2Vec | 0.691/0.260 | 0.773/0.372 | 0.700/0.261 | 127.9/23.3 |
| HashGNN | 0.489/0.455 | 0.769/0.701 | 0.520/0.485 | 69.1/55.0 |
| GraphSAGE | 0.332/0.364 | 0.494/0.530 | 0.354/0.388 | 10.3/9.1 |

## U3: Feature Update Simulation

Updates change temporary embedding features; original business properties are retained. Logical values to change for a complete update are averaged over all cases, rather than counting actual database writes.

| Representation | Mean logical values to change |
| --- | --- |
| N | 23.9 |
| D | 381.1 |

Remaining U3 statistics use independently filtered cases for each method: the maximum absolute null-run drift across N/D and inside/outside must be at most 0.01. RF is a neighborhood count at the configured hop depth; it is not the measured number of changed vectors. Property Hash has no RF value (shown as a dash).

| Method | Mean RF N/D | Drift inside N/D | Drift outside N/D | D WGS(q=0.5)/WGS(q=1) |
| --- | --- | --- | --- | --- |
| Property Hash | — | -0.0000/0.0557 | -0.0000/-0.0000 | 0.9340 |
| FastRP | 19817/20617 | 0.0217/0.0152 | 0.0000/0.0003 | 0.9716 |
| HashGNN | 405/19627 | 0.6329/0.6335 | 0.6181/0.6246 | 0.9806 |
| GraphSAGE | 6475/6633 | 0.1972/0.2811 | 0.2226/0.3020 | 1.0004 |

### Re-embedding Stability

| Method | Retained / total cases | Mean RF ratio N/D | Maximum retained null-run drift |
| --- | --- | --- | --- |
| Property Hash | 120/120 | — | 1.70e-16 |
| FastRP | 120/120 | 0.899 | 4.69e-03 |
| HashGNN | 120/120 | 0.247 | 1.67e-16 |
| GraphSAGE | 103/120 | 0.936 | 2.22e-16 |

### Partial Updates of D

Each case first computes WGS_D(q)/WGS_D(1); the table and curves average those ratios over the retained cases. WGS averages pairwise cosine similarity within each selected group, then averages groups equally. The reference is complete updating (q=1), so every curve equals 1 there. The grey dashed line is this reference; all four curves are D. The requested fraction q is rounded up to a whole number of sampled copies within each group.

| Method | q=0.0 | q=0.25 | q=0.5 | q=0.75 | q=1.0 |
| --- | --- | --- | --- | --- | --- |
| Property Hash | 0.9800 | 0.9377 | 0.9340 | 0.9740 | 1.0000 |
| FastRP | 0.9958 | 0.9737 | 0.9716 | 0.9896 | 1.0000 |
| HashGNN | 1.0040 | 0.9889 | 0.9806 | 0.9936 | 1.0000 |
| GraphSAGE | 1.0064 | 0.9982 | 1.0004 | 1.0047 | 1.0000 |
