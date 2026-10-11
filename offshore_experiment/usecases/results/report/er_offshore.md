# Offshore Entity Resolution (U4)

Source: [er_offshore.json](../er_offshore.json).

N = normalized representation; D = denormalized representation. Metric cells show N/D. Unavailable metrics appear as `nan`.

## Samples per FD

Sample counts are means over seeds, except usable duplicate pairs, which are taken from the first case for each FD.

| FD | Seeds | Usable duplicate pairs | Positive pairs | Positives sharing determinant | Entities |
| --- | ---: | ---: | ---: | ---: | ---: |
| service_provider | 10 | 3146 | 500 | 454 | 3977 |
| country | 10 | 4148 | 500 | 157 | 3965 |
| entity_country_reverse | 10 | 4160 | 500 | 157 | 3968 |

## Metrics: all FDs

Mean over seeds and FDs.

| Method | auc_random | auc_hard | mrr | auc_hard_same_det | auc_hard_diff_det |
| --- | ---: | ---: | ---: | ---: | ---: |
| Property Hash | 0.984/0.973 | 0.927/0.875 | 0.916/0.872 | 0.984/0.984 | 0.886/0.686 |
| FastRP | 0.798/0.776 | 0.449/0.601 | 0.497/0.549 | 0.829/0.792 | 0.091/0.433 |
| Node2Vec | 0.559/0.556 | 0.522/0.521 | 0.429/0.431 | 0.719/0.711 | 0.409/0.414 |
| HashGNN | 0.816/0.789 | 0.660/0.675 | 0.625/0.620 | 0.879/0.850 | 0.497/0.541 |
| GraphSAGE | 0.758/0.761 | 0.588/0.603 | 0.527/0.539 | 0.729/0.719 | 0.386/0.407 |

## Metrics: service_provider

Mean over seeds.

| Method | auc_random | auc_hard | mrr | auc_hard_same_det | auc_hard_diff_det |
| --- | ---: | ---: | ---: | ---: | ---: |
| Property Hash | 0.985/0.963 | 0.969/0.932 | 0.944/0.920 | 0.974/0.976 | 0.921/0.494 |
| FastRP | 0.781/0.702 | 0.636/0.550 | 0.597/0.494 | 0.694/0.575 | 0.056/0.298 |
| Node2Vec | 0.466/0.459 | 0.465/0.456 | 0.352/0.350 | 0.472/0.462 | 0.391/0.391 |
| HashGNN | 0.792/0.746 | 0.733/0.683 | 0.650/0.585 | 0.752/0.700 | 0.537/0.515 |
| GraphSAGE | 0.682/0.673 | 0.558/0.489 | 0.470/0.446 | 0.603/0.535 | 0.125/0.040 |

## Metrics: country

Mean over seeds.

| Method | auc_random | auc_hard | mrr | auc_hard_same_det | auc_hard_diff_det |
| --- | ---: | ---: | ---: | ---: | ---: |
| Property Hash | 0.983/0.979 | 0.907/0.848 | 0.903/0.848 | 0.989/0.988 | 0.869/0.784 |
| FastRP | 0.804/0.817 | 0.358/0.642 | 0.447/0.582 | 0.896/0.904 | 0.111/0.522 |
| Node2Vec | 0.603/0.610 | 0.546/0.560 | 0.466/0.474 | 0.840/0.834 | 0.411/0.436 |
| HashGNN | 0.838/0.807 | 0.636/0.668 | 0.626/0.631 | 0.943/0.923 | 0.495/0.551 |
| GraphSAGE | 0.795/0.798 | 0.604/0.647 | 0.552/0.579 | 0.817/0.802 | 0.509/0.576 |

## Metrics: entity_country_reverse

Mean over seeds.

| Method | auc_random | auc_hard | mrr | auc_hard_same_det | auc_hard_diff_det |
| --- | ---: | ---: | ---: | ---: | ---: |
| Property Hash | 0.982/0.978 | 0.906/0.844 | 0.902/0.847 | 0.988/0.986 | 0.868/0.779 |
| FastRP | 0.809/0.810 | 0.354/0.610 | 0.446/0.571 | 0.897/0.896 | 0.105/0.479 |
| Node2Vec | 0.608/0.599 | 0.556/0.547 | 0.471/0.467 | 0.844/0.836 | 0.424/0.415 |
| HashGNN | 0.818/0.814 | 0.611/0.674 | 0.600/0.643 | 0.944/0.928 | 0.459/0.557 |
| GraphSAGE | 0.796/0.812 | 0.602/0.674 | 0.558/0.592 | 0.767/0.819 | 0.526/0.605 |

## Paired Wilcoxon tests

N versus D for `auc_hard`, paired over seeds within each FD. Only comparisons with more than one seed and differences that are not all close to zero are tested. Median differences are N minus D.

| FD | Method | Median difference (N - D) | p-value |
| --- | --- | ---: | ---: |
| service_provider | Property Hash | +0.037 | 0.0020 |
| service_provider | FastRP | +0.081 | 0.0020 |
| service_provider | Node2Vec | +0.008 | 0.3223 |
| service_provider | HashGNN | +0.054 | 0.0098 |
| service_provider | GraphSAGE | +0.030 | 0.0371 |
| country | Property Hash | +0.059 | 0.0020 |
| country | FastRP | -0.284 | 0.0020 |
| country | Node2Vec | -0.019 | 0.3223 |
| country | HashGNN | -0.039 | 0.0273 |
| country | GraphSAGE | -0.064 | 0.1309 |
| entity_country_reverse | Property Hash | +0.061 | 0.0020 |
| entity_country_reverse | FastRP | -0.260 | 0.0020 |
| entity_country_reverse | Node2Vec | +0.006 | 0.4316 |
| entity_country_reverse | HashGNN | -0.064 | 0.0020 |
| entity_country_reverse | GraphSAGE | -0.063 | 0.1602 |

## Paper Table: U4 Entity Resolution

U4: Entity resolution, AUC against hard negatives, mean over 10 seeds. Overall includes all duplicate pairs; Disagree includes duplicate pairs with different determinant values. N = normalized; D = denormalized. Bold marks the better overall AUC when the paired Wilcoxon test over seeds has p < 0.05.

<table>
<thead>
<tr><th rowspan="3">Method</th><th colspan="4">service_provider (91% agree)</th><th colspan="4">country_codes (31% agree)</th></tr>
<tr><th colspan="2">Overall</th><th colspan="2">Disagree</th><th colspan="2">Overall</th><th colspan="2">Disagree</th></tr>
<tr><th>N</th><th>D</th><th>N</th><th>D</th><th>N</th><th>D</th><th>N</th><th>D</th></tr>
</thead>
<tbody>
<tr><td>Property Hash</td><td><strong>0.969</strong></td><td>0.932</td><td>0.921</td><td>0.494</td><td><strong>0.907</strong></td><td>0.848</td><td>0.869</td><td>0.784</td></tr>
<tr><td>FastRP</td><td><strong>0.636</strong></td><td>0.550</td><td>0.056</td><td>0.298</td><td>0.358</td><td><strong>0.642</strong></td><td>0.111</td><td>0.522</td></tr>
<tr><td>Node2Vec</td><td>0.465</td><td>0.456</td><td>0.391</td><td>0.391</td><td>0.546</td><td>0.560</td><td>0.411</td><td>0.436</td></tr>
<tr><td>HashGNN</td><td><strong>0.733</strong></td><td>0.683</td><td>0.537</td><td>0.515</td><td>0.636</td><td><strong>0.668</strong></td><td>0.495</td><td>0.551</td></tr>
<tr><td>GraphSAGE</td><td><strong>0.558</strong></td><td>0.489</td><td>0.125</td><td>0.040</td><td>0.604</td><td>0.647</td><td>0.509</td><td>0.576</td></tr>
</tbody>
</table>
