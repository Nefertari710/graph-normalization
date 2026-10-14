# Reference FD Normalization Experiment Report

Source: [reference_fd_experiment.py](../reference_fd_experiment.py).

Random orders per workload: 20.

The FDs are predefined and checked on the retained complete records. Offshore records also require exactly one registered_address edge to a valid Address node.

## Input Workloads

| Workload | Records | Features | References | FDs | FDs involving references |
| --- | --- | --- | --- | --- | --- |
| Northwind OrderInfo | 2027 | 16 | customer, employee, product, shipper | 4 | 2 |
| Offshore Intermediary | 10060 | 6 | registered_address | 5 | 1 |
| Offshore Officer | 392112 | 6 | registered_address | 1 | 1 |
| Offshore Entity | 102347 | 9 | registered_address | 5 | 1 |

## Normalization Results

Decompositions, blocks, and key translations list the distinct counts observed across random orders. Z-locality types are the union observed across those orders. Matching signatures compare feature sets and new-reference counts; they do not compare keys, FDs, or reference targets.

| Workload | Decompositions | Blocked FDs | Matching signatures | Z-locality types | Key translations with rho | Maximum time (ms) |
| --- | --- | --- | --- | --- | --- | --- |
| Northwind OrderInfo | [4] | [0] | Yes | L1, L2, L3 | [2] | 0.30 |
| Offshore Intermediary | [3] | [0] | Yes | L1, L3 | [0] | 0.11 |
| Offshore Officer | [1] | [0] | Yes | L3 | [0] | 0.05 |
| Offshore Entity | [2] | [2] | Yes | L1, L3 | [0] | 0.11 |

Timings cover schema normalization only, excluding CSV loading, FD checks, and storage estimation.

## Estimated Stored Occurrences

These are logical feature-occurrence counts, not bytes or measured database writes. Before = records × features. After uses distinct flattened-key values for each resulting type, including new references. Storage counts and result types use the final tested order.

| Workload | Before | After | Eliminated | Reduction (%) |
| --- | --- | --- | --- | --- |
| Northwind OrderInfo | 32432 | 16937 | 15495 | 47.78 |
| Offshore Intermediary | 60360 | 39528 | 20832 | 34.51 |
| Offshore Officer | 2352672 | 2229264 | 123408 | 5.25 |
| Offshore Entity | 921123 | 717541 | 203582 | 22.10 |

## Result Types

A type merged into a referenced target reuses the original reference; its storage estimate omits one reference feature. The features column still lists the complete schema features.

Blocked-FD details recheck candidates from the original closed cover when the final order reports blocks; they are not a complete list of remaining FDs in the final schema.

### Northwind OrderInfo

| Type | Distinct keys | Key | Flattened key | Features | Merged into referenced target |
| --- | --- | --- | --- | --- | --- |
| Northwind OrderInfo_Z4 | 69 | shipCity | shipCity | shipCity, shipCountry | No |
| Northwind OrderInfo_Z2 | 84 | shipPostalCode | shipPostalCode | ref3, shipPostalCode | No |
| Northwind OrderInfo_Z3 | 88 | customer | customer | customer, ref1, shipAddress | Yes |
| Northwind OrderInfo_Z1 | 790 | employee, orderDate, ref2 | customer, employee, orderDate | employee, freight, orderDate, ref2, requiredDate, shipName, shippedDate, shipper | No |
| Northwind OrderInfo | 2027 | product, ref0 | customer, employee, orderDate, product | discount, product, quantity, ref0, unitPrice | No |

Recorded blocked-FD details:

No blocked-FD details recorded.

### Offshore Intermediary

| Type | Distinct keys | Key | Flattened key | Features | Merged into referenced target |
| --- | --- | --- | --- | --- | --- |
| Offshore Intermediary_Z1 | 116 | country_codes | country_codes | countries, country_codes | No |
| Offshore Intermediary_Z3 | 6 | sourceID | sourceID | sourceID, valid_until | No |
| Offshore Intermediary_Z2 | 9582 | registered_address | registered_address | ref0, ref2, registered_address | Yes |
| Offshore Intermediary | 10060 | node_id | node_id | node_id, ref1 | No |

Recorded blocked-FD details:

No blocked-FD details recorded.

### Offshore Officer

| Type | Distinct keys | Key | Flattened key | Features | Merged into referenced target |
| --- | --- | --- | --- | --- | --- |
| Offshore Officer_Z1 | 268704 | registered_address | registered_address | registered_address, sourceID | Yes |
| Offshore Officer | 392112 | node_id | node_id | countries, country_codes, node_id, ref0, valid_until | No |

Recorded blocked-FD details:

No blocked-FD details recorded.

### Offshore Entity

| Type | Distinct keys | Key | Flattened key | Features | Merged into referenced target |
| --- | --- | --- | --- | --- | --- |
| Offshore Entity_Z1 | 62 | jurisdiction | jurisdiction | jurisdiction, jurisdiction_description | No |
| Offshore Entity_Z2 | 494 | countries | countries | countries, country_codes | No |
| Offshore Entity | 102347 | node_id | node_id | node_id, ref1, ref3, registered_address, service_provider, sourceID, valid_until | No |

Recorded blocked-FD details:

| Determinant | Dependent features |
| --- | --- |
| registered_address | sourceID |
| service_provider | sourceID, valid_until |
