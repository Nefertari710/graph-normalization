# Schema-level Normalize Experiment

Each workload is evaluated in 20 randomized orders of eligible FD choices. The random generator starts with seed 1 and is shared by the existing experiment passes.

Order-independent signature means that all sampled orders yield the same multiset of original feature sets and counts of new references. This is a finite signature comparison; keys, FDs and reference targets are not part of the signature. Decomposition and block columns list the observed counts across these runs.

Maximum runtime measures Normalize only, including its closed-cover construction. Workload construction, input reads and report writing are outside the timing.

## TPC-H

All subsets of the eight references are evaluated. Recovered checks only whether one additional normalization per variant returns the expected number of node types (initial types plus copied-path FDs); it does not check full schema equivalence.

| Metric | Value |
| --- | --- |
| Variants | 256 |
| Variants with blocked candidates | 0 |
| Variants with differing sampled signatures | 0 |
| Recovered variants (node-type count check) | 256 |
| Maximum decompositions | 21 |
| Maximum Normalize time (ms) | 4.563 |

## Offshore

| Workload | Node types | Declared FDs | Decompositions | Blocked candidates | Order-independent signature | Max Normalize time (ms) |
| --- | --- | --- | --- | --- | --- | --- |
| offshore | 4 | 12 | [7] | [0] | Yes | 0.291 |

## DBpedia

| Metric | Value |
| --- | --- |
| Classes | 17 |
| Declared FDs | 27 |
| Blocking classes | Drug |

The per-class table uses the existing first pass. The blocking-class summary uses the existing second pass, with another set of randomized orders.

| Workload | Node types | Declared FDs | Decompositions | Blocked candidates | Order-independent signature | Max Normalize time (ms) |
| --- | --- | --- | --- | --- | --- | --- |
| dbpedia Actor | 1 | 2 | [2] | [0] | Yes | 0.035 |
| dbpedia Bridge | 1 | 1 | [1] | [0] | Yes | 0.017 |
| dbpedia Dam | 1 | 1 | [1] | [0] | Yes | 0.015 |
| dbpedia Drug | 1 | 6 | [1] | [2] | Yes | 0.060 |
| dbpedia GovernmentAgency | 1 | 1 | [1] | [0] | Yes | 0.012 |
| dbpedia Person | 1 | 2 | [2] | [0] | Yes | 0.021 |
| dbpedia Philosopher | 1 | 2 | [2] | [0] | Yes | 0.019 |
| dbpedia Play | 1 | 1 | [1] | [0] | Yes | 0.010 |
| dbpedia PoliticalParty | 1 | 1 | [1] | [0] | Yes | 0.009 |
| dbpedia PopulatedPlace | 1 | 1 | [1] | [0] | Yes | 0.010 |
| dbpedia PublicTransitSystem | 1 | 1 | [1] | [0] | Yes | 0.009 |
| dbpedia RailwayLine | 1 | 1 | [1] | [0] | Yes | 0.009 |
| dbpedia RailwayStation | 1 | 1 | [1] | [0] | Yes | 0.009 |
| dbpedia Rocket | 1 | 1 | [1] | [0] | Yes | 0.010 |
| dbpedia Saint | 1 | 2 | [2] | [0] | Yes | 0.023 |
| dbpedia SnookerWorldRanking | 1 | 2 | [1] | [0] | Yes | 0.014 |
| dbpedia Station | 1 | 1 | [1] | [0] | Yes | 0.009 |

## Running examples

| Workload | Node types | Declared FDs | Decompositions | Blocked candidates | Order-independent signature | Max Normalize time (ms) |
| --- | --- | --- | --- | --- | --- | --- |
| running phi | 1 | 1 | [1] | [0] | Yes | 0.046 |
| running phi1,phi2 | 1 | 2 | [1] | [1] | Yes | 0.047 |
