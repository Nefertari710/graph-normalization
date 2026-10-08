# Northwind Functional-Dependency Normalization Experiment Analysis Report

## Abstract

This report analyzes 10 directly related table pairs in Northwind, with
**Product → Supplier** as the focus case for Steps 1–4, where
Supplier information is folded into each
Product tuple. All experiments run on
shadow projections tagged with `run_id`, while the original Northwind graph remains read-only. For every case,
the original graph's SHA-256 is identical before execution, during execution, and after cleanup.

The Product → Supplier folded join has \(J=77\)
Product
tuples and \(D=29\) distinct
Supplier keys participating in the join. Normalization can therefore eliminate
**\(J-D=48\) Supplier
tuple copies, representing
62.338% of J**. Further calculations give
**528 theoretical redundant dependent-property
slots**, **432 redundant non-NULL values**,
and **5,985 bytes of redundant value-payload
proxy**.

When updating `Supplier.Phone`, the folded
representation writes 77 property copies, whereas the normalized representation writes only
29 distinct Supplier nodes, giving write amplification of
**2.655×**. The client wall-clock medians are
**5.048 ms** and
**4.012 ms**, respectively, with a time ratio of
**1.258×**. The normalization median in Step 3
is **8.786 ms**. The median-based
break-even estimate is **8.48 batches, or approximately the 9th batch when counting complete batches**.

![Steps 1–4 summary](figures/08_focus_step_summary.png)

## 1. Research Questions

- **RQ1:** How much redundancy do functional dependencies cause, and how much can normalization save?
- **RQ2:** What is the one-time cost of decomposing a folded MV into a normalized MV?
- **RQ3:** How many physical writes and how much time does the same set of logical updates require
  in the folded and normalized representations?
- **RQ4:** How many batches of the same update workload are needed for update savings to recover
  the normalization cost?

## 2. Experimental Method

### Step 0: Create the folded baseline (untimed)

The program creates a run-scoped MV node for each join tuple, copies the referencing
table's properties and the referenced table's non-key properties into that node, and copies
the required boundary relationships using complete topology. This baseline
is the report's "denormalized/original representation"; the actual original graph is never
deleted or modified.

### Step 1: Calculate redundancy

Let \(J\) be the number of folded join tuples and \(D\) the number of distinct
referenced keys participating in the join:

\[
R_{tuple}=J-D
\]

For a referenced table with \(c_B\) columns and a key of length \(k_B\):

\[
R_{slot}=(J-D)(c_B-k_B)
\]

Theoretical slots include NULL positions; the non-NULL metric counts only duplicate
property values that actually exist. The payload metric counts UTF-8 bytes for strings and
the UTF-8 bytes of `str(value)` for other values. It is only a **proxy for value-content size, not Neo4j's
actual disk usage**. Topology relationship redundancy is a separate
relationship-level metric and cannot be added directly to property counts.

### Steps 2 and 4: Apply the same logical updates

Each case selects a non-key property from the referenced table and performs deterministic
value rotation over the active domain of the complete referenced relation. Both representations use
exactly the same key-to-value mapping:

- Step 2 updates all \(J\) duplicated positions in the folded MV;
- Step 4 updates \(D\) distinct referenced nodes in the normalized MV.

A measured run is one autocommit batch, timed through result consumption and
commit completion; restoration and validation are untimed. Each phase has
20 measured runs, and the report primarily uses the client wall-clock
median.

### Step 3: Normalization

The program recreates normalized referencing nodes from folded nodes, creates
referenced nodes after applying DISTINCT to referenced keys, adds explicit join
relationships, and migrates and deduplicates boundary relationships. Timing includes the explicit
transaction, data decomposition, and commit, but excludes DDL, validation,
refolding, and cleanup.

## 3. Experimental Results

### 3.1 RQ1: Redundancy savings

For this case, \(J-D=77-
29=48\).
Supplier has 11
dependent properties,
so the theoretical redundant slots are
\(48\times
11=528\).

The Supplier fanout histogram is
`{1: 3, 2: 10, 3: 12, 4: 2, 5: 2}`. A Supplier with fanout \(f\)
contributes \(f-1\) redundant tuple copies.

![Supplier fanout](figures/01_focus_fanout.png)

![Redundancy across all table pairs](figures/02_redundancy_by_pair.png)

| Table pair | Theoretical redundant property slots | Redundant non-NULL values | Redundant payload proxy (bytes) | Redundant topology relationships |
|---|---:|---:|---:|---:|
| Order → Customer | 7,410 | 6,712 (89.24%) | 79,542 (89.12%) | 0 |
| OrderDetail → Order | 17,225 | 16,345 (61.48%) | 181,246 (61.31%) | 3,975 |
| OrderDetail → Product | 18,702 | 18,702 (96.43%) | 99,116 (96.42%) | 4,156 |
| Product → Category | 207 | 207 (89.61%) | 20,530 (89.67%) | 0 |
| Product → Supplier | 528 | 432 (62.52%) | 5,985 (63.19%) | 0 |
| Order → Employee | 13,957 | 13,642 (98.93%) | 529,822 (98.92%) | 3,911 |
| Order → Shipper | 1,654 | 1,654 (99.64%) | 23,664 (99.64%) | 0 |
| EmployeeTerritory → Employee | 680 | 649 (81.43%) | 25,941 (81.78%) | 3,130 |
| EmployeeTerritory → Territory | 0 | 0 (0.00%) | 0 (0.00%) | 0 |
| Territory → Region | 49 | 49 (92.45%) | 2,156 (93.17%) | 0 |

Product → Supplier has topology relationship redundancy of
0; both the folded and normalized representations have
2,232 boundary relationships.
Relationship redundancy of 0 does not imply property redundancy of 0; the two metrics measure different resources.

### 3.2 RQ3: Update costs

One batch in this case contains 29 logical
Supplier updates.
The folded structure updates 77 property positions, while the normalized structure
updates 29 nodes, so:

\[
A_{write}=J/D=2.655
\]

The folded median is 5.048 ms,
and the normalized median is 4.012 ms.
The folded median is 25.825% higher; relative to the folded
baseline, normalization reduces the median by
20.524%.

![Folded and normalized updates](figures/03_update_comparison.png)

![Product → Supplier update raw samples](figures/04_focus_update_samples.png)

The folded focus samples have mean=9.282 ms,
median=5.048 ms, and max=
48.554 ms. Clearly slow samples raise the mean, so
this report emphasizes the median, IQR, and raw data points, rather than using mean bars as the main evidence.

### 3.3 RQ2: Normalization cost

77 folded nodes are decomposed into
77 normalized
Product nodes and
29 distinct Supplier
nodes, with
77 join relationships created.
The normalization median is 8.786 ms.

Thus, \(J-D\) measures how many **referenced property copies** are removed; it cannot be interpreted as
a net reduction of \(J-D\) physical nodes in the graph. Normalization adds distinct referenced
nodes and explicit join edges.

![Normalization effort](figures/05_normalization_effort.png)

### 3.4 Write amplification and time ratio

\(J/D\) determines the number of physical writes, but elapsed time also includes fixed or variable costs such as transaction startup,
query execution, index/cache access, result consumption, and commit. Therefore,
write amplification does not translate proportionally into time amplification.

![Write amplification and time ratio](figures/06_write_amplification_vs_speedup.png)

### 3.5 RQ4: Break-even

Break-even is defined only when \(J-D>0\) and the folded median is strictly greater than the normalized median:

\[
N_{break-even}=
\frac{T_{normalization,median}}
{T_{folded-update,median}-
  T_{normalized-update,median}}
\]

For Product → Supplier, the estimate is 8.786/(5.048-4.012)=8.480, so the cost is recovered at approximately the 9th complete batch.

![Break-even](figures/07_normalization_break_even.png)

EmployeeTerritory → Employee has redundancy, but its normalized median is slightly slower;
EmployeeTerritory → Territory has \(J=D=49\) and no tuple redundancy. For both cases,
break-even is undefined, rather than 0 or infinity. The Product → Category
estimate is approximately 1,940.6, but the median difference is only 0.004 ms, on the scale of microbenchmark noise,
so it should not be treated as a precise prediction.

## 4. Complete Results

| Table pair | J | D | J−D | Tuple redundancy | Folded update median (ms) | Normalized update median (ms) | Time ratio | Normalization median (ms) | Break-even batches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Order → Customer | 830 | 89 | 741 | 89.28% | 7.011 | 5.884 | 1.191× | 44.552 | 39.54 |
| OrderDetail → Order | 2,155 | 830 | 1,325 | 61.48% | 12.026 | 10.023 | 1.200× | 129.365 | 64.58 |
| OrderDetail → Product | 2,155 | 77 | 2,078 | 96.43% | 19.981 | 5.006 | 3.991× | 84.209 | 5.62 |
| Product → Category | 77 | 8 | 69 | 89.61% | 3.998 | 3.994 | 1.001× | 7.721 | 1940.56 |
| Product → Supplier | 77 | 29 | 48 | 62.34% | 5.048 | 4.012 | 1.258× | 8.786 | 8.48 |
| Order → Employee | 830 | 9 | 821 | 98.92% | 6.024 | 3.991 | 1.509× | 66.982 | 32.95 |
| Order → Shipper | 830 | 3 | 827 | 99.64% | 5.029 | 3.955 | 1.271× | 34.066 | 31.74 |
| EmployeeTerritory → Employee | 49 | 9 | 40 | 81.63% | 3.964 | 4.012 | 0.988× | 17.370 | N/A |
| EmployeeTerritory → Territory | 49 | 49 | 0 | 0.00% | 3.996 | 4.876 | 0.820× | 7.521 | N/A |
| Territory → Region | 53 | 4 | 49 | 92.45% | 4.083 | 3.943 | 1.035× | 6.087 | 43.49 |

Among the 10 cases,
9 have tuple redundancy, and
8 have a lower normalized median.
The strongest runtime result is OrderDetail → Product, with a folded/normalized median
ratio=3.991× and an estimated break-even of
5.62 batches.

The descriptive totals for the ten separate workloads are J=7,105,
D=1,107, and J−D=
5,998, with weighted tuple redundancy=
84.419%. These cases share or overlap source tables,
so these totals **do not equal** the net database savings from materializing all ten MVs simultaneously.

## 5. Discussion

1. When J>D, normalization eliminates duplicate values caused by functional dependencies; tuple, slot,
   non-NULL, and payload metrics describe this redundancy at different granularities.
2. Fewer physical writes do not guarantee a proportional reduction in wall-clock time. 8/10 cases have
   a lower normalized median, but fixed overhead dominates some small workloads.
3. OrderDetail → Product provides the strongest evidence of update performance gains:
   a 3.991× time ratio and median-based cost recovery after approximately
   6 batches.
4. "Original graph update" should be described precisely as a folded/denormalized shadow
   update; the actual source graph remains unchanged throughout the experiment.

## 6. Validity Limitations

- The data comes from one local warm-cache suite (`Neo4j/2026.06.0; Python 3.13.14; Neo4j driver 6.2.0`),
  without clearing the server page/query caches or randomly interleaving phases.
- The 20 timings are repeated transactions on the same database, rather than 20 independent
  database deployments. Bootstrap intervals describe only the resampling
  variability of the current sample sequence and are not used to infer population-level significance.
- Cases differ in the updated property, type, active-domain size, batch size, and
  topology. Absolute milliseconds across cases provide descriptive context only; the within-case
  comparison of folded and normalized representations is controlled.
- Update restoration/validation is untimed; normalization excludes DDL,
  validation, refolding, and cleanup. Break-even inherits these timing boundaries
  and is not a prediction of the full cost of a production migration.
- The payload proxy excludes Neo4j record headers, dynamic stores, indexes,
  labels, relationships, transaction logs, compression, and page layout.
  Actual disk usage requires a separate store-level experiment.
- This experiment does not measure read queries, concurrency, locking, or long-term effects.

## 7. Conclusion

The Product → Supplier case directly answers the three original tasks:

- **Redundancy saved by normalization:** 48
  Supplier tuple copies,
  528 theoretical dependent-property slots,
  432 redundant non-NULL values, and
  5,985 bytes of value-payload proxy;
- **Normalization time:** median within the defined data-rewrite scope=
  8.786 ms;
- **Update differences:** 77 versus
  29 property writes, folded median=
  5.048 ms, normalized median=
  4.012 ms, and folded/normalized=
  1.258×.

Based on the medians in this run, break-even is 8.48 batches, or approximately the 9th complete batch. The full suite also
shows that high FD/write redundancy
can produce clear update gains, while small workloads require careful interpretation using raw samples,
outliers, and fixed overhead.

## Appendix: Reproducibility

- Input file: `northwind_fd_experiment_results.json`
- Input SHA-256: `2d88fdf3d14d34f576c1ea26b67e19f9dfb290c814a430ffa8e74be6a4adc031`
- Report schema: v6
- Join fingerprint schema: v2
- Database: `northwind`
- Topology mode: `complete`
- The original graph fingerprints are identical before, during, and after the experiment for all cases
- All shadow graphs and temporary schemas are cleaned up at the end of each case

`case_summary.csv`, `timing_samples.csv`, and
`analysis_summary.json` contain the underlying data used by the report and figures; each figure is
exported as both a 300 dpi PNG and a scalable SVG.
