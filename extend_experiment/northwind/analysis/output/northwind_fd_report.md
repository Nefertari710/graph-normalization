# Northwind Functional-Dependency Normalization Experiment

## Abstract

This report evaluates the storage redundancy, update cost, and
normalization effort of direct related-table pairs in the Northwind
graph. The primary case is **Product → Supplier**, corresponding to
folding Supplier information into
Product rows. All experiments use
run-scoped shadow projections; the source Northwind graph is read-only
and was fingerprint-verified unchanged.

In the focus case, the folded join contains \(J=77\)
Product tuples and
\(D=29\) distinct participating
Supplier keys. Normalization therefore removes
**48 redundant Supplier tuple copies
(62.338% of J)**. These copies account for
**528 theoretical dependent-property
slots**, **432 duplicate non-NULL
values**, and **5,985 bytes of duplicate
value-payload proxy**.

Updating `Supplier.Phone` required
77 property writes in the folded projection and
29 in the normalized projection, a
**2.655× write amplification**. Median client
time was **5.048 ms folded** and
**4.012 ms normalized**, a
1.258× ratio. The normalization rewrite took a
median **8.786 ms**, yielding a
median-based break-even of **8.48 batches; the first whole-batch threshold is approximately batch 9**.

Across all 10 table pairs,
9/10 contain tuple
redundancy and 8/10
have a lower normalized update median. The results
support the expected reduction in physical writes, but also show that
fixed transaction/query overhead prevents write amplification from
translating proportionally into elapsed time.

![Step 1–4 focus-case summary](figures/08_focus_step_summary.png)

## 1. Research questions

- **RQ1 — Redundancy:** How much referenced-side FD redundancy does
  normalization remove?
- **RQ2 — Normalization effort:** How long does the one-off data
  rewrite take?
- **RQ3 — Update cost:** How much longer does the same logical update
  take on the folded representation than on the normalized one?
- **RQ4 — Amortization:** After how many comparable update batches does
  cumulative median update saving recover the normalization cost?

## 2. Experimental design

### Step 0 — Folded baseline (not timed)

The experiment builds a complete-topology, run-scoped folded
materialized-view projection. Referencing-table and non-key
referenced-table properties are copied into one MV node per join
tuple, using table-specific prefixes. Necessary boundary relationships
are copied. Source nodes and relationships are never deleted or
modified.

### Step 1 — Redundancy

Let \(J\) be the number of folded join tuples and \(D\) the number of
distinct referenced keys that participate in the join. Tuple
redundancy is:

\[
R_{tuple} = J-D
\]

If the referenced table has \(c_B\) columns and key length \(k_B\),
the number of dependent properties is \(c_B-k_B\), and theoretical
redundant property slots are:

\[
R_{slot}=(J-D)(c_B-k_B)
\]

The non-NULL metric counts only populated duplicate dependent values.
The payload metric sums UTF-8 bytes for strings and UTF-8 bytes of
`str(value)` for other types. It is a **value-content proxy, not Neo4j
physical store size**. Complete-topology relationship redundancy is a
separate graph-specific metric and is never added to property counts.

### Steps 2 and 4 — Controlled update

Each case chooses one eligible non-key referenced-table property. A
deterministic rotation over its full referenced-relation active domain
maps every participating referenced key to a different value. The same
key-to-value mapping is applied in both representations:

- Step 2 updates all \(J\) folded copies.
- Step 4 updates the \(D\) distinct normalized referenced nodes.

Both phases execute one autocommit batch through result consumption and
commit. Restore and validation are excluded from timing. There are
5 warm-ups and 20 measured runs per
representation.

### Step 3 — Normalization

The data rewrite recreates normalized referencing nodes, creates one
referenced node per distinct key, creates the join relationship, and
deduplicates/migrates copied referenced-side boundary relationships.
The timer includes explicit transaction begin, data decomposition, and
commit. It excludes schema DDL, validation, refolding, and cleanup.
There are 1 warm-up and
20 measured runs.

Client wall-clock median is the primary time metric. Neo4j-reported
time has integer-millisecond resolution and is frequently zero for
these small operations. Raw samples, IQR, and descriptive
10,000-resample bootstrap intervals are retained; no
significance test is claimed.

## 3. Results

### 3.1 RQ1 — Redundancy saved

For Product → Supplier,
\(R_{tuple}=77-29
=48\). Supplier has
11 dependent properties, so
\(R_{slot}=48\times
11=528\).
The actual non-NULL and payload-proxy measurements are
432 values
(62.518%) and
5,985 bytes
(63.193%).

The Supplier fanout histogram is
`{1: 3, 2: 10, 3: 12, 4: 2, 5: 2}`. A Supplier tuple with
fanout \(f\) contributes \(f-1\) redundant tuple copies.

![Supplier fanout](figures/01_focus_fanout.png)

![Redundancy by pair](figures/02_redundancy_by_pair.png)

| Pair | Redundant property slots | Redundant non-NULL values | Redundant payload proxy (bytes) | Redundant topology relationships |
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

The focus case has
0 redundant boundary
relationships: both folded and normalized states contain
2,232. This zero topology value does
not contradict the positive attribute redundancy; the two metrics
measure different resources.

### 3.2 RQ3 — Folded versus normalized updates

The focus update contains 29 logical
Supplier updates. It writes 77
folded nodes versus
29 normalized nodes, so physical write
amplification is exactly:

\[
A_{write}=J/D=77/
29
=2.655
\]

The folded update median is 5.048 ms
(descriptive bootstrap interval
5.009–
7.034 ms), and the
normalized median is 4.012 ms
(3.963–
4.065 ms). The folded
median is 25.825% higher;
equivalently, normalization reduces the median by
20.524% relative to the folded
baseline.

![Update comparison](figures/03_update_comparison.png)

![Focus update samples](figures/04_focus_update_samples.png)

The folded focus samples contain visible slow runs: the mean is
9.282 ms, median
5.048 ms, and maximum
48.554 ms. This skew is why medians and raw
samples, rather than mean bars, are the primary evidence.

### 3.3 RQ2 — Normalization effort

The focus rewrite converts 77 folded nodes into
77 normalized
Product nodes and
29 distinct
Supplier nodes, then
creates 77 join relationships. Its
median is 8.786 ms, with a descriptive
bootstrap interval of
8.345–
9.205 ms.

Consequently, \(J-D\) measures removed **referenced attribute copies**,
not a net reduction of \(J-D\) graph nodes. Normalization adds distinct
referenced nodes and explicit join relationships while removing
dependent values from their repeated folded locations.

![Normalization effort](figures/05_normalization_effort.png)

### 3.4 Write amplification and elapsed time

Physical write reduction is deterministic from \(J/D\), whereas client
time contains fixed transaction, planning/execution, cache, result
consumption, index, and commit costs. The relationship is therefore
not proportional. For example, Order → Shipper has
276.7× physical write amplification
but only a 1.271× median time ratio.

![Write amplification versus speedup](figures/06_write_amplification_vs_speedup.png)

### 3.5 RQ4 — Break-even

When folded median update time is strictly greater than normalized
median time and \(J-D>0\), the data-rewrite break-even is:

\[
N_{break-even}=
\frac{T_{normalization,median}}
{T_{folded-update,median}-
  T_{normalized-update,median}}
\]

For the focus case this is 8.786/(5.048-4.012)=8.480 batches, or approximately whole batch 9. A fractional estimate is an
amortization projection, not a guarantee for an individual run.

![Normalization break-even](figures/07_normalization_break_even.png)

Two cases have no break-even value. EmployeeTerritory → Employee has
positive redundancy but a slightly slower normalized median;
EmployeeTerritory → Territory has \(J=D=49\) and therefore no tuple
redundancy. Product → Category has a calculated break-even near
1,940.6 batches, but its median saving is only 0.004 ms—well within the
practical noise of such a small workload—so this number must not be
treated as a precise forecast.

## 4. Complete case results

| Pair | J | D | J−D | Tuple redundancy | Folded update median (ms) | Normalized update median (ms) | Time ratio | Normalization median (ms) | Break-even batches |
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

Nine of ten pairs have tuple redundancy; eight have a lower normalized
median. The strongest observed runtime result is
OrderDetail → Product: folded median is
3.991× normalized, with an estimated
5.62-batch break-even.

Descriptively summing the ten independent pair workloads gives
J=7,105, D=
1,107, and J−D=
5,998
(84.419% weighted redundancy), together
with 60,412 theoretical slots,
58,392 duplicate non-NULL values,
968,002 payload-proxy bytes, and
15,172 duplicate boundary
relationships. Because these projections overlap and are evaluated
separately, the sums are **not** the net saving from materializing all
ten pairs simultaneously.

## 5. Discussion

1. **Normalization removes FD redundancy whenever J>D.** The tuple
   formula and the property-level counts quantify different levels of
   the same repeated referenced data.
2. **Fewer physical writes do not guarantee proportionally faster
   transactions.** Eight within-case comparisons favor normalization,
   but fixed overhead dominates several small workloads.
3. **OrderDetail → Product is the strongest runtime evidence.** Its
   3.991× ratio and approximately
   6-batch break-even are
   materially clearer than near-tied microbenchmarks.
4. **Property redundancy and topology redundancy are distinct.**
   Normalization may save repeated attributes without saving boundary
   edges, or may additionally deduplicate referenced-owned boundary
   edges in complete-topology mode.
5. **The “original graph” performance baseline is the folded shadow
   graph.** The actual source Northwind graph remains unchanged.

## 6. Threats to validity

- The suite is one local warm-cache execution in
  `Neo4j/2026.06.0; Python 3.13.14; Neo4j driver 6.2.0`. Server page/query caches were not
  cleared, and phases were not interleaved or randomized.
- Twenty timings are repeated transactions on the same database, not
  twenty independent database deployments. Bootstrap intervals only
  describe resampling variability in this observed sequence.
- Different cases update different attributes, types, active-domain
  sizes, logical batch sizes, and graph topologies. Cross-case
  milliseconds are descriptive context; the controlled comparison is
  folded versus normalized within each case.
- Update restore and validation are excluded. Normalization excludes
  DDL, validation, refolding, and cleanup. Break-even inherits those
  exact timing scopes and is not a full production-migration forecast.
- Value-payload proxy excludes Neo4j record headers, dynamic stores,
  indexes, labels, relationships, transaction logs, compression, and
  page layout. Actual disk savings require store-level measurement.
- Read-query latency, concurrent workloads, locking, and long-running
  operational effects are outside this experiment.

## 7. Conclusion

The focus experiment answers the three requested outcomes directly:

- **Redundancy saved:** 48
  Supplier tuple copies,
  528 theoretical dependent-property
  slots, 432 duplicate non-NULL values,
  and 5,985 value-payload-proxy bytes.
- **Time to normalize:** 8.786 ms median
  for the defined shadow data-rewrite scope.
- **Update overhead:** 77 versus
  29 property writes and
  5.048 versus
  4.012 ms median client time, giving
  a 1.258× folded/normalized ratio.

On this workload, the median-based break-even is
8.48 batches; the first whole-batch threshold is approximately batch 9.
The wider suite confirms that large FD/write redundancy can produce
meaningful update gains, but also demonstrates that tiny workloads
must be interpreted with raw timing variability and fixed overhead in
view.

## Appendix A — Reproducibility

- Source file: `northwind_fd_experiment_results.json`
- Source SHA-256: `2d88fdf3d14d34f576c1ea26b67e19f9dfb290c814a430ffa8e74be6a4adc031`
- Report schema: v6
- Join fingerprint schema: v2
- Database: `northwind`
- Topology mode: `complete`
- Original graph: SHA-256 identical before, during, and after every case
- Shadow graph and temporary schema: cleaned after every case

`case_summary.csv`, `timing_samples.csv`, and
`analysis_summary.json` contain the data used by the figures and
report. SVG copies of every figure are provided for publication.
