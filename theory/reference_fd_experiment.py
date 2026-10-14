#!/usr/bin/env python3
"""Normalize on real schemata whose FDs involve references (Section 5, 'Reference-FDs').

Node types are built from real data: every FD is checked to hold exactly on the
nodes that carry all features involved (the paper's treatment of missing values).
References are single-valued edges to other node types and are compared by node
identity (the target's node_id).  Normalize (normalize_schemas.py) runs in R
random orders; we report the FDs, those with references, decompositions, blocks,
whether the result is order-independent, the Z-locality types that occur in any order and the number of key translations with rho, the runtime,
and the stored occurrences before and after normalization on the data:
before = |feat(T)| * |V_T|; after = sum over resulting node types of
|feat| * (number of distinct flattened-key values in the data), with the new
references rho counted as occurrences.

Workloads
  * Northwind OrderInfo: one node per order line, references product, customer,
    employee and shipVia; key {product, customer, orderDate, employee}; FDs
    discovered on the data, with references on both sides.
  * Offshore Leaks Intermediary, Officer, Entity with the reference
    registered_address (nodes with exactly one such edge) and the FDs that hold.
Usage: reference_fd_experiment.py [R]
Results are printed to the terminal and saved to report/reference_fd_experiment.md.
"""
import random, sys, time, itertools
from collections import Counter
from pathlib import Path
import pandas as pd
from normalize_schemas import NT, closure, closed_cover, decompose, signature

HERE = Path(__file__).resolve().parent
NW = HERE.parent / 'extend_experiment' / 'northwind' / 'northwind_dataset' / 'CSVs'
OL = HERE.parent / 'offshore_experiment' / 'dataset' / 'data'
REPORT = HERE / 'report' / 'reference_fd_experiment.md'

# ------------------------------------------------------------------ data
def northwind():
    o = pd.read_csv(NW / 'orders.csv', sep='|', dtype=str)
    d = pd.read_csv(NW / 'order_details.csv', sep='|', dtype=str)
    j = d.merge(o, on='OrderID').replace('NULL', pd.NA)
    ren = {'ProductID': 'product', 'CustomerID': 'customer', 'EmployeeID': 'employee', 'ShipVia': 'shipper',
           'OrderDate': 'orderDate', 'RequiredDate': 'requiredDate', 'ShippedDate': 'shippedDate', 'Freight': 'freight',
           'ShipName': 'shipName', 'ShipAddress': 'shipAddress', 'ShipCity': 'shipCity', 'ShipPostalCode': 'shipPostalCode',
           'ShipCountry': 'shipCountry', 'UnitPrice': 'unitPrice', 'Quantity': 'quantity', 'Discount': 'discount'}
    j = j.rename(columns=ren)[list(ren.values())].dropna()
    refs = {'product', 'customer', 'employee', 'shipper'}
    key = {'product', 'customer', 'orderDate', 'employee'}
    fds = [({'customer', 'orderDate', 'employee'},
            {'shipper', 'requiredDate', 'shippedDate', 'freight', 'shipName', 'shipAddress', 'shipCity', 'shipPostalCode', 'shipCountry'}),
           ({'customer'}, {'shipAddress', 'shipCity', 'shipPostalCode', 'shipCountry'}),
           ({'shipPostalCode'}, {'shipCity', 'shipCountry'}),
           ({'shipCity'}, {'shipCountry'})]
    return 'Northwind OrderInfo', j, refs, key, fds

def offshore_type(name, file, feats, fds, ra1, addr_ids):
    T = pd.read_csv(OL / file, dtype=str, low_memory=False).set_index('node_id')
    ids = T.index.intersection(ra1.index)
    T = T.loc[ids].copy(); T['registered_address'] = ra1.loc[ids].values
    T = T[T.registered_address.isin(addr_ids)]
    T.index.name = 'node_id'
    T = T.reset_index()[['node_id'] + feats + ['registered_address']].dropna()
    return 'Offshore ' + name, T, {'registered_address'}, {'node_id'}, fds

def offshore():
    r = pd.read_csv(OL / 'relationships.csv', dtype=str, low_memory=False, usecols=['node_id_start', 'node_id_end', 'rel_type'])
    ra = r[r.rel_type == 'registered_address']
    cnt = ra.groupby('node_id_start').size()
    ra1 = ra[ra.node_id_start.isin(cnt[cnt == 1].index)].set_index('node_id_start')['node_id_end']
    addr_ids = set(pd.read_csv(OL / 'nodes-addresses.csv', dtype=str, low_memory=False, usecols=['node_id'])['node_id'])
    A = {'registered_address'}
    yield offshore_type('Intermediary', 'nodes-intermediaries.csv',
                        ['countries', 'country_codes', 'sourceID', 'valid_until'],
                        [(A, {'countries', 'country_codes', 'sourceID', 'valid_until'}),
                         ({'countries'}, {'country_codes'}), ({'country_codes'}, {'countries'}),
                         ({'sourceID'}, {'valid_until'}), ({'valid_until'}, {'sourceID'})], ra1, addr_ids)
    yield offshore_type('Officer', 'nodes-officers.csv',
                        ['countries', 'country_codes', 'sourceID', 'valid_until'],
                        [(A, {'sourceID'})], ra1, addr_ids)
    yield offshore_type('Entity', 'nodes-entities.csv',
                        ['jurisdiction', 'jurisdiction_description', 'service_provider', 'countries', 'country_codes', 'sourceID', 'valid_until'],
                        [(A, {'sourceID'}), ({'service_provider'}, {'sourceID', 'valid_until'}),
                         ({'jurisdiction'}, {'jurisdiction_description'}),
                         ({'countries'}, {'country_codes'}), ({'country_codes'}, {'countries'})], ra1, addr_ids)

# ------------------------------------------------------------------ checks
def check_fds(df, fds):
    for V, W in fds:
        g = df.groupby(sorted(V))[sorted(W)].nunique()
        assert (g <= 1).all().all(), ('FD violated', V, W)
    return True

# ------------------------------------------------------------------ Normalize with bookkeeping
def ztype(nt, X, Z, V, W):
    det = lambda S: X <= nt.cl(S)
    if V | W <= Z: return 'L1'
    if det(V & Z): return 'L2'
    if not V & Z and (not W & Z or det(W & Z)): return 'L3'
    return None

def normalize_traced(nts, rng):
    counter = itertools.count()
    state = [closed_cover(nt) for nt in nts]
    flat = {}                      # new reference -> flattened determinant (original features)
    blocked, ndec, types, keyrho = set(), 0, Counter(), 0
    while True:
        cands = [(i, V) for i, nt in enumerate(state) for V, W in nt.fds
                 if not W <= V and not nt.key <= nt.cl(V) and (id(nt), V) not in blocked]
        if not cands: break
        i, V = rng.choice(cands); nt = state[i]
        d = decompose(nt, V, counter)
        if d is None:
            blocked.add((id(nt), V)); continue
        Z = nt.cl(V)
        for V2, W2 in nt.fds:
            if V2 != V: types[ztype(nt, V, Z, V2, W2)] += 1
        if nt.key & Z: keyrho += 1
        TZ, TP = d
        TZ.name = nt.name.split('_Z')[0] + '_Z%d' % (ndec + 1)
        rho = next(f for f in TP.feats if f not in nt.feats)
        flat[rho] = frozenset().union(*(flat.get(a, frozenset([a])) for a in V))
        blocked = {b for b in blocked if b[0] != id(nt)}
        state[i:i + 1] = [TZ, TP]; ndec += 1
    return state, len(blocked), ndec, types, keyrho, flat, [state[i].fds for i in range(len(state))]

def flatten(F, flat):
    return frozenset().union(*(flat.get(a, frozenset([a])) for a in F))

def occurrences(df, state, flat, refs):
    """A separated node type whose key is a single original reference r is in one-to-one
    correspondence with the referenced nodes and is merged with their type (as in the TPC-H
    experiment): its other features are stored once per referenced node, and the new
    reference to it coincides with r."""
    after, nodes = 0, {}
    for nt in state:
        k = sorted(flatten(nt.key, flat))
        n = len(df.drop_duplicates(subset=k))
        merged = len(nt.key) == 1 and k == list(nt.key) and k[0] in refs
        stored = len(nt.feats) - 1 if merged else len(nt.feats)
        nodes[nt.name] = dict(nodes=n, features=sorted(nt.feats), key=sorted(nt.key), flat_key=k, merged_into_target=merged)
        after += n * stored
    return after, nodes

def run(name, df, refs, key, fds, R, rng):
    check_fds(df, fds)
    nt = NT(name, df.columns, key, fds)
    ref_fds = sum(1 for V, W in fds if (V | W) & refs)
    sigs, blocks, decs, times, ztypes, keyrhos = set(), set(), set(), [], set(), set()
    for _ in range(R):
        t = time.perf_counter()
        st, b, d, types, keyrho, flat, _ = normalize_traced([nt], rng)
        times.append(time.perf_counter() - t)
        sigs.add(str(signature(st))); blocks.add(b); decs.add(d)
        ztypes |= {k for k, v in types.items() if v}; keyrhos.add(keyrho)
    before = len(df) * len(df.columns)
    after, nodes = occurrences(df, st, flat, refs)
    blocked_fds = []
    if b:
        # which FDs of the closed cover block: rerun and collect
        cc = closed_cover(nt)
        for V, W in cc.fds:
            if not nt.key <= nt.cl(V) and decompose(cc, V, itertools.count()) is None:
                blocked_fds.append((sorted(V), sorted(W)))
    return dict(workload=name, nodes=len(df), features=len(df.columns), references=sorted(refs), fds=len(fds), fds_with_refs=ref_fds,
                decompositions=sorted(decs), blocks=sorted(blocks), order_independent=len(sigs) == 1,
                z_types=sorted(ztypes), key_with_rho=sorted(keyrhos), max_ms=round(1000 * max(times), 2),
                occurrences_before=before, occurrences_after=after, eliminated=before - after,
                result_types={k: v for k, v in nodes.items()}, blocked_fds=blocked_fds)

def write_markdown(results, R):
    """Write the existing result records as an English Markdown report."""
    def table(headers, rows):
        def cell(value):
            return str(value).replace('|', '\\|').replace('\n', ' ')
        return '\n'.join([
            '| ' + ' | '.join(map(cell, headers)) + ' |',
            '| ' + ' | '.join('---' for _ in headers) + ' |',
            *('| ' + ' | '.join(map(cell, row)) + ' |' for row in rows),
        ])

    lines = [
        '# Reference FD Normalization Experiment Report', '',
        'Source: [reference_fd_experiment.py](../reference_fd_experiment.py).', '',
        f'Random orders per workload: {R}.', '',
        'The FDs are predefined and checked on the retained complete records. Offshore records also require exactly one registered_address edge to a valid Address node.', '',
        '## Input Workloads', '',
        table(['Workload', 'Records', 'Features', 'References', 'FDs', 'FDs involving references'],
              [[r['workload'], r['nodes'], r['features'], ', '.join(r['references']), r['fds'], r['fds_with_refs']] for r in results]), '',
        '## Normalization Results', '',
        'Decompositions, blocks, and key translations list the distinct counts observed across random orders. Z-locality types are the union observed across those orders. Matching signatures compare feature sets and new-reference counts; they do not compare keys, FDs, or reference targets.', '',
        table(['Workload', 'Decompositions', 'Blocked FDs', 'Matching signatures', 'Z-locality types', 'Key translations with rho', 'Maximum time (ms)'],
              [[r['workload'], r['decompositions'], r['blocks'], 'Yes' if r['order_independent'] else 'No', ', '.join(r['z_types']) or 'None', r['key_with_rho'], f"{r['max_ms']:.2f}"] for r in results]), '',
        'Timings cover schema normalization only, excluding CSV loading, FD checks, and storage estimation.', '',
        '## Estimated Stored Occurrences', '',
        'These are logical feature-occurrence counts, not bytes or measured database writes. Before = records × features. After uses distinct flattened-key values for each resulting type, including new references. Storage counts and result types use the final tested order.', '',
        table(['Workload', 'Before', 'After', 'Eliminated', 'Reduction (%)'],
              [[r['workload'], r['occurrences_before'], r['occurrences_after'], r['eliminated'],
                f"{100 * r['eliminated'] / r['occurrences_before']:.2f}" if r['occurrences_before'] else 'N/A'] for r in results]), '',
        '## Result Types', '',
        'A type merged into a referenced target reuses the original reference; its storage estimate omits one reference feature. The features column still lists the complete schema features.', '',
        'Blocked-FD details recheck candidates from the original closed cover when the final order reports blocks; they are not a complete list of remaining FDs in the final schema.', '',
    ]
    for r in results:
        lines.extend([
            '### ' + r['workload'], '',
            table(['Type', 'Distinct keys', 'Key', 'Flattened key', 'Features', 'Merged into referenced target'],
                  [[name, t['nodes'], ', '.join(t['key']), ', '.join(t['flat_key']), ', '.join(t['features']),
                    'Yes' if t['merged_into_target'] else 'No'] for name, t in r['result_types'].items()]), '',
            'Recorded blocked-FD details:', '',
        ])
        if r['blocked_fds']:
            lines.extend([table(['Determinant', 'Dependent features'],
                                [[', '.join(V), ', '.join(W)] for V, W in r['blocked_fds']]), ''])
        else:
            lines.extend(['No blocked-FD details recorded.', ''])
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text('\n'.join(lines), encoding='utf-8')
    print(f'Markdown report: {REPORT}')

def print_summary(result):
    """Print a compact text summary; detailed tables are in the Markdown report."""
    print(f"\n{result['workload']}")
    print(f"  Records: {result['nodes']:,}; features: {result['features']}; "
          f"FDs: {result['fds']} (with references: {result['fds_with_refs']})")
    print(f"  Decompositions: {', '.join(map(str, result['decompositions']))}; "
          f"blocked FDs: {', '.join(map(str, result['blocks']))}")
    print(f"  Matching signatures across tested orders: {'Yes' if result['order_independent'] else 'No'}")
    print(f"  Stored occurrences: {result['occurrences_before']:,} -> "
          f"{result['occurrences_after']:,}; eliminated: {result['eliminated']:,}")
    print(f"  Maximum Normalize time: {result['max_ms']:.2f} ms")

if __name__ == '__main__':
    R = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    rng = random.Random(1)
    out = []
    for name, df, refs, key, fds in [northwind(), *offshore()]:
        r = run(name, df, refs, key, fds, R, rng); out.append(r)
        print_summary(r)
    write_markdown(out, R)
