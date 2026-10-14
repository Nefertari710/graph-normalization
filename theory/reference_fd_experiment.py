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
"""
import json, random, sys, time, itertools
from collections import Counter
import pandas as pd
from normalize_schemas import NT, closure, closed_cover, decompose, signature

HERE = __file__.rsplit('/', 1)[0] if '/' in __file__ else '.'
NW = HERE + '/../case_study/northwind_dataset/CSVs/'
OL = HERE + '/../offshore-leak-database/'

# ------------------------------------------------------------------ data
def northwind():
    o = pd.read_csv(NW + 'orders.csv', sep='|', dtype=str)
    d = pd.read_csv(NW + 'order_details.csv', sep='|', dtype=str)
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
    T = pd.read_csv(OL + file, dtype=str, low_memory=False).set_index('node_id')
    ids = T.index.intersection(ra1.index)
    T = T.loc[ids].copy(); T['registered_address'] = ra1.loc[ids].values
    T = T[T.registered_address.isin(addr_ids)]
    T.index.name = 'node_id'
    T = T.reset_index()[['node_id'] + feats + ['registered_address']].dropna()
    return 'Offshore ' + name, T, {'registered_address'}, {'node_id'}, fds

def offshore():
    r = pd.read_csv(OL + 'relationships.csv', dtype=str, low_memory=False, usecols=['node_id_start', 'node_id_end', 'rel_type'])
    ra = r[r.rel_type == 'registered_address']
    cnt = ra.groupby('node_id_start').size()
    ra1 = ra[ra.node_id_start.isin(cnt[cnt == 1].index)].set_index('node_id_start')['node_id_end']
    addr_ids = set(pd.read_csv(OL + 'nodes-addresses.csv', dtype=str, low_memory=False, usecols=['node_id'])['node_id'])
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

if __name__ == '__main__':
    R = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    rng = random.Random(1)
    out = []
    for name, df, refs, key, fds in [northwind(), *offshore()]:
        r = run(name, df, refs, key, fds, R, rng); out.append(r)
        print(json.dumps(r, indent=1))
    json.dump(out, open(HERE + '/reference_fd_results.json', 'w'), indent=1)
