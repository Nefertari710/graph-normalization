#!/usr/bin/env python3
"""Schema-level evaluation of Normalize (Section 4.4 of the paper).

Normalize replaces the FDs over each node type by their closed cover (left-hand
sides of a minimal cover of the FDs and the key FD, each with its closure), and
repeatedly decomposes a node type along X -> X^+ \\ X for a declared FD whose
left-hand side X is no superkey, if the key and all other FDs over the node type
are Z-local (closure-based, Z = X^+); otherwise it blocks the FD.

Workloads:
  * TPC-H: for each of the 2^8 subsets of the references e1..e8, the
    de-normalized DAKGS copies all features of the referenced node type into the
    referencing one, which keeps the reference; the reference then determines the
    copies. Normalize should recover the normalized schema (separated node types
    whose features equal those of the referenced node types) without blocking.
  * Offshore Leaks: the 12 FDs over Address, Entity, Intermediary and Other used
    in the experiments, with node_id as key.
  * The running example with phi1 and phi2, which must block.
Each workload is run in R random orders; we report decompositions, blocks,
whether the result is independent of the order, and the runtime.
Usage: normalize_schemas.py [R]
"""
import random, sys, time
from itertools import combinations
from pathlib import Path

def closure(X, fds):
    C = set(X); changed = True
    while changed:
        changed = False
        for V, W in fds:
            if V <= C and not W <= C:
                C |= W; changed = True
    return frozenset(C)

def minimal_cover(fds):
    out = [(frozenset(V), a) for V, W in fds for a in W if a not in V]
    red = []
    for V, a in out:
        V = set(V)
        for b in sorted(V):
            if len(V) > 1 and a in closure(V - {b}, [(x, {y}) for x, y in out]):
                V.discard(b)
        red.append((frozenset(V), a))
    red = list(dict.fromkeys(red))
    i = 0
    while i < len(red):
        rest = red[:i] + red[i + 1:]
        if red[i][1] in closure(red[i][0], [(x, {y}) for x, y in rest]):
            red = rest
        else:
            i += 1
    return red

class NT:
    """node type: name, features, key, declared FDs [(V, W)]"""
    def __init__(self, name, feats, key, fds):
        self.name, self.feats, self.key = name, frozenset(feats), frozenset(key)
        self.fds = [(frozenset(V), frozenset(W)) for V, W in fds]
    def all_fds(self):
        return self.fds + [(self.key, self.feats)]
    def cl(self, X):
        return closure(X, self.all_fds())

def closed_cover(nt):
    M = minimal_cover(nt.fds + [(nt.key, nt.feats)])
    lhs = list(dict.fromkeys(V for V, _ in M))
    return NT(nt.name, nt.feats, nt.key, [(V, nt.cl(V) - V) for V in lhs if nt.cl(V) - V])

def decompose(nt, X, counter):
    Z = nt.cl(X)
    det = lambda S: X <= nt.cl(S)
    if nt.key & Z and not det(nt.key & Z):
        return None
    rho = "ref%d" % next(counter)
    tz, tp = [], []
    for V, W in nt.fds:
        if V | W <= Z:
            tz.append((V, W))
        elif det(V & Z):
            tp.append(((V - Z) | {rho}, W - Z))
        elif not V & Z and (not W & Z or det(W & Z)):
            tp.append((V, (W - Z) | ({rho} if W & Z else set())))
        else:
            return None
    TZ = NT(nt.name + "_Z", Z, X, [(V, W - V) for V, W in tz if W - V])
    keyp = nt.key if not nt.key & Z else (nt.key - Z) | {rho}
    TP = NT(nt.name, (nt.feats - Z) | {rho}, keyp, [(V, W - V) for V, W in tp if W - V])
    return TZ, TP

def normalize(nts, rng):
    import itertools
    counter = itertools.count()
    state = [closed_cover(nt) for nt in nts]
    blocked, ndec = set(), 0
    while True:
        cands = [(i, V) for i, nt in enumerate(state) for V, W in nt.fds
                 if not W <= V and not nt.key <= nt.cl(V) and (id(nt), V) not in blocked]
        if not cands:
            break
        i, V = rng.choice(cands)
        d = decompose(state[i], V, counter)
        if d is None:
            blocked.add((id(state[i]), V))
        else:
            # Normalize removes the blocked FDs of the decomposed node type from B
            blocked = {b for b in blocked if b[0] != id(state[i])}
            state[i:i + 1] = list(d); ndec += 1
    return state, len(blocked), ndec

def signature(state):
    """order-independent description: multiset of (features without new refs, #new refs)"""
    return sorted((tuple(sorted(f for f in nt.feats if not f.startswith("ref"))),
                   sum(f.startswith("ref") for f in nt.feats)) for nt in state)

# ---------------------------------------------------------------- TPC-H
TPCH = {   # node type: (non-key properties, key, references {role: target})
    "R": (["r_name", "r_comment"], ["r_regionkey"], {}),
    "N": (["n_name", "n_comment"], ["n_nationkey"], {"n_region": "R"}),
    "S": (["s_name", "s_address", "s_phone", "s_acctbal", "s_comment"], ["s_suppkey"], {"s_nation": "N"}),
    "C": (["c_name", "c_address", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"], ["c_custkey"], {"c_nation": "N"}),
    "P": (["p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"], ["p_partkey"], {}),
    "PS": (["ps_availqty", "ps_supplycost", "ps_comment"], ["ps_part", "ps_supp"], {"ps_part": "P", "ps_supp": "S"}),
    "O": (["o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"], ["o_orderkey"], {"o_cust": "C"}),
    "L": (["l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
           "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"],
          ["l_order", "l_linenumber"], {"l_order": "O", "l_ps": "PS"}),
}
EDGES = {"e1": ("L", "l_ps"), "e2": ("L", "l_order"), "e3": ("PS", "ps_part"), "e4": ("PS", "ps_supp"),
         "e5": ("O", "o_cust"), "e6": ("C", "c_nation"), "e7": ("S", "s_nation"), "e8": ("N", "n_region")}

def tpch_variant(E):
    """de-normalized node types for the set E of de-normalized references"""
    memo = {}
    def content(T, prefix):
        # features copied from T (non-key properties, references, copies) with their FDs
        props, key, refs = TPCH[T]
        feats = {prefix + p for p in props if p not in key} | {prefix + p for p in key if p not in refs}
        feats |= {prefix + r for r in refs}
        fds = []
        for e, (src, role) in EDGES.items():
            if src == T and e in E:
                tgt = TPCH[src][2][role]
                f2, d2 = content(tgt, prefix + role + ".")
                feats |= f2
                fds.append(({prefix + role}, f2))
                fds += d2
        return feats, fds
    nts = []
    for T, (props, key, refs) in TPCH.items():
        feats, fds = content(T, "")
        nts.append(NT(T, feats, set(key), fds))
    return nts

# ---------------------------------------------------------------- Offshore
def offshore():
    cols = {
        "Address": "node_id,address,name,countries,country_codes,sourceID,valid_until,note",
        "Entity": "node_id,name,original_name,former_name,jurisdiction,jurisdiction_description,company_type,address,internal_id,incorporation_date,inactivation_date,struck_off_date,dorm_date,status,service_provider,ibcRUC,country_codes,countries,sourceID,valid_until,note",
        "Intermediary": "node_id,name,status,internal_id,address,countries,country_codes,sourceID,valid_until,note",
        "Other": "node_id,name,type,incorporation_date,struck_off_date,closed_date,jurisdiction,jurisdiction_description,countries,country_codes,sourceID,valid_until,note",
    }
    fds = {
        "Address": [("countries", ["country_codes"])],
        "Entity": [("service_provider", ["sourceID", "valid_until"]), ("country_codes", ["countries"]), ("countries", ["country_codes"])],
        "Intermediary": [("country_codes", ["countries"]), ("countries", ["country_codes"]), ("valid_until", ["sourceID"]), ("sourceID", ["valid_until"])],
        "Other": [("jurisdiction", ["jurisdiction_description"]), ("jurisdiction_description", ["jurisdiction"]), ("country_codes", ["countries"]), ("countries", ["country_codes"])],
    }
    return [NT(T, c.split(","), {"node_id"}, [({V}, set(W)) for V, W in fds[T]]) for T, c in cols.items()]

# ---------------------------------------------------------------- DBpedia
def dbpedia(path=(Path(__file__).resolve().parent.parent / "extend_experiment"
                  / "dbpedia_experiment" / "find_functional_dependency"
                  / "functional_dependencies_all.json")):
    """one node type per class with discovered FDs; features: the FD attributes and the URI as key"""
    import json
    from collections import defaultdict
    d = json.load(open(path))
    by = defaultdict(list)
    for dep in d["dependencies"]:
        by[dep["class_name"]].append(({dep["x"]["name"]}, {dep["y"]["name"]}))
    nts = []
    for cls, fds in sorted(by.items()):
        feats = {"uri"} | {a for V, W in fds for a in V | W}
        nts.append(NT(cls, feats, {"uri"}, fds))
    return nts

def running(fdset):
    F = ["product", "customer", "shipper", "orderDate", "orderTime", "address", "paymentMethod", "quantity", "discount"]
    X1 = {"customer", "orderDate", "orderTime"}; X2 = {"customer", "orderDate", "address"}
    fds = [(X1, {"address", "paymentMethod"}), (X2, {"shipper"})] if fdset == "phi12" else [(X1, {"address", "paymentMethod", "shipper"})]
    return [NT("OrderInfo", F, {"product", "customer", "orderDate", "orderTime"}, fds)]

def run(name, nts, R, rng):
    sigs, blocks, decs, times = set(), set(), set(), []
    nfds = sum(len(nt.fds) for nt in nts)
    for _ in range(R):
        t = time.perf_counter()
        st, b, d = normalize(nts, rng)
        times.append(time.perf_counter() - t)
        sigs.add(str(signature(st))); blocks.add(b); decs.add(d)
    return dict(workload=name, node_types=len(nts), fds=nfds, decompositions=sorted(decs), blocks=sorted(blocks),
                order_independent=len(sigs) == 1, max_ms=round(1000 * max(times), 3))

if __name__ == "__main__":
    R = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    rng = random.Random(1)
    # TPC-H: all 256 variants
    tot = dict(variants=0, blocked_variants=0, order_dependent=0, recovered=0, max_ms=0.0, max_decompositions=0)
    for k in range(9):
        for E in combinations(sorted(EDGES), k):
            nts = tpch_variant(set(E))
            r = run("tpch", nts, R, rng)
            st, _, _ = normalize(nts, rng)
            tot["variants"] += 1
            tot["blocked_variants"] += max(r["blocks"]) > 0
            tot["order_dependent"] += not r["order_independent"]
            tot["max_ms"] = max(tot["max_ms"], r["max_ms"])
            tot["max_decompositions"] = max(tot["max_decompositions"], max(r["decompositions"]))
            # recovery: one separated node type per copied path, i.e., per copy FD
            expected = sum(1 for nt in nts) + sum(1 for nt in nts for V, W in nt.fds)
            tot["recovered"] += (len(st) == expected)
    print("TPC-H", tot)
    print(run("offshore", offshore(), R, rng))
    for nt in dbpedia():
        r = run("dbpedia " + nt.name, [nt], R, rng)
        if max(r["blocks"]) > 0 or len(nt.fds) > 2:
            print(r, [("".join(sorted(V)), "".join(sorted(W))) for V, W in nt.fds] if max(r["blocks"]) else "")
    allb = [nt for nt in dbpedia() if max(run("x", [nt], R, rng)["blocks"]) > 0]
    print("dbpedia classes", len(dbpedia()), "fds", sum(len(nt.fds) for nt in dbpedia()), "blocking classes", [nt.name for nt in allb])
    print(run("running phi", running("phi"), R, rng))
    print(run("running phi1,phi2", running("phi12"), R, rng))
