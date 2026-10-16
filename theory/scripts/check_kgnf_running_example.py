#!/usr/bin/env python3
"""Exhaustive check: can the running example be decomposed into KGNF with
the decompositions of the companion paper (Definition 4.4), i.e., is there
a sequence of decompositions along FDs in Sigma^+ such that every
decomposition is admissible (key and all other declared FDs Z-local) and
the result is in KGNF?

A decomposition of T along X -> Y (X not a superkey, X and Y disjoint,
Y non-empty, Y within the closure of X) separates Z = X | Y into T_Z (key X)
and T' (features outside Z plus the new reference rho, key kappa if it is
disjoint from Z, else (kappa minus Z) plus rho). Every Y between {one
feature} and the whole closure minus X is tried, not only X^+ minus X.
"""
from itertools import combinations
from functools import lru_cache

def closure(X, fds, key, feats):
    C = set(X)
    changed = True
    while changed:
        changed = False
        for V, W in list(fds) + [(key, feats)]:
            if V <= C and not W <= C:
                C |= W
                changed = True
    return frozenset(C)

def superkey(X, nt):
    feats, key, fds = nt
    return key <= closure(X, fds, key, feats)

def kgnf(state):
    for name, nt in state:
        feats, key, fds = nt
        for V, W in fds:
            if not W <= V and not superkey(V, nt):
                return False
    return True

def decompositions(name, nt):
    feats, key, fds = nt
    fl = sorted(feats)
    for k in range(1, len(fl) + 1):
        for Xt in combinations(fl, k):
            X = frozenset(Xt)
            if superkey(X, nt):
                continue
            rest = sorted(closure(X, fds, key, feats) - X)
            for m in range(1, len(rest) + 1):
                for Yt in combinations(rest, m):
                    Y = frozenset(Yt)
                    Z = X | Y
                    # key Z-local
                    if key & Z and not X <= key:
                        continue
                    rho = "rho[" + name + "|" + ",".join(sorted(X)) + "]"
                    others = [(V, W) for V, W in fds if not (V == X and W == Y)]
                    trans_Z, trans_P, ok = [], [], True
                    for V, W in others:
                        if V | W <= Z:                      # (L1)
                            trans_Z.append((V, W))
                        elif X <= V:                        # (L2)
                            trans_P.append(((V - Z) | {rho}, W - Z))
                        elif not V & Z and (not W & Z or X <= W):   # (L3)
                            trans_P.append((V, (W - Z) | ({rho} if W & Z else set())))
                        else:
                            ok = False
                            break
                    if not ok:
                        continue
                    keyP = key if not key & Z else (key - Z) | {rho}
                    TZ = (frozenset(Z), frozenset(X),
                          frozenset((frozenset(V), frozenset(W)) for V, W in trans_Z))
                    TP = (frozenset((feats - Z) | {rho}), frozenset(keyP),
                          frozenset((frozenset(V), frozenset(W)) for V, W in trans_P))
                    yield (name + "_Z" + "".join(sorted(X))[:0], (X, Y), TZ, TP)

def canon(state):
    return frozenset(nt for _, nt in state)

seen = {}
def search(state, depth, path):
    key = canon(state)
    if key in seen:
        return seen[key]
    if kgnf(state):
        seen[key] = path
        return path
    res = None
    for i, (name, nt) in enumerate(state):
        for _, (X, Y), TZ, TP in decompositions(name, nt):
            new = state[:i] + state[i + 1:] + ((name + "Z", TZ), (name + "'", TP))
            r = search(new, depth + 1, path + [(name, sorted(X), sorted(Y))])
            if r is not None:
                res = r
                break
        if res is not None:
            break
    seen[key] = res
    return res

F = frozenset(["product", "customer", "shipper", "orderDate", "orderTime",
               "address", "paymentMethod", "quantity", "discount"])
kappa = frozenset(["product", "customer", "orderDate", "orderTime"])
X1 = frozenset(["customer", "orderDate", "orderTime"])
X2 = frozenset(["customer", "orderDate", "address"])
phi = (X1, frozenset(["shipper", "address", "paymentMethod"]))
phi1 = (X1, frozenset(["address", "paymentMethod"]))
phi2 = (X2, frozenset(["shipper"]))

for label, fds in [("Sigma={phi}", [phi]), ("Sigma={phi1,phi2}", [phi1, phi2])]:
    seen.clear()
    start = (("OrderInfo", (F, kappa, frozenset(fds))),)
    r = search(start, 0, [])
    print(label, "->", "KGNF reachable via " + str(r) if r is not None else "no sequence reaches KGNF",
          "| states explored:", len(seen))
