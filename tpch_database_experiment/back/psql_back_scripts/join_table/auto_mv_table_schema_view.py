import re
from pathlib import Path
from itertools import combinations
from typing import Dict, List, Set, Tuple, Iterable, Optional
from collections import Counter

import psycopg2
from psycopg2 import sql


# =========================
# Config
# =========================
DB_CONFIG = dict(
    host="localhost",
    dbname="tpch_sf_01_test",
    user="root",
    password="",
    port=5432
)

STATEMENT_TIMEOUT = "5min"
MAX_ESTIMATED_ROWS = 200_000_000

EXCEED_DIR = Path("exceed_marks")
EXCEED_DIR.mkdir(parents=True, exist_ok=True)

DROP_NAME_LIKE = "q%_view_%"  # 只删实验 view；想删全部 public view 就改成 None

groups = [
    (2,  [("P", "PS"), ("S", "PS"), ("S", "N"), ("N", "R")]),
    (3,  [("L", "O"), ("O", "C")]),
    (4,  [("L", "O")]),
    (5,  [("L", "O"), ("O", "C"), ("C", "S"), ("L", "S"), ("S", "N"), ("N", "R")]),
    (7,  [("L", "O"), ("O", "C"), ("C", "N2"), ("L", "S"), ("S", "N1")]),
    (8,  [("L", "O"), ("L", "P"), ("O", "C"), ("C", "N1"), ("L", "S"), ("S", "N2"), ("N1", "R")]),
    (9,  [("L", "O"), ("L", "P"), ("L", "PS"), ("L", "S"), ("S", "N")]),
    (10, [("L", "O"), ("O", "C"), ("C", "N")]),
    (11, [("PS", "S"), ("S", "N")]),
    (12, [("L", "O")]),
    (13, [("O", "C")]),
    (14, [("L", "P")]),
    (15, [("L", "S")]),
    (16, [("PS", "P"), ("PS", "S")]),
    (17, [("L", "P")]),
    (18, [("L", "O"), ("O", "C")]),
    (19, [("L", "P")]),
    (20, [("L", "PS"), ("PS", "P"), ("PS", "S"), ("S", "N")]),
    (21, [("L", "S"), ("L", "O"), ("S", "N")]),
    (22, [("O", "C")]),
]

TABLE_MAP = {
    "L": "lineitem",
    "O": "orders",
    "C": "customer",
    "S": "supplier",
    "N": "nation",
    "N1": "nation",
    "N2": "nation",
    "R": "region",
    "P": "part",
    "PS": "partsupp"
}

# Table name map
DISPLAY_NAME_MAP = {
    "L": "lineitem",
    "O": "orders",
    "C": "customer",
    "S": "supplier",
    "N": "nation",
    "N1": "nation1",
    "N2": "nation2",
    "R": "region",
    "P": "part",
    "PS": "partsupp"
}


JOIN_RULES = {
    ("L", "O"): ("l_orderkey", "o_orderkey"),
    ("O", "C"): ("o_custkey", "c_custkey"),

    ("C", "N"): ("c_nationkey", "n_nationkey"),
    ("S", "N"): ("s_nationkey", "n_nationkey"),
    ("N", "R"): ("n_regionkey", "r_regionkey"),

    ("C", "N1"): ("c_nationkey", "n_nationkey"),
    ("C", "N2"): ("c_nationkey", "n_nationkey"),
    ("S", "N1"): ("s_nationkey", "n_nationkey"),
    ("S", "N2"): ("s_nationkey", "n_nationkey"),
    ("N1", "R"): ("n_regionkey", "r_regionkey"),

    ("L", "S"): ("l_suppkey", "s_suppkey"),
    ("L", "P"): ("l_partkey", "p_partkey"),
    ("L", "PS"): (("l_partkey", "l_suppkey"), ("ps_partkey", "ps_suppkey")),
    ("PS", "S"): ("ps_suppkey", "s_suppkey"),
    ("PS", "P"): ("ps_partkey", "p_partkey"),

    ("C", "S"): ("c_nationkey", "s_nationkey"),
}

CANONICAL_ORDER = ["L", "O", "C", "PS", "S", "P", "N", "N1", "N2", "R"]


# =========================
# Utils
# =========================
def safe_ident(name: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if name and name[0].isdigit():
        name = "t_" + name
    return name


def sym_rank(x: str) -> int:
    return CANONICAL_ORDER.index(x) if x in CANONICAL_ORDER else 999


def canonical_edge(e: Tuple[str, str]) -> Tuple[str, str]:
    a, b = e
    return (a, b) if sym_rank(a) <= sym_rank(b) else (b, a)


def aliases_for_symbols(syms: Iterable[str]) -> Dict[str, str]:
    return {s: s.lower() for s in syms}  # N1->n1, N2->n2


def get_keys(a: str, b: str):
    if (a, b) in JOIN_RULES:
        return JOIN_RULES[(a, b)]
    if (b, a) in JOIN_RULES:
        k1, k2 = JOIN_RULES[(b, a)]
        return k2, k1
    raise KeyError(f"No rule for edge ({a},{b})")


def induced_edges(full_edges: List[Tuple[str, str]], node_set: Set[str]) -> List[Tuple[str, str]]:
    es = []
    for a, b in full_edges:
        a, b = canonical_edge((a, b))
        if a in node_set and b in node_set:
            es.append((a, b))
    return sorted(set(es), key=lambda x: (sym_rank(x[0]), sym_rank(x[1]), x[0], x[1]))


def canonical_nodes_from_edges(full_edges: List[Tuple[str, str]]) -> List[str]:
    nodes = set()
    for a, b in full_edges:
        a, b = canonical_edge((a, b))
        nodes.add(a); nodes.add(b)
    return sorted(nodes, key=lambda x: (sym_rank(x), x))


def is_connected(node_set: Set[str], edges: List[Tuple[str, str]]) -> bool:
    if len(node_set) < 2 or not edges:
        return False
    adj: Dict[str, Set[str]] = {n: set() for n in node_set}
    for a, b in edges:
        adj[a].add(b)
        adj[b].add(a)
    start = next(iter(node_set))
    seen = {start}
    stack = [start]
    while stack:
        u = stack.pop()
        for v in adj.get(u, set()):
            if v not in seen:
                seen.add(v)
                stack.append(v)
    return seen == node_set


def enumerate_connected_node_sets(full_edges: List[Tuple[str, str]]) -> List[Set[str]]:
    nodes = canonical_nodes_from_edges(full_edges)
    out: List[Set[str]] = []
    for k in range(2, len(nodes) + 1):
        for comb in combinations(nodes, k):
            ns = set(comb)
            es = induced_edges(full_edges, ns)
            if is_connected(ns, es):
                out.append(ns)
    return out


def build_where_predicates(edge_set: List[Tuple[str, str]], alias: Dict[str, str]) -> List[str]:
    preds: List[str] = []
    for a, b in edge_set:
        lk, rk = get_keys(a, b)
        aa, bb = alias[a], alias[b]
        if isinstance(lk, tuple):
            preds.append("(" + " AND ".join([f'{aa}."{l}" = {bb}."{r}"' for l, r in zip(lk, rk)]) + ")")
        else:
            preds.append(f'({aa}."{lk}" = {bb}."{rk}")')
    return preds


def build_join_predicate(a: str, b: str, alias: Dict[str, str]) -> str:
    lk, rk = get_keys(a, b)
    aa, bb = alias[a], alias[b]
    if isinstance(lk, tuple):
        return "(" + " AND ".join([f'{aa}."{l}" = {bb}."{r}"' for l, r in zip(lk, rk)]) + ")"
    return f'({aa}."{lk}" = {bb}."{rk}")'


def build_adjacency(edge_set: List[Tuple[str, str]]) -> Dict[str, Set[str]]:
    adj: Dict[str, Set[str]] = {}
    for a, b in edge_set:
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)
    return adj


def shortest_distances(start: str, adj: Dict[str, Set[str]], blocked: Optional[Set[str]] = None) -> Dict[str, int]:
    blocked = blocked or set()
    if start in blocked:
        return {}

    dist: Dict[str, int] = {start: 0}
    queue: List[str] = [start]
    idx = 0
    while idx < len(queue):
        u = queue[idx]
        idx += 1
        for v in sorted(adj.get(u, set()), key=lambda x: (sym_rank(x), x)):
            if v in blocked or v in dist:
                continue
            dist[v] = dist[u] + 1
            queue.append(v)
    return dist


def customer_preserving_nodes(syms: Tuple[str, ...], edge_set: List[Tuple[str, str]]) -> Set[str]:
    if "C" not in syms or "O" not in syms:
        return set()

    adj = build_adjacency(edge_set)
    dist = shortest_distances("O", adj, blocked={"C"})
    return set(dist.keys())


def build_from_join_clause(syms: Tuple[str, ...], edge_set: List[Tuple[str, str]], alias: Dict[str, str]) -> str:
    preserve_customer = "C" in syms and "O" in syms
    nullable_nodes = customer_preserving_nodes(syms, edge_set) if preserve_customer else set()

    root = "C" if preserve_customer else syms[0]
    adj = build_adjacency(edge_set)
    dist_from_root = shortest_distances(root, adj)
    dist_from_o = shortest_distances("O", adj, blocked={"C"}) if preserve_customer else {}

    joined = {root}
    clauses = [f"{TABLE_MAP[root]} {alias[root]}"]

    while len(joined) < len(syms):
        candidates = [s for s in syms if s not in joined and any(n in joined for n in adj.get(s, set()))]
        if not candidates:
            raise ValueError(f"Unable to build join tree for nodes={syms}, edges={edge_set}")

        def candidate_key(s: str):
            is_nullable = 0 if s in nullable_nodes else 1
            branch_dist = dist_from_o.get(s, 10**9) if s in nullable_nodes else dist_from_root.get(s, 10**9)
            return (is_nullable, branch_dist, sym_rank(s), s)

        child = min(candidates, key=candidate_key)
        joined_neighbors = sorted([n for n in adj.get(child, set()) if n in joined], key=lambda x: (sym_rank(x), x))

        if child in nullable_nodes:
            parent = min(
                joined_neighbors,
                key=lambda x: (
                    0 if x in nullable_nodes else 1,
                    dist_from_o.get(x, 10**9),
                    sym_rank(x),
                    x,
                ),
            )
            join_type = "LEFT JOIN"
        else:
            parent = min(joined_neighbors, key=lambda x: (dist_from_root.get(x, 10**9), sym_rank(x), x))
            join_type = "INNER JOIN"

        on_parts = [build_join_predicate(parent, child, alias)]
        extra_neighbors = [n for n in joined_neighbors if n != parent]
        for n in extra_neighbors:
            on_parts.append(build_join_predicate(n, child, alias))

        clauses.append(
            f'{join_type} {TABLE_MAP[child]} {alias[child]} ON ' + " AND ".join(on_parts)
        )
        joined.add(child)

    return "\n            ".join(clauses)


# =========================
# Naming
# =========================
def view_name(qid: int, node_set: Set[str]) -> str:
    syms = tuple(sorted(node_set, key=lambda x: (sym_rank(x), x)))
    tables_part = "_".join([DISPLAY_NAME_MAP[s] for s in syms])
    return safe_ident(f"q{qid}_view_{tables_part}")


def exceed_mark_path(qid: int, node_set: Set[str]) -> Path:
    syms = tuple(sorted(node_set, key=lambda x: (sym_rank(x), x)))
    return EXCEED_DIR / safe_ident(f"Q{qid}_" + "_".join(syms) + "_exceed")


# =========================
# DB helpers
# =========================
def drop_views(cur, conn, schema: str = "public", name_like: Optional[str] = None):
    if name_like is None:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.views
            WHERE table_schema=%s
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_name;
            """,
            (schema,)
        )
    else:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.views
            WHERE table_schema=%s
              AND table_name LIKE %s
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_name;
            """,
            (schema, name_like)
        )
    rows = cur.fetchall()

    if not rows:
        print("ℹ️ No views to drop.")
        return

    print("\n🧹 Views to drop:")
    print("--------------------------------------------------")
    for view_schema, view_name_ in rows:
        print(f"{view_schema}.{view_name_}")
    print("--------------------------------------------------")

    for view_schema, view_name_ in rows:
        cur.execute(
            sql.SQL("DROP VIEW IF EXISTS {}.{} CASCADE;").format(
                sql.Identifier(view_schema),
                sql.Identifier(view_name_)
            )
        )
    conn.commit()
    print(f"\n✅ Dropped {len(rows)} view(s).")


def get_table_columns(cur, table_name: str, schema: str = "public") -> List[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position;
        """,
        (schema, table_name)
    )
    return [r[0] for r in cur.fetchall()]


def strip_table_prefix(col: str, base_table: str) -> str:
    """
    只对 nation 用：n_nationkey -> nationkey, n_name -> name, n_regionkey -> regionkey
    如果你以后也要对 region(r_) 做同样处理，可以扩展这里。
    """
    if base_table == "nation" and col.startswith("n_"):
        return col[2:]
    return col


def build_select_list_pretty_nation_only(cur, syms: Tuple[str, ...], alias: Dict[str, str], schema: str = "public") -> str:
    """
    目标：
      - 普通表：alias.*
      - 只有 nation 多实例（n1/n2）时：
          n1."n_nationkey" AS n1_nationkey
          n2."n_nationkey" AS n2_nationkey
        也就是 alias + "_" + 去掉 n_ 前缀后的列名
    """
    base_tables = [TABLE_MAP[s] for s in syms]
    cnt = Counter(base_tables)

    parts: List[str] = []
    for s in syms:
        tbl = TABLE_MAP[s]
        al = alias[s]

        if cnt[tbl] == 1:
            parts.append(f"{al}.*")
        else:
            # 同一 base table 出现多次（你这里主要是 nation）
            cols = get_table_columns(cur, tbl, schema=schema)
            for col in cols:
                out_col = strip_table_prefix(col, tbl)     # n_nationkey -> nationkey
                parts.append(f'{al}."{col}" AS {al}_{out_col}')

    return ",\n        ".join(parts)

# =========================
# EXPLAIN explosion check
# =========================
EXPLAIN_ROWS_RE = re.compile(r"rows=(\d+)", re.IGNORECASE)

def estimate_rows_by_explain(cur, select_sql: str) -> int:
    cur.execute("EXPLAIN " + select_sql)
    lines = [r[0] for r in cur.fetchall()]
    max_rows = 0
    for line in lines:
        m = EXPLAIN_ROWS_RE.search(line)
        if m:
            max_rows = max(max_rows, int(m.group(1)))
    return max_rows


# =========================
# Create view for one node_set
# =========================
def create_view_for_node_set(cur, conn, qid: int, full_edges: List[Tuple[str, str]], node_set: Set[str]):
    syms = tuple(sorted(node_set, key=lambda x: (sym_rank(x), x)))
    alias = aliases_for_symbols(syms)

    es = induced_edges(full_edges, node_set)
    view = view_name(qid, node_set)

    from_join_clause = build_from_join_clause(syms, es, alias)

    # ✅ 关键：只对重复 base table（nation 多实例）展开并改名为 n1_nationkey 风格
    select_list = build_select_list_pretty_nation_only(cur, syms, alias, schema="public")

    select_sql = f"""
        SELECT
            {select_list}
        FROM {from_join_clause}
        """.strip()

    # ✅ EXPLAIN pre-check
    try:
        est_rows = estimate_rows_by_explain(cur, select_sql)
        print(f"📈 Q{qid} estimated_rows = {est_rows}")
        if est_rows >= MAX_ESTIMATED_ROWS:
            mark = exceed_mark_path(qid, node_set)
            mark.touch()
            print(f"💥 Skip CREATE (explosion risk): est_rows={est_rows} >= {MAX_ESTIMATED_ROWS} -> mark exceed: {mark.name}")
            return
    except Exception as e:
        print(f"⚠️ EXPLAIN failed, fallback to timeout-only. err={type(e).__name__}: {e}")

    drop_sql = sql.SQL("DROP VIEW IF EXISTS {} CASCADE;").format(sql.Identifier(view))
    create_sql_str = f"""
    CREATE VIEW {view} AS
    {select_sql};
    """.strip()

    print("\n==============================================================")
    print(f"Q{qid} 🧩 nodes = {syms}")
    print(f"Q{qid} 🔗 edges = {es}")
    print(f"Q{qid} 🧱 VIEW  = {view}")
    print(create_sql_str)
    print("==============================================================")

    try:
        cur.execute(drop_sql)
        cur.execute(create_sql_str)
        conn.commit()
        print(f"✅ Created: {view}")
    except psycopg2.errors.QueryCanceled:
        conn.rollback()
        mark = exceed_mark_path(qid, node_set)
        mark.touch()
        print(f"⏱️ Timeout ({STATEMENT_TIMEOUT}) -> mark exceed: {mark.name}")
    except Exception:
        conn.rollback()
        raise


# =========================
# Main
# =========================
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        print("✅ Connected to PostgreSQL")
        # cur.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}';")
        # conn.commit()
        # print(f"⏱️ statement_timeout = {STATEMENT_TIMEOUT}")

        cur.execute("SET statement_timeout = 0;")  # 0 = no timeout
        conn.commit()
        print("⏱️ statement_timeout disabled (0)")

        # 只清理当前实验生成的普通 view，不影响 materialized view
        drop_views(cur, conn, schema="public", name_like=DROP_NAME_LIKE)

        for qid, full_edges in groups:
            full_edges = [canonical_edge(e) for e in full_edges]

            node_sets = enumerate_connected_node_sets(full_edges)
            print(f"\n🧮 Q{qid}: connected node-sets = {len(node_sets)}")

            for node_set in node_sets:
                create_view_for_node_set(cur, conn, qid, full_edges, node_set)

        print("\nAll Done 🎉")

    finally:
        cur.close()
        conn.close()
        print("🔌 Connection closed.")
