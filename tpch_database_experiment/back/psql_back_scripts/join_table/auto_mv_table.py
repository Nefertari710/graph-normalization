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

DROP_NAME_LIKE = "q%_mv_%"  # 只删实验 MV；想删全部 public MV 就改成 None

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
    (22, [("C", "O")]),
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


MV_RULES = {
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
    if (a, b) in MV_RULES:
        return MV_RULES[(a, b)]
    if (b, a) in MV_RULES:
        k1, k2 = MV_RULES[(b, a)]
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


# =========================
# Naming
# =========================
def mv_name(qid: int, node_set: Set[str]) -> str:
    syms = tuple(sorted(node_set, key=lambda x: (sym_rank(x), x)))
    tables_part = "_".join([DISPLAY_NAME_MAP[s] for s in syms])
    return safe_ident(f"q{qid}_mv_{tables_part}")


def exceed_mark_path(qid: int, node_set: Set[str]) -> Path:
    syms = tuple(sorted(node_set, key=lambda x: (sym_rank(x), x)))
    return EXCEED_DIR / safe_ident(f"Q{qid}_" + "_".join(syms) + "_exceed")


# =========================
# DB helpers
# =========================
def drop_mvs(cur, conn, schema: str = "public", name_like: Optional[str] = None):
    if name_like is None:
        cur.execute(
            """
            SELECT schemaname, matviewname
            FROM pg_matviews
            WHERE schemaname=%s
            ORDER BY matviewname;
            """,
            (schema,)
        )
    else:
        cur.execute(
            """
            SELECT schemaname, matviewname
            FROM pg_matviews
            WHERE schemaname=%s AND matviewname LIKE %s
            ORDER BY matviewname;
            """,
            (schema, name_like)
        )
    rows = cur.fetchall()

    if not rows:
        print("ℹ️ No materialized views to drop.")
        return

    print("\n🧹 Materialized views to drop:")
    print("--------------------------------------------------")
    for schemaname, matviewname in rows:
        print(f"{schemaname}.{matviewname}")
    print("--------------------------------------------------")

    for schemaname, matviewname in rows:
        cur.execute(
            sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {}.{} CASCADE;").format(
                sql.Identifier(schemaname),
                sql.Identifier(matviewname)
            )
        )
    conn.commit()
    print(f"\n✅ Dropped {len(rows)} materialized view(s).")


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







# ============================================================
# Index config (适配你的 MV 输出列名，支持 N1/N2)
# ============================================================

# 注意：这里按“符号”定义 PK/FK（而不是按 base table）
PK_COLS_BY_SYM: Dict[str, Tuple[str, ...]] = {
    "R":  ("r_regionkey",),
    "N":  ("n_nationkey",),
    "N1": ("n_nationkey",),
    "N2": ("n_nationkey",),

    "S":  ("s_suppkey",),
    "C":  ("c_custkey",),
    "P":  ("p_partkey",),
    "PS": ("ps_partkey", "ps_suppkey"),
    "O":  ("o_orderkey",),
    "L":  ("l_orderkey", "l_linenumber"),
}

FK_COLS_BY_SYM: Dict[str, List[Tuple[str, ...]]] = {
    "S":  [("s_nationkey",)],
    "C":  [("c_nationkey",)],
    "O":  [("o_custkey",)],
    "L":  [("l_orderkey",), ("l_partkey",), ("l_suppkey",)],
    "PS": [("ps_partkey",), ("ps_suppkey",)],

    # nation -> region
    "N":  [("n_regionkey",)],
    "N1": [("n_regionkey",)],
    "N2": [("n_regionkey",)],
}


def _strip_prefix_for_nation(col: str) -> str:
    """
    你希望：n_nationkey -> nationkey, n_regionkey -> regionkey, n_name -> name
    """
    return col[2:] if col.startswith("n_") else col


def _mv_output_col_name(sym: str, base_table: str, alias_map: Dict[str, str], col: str, base_table_is_duplicated: bool) -> str:
    """
    把“原表列名”映射为 MV 输出列名。

    规则：
    - 如果该 base_table 没重复：输出列名就是原列名（例如 c_custkey）
    - 如果该 base_table 重复（你的场景主要是 nation 多实例）：
        输出列名 = <alias>_<去掉 n_ 前缀后的列名>
        例如：sym=N1, alias=n1, col=n_nationkey => n1_nationkey
    """
    if not base_table_is_duplicated:
        return col

    # 目前只需要对 nation 重复做“去前缀”
    if base_table == "nation":
        return f"{alias_map[sym]}_{_strip_prefix_for_nation(col)}"

    # 如果未来有其它重复表，也可以在这里扩展
    return f"{alias_map[sym]}_{col}"


def collect_index_columns_for_mv_output(
    syms: Tuple[str, ...],
    alias_map: Dict[str, str],
) -> List[Tuple[str, ...]]:
    """
    生成“应该在 MV 上建索引的列组合”，列名必须与 MV 输出列名一致：
      - 普通表：c_custkey / s_suppkey ...
      - nation 多实例：n1_nationkey / n2_regionkey ...
    """
    base_tables = [TABLE_MAP[s] for s in syms]
    base_cnt = Counter(base_tables)

    cols: List[Tuple[str, ...]] = []

    for s in syms:
        base_tbl = TABLE_MAP[s]
        duplicated = base_cnt[base_tbl] > 1

        # PK
        pk = PK_COLS_BY_SYM.get(s)
        if pk:
            mapped_pk = tuple(_mv_output_col_name(s, base_tbl, alias_map, c, duplicated) for c in pk)
            cols.append(mapped_pk)
            # 复合 PK 拆单列（你原来脚本的行为）
            if len(mapped_pk) > 1:
                for c in mapped_pk:
                    cols.append((c,))

        # FK
        for fk in FK_COLS_BY_SYM.get(s, []):
            mapped_fk = tuple(_mv_output_col_name(s, base_tbl, alias_map, c, duplicated) for c in fk)
            cols.append(mapped_fk)

    # 去重保持顺序
    seen = set()
    out: List[Tuple[str, ...]] = []
    for t in cols:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def create_indexes_for_table(
    cur,
    conn,
    table_name: str,
    syms: Tuple[str, ...],
    alias_map: Dict[str, str],
    schema: str = "public",
):
    """
    在 MV / join table 上创建索引
    index name: idx_<tablesymbols>_<columns>
    例如: idx_loc_c_custkey
    """

    idx_cols = collect_index_columns_for_mv_output(syms, alias_map)

    # 表 symbol 前缀，例如 LOCNR
    sym_prefix = "".join(s.lower() for s in syms)

    full_table = sql.SQL("{}.{}").format(
        sql.Identifier(schema),
        sql.Identifier(table_name)
    )

    print(f"\n🧷 Creating indexes for {schema}.{table_name}")
    print("--------------------------------------------------")

    for col_tuple in idx_cols:

        col_part = "_".join(col_tuple)

        # index name
        idx_name = safe_ident(f"idx_q{qid}_{sym_prefix}_{col_part}")

        cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in col_tuple)

        q = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
            sql.Identifier(idx_name),
            full_table,
            cols_sql
        )

        # 输出
        # print(f"{idx_name}  ->  ({', '.join(col_tuple)})")
        print(q.as_string(conn))

        cur.execute(q)

    conn.commit()

    print("--------------------------------------------------")
    print(f"✅ Index done: {schema}.{table_name}")

# Create index finish


















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
# Create MV for one node_set
# =========================
def create_mv_for_node_set(cur, conn, qid: int, full_edges: List[Tuple[str, str]], node_set: Set[str]):
    syms = tuple(sorted(node_set, key=lambda x: (sym_rank(x), x)))
    alias = aliases_for_symbols(syms)

    es = induced_edges(full_edges, node_set)
    mv = mv_name(qid, node_set)

    from_clause = ", ".join([f"{TABLE_MAP[s]} {alias[s]}" for s in syms])
    where_clause = " AND ".join(build_where_predicates(es, alias))

    # ✅ 关键：只对重复 base table（nation 多实例）展开并改名为 n1_nationkey 风格
    select_list = build_select_list_pretty_nation_only(cur, syms, alias, schema="public")

    select_sql = f"""
    SELECT
        {select_list}
    FROM {from_clause}
    WHERE {where_clause}
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

    drop_sql = sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;").format(sql.Identifier(mv))
    create_sql_str = f"""
    CREATE MATERIALIZED VIEW {mv} AS
    {select_sql};
    """.strip()

    print("\n==============================================================")
    print(f"Q{qid} 🧩 nodes = {syms}")
    print(f"Q{qid} 🔗 edges = {es}")
    print(f"Q{qid} 🧱 MV    = {mv}")
    print(create_sql_str)
    print("==============================================================")

    try:
        cur.execute(drop_sql)
        cur.execute(create_sql_str)
        conn.commit()
        print(f"✅ Created: {mv}")

        cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(mv)))
        conn.commit()
        # Create index
        create_indexes_for_table(cur, conn, mv, syms, alias, schema="public")
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

        # ✅ drop MVs first
        drop_mvs(cur, conn, schema="public", name_like=DROP_NAME_LIKE)

        for qid, full_edges in groups:
            full_edges = [canonical_edge(e) for e in full_edges]

            node_sets = enumerate_connected_node_sets(full_edges)
            print(f"\n🧮 Q{qid}: connected node-sets = {len(node_sets)}")

            for node_set in node_sets:
                create_mv_for_node_set(cur, conn, qid, full_edges, node_set)

        print("\nAll Done 🎉")

    finally:
        cur.close()
        conn.close()
        print("🔌 Connection closed.")