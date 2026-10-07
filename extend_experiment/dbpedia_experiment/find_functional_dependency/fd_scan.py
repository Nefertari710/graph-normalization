"""Disk-backed, exact class scans for find_functional_dependency.py.

Only the current Neo4j record, a bounded term cache, and field statistics stay
in Python memory. Temporary SQLite data are removed after each class, including
when a query fails or the user interrupts the scan. No database writes are made.
"""

from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter
from contextlib import closing
from functools import lru_cache
from itertools import product
from pathlib import Path
from tempfile import TemporaryDirectory

from neo4j import Query


def _create_store(db):
    # This is disposable scratch data; durability would only add disk traffic.
    db.executescript("""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        PRAGMA temp_store=FILE;
        PRAGMA cache_size=-32768;
        CREATE TABLE subjects (row INTEGER PRIMARY KEY, uri TEXT NOT NULL, name TEXT NOT NULL);
        CREATE TABLE terms (id INTEGER PRIMARY KEY, identity TEXT NOT NULL UNIQUE);
        CREATE TABLE cells (
            field INTEGER NOT NULL, row INTEGER NOT NULL, value INTEGER NOT NULL,
            PRIMARY KEY (field, row)
        ) WITHOUT ROWID;
    """)


def _load_class(db, session, class_uri, args, helpers):
    fields, scalar, multivalued = {}, Counter(), Counter()

    @lru_cache(maxsize=10_000)
    def term_id(kind, term):
        # Preserve the complete RDF identity, including datatype and language.
        identity = json.dumps((kind, term), ensure_ascii=False, separators=(",", ":"))
        db.execute("INSERT OR IGNORE INTO terms(identity) VALUES (?)", (identity,))
        return db.execute("SELECT id FROM terms WHERE identity=?", (identity,)).fetchone()[0]

    query = f"""
        MATCH (n:{helpers.quoted_label(class_uri)})
        RETURN n.uri AS uri, n[$label_key] AS names, n.rdf_literals AS literals
    """
    total = 0
    for record in session.run(Query(query, timeout=args.timeout), label_key=helpers.RDFS_LABEL):
        if not record["uri"]:
            raise ValueError("A class instance has no URI; check the imported graph.")
        names = record["names"] or []
        db.execute("INSERT INTO subjects VALUES (?, ?, ?)",
                   (total, record["uri"], names[0] if names else helpers.short(record["uri"])))
        cells = []
        for field, values in helpers.fields_from_record(record).items():
            field_id = fields.setdefault(field, len(fields))
            if len(values) == 1:
                cells.append((field_id, total, term_id(field[0], next(iter(values)))))
                scalar[field] += 1
            else:
                multivalued[field] += 1
        db.executemany("INSERT INTO cells VALUES (?, ?, ?)", cells)
        total += 1
        if total % 10_000 == 0:
            db.commit()
            print(f"[read] {helpers.short(class_uri)}: {total:,} instances (temporary disk storage)", flush=True)
    term_id.cache_clear()
    db.commit()
    print(f"[index] {helpers.short(class_uri)}: {total:,} instances; indexing scalar fields...", flush=True)
    db.execute("CREATE INDEX cells_by_value ON cells(field, value, row)")
    db.commit()
    return total, fields, scalar, multivalued


# CROSS JOIN fixes the loop order: stream X by value, then look up Y by its
# (field,row) primary key. Large classes do not require an in-memory join/sort.
_PAIR_FROM = """
    FROM cells AS x INDEXED BY cells_by_value
    CROSS JOIN cells AS y ON y.field=? AND y.row=x.row
    WHERE x.field=?
"""


def _check_pair(db, x_id, y_id, total, args):
    threshold = max(args.min_support, math.ceil(total * args.min_coverage))
    eligible = distinct_x = repeated = repeated_rows = 0
    identical, examples = True, []
    query = "SELECT x.value, count(*), min(y.value), max(y.value) " + _PAIR_FROM + " GROUP BY x.value"
    with closing(db.execute(query, (y_id, x_id))) as groups:
        for x, count, y_min, y_max in groups:
            if y_min != y_max:
                return None
            eligible += count
            distinct_x += 1
            identical = identical and x == y_min
            if count > 1:
                repeated += 1
                repeated_rows += count
                # Term IDs reflect insertion order, so they are not a stable RDF
                # ordering. Use the decoded term just like the in-memory engine.
                x_sort_key = repr(tuple(_read_term(db, x)))
                examples.append((count, x_sort_key, x, y_min))
                examples.sort(key=lambda item: (-item[0], item[1]))
                del examples[2:]
    if eligible < threshold or repeated < args.min_repeated_groups or identical:
        return None
    # SQLite uses disk-backed temporary storage for distinct values; Python does
    # not accumulate an unbounded set of all dependent values.
    distinct_y = db.execute("SELECT count(DISTINCT y.value) " + _PAIR_FROM, (y_id, x_id)).fetchone()[0]
    if distinct_y < 2:
        return None
    return {
        "eligible_instances": eligible, "coverage": eligible / total,
        "distinct_x": distinct_x, "distinct_y": distinct_y,
        "repeated_x_groups": repeated, "instances_in_repeated_groups": repeated_rows,
        "redundant_rows": eligible - distinct_x, "violating_x_groups": 0,
        "examples": [
            {"group_size": count, "x_id": x, "y_id": y}
            for count, _, x, y in examples
        ],
    }


def _field_info(field, scalar, multivalued, total, helpers):
    return {"kind": field[0], "uri": field[1], "name": helpers.short(field[1]),
            "single_valued_instances": scalar[field], "multivalued_instances": multivalued[field],
            "missing_instances": total - scalar[field] - multivalued[field]}


def _decorate(db, result, class_uri, x_field, y_field, fields, scalar, multivalued, total, helpers):
    for example in result["examples"]:
        x_term_id = example.pop("x_id")
        y_term_id = example.pop("y_id")
        for side, field, term_id in (("x", x_field, x_term_id), ("y", y_field, y_term_id)):
            identity = db.execute("SELECT identity FROM terms WHERE id=?", (term_id,)).fetchone()[0]
            _, term = json.loads(identity)
            example[side] = helpers.term_info(field, term)
        rows = db.execute("""
            SELECT s.uri, s.name FROM cells AS x
            JOIN cells AS y ON y.field=? AND y.row=x.row
            JOIN subjects AS s ON s.row=x.row
            WHERE x.field=? AND x.value=? ORDER BY x.row LIMIT 3
        """, (fields[y_field], fields[x_field], x_term_id))
        example["instances"] = [{"uri": uri, "name": name} for uri, name in rows]
    result.update({
        "class_uri": class_uri, "class_name": helpers.short(class_uri), "class_instances": total,
        "scope": "whole_class" if result["eligible_instances"] == total else "complete_single_valued_subset",
        "excluded_instances": total - result["eligible_instances"],
        "x": _field_info(x_field, scalar, multivalued, total, helpers),
        "y": _field_info(y_field, scalar, multivalued, total, helpers),
    })
    return result


def _multi_from(y_count):
    joins = " ".join(
        f"JOIN cells AS y{index} ON y{index}.field=? AND y{index}.row=x.row"
        for index in range(y_count)
    )
    return f"FROM cells AS x INDEXED BY cells_by_value {joins} WHERE x.field=?"


def _check_multi(db, x_id, y_ids, total, args):
    """Check X -> a Y tuple by streaming the common rows in X order."""
    if len(y_ids) > 63:
        raise ValueError("SQLite can check at most 63 dependent fields in one 1-to-N candidate")
    from_sql = _multi_from(len(y_ids))
    y_select = ", ".join(f"y{index}.value" for index in range(len(y_ids)))
    query = f"SELECT x.row, x.value, {y_select} {from_sql} ORDER BY x.value, x.row"
    parameters = (*y_ids, x_id)
    threshold = max(args.min_support, math.ceil(total * args.min_coverage))
    eligible = distinct_x = repeated = repeated_rows = 0
    examples = []
    current_x = current_ys = None
    first_ys = None
    y_varies = [False] * len(y_ids)
    y_differs_from_x = [False] * len(y_ids)
    group_count = 0
    group_rows = []

    def finish_group():
        nonlocal distinct_x, repeated, repeated_rows
        if group_count == 0:
            return
        distinct_x += 1
        if group_count > 1:
            repeated += 1
            repeated_rows += group_count
            x_sort_key = repr(tuple(_read_term(db, current_x)))
            examples.append(
                (group_count, x_sort_key, current_x, current_ys, tuple(group_rows))
            )
            examples.sort(key=lambda item: (-item[0], item[1]))
            del examples[2:]

    with closing(db.execute(query, parameters)) as rows:
        for row, x_value, *ys in rows:
            ys = tuple(ys)
            if first_ys is None:
                first_ys = ys
            for position, y_value in enumerate(ys):
                y_varies[position] = y_varies[position] or y_value != first_ys[position]
                y_differs_from_x[position] = y_differs_from_x[position] or y_value != x_value
            eligible += 1
            if current_x != x_value:
                finish_group()
                current_x, current_ys, group_count, group_rows = x_value, ys, 1, [row]
            else:
                if current_ys != ys:
                    return None
                group_count += 1
                if len(group_rows) < 3:
                    group_rows.append(row)
    finish_group()
    if (eligible < threshold or repeated < args.min_repeated_groups
            or not all(y_varies) or not all(y_differs_from_x)):
        return None
    distinct_query = f"SELECT count(*) FROM (SELECT 1 {from_sql} GROUP BY {y_select})"
    distinct_y_tuples = db.execute(distinct_query, parameters).fetchone()[0]
    if distinct_y_tuples < 2:
        return None
    return {
        "eligible_instances": eligible, "coverage": eligible / total,
        "distinct_x": distinct_x, "distinct_y_tuples": distinct_y_tuples,
        "repeated_x_groups": repeated, "instances_in_repeated_groups": repeated_rows,
        "redundant_rows": eligible - distinct_x, "violating_x_groups": 0,
        "examples": [
            {"group_size": count, "x_id": x, "y_ids": ys, "rows": rows}
            for count, _, x, ys, rows in examples
        ],
    }


def _read_term(db, term_id):
    identity = db.execute("SELECT identity FROM terms WHERE id=?", (term_id,)).fetchone()[0]
    return json.loads(identity)[1]


def _decorate_multi(db, result, class_uri, x_field, y_fields,
                    scalar, multivalued, total, helpers):
    for example in result["examples"]:
        example["x"] = helpers.term_info(x_field, _read_term(db, example.pop("x_id")))
        y_ids = example.pop("y_ids")
        example["ys"] = [
            {"field": {"kind": field[0], "uri": field[1], "name": helpers.short(field[1])},
             "value": helpers.term_info(field, _read_term(db, term_id))}
            for field, term_id in zip(y_fields, y_ids)
        ]
        row_ids = example.pop("rows")
        placeholders = ",".join("?" for _ in row_ids)
        rows = db.execute(
            f"SELECT uri, name FROM subjects WHERE row IN ({placeholders}) ORDER BY row", row_ids,
        )
        example["instances"] = [{"uri": uri, "name": name} for uri, name in rows]
    result.update({
        "class_uri": class_uri, "class_name": helpers.short(class_uri), "class_instances": total,
        "dependent_count": len(y_fields),
        "scope": "whole_class" if result["eligible_instances"] == total else "complete_single_valued_subset",
        "excluded_instances": total - result["eligible_instances"],
        "x": _field_info(x_field, scalar, multivalued, total, helpers),
        "ys": [_field_info(field, scalar, multivalued, total, helpers) for field in y_fields],
    })
    return result


def _find_multi(db, class_uri, findings, fields, scalar, multivalued, total, args, helpers):
    results = []
    for x_field, candidate_ys in helpers.multi_candidate_groups(findings, args.min_dependents):
        def evaluate(y_fields):
            return _check_multi(db, fields[x_field], tuple(fields[field] for field in y_fields), total, args)

        for y_fields, result in helpers.maximal_qualifying_rhs(
                candidate_ys, args.min_dependents, evaluate):
            results.append(_decorate_multi(
                db, result, class_uri, x_field, y_fields,
                scalar, multivalued, total, helpers,
            ))
    results.sort(key=lambda row: (-row["dependent_count"], -row["coverage"],
                                  row["x"]["uri"], tuple(field["uri"] for field in row["ys"])))
    return results


def scan_class(session, class_uri, args, helpers=None):
    """Check all qualifying ordered field pairs in every instance of one class.

    Return (unary findings, multi-RHS findings, instance count, pair count).
    ``helpers`` can be the caller
    module (``sys.modules[__name__]``) so parsing/display logic remains shared.
    ``args.temp_dir`` optionally chooses the scratch volume; ``args.timeout``
    applies to the full Neo4j stream (0 disables its transaction timeout).
    """
    if helpers is None:
        import find_functional_dependency as helpers
    temp_dir = getattr(args, "temp_dir", None)
    if temp_dir is not None:
        Path(temp_dir).mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="dbpedia-fd-", dir=temp_dir) as scratch:
        with closing(sqlite3.connect(Path(scratch) / "class.sqlite3")) as db:
            _create_store(db)
            total, fields, scalar, multivalued = _load_class(db, session, class_uri, args, helpers)
            threshold = max(args.min_support, math.ceil(total * args.min_coverage))
            determinants, dependents = [], []
            for field in sorted(fields):
                if scalar[field] < threshold:
                    continue
                distinct, repeated = db.execute("""
                    SELECT count(*), coalesce(sum(n > 1), 0) FROM (
                        SELECT count(*) AS n FROM cells WHERE field=? GROUP BY value
                    )
                """, (fields[field],)).fetchone()
                if distinct >= 2:
                    dependents.append(field)
                    if repeated >= args.min_repeated_groups:
                        determinants.append(field)
            possible = len(determinants) * len(dependents) - len(set(determinants) & set(dependents))
            print(f"[pairs] {helpers.short(class_uri)}: {possible:,} candidate directions after safe support pruning", flush=True)
            findings, pair_count = [], 0
            for x_field, y_field in product(determinants, dependents):
                if x_field == y_field:
                    continue
                pair_count += 1
                result = _check_pair(db, fields[x_field], fields[y_field], total, args)
                if result is not None:
                    findings.append(_decorate(db, result, class_uri, x_field, y_field, fields,
                                              scalar, multivalued, total, helpers))
                if pair_count % 25 == 0 or pair_count == possible:
                    print(f"[pairs] {helpers.short(class_uri)}: {pair_count:,}/{possible:,}; {len(findings):,} dependencies", flush=True)
            findings.sort(key=lambda row: (-row["coverage"], -row["redundant_rows"],
                                            row["x"]["uri"], row["y"]["uri"]))
            multi_findings = _find_multi(
                db, class_uri, findings, fields, scalar, multivalued, total, args, helpers,
            )
            return findings, multi_findings, total, pair_count
