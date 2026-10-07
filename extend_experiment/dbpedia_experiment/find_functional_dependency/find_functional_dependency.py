#!/usr/bin/env python3
"""Find node-internal X -> Y and X -> {Y1, ..., Yn} property dependencies.

One row is one entity and each candidate field is an RDF literal node property.
Only rows with exactly one distinct RDF literal in every participating property are eligible.
Every eligible row in a class is checked; no sampling or LIMIT is used.
Missing/multivalued rows are reported, not treated as evidence for an FD.
Connection settings are defined below; this script never imports or writes data.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import sys
import sqlite3
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import permutations
from pathlib import Path
from urllib.parse import unquote

from neo4j import GraphDatabase, Query, READ_ACCESS
from neo4j.exceptions import Neo4jError, ServiceUnavailable, SessionExpired

# Neo4j connection defaults. Environment variables and command-line options
# override these values. Leave the password empty to enter it securely at run time.
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""
NEO4J_DATABASE = "dbpedia1"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

TARGET_COUNT = 0  # 0 = keep searching; no result limit
MIN_SUPPORT = 20
MIN_COVERAGE = 0.2
MIN_REPEATED_GROUPS = 3
MIN_DEPENDENTS = 2
MAX_CLASS_SIZE = 0  # 0 = include classes of every size
MAX_PER_CLASS = 0  # 0 = keep all qualifying ordered dependencies
MEMORY_CLASS_SIZE = 100_000  # Larger classes use temporary SQLite, not a skip.
QUERY_TIMEOUT = 0  # 0 = no transaction timeout for a full class stream
OUTPUT = Path(__file__).with_name("functional_dependencies_all.json")
REPORT_VERSION = 5
INTERNAL_LABELS = {"DBpediaResource", "DBpediaImport"}
ONTOLOGY = "http://dbpedia.org/ontology/"
# Names and identifiers are poor evidence of repeated, reusable information.
EXCLUDED_NAMES = {
    "label", "name", "alias", "title", "abstract", "description", "comment",
    "wikiPageID", "wikiPageRevisionID", "wikiPageWikiLink", "wikiPageExternalLink",
    "wikiPageRedirects", "wikiPageDisambiguates", "sameAs", "thumbnail", "depiction",
}


def short(uri):
    """Readable display only; comparisons always use the original full URI."""
    return unquote(uri.rsplit("/", 1)[-1].rsplit("#", 1)[-1])


def quoted_label(uri):
    return "`" + uri.replace("\\u0060", "`").replace("`", "``") + "`"


def fields_from_record(record):
    """Return node properties while preserving RDF datatype/language identity."""
    fields = defaultdict(set)
    for raw in record["literals"] or []:
        predicate, value, datatype, language = json.loads(raw)
        if short(predicate) not in EXCLUDED_NAMES:
            fields[("literal", predicate)].add((value, datatype, language))
    return fields


def read_class(session, class_uri, timeout):
    """Read all instances of one explicit class and their literal properties."""
    query = f"""
        MATCH (n:{quoted_label(class_uri)})
        RETURN n.uri AS uri, n[$label_key] AS names,
               n.rdf_literals AS literals
    """
    subjects, columns, multivalued = [], defaultdict(dict), Counter()
    for record in session.run(Query(query, timeout=timeout), label_key=RDFS_LABEL):
        if not record["uri"]:
            raise ValueError("A class instance has no URI; check the imported graph.")
        index = len(subjects)
        names = record["names"] or []
        subjects.append({"uri": record["uri"], "name": names[0] if names else short(record["uri"])})
        for field, values in fields_from_record(record).items():
            if len(values) == 1:
                columns[field][index] = next(iter(values))
            else:
                multivalued[field] += 1
        if len(subjects) % 10_000 == 0:
            print(f"[read] {short(class_uri)}: {len(subjects):,} instances", flush=True)
    return subjects, columns, multivalued


def check_dependency(x_values, y_values, total, min_support, min_coverage, min_repeated_groups):
    """Return exact statistics, or None for a violation/insufficient evidence.

    Keys are entity row numbers; values are immutable RDF terms. Columns already
    contain only single-valued cells. A unique X and a constant Y are filtered.
    """
    eligible = x_values.keys() & y_values.keys()
    if len(eligible) < max(min_support, math.ceil(total * min_coverage)):
        return None
    mapping, counts = {}, Counter()
    identical = True
    for index in eligible:
        x, y = x_values[index], y_values[index]
        if x in mapping and mapping[x] != y:
            return None
        mapping[x] = y
        counts[x] += 1
        identical = identical and x == y
    repeated = [x for x, count in counts.items() if count > 1]
    if identical or len(set(mapping.values())) < 2 or len(repeated) < min_repeated_groups:
        return None
    example_groups = []
    for x in sorted(repeated, key=lambda value: (-counts[value], repr(value)))[:2]:
        members = sorted(index for index in eligible if x_values[index] == x)
        example_groups.append({"x": x, "y": mapping[x], "group_size": counts[x], "rows": members[:3]})
    return {
        "eligible_instances": len(eligible),
        "coverage": len(eligible) / total,
        "distinct_x": len(mapping),
        "distinct_y": len(set(mapping.values())),
        "repeated_x_groups": len(repeated),
        "instances_in_repeated_groups": sum(counts[x] for x in repeated),
        "redundant_rows": len(eligible) - len(mapping),
        "violating_x_groups": 0,
        "examples": example_groups,
    }


def field_info(field, values, multivalued, total):
    return {"kind": field[0], "uri": field[1], "name": short(field[1]),
            "single_valued_instances": len(values), "multivalued_instances": multivalued,
            "missing_instances": total - len(values) - multivalued}


def term_info(_field, term):
    """Describe one RDF literal value from a node property."""
    return {"value": term[0], "datatype": term[1], "language": term[2]}


def multi_candidate_groups(findings, min_dependents):
    """Group exact unary X -> Y findings into candidate X -> {Y1, ..., Yn}."""
    grouped = defaultdict(set)
    for finding in findings:
        x_field = (finding["x"]["kind"], finding["x"]["uri"])
        y_field = (finding["y"]["kind"], finding["y"]["uri"])
        grouped[x_field].add(y_field)
    return [
        (x_field, tuple(sorted(y_fields)))
        for x_field, y_fields in sorted(grouped.items())
        if len(y_fields) >= min_dependents
    ]


def maximal_qualifying_rhs(candidate_ys, min_dependents, evaluate):
    """Return maximal qualifying RHS sets, including valid sparse subsets.

    Adding dependent fields can only reduce the common eligible population. If
    the full set fails the evidence thresholds, smaller sets are tried. Once a
    set passes, its subsets are redundant and are not reported.
    """
    accepted, accepted_sets, visited = [], [], set()

    def visit(rhs):
        rhs = tuple(sorted(rhs))
        frozen = frozenset(rhs)
        if len(rhs) < min_dependents or frozen in visited:
            return
        visited.add(frozen)
        if any(frozen <= existing for existing in accepted_sets):
            return
        result = evaluate(rhs)
        if result is not None:
            accepted.append((rhs, result))
            accepted_sets.append(frozen)
            return
        for removed in rhs:
            visit(tuple(field for field in rhs if field != removed))

    visit(candidate_ys)
    accepted.sort(key=lambda item: (-len(item[0]), item[0]))
    return accepted


def check_multi_dependency(x_values, y_columns, total, min_support, min_coverage, min_repeated_groups):
    """Check X -> a tuple of Y values on their common scalar-valued rows."""
    eligible = set(x_values)
    for values in y_columns:
        eligible.intersection_update(values)
    if len(eligible) < max(min_support, math.ceil(total * min_coverage)):
        return None
    mapping, counts = {}, Counter()
    for index in sorted(eligible):
        x = x_values[index]
        ys = tuple(values[index] for values in y_columns)
        if x in mapping and mapping[x] != ys:
            return None
        mapping[x] = ys
        counts[x] += 1
    repeated = [x for x, count in counts.items() if count > 1]
    distinct_y_tuples = len(set(mapping.values()))
    # Every RHS field must contribute evidence in this exact common population:
    # at least two values, and it cannot merely repeat X.
    informative_ys = all(
        len({ys[position] for ys in mapping.values()}) >= 2
        and any(x != ys[position] for x, ys in mapping.items())
        for position in range(len(y_columns))
    )
    if not informative_ys or distinct_y_tuples < 2 or len(repeated) < min_repeated_groups:
        return None
    examples = []
    for x in sorted(repeated, key=lambda value: (-counts[value], repr(value)))[:2]:
        rows = [index for index in sorted(eligible) if x_values[index] == x][:3]
        examples.append({"x": x, "ys": mapping[x], "group_size": counts[x], "rows": rows})
    return {
        "eligible_instances": len(eligible), "coverage": len(eligible) / total,
        "distinct_x": len(mapping), "distinct_y_tuples": distinct_y_tuples,
        "repeated_x_groups": len(repeated),
        "instances_in_repeated_groups": sum(counts[x] for x in repeated),
        "redundant_rows": len(eligible) - len(mapping), "violating_x_groups": 0,
        "examples": examples,
    }


def decorate_multi_dependency(result, class_uri, x_field, y_fields, subjects,
                              columns, multivalued, total):
    """Attach readable fields and examples to a strict multi-RHS result."""
    for example in result["examples"]:
        example["x"] = term_info(x_field, example["x"])
        example["ys"] = [
            {"field": {"kind": field[0], "uri": field[1], "name": short(field[1])},
             "value": term_info(field, value)}
            for field, value in zip(y_fields, example["ys"])
        ]
        example["instances"] = [subjects[index] for index in example.pop("rows")]
    result.update({
        "class_uri": class_uri, "class_name": short(class_uri), "class_instances": total,
        "dependent_count": len(y_fields),
        "scope": "whole_class" if result["eligible_instances"] == total else "complete_single_valued_subset",
        "excluded_instances": total - result["eligible_instances"],
        "x": field_info(x_field, columns[x_field], multivalued[x_field], total),
        "ys": [field_info(field, columns[field], multivalued[field], total) for field in y_fields],
    })
    return result


def find_multi_in_class(class_uri, subjects, columns, multivalued, findings, args):
    """Find maximal strict X -> {Y1, ..., Yn} dependencies in one class."""
    total = len(subjects)
    results = []
    for x_field, candidate_ys in multi_candidate_groups(findings, args.min_dependents):
        def evaluate(y_fields):
            return check_multi_dependency(
                columns[x_field], [columns[field] for field in y_fields], total,
                args.min_support, args.min_coverage, args.min_repeated_groups,
            )

        for y_fields, result in maximal_qualifying_rhs(candidate_ys, args.min_dependents, evaluate):
            results.append(decorate_multi_dependency(
                result, class_uri, x_field, y_fields, subjects, columns, multivalued, total,
            ))
    results.sort(key=lambda row: (-row["dependent_count"], -row["coverage"],
                                  row["x"]["uri"], tuple(field["uri"] for field in row["ys"])))
    return results


def find_in_class(class_uri, subjects, columns, multivalued, args):
    total = len(subjects)
    threshold = max(args.min_support, math.ceil(total * args.min_coverage))
    fields = sorted(field for field, values in columns.items() if len(values) >= threshold)
    findings = []
    for x_field, y_field in permutations(fields, 2):
        result = check_dependency(columns[x_field], columns[y_field], total,
                                  args.min_support, args.min_coverage, args.min_repeated_groups)
        if result is None:
            continue
        for example in result["examples"]:
            example["x"] = term_info(x_field, example["x"])
            example["y"] = term_info(y_field, example["y"])
            example["instances"] = [subjects[index] for index in example.pop("rows")]
        result.update({
            "class_uri": class_uri, "class_name": short(class_uri), "class_instances": total,
            "scope": "whole_class" if result["eligible_instances"] == total else "complete_single_valued_subset",
            "excluded_instances": total - result["eligible_instances"],
            "x": field_info(x_field, columns[x_field], multivalued[x_field], total),
            "y": field_info(y_field, columns[y_field], multivalued[y_field], total),
        })
        findings.append(result)
    findings.sort(key=lambda row: (-row["coverage"], -row["redundant_rows"],
                                   row["x"]["uri"], row["y"]["uri"]))
    return findings


def import_status(session, labels, timeout):
    """Separate finished writes from RDF verification, including legacy markers."""
    if "DBpediaImport" not in labels:
        return {"complete": None, "verified": None, "manifest": None}
    record = session.run(Query("MATCH (m:DBpediaImport) RETURN properties(m) AS data", timeout=timeout)).single()
    data = record["data"] if record else {}
    # Older importers set complete only after their full RDF verification passed.
    verified = data.get("verified", data.get("complete"))
    return {"complete": data.get("complete"), "verified": verified, "manifest": data.get("manifest")}


def write_report(report, output):
    output.parent.mkdir(parents=True, exist_ok=True)
    report["last_saved_at"] = datetime.now(timezone.utc).isoformat()
    finished = {row["class_uri"] for row in report["classes_scanned"] + report["classes_skipped"]}
    report["classes_pending"] = sorted(set(report["inventory"]) - finished)
    # A completed class is the checkpoint unit. Incomplete classes are retried.
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(output)
    lines = ["# DBpedia node-property functional dependencies", "",
             f"Search status: **{report['status']}**. Classes scanned: {len(report['classes_scanned'])}; "
             f"excluded by thresholds/limits: {len(report['classes_skipped'])}; pending: {len(report['classes_pending'])}; "
             f"failed: {len(report['classes_failed'])}; single-RHS dependencies: {len(report['dependencies'])}; "
             f"1→N dependencies: {len(report.get('multi_dependencies', []))}.",
             f"Last saved (UTC): {report['last_saved_at']}", "",
             "Relational view: each explicit class is a relation, each node instance is one row, and each RDF literal predicate is an attribute.",
             "Search domain: node-internal RDF literal properties in all selected explicit classes, including non-DBpedia namespaces; "
             "single-property X → Y and X → {Y1, ..., Yn}.",
             "Outgoing relationships, node URI, RDF type, labels, and import metadata are not FD candidate fields.",
             "Here 1→N means one determinant property determines N dependent properties; it is not a graph one-to-many relationship.",
             "A multi-RHS dependency uses nodes where X and every Y are present and single-valued.",
             "Multi-RHS candidates combine qualifying single-RHS dependencies with the same X, then recheck their common population.",
             "Every listed Y must vary and must not merely repeat X in that common population; only maximal qualifying RHS sets are shown.",
             "Untyped entities and composite determinant properties are outside this class-scoped search.",
             f"Thresholds: support >= {report['settings']['min_support']}; coverage >= {report['settings']['min_coverage']:.0%}; "
             f"repeated X groups >= {report['settings']['min_repeated_groups']}. Names/metadata and trivial dependencies are excluded.",
             f"Limits (0 = unlimited): single-RHS total={report['settings']['count']}, class size={report['settings']['max_class_size']}, "
             f"per class for each result kind={report['settings']['max_per_class']}. Both qualifying directions are retained.",
             "Scope: each result checks every instance in its class with exactly one RDF literal in every participating property.",
             "Missing and multivalued instances are excluded and counted. No sampling is used.",
             "These are observed data dependencies, not guaranteed ontology/business rules.",
             f"Import complete at start: {report['import_at_start']['complete']}; latest observation: {report['import_at_end']['complete']}.",
             f"Post-import RDF verification recorded at start: {report['import_at_start']['verified']}; "
             f"latest observation: {report['import_at_end']['verified']}.",
             "Import completion records finished writes; RDF verification is a separate status.",
             "Classes use explicit labels only (no subclass inference). Literal comparison preserves datatype/language.",
             "Concurrent writes are not blocked. These results are observations, not an isolated database snapshot.",
             "Rerun the search after any data changes to refresh the observed dependencies.", ""]
    multi_dependencies = report.get("multi_dependencies", [])
    if multi_dependencies:
        lines.extend(["## One determinant → multiple dependent fields", "",
                      "Each row uses one common eligible population for X and every listed Y.", "",
                      "| # | Class | X → {Y1, …, Yn} | N | Eligible / class | Coverage | Repeated X groups |",
                      "|---|---|---|---:|---:|---:|---:|"])
        for index, finding in enumerate(multi_dependencies, 1):
            dependent_names = ", ".join(field["name"] for field in finding["ys"])
            lines.append(f"| M{index} | {finding['class_name']} | {finding['x']['name']} → "
                         f"{{{dependent_names}}} | {finding['dependent_count']} | "
                         f"{finding['eligible_instances']:,} / {finding['class_instances']:,} | "
                         f"{finding['coverage']:.1%} | {finding['repeated_x_groups']:,} |")
        for index, finding in enumerate(multi_dependencies, 1):
            dependent_names = ", ".join(field["name"] for field in finding["ys"])
            lines.extend(["", f"### M{index}. {finding['class_name']}: {finding['x']['name']} → {{{dependent_names}}}", "",
                          f"Class: `{finding['class_uri']}`", "",
                          f"X ({finding['x']['kind']}): `{finding['x']['uri']}`", "",
                          f"Common scope: `{finding['scope']}`; eligible instances: "
                          f"{finding['eligible_instances']:,} / {finding['class_instances']:,}; "
                          "conflicting X groups: 0.", "", "Dependent fields:", ""])
            lines.extend(f"- {field['name']} ({field['kind']}): `{field['uri']}`"
                         for field in finding["ys"])
            for example in finding["examples"]:
                values = ", ".join(
                    f"{item['field']['name']}={json.dumps(item['value'], ensure_ascii=False)}"
                    for item in example["ys"]
                )
                lines.extend(["", f"Example: X=`{json.dumps(example['x'], ensure_ascii=False)}` → "
                              f"{{{values}}} ({example['group_size']:,} instances share this X).", ""])
                lines.extend(f"- {node['name']}: `{node['uri']}`" for node in example["instances"])
        lines.extend(["", "## Single-dependent-field evidence", ""])
    lines.extend(["| # | Class | X → Y | Eligible / class | Coverage | Repeated X groups |",
                  "|---|---|---|---|---|---|"])
    for index, finding in enumerate(report["dependencies"], 1):
        lines.append(f"| {index} | {finding['class_name']} | {finding['x']['name']} → {finding['y']['name']} | "
                     f"{finding['eligible_instances']:,} / {finding['class_instances']:,} | "
                     f"{finding['coverage']:.1%} | {finding['repeated_x_groups']:,} |")
    for index, finding in enumerate(report["dependencies"], 1):
        lines.extend(["", f"## {index}. {finding['class_name']}: {finding['x']['name']} → {finding['y']['name']}", "",
                      f"Class: `{finding['class_uri']}`", "",
                      f"X ({finding['x']['kind']}): `{finding['x']['uri']}`", "",
                      f"Y ({finding['y']['kind']}): `{finding['y']['uri']}`", "",
                      f"Scope: `{finding['scope']}`; excluded instances: {finding['excluded_instances']:,}; conflicting X groups: 0.", ""])
        for side in ("x", "y"):
            field = finding[side]
            lines.append(f"- {side.upper()}: {field['missing_instances']:,} missing; {field['multivalued_instances']:,} multivalued.")
        for example in finding["examples"]:
            lines.extend(["", f"Example: `{json.dumps(example['x'], ensure_ascii=False)}` → `{json.dumps(example['y'], ensure_ascii=False)}` "
                          f"({example['group_size']:,} instances share this X).", ""])
            lines.extend(f"- {node['name']}: `{node['uri']}`" for node in example["instances"])
    if report["classes_failed"]:
        lines.extend(["", "## Classes requiring retry", ""])
        lines.extend(f"- `{row['class_uri']}`: {row['error']}" for row in report["classes_failed"])
    markdown = output.with_suffix(".md")
    temporary = markdown.with_suffix(".md.tmp")
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temporary.replace(markdown)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI") or NEO4J_URI)
    parser.add_argument("--user", default=os.getenv("NEO4J_USER") or NEO4J_USER)
    parser.add_argument("--database", default=os.getenv("NEO4J_DATABASE") or NEO4J_DATABASE)
    parser.add_argument("--count", type=int, default=TARGET_COUNT,
                        help="Single-RHS result limit; 0 (default) searches all classes.")
    parser.add_argument("--classes", nargs="+", help="Optional class names or full URIs; default: all explicit RDF classes.")
    parser.add_argument("--min-support", type=int, default=MIN_SUPPORT)
    parser.add_argument("--min-coverage", type=float, default=MIN_COVERAGE)
    parser.add_argument("--min-repeated-groups", type=int, default=MIN_REPEATED_GROUPS)
    parser.add_argument("--min-dependents", type=int, default=MIN_DEPENDENTS,
                        help="Minimum RHS fields in a 1-to-N dependency (default: 2).")
    parser.add_argument("--max-class-size", type=int, default=MAX_CLASS_SIZE, help="Optional size cap; 0 includes all sizes.")
    parser.add_argument("--max-per-class", type=int, default=MAX_PER_CLASS, help="Optional result cap per class; 0 keeps all.")
    parser.add_argument("--memory-class-size", type=int, default=MEMORY_CLASS_SIZE, help="Larger classes use temporary SQLite; 0 always uses SQLite.")
    parser.add_argument("--temp-dir", type=Path, help="Optional directory for temporary SQLite data.")
    parser.add_argument("--timeout", type=float, default=QUERY_TIMEOUT, help="Seconds per Neo4j query; 0 disables the transaction timeout.")
    parser.add_argument("--resume", action="store_true", help="Reuse completed classes from the matching output JSON. Assumes no graph writes.")
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args(argv)
    if not 0 < args.min_coverage <= 1:
        parser.error("--min-coverage must be in (0, 1].")
    if min(args.min_support, args.min_repeated_groups) <= 0:
        parser.error("Support and repeated-group thresholds must be positive.")
    if args.min_dependents < 2:
        parser.error("--min-dependents must be at least 2.")
    if min(args.count, args.max_class_size, args.max_per_class, args.memory_class_size, args.timeout) < 0:
        parser.error("Limits and timeout cannot be negative; use 0 for unlimited.")
    if args.output.suffix.lower() != ".json":
        parser.error("--output must end with .json (a matching .md is also written).")
    return args


def search_settings(args):
    return {"database": args.database, "classes": sorted(set(args.classes)) if args.classes else None,
            "field_scope": "node_internal_rdf_literal_properties",
            "count": args.count, "max_class_size": args.max_class_size, "max_per_class": args.max_per_class,
            "min_support": args.min_support, "min_coverage": args.min_coverage,
            "min_repeated_groups": args.min_repeated_groups, "min_dependents": args.min_dependents,
            "excluded_names": sorted(EXCLUDED_NAMES)}


def prepare_report(args, inventory, status):
    settings = search_settings(args)
    source_key = hashlib.sha256(json.dumps([args.uri, args.user, args.database]).encode()).hexdigest()
    if args.resume:
        report = json.loads(args.output.read_text(encoding="utf-8"))
        expected = {"report_version": REPORT_VERSION, "settings": settings, "source_key": source_key,
                    "inventory": inventory, "import_at_end": status}
        for key, value in expected.items():
            if report.get(key) != value:
                raise ValueError(f"Cannot resume: {key} changed. Rerun without --resume to start a fresh search.")
        print(f"[resume] Reusing {len(report['classes_scanned'])} completed classes; graph contents must be unchanged.", flush=True)
        report["classes_failed"] = []  # Failures remain pending and will be retried.
        report["status"] = "running"
        report["whole_database_search_complete"] = False
        report.pop("finished_at", None)
        report["resumed_at"] = datetime.now(timezone.utc).isoformat()
        return report
    return {"report_version": REPORT_VERSION, "started_at": datetime.now(timezone.utc).isoformat(),
              "database": args.database, "source_key": source_key, "inventory": inventory,
              "field_scope": "node_internal_rdf_literal_properties",
              "relational_view": "Each explicit class is a relation, each node instance is one row, and each RDF literal predicate is an attribute.",
              "scope_definition": "All instances with exactly one distinct RDF literal in X and every dependent node property, within an explicit class label; outgoing relationships and subclass inference are excluded.",
              "multi_rhs_method": "Combine qualifying single-RHS dependencies with the same X, revalidate exact Y tuples on their common scalar-valued population, and retain maximal qualifying RHS sets.",
              "comparison": "Exact RDF term identity, including literal datatype and language; no numeric coercion.",
              "consistency": "Concurrent writes are not blocked; no isolated database snapshot is guaranteed.",
              "requested_count": args.count, "settings": settings,
              "import_at_start": status, "import_at_end": status, "status": "running", "active_class": None,
              "whole_database_search_complete": False,
              "classes_scanned": [], "classes_skipped": [], "classes_failed": [],
              "dependencies": [], "multi_dependencies": []}


def run_search(session, args, labels):
    status = import_status(session, labels, args.timeout)
    print(f"[database] {args.database}; import complete: {status['complete']}; RDF verified: {status['verified']}", flush=True)
    requested = [name if "://" in name else ONTOLOGY + name for name in args.classes or []]
    if set(requested) - labels:
        raise ValueError("Unknown class labels: " + ", ".join(sorted(set(requested) - labels)))
    inventory = {}
    for uri in sorted(set(requested) if requested else labels - INTERNAL_LABELS):
        inventory[uri] = session.run(Query(f"MATCH (n:{quoted_label(uri)}) RETURN count(n) AS size", timeout=args.timeout)).single()["size"]
    report = prepare_report(args, inventory, status)
    print(f"[scope] {len(inventory)} explicit classes; {sum(inventory.values()):,} class memberships; "
          f"result limit: {args.count or 'none'}; class-size limit: {args.max_class_size or 'none'}", flush=True)
    finished = {row["class_uri"] for row in report["classes_scanned"] + report["classes_skipped"]}
    order = list(dict.fromkeys(requested)) if requested else sorted(inventory, key=lambda uri: (inventory[uri], uri))
    write_report(report, args.output)
    try:
        for uri in order:
            if uri in finished:
                continue
            if args.count and len(report["dependencies"]) >= args.count:
                report["status"] = "limited"
                break
            size = inventory[uri]
            if size < args.min_support or (args.max_class_size and size > args.max_class_size):
                report["classes_skipped"].append({"class_uri": uri, "size": size,
                    "reason": "below_min_support" if size < args.min_support else "class_size_limit"})
                write_report(report, args.output)
                continue
            report["active_class"] = uri
            write_report(report, args.output)
            print(f"[scan] {short(uri)}: {size:,} instances; {len(report['classes_scanned'])} classes completed", flush=True)
            try:
                if size <= args.memory_class_size:
                    subjects, columns, multivalued = read_class(session, uri, args.timeout)
                    total = len(subjects)
                    findings = find_in_class(uri, subjects, columns, multivalued, args)
                    multi_findings = find_multi_in_class(
                        uri, subjects, columns, multivalued, findings, args,
                    )
                    del subjects, columns, multivalued
                    engine = "memory"
                else:
                    from fd_scan import scan_class
                    findings, multi_findings, total, _ = scan_class(
                        session, uri, args, helpers=sys.modules[__name__],
                    )
                    engine = "sqlite"
                if total != size:
                    raise ValueError(f"Class size changed: expected {size}, read {total}. Rerun after writes finish.")
            except (ValueError, OSError, sqlite3.Error, Neo4jError, ServiceUnavailable, SessionExpired) as error:
                report["classes_failed"].append({"class_uri": uri, "error": str(error)})
                report["active_class"] = None
                print(f"[failed] {short(uri)}: {error}; continuing with other classes", flush=True)
                write_report(report, args.output)
                continue
            selected = findings[:args.max_per_class] if args.max_per_class else findings
            selected_multi = multi_findings[:args.max_per_class] if args.max_per_class else multi_findings
            if args.count:
                selected = selected[:max(0, args.count - len(report["dependencies"]))]
            report["dependencies"].extend(selected)
            report["multi_dependencies"].extend(selected_multi)
            report["classes_scanned"].append({"class_uri": uri, "instances": total, "engine": engine,
                                             "qualifying_dependencies": len(findings), "saved_dependencies": len(selected),
                                             "qualifying_multi_dependencies": len(multi_findings),
                                             "saved_multi_dependencies": len(selected_multi)})
            report["active_class"] = None
            print(f"[checked] {short(uri)}: {len(findings)} single-RHS, {len(multi_findings)} 1-to-N; "
                  f"totals saved: {len(report['dependencies'])} and {len(report['multi_dependencies'])}", flush=True)
            write_report(report, args.output)
        else:
            report["status"] = "incomplete" if report["classes_failed"] else "completed"
        report["import_at_end"] = import_status(session, labels, args.timeout)
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
    except BaseException as error:
        report["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
        write_report(report, args.output)
        raise
    report["whole_database_search_complete"] = (
        report["status"] == "completed" and not args.classes and not args.max_class_size
        and all(row["saved_dependencies"] == row["qualifying_dependencies"]
                and row["saved_multi_dependencies"] == row["qualifying_multi_dependencies"]
                for row in report["classes_scanned"]))
    write_report(report, args.output)
    print(f"[done] Status: {report['status']}; {len(report['dependencies'])} single-RHS and "
          f"{len(report['multi_dependencies'])} 1-to-N dependencies. "
          f"Reports: {args.output} and {args.output.with_suffix('.md')}", flush=True)
    return 0 if report["status"] in {"completed", "limited"} and not report["classes_failed"] else 2


def main(argv=None):
    args = parse_args(argv)
    password = os.getenv("NEO4J_PASSWORD") or NEO4J_PASSWORD
    if not password:
        from getpass import getpass
        password = getpass("Neo4j password: ")
    # Prevent an IDE run and a terminal run from overwriting the same checkpoint.
    lock_id = hashlib.sha256(str(args.output.resolve()).encode()).hexdigest()
    with (Path(tempfile.gettempdir()) / f"dbpedia-fd-{lock_id}.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("Another search is writing this output. Use a different --output or wait for it to finish.") from error
        with GraphDatabase.driver(args.uri, auth=(args.user, password), connection_timeout=10) as driver:
            with driver.session(database=args.database, default_access_mode=READ_ACCESS, fetch_size=1000) as session:
                labels = {row["label"] for row in session.run(Query("CALL db.labels() YIELD label RETURN label", timeout=args.timeout))}
                if "DBpediaResource" not in labels:
                    raise ValueError("No imported DBpediaResource label found in the selected database.")
                return run_search(session, args, labels)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[stopped] Search interrupted.")
        sys.exit(130)
    except (ValueError, OSError, sqlite3.Error, Neo4jError, ServiceUnavailable, SessionExpired) as error:
        print(f"[error] {error}")
        sys.exit(1)
