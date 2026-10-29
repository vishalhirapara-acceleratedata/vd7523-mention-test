"""Structural checks over an Ossie model document (VIBEDATA part).

Runs after the vendored upstream `validate_document()`. Answers one question:
*is this document well-formed and fully resolved?* — every declared name exists,
every reference points at something declared, every payload uses the frozen key
set in the right shape.

It does not judge meaning. Whether a mapping is honest, whether every field is
supplied, whether an aggregate computes the same aggregation, and whether a
table's population matches the star's are content questions, reviewed by the
agent against the mart SQL (the validating-semantic-model skill).

Pure functions: input is the parsed YAML dict, output is a list of diagnostics.
"""

import json
import re
from pathlib import Path

import ossie_functions
import ossie_sql
from jsonschema import Draft202012Validator
from ossie_sql import ExpressionError

VENDOR = "VIBEDATA"

# The current key-set generation; vibedata-schema.json's $defs/version holds the same value.
VIBEDATA_VERSION = "1.5"
SCHEMA_PATH = Path(__file__).parent / "vibedata-schema.json"
CARDINALITIES = {"many-to-one", "one-to-one", "many-to-many"}
JOIN_TYPES = {"LEFT OUTER", "INNER", "FULL OUTER", "RIGHT OUTER"}
AGGREGATIONS = {"SUM", "MIN", "MAX", "NON_ADDITIVE"}
TIME_SERIES_FUNCTIONS = {"AGO", "TODATE", "ROLLING"}
UNRELATED = {"error", "total"}
DIMENSION_RULES = {"AVG", "SUM", "MIN", "MAX", "MEDIAN", "FIRST", "LAST"}
SCD_ROLLUPS = {"as_of", "current"}
# A metric computed from another metric rather than from rows: at most one per metric.
COMPUTED_KEYS = ("derived_expression", "at_levels", "time_series", "based_on_dimensions")

MODEL_KEYS = {"vibedata_version", "physical_tables", "physical_joins"}
DATASET_KEYS = {"vibedata_version", "scd", "logical_sources", "levels", "row_level",
                "hierarchies"}
RELATIONSHIP_KEYS = {"vibedata_version", "physical_join", "cardinality",
                     "join_type", "join_cost", "join_expression", "note", "to_level",
                     "weight"}
PHYSICAL_TABLE_KEYS = {"name", "source", "dbt_model", "table_type", "alias_of", "role",
                       "partitioned_by", "note"}
PHYSICAL_JOIN_KEYS = {"name", "from", "to"}
LTS_KEYS = {"name", "physical_table", "priority_group", "content_grain",
            "content_levels", "content_filter", "navigable",
            "join_cost", "note", "column_map"}
HIERARCHY_KEYS = {"name", "is_time", "scd_rollup", "levels"}
LEVEL_KEYS = {"name", "keys", "attributes"}
LEVEL_KEY_KEYS = {"name", "fields", "primary", "display"}
ROW_LEVEL_KEYS = {"level", "versioned"}
LEVEL_REF_KEYS = {"via", "level", "key", "key_columns"}
METRIC_KEYS = {"vibedata_version", "aggregation", "derived_expression", "at_levels",
               "time_series", "based_on_dimensions", "filter", "unrelated_dimensions"}
TIME_SERIES_KEYS = {"function", "of", "via", "level", "offset", "periods"}
BASED_ON_KEYS = {"of", "across", "rule"}
SCD_KEYS = {"type", "natural_key", "valid_from", "valid_to", "is_current",
            "open_ended_value"}


HELD_BY = re.compile(r"held by\s+[A-Za-z_][A-Za-z0-9_]*")


def _error(code, message):
    return {"severity": "error", "code": code, "message": message}


def _names_in(expression):
    """(names, :placeholders) a predicate or formula reads, parsed by the shared SQL layer.

    Raises ExpressionError when the text does not parse.
    """
    return ossie_sql.names(expression)


def _qualified_in(expression):
    """`(dataset, field)` references in an expression, and the bare names left over.

    Parsed by the shared Ossie/sqlglot layer; raises ExpressionError when it does not parse.
    """
    columns, _ = ossie_sql.names(expression)
    pairs = {tuple(c.split(".", 1)) for c in columns if "." in c}
    return pairs, {c for c in columns if "." not in c}


def _metric_columns(metric):
    """`(dataset, field)` pairs a metric's ANSI/Ossie expressions read; unparsable ones add none."""
    pairs = set()
    for dialect in ((metric.get("expression") or {}).get("dialects") or []):
        if dialect.get("dialect") in ("ANSI_SQL", "OSSIE_SQL_2026"):
            try:
                pairs |= _qualified_in(dialect.get("expression"))[0]
            except ExpressionError:
                continue
    return pairs


def _dimension_sources(doc):
    """Per dataset, the physical tables of its own coarser sources and the level each holds."""
    out = {}
    for dataset in doc.get("datasets", []):
        if not isinstance(dataset, dict):
            continue
        for data in _extensions(dataset):
            try:
                payload = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                continue
            for src in (payload.get("logical_sources") or []) if isinstance(payload, dict) else []:
                for ref in (src.get("content_levels") or []) if isinstance(src, dict) else []:
                    if isinstance(ref, dict) and "via" not in ref and ref.get("level"):
                        out.setdefault(dataset.get("name"), {})[src.get("physical_table")] = ref["level"]
    return out


def _fields_by_dataset(doc):
    return {d.get("name"): {f.get("name") for f in d.get("fields") or [] if isinstance(f, dict)}
            for d in doc.get("datasets", []) if isinstance(d, dict)}


def _level_fields(payload):
    """Fields each level carries: its own keys and attributes plus every coarser level's.

    A source held at a level can supply exactly these fields, because a coarser level's
    attributes are functionally determined by the finer member.
    """
    own = {}
    for level in payload.get("levels") or []:
        if isinstance(level, dict) and level.get("name"):
            fields = set(level.get("attributes") or [])
            for key in level.get("keys") or []:
                if isinstance(key, dict):
                    fields.update(key.get("fields") or [])
            own[level["name"]] = fields
    out = {name: set(fields) for name, fields in own.items()}
    for hier in payload.get("hierarchies") or []:
        names = [n for n in (hier.get("levels") or []) if isinstance(n, str)] if isinstance(hier, dict) else []
        for i, name in enumerate(names):
            for coarser in names[:i]:
                out.setdefault(name, set()).update(own.get(coarser, set()))
    return out


def _dimension_index(doc):
    """Levels per dataset: {dataset: {"levels": {name: {keys, primary}}, "time_levels", "primary_key"}}.

    A level's keys map key name to its fields; `primary` is the primary key's fields.
    `time_levels` are the levels that sit in a hierarchy marked is_time.
    """
    out = {}
    for dataset in doc.get("datasets", []):
        if not isinstance(dataset, dict):
            continue
        levels, time_levels = {}, set()
        for data in _extensions(dataset):
            try:
                payload = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            for level in payload.get("levels") or []:
                if not isinstance(level, dict) or not level.get("name"):
                    continue
                keys, primary = {}, None
                for key in level.get("keys") or []:
                    if isinstance(key, dict) and key.get("name"):
                        keys[key["name"]] = key.get("fields") or []
                        if key.get("primary") is True:
                            primary = key["name"]
                levels[level["name"]] = {"keys": keys, "primary": primary}
            for hier in payload.get("hierarchies") or []:
                if isinstance(hier, dict) and hier.get("is_time") is True:
                    time_levels.update(n for n in hier.get("levels") or [] if isinstance(n, str))
        out[dataset.get("name")] = {"levels": levels, "time_levels": time_levels,
                                    "primary_key": dataset.get("primary_key") or []}
    return out


def _check_level_ref(ref, where, context, from_dataset=None, need_level=False,
                     aggregate=None):
    """A level a source holds or a metric is evaluated at, reached through a relationship.

    `level` names a level of the relationship's target; omitted, the target's own rows.
    `key` picks one of the level's keys (default: its primary key). An aggregate source
    (`aggregate` is its physical table) must say which of its columns carry that key
    (`key_columns`), and nothing else may. Returns (diagnostics, {"dataset", "level",
    "is_time"} or None).
    """
    if not isinstance(ref, dict):
        return [_error("vibedata/level-ref-not-object", f"{where}: level entry must be an object")], None
    diags = _unknown_keys(ref, LEVEL_REF_KEYS, where)
    rel = context["relationships"].get(ref.get("via"))
    if rel is None:
        return diags + [_error("vibedata/level-ref-unknown-relationship",
                               f"{where}: via '{ref.get('via')}' is not a relationship")], None
    if from_dataset is not None and rel.get("from") != from_dataset:
        diags.append(_error("vibedata/level-ref-wrong-dataset",
                            f"{where}: via '{ref.get('via')}' does not start at dataset "
                            f"'{from_dataset}'"))
    target = rel.get("to")
    dim = context["dimensions"].get(target, {"levels": {}, "time_levels": set(),
                                             "primary_key": []})
    level_name, key_name = ref.get("level"), ref.get("key")
    info = {"dataset": target, "level": level_name,
            "is_time": level_name in dim["time_levels"]}
    if level_name is None:
        if need_level:
            diags.append(_error("vibedata/level-ref-needs-level",
                                f"{where}: names no level of dataset '{target}'"))
        if key_name is not None:
            diags.append(_error("vibedata/level-ref-unknown-key",
                                f"{where}: key '{key_name}' needs a level"))
        key_fields = dim["primary_key"]
    else:
        level = dim["levels"].get(level_name)
        if level is None:
            return diags + [_error("vibedata/level-ref-unknown-level",
                                   f"{where}: level '{level_name}' is not a level of dataset "
                                   f"'{target}'")], None
        key_name = key_name or level["primary"]
        if key_name not in level["keys"]:
            return diags + [_error("vibedata/level-ref-unknown-key",
                                   f"{where}: key '{key_name}' is not a key of level "
                                   f"'{level_name}'")], info
        key_fields = level["keys"][key_name]
    columns = ref.get("key_columns")
    if aggregate is None:
        if columns is not None:
            diags.append(_error("vibedata/level-ref-key-columns-misplaced",
                                f"{where}: key_columns belong only to an aggregate source's "
                                "content_levels"))
        return diags, info
    if (not isinstance(columns, list) or not columns
            or not all(isinstance(c, str) and c for c in columns)):
        diags.append(_error("vibedata/level-ref-key-columns-missing",
                            f"{where}: an aggregate names the columns that carry the level's "
                            "key (key_columns), so it can be joined at that level"))
    elif len(columns) != len(key_fields):
        diags.append(_error("vibedata/level-ref-key-columns-arity",
                            f"{where}: key_columns has {len(columns)} column(s) for a key of "
                            f"{len(key_fields)} field(s)"))
    reached = context["join_targets"].get(ref.get("via"))
    # A coarser source of the dimension held at this very level (a month table) is as
    # good a join target as the row-level table the relationship reaches.
    targets = {reached} | {table for table, held in
                           context["dimension_sources"].get(target, {}).items()
                           if held == level_name}
    if reached and not any((aggregate, t) in context["join_pairs"] for t in targets):
        diags.append(_error("vibedata/content-level-unjoined",
                            f"{where}: no physical join from '{aggregate}' to "
                            f"{' or '.join(sorted(t for t in targets if t))}, the tables that "
                            f"hold '{ref.get('via')}' at this level"))
    return diags, info


def _extensions(node):
    """The decoded VIBEDATA payloads on a model/dataset/relationship node."""
    out = []
    if not isinstance(node, dict):
        return out
    for ext in node.get("custom_extensions") or []:
        if isinstance(ext, dict) and ext.get("vendor_name") == VENDOR:
            out.append(ext.get("data"))
    return out


def _decode(data, where):
    try:
        payload = json.loads(data)
    except (TypeError, json.JSONDecodeError) as e:
        return None, [_error("vibedata/data-not-json",
                             f"{where}: VIBEDATA data is not valid JSON: {e}")]
    if not isinstance(payload, dict):
        return None, [_error("vibedata/data-not-object",
                             f"{where}: VIBEDATA data must be a JSON object")]
    return payload, []


def _model_payload(model):
    """Model-level VIBEDATA payload. Duplicates are an error, not a merge."""
    diags = []
    raws = _extensions(model)
    if len(raws) > 1:
        diags.append(_error("vibedata/duplicate-entry",
                            "model: multiple VIBEDATA entries; keep exactly one"))
    merged = {}
    for data in raws:
        payload, errs = _decode(data, "model")
        diags.extend(errs)
        if payload:
            merged.update(payload)
    return merged, diags


def _unknown_keys(payload, allowed, where):
    return [_error("vibedata/unknown-key",
                   f"{where}: unknown VIBEDATA key '{k}'")
            for k in payload if k not in allowed]


def _collect_versions(doc):
    """Every vibedata_version in the document, with where it sits."""
    found = []

    def grab(node, where):
        if not isinstance(node, dict):
            return
        for data in _extensions(node):
            try:
                payload = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                found.append((payload.get("vibedata_version"), where))

    grab(doc, "model")
    for dataset in doc.get("datasets", []):
        grab(dataset, f"dataset '{dataset.get('name', '<unnamed>')}'")
    for rel in doc.get("relationships", []):
        grab(rel, f"relationship '{rel.get('name', '<unnamed>')}'")
    for metric in doc.get("metrics", []):
        grab(metric, f"metric '{metric.get('name', '<unnamed>')}'")
    return found


def physical_table_index(doc):
    """Model-level physical tables by name: identity fields used for drift checks."""
    payload, _ = _model_payload(doc)
    out = {}
    for t in payload.get("physical_tables", []):
        if isinstance(t, dict) and t.get("name"):
            out[t["name"]] = {k: t.get(k) for k in
                              ("source", "dbt_model", "table_type", "alias_of", "role")}
    return out


def validate_vibedata(doc):
    """Structural VIBEDATA checks. Returns diagnostics."""
    diags = []
    if not isinstance(doc, dict) or "datasets" not in doc:
        return diags
    model_name = doc.get("name", "<unnamed>")

    versions = _collect_versions(doc)
    if any(v is None for v, _ in versions):
        diags.append(_error("vibedata/version-missing",
                            f"model '{model_name}': every VIBEDATA payload carries "
                            "vibedata_version"))
    elif len({v for v, _ in versions}) > 1:
        detail = ", ".join(f"{w}={v!r}" for v, w in versions)
        diags.append(_error("vibedata/version-mismatch",
                            f"model '{model_name}': mixed vibedata_version: {detail}"))
    elif versions and versions[0][0] != VIBEDATA_VERSION:
        diags.append(_error("vibedata/version-unsupported",
                            f"model '{model_name}': vibedata_version {versions[0][0]!r} is "
                            f"not the current key set {VIBEDATA_VERSION!r}; upgrade every "
                            "payload (S6a)"))

    model_payload, errs = _model_payload(doc)
    diags.extend(errs)
    diags.extend(_unknown_keys(model_payload, MODEL_KEYS, "model"))
    physical_tables = {t.get("name"): t
                       for t in model_payload.get("physical_tables", [])
                       if isinstance(t, dict) and t.get("name")}
    for t in model_payload.get("physical_tables", []):
        if not isinstance(t, dict) or not t.get("name"):
            diags.append(_error("vibedata/physical-table-nameless",
                                f"model '{model_name}': physical_tables entry without a name"))
            continue
        diags.extend(_unknown_keys(t, PHYSICAL_TABLE_KEYS, f"physical table '{t['name']}'"))
        alias_of = t.get("alias_of")
        if alias_of and (alias_of not in physical_tables or alias_of == t["name"]):
            diags.append(_error("vibedata/alias-dangling",
                                f"model '{model_name}': physical table '{t['name']}' is an "
                                f"alias of unknown table '{alias_of}'"))
        partitioned_by = t.get("partitioned_by")
        if partitioned_by is not None and (
                not isinstance(partitioned_by, list)
                or not all(isinstance(c, str) and c for c in partitioned_by)):
            diags.append(_error("vibedata/bad-partitioned-by",
                                f"physical table '{t['name']}': partitioned_by must be a list "
                                "of column names"))

    physical_join_names = set()
    for j in model_payload.get("physical_joins", []):
        if not isinstance(j, dict):
            diags.append(_error("vibedata/join-not-object",
                                f"model '{model_name}': physical join entry must be an object"))
            continue
        diags.extend(_unknown_keys(j, PHYSICAL_JOIN_KEYS,
                                   f"physical join '{j.get('name', '<unnamed>')}'"))
        physical_join_names.add(j.get("name"))
        for end in ("from", "to"):
            if j.get(end) not in physical_tables:
                diags.append(_error("vibedata/join-dangling",
                                    f"model '{model_name}': physical join "
                                    f"'{j.get('name', '<unnamed>')}' references unknown "
                                    f"physical table '{j.get(end)}'"))

    reached = {j.get("from") for j in model_payload.get("physical_joins", [])
               if isinstance(j, dict)}
    reached |= {j.get("to") for j in model_payload.get("physical_joins", [])
                if isinstance(j, dict)}
    for name in sorted(physical_tables):
        if name not in reached:
            diags.append({"severity": "warning", "code": "vibedata/table-unjoined",
                          "message": f"model '{model_name}': physical table '{name}' appears "
                                     "in no physical join"})

    dataset_names = {d.get("name") for d in doc.get("datasets", []) if d.get("name")}
    metric_names = {m.get("name") for m in doc.get("metrics", [])
                    if isinstance(m, dict) and m.get("name")}
    joins = {j.get("name"): j for j in model_payload.get("physical_joins", [])
             if isinstance(j, dict)}
    join_targets = {}
    for rel in doc.get("relationships", []):
        for data in _extensions(rel):
            try:
                rel_payload = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                continue
            join = joins.get(rel_payload.get("physical_join")) if isinstance(rel_payload, dict) else None
            if join:
                join_targets[rel.get("name")] = join.get("to")
    context = {"physical_tables": physical_tables, "metric_names": metric_names,
               "fields": _fields_by_dataset(doc),
               "dimension_sources": _dimension_sources(doc),
               "dimensions": _dimension_index(doc),
               "join_targets": join_targets,
               "join_pairs": {(j.get("from"), j.get("to")) for j in joins.values()},
               "relationships": {r.get("name"): r for r in doc.get("relationships", [])
                                 if isinstance(r, dict) and r.get("name")}}
    for dataset in doc.get("datasets", []):
        diags.extend(_validate_dataset(dataset, context))
    diags.extend(_validate_metrics(doc, context))
    for rel in doc.get("relationships", []):
        diags.extend(_validate_relationship(rel, dataset_names, physical_join_names, doc))
        diags.extend(_validate_level_join(rel, doc, context))
        diags.extend(_validate_from_columns(rel, doc))
        diags.extend(_validate_to_columns(rel, doc))
    diags.extend(_schema_constraints(doc))
    diags.extend(_expression_checks(doc))
    return diags


def _validate_dataset(dataset, context):
    physical_tables = context["physical_tables"]
    metric_names = context["metric_names"]
    diags = []
    ds_name = dataset.get("name", "<unnamed>")
    where = f"dataset '{ds_name}'"
    fields = {f.get("name") for f in dataset.get("fields", []) if isinstance(f, dict)}
    for key_name, key_cols in (("primary_key", dataset.get("primary_key")),
                               ("unique_keys", dataset.get("unique_keys"))):
        groups = key_cols if key_name == "unique_keys" else [key_cols] if key_cols else []
        for group in groups or []:
            for col in group if isinstance(group, list) else []:
                if col not in fields:
                    diags.append(_error("vibedata/bad-key-column",
                                        f"{where}: {key_name} names '{col}', which is not a "
                                        "declared field"))
    payloads = []
    for data in _extensions(dataset):
        payload, errs = _decode(data, where)
        diags.extend(errs)
        if payload:
            payloads.append(payload)
    if len(payloads) > 1:
        diags.append(_error("vibedata/duplicate-entry",
                            f"{where}: multiple VIBEDATA entries; keep exactly one"))
    if not payloads:
        return diags
    if not (payload.get("logical_sources") or []):
        diags.append(_error("vibedata/lts-empty",
                            f"{where}: declares no logical source, so nothing supplies it"))
    payload = {}
    for p in payloads:
        payload.update(p)
    diags.extend(_unknown_keys(payload, DATASET_KEYS, where))

    level_names = []
    for level in payload.get("levels", []) or []:
        if not isinstance(level, dict) or not level.get("name"):
            diags.append(_error("vibedata/level-not-object",
                                f"{where}: each level is an object with a name"))
            continue
        level_names.append(level["name"])
        diags.extend(_check_level(level, f"{where} level '{level['name']}'", fields))
    for name in sorted({n for n in level_names if level_names.count(n) > 1}):
        diags.append(_error("vibedata/level-duplicate",
                            f"{where}: level '{name}' is declared twice"))
    scd_type = (payload.get("scd") or {}).get("type") if isinstance(payload.get("scd"), dict) else None
    diags.extend(_check_row_level(payload, dataset, where, scd_type))
    for hier in payload.get("hierarchies", []):
        if not isinstance(hier, dict):
            diags.append(_error("vibedata/hierarchy-not-object",
                                f"{where}: hierarchy entry must be an object"))
            continue
        hier_where = f"{where} hierarchy '{hier.get('name', '<unnamed>')}'"
        diags.extend(_unknown_keys(hier, HIERARCHY_KEYS, hier_where))
        members = hier.get("levels")
        if not isinstance(members, list) or not members:
            diags.append(_error("vibedata/hierarchy-levels",
                                f"{hier_where}: levels lists the dataset's level names, "
                                "coarsest first"))
            members = []
        for name in members:
            if name not in level_names:
                diags.append(_error("vibedata/hierarchy-unknown-level",
                                    f"{hier_where}: '{name}' is not a level of this dataset"))
        if len(set(map(str, members))) != len(members):
            diags.append(_error("vibedata/hierarchy-levels",
                                f"{hier_where}: a level appears twice"))
        rollup = hier.get("scd_rollup")
        if scd_type == 2 and rollup not in SCD_ROLLUPS:
            diags.append(_error("vibedata/hierarchy-scd-rollup",
                                f"{hier_where}: a versioned dimension states whether a member "
                                f"rolls up as of the fact's date or as it is now: scd_rollup "
                                f"{sorted(SCD_ROLLUPS)}"))
        elif scd_type != 2 and rollup is not None:
            diags.append(_error("vibedata/hierarchy-scd-rollup",
                                f"{hier_where}: scd_rollup applies only to a Type 2 dimension"))

    scd = payload.get("scd")
    if scd:
        if not isinstance(scd, dict):
            diags.append(_error("vibedata/scd-not-object", f"{where}: scd must be an object"))
            scd = {}
        diags.extend(_unknown_keys(scd, SCD_KEYS, f"{where} scd"))
        if scd.get("type") not in (1, 2, 3):
            diags.append(_error("vibedata/scd-bad-type",
                                f"{where}: scd.type must be 1, 2, or 3"))
        for key in ("natural_key", "valid_from", "valid_to", "is_current"):
            col = scd.get(key)
            if col and col not in fields:
                diags.append(_error("vibedata/scd-unknown-field",
                                    f"{where}: scd.{key} '{col}' is not a declared field"))

    sources = payload.get("logical_sources", [])
    if not isinstance(sources, list):
        diags.append(_error("vibedata/lts-not-list",
                            f"{where}: logical_sources must be a list"))
        sources = []
    suppliable = fields | metric_names
    for src in sources:
        src_name = src.get("name", "<unnamed>") if isinstance(src, dict) else "<unnamed>"
        if not isinstance(src, dict):
            diags.append(_error("vibedata/lts-not-object",
                                f"{where}: logical source '{src_name}' must be an object"))
            continue
        diags.extend(_unknown_keys(src, LTS_KEYS, f"{where} logical source '{src_name}'"))
        priority = src.get("priority_group", 1)
        if not isinstance(priority, int) or isinstance(priority, bool) or priority < 0:
            diags.append(_error("vibedata/bad-priority",
                                f"{where}: logical source '{src_name}' priority_group "
                                "must be an integer >= 0"))
        grain = src.get("content_grain", [])
        if not isinstance(grain, list) or not all(isinstance(g, str) for g in grain):
            diags.append(_error("vibedata/bad-grain",
                                f"{where}: logical source '{src_name}' content_grain "
                                "must be a list of grain names"))
        for i, ref in enumerate(src.get("content_levels") or []):
            aggregate = (src.get("physical_table")
                         if src.get("priority_group", 1) == 0 else None)
            ref_where = f"{where} logical source '{src_name}' content_levels[{i}]"
            if isinstance(ref, dict) and "via" not in ref:
                diags.extend(_check_own_level(ref, ref_where, payload, aggregate,
                                              src.get("column_map") or {}))
                continue
            ref_diags, _ = _check_level_ref(ref, ref_where, context, from_dataset=ds_name,
                                            aggregate=aggregate)
            diags.extend(ref_diags)
        navigable = src.get("navigable", True)
        if not isinstance(navigable, bool):
            diags.append(_error("vibedata/bad-navigable",
                                f"{where}: logical source '{src_name}' navigable must be "
                                "true or false"))
        bound_metrics = sorted(set(src.get("column_map") or {}) & metric_names)
        if (src.get("priority_group", 1) == 0 and bound_metrics
                and not HELD_BY.search(src.get("note") or "")):
            diags.append(_error("vibedata/aggregate-binding-unheld",
                                f"{where}: logical source '{src_name}' binds metrics "
                                f"({', '.join(bound_metrics)}) from an aggregate, so its note "
                                "names the test that proves they equal the detail ('held by "
                                "<test>')"))
        content_filter = src.get("content_filter")
        if content_filter is not None:
            diags.extend(_check_content_filter(content_filter, src, f"{where} logical source "
                                               f"'{src_name}'", fields))
        if src.get("physical_table") not in physical_tables:
            diags.append(_error("vibedata/lts-dangling-table",
                                f"{where}: logical source '{src_name}' references unknown "
                                f"physical table '{src.get('physical_table')}'"))
        column_map = src.get("column_map", {}) or {}
        if not isinstance(column_map, dict):
            diags.append(_error("vibedata/bad-column-map",
                                f"{where}: logical source '{src_name}' column_map must be "
                                "an object of logical name to physical column"))
            column_map = {}
        for logical_col, physical_col in column_map.items():
            if logical_col not in suppliable:
                diags.append(_error("vibedata/lts-unknown-field",
                                    f"{where}: logical source '{src_name}' maps undeclared "
                                    f"field or metric '{logical_col}'"))
            if not isinstance(physical_col, str) or not physical_col:
                diags.append(_error("vibedata/bad-column-map",
                                    f"{where}: logical source '{src_name}' maps "
                                    f"'{logical_col}' to a non-string column"))
    return diags


def _check_level(level, where, fields):
    """Keys identify one member each; exactly one is primary, at most one is displayed."""
    diags = _unknown_keys(level, LEVEL_KEYS, where)
    keys = level.get("keys")
    if not isinstance(keys, list) or not keys:
        return diags + [_error("vibedata/level-keys-missing",
                               f"{where}: needs keys: the fields that identify one member")]
    names, primaries, displays = [], 0, 0
    for key in keys:
        if not isinstance(key, dict) or not key.get("name"):
            diags.append(_error("vibedata/level-key-not-object",
                                f"{where}: each key is an object with a name"))
            continue
        key_where = f"{where} key '{key['name']}'"
        diags.extend(_unknown_keys(key, LEVEL_KEY_KEYS, key_where))
        names.append(key["name"])
        primaries += key.get("primary") is True
        displays += key.get("display") is True
        key_fields = key.get("fields")
        if not isinstance(key_fields, list) or not key_fields:
            diags.append(_error("vibedata/level-key-fields",
                                f"{key_where}: fields lists the dataset fields that form it"))
            key_fields = []
        for col in key_fields:
            if col not in fields:
                diags.append(_error("vibedata/level-unknown-field",
                                    f"{key_where}: '{col}' is not a declared field"))
    if primaries != 1:
        diags.append(_error("vibedata/level-primary-key",
                            f"{where}: exactly one key is primary (has {primaries}); it is "
                            "unique at the level and is what joins and blends on it"))
    if displays > 1:
        diags.append(_error("vibedata/level-display-key",
                            f"{where}: at most one key is the display key (has {displays})"))
    for name in sorted({n for n in names if names.count(n) > 1}):
        diags.append(_error("vibedata/level-key-duplicate",
                            f"{where}: key '{name}' is declared twice"))
    for col in level.get("attributes") or []:
        if col not in fields:
            diags.append(_error("vibedata/level-unknown-field",
                                f"{where}: attribute '{col}' is not a declared field"))
    return diags


def _check_row_level(payload, dataset, where, scd_type):
    """Which level a row of the dataset is, and whether rows are versions of its members."""
    levels = {lv.get("name"): lv for lv in payload.get("levels") or []
              if isinstance(lv, dict) and lv.get("name")}
    row = payload.get("row_level")
    if row is None:
        if levels:
            return [_error("vibedata/row-level-missing",
                           f"{where}: a dataset with levels names the level its rows are "
                           "(row_level)")]
        return []
    if not isinstance(row, dict):
        return [_error("vibedata/row-level-not-object", f"{where}: row_level must be an object")]
    diags = _unknown_keys(row, ROW_LEVEL_KEYS, f"{where} row_level")
    level = levels.get(row.get("level"))
    if level is None:
        return diags + [_error("vibedata/row-level-unknown",
                               f"{where}: row_level '{row.get('level')}' is not a level of "
                               "this dataset")]
    versioned = row.get("versioned")
    if not isinstance(versioned, bool):
        return diags + [_error("vibedata/row-level-not-object",
                               f"{where}: row_level.versioned must be true or false")]
    keys = [k for k in level.get("keys") or [] if isinstance(k, dict)]
    primary = next((k.get("fields") for k in keys if k.get("primary") is True), None)
    if versioned:
        natural = (payload.get("scd") or {}).get("natural_key") if scd_type == 2 else None
        if natural is None or primary != [natural]:
            diags.append(_error("vibedata/row-level-versioned",
                                f"{where}: versioned rows need a Type 2 scd whose natural_key "
                                "is the row level's primary key"))
    else:
        if scd_type == 2:
            diags.append(_error("vibedata/row-level-versioned",
                                f"{where}: a Type 2 dimension's rows are versions of its "
                                "members (versioned: true)"))
        if dataset.get("primary_key") not in [k.get("fields") for k in keys]:
            diags.append(_error("vibedata/row-level-key",
                                f"{where}: the primary key {dataset.get('primary_key')} must be "
                                f"one of level '{row.get('level')}''s keys, so a row and a "
                                "member are the same thing"))
    return diags


def _check_own_level(ref, where, payload, aggregate, column_map):
    """A dimension source held at one of the dimension's own coarser levels (a month table
    beside the day table). It names the level with no `via`, the columns carrying the
    level's key, and maps only the fields that level determines."""
    diags = _unknown_keys(ref, {"level", "key", "key_columns"}, where)
    levels = {lv.get("name"): lv for lv in payload.get("levels") or [] if isinstance(lv, dict)}
    level = levels.get(ref.get("level"))
    if level is None:
        return diags + [_error("vibedata/level-ref-unknown-level",
                               f"{where}: level '{ref.get('level')}' is not a level of this "
                               "dataset")]
    keys = {k.get("name"): k.get("fields") or [] for k in level.get("keys") or []
            if isinstance(k, dict)}
    primary = next((k.get("name") for k in level.get("keys") or []
                    if isinstance(k, dict) and k.get("primary") is True), None)
    key_name = ref.get("key") or primary
    if key_name not in keys:
        diags.append(_error("vibedata/level-ref-unknown-key",
                            f"{where}: key '{key_name}' is not a key of level '{level['name']}'"))
    columns = ref.get("key_columns")
    if aggregate is None:
        diags.append(_error("vibedata/own-level-on-detail",
                            f"{where}: only a coarser dimension source (priority_group 0) "
                            "declares its own level; the detail source is the row level"))
    elif not isinstance(columns, list) or len(columns) != len(keys.get(key_name, [])):
        diags.append(_error("vibedata/level-ref-key-columns-missing",
                            f"{where}: name the columns that carry the level's key "
                            "(key_columns), one per key field"))
    allowed = _level_fields(payload).get(level["name"], set())
    for field_name in sorted(set(column_map) - allowed):
        diags.append(_error("vibedata/own-level-finer-field",
                            f"{where}: maps '{field_name}', which level '{level['name']}' does "
                            "not determine (not a key or attribute of it or a coarser level)"))
    return diags


def _check_content_filter(content_filter, src, where, fields):
    """A content filter is a predicate in the dataset's own fields, held by a test.

    A withdrawn source (empty column_map) binds nothing, so its filter needs no test.
    """
    if not isinstance(content_filter, str) or not content_filter.strip():
        return [_error("vibedata/bad-content-filter",
                       f"{where}: content_filter must be a non-empty SQL predicate")]
    diags = []
    try:
        names, refs = _names_in(content_filter)
    except ExpressionError as e:
        return [_error("vibedata/content-filter-unparsable",
                       f"{where}: content_filter does not parse: {e}")]
    for name in sorted(names - fields):
        diags.append(_error("vibedata/content-filter-unknown-field",
                            f"{where}: content_filter names '{name}', which is not a "
                            "declared field of the dataset"))
    for ref in sorted(refs):
        diags.append(_error("vibedata/content-filter-variable",
                            f"{where}: content_filter reads ':{ref}'; model variables are not "
                            "supported yet, so write the value as a literal"))
    if src.get("column_map") and not HELD_BY.search(src.get("note") or ""):
        diags.append(_error("vibedata/content-filter-unheld",
                            f"{where}: a content_filter needs a note naming the test that "
                            "holds it ('held by <test>')"))
    return diags


def _metric_payload(metric, where):
    diags, payloads = [], []
    for data in _extensions(metric):
        payload, errs = _decode(data, where)
        diags.extend(errs)
        if payload:
            payloads.append(payload)
    if len(payloads) > 1:
        diags.append(_error("vibedata/duplicate-entry",
                            f"{where}: multiple VIBEDATA entries; keep exactly one"))
    merged = {}
    for p in payloads:
        merged.update(p)
    return merged, diags


def _validate_metrics(doc, context):
    """Rollup rule, post-aggregation formula, pinned levels and time series on metrics."""
    diags = []
    metrics = {m.get("name"): m for m in doc.get("metrics", [])
               if isinstance(m, dict) and m.get("name")}
    mapped = set()
    for dataset in doc.get("datasets", []):
        for data in _extensions(dataset):
            try:
                payload = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                continue
            for src in (payload.get("logical_sources") or []) if isinstance(payload, dict) else []:
                if isinstance(src, dict) and isinstance(src.get("column_map"), dict):
                    mapped.update(src["column_map"])
    inputs = {}
    for name, metric in metrics.items():
        where = f"metric '{name}'"
        payload, errs = _metric_payload(metric, where)
        diags.extend(errs)
        if not payload:
            continue
        diags.extend(_unknown_keys(payload, METRIC_KEYS, where))
        kinds = [k for k in COMPUTED_KEYS if payload.get(k) is not None]
        if len(kinds) > 1:
            diags.append(_error("vibedata/metric-semantics-conflict",
                                f"{where}: declares {', '.join(kinds)}; a metric wraps one "
                                "other metric one way, so split it into a chain"))
        if any(k != "at_levels" for k in kinds) and name in mapped:
            diags.append(_error("vibedata/computed-metric-mapped",
                                f"{where}: computed from other metrics after aggregation, so "
                                "it must not be bound to a column"))
        inputs[name] = _metric_inputs(payload)
        aggregation = payload.get("aggregation")
        if aggregation is not None and aggregation not in AGGREGATIONS:
            diags.append(_error("vibedata/bad-aggregation",
                                f"{where}: aggregation '{aggregation}' must be one of "
                                f"{sorted(AGGREGATIONS)}"))
        derived = payload.get("derived_expression")
        if derived is not None:
            try:
                names, _ = _names_in(derived) if isinstance(derived, str) else (set(), set())
            except ExpressionError:
                names = set()
            if not isinstance(derived, str) or not names:
                diags.append(_error("vibedata/bad-derived-expression",
                                    f"{where}: derived_expression must be a formula over "
                                    "other metrics"))
            for ref in sorted(names - (set(metrics) - {name})):
                diags.append(_error("vibedata/derived-unknown-metric",
                                    f"{where}: derived_expression names '{ref}', which is "
                                    "not another metric of this model"))
        unrelated = payload.get("unrelated_dimensions")
        if unrelated is not None and unrelated not in UNRELATED:
            diags.append(_error("vibedata/bad-unrelated",
                                f"{where}: unrelated_dimensions must be one of "
                                f"{sorted(UNRELATED)}"))
        metric_filter = payload.get("filter")
        if metric_filter is not None:
            diags.extend(_check_metric_filter(metric_filter, metrics[name], kinds, where,
                                              context))
        for i, ref in enumerate(payload.get("at_levels") or []):
            ref_diags, _ = _check_level_ref(ref, f"{where} at_levels[{i}]", context,
                                            need_level=True)
            diags.extend(ref_diags)
        series = payload.get("time_series")
        if series is not None:
            diags.extend(_check_time_series(series, name, metrics, where, context))
        based_on = payload.get("based_on_dimensions")
        if based_on is not None:
            diags.extend(_check_based_on(based_on, name, metrics, where, context))
    diags.extend(_metric_cycles(inputs))
    return diags


def _check_metric_filter(metric_filter, metric, kinds, where, context):
    """OBIEE FILTER(measure USING ...): the rows a metric reads, stated as a predicate.

    It names `dataset.field` columns, every one of which the core expression must also
    read, so the declared population and the computed one cannot silently part.
    """
    if not isinstance(metric_filter, str) or not metric_filter.strip():
        return [_error("vibedata/bad-filter", f"{where}: filter must be a SQL predicate")]
    diags = []
    if [k for k in kinds if k != "at_levels"]:
        diags.append(_error("vibedata/filter-on-computed-metric",
                            f"{where}: a computed metric inherits the filters of the metrics "
                            "it reads; declare the filter on them"))
    try:
        pairs, bare = _qualified_in(metric_filter)
    except ExpressionError as e:
        return diags + [_error("vibedata/filter-unparsable",
                               f"{where}: filter does not parse: {e}")]
    for name in sorted(bare):
        diags.append(_error("vibedata/filter-unqualified",
                            f"{where}: filter names '{name}'; write it as dataset.field"))
    fields = context["fields"]
    read = _metric_columns(metric)
    for dataset, field_name in sorted(pairs):
        if field_name not in fields.get(dataset, set()):
            diags.append(_error("vibedata/filter-unknown-field",
                                f"{where}: filter names '{dataset}.{field_name}', which is not "
                                "a declared field"))
        elif (dataset, field_name) not in read:
            diags.append(_error("vibedata/filter-not-in-expression",
                                f"{where}: filter reads '{dataset}.{field_name}' but the core "
                                "expression does not, so the two populations can part"))
    return diags


def _metric_inputs(payload):
    """The other metrics a computed metric reads."""
    out = set()
    if isinstance(payload.get("derived_expression"), str):
        try:
            out |= _names_in(payload["derived_expression"])[0]
        except ExpressionError:
            pass
    for key in ("time_series", "based_on_dimensions"):
        if isinstance(payload.get(key), dict) and payload[key].get("of"):
            out.add(payload[key]["of"])
    return out


def _metric_cycles(inputs):
    """A metric must not reach itself through the metrics it reads."""
    diags, reported = [], set()
    for start in sorted(inputs):
        stack, seen = list(inputs[start]), set()
        while stack:
            current = stack.pop()
            if current == start:
                if start not in reported:
                    diags.append(_error("vibedata/metric-cycle",
                                        f"metric '{start}': reaches itself through the "
                                        "metrics it is computed from"))
                    reported.add(start)
                break
            if current in seen:
                continue
            seen.add(current)
            stack.extend(inputs.get(current, ()))
    return diags


def _check_based_on(rule, name, metrics, where, context):
    """OBIEE dimension-based aggregation: `of` is evaluated at the query's grain refined by
    the `across` levels, and `rule` then aggregates those values across that dimension."""
    if not isinstance(rule, dict):
        return [_error("vibedata/bad-based-on", f"{where}: based_on_dimensions must be an object")]
    diags = _unknown_keys(rule, BASED_ON_KEYS, f"{where} based_on_dimensions")
    base = metrics.get(rule.get("of"))
    if base is None or rule.get("of") == name:
        diags.append(_error("vibedata/based-on-unknown-metric",
                            f"{where}: based_on_dimensions.of '{rule.get('of')}' is not another "
                            "metric of this model"))
    elif json.dumps(base.get("expression"), sort_keys=True) != json.dumps(
            metrics[name].get("expression"), sort_keys=True):
        diags.append(_error("vibedata/based-on-expression",
                            f"{where}: expression must equal that of '{rule.get('of')}', the "
                            "metric it aggregates"))
    how = rule.get("rule")
    if how not in DIMENSION_RULES:
        diags.append(_error("vibedata/bad-based-on",
                            f"{where}: based_on_dimensions.rule must be one of "
                            f"{sorted(DIMENSION_RULES)}"))
    across = rule.get("across")
    if not isinstance(across, list) or not across:
        return diags + [_error("vibedata/bad-based-on",
                               f"{where}: based_on_dimensions.across lists the levels the rule "
                               "aggregates across")]
    infos = []
    for i, ref in enumerate(across):
        ref_diags, info = _check_level_ref(ref, f"{where} based_on_dimensions.across[{i}]",
                                           context)
        diags.extend(ref_diags)
        infos.append(info)
    if how in ("FIRST", "LAST") and (len(across) != 1 or infos[0] is None
                                     or not infos[0]["is_time"]):
        diags.append(_error("vibedata/based-on-order-not-time",
                            f"{where}: {how} needs exactly one level, in a time hierarchy "
                            "(is_time), whose key orders the members chronologically"))
    return diags


def _check_time_series(series, name, metrics, where, context):
    if not isinstance(series, dict):
        return [_error("vibedata/bad-time-series", f"{where}: time_series must be an object")]
    diags = _unknown_keys(series, TIME_SERIES_KEYS, f"{where} time_series")
    function = series.get("function")
    if function not in TIME_SERIES_FUNCTIONS:
        diags.append(_error("vibedata/bad-time-series",
                            f"{where}: time_series.function must be one of "
                            f"{sorted(TIME_SERIES_FUNCTIONS)}"))
    base = metrics.get(series.get("of"))
    if base is None or series.get("of") == name:
        diags.append(_error("vibedata/time-series-unknown-metric",
                            f"{where}: time_series.of '{series.get('of')}' is not another "
                            "metric of this model"))
    elif json.dumps(base.get("expression"), sort_keys=True) != json.dumps(
            metrics[name].get("expression"), sort_keys=True):
        diags.append(_error("vibedata/time-series-expression",
                            f"{where}: expression must equal that of '{series.get('of')}', "
                            "the metric it shifts"))
    offset = series.get("offset")
    if function == "AGO" and (not isinstance(offset, int) or isinstance(offset, bool)
                              or offset < 1):
        diags.append(_error("vibedata/bad-time-series",
                            f"{where}: AGO needs an integer offset of at least 1"))
    if function in ("TODATE", "ROLLING") and offset is not None:
        diags.append(_error("vibedata/bad-time-series",
                            f"{where}: {function} takes no offset"))
    periods = series.get("periods")
    if function == "ROLLING" and (not isinstance(periods, int) or isinstance(periods, bool)
                                  or periods < 2):
        diags.append(_error("vibedata/bad-time-series",
                            f"{where}: ROLLING needs an integer periods of at least 2"))
    if function != "ROLLING" and periods is not None:
        diags.append(_error("vibedata/bad-time-series",
                            f"{where}: periods belongs to ROLLING only"))
    ref = {k: series.get(k) for k in ("via", "level")}
    ref_diags, info = _check_level_ref(ref, f"{where} time_series", context, need_level=True)
    diags.extend(ref_diags)
    if info is not None and series.get("level") and not info["is_time"]:
        diags.append(_error("vibedata/time-series-not-time",
                            f"{where}: level '{series.get('level')}' is in no time hierarchy "
                            "(is_time)"))
    return diags


def _check_join_expression(text, rel, where, doc):
    """A join predicate in Ossie's FieldExpr grammar: every column is `dataset.field`,
    the dataset is one of the two the relationship joins, the field is declared there."""
    try:
        names, variables = _names_in(text) if isinstance(text, str) else (set(), set())
    except ExpressionError as e:
        return [_error("vibedata/join-expression-unparsable",
                       f"{where}: join_expression does not parse: {e}")]
    if not isinstance(text, str) or not names:
        return [_error("vibedata/bad-join-expression",
                       f"{where}: join_expression must be a predicate over the two datasets' "
                       "fields")]
    sides = (rel.get("from"), rel.get("to"))
    if sides[0] == sides[1]:
        return [_error("vibedata/join-expression-self",
                       f"{where}: a relationship from a dataset to itself cannot name its two "
                       "sides by dataset; declare it on the core from/to columns")]
    fields = {d.get("name"): {f.get("name") for f in d.get("fields") or [] if isinstance(f, dict)}
              for d in doc.get("datasets", []) if isinstance(d, dict)}
    diags = []
    for ref in sorted(variables):
        diags.append(_error("vibedata/join-expression-variable",
                            f"{where}: join_expression reads ':{ref}'; a join predicate reads "
                            "only the two datasets' fields"))
    for name in sorted(names):
        table, _, column = name.rpartition(".")
        if table not in sides:
            diags.append(_error("vibedata/join-expression-unqualified",
                                f"{where}: '{name}' must be qualified with '{sides[0]}' or "
                                f"'{sides[1]}' (Ossie dataset.field)"))
        elif column not in fields.get(table, set()):
            diags.append(_error("vibedata/join-expression-unknown-field",
                                f"{where}: '{name}' is not a declared field of dataset "
                                f"'{table}'"))
    return diags


def _validate_level_join(rel, doc, context):
    """`to_level` joins a member of a versioned dimension, not a row; `weight` allocates."""
    diags = []
    where = f"relationship '{rel.get('name', '<unnamed>')}'"
    payload = {}
    for data in _extensions(rel):
        try:
            decoded = json.loads(data)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(decoded, dict):
            payload.update(decoded)
    to_level = payload.get("to_level")
    if to_level is not None:
        level = context["dimensions"].get(rel.get("to"), {}).get("levels", {}).get(to_level)
        if level is None:
            diags.append(_error("vibedata/level-ref-unknown-level",
                                f"{where}: to_level '{to_level}' is not a level of dataset "
                                f"'{rel.get('to')}'"))
        elif level["keys"].get(level["primary"]) != rel.get("to_columns"):
            diags.append(_error("vibedata/to-level-key-mismatch",
                                f"{where}: to_columns must be the primary key of level "
                                f"'{to_level}' ({level['keys'].get(level['primary'])})"))
    weight = payload.get("weight")
    if weight is not None:
        source = next((d for d in doc.get("datasets", []) if d.get("name") == rel.get("from")), {})
        field = next((f for f in source.get("fields") or []
                      if isinstance(f, dict) and f.get("name") == weight), None)
        if field is None:
            diags.append(_error("vibedata/weight-unknown-field",
                                f"{where}: weight '{weight}' is not a declared field of "
                                f"'{rel.get('from')}'"))
        elif field.get("datatype") not in ("Float", "Decimal"):
            diags.append(_error("vibedata/weight-not-numeric",
                                f"{where}: weight '{weight}' must be a Float or Decimal share"))
    return diags


def metric_dimensionality(doc):
    """For each metric: the datasets its rows come from and every dataset a query may group
    or filter it by. Reachable means many-to-one relationships from the fact, plus a
    weighted bridge entered from its one side (the weight makes the fan-out an allocation).
    A metric with `unrelated_dimensions: total` answers every dimension, repeating its
    value where it has no path; a derived metric answers what all its inputs answer."""
    edges = {}
    for rel in doc.get("relationships", []):
        if not isinstance(rel, dict):
            continue
        edges.setdefault(rel.get("from"), set()).add(rel.get("to"))
        for data in _extensions(rel):
            try:
                decoded = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(decoded, dict) and decoded.get("weight"):
                edges.setdefault(rel.get("to"), set()).add(rel.get("from"))

    def reach(start):
        seen, stack = set(), [start]
        while stack:
            for nxt in edges.get(stack.pop(), ()):
                if nxt not in seen and nxt != start:
                    seen.add(nxt)
                    stack.append(nxt)
        return seen

    metrics = {m.get("name"): m for m in doc.get("metrics", []) if isinstance(m, dict)}
    all_datasets = {d.get("name") for d in doc.get("datasets", []) if isinstance(d, dict)}
    # A dimension is anything a relationship lands on, or a weighted bridge; other facts are not.
    dimensions = {rel.get("to") for rel in doc.get("relationships", []) if isinstance(rel, dict)}
    dimensions |= {n for targets in edges.values() for n in targets}
    out, resolving = {}, set()

    def resolve(name):
        if name in out:
            return out[name]
        if name in resolving or name not in metrics:
            return {"facts": [], "dimensions": [], "unrelated_dimensions": "error"}
        resolving.add(name)
        metric = metrics[name]
        payload = {}
        for data in _extensions(metric):
            try:
                payload.update(json.loads(data))
            except (TypeError, json.JSONDecodeError, ValueError):
                continue
        unrelated = payload.get("unrelated_dimensions", "error")
        inputs = sorted(_metric_inputs(payload) & set(metrics))
        if inputs:
            parts = [resolve(i) for i in inputs]
            facts = sorted({f for part in parts for f in part["facts"]})
            answered = [set(part["dimensions"]) for part in parts
                        if part["unrelated_dimensions"] != "total"]
            dims = set.intersection(*answered) if answered else set(dimensions)
        else:
            pairs = _metric_columns(metric)
            facts = sorted({d for d, _ in pairs if d in all_datasets})
            reached = [reach(f) for f in facts]
            dims = set.intersection(*reached) if reached else set()
        if unrelated == "total":
            dims = set(dimensions)
        out[name] = {"facts": facts, "dimensions": sorted(dims - set(facts)),
                     "unrelated_dimensions": unrelated}
        resolving.discard(name)
        return out[name]

    for name in metrics:
        resolve(name)
    return out


def _validate_relationship(rel, dataset_names, physical_join_names, doc):
    diags = []
    rel_name = rel.get("name", "<unnamed>")
    where = f"relationship '{rel_name}'"
    payloads = []
    for data in _extensions(rel):
        payload, errs = _decode(data, where)
        diags.extend(errs)
        if payload:
            payloads.append(payload)
    if len(payloads) > 1:
        diags.append(_error("vibedata/duplicate-entry",
                            f"{where}: multiple VIBEDATA entries; keep exactly one"))
    for payload in payloads:
        diags.extend(_unknown_keys(payload, RELATIONSHIP_KEYS, where))
        join = payload.get("physical_join")
        if join and join not in physical_join_names:
            diags.append(_error("vibedata/join-dangling",
                                f"{where}: physical_join '{join}' names no model-level "
                                "physical join"))
        if payload.get("join_expression") is not None:
            diags.extend(_check_join_expression(payload["join_expression"], rel, where, doc))
        cardinality = payload.get("cardinality")
        if cardinality and cardinality not in CARDINALITIES:
            diags.append(_error("vibedata/bad-cardinality",
                                f"{where}: cardinality '{cardinality}' must be one of "
                                f"{sorted(CARDINALITIES)}"))
        join_type = payload.get("join_type")
        if join_type is not None and join_type not in JOIN_TYPES:
            diags.append(_error("vibedata/bad-join-type",
                                f"{where}: join_type {join_type!r} must be one of "
                                f"{sorted(JOIN_TYPES)}"))
    return diags


def _validate_to_columns(rel, doc):
    """A relationship's to_columns must name fields the to dataset declares."""
    diags = []
    rel_name = rel.get("name", "<unnamed>")
    datasets = {d.get("name"): d for d in doc.get("datasets", []) if isinstance(d, dict)}
    target = datasets.get(rel.get("to"))
    columns = rel.get("to_columns")
    if not target or not isinstance(columns, list):
        return diags
    fields = {f.get("name") for f in target.get("fields", []) if isinstance(f, dict)}
    for column in columns:
        if column not in fields:
            diags.append(_error("relationship/to-column-unknown",
                                f"relationship '{rel_name}': to_column '{column}' is not a "
                                f"declared field of dataset '{rel.get('to')}'"))
    return diags


def _validate_from_columns(rel, doc):
    """A relationship's from_columns must name fields the from dataset declares."""
    diags = []
    rel_name = rel.get("name", "<unnamed>")
    datasets = {d.get("name"): d for d in doc.get("datasets", []) if isinstance(d, dict)}
    source = datasets.get(rel.get("from"))
    columns = rel.get("from_columns")
    if not source or not isinstance(columns, list):
        return diags
    fields = {f.get("name") for f in source.get("fields", []) if isinstance(f, dict)}
    for column in columns:
        if column not in fields:
            diags.append(_error("relationship/from-column-unknown",
                                f"relationship '{rel_name}': from_column '{column}' is not "
                                f"a declared field of dataset '{rel.get('from')}'"))
    return diags


# -- schema constraints ---------------------------------------------------------
#
# The checks above own keys, shapes, enums and references. vibedata-schema.json also
# constrains values (an identifier, a schema.table relation, an ISO date, a non-empty
# note); those are reported here, one code per JSON Schema keyword, and nothing the
# checks above already report is repeated.

SCHEMA_KEYWORDS = {"pattern", "anyOf", "minLength", "maxLength"}
PAYLOAD_DEFS = {"model": "modelPayload", "dataset": "datasetPayload",
                "relationship": "relationshipPayload", "metric": "metricPayload"}
_VALIDATORS = {}


def _payload_validator(component):
    if not _VALIDATORS:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        for name, ref in PAYLOAD_DEFS.items():
            _VALIDATORS[name] = Draft202012Validator(
                {"$ref": f"#/$defs/{ref}", "$defs": schema["$defs"]})
    return _VALIDATORS[component]


def _component_payloads(doc):
    """(component, where, payload) for every decodable VIBEDATA payload in `doc`."""
    nodes = [("model", "model", doc)]
    for key, component in (("datasets", "dataset"), ("relationships", "relationship"),
                           ("metrics", "metric")):
        for node in doc.get(key) or []:
            if isinstance(node, dict):
                nodes.append((component, f"{component} '{node.get('name', '<unnamed>')}'",
                              node))
    for component, where, node in nodes:
        for data in _extensions(node):
            try:
                payload = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                yield component, where, payload


def _schema_constraints(doc):
    diags = []
    for component, where, payload in _component_payloads(doc):
        for err in _payload_validator(component).iter_errors(payload):
            if err.validator not in SCHEMA_KEYWORDS:
                continue
            path = ".".join(str(p) for p in err.absolute_path) or "(payload)"
            what = ("is not an Ossie SQL identifier" if err.validator == "anyOf"
                    else err.message)
            diags.append(_error(f"vibedata/schema-{err.validator.lower()}",
                                f"{where}: {path}: {err.instance!r} {what}"
                                if err.validator == "anyOf" else f"{where}: {path}: {what}"))
    return diags


# -- expression kinds and Ossie core conformance ------------------------------------

ANSI_DIALECTS = ("ANSI_SQL", "OSSIE_SQL_2026")


def _core_expressions(node):
    """The ANSI/Ossie-dialect expression texts of a field or metric."""
    for dialect in ((node.get("expression") or {}).get("dialects") or []):
        if isinstance(dialect, dict) and dialect.get("dialect") in ANSI_DIALECTS \
                and isinstance(dialect.get("expression"), str):
            yield dialect["expression"]


def _parsed(text):
    try:
        return ossie_sql.parse(text)
    except ExpressionError:
        return None  # a parse failure is reported where the expression is first checked


def _unlisted(node, where):
    names = ossie_functions.unlisted(node)
    if not names:
        return []
    return [{"severity": "warning", "code": "ossie/function-unlisted",
             "message": f"{where}: {', '.join(names)} is not in Ossie's REQUIRED or "
                        "RECOMMENDED function list, so the expression may not port"}]


def _kind(text, kind, code, where):
    if not isinstance(text, str) or _parsed(text) is None:
        return []
    reason = ossie_sql.check_kind(text, kind)
    diags = [_error(code, f"{where}: {reason}")] if reason else []
    return diags + _unlisted(_parsed(text), where)


def _expression_checks(doc):
    """Each expression is the kind its place needs; Ossie core's own aggregation rules."""
    diags = []
    for dataset in doc.get("datasets") or []:
        if not isinstance(dataset, dict):
            continue
        name = dataset.get("name", "<unnamed>")
        for field in dataset.get("fields") or []:
            if not isinstance(field, dict):
                continue
            where = f"field '{name}.{field.get('name')}'"
            for text in _core_expressions(field):
                node = _parsed(text)
                if node is None:
                    continue
                if ossie_sql.has_aggregate(node):
                    diags.append(_error("ossie/field-aggregate",
                                        f"{where}: a field expression aggregates; an "
                                        "aggregate is a metric"))
                diags.extend(_unlisted(node, where))
        for data in _extensions(dataset):
            try:
                payload = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                continue
            for src in (payload.get("logical_sources") or []) if isinstance(payload, dict) else []:
                if isinstance(src, dict) and src.get("content_filter") is not None:
                    diags.extend(_kind(src["content_filter"], ossie_sql.PREDICATE,
                                       "vibedata/content-filter-kind",
                                       f"{name}.{src.get('name')} content_filter"))
    for metric in doc.get("metrics") or []:
        if not isinstance(metric, dict):
            continue
        where = f"metric '{metric.get('name')}'"
        for text in _core_expressions(metric):
            node = _parsed(text)
            if node is None:
                continue
            if not ossie_sql.has_aggregate(node):
                diags.append(_error("ossie/metric-not-aggregate",
                                    f"{where}: a metric expression aggregates rows; this "
                                    "one has no aggregate function"))
            diags.extend(_unlisted(node, where))
        for data in _extensions(metric):
            try:
                payload = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            if payload.get("derived_expression") is not None:
                diags.extend(_kind(payload["derived_expression"], ossie_sql.METRIC_FORMULA,
                                   "vibedata/derived-kind", f"{where} derived_expression"))
            if payload.get("filter") is not None:
                diags.extend(_kind(payload["filter"], ossie_sql.PREDICATE,
                                   "vibedata/filter-kind", f"{where} filter"))
    for rel in doc.get("relationships") or []:
        for data in _extensions(rel):
            try:
                payload = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict) and payload.get("join_expression") is not None:
                diags.extend(_kind(payload["join_expression"], ossie_sql.PREDICATE,
                                   "vibedata/join-expression-kind",
                                   f"relationship '{rel.get('name')}' join_expression"))
    return diags
