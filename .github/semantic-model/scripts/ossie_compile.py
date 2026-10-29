"""Compile a semantic query against one Ossie model document into SQL.

A query names metrics, the levels to group by (each reached through a relationship
from the fact, as `{via, level, key}`), and an optional filter over the fact's
fields. The compiler answers it from an aggregate source only when that is provably
the same answer as the detail, and otherwise from the detail:

1. the source binds every metric input, with a rollup rule (`aggregation`) of SUM,
   MIN or MAX: a NON_ADDITIVE metric is never read from an aggregate;
2. every role the query groups by is a content level of the source, and the queried
   level is that level or above it in one of the dimension's hierarchies;
3. a versioned dimension is never rolled up from an aggregate: rolling a version up
   to its parent "as of" the fact needs the fact's date, which the aggregate lost;
4. the query filter implies the content filter (every content conjunct appears in the
   query filter, compared in canonical form: sound, not complete);
5. the rest of the query filter reads only fields the source maps;
6. a source marked `navigable: false` is used only when the query names it.

The detail joins each dimension with a LEFT JOIN on a many-to-one relationship, so it
neither drops nor repeats a fact row. The join is the relationship's `join_expression`
when it declares one, else its core from/to columns. A `derived_expression` is applied after its
inputs are aggregated. Level-based, time-series and dimension-rule metrics, metrics
over several datasets and `scd_rollup: current` are not compiled yet: they raise
CompileError rather than return a wrong answer.
"""

import json
from dataclasses import dataclass, field

import ossie_sql

VENDOR = "VIBEDATA"
ROLLUPS = {"SUM", "MIN", "MAX"}
UNSUPPORTED = ("at_levels", "time_series", "based_on_dimensions")


class CompileError(ValueError):
    """The query cannot be compiled soundly."""


class _Ineligible(Exception):
    """An aggregate source cannot answer the query; the message says why."""


def _payload(node):
    for ext in node.get("custom_extensions") or []:
        if isinstance(ext, dict) and ext.get("vendor_name") == VENDOR:
            try:
                data = json.loads(ext.get("data") or "{}")
            except json.JSONDecodeError:
                return {}
            return data if isinstance(data, dict) else {}
    return {}


@dataclass
class Query:
    metrics: list
    group_by: list = field(default_factory=list)
    filter: str | None = None
    source: str | None = None  # an LTS name, or "detail"

    @classmethod
    def from_dict(cls, data):
        return cls(metrics=list(data.get("metrics") or []),
                   group_by=list(data.get("group_by") or []),
                   filter=data.get("filter"), source=data.get("source"))


@dataclass
class Compiled:
    sql: str
    source: str
    columns: list
    rejected: dict


class Model:
    """One parsed Ossie document; `relation(table)` returns the SQL relation for a
    physical_tables entry."""

    def __init__(self, doc, relation):
        self.doc = doc
        self.relation = relation
        model = _payload(doc)
        self.tables = {t["name"]: t for t in model.get("physical_tables") or []}
        self.datasets = {d["name"]: d for d in doc.get("datasets") or []}
        self.payloads = {name: _payload(d) for name, d in self.datasets.items()}
        self.relationships = {r["name"]: r for r in doc.get("relationships") or []}
        self.metrics = {m["name"]: m for m in doc.get("metrics") or []}

    # -- datasets and levels -------------------------------------------------

    def sources(self, dataset):
        return self.payloads[dataset].get("logical_sources") or []

    def detail(self, dataset):
        for src in self.sources(dataset):
            table = self.tables.get(src.get("physical_table"), {})
            if src.get("priority_group", 1) != 0 and table.get("table_type") != "ALIAS":
                return src
        raise CompileError(f"dataset '{dataset}' has no detail source")

    def column(self, src, logical, dataset):
        physical = (src.get("column_map") or {}).get(logical)
        if physical is None:
            raise CompileError(f"source '{src['name']}' of '{dataset}' maps no column for "
                               f"'{logical}'")
        return physical

    def row_level(self, dataset):
        return (self.payloads[dataset].get("row_level") or {}).get("level")

    def level_fields(self, dataset, level, key=None):
        """The fields of a level's key (default the primary); level None is the row key."""
        if level is None:
            return list(self.datasets[dataset].get("primary_key") or [])
        for lv in self.payloads[dataset].get("levels") or []:
            if lv["name"] == level:
                for k in lv["keys"]:
                    if (key is None and k.get("primary")) or k["name"] == key:
                        return list(k["fields"])
                raise CompileError(f"level '{level}' of '{dataset}' has no key '{key}'")
        raise CompileError(f"dataset '{dataset}' has no level '{level}'")

    def at_or_above(self, dataset, upper, lower):
        upper = upper or self.row_level(dataset)
        lower = lower or self.row_level(dataset)
        if upper == lower:
            return True
        for hier in self.payloads[dataset].get("hierarchies") or []:
            names = hier.get("levels") or []
            if upper in names and lower in names and names.index(upper) < names.index(lower):
                return True
        return False

    def versioned(self, dataset):
        return bool((self.payloads[dataset].get("row_level") or {}).get("versioned"))

    # -- metrics --------------------------------------------------------------

    def expression(self, metric):
        for d in self.metrics[metric]["expression"]["dialects"]:
            if d.get("dialect") in ("ANSI_SQL", "OSSIE_SQL_2026"):
                return d["expression"]
        raise CompileError(f"metric '{metric}' has no ANSI_SQL expression")

    def kind(self, metric):
        if metric not in self.metrics:
            raise CompileError(f"unknown metric '{metric}'")
        payload = _payload(self.metrics[metric])
        for key in UNSUPPORTED:
            if payload.get(key) is not None:
                raise CompileError(f"metric '{metric}' uses {key}, which the compiler does "
                                   "not compile yet")
        return "derived" if payload.get("derived_expression") else "base"

    def formula(self, metric):
        return _payload(self.metrics[metric])["derived_expression"]

    def aggregation(self, metric):
        return _payload(self.metrics[metric]).get("aggregation")

    def base_inputs(self, metric, seen=()):
        if metric in seen:
            raise CompileError(f"metric '{metric}' reaches itself")
        if self.kind(metric) == "base":
            return [metric]
        names, _ = ossie_sql.names(self.formula(metric))
        out = []
        for name in sorted(names):
            out += [m for m in self.base_inputs(name, seen + (metric,)) if m not in out]
        return out

    def fact_of(self, metrics):
        facts = set()
        for metric in metrics:
            for base in self.base_inputs(metric):
                columns, _ = ossie_sql.names(self.expression(base))
                tables = {c.split(".")[0] for c in columns if "." in c}
                if len(tables) != 1:
                    raise CompileError(f"metric '{base}' reads {sorted(tables) or 'no dataset'}; "
                                       "the compiler handles single-dataset metrics only")
                facts |= tables
        if len(facts) != 1:
            raise CompileError(f"the metrics read different datasets: {sorted(facts)}")
        return facts.pop()


def _metric_sql(model, metric, base_sql):
    """SQL for a metric: base metrics by `base_sql`, a formula over its inputs' SQL."""
    if model.kind(metric) == "base":
        return base_sql(metric)
    return ossie_sql.rewrite(model.formula(metric),
                             name=lambda n: _metric_sql(model, n, base_sql))


def _alias(ref, i, field_name, many):
    stem = f"{ref['via']}__{ref.get('level') or 'row'}"
    return f"{stem}__{field_name}" if many else stem


def _assemble(select, relation, alias, joins, where, grouped):
    lines = ["select", "    " + ",\n    ".join(select), f"from {relation} as {alias}"]
    lines += joins
    if where:
        lines.append("where " + "\n    and ".join(ossie_sql.grouped(term) for term in where))
    if grouped:
        lines.append("group by " + ", ".join(str(i + 1) for i in range(grouped)))
    return "\n".join(lines)


def _compile_detail(model, query, fact):
    src = model.detail(fact)
    selects, joins, columns = [], [], []
    for i, ref in enumerate(query.group_by):
        rel = model.relationships.get(ref.get("via"))
        if rel is None or rel.get("from") != fact:
            raise CompileError(f"group_by via '{ref.get('via')}' is not a relationship from "
                               f"'{fact}'")
        cardinality = _payload(rel).get("cardinality", "many-to-one")
        if cardinality not in ("many-to-one", "one-to-one"):
            raise CompileError(f"relationship '{rel['name']}' is {cardinality}; joining it "
                               "would repeat fact rows")
        target = rel["to"]
        level = ref.get("level")
        fields = model.level_fields(target, level, ref.get("key"))
        many = len(fields) > 1
        if level is None or level == model.row_level(target) and not model.versioned(target) \
                and fields == list(model.datasets[target].get("primary_key") or []):
            # The fact carries the row key itself.
            exprs = [f"f.{model.column(src, c, fact)}" for c in rel["from_columns"]]
        else:
            rollup = next((h.get("scd_rollup") for h in model.payloads[target].get("hierarchies") or []
                           if level in (h.get("levels") or [])), None)
            if model.versioned(target) and rollup == "current":
                raise CompileError(f"'{target}' rolls up as it is now (scd_rollup: current), "
                                   "which the compiler does not compile yet")
            dim_src = model.detail(target)
            table = model.tables[dim_src["physical_table"]]
            predicate = _payload(rel).get("join_expression")
            if predicate:
                def side(table, name, i=i, dim_src=dim_src, target=target):
                    if table == fact:
                        return f"f.{model.column(src, name, fact)}"
                    return f"d{i}.{model.column(dim_src, name, target)}"
                on = ossie_sql.rewrite(predicate, column=side)
            else:
                on = " and ".join(
                    f"f.{model.column(src, fc, fact)} = d{i}.{model.column(dim_src, tc, target)}"
                    for fc, tc in zip(rel["from_columns"], rel["to_columns"]))
            joins.append(f"left join {model.relation(table)} as d{i} on {on}")
            exprs = [f"d{i}.{model.column(dim_src, c, target)}" for c in fields]
        for expr, name in zip(exprs, fields):
            alias = _alias(ref, i, name, many)
            selects.append(f"{expr} as {alias}")
            columns.append(alias)
    grouped = len(selects)

    def base_sql(metric):
        return ossie_sql.rewrite(model.expression(metric),
                                 column=lambda t, n: f"f.{model.column(src, n, fact)}")

    for metric in query.metrics:
        selects.append(f"{_metric_sql(model, metric, base_sql)} as {metric}")
        columns.append(metric)
    where = [_rewrite(term, column=lambda t, n: f"f.{model.column(src, n, fact)}")
             for term in ossie_sql.conjuncts(query.filter)]
    relation = model.relation(model.tables[src["physical_table"]])
    return _assemble(selects, relation, "f", joins, where, grouped), columns


def _rewrite(text, **replace):
    """ossie_sql.rewrite, with an unsupported construct reported as a CompileError."""
    try:
        return ossie_sql.rewrite(text, **replace)
    except ossie_sql.ExpressionError as e:
        raise CompileError(str(e)) from e


def _compile_aggregate(model, query, fact, src, forced):
    if src.get("navigable", True) is False and not forced:
        raise _Ineligible("not navigable: read only when the query names it")
    held = {ref["via"]: ref for ref in src.get("content_levels") or []}
    selects, joins, columns = [], [], []
    for i, ref in enumerate(query.group_by):
        content = held.get(ref.get("via"))
        if content is None:
            raise _Ineligible(f"holds no level of role '{ref.get('via')}'")
        target = model.relationships[ref["via"]]["to"]
        level, key = ref.get("level"), ref.get("key")
        fields = model.level_fields(target, level, key)
        many = len(fields) > 1
        content_fields = model.level_fields(target, content.get("level"), content.get("key"))
        same_level = (level or model.row_level(target)) == (content.get("level") or model.row_level(target))
        if same_level and fields == content_fields:
            exprs = [f"a.{c}" for c in content["key_columns"]]
        else:
            if not model.at_or_above(target, level, content.get("level")):
                raise _Ineligible(f"holds '{ref['via']}' at level "
                                  f"'{content.get('level') or 'row'}', which does not roll up "
                                  f"to '{level or 'row'}'")
            if model.versioned(target):
                raise _Ineligible(f"'{target}' is versioned; rolling it up from an aggregate "
                                  "needs the fact's date")
            dim_src = model.detail(target)
            table = model.relation(model.tables[dim_src["physical_table"]])
            have = [model.column(dim_src, c, target) for c in content_fields]
            want = [model.column(dim_src, c, target) for c in fields]
            pick = ", ".join([f"{c} as k{j}" for j, c in enumerate(have)]
                             + [f"{c} as v{j}" for j, c in enumerate(want)])
            on = " and ".join(f"d{i}.k{j} = a.{c}" for j, c in enumerate(content["key_columns"]))
            joins.append(f"left join (select distinct {pick} from {table}) as d{i} on {on}")
            exprs = [f"d{i}.v{j}" for j in range(len(want))]
        for expr, name in zip(exprs, fields):
            alias = _alias(ref, i, name, many)
            selects.append(f"{expr} as {alias}")
            columns.append(alias)
    grouped = len(selects)
    column_map = src.get("column_map") or {}

    def base_sql(metric):
        rule = model.aggregation(metric)
        if rule not in ROLLUPS:
            raise _Ineligible(f"metric '{metric}' has rollup rule {rule}, which an aggregate "
                              "cannot apply")
        if metric not in column_map:
            raise _Ineligible(f"binds no column for '{metric}'")
        return f"{rule.lower()}(a.{column_map[metric]})"

    for metric in query.metrics:
        selects.append(f"{_metric_sql(model, metric, base_sql)} as {metric}")
        columns.append(metric)
    asked = ossie_sql.conjuncts(query.filter)
    content_terms = ossie_sql.conjuncts(src.get("content_filter"))
    missing = [t for t in content_terms if t not in asked]
    if missing:
        raise _Ineligible("the query filter does not imply its content filter (missing "
                          + "; ".join(missing) + ")")

    def mapped(table, name):
        if name not in column_map or name in model.metrics:
            raise _Ineligible(f"the filter reads '{name}', which it does not map")
        return f"a.{column_map[name]}"

    where = [_rewrite(t, column=mapped) for t in asked if t not in content_terms]
    relation = model.relation(model.tables[src["physical_table"]])
    return _assemble(selects, relation, "a", joins, where, grouped), columns


def compile_query(model, query):
    """Compile `query` (a Query) against `model`; return a Compiled."""
    if not query.metrics:
        raise CompileError("a query names at least one metric")
    fact = model.fact_of(query.metrics)
    rejected = {}
    aggregates = [s for s in model.sources(fact) if s.get("priority_group", 1) == 0]
    if query.source not in (None, "detail"):
        chosen = next((s for s in aggregates if s["name"] == query.source), None)
        if chosen is None:
            raise CompileError(f"'{query.source}' is no aggregate source of '{fact}'")
        try:
            sql, columns = _compile_aggregate(model, query, fact, chosen, forced=True)
        except _Ineligible as e:
            raise CompileError(f"'{chosen['name']}' cannot answer the query: {e}") from e
        return Compiled(sql, chosen["name"], columns, rejected)
    if query.source is None:
        ranked = sorted(enumerate(aggregates),
                        key=lambda p: (p[1].get("join_cost", float("inf")), p[0]))
        for _, src in ranked:
            try:
                sql, columns = _compile_aggregate(model, query, fact, src, forced=False)
            except _Ineligible as e:
                rejected[src["name"]] = str(e)
                continue
            return Compiled(sql, src["name"], columns, rejected)
    sql, columns = _compile_detail(model, query, fact)
    return Compiled(sql, model.detail(fact)["name"], columns, rejected)
