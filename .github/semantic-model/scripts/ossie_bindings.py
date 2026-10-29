"""Check each aggregate binding against the SQL that builds the aggregate.

An aggregate logical source claims that its columns give the detail's answer. The
generated content test checks that claim on data. This module checks it on the
compiled SQL in dbt's manifest, so the claim is checked even when no warehouse is
built. For each aggregate source (`priority_group: 0`) it finds the SELECT that
aggregates each bound metric's column, then compares:

- `bindings/aggregation-mismatch` (error): the column's aggregation cannot roll up to
  the metric's. A SUM metric needs SUM or COUNT; MIN needs MIN; MAX needs MAX.
- `bindings/key-not-grouped` (error): a content level's key column is neither selected
  nor grouped by that SELECT, when it is the model's final SELECT. In an inner SELECT a
  key may be renamed or joined to on the way out, so there it is `bindings/unproven`.
- `bindings/finer-than-declared` (warning): that SELECT groups by the key of a level
  below one of the source's content levels, in that level's own dimension, so the
  source is finer than declared. A coarser level's key or an attribute adds no rows and
  is not flagged.
- `bindings/undeclared-filter` (warning): that SELECT, or a CTE it reads, filters rows
  with WHERE, but the source declares no content filter. A non-empty filter that is
  spelled differently is left to the content test, which compares it on data.
- `bindings/unproven` (warning): the model is not in the manifest, the SQL does not
  parse, or the column cannot be traced to an aggregation.

Only a mismatch the SQL proves is an error, so a parser gap never blocks a change.
Pure functions: input is parsed documents and the parsed manifest, output diagnostics.
"""

import re

import sqlglot
from sqlglot import exp

import ossie_sql
from ossie_compile import ROLLUPS, Model, _payload

# The aggregations a pre-aggregated column may hold, per metric rollup. A COUNT is a sum
# of ones, so it rolls up by SUM.
ROLLS_UP = {"SUM": {"SUM", "COUNT"}, "MIN": {"MIN"}, "MAX": {"MAX"}}
JINJA = re.compile(r"{{|{%")
REF = re.compile(r"{{\s*(?:ref|source)\(([^)]*)\)\s*}}")


def _diag(severity, code, message, file):
    return {"severity": severity, "code": code, "message": message, "file": file}


def model_sql(manifest):
    """{dbt model name: SQL} from a dbt manifest, compiled where dbt compiled it.

    Without compiled code, raw SQL whose only Jinja is `ref` and `source` is used with
    each call replaced by its last argument; any other Jinja leaves the model out.
    """
    out = {}
    for node in (manifest.get("nodes") or {}).values():
        if node.get("resource_type") != "model" or not node.get("name"):
            continue
        sql = node.get("compiled_code") or node.get("compiled_sql")
        if not sql:
            raw = node.get("raw_code") or node.get("raw_sql") or ""
            raw = REF.sub(lambda m: m.group(1).split(",")[-1].strip().strip("'\""), raw)
            sql = None if JINJA.search(raw) else raw
        if sql:
            out[node["name"]] = sql
    return out


def _kind(agg):
    if isinstance(agg, exp.Count):
        return "COUNT_DISTINCT" if isinstance(agg.this, exp.Distinct) else "COUNT"
    if isinstance(agg.this, exp.Distinct) and not isinstance(agg, (exp.Min, exp.Max)):
        return f"{type(agg).__name__.upper()}_DISTINCT"  # SUM(DISTINCT x) does not roll up; MIN/MAX do
    for cls, name in ((exp.Sum, "SUM"), (exp.Min, "MIN"), (exp.Max, "MAX"), (exp.Avg, "AVG")):
        if isinstance(agg, cls):
            return name
    return type(agg).__name__.upper()


def _aggregates(node):
    """The aggregate calls in `node` that group rows: not inside a window or another one."""
    out = []
    for agg in node.find_all(exp.AggFunc):
        parent = agg.parent
        while parent is not None and parent is not node \
                and not isinstance(parent, (exp.AggFunc, exp.Window)):
            parent = parent.parent
        if parent is None or parent is node or not isinstance(parent, (exp.AggFunc, exp.Window)):
            out.append(agg)
    return out


def _outer_aggregations(node):
    return {_kind(agg) for agg in _aggregates(node)}


def _name(node):
    return (node.alias_or_name or "").lower()


class Query:
    """One parsed model: its CTEs by name and its final SELECT."""

    def __init__(self, sql, dialect):
        tree = sqlglot.parse_one(sql, dialect=dialect)
        self.ctes = {}
        for cte in tree.find_all(exp.CTE):
            self.ctes[cte.alias_or_name.lower()] = cte.this
        self.final = self._unwrap(tree)

    def _sources(self, select):
        """{alias: SELECT or None} for each relation `select` reads; None is a table."""
        out = {}
        tables = []
        source = select.args.get("from") or select.args.get("from_")  # sqlglot < 30 / >= 30
        if source is not None:
            tables.append(source.this)
        tables += [j.this for j in select.args.get("joins") or []]
        for table in tables:
            alias = (table.alias_or_name or "").lower()
            if isinstance(table, exp.Subquery):
                out[alias] = table.this
            elif isinstance(table, exp.Table) and not table.db and table.name.lower() in self.ctes:
                out[alias] = self.ctes[table.name.lower()]
            else:
                out[alias] = None
        return out

    def _unwrap(self, node):
        """Step through `select * from <cte>` wrappers to the SELECT that shapes the rows."""
        while isinstance(node, exp.Select):
            star = (len(node.expressions) == 1 and isinstance(node.expressions[0], exp.Star))
            plain = not any(node.args.get(k) for k in ("where", "group", "joins", "having"))
            sources = self._sources(node)
            if not (star and plain and len(sources) == 1):
                break
            inner = next(iter(sources.values()))
            if inner is None:
                break
            node = inner
        return node

    def trace(self, select, column, depth=0):
        """(SELECT that aggregates `column`, its projection) or (None, reason)."""
        if depth > 20 or not isinstance(select, exp.Select):
            return None, "the SQL is not a plain SELECT"
        sources = self._sources(select)
        stars = []
        for projection in select.expressions:
            if isinstance(projection, exp.Star):
                stars.append(None)
                continue
            if isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star):
                stars.append((projection.table or "").lower())
                continue
            if _name(projection) != column:
                continue
            if _aggregates(projection):
                return select, projection
            inner = projection.this if isinstance(projection, exp.Alias) else projection
            if isinstance(inner, exp.Column):
                source = sources.get((inner.table or "").lower())
                if source is None and not inner.table and len(sources) == 1:
                    source = next(iter(sources.values()))
                if source is not None:
                    return self.trace(self._unwrap(source), inner.name.lower(), depth + 1)
            return None, f"'{column}' is not computed by an aggregation in the SQL"
        for alias in stars:
            if alias is None:
                candidates = list(sources.values()) if len(sources) == 1 else []
            else:
                candidates = [sources.get(alias)]
            for source in candidates:
                if source is None:
                    continue
                found = self.trace(self._unwrap(source), column, depth + 1)
                if found[0] is not None:
                    return found
        return None, f"no column '{column}' in the SQL's select list"

    def inner_kinds(self, select, projection, depth=0):
        """Aggregations under `projection`'s aggregate arguments, traced into the SELECTs read."""
        kinds = set()
        if depth > 20:
            return kinds
        sources = self._sources(select)
        for agg in _aggregates(projection):
            for column in agg.find_all(exp.Column):
                if isinstance(column.this, exp.Star):
                    continue
                source = sources.get((column.table or "").lower())
                if source is None and not column.table and len(sources) == 1:
                    source = next(iter(sources.values()))
                if source is None:
                    continue
                inner, found = self.trace(self._unwrap(source), column.name.lower())
                if inner is not None:
                    kinds |= _outer_aggregations(found) | self.inner_kinds(inner, found, depth + 1)
        return kinds

    def filters(self, select, depth=0):
        """The AND-ed WHERE terms of `select` and of the CTEs and subqueries it reads."""
        terms = []
        where = select.args.get("where")
        if where is not None:
            node = where.this
            terms += node.flatten() if isinstance(node, exp.And) else [node]
        source = select.args.get("from") or select.args.get("from_")
        if depth < 20 and source is not None:
            inner = self._sources(select).get((source.this.alias_or_name or "").lower())
            if isinstance(inner, exp.Select):  # the rows' own chain, not a joined dimension
                terms += self.filters(inner, depth + 1)
        return terms


def _canonical(term):
    """A WHERE term with table qualifiers dropped, in the generic dialect."""
    term = term.copy()
    for column in term.find_all(exp.Column):
        column.set("table", None)
    try:
        return ossie_sql.conjuncts(term.sql())
    except ossie_sql.ExpressionError:
        return [term.sql().lower()]


def check(doc, sources_sql, where, dialect="duckdb"):
    """Diagnostics for one model document against {dbt model: SQL}."""
    diags = []
    payload = _payload(doc)
    tables = {t["name"]: t for t in payload.get("physical_tables") or [] if isinstance(t, dict)}
    metrics = {m["name"]: m for m in doc.get("metrics") or [] if isinstance(m, dict)}
    model = Model(doc, lambda t: t.get("source"))
    for dataset in doc.get("datasets") or []:
        for src in _payload(dataset).get("logical_sources") or []:
            if src.get("priority_group", 1) != 0 or not list(_bound(src, metrics)):
                continue  # a source that binds no metric has nothing to prove
            label = f"logical source '{dataset.get('name')}.{src.get('name')}'"
            table = tables.get(src.get("physical_table")) or {}
            dbt_model = table.get("dbt_model")
            sql = sources_sql.get(dbt_model)
            if not sql:
                diags.append(_diag("warning", "bindings/unproven",
                                   f"{label}: dbt model '{dbt_model}' has no SQL in the "
                                   "manifest; run `dbt compile` first", where))
                continue
            try:
                query = Query(sql, dialect)
            except sqlglot.errors.SqlglotError as e:
                diags.append(_diag("warning", "bindings/unproven",
                                   f"{label}: the SQL of '{dbt_model}' does not parse: "
                                   f"{str(e).splitlines()[0]}", where))
                continue
            diags += _source_checks(query, src, metrics, label, where, model)
    return diags


def _finer(column, src, model):
    """True when `column` keys a level of a content level's dimension below that level.

    Grouping by a coarser level's key (a borough beside its zone) or by an attribute
    adds no rows, so only a strictly finer level of the same dimension is flagged.
    """
    for ref in src.get("content_levels") or []:
        relationship = model.relationships.get(ref.get("via")) or {}
        dataset = relationship.get("to")
        if dataset not in model.datasets:
            continue
        levels = model.payloads[dataset].get("levels") or []
        row = model.row_level(dataset)
        for level in levels:
            fields = {str(f).lower() for key in level.get("keys") or []
                      for f in key.get("fields") or []}
            if column in fields and _below(model, dataset, level["name"], ref.get("level")):
                return True
        if ref.get("level") in (None, row):
            continue
        row_keys = {str(k).lower() for k in model.datasets[dataset].get("primary_key") or []}
        column_map = {k: str(v).lower() for k, v in (src.get("column_map") or {}).items()}
        row_keys |= {column_map.get(f, str(f).lower())
                     for f in relationship.get("from_columns") or []}
        if column in row_keys:
            return True
    return False


def _below(model, dataset, level, target):
    """True only when `level` is provably finer than `target` (None: the row level): below it
    in a hierarchy, or the row level under a coarser target. An unordered level is not finer."""
    row = model.row_level(dataset)
    target = target or row
    if level == target:
        return False
    if level == row:
        return True
    return model.at_or_above(dataset, target, level)


def _bound(src, metrics):
    for logical, physical in sorted((src.get("column_map") or {}).items()):
        metric = metrics.get(logical)
        if metric is None:
            continue
        payload = _payload(metric)
        if payload.get("derived_expression") or payload.get("aggregation") not in ROLLUPS:
            continue
        yield logical, str(physical).lower(), payload["aggregation"]


def _source_checks(query, src, metrics, label, where, model):
    diags = []
    aggregating = []
    for metric, column, rollup in _bound(src, metrics):
        select, found = query.trace(query.final, column)
        if select is None:
            diags.append(_diag("warning", "bindings/unproven",
                               f"{label}: cannot trace metric '{metric}': {found}", where))
            continue
        if not any(select is s for s in aggregating):
            aggregating.append(select)
        kinds = _outer_aggregations(found)
        nested = query.inner_kinds(select, found)
        if len(kinds) != 1:
            diags.append(_diag("warning", "bindings/unproven",
                               f"{label}: '{column}' combines {sorted(kinds)}, so its rollup "
                               f"for '{metric}' cannot be proven", where))
        elif not (kinds | nested) <= ROLLS_UP[rollup]:
            wrong = sorted((kinds | nested) - ROLLS_UP[rollup])
            diags.append(_diag("error", "bindings/aggregation-mismatch",
                               f"{label}: metric '{metric}' rolls up by {rollup}, but "
                               f"'{column}' is computed with {', '.join(wrong)}, which does not",
                               where))
    keys = [str(c).lower() for ref in src.get("content_levels") or []
            for c in ref.get("key_columns") or []]
    for select in aggregating:
        groups = {_name(p) for p in select.expressions
                  if not isinstance(p, exp.Star) and not _aggregates(p)}
        grouped_by = {c.name.lower() for g in (select.args.get("group") or exp.Group()).expressions
                      for c in g.find_all(exp.Column)}
        # The final SELECT names the published columns, so a key it lacks is proven missing;
        # an inner one may be renamed or joined to on the way out, so there it is unproven.
        final = select is query.final
        for key in keys:
            if key not in groups and key not in grouped_by:
                diags.append(_diag("error" if final else "warning",
                                   "bindings/key-not-grouped" if final else "bindings/unproven",
                                   f"{label}: content-level key '{key}' is not a grouping column "
                                   f"of the aggregating SELECT (it groups {sorted(groups)})",
                                   where))
        finer = sorted(c for c in (groups | grouped_by) - set(keys) if _finer(c, src, model))
        if finer:
            diags.append(_diag("warning", "bindings/finer-than-declared",
                               f"{label}: the SQL also groups by {finer}, which the model "
                               "declares as keys below a content level of this source; the "
                               "source is finer than its content levels say", where))
        if not src.get("content_filter") and query.filters(select):
            terms = sorted({t for term in query.filters(select) for t in _canonical(term)})
            diags.append(_diag("warning", "bindings/undeclared-filter",
                               f"{label}: the SQL filters rows with {terms}, but the source "
                               "declares no content_filter, so it claims the whole detail "
                               "population", where))
    return diags
