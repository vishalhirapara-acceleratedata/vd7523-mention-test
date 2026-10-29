"""The one SQL expression layer every other tool uses, aligned with Ossie's own.

Every predicate (`content_filter`, `join_expression`, a query filter) and formula
(`derived_expression`, a metric's core expression) is checked here exactly as Apache
Ossie checks a core expression: by the vendored `validate_sql_expression`, with the
same sqlglot dialect mapping and the same single-expression rule. It is then parsed in
that generic (OSSIE_SQL_2026 / ANSI) dialect, and unquoted identifiers are normalised,
because the Ossie expression language makes them case-insensitive.

One thing goes beyond Ossie's grammar: compiled SQL, and so every generated test, is
rendered in sqlglot's `duckdb` dialect. A `:name` placeholder parses, but nothing may use one yet:
the validator reports it, and `rewrite` refuses it.
"""

import importlib.util
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

TARGET = "duckdb"
OSSIE_DIALECT = "OSSIE_SQL_2026"

_spec = importlib.util.spec_from_file_location(
    "ossie_vendor_validate_sql",
    Path(__file__).parent / "ossie_vendor" / "validation" / "validate.py")
_vendor = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_vendor)


class ExpressionError(ValueError):
    """An expression Ossie's own check rejects, or that does not parse."""


def parse(text):
    """Check `text` as Ossie does, then parse it with unquoted identifiers normalised."""
    if not isinstance(text, str) or not text.strip():
        raise ExpressionError("empty expression")
    error = _vendor.validate_sql_expression(text, OSSIE_DIALECT, "expression")
    if error:
        raise ExpressionError(error.replace("[SQL] expression: ", ""))
    try:
        node = sqlglot.parse_one(text)
    except sqlglot.errors.SqlglotError as e:
        raise ExpressionError(str(e).splitlines()[0]) from e
    if node is None:
        raise ExpressionError("empty expression")
    return normalize_identifiers(node)


def names(text):
    """(column names, :placeholder names) an expression reads. Qualified columns keep 'table.name'."""
    node = parse(text)
    columns = {f"{c.table}.{c.name}" if c.table else c.name for c in node.find_all(exp.Column)}
    variables = {p.name for p in node.find_all(exp.Placeholder) if p.name}
    return columns, variables


def conjuncts(text):
    """The canonical AND-ed terms of a predicate; None or empty text is no condition.

    Keywords, spacing and unquoted identifier case are normalised, so two spellings of
    one condition compare equal. Terms render in the generic dialect.
    """
    if text is None or not str(text).strip():
        return []
    node = parse(text)
    terms = node.flatten() if isinstance(node, exp.And) else [node]
    return [term.sql() for term in terms]


def rewrite(text, column=None, name=None):
    """Return `text` rendered in the target dialect with columns and bare names replaced.

    `column(table, name)` returns replacement SQL for a column reference (or None to
    keep it); `name(identifier)` handles unqualified columns that stand for another
    object (a metric in a formula). A `:placeholder` raises ExpressionError.
    """
    node = parse(text)

    def swap(n):
        if isinstance(n, exp.Placeholder):
            raise ExpressionError(f"':{n.name}' is a variable; variables are not supported yet")
        if isinstance(n, exp.Column):
            replacement = None
            if not n.table and name is not None:
                replacement = name(n.name)
            if replacement is None and column is not None:
                replacement = column(n.table or None, n.name)
            if replacement is not None:
                return exp.paren(sqlglot.parse_one(replacement, dialect=TARGET), copy=False)
        return n

    return node.transform(swap).sql(dialect=TARGET)



def grouped(sql):
    """`sql` (rendered in TARGET) parenthesised when it is an AND/OR at its root.

    conjuncts() splits only the top-level AND, so a term may itself be an OR; joined
    back with AND unparenthesised, `w AND a OR b` would read `(w AND a) OR b`.
    """
    node = sqlglot.parse_one(sql, dialect=TARGET)
    return f"({sql})" if isinstance(node, exp.Connector) else sql

# -- expression kinds ---------------------------------------------------------
#
# Every VIBEDATA expression is one of two kinds, named in vibedata-schema.json by
# `x-ossie-expression-kind`: a `predicate` (content_filter, metric filter,
# join_expression) or a `metric-formula` (derived_expression). Ossie core's own
# expressions are checked for aggregation: a metric aggregates, a field does not.

PREDICATE = "predicate"
METRIC_FORMULA = "metric-formula"
# Calls that return a boolean without being sqlglot predicates.
_BOOLEAN_CALLS = (exp.Contains, exp.StartsWith, exp.EndsWith)


def _is_aggregate(node):
    import ossie_functions  # beside this module; imported late to keep parse() light
    return ossie_functions.is_aggregate(node)


def _root(node):
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def classify(node):
    """What a parsed expression is at its root: 'predicate', 'aggregate' or 'scalar'."""
    root = _root(node)
    if isinstance(root, (exp.Predicate, exp.Connector, exp.Not, exp.Boolean) + _BOOLEAN_CALLS):
        return "predicate"
    if has_aggregate(node):
        return "aggregate"
    return "scalar"


def has_aggregate(node):
    return any(_is_aggregate(n) for n in node.find_all(exp.Func))


def _forbidden(node):
    """Constructs no VIBEDATA expression may hold, as reasons."""
    reasons = []
    if has_aggregate(node):
        reasons.append("an aggregate function")
    if node.find(exp.Window):
        reasons.append("a window function")
    if node.find(exp.Select, exp.Subquery, exp.Exists):
        reasons.append("a subquery")
    return reasons


def check_kind(text, kind):
    """Why `text` is not an expression of `kind`, or None when it is.

    A predicate is boolean at its root (a comparison, AND/OR/NOT, IS [NOT] NULL, IN
    with a value list, BETWEEN, LIKE/ILIKE, a boolean function or a bare boolean
    column). A metric formula is arithmetic over metric names and scalar functions
    (NULLIF, COALESCE, CASE, CAST and the like) and is not itself a predicate. Neither
    holds an aggregate, a window function or a subquery. Raises ExpressionError when
    `text` does not parse.
    """
    node = parse(text)
    reasons = _forbidden(node)
    boolean = classify(node) == "predicate"
    if kind == PREDICATE and not (boolean or isinstance(_root(node), exp.Column)):
        reasons.insert(0, "is not boolean-valued")
    if kind == METRIC_FORMULA and boolean:
        reasons.insert(0, "is a predicate, not a formula")
    if not reasons:
        return None
    return f"a {kind} " + "; ".join(r if r.startswith("is ") else f"holds {r}"
                                     for r in reasons)
