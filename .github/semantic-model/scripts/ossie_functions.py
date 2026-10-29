"""The functions Apache Ossie's expression language lists, and the calls outside it.

The catalogue is the REQUIRED and RECOMMENDED functions of Ossie's
`core-spec/expression_language.md` (EXPERIMENTAL ones are left out: a model that uses
one is not portable yet). A call outside it is reported as a warning, never an error:
the spec allows dialect extensions, it only promises portability for these.

Replace this table with upstream's `ossie_sql.compliance_level` when apache/ossie
PR #222 lands, so the list has one owner.
"""

import sqlglot
from sqlglot import exp

# Function names as the spec spells them, by its section.
SPEC_FUNCTIONS = {
    "aggregation": ["SUM", "COUNT", "AVG", "MIN", "MAX", "STDDEV", "STDDEV_POP",
                    "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP", "MEDIAN",
                    "PERCENTILE_CONT", "PERCENTILE_DISC", "APPROX_COUNT_DISTINCT",
                    "APPROX_PERCENTILE"],
    "date_time": ["CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_TIME", "YEAR", "QUARTER",
                  "MONTH", "DAY", "DAYOFYEAR", "HOUR", "MINUTE", "SECOND", "EXTRACT",
                  "DATE_PART", "DATE_TRUNC", "DATEADD", "DATEDIFF", "TO_DATE", "TO_TIMESTAMP"],
    "string": ["CONCAT", "LENGTH", "LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM", "LEFT",
               "RIGHT", "SUBSTRING", "REPLACE", "SPLIT_PART", "POSITION", "CHARINDEX",
               "CONTAINS", "STARTSWITH", "ENDSWITH", "REGEXP_LIKE", "REGEXP_EXTRACT",
               "REGEXP_REPLACE", "REGEXP_COUNT"],
    "math": ["ABS", "ROUND", "FLOOR", "CEIL", "CEILING", "TRUNC", "TRUNCATE", "MOD", "SIGN",
             "POWER", "SQRT", "EXP", "LN", "LOG", "LOG10", "SIN", "COS", "TAN", "ASIN",
             "ACOS", "ATAN", "ATAN2", "RADIANS", "DEGREES", "PI", "GREATEST", "LEAST"],
    "conditional": ["IF", "IFF", "NULLIF", "COALESCE", "IFNULL", "NVL", "NVL2",
                    "ZEROIFNULL", "NULLIFZERO"],
    "window": ["ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "PERCENT_RANK", "CUME_DIST",
               "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE"],
    "conversion": ["CAST", "TRY_CAST"],
}
NAMES = {name for names in SPEC_FUNCTIONS.values() for name in names}

# Constructs the spec lists as syntax rather than functions; sqlglot models them as
# function nodes, so they are never reported.
CONSTRUCTS = (exp.Connector, exp.Case, exp.If, exp.Cast, exp.TryCast, exp.Extract,
              exp.StrPosition, exp.Nullif, exp.Coalesce)

# Spec aggregates sqlglot keeps as anonymous calls rather than an AggFunc class.
ANONYMOUS_AGGREGATES = {"APPROX_PERCENTILE"}


def _classes():
    """The sqlglot node class each spec name parses to, so aliases count as listed."""
    out = set()
    for name in NAMES:
        for args in ("a, b", "a", ""):
            try:
                node = sqlglot.parse_one(f"{name}({args})")
            except sqlglot.errors.SqlglotError:
                continue
            if isinstance(node, exp.Func) and not isinstance(node, exp.Anonymous):
                out.add(type(node))
            break
    return frozenset(out)


LISTED = _classes()


def unlisted(node):
    """Names of the function calls in parsed `node` that the spec does not list, sorted."""
    out = set()
    for call in node.find_all(exp.Func):
        if isinstance(call, CONSTRUCTS):
            continue
        if isinstance(call, exp.Anonymous):
            if call.name.upper() not in NAMES:
                out.add(call.name.upper())
        elif type(call) not in LISTED:
            out.add(call.sql_name())
    return sorted(out)


def is_aggregate(call):
    return isinstance(call, exp.AggFunc) or (
        isinstance(call, exp.Anonymous) and call.name.upper() in ANONYMOUS_AGGREGATES)
