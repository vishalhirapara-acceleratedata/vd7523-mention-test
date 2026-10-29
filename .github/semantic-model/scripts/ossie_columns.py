"""Check every physical column the model names against dbt's catalog.

`validate` cannot see the marts, so a `column_map` onto a column that does not exist
validates green. dbt's `catalog.json` (written by `dbt docs generate` against a built
warehouse) lists every relation's real columns. This module checks, per document:

- every physical table's `source` is a relation the catalog knows;
- every logical source's `column_map` targets a column of its physical table;
- every aggregate content level's `key_columns` are columns of the aggregate;
- every field expression reads only columns of its dataset's `source`;
- a field that reads exactly one column declares a `datatype` the column's catalog type
  can hold: a different class (text, number, temporal, boolean) is an error, a different
  type within one class (Integer over DECIMAL, DateTime over TIMESTAMPTZ) a warning.

Pure functions: input is parsed documents and the parsed catalog, output diagnostics.
"""

import json

from sqlglot import exp

import ossie_sql

VENDOR = "VIBEDATA"


def _payload(node):
    for ext in node.get("custom_extensions") or []:
        if isinstance(ext, dict) and ext.get("vendor_name") == VENDOR:
            try:
                data = json.loads(ext.get("data") or "{}")
            except json.JSONDecodeError:
                return {}
            return data if isinstance(data, dict) else {}
    return {}


def relations(catalog):
    """{'schema.name': {column, ...}} for every node and source in a dbt catalog."""
    out = {}
    for section in ("nodes", "sources"):
        for entry in (catalog.get(section) or {}).values():
            meta = entry.get("metadata") or {}
            if not meta.get("schema") or not meta.get("name"):
                continue
            columns = {(c.get("name") or key).lower()
                       for key, c in (entry.get("columns") or {}).items()}
            out[f"{meta['schema']}.{meta['name']}".lower()] = columns
    return out


def types(catalog):
    """{'schema.name': {column: catalog type}} for every node and source in a dbt catalog."""
    out = {}
    for section in ("nodes", "sources"):
        for entry in (catalog.get(section) or {}).values():
            meta = entry.get("metadata") or {}
            if not meta.get("schema") or not meta.get("name"):
                continue
            out[f"{meta['schema']}.{meta['name']}".lower()] = {
                (c.get("name") or key).lower(): str(c.get("type") or "")
                for key, c in (entry.get("columns") or {}).items()}
    return out


# Ossie datatype -> (class, type); a catalog type is read into the same pair.
DATATYPES = {
    "String": ("text", "text"), "Integer": ("number", "integer"),
    "Decimal": ("number", "decimal"), "Float": ("number", "float"),
    "Boolean": ("boolean", "boolean"), "Date": ("temporal", "date"),
    "Time": ("temporal", "time"), "DateTime": ("temporal", "timestamp"),
    "DateTimeTz": ("temporal", "timestamptz"),
}


def catalog_type(text):
    """(class, type) for a warehouse type string, or None when it is not recognised."""
    t = text.strip().upper()
    if not t or t.startswith(("STRUCT", "MAP", "ARRAY", "LIST", "UNION", "VARIANT", "OBJECT",
                              "GEOGRAPHY", "GEOMETRY", "POINT")) or "[" in t or "<" in t:
        return None  # a compound or spatial type is not judged
    if "TIME ZONE" in t and "WITHOUT" not in t or t in ("TIMESTAMPTZ", "TIMESTAMP_TZ",
                                                        "TIMESTAMP_LTZ", "DATETIMEOFFSET"):
        return ("temporal", "timestamptz")
    if t.startswith(("TIMESTAMP", "DATETIME")):
        return ("temporal", "timestamp")
    if t == "DATE":
        return ("temporal", "date")
    if t.startswith("TIME"):
        return ("temporal", "time")
    if t.startswith("BOOL") or t == "BIT":
        return ("boolean", "boolean")
    if t.startswith(("DECIMAL", "NUMERIC", "NUMBER")):
        scale = t.rstrip(")").split(",")[-1].strip() if "," in t else None
        return ("number", "integer" if scale == "0" else "decimal")  # NUMBER(38,0) is an integer
    if t.startswith(("DOUBLE", "FLOAT", "REAL")):
        return ("number", "float")
    if "INT" in t and not t.startswith("INTERVAL"):
        return ("number", "integer")
    if t.startswith(("VARCHAR", "CHAR", "TEXT", "STRING", "NVARCHAR", "NCHAR", "CHARACTER",
                     "UUID")):
        return ("text", "text")
    return None


def _error(code, message, where):
    return {"severity": "error", "code": code, "message": message, "file": where}


def check(doc, known, where, column_types=None):
    """Diagnostics for one document `doc` against catalog relations `known`.

    `column_types`, from `types(catalog)`, adds the datatype check; without it only
    existence is checked.
    """
    diags = []
    model = _payload(doc)
    tables = {t["name"]: t for t in model.get("physical_tables") or [] if isinstance(t, dict)}

    def columns_of(source, what):
        key = (source or "").lower()
        if key not in known and len(key.split(".")) > 2:
            key = ".".join(key.split(".")[-2:])  # catalog.schema.table: the catalog keys schema.table
        if key not in known and (not key or any(c.isspace() or c == "(" for c in key)):
            return None  # a query, not a relation: nothing to look up
        if key not in known:
            diags.append(_error("columns/unknown-relation",
                                f"{what}: '{source}' is no relation in the catalog", where))
            return None
        return known[key]

    for name, table in sorted(tables.items()):
        columns_of(table.get("source"), f"physical table '{name}'")
    for dataset in doc.get("datasets") or []:
        ds = dataset.get("name", "<unnamed>")
        source_columns = columns_of(dataset.get("source"), f"dataset '{ds}'")
        for field in dataset.get("fields") or []:
            typed = False
            for dialect in (field.get("expression") or {}).get("dialects") or []:
                if dialect.get("dialect") not in ("ANSI_SQL", "OSSIE_SQL_2026"):
                    continue
                try:
                    read, _ = ossie_sql.names(dialect.get("expression"))
                except ossie_sql.ExpressionError:
                    continue  # validate reports an expression that does not parse
                for column in sorted(c for c in read if "." not in c):
                    if source_columns is not None and column not in source_columns:
                        diags.append(_error("columns/field-unknown-column",
                                            f"field '{ds}.{field.get('name')}' reads "
                                            f"'{column}', which '{dataset.get('source')}' "
                                            "does not have", where))
                column = _bare_column(dialect.get("expression"))
                if column_types is not None and not typed and column \
                        and source_columns is not None and column in source_columns:
                    typed = True
                    diags.extend(_datatype(field, ds, dataset.get("source"), column,
                                           column_types, where))
        for src in _payload(dataset).get("logical_sources") or []:
            table = tables.get(src.get("physical_table"))
            if table is None:
                continue
            what = f"logical source '{ds}.{src.get('name')}'"
            columns = columns_of(table.get("source"), what)
            if columns is None:
                continue
            for logical, physical in sorted((src.get("column_map") or {}).items()):
                if str(physical).lower() not in columns:
                    diags.append(_error("columns/map-unknown-column",
                                        f"{what} maps '{logical}' to '{physical}', which "
                                        f"'{table.get('source')}' does not have", where))
            for ref in src.get("content_levels") or []:
                for column in ref.get("key_columns") or []:
                    if str(column).lower() not in columns:
                        diags.append(_error("columns/key-unknown-column",
                                            f"{what} carries the {ref.get('via')} key in "
                                            f"'{column}', which '{table.get('source')}' does "
                                            "not have", where))
    return diags


def _bare_column(text):
    """The column an expression is, when it is nothing but one unqualified column."""
    try:
        node = ossie_sql.parse(text)
    except ossie_sql.ExpressionError:
        return None
    return node.name.lower() if isinstance(node, exp.Column) and not node.table else None


def _datatype(field, ds, source, column, column_types, where):
    declared = DATATYPES.get(field.get("datatype"))
    raw = (column_types.get((source or "").lower()) or {}).get(column, "")
    actual = catalog_type(raw)
    if declared is None or actual is None or declared == actual:
        return []
    what = (f"field '{ds}.{field.get('name')}' declares {field.get('datatype')}, but "
            f"'{source}.{column}' is {raw}")
    if declared[0] != actual[0]:
        return [_error("columns/datatype-mismatch", what, where)]
    return [{"severity": "warning", "code": "columns/datatype-mismatch",
             "message": what + "; same class, different type", "file": where}]
