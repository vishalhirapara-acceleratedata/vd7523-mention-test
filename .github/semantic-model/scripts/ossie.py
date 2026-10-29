"""Ossie semantic-model tooling, run from the semantic-model skill against a Domain checkout.

Commands: `validate` (models, ontologies and mappings: schema, then the checks that
kind allows), `coverage` (dbt models on disk vs `dbt_model` names), `suppliers`,
`notes`, `generate-tests`, `compile`, `columns`, `bindings` (aggregate bindings vs
the compiled mart SQL), `dimensionality`, `refs` (mapping
reference integrity and the identity lint), `glossary` (ontology concepts vs the
glossary's terms), `version`. Nothing here writes consumer formats, and nothing judges
meaning: the validating skills review that.

Usage:
  python3 ossie.py validate <file-or-dir>
  python3 ossie.py coverage <model-file-or-dir> <dbt-models-dir>
  python3 ossie.py suppliers <model-file-or-dir>
  python3 ossie.py notes <model-file-or-dir> <tests-dir> <adr-dir>
  python3 ossie.py generate-tests <model-dir> <dbt-tests-dir> [--check]
  python3 ossie.py compile <model-file> <query-file>
  python3 ossie.py columns <model-file-or-dir> <dbt-catalog.json>
  python3 ossie.py bindings <model-file-or-dir> <dbt-manifest.json> [--dialect duckdb]
  python3 ossie.py dimensionality <model-file-or-dir>
  python3 ossie.py refs <ontology-dir> <model-dir>
  python3 ossie.py glossary <ontology-dir> <context-md>
  python3 ossie.py version

JSON goes to stdout (agent- and CI-parseable), human prose to stderr. Every
command exits 0 when it finds nothing to fix (warnings allowed) and 1 otherwise,
including when it cannot read its input or a check crashes.
"""

import functools
import importlib.util
import json
import re
import sys
from pathlib import Path

import typer

sys.path.insert(0, str(Path(__file__).parent))

from ossie_bindings import check as check_bindings  # noqa: E402
from ossie_bindings import model_sql  # noqa: E402
from ossie_columns import check as check_columns  # noqa: E402
from ossie_columns import relations as catalog_relations  # noqa: E402
from ossie_columns import types as catalog_types  # noqa: E402
from ossie_compile import CompileError, Model, Query, compile_query  # noqa: E402
from ossie_content import TEST_FILE as CONTENT_TEST  # noqa: E402
from ossie_content import generate as generate_content_tests  # noqa: E402
from ossie_levels import generate as generate_level_tests  # noqa: E402
from ossie_ontology import (  # noqa: E402
    Model as MappedModel,
    Ontology,
    declaration_warnings,
    glossary_drift,
    identity_checks,
    ontology_checks,
    reference_checks,
    unmapped_identity_checks,
)
from ossie_vibedata import (  # noqa: E402
    _names_in,
    metric_dimensionality,
    physical_table_index,
    validate_vibedata,
)

TOOLS_DIR = Path(__file__).parent
VENDOR_DIR = TOOLS_DIR / "ossie_vendor"

_spec = importlib.util.spec_from_file_location(
    "ossie_vendor_validate", VENDOR_DIR / "validation" / "validate.py")
_vendor = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_vendor)

app = typer.Typer(no_args_is_help=True)


def _fail(diag):
    """Print one error diagnostic as the command's JSON and exit 1."""
    print(json.dumps([diag], indent=2))
    raise typer.Exit(1)


def _guarded(command):
    """Keep stdout JSON and the exit code 1 when a check itself crashes."""
    @functools.wraps(command)
    def run(*args, **kwargs):
        try:
            return command(*args, **kwargs)
        except (typer.Exit, typer.Abort):
            raise
        except Exception as e:  # noqa: BLE001 - report, never traceback
            _fail({"severity": "error", "code": "cli/check-crashed",
                   "message": f"{command.__name__} crashed: {type(e).__name__}: {e}"})
    return run


MODEL_SUFFIX = ".ossie.yaml"
ONTOLOGY_SUFFIX = ".ontology.yaml"
MAPPING_SUFFIX = ".mapping.yaml"
SUFFIXES = (MODEL_SUFFIX, ONTOLOGY_SUFFIX, MAPPING_SUFFIX)


def _resolve_targets(model: str) -> list[Path]:
    """Model documents only: the commands that read datasets and metrics."""
    p = Path(model)
    if p.is_dir():
        return sorted(p.glob(f"*{MODEL_SUFFIX}"))
    return [p]


def _canonical_targets(path: str) -> list[Path]:
    """Every canonical document under a path: models, ontologies and mappings."""
    p = Path(path)
    if p.is_dir():
        return sorted(item for suffix in SUFFIXES for item in p.glob(f"*{suffix}"))
    return [p]


def _kind(doc) -> str:
    """A canonical document's kind, from the key that only it carries."""
    if isinstance(doc, dict) and "concept_mappings" in doc:
        return "mapping"
    if isinstance(doc, dict) and "ontology" in doc:
        return "ontology"
    return "model"


UNCODED_CODES = (("does not cover the primary key or a unique key",
                  "relationship/to-column-not-a-key"),)


def _as_diag(entry) -> dict:
    if isinstance(entry, _vendor.ValidationWarning):
        message = str(entry)
        code = next((c for needle, c in UNCODED_CODES if needle in message), None)
        diag = {"severity": "warning", "message": message}
        if code:
            diag["code"] = code
        return diag
    if isinstance(entry, dict):
        return entry
    return {"severity": "error", "message": str(entry)}


UPSTREAM_PIN = "c5cb931d72029dc523a5dd18d267f083403786f2"
UPSTREAM_DATE = "2026-10-14"
SPEC_VERSION = "0.2.0.dev0"
TOOL_VERSION = "0.1.0"


@app.command()
def version():
    """Pin report: tool, spec, and upstream validator commit, as JSON."""
    # Also forces Typer group mode so `validate` keeps its subcommand name
    # when `export` lands as a second command.
    print(json.dumps({"tool": TOOL_VERSION, "spec": SPEC_VERSION,
                      "upstream_commit": UPSTREAM_PIN, "upstream_date": UPSTREAM_DATE}))


def _read_text(target: Path):
    """Read UTF-8 text, returning (text, diag); never prints (stdout is JSON)."""
    try:
        return target.read_text(encoding="utf-8"), None
    except OSError as e:
        return None, {"severity": "error", "code": "cli/unreadable",
                      "message": f"Could not read '{target}': {e}"}
    except UnicodeDecodeError as e:
        return None, {"severity": "error", "code": "cli/unreadable",
                      "message": f"'{target}' is not valid UTF-8 text: {e}"}


def _parse(target: Path):
    """Read and parse one model file, returning (doc, diag); never raises, never prints."""
    text, diag = _read_text(target)
    if diag:
        diag["file"] = str(target)
        return None, diag
    try:
        doc = _vendor.load_yaml_named(text, str(target))
    except Exception as e:  # noqa: BLE001 - report, never traceback
        return None, {"severity": "error", "code": "cli/bad-yaml", "file": str(target),
                      "message": f"'{target}': invalid YAML: {e}"}
    if not isinstance(doc, dict):
        return None, _not_a_document(target, doc)
    return doc, None


def _not_a_document(target, doc):
    return {"severity": "error", "code": "cli/not-a-document", "file": str(target),
            "message": f"'{target}' is a {type(doc).__name__}, not a document; a canonical "
                       f"document is a mapping with a version, a name and its own content key"}


def _require_dir(path: Path, role: str):
    """A gate on a path that is not a directory fails; it never reports zero findings."""
    if not path.is_dir():
        _fail({"severity": "error", "code": "cli/no-files",
               "message": f"{role} '{path}' is not a directory; a mistyped path is an "
                          f"error, not an empty result"})


def _load_kind(directory: Path, suffix: str):
    """Every document of one kind in DIRECTORY as (path, doc), and their diagnostics."""
    docs, diags = [], []
    for path in sorted(directory.glob(f"*{suffix}")):
        doc, diag = _parse(path)
        if diag:
            diags.append(diag)
        else:
            docs.append((path, doc))
    return docs, diags


def _load_docs(model):
    """Every parsed document under MODEL, or the first unreadable one's diagnostic and exit 1."""
    targets = _resolve_targets(model)
    if not targets:
        _fail({"severity": "error", "code": "cli/no-files",
               "message": f"No *.ossie.yaml under '{model}'"})
    docs = []
    for target in targets:
        doc, diag = _parse(target)
        if diag:
            _fail(diag)
        if _kind(doc) != "model":
            _fail({"severity": "error", "code": "cli/not-a-model", "file": str(target),
                   "message": f"'{target}' is an {_kind(doc)} document; this command reads "
                              f"model documents (*{MODEL_SUFFIX}) only"})
        docs.append((target, doc))
    return docs


SCHEMA_PATHS = {
    "model": ("core-spec", "ossie-schema.json"),
    "ontology": ("ontology", "ontology.json"),
    "mapping": ("ontology", "mapping.json"),
}


def _load_schema(kind: str = "model"):
    """Load and metaschema-check a vendored schema; (schema, diag)."""
    path = VENDOR_DIR.joinpath(*SCHEMA_PATHS[kind])
    text, diag = _read_text(path)
    if diag:
        return None, diag
    try:
        schema = json.loads(text)
    except json.JSONDecodeError as e:
        return None, {"severity": "error", "code": "cli/bad-schema",
                      "message": f"Invalid JSON in schema {path}: {e}"}
    try:
        _vendor.Draft202012Validator.check_schema(schema)
    except _vendor.SchemaError as e:
        return None, {"severity": "error", "code": "cli/bad-schema",
                      "message": f"Invalid schema {path}: {e.message}"}
    return schema, None


def _dataset_fingerprint(dataset, alias_names=frozenset()):
    """Identity of a dataset that must match wherever the same name appears.

    Role logical sources point at ALIAS handles and are local to a document whose
    facts play that role, so they are excluded; everything else about a shared
    dimension must match, including its SCD shape and hierarchies.
    """
    fields = {}
    for field in dataset.get("fields") or []:
        if isinstance(field, dict) and field.get("name"):
            fields[field["name"]] = json.dumps(
                [field.get("expression"), field.get("datatype"), field.get("dimension")],
                sort_keys=True)
    payload = {}
    for data in (dataset.get("custom_extensions") or []):
        if isinstance(data, dict) and data.get("vendor_name") == "VIBEDATA":
            try:
                payload.update(json.loads(data.get("data") or "{}"))
            except json.JSONDecodeError:
                pass
    sources = {}
    for source in payload.get("logical_sources") or []:
        if not isinstance(source, dict):
            continue
        if source.get("physical_table") in alias_names:
            continue
        sources[source.get("name")] = json.dumps(
            [source.get("physical_table"), source.get("priority_group"),
             source.get("content_grain"), source.get("column_map"),
             source.get("content_levels"), source.get("content_filter"),
             source.get("navigable")],
            sort_keys=True)
    return {"source": dataset.get("source"),
            "primary_key": dataset.get("primary_key"),
            "unique_keys": dataset.get("unique_keys"),
            "fields": fields,
            "scd": payload.get("scd"),
            "levels": payload.get("levels"),
            "row_level": payload.get("row_level"),
            "hierarchies": payload.get("hierarchies"),
            "logical_sources": sources}


def _vibedata_of(node):
    """The decoded VIBEDATA payload on a node, version stripped; {} when absent or bad."""
    payload = {}
    for ext in node.get("custom_extensions") or []:
        if isinstance(ext, dict) and ext.get("vendor_name") == "VIBEDATA":
            try:
                decoded = json.loads(ext.get("data") or "{}")
            except json.JSONDecodeError:
                continue
            if isinstance(decoded, dict):
                payload.update(decoded)
    payload.pop("vibedata_version", None)
    return payload


def _supplied_through_inputs(metric, supplied):
    """A derived, time-series or dimension-rule metric is supplied when every input it reads is."""
    payload = _vibedata_of(metric)
    for key in ("time_series", "based_on_dimensions"):
        if isinstance(payload.get(key), dict):
            return payload[key].get("of") in supplied
    formula = payload.get("derived_expression")
    if isinstance(formula, str):
        try:
            names, _ = _names_in(formula)
        except ValueError:
            return False
        return bool(names) and names <= supplied
    return False


def cross_document_drift(docs):
    """Same name, different definition across documents: duplication that drifted.

    Shared dimensions are copied per star (relationships cannot cross documents),
    so a copy that disagrees with its twin is a silent second definition.
    """
    diags = []
    seen = {}
    for target, doc in docs:
        if not isinstance(doc, dict):
            continue
        aliases = frozenset(
            name for name, table in physical_table_index(doc).items()
            if table.get("table_type") == "ALIAS")
        for dataset in doc.get("datasets") or []:
            if isinstance(dataset, dict) and dataset.get("name"):
                seen.setdefault(("dataset", dataset["name"]), []).append(
                    (target, _dataset_fingerprint(dataset, aliases)))
        for metric in doc.get("metrics") or []:
            if isinstance(metric, dict) and metric.get("name"):
                seen.setdefault(("metric", metric["name"]), []).append(
                    (target, json.dumps([metric.get("expression"), _vibedata_of(metric)],
                                        sort_keys=True)))
        for name, table in physical_table_index(doc).items():
            seen.setdefault(("physical_table", name), []).append((target, table))
    for (kind, name), entries in seen.items():
        if len(entries) < 2:
            continue
        first_target, first = entries[0]
        for target, value in entries[1:]:
            if value != first:
                diags.append({
                    "severity": "error", "code": "crossdoc/diverged",
                    "message": f"{kind} '{name}' is defined differently in '{first_target}' "
                               f"and '{target}'; copies of a shared {kind} must match",
                    "file": target})
    return diags


@app.command()
@_guarded
def coverage(model: str, models_root: str):
    """Every dbt model under MODELS_ROOT must be named by a dbt_model; report both gaps."""
    docs = _load_docs(model)
    if not Path(models_root).is_dir():
        _fail({"severity": "error", "code": "coverage/no-models-dir",
               "message": f"'{models_root}' is not a directory"})
    named = set()
    for _, doc in docs:
        for name, table in physical_table_index(doc).items():
            if table.get("dbt_model"):
                named.add(table["dbt_model"])
    on_disk = {path.stem for path in Path(models_root).rglob("*.sql")}
    diags = [{"severity": "error", "code": "coverage/unmapped",
              "message": f"dbt model '{name}' is in the lakehouse but no dbt_model "
                         f"names it; register it or record why in a note"}
             for name in sorted(on_disk - named)]
    diags += [{"severity": "error", "code": "coverage/phantom",
               "message": f"dbt_model '{name}' names no file under {models_root}"}
              for name in sorted(named - on_disk)]
    print(json.dumps(diags, indent=2))
    errors = [d for d in diags if d["severity"] == "error"]
    typer.echo(f"{len(on_disk)} model(s) on disk, {len(named)} named, "
               f"{len(errors)} coverage error(s).", err=True)
    raise typer.Exit(1 if errors else 0)


@app.command()
@_guarded
def suppliers(model: str):
    """Declared fields and metrics that no logical source supplies (content instrument)."""
    diags = []
    for target, doc in _load_docs(model):
        supplied = set()
        for dataset in doc.get("datasets") or []:
            if not isinstance(dataset, dict):
                continue
            for source in _sources_of(dataset):
                column_map = source.get("column_map")
                if isinstance(column_map, dict):
                    supplied.update(column_map.keys())
            for field in dataset.get("fields") or []:
                if isinstance(field, dict) and field.get("name") not in supplied:
                    diags.append({"severity": "finding", "code": "suppliers/field",
                                  "file": str(target),
                                  "message": f"{dataset.get('name')}.{field['name']} is "
                                             "supplied by no logical source"})
        metrics = [m for m in doc.get("metrics") or [] if isinstance(m, dict)]
        grew = True
        while grew:  # inputs may themselves be derived
            grew = False
            for metric in metrics:
                if (metric.get("name") not in supplied
                        and _supplied_through_inputs(metric, supplied)):
                    supplied.add(metric["name"])
                    grew = True
        for metric in metrics:
            if metric.get("name") not in supplied:
                diags.append({"severity": "finding", "code": "suppliers/metric",
                              "file": str(target),
                              "message": f"metric '{metric['name']}' is supplied by no "
                                         "logical source"})
    print(json.dumps(diags, indent=2))
    typer.echo(f"{len(diags)} unsupplied item(s).", err=True)
    raise typer.Exit(1 if diags else 0)


POINTER = re.compile(r"held by\s+([A-Za-z_][A-Za-z0-9_]*)")
ADR_REF = re.compile(r"ADR (\d{4})")


@app.command()
@_guarded
def notes(model: str, tests: str, adr: str):
    """Every pointer a note carries must resolve: `held by <test>` and `ADR NNNN`."""
    diags = []
    tests_dir = Path(tests)
    adr_dir = Path(adr)
    for path, role in ((tests_dir, "tests"), (adr_dir, "adr")):
        if not path.is_dir():
            diags.append({"severity": "warning", "code": "notes/no-directory",
                          "message": f"{role} directory '{path}' does not exist, so no pointer "
                                     f"into it can be checked; a clean run here proves nothing"})
    for target, doc in _load_docs(model):
        for dataset in doc.get("datasets") or []:
            for source in _sources_of(dataset):
                note = source.get("note")
                if not isinstance(note, str) or not note:
                    continue
                for name in POINTER.findall(note):
                    if not (tests_dir / f"{name}.sql").exists():
                        diags.append({"severity": "error", "code": "notes/dangling-test",
                                      "file": str(target),
                                      "message": f"{dataset.get('name')}.{source.get('name')} "
                                                 f"points at test '{name}', which is not "
                                                 f"{tests_dir}/{name}.sql"})
                for number in ADR_REF.findall(note):
                    if not list(adr_dir.glob(f"{number}-*.md")):
                        diags.append({"severity": "error", "code": "notes/dangling-adr",
                                      "file": str(target),
                                      "message": f"{dataset.get('name')}.{source.get('name')} "
                                                 f"cites ADR {number}, which has no file in "
                                                 f"{adr_dir}"})
    print(json.dumps(diags, indent=2))
    errors = [d for d in diags if d["severity"] == "error"]
    typer.echo(f"{len(errors)} dangling pointer(s), "
               f"{len(diags) - len(errors)} unchecked director(ies).", err=True)
    raise typer.Exit(1 if errors else 0)


@app.command("generate-tests")
@_guarded
def generate_tests(model: str, tests_dir: str,
                   check: bool = typer.Option(False, "--check",
                                              help="Exit 1 when a file in TESTS_DIR is not what "
                                                   "MODEL generates")):
    """Write every generated dbt test: aggregate content, level keys (error), levels (warn)."""
    docs = [doc for _, doc in _load_docs(model)]
    try:
        files = {CONTENT_TEST: generate_content_tests(docs), **generate_level_tests(docs)}
    except CompileError as e:
        _fail({"severity": "error", "code": "generate-tests/compile", "message": str(e)})
    directory = Path(tests_dir)
    if not directory.is_dir():
        _fail({"severity": "error", "code": "generate-tests/no-tests-dir",
               "message": f"'{tests_dir}' is not a directory"})
    stale = []
    for name, sql in sorted(files.items()):
        path = directory / name
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current == sql:
            continue
        if check:
            stale.append({"severity": "error", "code": "generate-tests/stale", "file": str(path),
                          "message": f"'{path}' is not what the model generates; run "
                                     "generate-tests without --check"})
        else:
            path.write_text(sql, encoding="utf-8")
            typer.echo(f"wrote {path}", err=True)
    print(json.dumps(stale, indent=2))
    typer.echo(f"{len(files)} generated test(s), {len(stale)} stale.", err=True)
    raise typer.Exit(1 if stale else 0)


@app.command("columns")
@_guarded
def columns(model: str, catalog: str):
    """Every physical column the model names exists in dbt's catalog.json, with a compatible type."""
    path = Path(catalog)
    if not path.exists():
        print(json.dumps([{"severity": "error", "code": "columns/no-catalog",
                           "message": f"'{catalog}' does not exist; run `dbt docs generate` "
                                      "against a built warehouse first"}]))
        raise typer.Exit(1)
    parsed = json.loads(path.read_text())
    known = catalog_relations(parsed)
    column_types = catalog_types(parsed)
    diags = []
    for target, doc in _load_docs(model):
        diags.extend(check_columns(doc, known, str(target), column_types))
    print(json.dumps(diags, indent=2))
    errors = [d for d in diags if d["severity"] == "error"]
    typer.echo(f"{len(known)} relation(s) in the catalog, {len(errors)} error(s), "
               f"{len(diags) - len(errors)} warning(s).", err=True)
    raise typer.Exit(1 if errors else 0)


@app.command("bindings")
@_guarded
def bindings(model: str, manifest: str, dialect: str = "duckdb"):
    """Each aggregate binding agrees with the compiled SQL that builds the aggregate."""
    path = Path(manifest)
    if not path.exists():
        print(json.dumps([{"severity": "error", "code": "bindings/no-manifest",
                           "message": f"'{manifest}' does not exist; run `dbt compile` (or "
                                      "`dbt parse` for models with no Jinja beyond ref) first"}]))
        raise typer.Exit(1)
    sql = model_sql(json.loads(path.read_text()))
    diags = []
    for target, doc in _load_docs(model):
        diags.extend(check_bindings(doc, sql, str(target), dialect))
    print(json.dumps(diags, indent=2))
    errors = [d for d in diags if d["severity"] == "error"]
    typer.echo(f"{len(sql)} model(s) with SQL, {len(errors)} error(s), "
               f"{len(diags) - len(errors)} warning(s). Static: the content test checks the "
               "same bindings on data.", err=True)
    raise typer.Exit(1 if errors else 0)


@app.command("compile")
@_guarded
def compile_command(model: str, query_file: str):
    """Compile a semantic query (YAML or JSON) against one model document; print JSON."""
    docs = [doc for _, doc in _load_docs(model)]
    if len(docs) != 1:
        print(json.dumps([{"severity": "error", "code": "compile/one-document",
                           "message": "compile reads exactly one model document"}]))
        raise typer.Exit(1)
    query = Query.from_dict(_vendor.load_yaml_named(Path(query_file).read_text(), query_file))
    try:
        result = compile_query(Model(docs[0], lambda t: t["source"]), query)
    except CompileError as e:
        print(json.dumps([{"severity": "error", "code": "compile/error", "message": str(e)}]))
        raise typer.Exit(1) from e
    print(json.dumps({"source": result.source, "columns": result.columns,
                      "rejected": result.rejected, "sql": result.sql}, indent=2))


def _sources_of(dataset):
    if not isinstance(dataset, dict):
        return
    for ext in dataset.get("custom_extensions") or []:
        if not isinstance(ext, dict) or ext.get("vendor_name") != "VIBEDATA":
            continue
        try:
            payload = json.loads(ext.get("data") or "{}")
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        for source in payload.get("logical_sources") or []:
            if isinstance(source, dict):
                yield source


@app.command()
@_guarded
def dimensionality(model: str):
    """Per metric: its facts and the dimensions a query may group or filter it by.

    Engines and exporters read this to reject a query that groups a metric by a
    dimension it cannot reach (unrelated_dimensions: error) or to repeat its total there.
    """
    out = {}
    for target, doc in _load_docs(model):
        out[doc.get("name", str(target))] = metric_dimensionality(doc)
    print(json.dumps(out, indent=2))


@app.command()
@_guarded
def refs(ontology: str, model: str):
    """Every mapping ref resolves and identifies each member by its natural key, completely."""
    ontology_dir, model_dir = Path(ontology), Path(model)
    _require_dir(ontology_dir, "ontology")
    _require_dir(model_dir, "model")
    ontology_docs, diags = _load_kind(ontology_dir, ONTOLOGY_SUFFIX)
    model_docs, model_diags = _load_kind(model_dir, MODEL_SUFFIX)
    mapping_docs, mapping_diags = _load_kind(ontology_dir, MAPPING_SUFFIX)
    diags += model_diags + mapping_diags
    ontologies = {doc.get("name"): Ontology(doc) for _, doc in ontology_docs}
    models = {doc.get("name"): (path, doc) for path, doc in model_docs}

    mapped = set()
    for path, doc in mapping_docs:
        file = str(path)
        model_name = (doc.get("semantic_model_ref") or {}).get("name")
        ontology_name = (doc.get("ontology_ref") or {}).get("name")
        if model_name in models:
            mapped.add(model_name)
            model_index = MappedModel(models[model_name][1])
        else:
            model_index = None
            diags.append({"severity": "error", "code": "mapping/unresolved-ref", "file": file,
                          "message": f"semantic_model_ref '{model_name}' matches no document "
                                     f"name in {model_dir}"})
        ontology_index = ontologies.get(ontology_name)
        if ontology_index is None:
            diags.append({"severity": "error", "code": "mapping/unresolved-ref", "file": file,
                          "message": f"ontology_ref '{ontology_name}' matches no document "
                                     f"name in {ontology_dir}"})
        diags += reference_checks(doc, ontology_index, model_index, ontology_name, model_name,
                                  file)
        if ontology_index and model_index:
            diags += identity_checks(doc, ontology_index, model_index, file)
            diags += unmapped_identity_checks(doc, ontology_index, model_index, file)

    for name in sorted(set(models) - mapped):
        diags.append({"severity": "warning", "code": "mapping/none",
                      "file": str(models[name][0]),
                      "message": f"model '{name}' has no mapping; each document is mapped or "
                                 f"its description says why no concept reaches it"})
    print(json.dumps(diags, indent=2))
    errors = [d for d in diags if d["severity"] == "error"]
    typer.echo(f"{len(mapping_docs)} mapping(s), {len(errors)} error(s), "
               f"{len(diags) - len(errors)} warning(s). Declaration-only: no expression is "
               f"executed.", err=True)
    raise typer.Exit(1 if errors else 0)


@app.command()
@_guarded
def glossary(ontology: str, context: str):
    """Concepts vs the glossary's `**Term**:` headings; an entity type with no term fails."""
    text, diag = _read_text(Path(context))
    if diag:
        _fail(diag)
    _require_dir(Path(ontology), "ontology")
    docs, diags = _load_kind(Path(ontology), ONTOLOGY_SUFFIX)
    if diags:
        _fail(diags[0])
    out = glossary_drift([doc for _, doc in docs], text)
    print(json.dumps(out, indent=2))
    missing = out["entity_types_without_term"]
    typer.echo(f"{len(missing)} entity type(s) without a term (a finding), "
               f"{len(out['terms_without_concept'])} term(s) without a concept and "
               f"{len(out['value_types_without_term'])} value type(s) without a term "
               f"(reported for review).", err=True)
    raise typer.Exit(1 if missing else 0)


@app.command()
@_guarded
def validate(model: str):
    """Validate canonical file(s): the schema the document's shape picks, then its checks."""
    targets = _canonical_targets(model)
    if not targets:
        _fail({"severity": "error", "code": "cli/no-files",
               "message": f"No {' / '.join('*' + s for s in SUFFIXES)} under '{model}'"})
    schemas = {}
    diags = []
    parsed = []
    for target in targets:
        text, read_diag = _read_text(target)
        if read_diag:
            read_diag["file"] = str(target)
            diags.append(read_diag)
            continue
        try:
            data = _vendor.load_yaml_named(text, str(target))
        except Exception as e:  # noqa: BLE001 - report, don't crash
            diags.append({"severity": "error", "code": "cli/bad-yaml", "file": str(target),
                          "message": f"'{target}': invalid YAML: {e}"})
            continue
        if not isinstance(data, dict):
            diags.append(_not_a_document(target, data))
            continue
        kind = _kind(data)
        if kind not in schemas:
            schemas[kind] = _load_schema(kind)
        schema, schema_diag = schemas[kind]
        if schema_diag:
            _fail(schema_diag)
        try:
            file_diags = [_as_diag(e) for e in _vendor.validate_document(data, schema)]
            if not any(d["severity"] == "error" for d in file_diags):
                if kind == "model":
                    file_diags.extend(validate_vibedata(data))
                elif kind == "ontology":
                    file_diags.extend(ontology_checks(data))
                else:
                    file_diags.extend(declaration_warnings(data))
        except Exception as e:  # noqa: BLE001 - a check bug must not kill JSON output
            file_diags = [{"severity": "error", "code": "cli/check-crashed",
                           "message": f"'{target}': validator crashed: {type(e).__name__}: {e}"}]
        for d in file_diags:
            d.setdefault("file", str(target))
        diags.extend(file_diags)
        if kind == "model":
            parsed.append((str(target), data))
    if len(parsed) > 1:
        diags.extend(cross_document_drift(parsed))
    if Path(model).is_dir():
        for path in sorted(Path(model).glob("*.y*ml")):
            if not path.name.endswith(SUFFIXES):
                diags.append({"severity": "error", "code": "cli/unmatched-file",
                              "file": str(path),
                              "message": f"'{path}' is not one of "
                                         f"{', '.join('*' + s for s in SUFFIXES)}; "
                                         f"rename it or move it out of the directory"})
    print(json.dumps(diags, indent=2))
    errors = [d for d in diags if d["severity"] == "error"]
    warnings = [d for d in diags if d["severity"] == "warning"]
    typer.echo(f"{len(targets)} file(s), {len(errors)} error(s), "
               f"{len(warnings)} warning(s). Structural check only — content review is the "
               f"validating skills' call.", err=True)
    raise typer.Exit(1 if errors else 0)


if __name__ == "__main__":
    app()
