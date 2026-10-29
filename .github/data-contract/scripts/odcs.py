"""ODCS data-contract tooling, run from the data-contract skill against a Domain checkout.

Commands: `check` (schema and structure of the committed contracts, one contract per
boundary relation, and their agreement with the canonical model that names each gold
relation), `check --relations` (adds the comparison against the built relation), `reverse`
(draft the boundary's contracts from a built warehouse, with the questions neither the
warehouse nor the model can settle), `generate` (write the dbt keys and dlt hints from the
contracts), `diff` (classify each changed contract against a git base and require the version
bump the change needs), `profile` (test each contract's key, `required` and value-set claims
against the built relation), `results` (map a dbt run's test results to the contract
assertions they enforce), `version`.

The boundary is where a contract is owed: every relation a dlt source loads, and every
relation a gold layer builds. A `contract_layers` list in `repo-map.json` replaces that
default with the layers it names. A contract outside the boundary is checked like any other;
it is never required.

This script proves shape and reports facts. It never judges meaning: the grain, whether a
`required` is honest, and whether a quality rule encodes the real business answer are the
`validating-data-contract` review, not a pass/fail here.

Every location resolves from `repo-map.json`: a layer's `path` for the dbt models it builds,
the seeds it holds, and a source's `docs` file for the resources it lands. The warehouse is
consulted only by `check --relations`, `reverse` and `profile`, which read a local database
file (`--catalog`, else the sandbox the Intent's platform names) and refuse rather than skip
when none resolves. `check --relations` also reads dbt's `catalog.json`.

Usage (from the Domain's repository root):
  python3 odcs.py check [--root PATH] [--relations] [--catalog PATH]
  python3 odcs.py reverse [--root PATH] [--catalog PATH] [--force]
  python3 odcs.py generate [--root PATH]
  python3 odcs.py diff <base-ref> [--root PATH]
  python3 odcs.py profile [--root PATH] [--catalog PATH] [--relation SCHEMA.TABLE ...]
  python3 odcs.py results <run_results.json> [--manifest PATH] [--root PATH]
  python3 odcs.py version

JSON diagnostics go to stdout (agent- and CI-parseable): a list, except `results`, which
prints one JSON record per line. Exit 0 when no errors (warnings allowed), 1 otherwise,
including when an input cannot be read or a check crashes.
"""

import ast
import collections
import functools
import json
import re
import subprocess
import sys
import uuid
from pathlib import Path

import jsonschema
import typer
import yaml

APP_DIR = Path(__file__).resolve().parent
VENDOR_DIR = APP_DIR / "odcs_vendor"
SCHEMA_FILE = VENDOR_DIR / "odcs-json-schema-v3.2.0.json"
SPEC_VERSION = "v3.2.0"
UPSTREAM_COMMIT = "8b551607aad6801979bf4c209385e78fc4d029e5"

# The plugin's own scripts, where the platform modules and the Intent-context reader live.
PLUGIN_SCRIPTS = APP_DIR.parents[2] / "scripts"

# Sections that assert something. A present-but-empty one claims a promise nobody made.
ROOT_SECTIONS = ("servers", "authoritativeDefinitions", "tags")

# Ossie `DataType` reduced to the ODCS `logicalType` it states; `Opaque` states none.
LOGICAL_TYPES = {
    "String": "string",
    "Integer": "integer",
    "Decimal": "number",
    "Float": "number",
    "Boolean": "boolean",
    "Date": "date",
    "Time": "time",
    "DateTime": "timestamp",
    "DateTimeTz": "timestamp",
    "Opaque": None,
}

# dbt reads a model's properties under `models:` and a seed's under `seeds:`.
PROPERTIES_SECTION = {"model": "models", "seed": "seeds"}

# Relations the installed loader library writes into every dlt schema. No dlt resource declares
# them, so none is on the boundary, a contract on one is no orphan, and nothing is generated
# from it.
AUDIT_RELATIONS = ("audit_runs", "audit_table_loads")

# A local database column type reduced to the ODCS `logicalType` that states the same value.
PHYSICAL_LOGICAL = {
    "VARCHAR": "string",
    "CHAR": "string",
    "TEXT": "string",
    "STRING": "string",
    "UUID": "string",
    "JSON": "string",
    "BIGINT": "integer",
    "INTEGER": "integer",
    "SMALLINT": "integer",
    "TINYINT": "integer",
    "HUGEINT": "integer",
    "UBIGINT": "integer",
    "UINTEGER": "integer",
    "USMALLINT": "integer",
    "UTINYINT": "integer",
    "DECIMAL": "number",
    "DOUBLE": "number",
    "FLOAT": "number",
    "REAL": "number",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIME": "time",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamp",
    "TIMESTAMP_NS": "timestamp",
    "TIMESTAMP_MS": "timestamp",
    "TIMESTAMP_S": "timestamp",
}

# A dbt generic test as the ODCS library rule it asserts. A `relationships` test is not a rule:
# it becomes the property's `relationships[]` entry instead.
RULE_METRICS = {"not_null": "nullValues", "unique": "duplicateValues",
                "accepted_values": "invalidValues"}

# A dbt `relationships` test names its target as a rendered reference.
REF_REFERENCE = re.compile(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)")
SOURCE_REFERENCE = re.compile(r"source\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)")

# What `reverse` could not settle, queued for the review instead of guessed. A finding is a
# warning: the tool reports the doubt and the run still succeeds.
NO_GRAIN = "finding/no-grain"
UNDESCRIBED = "finding/undescribed"
UNPROVEN_NULLABLE = "finding/unproven-nullable"
KEY_REPEATS = "finding/key-repeats"
OPEN_ENUM = "finding/open-enum"
UNDECLARED_FOREIGN_KEY = "finding/undeclared-foreign-key"

# A name that reads as the relation's own key, and one that reads as a reference to another
# relation's. The shape is the evidence; only a declaration or the data can settle it.
KEY_SHAPED = re.compile(r"_(key|natural_key)$")
REFERENCE_SHAPED = re.compile(r"_(key|id)$")

# At most this many distinct values is the shape of an enum; above it a column reads as free
# text, a name or a code. A judgement, stated here so a review can argue with the number.
ENUM_MAX_DISTINCT = 12

# Files that carry sources, unit tests or semantics; a generated key has no home in them.
RESERVED_PROPERTIES = ("_sources.yml", "_unit_tests.yml", "_semantic.yml")

# A local database physical type reduced to the dlt column type that lands the same value.
DLT_TYPES = {
    "VARCHAR": "text",
    "DOUBLE": "double",
    "DECIMAL": "decimal",
    "BIGINT": "bigint",
    "INTEGER": "bigint",
    "HUGEINT": "bigint",
    "BOOLEAN": "bool",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamp",
}

# The quality metrics that reduce to a generic dbt test, and the test each becomes.
QUALITY_TESTS = {"duplicateValues": "unique", "nullValues": "not_null"}

app = typer.Typer(add_completion=False)


def _diag(severity: str, code: str, message: str, file: object = None) -> dict:
    diag = {"severity": severity, "code": code, "message": message}
    if file is not None:
        diag["file"] = str(file)
    return diag


class _Refused(Exception):
    """A contract the tool will not write from, carrying the diagnostic that says why."""

    def __init__(self, diag: dict):
        super().__init__(diag["message"])
        self.diag = diag


class _IndentedDumper(yaml.SafeDumper):
    """PyYAML puts a block sequence flush with its key; a Domain's YAML indents it."""

    def increase_indent(self, flow: bool = False, indentless: bool = False):
        return super().increase_indent(flow, False)


def _fail(diag: dict):
    """Print one error diagnostic as the command's JSON and exit 1."""
    print(json.dumps([diag]))
    raise typer.Exit(1)


def _guarded(command):
    """Keep stdout JSON and the exit code 1 when a check itself crashes."""
    @functools.wraps(command)
    def run(*args, **kwargs):
        try:
            return command(*args, **kwargs)
        except (typer.Exit, typer.Abort):
            raise
        except _Refused as refusal:
            _fail(refusal.diag)
        except Exception as e:  # noqa: BLE001 - report, never traceback
            _fail(_diag("error", "cli/check-crashed",
                        f"{command.__name__} crashed: {type(e).__name__}: {e}"))
    return run


def _repo_root(root: str) -> Path:
    """The Domain root: `--root`, else the nearest directory above the working one with a map."""
    start = Path(root).resolve() if root else Path.cwd().resolve()
    for candidate in [start, *start.parents]:
        if (candidate / "repo-map.json").is_file():
            return candidate
    raise _Refused(_diag("error", "map/missing",
                         f"no repo-map.json at or above {start}; run from the Domain's root"))


def _repo_map(root: Path) -> dict:
    return json.loads((root / "repo-map.json").read_text(encoding="utf-8"))


def _declared(root: Path, raw: str) -> Path:
    """The path an artifact pattern names, up to its first placeholder."""
    prefix = raw.split("{")[0].split("*")[0].rstrip("/")
    return root / prefix


def _schema() -> dict:
    """The vendored schema. Never fetched: a validation run is offline by construction."""
    return json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))


def _one_per_defect(diags: list[dict]) -> list[dict]:
    """Collapse the same defect reported twice.

    Draft 2020-12 reports one unknown key as both `additionalProperties` and
    `unevaluatedProperties`. The location and the offending names are the defect; the
    validator that noticed it is not.
    """
    seen = set()
    unique = []
    for diag in diags:
        where = diag["message"].split(": ", 1)[0] + diag["message"].split(": ")[1]
        names = tuple(sorted(set(re.findall(r"'([^']+)'", diag["message"]))))
        key = (diag["code"], where, names)
        if key not in seen:
            seen.add(key)
            unique.append(diag)
    return unique


def _schema_diagnostics(doc: object, path: Path) -> list[dict]:
    """Validate one document against the vendored schema."""
    validator = jsonschema.Draft202012Validator(_schema())
    errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path))
    diags = [
        _diag(
            "error",
            "schema/invalid",
            f"{path}: {'.'.join(str(p) for p in error.absolute_path) or '<root>'}: {error.message}",
        )
        for error in errors
    ]
    return _one_per_defect(diags)


def declared_relations(root: Path, repo_map: dict) -> dict[tuple[str, str], str]:
    """Every relation the map declares, as `(schema, table)` mapped to what declares it.

    dbt models and seeds come from the layer paths. Raw relations come from each source's
    resource docs. The loader's audit relations are written by the installed library and appear
    in none of these: a contract on one is no orphan, and none is owed.
    """
    expected: dict[tuple[str, str], str] = {}
    for name, layer in (repo_map.get("layers") or {}).items():
        path = root / layer["path"] if layer.get("path") else None
        if path is None or not path.is_dir() or layer.get("built_by") != "dbt":
            continue
        # A dbt layer builds whatever it holds: a model from a `.sql`, a seed from a `.csv`.
        # Which suffix belongs to which is the file's fact, not the layer's name.
        for suffix in (".sql", ".csv"):
            for file in sorted(path.rglob(f"*{suffix}")):
                expected[(layer["schema"], file.stem)] = f"layer {name}"
    for name, source in (repo_map.get("sources") or {}).items():
        docs = source.get("docs")
        if not docs:
            continue
        entries = yaml.safe_load((root / docs).read_text(encoding="utf-8")) or {}
        for resource in sorted(entries.get("resources") or {}):
            expected[(source["dataset"], resource)] = f"source {name}"
    return expected


def _unknown_contract_layers(repo_map: dict) -> list[dict]:
    """A `contract_layers` entry that names no layer scopes nothing; saying so is the report."""
    named = repo_map.get("contract_layers")
    if named is None:
        return []
    if not isinstance(named, list):
        return [_diag("error", "map/contract-layers",
                      "repo-map.json `contract_layers` is not a list of layer names")]
    layers = repo_map.get("layers") or {}
    return [
        _diag("error", "map/contract-layers",
              f"repo-map.json `contract_layers` names `{name}`, which `layers` does not declare")
        for name in named
        if name not in layers
    ]


def boundary_relations(repo_map: dict,
                       declared: dict[tuple[str, str], str]) -> dict[tuple[str, str], str]:
    """The relations a contract is owed for: the inputs and outputs of the Domain.

    By default, every relation a dlt source loads (a consumer of the load sees its shape first)
    and every relation a gold layer builds (what a consumer outside the repo reads). Staging,
    intermediate and seed layers are guarded by their dbt tests and owe none. `contract_layers`,
    when the map declares it, replaces that default with the layers it names, a dlt layer
    contributing the resources its sources land in that schema.
    """
    layers = repo_map.get("layers") or {}
    named = repo_map.get("contract_layers")
    if isinstance(named, list):
        schemas = {layers[name]["schema"] for name in named if name in layers}
        return {relation: by for relation, by in declared.items() if relation[0] in schemas}
    gold = {layers[name]["schema"] for name in repo_map.get("gold_layers") or []
            if name in layers}
    return {
        relation: by
        for relation, by in declared.items()
        if by.startswith("source ") or (relation[0] in gold and by.startswith("layer "))
    }


def _boundary_rank(relation: tuple[str, str], repo_map: dict,
                   declared: dict[tuple[str, str], str]) -> int:
    """Gold first, then inputs, then anything else: the order a reviewer triages in."""
    layers = repo_map.get("layers") or {}
    gold = {layers[name]["schema"] for name in repo_map.get("gold_layers") or []
            if name in layers}
    if relation[0] in gold:
        return 0
    if (declared.get(relation) or "").startswith("source "):
        return 1
    return 2


def _contract_relations(root: Path, repo_map: dict) -> dict[tuple[str, str], Path]:
    """Every committed contract, keyed by the relation its path names."""
    return _contracts_under(_declared(root, repo_map["artifact_paths"]["data_contract"]))


CONTRACT_SUFFIX = ".odcs.yaml"


def _relation_of(parts: tuple[str, ...]) -> tuple[str, str] | None:
    """The relation a `{schema}/{table}.odcs.yaml` path names; a dotted table is its own name."""
    if len(parts) != 2 or not parts[1].endswith(CONTRACT_SUFFIX):
        return None
    return parts[0], parts[1][:-len(CONTRACT_SUFFIX)]


def _contracts_under(base: Path) -> dict[tuple[str, str], Path]:
    """Every `{schema}/{table}.odcs.yaml` under `base`, keyed by the relation its path names."""
    found: dict[tuple[str, str], Path] = {}
    if not base.is_dir():
        return found
    for path in sorted(base.rglob(f"*{CONTRACT_SUFFIX}")):
        relation = _relation_of(path.relative_to(base).parts)
        if relation is not None:
            found[relation] = path
    return found


def _misplaced(base: Path, root: Path) -> list[dict]:
    """A contract file off the `{schema}/{table}` layout names no relation, so nothing checks it."""
    if not base.is_dir():
        return []
    return [_diag("warning", "contract/misplaced",
                  f"{_display(path, root)}: not at <contracts>/<schema>/<table>{CONTRACT_SUFFIX}, "
                  f"so no check reads it; move it or remove it", _display(path, root))
            for path in sorted(base.rglob(f"*{CONTRACT_SUFFIX}"))
            if _relation_of(path.relative_to(base).parts) is None]


def _committed_contracts(root: Path, repo_map: dict) -> dict[tuple[str, str], Path]:
    """Every committed contract, or a refusal when the map declares no place to keep them."""
    if not (repo_map.get("artifact_paths") or {}).get("data_contract"):
        raise _Refused(_diag(
            "error", "contract/unconfigured",
            "repo-map.json declares no artifact_paths.data_contract, so no contract can be found",
        ))
    return _contract_relations(root, repo_map)


def _empty_sections(doc: dict, path: Path) -> list[dict]:
    diags = []
    for section in ROOT_SECTIONS:
        if section in doc and not doc[section]:
            diags.append(_diag("error", "contract/empty-section",
                               f"{path}: `{section}` is present and empty; omit it instead"))
    for element in doc.get("schema") or []:
        if isinstance(element, dict) and "quality" in element and not element["quality"]:
            diags.append(_diag("error", "contract/empty-section",
                               f"{path}: `schema[].quality` is present and empty; omit it instead"))
    return diags


def _prose(text: object) -> str | None:
    """One flowing line, or nothing when there is no prose to state.

    `{{ doc("...") }}` is a pointer dbt resolves at parse time, not a description; a contract
    that copied it would carry syntax no reader or tool outside dbt can turn into an answer.
    """
    value = " ".join(str(text).split()) if text is not None else ""
    if not value or "{{" in value:
        return None
    return value


def _grain(doc: dict, path: Path) -> list[dict]:
    """A warning, not an error: an onboarding run may leave the grain to review."""
    elements = [e for e in (doc.get("schema") or []) if isinstance(e, dict)]
    stated = [e for e in elements if _prose(e.get("dataGranularityDescription"))]
    if stated:
        return []
    return [_diag("warning", "contract/no-grain",
                  f"{path}: no `dataGranularityDescription`; the review refuses a contract "
                  f"a shipped intent presents as complete without one")]


def _canonical_datasets(root: Path, repo_map: dict) -> dict[tuple[str, str], tuple[dict, Path]]:
    """Every dataset the canonical model declares, keyed by the relation its `source` names.

    Each value keeps the document it was read from, so a contract can name its authority and
    the two readings cannot disagree. Empty when the repo has adopted no canonical model: a
    contract set is not broken for lacking something it never promised to agree with.
    """
    raw = (repo_map.get("artifact_paths") or {}).get("logical_model")
    if not raw:
        return {}
    base = _declared(root, raw)
    datasets: dict[tuple[str, str], tuple[dict, Path]] = {}
    if not base.is_dir():
        return datasets
    for path in sorted(base.glob("*.ossie.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for dataset in doc.get("datasets") or []:
            if not isinstance(dataset, dict):
                continue
            source = str(dataset.get("source") or "").split(".", 1)
            if len(source) == 2:
                datasets[(source[0], source[1])] = (dataset, path)
    return datasets


def _gold_schemas(repo_map: dict, expected: dict[tuple[str, str], str]) -> set[str]:
    """Schemas holding gold relations: the layers the map declares gold, where they are built.

    The map is the only source. Another Domain names its layers differently, so a layer name is
    something this tool cannot know; guessing one — by exclusion, or by the shape of the
    directory — would gate relations the Domain never called gold and pass over the ones it did.
    An undeclared set is therefore empty and names nothing, and `check` warns about the gap
    instead of inventing a gold set to fill it.
    """
    layers = repo_map.get("layers") or {}
    built = {schema for schema, _ in expected}
    return {
        layers[name]["schema"]
        for name in repo_map.get("gold_layers") or []
        if name in layers and layers[name]["schema"] in built
    }


def _properties(contract: dict) -> dict[str, dict]:
    """The contract's properties by name, across every schema element."""
    props: dict[str, dict] = {}
    for element in contract.get("schema") or []:
        if isinstance(element, dict):
            for prop in element.get("properties") or []:
                if isinstance(prop, dict) and prop.get("name"):
                    props[prop["name"]] = prop
    return props


def _property_diagnostics(where: str, prop: dict, field: dict, primary: bool) -> list[dict]:
    """One contract property against the model field the document declares for it.

    Only the model's own declarations are compared: it is curated, so a property it never
    names is the contract's business.
    """
    diags = []
    datatype = field.get("datatype")
    if datatype in LOGICAL_TYPES:
        want = LOGICAL_TYPES[datatype]
        got = prop.get("logicalType")
        if got != want:
            diags.append(_diag("error", "gold/divergent",
                               f"{where}: logicalType is {got!r}, the canonical model's "
                               f"{datatype} is {want!r}"))
        zone = (prop.get("logicalTypeOptions") or {}).get("timezone")
        if datatype == "DateTimeTz" and not zone:
            diags.append(_diag("error", "gold/divergent",
                               f"{where}: `{datatype}` needs logicalTypeOptions.timezone true"))
        elif datatype == "DateTime" and zone:
            diags.append(_diag("error", "gold/divergent",
                               f"{where}: `{datatype}` must not carry logicalTypeOptions.timezone"))
    if primary and prop.get("primaryKey") is not True:
        diags.append(_diag("error", "gold/divergent",
                           f"{where}: the canonical model declares this primary key and the "
                           f"property does not set primaryKey true"))
    return diags


def _loader_columns(relation: tuple[str, str], contract: dict, repo_map: dict) -> list[dict]:
    """A raw relation's contract declares no `_dlt_*` column: dlt adds those to the table.

    A dbt model is the exception and the reason for the distinction: its enforced contract must
    declare every output column, so a model that selects the loader's column keeps it.
    """
    if _is_dbt(relation, repo_map):
        return []
    return [
        _diag("error", "contract/loader-column",
              f"{relation[0]}.{relation[1]}: `{name}` is a column the loader adds to a "
              f"dlt-loaded relation, not one the relation yields")
        for name in sorted(_properties(contract))
        if name.startswith("_dlt")
    ]


def _gold_agreement(relation: tuple[str, str], path: Path, contract: dict,
                    documents: dict[tuple[str, str], tuple[dict, Path]],
                    gold: set[str]) -> list[dict]:
    """A gold contract against the canonical document that names its relation."""
    label = f"{relation[0]}.{relation[1]}"
    declared_document = documents.get(relation)
    if declared_document is None:
        if relation[0] not in gold:
            return []
        return [_diag("error", "gold/undeclared",
                      f"{path}: no canonical document names the gold relation {label}")]
    dataset = declared_document[0]
    declared = {
        field["name"]: field
        for field in dataset.get("fields") or []
        if isinstance(field, dict) and field.get("name")
    }
    keys = set(dataset.get("primary_key") or [])
    props = _properties(contract)
    diags = []
    for name, field in declared.items():
        prop = props.get(name)
        if prop is None:
            diags.append(_diag("error", "gold/omits-declared",
                               f"{label}: the contract omits `{name}`, which the canonical "
                               f"model declares"))
        else:
            diags.extend(_property_diagnostics(f"{label}.{name}", prop, field, name in keys))
    diags.extend(_declared_relationships(relation, path, dataset, props, documents,
                                        declared_document[1]))
    diags.extend(_declared_unique_keys(label, dataset))
    return diags


def _declared_relationships(relation: tuple[str, str], path: Path, dataset: dict,
                            props: dict[str, dict],
                            documents: dict[tuple[str, str], tuple[dict, Path]],
                            document_path: Path) -> list[dict]:
    """Every relationship the document declares FROM this dataset, against the contract.

    The document states the identity graph; a contract that drops a relationship publishes a
    shape a consumer cannot join on, and nothing else compares the two.
    """
    targets = {
        str(doc.get("name")): str(doc.get("source")).split(".", 1)[-1]
        for doc, _path in documents.values()
        if doc.get("name") and doc.get("source")
    }
    document = yaml.safe_load(document_path.read_text(encoding="utf-8")) or {}
    diags = []
    for relationship in document.get("relationships") or []:
        if relationship.get("from") != dataset.get("name"):
            continue
        target = targets.get(str(relationship.get("to")))
        columns = relationship.get("to_columns") or []
        if target is None or not columns:
            continue
        for column in relationship.get("from_columns") or []:
            prop = props.get(column)
            if prop is None:
                continue
            declared = [
                str(entry.get("to") or "").split(".")
                for entry in prop.get("relationships") or []
                if isinstance(entry, dict)
            ]
            if not any(len(parts) >= 2 and parts[-1] == columns[0] and parts[-2] == target
                       for parts in declared):
                diags.append(_diag(
                    # A warning: carrying the edge publishes a shape and adds a dbt test, so
                    # authoring it is a deliberate change, not a gate fix. Reported, never silent.
                    "warning", "gold/missing-relationship",
                    f"{relation[0]}.{relation[1]}.{column}: the canonical model declares "
                    f"`{relationship.get('name')}` to `{target}.{columns[0]}` and the contract "
                    f"declares no such relationship",
                ))
    return diags


def _declared_unique_keys(label: str, dataset: dict) -> list[dict]:
    """A composite unique key is a promise ODCS cannot state: report it, never drop it.

    A warning, because the contract has no field for a multi-column key: `unique` is per
    property. Saying nothing would lose the key; failing would demand an inexpressible fix.
    """
    diags = []
    for key in dataset.get("unique_keys") or []:
        if not isinstance(key, list) or len(key) < 2:
            continue
        diags.append(_diag(
            "warning", "gold/composite-key",
            f"{label}: the canonical model declares the composite unique key "
            f"({', '.join(str(k) for k in key)}); ODCS states uniqueness per property, so the "
            f"contract cannot carry it and a reader needs the document",
        ))
    return diags


def _layer_for(repo_map: dict, schema: str) -> dict | None:
    """The layer that declares a schema; the map is the only place a schema is named."""
    for layer in (repo_map.get("layers") or {}).values():
        if layer.get("schema") == schema:
            return layer
    return None


def _is_dbt(relation: tuple[str, str], repo_map: dict) -> bool:
    """Whether a dbt layer builds the relation, as opposed to a dlt pipeline or the loader."""
    layer = _layer_for(repo_map, relation[0])
    return layer is not None and layer.get("built_by") == "dbt"


def _is_loader_column(relation: tuple[str, str], name: str, repo_map: dict) -> bool:
    """A `_dlt_*` column dlt adds to a relation it loaded.

    A dbt model that selects the column propagates it deliberately, and its enforced contract
    must declare every output column, so only a raw relation excludes the loader's own.
    """
    return name.startswith("_dlt") and not _is_dbt(relation, repo_map)


def _is_audit(relation: tuple[str, str], repo_map: dict) -> bool:
    """A relation the installed loader writes into a dlt layer's schema, not a dlt resource."""
    layer = _layer_for(repo_map, relation[0])
    return (
        layer is not None
        and layer.get("built_by") == "dlt"
        and relation[1] in AUDIT_RELATIONS
    )


def _hints_module(root: Path, repo_map: dict) -> Path:
    """`contracts.py` under the ingestion root the map declares."""
    raw = (repo_map.get("artifact_paths") or {}).get("ingestion_root")
    if not raw:
        raise _Refused(_diag("error", "contract/unconfigured",
                             "repo-map.json declares no artifact_paths.ingestion_root, so no "
                             "dlt hints module can be placed"))
    return _declared(root, raw) / "contracts.py"


def _properties_file(root: Path, layer: dict, table: str, section: str) -> Path:
    """The layer file that already names this relation, or the one this tool creates."""
    directory = root / layer["path"]
    if directory.is_dir():
        for path in sorted(directory.glob("*.yml")) + sorted(directory.glob("*.yaml")):
            if path.name in RESERVED_PROPERTIES:
                continue
            try:
                doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            except (yaml.YAMLError, OSError):
                continue
            if _document_entry(doc, section, table) is not None:
                return path
    return directory / "_generated.yml"


def _document_entry(doc: dict, section: str, table: str) -> dict | None:
    """The properties entry named `table` under `section`."""
    for entry in doc.get(section) or []:
        if isinstance(entry, dict) and entry.get("name") == table:
            return entry
    return None


def _target(root: Path, repo_map: dict, relation: tuple[str, str]) -> tuple[str, Path] | None:
    """What kind of relation this is and which generated artifact its keys belong to."""
    if _is_audit(relation, repo_map):
        return None
    layer = _layer_for(repo_map, relation[0])
    if layer is None:
        return None
    if layer.get("built_by") == "dlt":
        return "dlt", _hints_module(root, repo_map)
    kind = "seed" if (root / layer["path"] / f"{relation[1]}.csv").is_file() else "model"
    return kind, _properties_file(root, layer, relation[1], PROPERTIES_SECTION[kind])


def _dlt_type(physical: str) -> str | None:
    """A local database type reduced to the dlt column type that lands the same value."""
    return DLT_TYPES.get(physical.split("(", 1)[0].strip().upper())


def _relationship_test(where: str, to: str) -> dict:
    """One declared property relationship as the dbt test that enforces it.

    dbt resolves a target by `ref()`, which names a model rather than a schema-qualified
    relation, so `core.dim_customer.customer_key` and `dim_customer.customer_key` both name
    `dim_customer`.
    """
    relation, _, field = str(to).rpartition(".")
    table = relation.rpartition(".")[2]
    if not table or not field:
        raise _Refused(_diag("error", "contract/unmapped-relationship",
                             f"{where}: `to: {to}` names no `<relation>.<column>`"))
    return {"relationships": {"arguments": {"to": f"ref('{table}')", "field": field}}}


def _sole_key(contract: dict) -> str | None:
    """The property that is the whole primary key, or None for none or a composite key.

    A composite key has no generic per-column test; `profile` checks it against the data.
    """
    keys = [name for name, prop in _properties(contract).items() if prop.get("primaryKey") is True]
    return keys[0] if len(keys) == 1 else None


def _data_tests(prop: dict, where: str, sole_key: bool = False) -> list:
    """The generic dbt tests a property's claims reduce to: `required` (not_null), a
    single-column `primaryKey` (unique), its quality rules and its relationships."""
    tests: list = []
    if prop.get("required") is True:
        tests.append("not_null")
    if sole_key:
        tests.append("unique")
    for rule in prop.get("quality") or []:
        if not isinstance(rule, dict) or rule.get("type") != "library":
            continue
        if rule.get("mustBe") != 0:
            continue
        metric = rule.get("metric")
        if metric in QUALITY_TESTS:
            if QUALITY_TESTS[metric] not in tests:
                tests.append(QUALITY_TESTS[metric])
        elif metric == "invalidValues":
            values = (rule.get("arguments") or {}).get("valid_values")
            if values and not any(isinstance(test, dict) for test in tests):
                tests.append({"accepted_values": {"arguments": {"values": values}}})
    for relationship in prop.get("relationships") or []:
        declared = relationship if isinstance(relationship, dict) else {}
        tests.append(_relationship_test(where, declared.get("to")))
    return tests


def _owned(relation: tuple[str, str], contract: dict, kind: str) -> dict:
    """The keys `generate` owns for one contract, and nothing else.

    A model carries the enforced contract, a `data_type` per property and the `data_tests` its
    rules reduce to; a seed carries only the generated tests, since dbt's contract is
    model-only; a dlt table carries its hints. A key the contract cannot ground is a refusal,
    never a guess.
    """
    for server in contract.get("servers") or []:
        stated = (server or {}).get("schema") if isinstance(server, dict) else None
        if stated and stated != relation[0]:
            raise _Refused(_diag("error", "servers/mismatch",
                                 f"{relation[0]}.{relation[1]}: `servers[].schema` says "
                                 f"{stated!r}, the map declares {relation[0]!r}"))
    owned: dict = {"columns": {}}
    sole = _sole_key(contract)
    if kind == "model":
        owned["contract"] = {"enforced": True}
    for name, prop in _properties(contract).items():
        where = f"{relation[0]}.{relation[1]}.{name}"
        keys: dict = {}
        if kind == "seed":
            tests = _data_tests(prop, where, name == sole)
            if not tests:
                continue
            keys["data_tests"] = tests
        else:
            physical = str(prop.get("physicalType") or "").strip()
            if kind == "dlt":
                mapped = _dlt_type(physical) if physical else None
                if mapped is None:
                    raise _Refused(_diag("error", "contract/unmapped-type",
                                         f"{where}: `{physical or 'no physicalType'}` has no dlt "
                                         f"column type"))
                keys["data_type"] = mapped
                keys["nullable"] = not prop.get("required", False)
            elif physical:
                keys["data_type"] = physical
            else:
                raise _Refused(_diag("error", "contract/unmapped-type",
                                     f"{where}: no `physicalType`, so no dbt `data_type`"))
            if kind == "model":
                tests = _data_tests(prop, where, name == sole)
                if tests:
                    keys["data_tests"] = tests
        owned["columns"][name] = keys
    return owned


ITEM = re.compile(r"^(\s*)- name:\s*(.+?)\s*$")
KEY = re.compile(r"^(\s*)([A-Za-z_][\w-]*):\s*(.*)$")


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _block_end(lines: list[str], index: int, indent: int) -> int:
    """The line after the block that the `- name:` line at `indent` opens: a sibling ends it."""
    for other in range(index + 1, len(lines)):
        text = lines[other].strip()
        if text and not text.startswith("#") and _indent(lines[other]) <= indent:
            return other
    return len(lines)


def _key_block_end(lines: list[str], index: int, indent: int) -> int:
    """The line after the value of the key at `indent`.

    A block sequence may sit at the same indent as the key that owns it, so a `- ` line there
    is the value; the next mapping key is what ends it.
    """
    for other in range(index + 1, len(lines)):
        text = lines[other].strip()
        if not text or text.startswith("#"):
            continue
        if _indent(lines[other]) < indent:
            return other
        if _indent(lines[other]) == indent and not text.startswith("-"):
            return other
    return len(lines)


def _content_end(lines: list[str], start: int, end: int) -> int:
    """The span without its trailing blank lines."""
    while end > start and not lines[end - 1].strip():
        end -= 1
    return end


def _section(lines: list[str], key: str) -> tuple[int, int] | None:
    """The line span that a top-level `key:` opens."""
    for index, line in enumerate(lines):
        match = KEY.match(line)
        if match and _indent(line) == 0 and match.group(2) == key and not match.group(3):
            return index + 1, _key_block_end(lines, index, 0)
    return None


def _entry(lines: list[str], name: str, lo: int, hi: int) -> tuple[int, int] | None:
    """The `- name: <name>` item inside lines[lo:hi]."""
    for index in range(lo, hi):
        match = ITEM.match(lines[index])
        if match and _scalar(match.group(2)) == name:
            return index, _block_end(lines, index, len(match.group(1)))
    return None


def _scalar(text: str):
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        return text


def _key_indent(lines: list[str], start: int, end: int) -> int:
    """The indent of the mapping keys belonging to the item that starts at `start`."""
    for index in range(start + 1, end):
        if lines[index].strip() and not lines[index].lstrip().startswith("#"):
            return _indent(lines[index])
    return _indent(lines[start]) + 2


def _key_span(lines: list[str], start: int, end: int, key: str) -> tuple[int, int] | None:
    """(line, span end) of `key:` among the item's own keys."""
    indent = _key_indent(lines, start, end)
    for index in range(start + 1, end):
        match = KEY.match(lines[index])
        if match and _indent(lines[index]) == indent and match.group(2) == key:
            return index, _key_block_end(lines, index, indent)
    return None


def _put(lines: list[str], start: int, end: int, key: str, value: str,
         before: str = "") -> list[str]:
    """Set `key: value` on the item, appended last or ahead of `before`; sibling keys keep order."""
    span = _key_span(lines, start, end, key)
    rendered = f"{' ' * _key_indent(lines, start, end)}{key}: {value}"
    if span is not None:
        return [*lines[:span[0]], rendered, *lines[_content_end(lines, *span):]]
    at = _content_end(lines, start, end)
    anchor = _key_span(lines, start, end, before) if before else None
    at = anchor[0] if anchor else at
    return [*lines[:at], rendered, *lines[at:]]


def _column_lines(name: str, keys: dict, indent: int) -> list[str]:
    """A `columns[]` entry at `indent`, with its keys one level in."""
    pad = " " * indent
    return [f"{pad}- name: {name}",
            *(f"{pad}  {key}: {value}" for key, value in keys.items())]


def _first_item_indent(lines: list[str], lo: int, hi: int, default: int) -> int:
    for index in range(lo, hi):
        match = ITEM.match(lines[index])
        if match:
            return len(match.group(1))
    return default + 2


def _set_column(lines: list[str], start: int, end: int, column: str, key: str,
                value: str) -> list[str]:
    """Put `key: value` on a column entry, adding the entry when the model has none."""
    span = _key_span(lines, start, end, "columns")
    if span is None:
        at = _content_end(lines, start, end)
        indent = _key_indent(lines, start, end)
        return [*lines[:at], f"{' ' * indent}columns:",
                *_column_lines(column, {key: value}, indent + 2), *lines[at:]]
    index, columns_end = span
    item = _entry(lines, column, index + 1, columns_end)
    if item is not None:
        return _put(lines, item[0], item[1], key, value)
    at = _content_end(lines, index + 1, columns_end)
    indent = _first_item_indent(lines, index + 1, columns_end, _indent(lines[index]))
    return [*lines[:at], *_column_lines(column, {key: value}, indent), *lines[at:]]


def _entry_lines(table: str, owned: dict, indent: int) -> list[str]:
    """A whole properties entry, for a file that has none for this relation."""
    pad = " " * indent
    lines = [f"{pad}- name: {table}"]
    if "contract" in owned:
        lines.append(f"{pad}  contract: {_flow(owned['contract'])}")
    if owned["columns"]:
        lines.append(f"{pad}  columns:")
        for column, keys in owned["columns"].items():
            lines.extend(_column_lines(column, {k: _flow(v) for k, v in keys.items()},
                                       indent + 4))
    return lines


def _flow(value) -> str:
    """One line of YAML: a scalar, or a collection in flow style."""
    return yaml.safe_dump(value, default_flow_style=True, width=1000).removesuffix(
        "\n...\n").strip()


def _drop_renamed_tests(lines: list[str], start: int, end: int, column: str,
                        keys: dict) -> list[str]:
    """Drop a column's `tests` when `data_tests` now states exactly the same rules.

    dbt refuses both keys on one column, so writing the same list again is a rename rather than
    a second declaration. A list that differs is left alone: the refusal is then dbt's to make.
    The whole block is parsed, so a block sequence and a flow list compare as the rules they are.
    """
    if "data_tests" not in keys:
        return lines
    block = _key_span(lines, start, end, "columns")
    if block is None:
        return lines
    item = _entry(lines, column, block[0] + 1, block[1])
    if item is None:
        return lines
    span = _key_span(lines, item[0], item[1], "tests")
    if span is None:
        return lines
    block_end = _content_end(lines, *span)
    declared = yaml.safe_load("\n".join(lines[span[0]:block_end]))
    if not isinstance(declared, dict) or declared.get("tests") != keys["data_tests"]:
        return lines
    return [*lines[:span[0]], *lines[block_end:]]


def _apply(text: str, section: str, table: str, owned: dict) -> str:
    """The properties file with this relation's owned keys written in; everything else intact."""
    lines = text.splitlines()
    span = _section(lines, section)
    if span is None:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(f"{section}:")
        span = (len(lines), len(lines))
    found = _entry(lines, table, *span)
    if found is None:
        at = _content_end(lines, *span)
        lines[at:at] = _entry_lines(table, owned, _first_item_indent(lines, *span, 0))
        return _joined(lines)
    # Re-found after every insertion: the file is small and the line numbers move.
    start, end = found
    for column, keys in owned["columns"].items():
        for key, value in keys.items():
            lines = _set_column(lines, start, end, column, key, _flow(value))
            start, end = _entry(lines, table, *_section(lines, section))
        lines = _drop_renamed_tests(lines, start, end, column, keys)
        start, end = _entry(lines, table, *_section(lines, section))
    if "contract" in owned:
        lines = _put(lines, start, end, "contract", _flow(owned["contract"]), before="columns")
    return _joined(lines)


def _joined(lines: list[str]) -> str:
    return "\n".join(lines).rstrip("\n") + "\n"


def _base(path: Path) -> str:
    """A properties file's text; one to be created starts as a bare dbt properties document."""
    return path.read_text(encoding="utf-8") if path.is_file() else "version: 2\n"


def _module_text(tables: dict[str, dict[str, dict]]) -> str:
    """The dlt hints module, written deterministically so a re-run is byte-identical."""
    lines = [
        '"""dlt column hints, generated from the data contracts.',
        "",
        "Do not edit: `generate` owns this file and `check` fails when it is stale.",
        '"""',
        "",
        "COLUMNS: dict[str, dict[str, dict]] = {",
    ]
    for table in sorted(tables):
        lines.append(f'    "{table}": {{')
        for column, hint in tables[table].items():
            kind = hint["data_type"]
            nullable = hint["nullable"]
            lines.append(f'        "{column}": {{"data_type": "{kind}", '
                         f'"nullable": {nullable}}},')
        lines.append("    },")
    lines.append("}")
    return "\n".join(lines) + "\n"


def _columns_module(path: Path) -> dict | None:
    """The generated module's COLUMNS, read without importing it; None when it does not parse."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, ValueError):
        return None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            target = node.targets[0]
        elif isinstance(node, ast.AnnAssign):
            target = node.target
        else:
            continue
        if getattr(target, "id", "") == "COLUMNS" and node.value is not None:
            return ast.literal_eval(node.value)
    return {}


def _display(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _stale(path: Path, root: Path, key: str) -> dict:
    return _diag(
        "error", "generated/stale",
        f"{_display(path, root)}: {key} is stale; run generate. If the contract no longer "
        f"produces this key at all, remove the key instead — generate only writes the keys a "
        f"contract yields and never deletes one, so a withdrawn rule or a dropped property is "
        f"the one case it cannot clear",
    )


# The column keys `generate` owns per kind: what it writes, plus `tests`, the legacy spelling it
# renames to `data_tests`. A floor, not a lookup of what this contract happened to yield: a key
# `generate` writes for the kind is reportable even where this contract states no instance.
OWNED_COLUMN_KEYS = {"model": ("data_type", "data_tests", "tests"),
                     "seed": ("data_tests", "tests")}


def _stale_keys(found: dict, owned: dict, kind: str, table: str) -> list[str]:
    """Owned column keys on disk that `_owned` does not produce here.

    The vocabulary is the kind's capability set union `_owned`'s own output for this contract, so
    a key the contract does not state is still reportable: `generate` never deletes a key it
    cannot account for, and a leftover must be visible rather than disappear with the vocabulary.
    A legacy `tests` list is always a leftover, because `generate` renames every one it can
    restate to `data_tests`, and dbt refuses a column carrying both.
    """
    stale = [f"{table}.contract"] if "contract" in found and "contract" not in owned else []
    column_keys = sorted(
        set(OWNED_COLUMN_KEYS.get(kind, ()))
        | {key for keys in owned["columns"].values() for key in keys}
    )
    for column in found.get("columns") or []:
        if not isinstance(column, dict):
            continue
        declared = owned["columns"].get(column.get("name"), {})
        stale.extend(f"{table}.columns.{column.get('name')}.{key}"
                     for key in column_keys if key in column and key not in declared)
    return stale


def _stale_diagnostics(root: Path, repo_map: dict, contracts: dict,
                       docs: dict) -> list[dict]:
    """What `generate` would write, against what is on disk: one computation, no second copy.

    A contract whose generated artifact or entry is absent is stale: `generate` never having
    run is exactly the drift this check exists to catch.
    """
    targets: dict[Path, list[tuple[tuple[str, str], dict, str]]] = {}
    diags: list[dict] = []
    for relation, _ in sorted(contracts.items()):
        doc = docs.get(relation)
        if doc is None:
            continue
        try:
            target = _target(root, repo_map, relation)
        except _Refused as refusal:
            diags.append(refusal.diag)
            continue
        if target is not None:
            targets.setdefault(target[1], []).append((relation, doc, target[0]))

    hints: dict[str, dict[str, dict]] = {}
    module: Path | None = None
    on_disk_module: dict = {}
    for path, entries in sorted(targets.items()):
        if entries[0][2] == "dlt":
            module = path
            on_disk_module = _columns_module(path) if path.is_file() else {}
        else:
            doc = yaml.safe_load(path.read_text(encoding="utf-8")) if path.is_file() else {}
        for relation, contract, kind in entries:
            try:
                owned = _owned(relation, contract, kind)
            except _Refused as refusal:
                diags.append(refusal.diag)
                continue
            if kind == "dlt":
                hints[relation[1]] = owned["columns"]
                continue
            table, section = relation[1], PROPERTIES_SECTION[kind]
            found = _document_entry(doc, section, table)
            if found is None:
                diags.append(_stale(path, root, f"{section}.{table}"))
                continue
            if "contract" in owned and found.get("contract") != owned["contract"]:
                diags.append(_stale(path, root, f"{table}.contract"))
            for key in _stale_keys(found, owned, kind, table):
                diags.append(_stale(path, root, key))
            columns = {column.get("name"): column
                       for column in found.get("columns") or [] if isinstance(column, dict)}
            for column, keys in owned["columns"].items():
                for key, value in keys.items():
                    if (columns.get(column) or {}).get(key) != value:
                        diags.append(_stale(path, root, f"{table}.columns.{column}.{key}"))

    if hints and module is not None:
        if not module.is_file() or on_disk_module is None:
            diags.append(_stale(module, root, "COLUMNS"))
        else:
            for table in sorted(set(hints) | set(on_disk_module)):
                want, got = hints.get(table) or {}, on_disk_module.get(table) or {}
                for column in sorted(set(want) | set(got)):
                    if want.get(column) != got.get(column):
                        diags.append(_stale(module, root, f'COLUMNS["{table}"]["{column}"]'))
    return diags


def _server_types() -> list[str]:
    """The `servers[].type` values the vendored standard names."""
    return list(_schema()["$defs"]["Server"]["properties"]["type"]["enum"])


def _server_type(platform: str) -> str:
    """The standard's server type for a platform: its own name, or the engine its name leads with.

    A platform the standard has no type for is `custom`, the type the standard keeps for a
    technology it does not name.
    """
    types = _server_types()
    if platform in types:
        return platform
    engine = platform.split("_", 1)[0]
    return engine if engine in types else "custom"


def _platform(repo_map: dict) -> str:
    """The platform the map declares. One Domain runs on one, so the map is the only source."""
    return str(repo_map.get("data_platform") or "")


def _platform_destination(repo_map: dict) -> dict | None:
    """What the plugin's platform module says the Intent's sandbox is; None outside a runtime.

    The module owns the platform's facts, so this tool names none: it asks for the expected
    destination and reads only whether that is a local database file.
    """
    if not (PLUGIN_SCRIPTS / "platforms").is_dir():
        return None
    if str(PLUGIN_SCRIPTS) not in sys.path:
        sys.path.insert(0, str(PLUGIN_SCRIPTS))
    try:
        from intent_context import load_intent_context

        context = dict(load_intent_context())
    except Exception:  # noqa: BLE001 - no Intent context: the caller passes --catalog
        return None
    from platforms import UnknownPlatformError, load

    platform = context.get("VD_DOMAIN_DATA_PLATFORM") or _platform(repo_map)
    try:
        return {**load(platform).expected_destination(context), "platform": platform}
    except UnknownPlatformError:
        return {"platform": platform}


def _warehouse(command: str, repo_map: dict, catalog: str, json_catalog: bool = False) -> Path:
    """The built warehouse a dynamic command reads, or the refusal that says why there is none.

    `--catalog` wins. Otherwise the Intent's platform names its sandbox; only a local database
    file is read here, so another platform is refused by name rather than skipped. Asking for a
    check against data and getting silence is the false pass this exists to prevent.
    """
    if catalog:
        path = Path(catalog).resolve()
        if not path.is_file():
            raise _Refused(_diag("error", "catalog/missing",
                                 f"{path}: no such file; build the Domain or pass --catalog"))
        return path
    destination = _platform_destination(repo_map)
    if destination is None:
        raise _Refused(_diag("error", "catalog/missing",
                             f"{command}: no Intent context names a warehouse; pass --catalog "
                             f"with the built local database file"
                             + (" or dbt's catalog.json" if json_catalog else "")))
    local = destination.get("path") if destination.get("family") == "duckdb" else None
    if not local:
        raise _Refused(_diag(
            "error", f"{command}/unsupported-platform",
            f"{command}: the `{destination.get('platform')}` platform's sandbox is not a local "
            f"database file, which is all {command} reads"
            + ("; pass --catalog with dbt's catalog.json from `dbt docs generate`"
               if json_catalog else "")))
    path = Path(local)
    if not path.is_file():
        raise _Refused(_diag("error", "catalog/missing",
                             f"{path}: the sandbox database does not exist yet; build the Domain "
                             f"before {command}"))
    return path


def _duckdb():
    """The local database driver, imported only by the commands that read a warehouse."""
    import duckdb

    return duckdb


def _landed_columns(catalog: Path) -> dict[tuple[str, str], list[tuple[str, str, bool]]]:
    """Every relation's `(column, physical type, nullable)` in the built warehouse, in order.

    A `.json` catalog is dbt's `catalog.json`: its nodes and sources carry the same facts for any
    platform, without nullability, which `check --relations` does not need.
    """
    found: dict[tuple[str, str], list[tuple[str, str, bool]]] = {}
    if catalog.suffix == ".json":
        try:
            doc = json.loads(catalog.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise _Refused(_diag("error", "catalog/unreadable", f"{catalog}: {exc}"))
        for section in ("nodes", "sources"):
            for entry in (doc.get(section) or {}).values():
                meta = entry.get("metadata") or {}
                relation = (str(meta.get("schema") or ""), str(meta.get("name") or ""))
                columns = sorted((entry.get("columns") or {}).values(),
                                 key=lambda column: column.get("index") or 0)
                found[relation] = [(str(c.get("name")), str(c.get("type") or ""), True)
                                   for c in columns]
        return found
    duckdb = _duckdb()
    try:
        con = duckdb.connect(str(catalog), read_only=True)
        try:
            rows = con.execute(
                """
                select table_schema, table_name, column_name, data_type, is_nullable
                from information_schema.columns
                where table_schema not in ('information_schema', 'pg_catalog')
                order by table_schema, table_name, ordinal_position
                """
            ).fetchall()
        finally:
            con.close()
    except duckdb.Error as exc:
        raise _Refused(_diag("error", "catalog/unreadable", f"{catalog}: {exc}"))
    for schema, table, column, physical, nullable in rows:
        found.setdefault((schema, table), []).append((column, physical, nullable != "NO"))
    return found


def _landed(relation: tuple[str, str], landed: dict) -> list[tuple[str, str, bool]] | None:
    """The relation's landed columns; a catalog's identifier case is the platform's, not ours."""
    if relation in landed:
        return landed[relation]
    folded = (relation[0].lower(), relation[1].lower())
    for key, columns in landed.items():
        if (key[0].lower(), key[1].lower()) == folded:
            return columns
    return None


def _server_diagnostics(relation: tuple[str, str], path: Path, doc: dict,
                        repo_map: dict, root: Path) -> list[dict]:
    """A contract's `servers` against the platform the map declares.

    Two forms state the platform: `custom` carrying it as `customProperties.platform`, which is
    what `reverse` drafts, or the standard's own type for it, which an author may use when they
    fill in the coordinates that type requires (the schema checks those).
    """
    platform = _platform(repo_map)
    typed = _server_type(platform)
    if "servers" not in doc:
        # An empty list is the empty-section check's finding, not a second one here.
        return [_diag("error", "servers/mismatch",
                      f"{path}: no `servers`, and the map declares `{platform}`; a contract "
                      f"states the platform that serves it", path)]
    for server in doc.get("servers") or []:
        if not isinstance(server, dict):
            continue
        kind = server.get("type")
        stated = next((p.get("value") for p in server.get("customProperties") or []
                       if isinstance(p, dict) and p.get("property") == "platform"), None)
        if kind == "custom" and stated == platform:
            continue
        if kind == typed and typed != "custom":
            continue
        want = f"`custom` with customProperties.platform {platform!r}"
        if typed != "custom":
            want += f", or `{typed}`"
        return [_diag("error", "servers/mismatch",
                      f"{path}: `type` is {kind!r} (platform {stated!r}) and the map's "
                      f"`{platform}` platform makes it {want}", path)]
    return []


def _declared_name(relation: tuple[str, str], path: Path, doc: dict) -> list[dict]:
    """The contract's own `schema[].name` against the relation its path names.

    The path places the file; the name is what a consumer reads. A rename of one without the
    other leaves a contract describing a relation that does not exist.
    """
    names = [
        element.get("name")
        for element in doc.get("schema") or []
        if isinstance(element, dict) and element.get("name")
    ]
    if not names:
        return []
    if relation[1] in names:
        return []
    return [_diag("error", "contract/misnamed",
                  f"{path}: `schema[].name` is {names[0]!r} and the path names "
                  f"`{relation[1]}`; a contract states the relation it governs", path)]


def _relation_diagnostics(relation: tuple[str, str], path: Path, doc: dict,
                          landed: dict, repo_map: dict) -> list[dict]:
    """The contract's columns against the columns the catalog holds for the relation.

    A column the catalog has and the contract does not is the drift this exists for: the
    contract states the shape, and the landed relation still carries a column it dropped.
    """
    label = f"{relation[0]}.{relation[1]}"
    columns = _landed(relation, landed)
    if not columns:
        return [_diag("error", "relations/mismatch",
                      f"{label}: the catalog holds no such relation, so the contract describes "
                      f"nothing; build it before comparing", path)]
    # A loader column is not the relation's shape, the same rule the contract side applies.
    built = {name.lower(): name for name, _type, _nullable in columns
             if not _is_loader_column(relation, name, repo_map)}
    declared = {name.lower(): name for name in _properties(doc)}
    diags = []
    for key in sorted(set(declared) - set(built)):
        diags.append(_diag("error", "relations/mismatch",
                           f"{label}: the contract declares `{declared[key]}` and the built "
                           f"relation has no such column", path))
    for key in sorted(set(built) - set(declared)):
        diags.append(_diag("error", "relations/mismatch",
                           f"{label}: the built relation carries `{built[key]}` and the contract "
                           f"does not declare it", path))
    return diags


def _server(root: Path, repo_map: dict, relation: tuple[str, str]) -> dict:
    """Where the relation lives: the Domain as the server, the platform it names as `custom`.

    The standard's own type for a platform requires coordinates (a database file, a project, an
    account) that live in the Intent's settings, never in the repository, so a draft cannot
    ground them and states the platform instead.
    """
    return {
        "server": repo_map.get("repo_root") or root.name,
        "type": "custom",
        "schema": relation[0],
        "customProperties": [{"property": "platform", "value": _platform(repo_map) or "unknown"}],
    }


def _properties_entry(root: Path, repo_map: dict, relation: tuple[str, str]) -> dict | None:
    """The relation's entry in the dbt properties file that already names it, if one does.

    True of a dbt model and a seed; a dlt-loaded relation has no properties file, so it has
    nothing to contribute and nothing is looked for.
    """
    layer = _layer_for(repo_map, relation[0])
    if layer is None or layer.get("built_by") != "dbt" or not layer.get("path"):
        return None
    kind = "seed" if (root / layer["path"] / f"{relation[1]}.csv").is_file() else "model"
    path = _properties_file(root, layer, relation[1], PROPERTIES_SECTION[kind])
    if not path.is_file():
        return None
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (yaml.YAMLError, OSError):
        return None
    return _document_entry(doc, PROPERTIES_SECTION[kind], relation[1])


def _logical_types(relation: tuple[str, str], column: str, physical: str,
                   field: dict | None) -> tuple[str | None, bool]:
    """(logicalType, carries a timezone) for one column.

    The canonical model's `datatype` wins where it declares the field: it is the agreed
    meaning, and the contract exists to agree with it. The physical type answers otherwise.
    """
    datatype = (field or {}).get("datatype")
    if datatype in LOGICAL_TYPES:
        return LOGICAL_TYPES[datatype], datatype == "DateTimeTz"
    base = physical.split("(", 1)[0].strip().upper()
    logical = PHYSICAL_LOGICAL.get(base)
    if logical is None:
        raise _Refused(_diag("error", "contract/unmapped-type",
                             f"{relation[0]}.{relation[1]}.{column}: `{physical}` has no ODCS "
                             f"logicalType"))
    return logical, "WITH TIME ZONE" in base


def _declared_tests(column: dict | None):
    """The generic tests a dbt column entry declares, as (name, arguments) pairs."""
    declared = (column or {}).get("tests") or (column or {}).get("data_tests") or []
    for test in declared:
        if isinstance(test, str):
            yield test, {}
        elif isinstance(test, dict) and len(test) == 1:
            name, arguments = next(iter(test.items()))
            yield name, arguments if isinstance(arguments, dict) else {}


def _quality_rules(column: dict | None) -> list[dict]:
    """The rules an existing dbt generic test already asserts, in the shape `generate` reads."""
    rules = []
    for name, arguments in _declared_tests(column):
        if name not in RULE_METRICS:
            continue
        rule: dict = {"type": "library", "metric": RULE_METRICS[name]}
        if name == "accepted_values":
            values = arguments.get("values") or (arguments.get("arguments") or {}).get("values")
            if not values:
                continue
            rule["arguments"] = {"valid_values": list(values)}
        rule["mustBe"] = 0
        rules.append(rule)
    return rules


def _relationships(column: dict | None) -> list[dict]:
    """Every `relationships` test as the property-level foreign key it asserts."""
    found = []
    for name, arguments in _declared_tests(column):
        if name != "relationships":
            continue
        # dbt nests a generic test's parameters under `arguments`; the flat form predates it.
        arguments = arguments.get("arguments") or arguments
        field = arguments.get("field")
        target = str(arguments.get("to") or "")
        match = REF_REFERENCE.search(target) or SOURCE_REFERENCE.search(target)
        if match and field:
            found.append({"to": f"{match.group(1)}.{field}"})
    return found


def _catalog_relations(catalog: Path, repo_map: dict,
                       boundary: dict[tuple[str, str], str]) -> dict[tuple[str, str], list[dict]]:
    """Every column of every boundary relation the warehouse holds, in catalog order.

    `_dlt_*` bookkeeping, a loader column on a relation dlt loads, and a relation off the
    boundary are not drafted.
    """
    relations: dict[tuple[str, str], list[dict]] = {}
    for relation, columns in _landed_columns(catalog).items():
        if relation not in boundary or relation[1].startswith("_dlt"):
            continue
        for column, physical, nullable in columns:
            if _is_loader_column(relation, column, repo_map):
                continue
            relations.setdefault(relation, []).append({
                "name": column, "physicalType": physical, "nullable": nullable,
            })
    return relations


def _identifier(name: str) -> str:
    """A column name as a quoted identifier, so a reserved word or a space still resolves."""
    return '"' + name.replace('"', '""') + '"'


def _relation_sql(relation: tuple[str, str]) -> str:
    return f"{_identifier(relation[0])}.{_identifier(relation[1])}"


def _relation_evidence(con, relation: tuple[str, str], columns: list[dict]) -> dict:
    """Rows, nulls and distinct values per column, from the warehouse itself.

    One aggregate per relation: a finding is a claim about the data, so the data is what
    settles it, and an aggregate is the same answer on every run.
    """
    select = ["count(*) as total"]
    for index, column in enumerate(columns):
        name = _identifier(column["name"])
        select.append(f"count(*) filter (where {name} is null) as nulls_{index}")
        select.append(f"count(distinct {name}) as distinct_{index}")
    row = con.execute(f"select {', '.join(select)} from {_relation_sql(relation)}").fetchone()
    evidence = {"rows": row[0], "columns": {}}
    for index, column in enumerate(columns):
        evidence["columns"][column["name"]] = {
            "nulls": row[1 + 2 * index], "distinct": row[2 + 2 * index],
        }
    return evidence


def _evidence(catalog: Path, relations: dict[tuple[str, str], list[dict]]) -> dict:
    """The data behind every relation a contract is owed for."""
    duckdb = _duckdb()

    evidence: dict[tuple[str, str], dict] = {}
    try:
        con = duckdb.connect(str(catalog), read_only=True)
        try:
            for relation, columns in sorted(relations.items()):
                evidence[relation] = _relation_evidence(con, relation, columns)
        finally:
            con.close()
    except duckdb.Error as exc:
        raise _Refused(_diag("error", "catalog/unreadable", f"{catalog}: {exc}"))
    return evidence


def _settled_metrics(prop: dict) -> set[str]:
    """The rules that assert zero violations: only those settle a doubt."""
    return {
        rule.get("metric")
        for rule in prop.get("quality") or []
        if isinstance(rule, dict) and rule.get("type") == "library" and rule.get("mustBe") == 0
    }


def _property_findings(relation: tuple[str, str], prop: dict, rows: int, statistics: dict,
                       keys: set[str]) -> list[dict]:
    """The doubts one contract property leaves open, each with the evidence gathered so far."""
    name = prop["name"]
    where = f"{relation[0]}.{relation[1]}.{name}"
    nulls, distinct = statistics["nulls"], statistics["distinct"]
    non_null = rows - nulls
    settled = _settled_metrics(prop)
    diags = []
    if not _prose(prop.get("description")):
        diags.append(_diag("warning", UNDESCRIBED,
                           f"{where}: no description; what does {name} mean?"))
    if nulls and not prop.get("required") and "nullValues" not in settled:
        diags.append(_diag("warning", UNPROVEN_NULLABLE,
                           f"{where}: {nulls} of {rows} rows are null and no rule settles whether "
                           f"that is allowed; is {name} required, or is a null a real state?"))
    repeats = rows > 0 and distinct < non_null
    candidate_key = name in keys or KEY_SHAPED.search(name) is not None
    if candidate_key and repeats:
        diags.append(_diag("warning", KEY_REPEATS,
                           f"{where}: {non_null - distinct} duplicate value(s) across {non_null} "
                           f"non-null rows under a key name; which column(s) identify one row of "
                           f"{relation[1]}, and is {name} one of them or a repeated reference — if "
                           f"a reference, what does it target?"))
    # An identifier's value set is not a domain: "which values are valid" has a false premise for
    # a key, whose own question (class 6) asks what it points at instead.
    if (rows and 1 < distinct <= ENUM_MAX_DISTINCT and distinct < rows
            and prop.get("logicalType") == "string" and "invalidValues" not in settled
            and REFERENCE_SHAPED.search(name) is None):
        diags.append(_diag("warning", OPEN_ENUM,
                           f"{where}: {distinct} distinct values and no accepted_values test; "
                           f"which values are valid, and is the set closed?"))
    if (REFERENCE_SHAPED.search(name) is not None and not name.startswith("_dlt")
            and not prop.get("relationships") and not prop.get("primaryKey")
            and not (candidate_key and repeats)):
        diags.append(_diag("warning", UNDECLARED_FOREIGN_KEY,
                           f"{where}: the name reads as a reference and no relationships test "
                           f"states its target; what does {name} reference, or is it this "
                           f"relation's own identifier?"))
    return diags


def _findings(relation: tuple[str, str], contract: dict, evidence: dict,
              keys: set[str]) -> list[dict]:
    """Every doubt one contract leaves open, as the question that would settle it.

    A finding is raised only where neither the contract, the canonical model, the dbt tests nor
    the data settle the point: what is already answered is not queued, and nothing is guessed.
    """
    label = f"{relation[0]}.{relation[1]}"
    elements = [element for element in contract.get("schema") or [] if isinstance(element, dict)]
    element = elements[0] if elements else {}
    description = contract.get("description")
    purpose = description.get("purpose") if isinstance(description, dict) else description
    diags = []
    if not _prose(element.get("dataGranularityDescription")):
        diags.append(_diag("warning", NO_GRAIN,
                           f"{label}: no grain statement; what does one row of "
                           f"{relation[1]} represent?"))
    if not any(_prose(prose) for prose in
               (purpose, element.get("description"), element.get("dataGranularityDescription"))):
        diags.append(_diag("warning", UNDESCRIBED,
                           f"{label}: nothing describes this table; what is {relation[1]} for?"))
    for name, prop in sorted(_properties(contract).items()):
        diags.extend(_property_findings(
            relation, prop, evidence["rows"],
            evidence["columns"].get(name) or {"nulls": 0, "distinct": 0}, keys))
    return diags


def _draft(root: Path, repo_map: dict, relation: tuple[str, str], columns: list[dict],
           documents: dict[tuple[str, str], tuple[dict, Path]]) -> dict:
    """One contract, holding only what the catalog, the canonical model and dbt already state."""
    schema, table = relation
    document = documents.get(relation)
    dataset = document[0] if document else {}
    entry = _properties_entry(root, repo_map, relation) or {}
    fields = {field["name"]: field for field in dataset.get("fields") or []
              if isinstance(field, dict) and field.get("name")}
    described = {column["name"]: column for column in entry.get("columns") or []
                 if isinstance(column, dict) and column.get("name")}
    keys = set(dataset.get("primary_key") or [])

    element: dict = {"name": table}
    grain = _prose(dataset.get("description")) or _prose(entry.get("description"))
    if grain:
        element["dataGranularityDescription"] = grain

    properties = []
    for column in columns:
        field = fields.get(column["name"])
        declared = described.get(column["name"])
        logical, timezone = _logical_types(relation, column["name"], column["physicalType"], field)
        prop: dict = {"name": column["name"]}
        if logical:
            prop["logicalType"] = logical
        prop["physicalType"] = column["physicalType"]
        if timezone:
            prop["logicalTypeOptions"] = {"timezone": True}
        if not column["nullable"]:
            prop["required"] = True
        if column["name"] in keys:
            prop["primaryKey"] = True
        description = (_prose((declared or {}).get("description"))
                       or _prose((field or {}).get("description")))
        if description:
            prop["description"] = description
        rules = _quality_rules(declared)
        if rules:
            prop["quality"] = rules
        relationships = _relationships(declared)
        if relationships:
            prop["relationships"] = relationships
        properties.append(prop)
    if properties:
        element["properties"] = properties

    doc = {
        "apiVersion": SPEC_VERSION,
        "kind": "DataContract",
        "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{schema}.{table}")),
        "name": table,
        "version": "1.0.0",
        "status": "active",
        "servers": [_server(root, repo_map, relation)],
        "schema": [element],
    }
    if document is not None:
        doc["authoritativeDefinitions"] = [
            {"url": _display(document[1], root), "type": "businessDefinition"},
        ]
    return doc


def _reviewed(target: Path, draft: dict, existing: bool) -> dict:
    """The contract a reader would find: the committed one, unless it cannot be read.

    `reverse` leaves a reviewed file alone, so the findings triage what is on disk rather than
    the draft it would have written. A file that does not parse contributes the draft; `check`
    is where a broken contract fails.
    """
    if not existing:
        return draft
    try:
        doc = yaml.safe_load(target.read_text(encoding="utf-8"))
    except (yaml.YAMLError, OSError):
        return draft
    return doc if isinstance(doc, dict) else draft


def _dump(doc: dict) -> str:
    """One contract as YAML: the order it was built in, never wrapped, so a re-run is identical.

    The dumper indents a block sequence under its key, which is how the rest of the repo's
    YAML reads; PyYAML's default puts it flush with the key.
    """
    return yaml.dump(doc, Dumper=_IndentedDumper, sort_keys=False, allow_unicode=True, width=1000)


@app.command()
def version():
    """Pin report: tool, standard version and the vendored upstream commit, as JSON."""
    print(json.dumps({"tool": "odcs", "spec": SPEC_VERSION, "upstream_commit": UPSTREAM_COMMIT}))


@app.command()
@_guarded
def reverse(root: str = "", catalog: str = "", force: bool = False):
    """Draft one contract per boundary relation, grounded only in the catalog, the model and dbt.

    The warehouse is the physical truth: every property and its type comes from
    `information_schema`. A field the canonical model declares overrides the physical type, and
    an existing dbt generic test becomes a quality rule or a declared relationship. Nothing is
    invented: a relation no description reaches, or a gold relation no document names, is left
    for the review rather than filled in. The id is derived from the relation, so a forced
    rewrite reproduces every file. Only the boundary is drafted; a relation off it owes none.

    An existing contract is a draft the review already worked on, so this leaves it alone and
    reports how many it skipped; `--force` rewrites them all.

    Every relation also yields its open questions as `finding/*` warnings, computed against the
    contract on disk (or the draft, where there is none), gold relations first, then inputs: a
    doubt the model, the tests or the data already settles is not raised, and one nothing
    settles is, naming the relation, the column and the question that would close it.
    """
    repo_root = _repo_root(root)
    repo_map = _repo_map(repo_root)
    raw = (repo_map.get("artifact_paths") or {}).get("data_contract")
    if not raw:
        raise _Refused(_diag("error", "contract/unconfigured",
                             "repo-map.json declares no artifact_paths.data_contract, so no "
                             "contract can be placed"))
    warehouse = _warehouse("reverse", repo_map, catalog)
    documents = _canonical_datasets(repo_root, repo_map)
    declared = declared_relations(repo_root, repo_map)
    relations = _catalog_relations(warehouse, repo_map, boundary_relations(repo_map, declared))
    evidence = _evidence(warehouse, relations)

    drafts: list[tuple[tuple[str, str], dict]] = []
    diags: list[dict] = []
    for relation, columns in sorted(relations.items()):
        try:
            drafts.append((relation, _draft(repo_root, repo_map, relation, columns, documents)))
        except _Refused as refusal:
            diags.append(refusal.diag)
    if diags:
        print(json.dumps(diags))
        raise typer.Exit(1)

    base = _declared(repo_root, raw)
    keys = {
        relation: set(dataset.get("primary_key") or [])
        for relation, (dataset, _) in documents.items()
    }
    per_relation: list[tuple[tuple[int, tuple[str, str]], list[dict]]] = []
    skipped = 0
    for relation, doc in drafts:
        target = base / relation[0] / f"{relation[1]}.odcs.yaml"
        existing = target.is_file() and not force
        if existing:
            skipped += 1
        found = _findings(relation, _reviewed(target, doc, existing), evidence[relation],
                          keys.get(relation, set()))
        found.sort(key=lambda diag: (diag["message"].split(": ", 1)[0], diag["code"]))
        per_relation.append(((_boundary_rank(relation, repo_map, declared), relation), found))
        if existing:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_dump(doc), encoding="utf-8")
    findings = [diag for _rank, found in sorted(per_relation, key=lambda item: item[0])
                for diag in found]
    if skipped:
        findings.append(_diag(
            "warning", "contract/exists",
            f"{skipped} contract(s) already exist and were left alone; pass --force to rewrite "
            f"them"))
    print(json.dumps(findings))


@app.command()
@_guarded
def generate(root: str = ""):
    """Write the dbt contract keys and dlt column hints that the contracts own.

    Only the keys this tool owns are written, and nothing is written at all when a contract
    already disagrees with the map: a contract set that is inconsistent is not a source.
    """
    repo_root = _repo_root(root)
    repo_map = _repo_map(repo_root)
    contracts = _committed_contracts(repo_root, repo_map)

    texts: dict[Path, str] = {}
    hints: dict[str, dict[str, dict]] = {}
    diags: list[dict] = []
    for relation, path in sorted(contracts.items()):
        try:
            doc = yaml.safe_load(path.read_text(encoding="utf-8"))
            if not isinstance(doc, dict):
                raise _Refused(_diag("error", "schema/invalid", f"{path}: not a mapping", path))
            target = _target(repo_root, repo_map, relation)
            if target is None:
                continue
            kind, file = target
            owned = _owned(relation, doc, kind)
        except _Refused as refusal:
            diags.append(refusal.diag)
            continue
        except yaml.YAMLError as exc:
            diags.append(_diag("error", "schema/invalid", f"{path}: {exc}", path))
            continue
        if kind == "dlt":
            hints[relation[1]] = owned["columns"]
        else:
            texts[file] = _apply(texts.get(file) or _base(file), PROPERTIES_SECTION[kind],
                                 relation[1], owned)

    if diags:
        print(json.dumps(diags))
        raise typer.Exit(1)

    for file, text in texts.items():
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(text, encoding="utf-8")
    if hints:
        module = _hints_module(repo_root, repo_map)
        module.parent.mkdir(parents=True, exist_ok=True)
        module.write_text(_module_text(hints), encoding="utf-8")
    print(json.dumps([]))


@app.command()
@_guarded
def check(root: str = "", relations: bool = False, catalog: str = ""):
    """Validate every committed contract against the repo map and the vendored schema.

    Every boundary relation owes a contract (`contract/missing`); a contract naming no relation
    the map declares is an orphan. A contract off the boundary is checked like any other.

    Where the repo has adopted a canonical model, a gold contract must also agree with the
    document that names its relation. A schema that model documents no relation in is not yet
    modelled: its gold relations warn (`contract/no-document`) instead of failing, because the
    review asks for those documents and the gate has nothing to agree with. A map that declares
    no gold layers leaves nothing to agree with at all: `map/no-gold-layers` reports that gap
    rather than inventing a set. `--relations` adds the comparison against the built relation
    and fails when no catalog resolves, rather than skipping: asking for the check and getting
    silence is the false pass it exists to prevent.
    """
    repo_root = _repo_root(root)
    repo_map = _repo_map(repo_root)
    contracts = _committed_contracts(repo_root, repo_map)

    declared = declared_relations(repo_root, repo_map)
    boundary = boundary_relations(repo_map, declared)
    documents = _canonical_datasets(repo_root, repo_map)
    gold = _gold_schemas(repo_map, declared) if documents else set()
    # A schema the canonical model documents no relation in is not yet modelled: the review
    # asks for a document, the gate does not fail a relation that never had one to agree with.
    modelled = gold & {schema for schema, _ in documents}
    diags: list[dict] = _unknown_contract_layers(repo_map)
    diags.extend(_misplaced(_declared(repo_root, repo_map["artifact_paths"]["data_contract"]),
                            repo_root))
    parsed: dict[tuple[str, str], dict] = {}

    # A canonical model with no gold layers declared leaves agreement unchecked. Saying so is the
    # report; guessing which layers are gold would gate relations the map never claimed.
    if documents and not (repo_map.get("gold_layers") or []):
        diags.append(_diag("warning", "map/no-gold-layers",
                           "repo-map.json declares no gold_layers, so no relation is gated against "
                           "the canonical model; declare the layers that hold gold"))

    for relation, declarer in sorted(boundary.items()):
        if relation not in contracts:
            diags.append(_diag("error", "contract/missing",
                               f"{relation[0]}.{relation[1]}: declared by {declarer}, on the "
                               f"contract boundary, and has no contract"))
    for relation, path in sorted(contracts.items()):
        if relation in declared or _is_audit(relation, repo_map):
            continue
        diags.append(_diag("error", "contract/orphan",
                           f"{path}: names no relation the map declares", path))
    for relation, path in sorted(contracts.items()):
        if relation[0] in gold and relation not in documents:
            diags.append(_diag("warning", "contract/no-document",
                               f"{relation[0]}.{relation[1]}: no canonical document names this "
                               f"gold relation; the review asks for one", path))

    for relation, path in sorted(contracts.items()):
        try:
            doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            diags.append(_diag("error", "schema/invalid", f"{path}: {exc}", path))
            continue
        if not isinstance(doc, dict):
            diags.append(_diag("error", "schema/invalid", f"{path}: not a mapping", path))
            continue
        parsed[relation] = doc
        diags.extend(_schema_diagnostics(doc, path))
        diags.extend(_empty_sections(doc, path))
        diags.extend(_grain(doc, path))
        diags.extend(_declared_name(relation, path, doc))
        diags.extend(_loader_columns(relation, doc, repo_map))
        if documents:
            diags.extend(_gold_agreement(relation, path, doc, documents, modelled))

    diags.extend(_stale_diagnostics(repo_root, repo_map, contracts, parsed))

    for relation, path in sorted(contracts.items()):
        doc = parsed.get(relation)
        if doc is not None:
            diags.extend(_server_diagnostics(relation, path, doc, repo_map, repo_root))

    if relations:
        try:
            landed = _landed_columns(_warehouse("relations", repo_map, catalog, json_catalog=True))
        except _Refused as refusal:
            diags.append(refusal.diag)
        else:
            for relation, path in sorted(contracts.items()):
                doc = parsed.get(relation)
                if doc is not None:
                    diags.extend(_relation_diagnostics(relation, path, doc, landed, repo_map))

    print(json.dumps(diags))
    raise typer.Exit(1 if any(d["severity"] == "error" for d in diags) else 0)


@app.command()
@_guarded
def validate(contracts: str = typer.Argument(..., help="the contracts directory"),
             root: str = ""):
    """The checks one contract settles on its own, with no repo map: schema validity, no empty
    section, a stated grain and a `schema[].name` that matches its path.

    For a caller such as Domain CI that reads none of the Domain's own files. `check` runs these
    too, plus everything that needs the map: the boundary, orphans, gold agreement, generated
    keys and `servers`.
    """
    repo_root, base = _contracts_location(root, contracts)
    if not base.is_dir():
        _fail(_diag("error", "contract/no-directory",
                    f"{_display(base, repo_root)}: no such contracts directory"))
    diags: list[dict] = _misplaced(base, repo_root)
    for relation, path in sorted(_contracts_under(base).items()):
        shown = _display(path, repo_root)
        doc = _loaded(path.read_text(encoding="utf-8"))
        if doc is None:
            diags.append(_diag("error", "schema/invalid", f"{shown}: not a YAML mapping", shown))
            continue
        diags.extend(_schema_diagnostics(doc, shown))
        diags.extend(_empty_sections(doc, shown))
        diags.extend(_grain(doc, shown))
        diags.extend(_declared_name(relation, shown, doc))
    print(json.dumps(diags))
    raise typer.Exit(1 if any(d["severity"] == "error" for d in diags) else 0)


# ── diff: what a change did to the promise, and the version it needs ─────────────────────────

# The semver move a change needs, smallest first. A version moved less than its change needs
# fails; one moved more passes, since nothing breaks for a consumer told more than happened.
PATCH, MINOR, MAJOR = 1, 2, 3
LEVEL_NAMES = {MAJOR: "breaking", MINOR: "additive", PATCH: "descriptive"}
SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:[-+][0-9A-Za-z.+-]*)?$")

# Keys a change to which alters what a consumer's query returns or may rely on. Every other
# key of a property or a schema element is prose, and a change to it is descriptive.
TYPE_KEYS = ("logicalType", "physicalType", "logicalTypeOptions")
KEY_KEYS = ("primaryKey", "primaryKeyPosition")
ELEMENT_CLAIMS = ("name", "properties", "quality", "relationships", "dataGranularityDescription")
PROPERTY_CLAIMS = ("name", *TYPE_KEYS, *KEY_KEYS, "required", "quality", "relationships")


def _semver(text: object) -> tuple[int, int, int] | None:
    match = SEMVER.match(str(text or ""))
    return tuple(int(part) for part in match.groups()) if match else None  # type: ignore[return-value]


def _moved(before: tuple[int, int, int], after: tuple[int, int, int]) -> int:
    """The level a version moved by: 0 when it did not move forward."""
    if after[0] > before[0]:
        return MAJOR
    if after[0] == before[0] and after[1] > before[1]:
        return MINOR
    if after[:2] == before[:2] and after[2] > before[2]:
        return PATCH
    return 0


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _promise(item: object) -> str:
    """A rule or relationship without its wording: the part a consumer relies on."""
    if isinstance(item, dict):
        item = {k: v for k, v in item.items() if k not in ("description", "name")}
    return _canonical(item)


def _listed(changes: list, label: str, before: object, after: object, what: str) -> None:
    """A list of promises (rules, relationships): one withdrawn is breaking, one added additive.

    A change to a promise's `description` or `name` alone is descriptive.
    """
    old = {_promise(item): item for item in before or [] if item is not None}
    new = {_promise(item): item for item in after or [] if item is not None}
    for key in sorted(set(old) - set(new)):
        changes.append((MAJOR, f"{label}: {what} {key} withdrawn"))
    for key in sorted(set(new) - set(old)):
        changes.append((MINOR, f"{label}: {what} {key} added"))
    for key in sorted(set(old) & set(new)):
        if _canonical(old[key]) != _canonical(new[key]):
            changes.append((PATCH, f"{label}: {what} {key} reworded"))


def _prose_keys(changes: list, label: str, before: dict, after: dict, claims: tuple) -> None:
    for key in sorted((set(before) | set(after)) - set(claims)):
        if _canonical(before.get(key)) != _canonical(after.get(key)):
            changes.append((PATCH, f"{label}: `{key}` changed"))


def _property_changes(changes: list, label: str, before: dict, after: dict) -> None:
    for key in (*TYPE_KEYS, *KEY_KEYS):
        was_value, now_value = before.get(key), after.get(key)
        if key == "physicalType" and isinstance(was_value, str) and isinstance(now_value, str):
            was_value, now_value = was_value.upper(), now_value.upper()
        if _canonical(was_value) != _canonical(now_value):
            changes.append((MAJOR, f"{label}: `{key}` {before.get(key)!r} -> {after.get(key)!r}"))
    was, now = before.get("required") is True, after.get("required") is True
    if now and not was:
        changes.append((MAJOR, f"{label}: `required` newly true refuses a null a consumer may "
                               f"already receive"))
    elif was and not now:
        changes.append((MAJOR, f"{label}: `required` withdrawn, so a consumer may now receive "
                               f"a null"))
    _listed(changes, label, before.get("quality"), after.get("quality"), "quality rule")
    _listed(changes, label, before.get("relationships"), after.get("relationships"),
            "relationship")
    _prose_keys(changes, label, before, after, PROPERTY_CLAIMS)


def _named(items: object) -> dict[str, dict]:
    return {str(item["name"]): item for item in items or []
            if isinstance(item, dict) and item.get("name")}


def _server_changes(changes: list, before: object, after: object) -> None:
    """`servers` as a multiset of entries: order is no claim, a server added is additive, one
    withdrawn or changed (a withdrawal and an addition) is breaking."""
    old = collections.Counter(_canonical(item) for item in before or [])
    new = collections.Counter(_canonical(item) for item in after or [])
    for key in sorted((old - new).elements()):
        changes.append((MAJOR, f"`servers`: {key} withdrawn; the relation is no longer "
                               f"published there"))
    for key in sorted((new - old).elements()):
        changes.append((MINOR, f"`servers`: {key} added"))


def contract_changes(before: dict, after: dict) -> list[tuple[int, str]]:
    """Every change between two versions of one contract, each with the level it needs.

    Breaking: a property removed (a rename is a removal and an addition), a type, key or grain
    changed, a rule or relationship withdrawn, `required` turned either way. Additive: a
    property, rule or relationship added. Descriptive: any other key, which is prose.
    """
    changes: list[tuple[int, str]] = []
    if before.get("id") != after.get("id"):
        changes.append((MAJOR, f"`id` {before.get('id')!r} -> {after.get('id')!r}"))
    _server_changes(changes, before.get("servers"), after.get("servers"))
    _prose_keys(changes, "contract", before, after, ("id", "version", "schema", "servers"))
    old_elements, new_elements = _named(before.get("schema")), _named(after.get("schema"))
    for name in sorted(set(old_elements) - set(new_elements)):
        changes.append((MAJOR, f"{name}: schema element removed or renamed"))
    for name in sorted(set(new_elements) - set(old_elements)):
        changes.append((MINOR, f"{name}: schema element added"))
    for name in sorted(set(old_elements) & set(new_elements)):
        old, new = old_elements[name], new_elements[name]
        was = _prose(old.get("dataGranularityDescription"))
        now = _prose(new.get("dataGranularityDescription"))
        if was and was != now:
            changes.append((MAJOR, f"{name}: grain changed"))
        elif now and not was:
            changes.append((PATCH, f"{name}: grain stated"))
        _listed(changes, name, old.get("quality"), new.get("quality"), "quality rule")
        _listed(changes, name, old.get("relationships"), new.get("relationships"),
                "relationship")
        _prose_keys(changes, name, old, new, ELEMENT_CLAIMS)
        old_props, new_props = _named(old.get("properties")), _named(new.get("properties"))
        for prop in sorted(set(old_props) - set(new_props)):
            changes.append((MAJOR, f"{name}.{prop}: property removed or renamed"))
        for prop in sorted(set(new_props) - set(old_props)):
            changes.append((MINOR, f"{name}.{prop}: property added"))
        for prop in sorted(set(old_props) & set(new_props)):
            _property_changes(changes, f"{name}.{prop}", old_props[prop], new_props[prop])
    return changes


def _contracts_location(root: str, contracts: str) -> tuple[Path, Path]:
    """The Domain root and its contracts directory: from `contracts` when given, which reads no
    repo map, else from the map's `artifact_paths.data_contract`."""
    if contracts:
        repo_root = Path(root).resolve() if root else Path.cwd().resolve()
        return repo_root, (repo_root / contracts).resolve()
    repo_root = _repo_root(root)
    repo_map = _repo_map(repo_root)
    if not (repo_map.get("artifact_paths") or {}).get("data_contract"):
        _committed_contracts(repo_root, repo_map)  # refuses with contract/unconfigured
    return repo_root, _declared(repo_root, repo_map["artifact_paths"]["data_contract"])


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True)
    if result.returncode != 0:
        raise _Refused(_diag("error", "diff/unknown-ref",
                             f"git {' '.join(args)}: {result.stderr.strip() or 'failed'}"))
    return result.stdout


def _contracts_at(root: Path, base: Path, ref: str) -> dict[tuple[str, str], str]:
    """Every contract the base ref holds under `base`, keyed by relation, as a path relative to
    the root.

    A directory outside the root is refused: git sees nothing there, and comparing nothing would
    pass every change.
    """
    try:
        relative_base = base.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        raise _Refused(_diag("error", "diff/contracts-outside-root",
                             f"{base}: the contracts directory is not under the root {root}; "
                             f"pass a directory inside it"))
    _git(root, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
    found: dict[tuple[str, str], str] = {}
    for line in _git(root, "ls-tree", "-r", "--name-only", ref, "--", relative_base).splitlines():
        relation = _relation_of(Path(line).relative_to(relative_base).parts)
        if relation is not None:
            found[relation] = line
    return found


def _loaded(text: str) -> dict | None:
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError:
        return None
    return doc if isinstance(doc, dict) else None


@app.command()
@_guarded
def diff(base_ref: str = typer.Argument(..., help="git ref the change is compared against"),
         root: str = "",
         contracts: str = typer.Option("", help="the contracts directory, relative to the root; "
                                                "given, no repo-map.json is read")):
    """Classify each contract the change touched and require the version bump it needs.

    Each contract on disk is compared with the same file at `base_ref`. Every change is reported
    with its level (`diff/breaking`, `diff/additive`, `diff/descriptive`, severity info), and a
    contract whose `version` moved less than its largest change needs fails
    `diff/version-not-bumped`. A new contract needs only a valid version. A removed contract is a
    warning, `diff/contract-removed`: the relation's removal is the change, and it is the
    consumer-facing one.

    `--contracts` names the directory directly, for a caller such as Domain CI that reads no
    repo map; the root is then `--root`, else the working directory.
    """
    repo_root, base_dir = _contracts_location(root, contracts)
    head = _contracts_under(base_dir)
    fork = base_ref
    try:  # the fork point, so commits that landed on the base since are not this change's
        fork = _git(repo_root, "merge-base", base_ref, "HEAD").strip() or base_ref
    except _Refused:
        pass  # an unknown ref is reported by _contracts_at below
    base = _contracts_at(repo_root, base_dir, fork)
    diags: list[dict] = []
    for relation in sorted(set(head) | set(base)):
        label = f"{relation[0]}.{relation[1]}"
        if relation not in head:
            diags.append(_diag("warning", "diff/contract-removed",
                               f"{label}: the contract is gone since {base_ref}; removing the "
                               f"relation breaks every consumer of it", base[relation]))
            continue
        path = head[relation]
        shown = _display(path, repo_root)
        after = _loaded(path.read_text(encoding="utf-8"))
        if after is None:
            diags.append(_diag("error", "diff/unreadable", f"{shown}: not a YAML mapping", shown))
            continue
        if _semver(after.get("version")) is None:
            diags.append(_diag("error", "diff/invalid-version",
                               f"{shown}: `version` {after.get('version')!r} is not semver "
                               f"MAJOR.MINOR.PATCH", shown))
            continue
        if relation not in base:
            continue
        before = _loaded(_git(repo_root, "show", f"{fork}:./{base[relation]}"))
        if before is None:
            diags.append(_diag("warning", "diff/unreadable",
                               f"{shown}: the {base_ref} version is not a YAML mapping, so "
                               f"nothing compares it", shown))
            continue
        changes = contract_changes(before, after)
        for level, text in changes:
            diags.append(_diag("info", f"diff/{LEVEL_NAMES[level]}", f"{label}: {text}", shown))
        old_version = _semver(before.get("version"))
        new_version = _semver(after.get("version"))
        if old_version is not None and new_version < old_version:
            diags.append(_diag("error", "diff/version-not-bumped",
                               f"{label}: `version` went back from {before.get('version')} to "
                               f"{after.get('version')}", shown))
            continue
        needed = max((level for level, _text in changes), default=0)
        moved = _moved(old_version, new_version) if old_version is not None else MAJOR
        if moved < needed:
            diags.append(_diag(
                "error", "diff/version-not-bumped",
                f"{label}: a {LEVEL_NAMES[needed]} change needs a "
                f"{('patch', 'minor', 'major')[needed - 1]} bump and `version` moved "
                f"{('no', 'a patch', 'a minor', 'a major')[moved]} step "
                f"({before.get('version')} -> {after.get('version')})", shown))
    print(json.dumps(diags))
    raise typer.Exit(1 if any(d["severity"] == "error" for d in diags) else 0)


# ── profile: the contract's claims against the data ──────────────────────────────────────────

# The comparisons a library rule may state, as the test a measured count passes.
OPERATORS = {
    "mustBe": lambda count, bound: count == bound,
    "mustBeLessThan": lambda count, bound: count < bound,
    "mustBeLessOrEqualTo": lambda count, bound: count <= bound,
}


def _bounded(rule: dict) -> tuple[str, int] | None:
    for operator in OPERATORS:
        if isinstance(rule.get(operator), int) and not isinstance(rule.get(operator), bool):
            return operator, rule[operator]
    return None


def _profile_relation(con, relation: tuple[str, str], path: str, doc: dict,
                      columns: list[tuple[str, str, bool]] | None) -> list[dict]:
    """Every claim one contract makes about its data that a query can contradict."""
    label = f"{relation[0]}.{relation[1]}"
    if not columns:
        return [_diag("error", "profile/missing-relation",
                      f"{label}: the warehouse holds no such relation; build it before "
                      f"profiling", path)]
    # The local database resolves identifiers whatever their case, so case is not a claim.
    landed = {name.lower(): name for name, _type, _nullable in columns}
    props = _properties(doc)
    table = _relation_sql(relation)
    diags: list[dict] = []

    def present(name: str) -> bool:
        if name.lower() in landed:
            return True
        diags.append(_diag("error", "profile/missing-column",
                           f"{label}.{name}: the contract makes a claim about a column the "
                           f"relation does not have", path))
        return False

    keyed = sorted(
        (prop.get("primaryKeyPosition") or 0, name)
        for name, prop in props.items() if prop.get("primaryKey") is True)
    keys = [name for _position, name in keyed]
    if keys and all([present(name) for name in keys]):
        identifiers = [_identifier(landed[name.lower()]) for name in keys]
        # A row with a null key part has no key value to repeat; a required null is its own claim.
        known = " and ".join(f"{column} is not null" for column in identifiers)
        groups, rows = con.execute(
            f"select count(*), coalesce(sum(n), 0) from (select count(*) as n from {table} "
            f"where {known} group by {', '.join(identifiers)} having count(*) > 1)").fetchone()
        if groups:
            diags.append(_diag("error", "profile/key-repeats",
                               f"{label}: {groups} value(s) of the primary key ({', '.join(keys)}) "
                               f"repeat across {rows} rows; the key claims one row each", path))
    for name, prop in sorted(props.items()):
        if prop.get("required") is True and present(name):
            nulls = con.execute(
                f"select count(*) from {table} where {_identifier(landed[name.lower()])} is null"
            ).fetchone()[0]
            if nulls:
                diags.append(_diag("error", "profile/required-null",
                                   f"{label}.{name}: {nulls} row(s) are null and the property "
                                   f"is required", path))
        for rule in prop.get("quality") or []:
            if not (isinstance(rule, dict) and rule.get("type") == "library"
                    and rule.get("metric") == "invalidValues"):
                continue
            values = [v for v in (rule.get("arguments") or {}).get("valid_values") or []
                      if v is not None]  # a NULL in NOT IN makes every row unknown
            bound = _bounded(rule)
            if not values or bound is None or not present(name):
                continue
            column = _identifier(landed[name.lower()])
            marks = ", ".join("?" for _ in values)
            where = f"{column} is not null and {column} not in ({marks})"
            outside = con.execute(f"select count(*) from {table} where {where}",
                                  list(values)).fetchone()[0]
            if OPERATORS[bound[0]](outside, bound[1]):
                continue
            sample = [row[0] for row in con.execute(
                f"select distinct cast({column} as varchar) from {table} where {where} "
                f"order by 1 limit 5", list(values)).fetchall()]
            diags.append(_diag("error", "profile/invalid-values",
                               f"{label}.{name}: {outside} row(s) hold a value outside "
                               f"valid_values (e.g. {', '.join(sample)}), and the rule says "
                               f"{bound[0]} {bound[1]}", path))
    return diags


@app.command()
@_guarded
def profile(root: str = "", catalog: str = "",
            relation: list[str] = typer.Option(
                [], help="SCHEMA.TABLE to profile; repeat it, or omit it for every contract")):
    """Test each contract's data claims against the built relation, over every row.

    The primary key (every `primaryKey: true` property together, the grain the contract names)
    must not repeat (`profile/key-repeats`), a `required: true` property must hold no null
    (`profile/required-null`), and an `invalidValues` rule's count of values outside its
    `valid_values` must meet its operator (`profile/invalid-values`). Each contradicted claim is
    an error. With no built warehouse this refuses (`catalog/missing`) rather than passing.
    """
    repo_root = _repo_root(root)
    repo_map = _repo_map(repo_root)
    contracts = _committed_contracts(repo_root, repo_map)
    wanted = {tuple(name.split(".", 1)) for name in relation}
    unknown = sorted(".".join(name) for name in wanted if name not in contracts)
    if unknown:
        raise _Refused(_diag("error", "profile/unknown-relation",
                             f"no contract for {', '.join(unknown)}"))
    warehouse = _warehouse("profile", repo_map, catalog)
    if warehouse.suffix == ".json":
        raise _Refused(_diag("error", "catalog/unreadable",
                             f"{warehouse}: profile queries rows, so it reads a local database "
                             f"file, not dbt's catalog.json"))
    landed = _landed_columns(warehouse)
    duckdb = _duckdb()
    diags: list[dict] = []
    try:
        con = duckdb.connect(str(warehouse), read_only=True)
    except duckdb.Error as exc:
        raise _Refused(_diag("error", "catalog/unreadable", f"{warehouse}: {exc}"))
    try:
        for name, path in sorted(contracts.items()):
            if wanted and name not in wanted:
                continue
            shown = _display(path, repo_root)
            doc = _loaded(path.read_text(encoding="utf-8"))
            if doc is None:
                diags.append(_diag("error", "schema/invalid", f"{shown}: not a mapping", shown))
                continue
            try:
                diags.extend(_profile_relation(con, name, shown, doc, _landed(name, landed)))
            except duckdb.Error as exc:
                diags.append(_diag("error", "profile/query-failed",
                                   f"{name[0]}.{name[1]}: {exc}", shown))
    finally:
        con.close()
    print(json.dumps(diags))
    raise typer.Exit(1 if any(d["severity"] == "error" for d in diags) else 0)


# ── results: a dbt run's test results as contract assertions ─────────────────────────────────

# The generic test `generate` renders for each rule, back to the rule it renders.
TEST_RULES = {"not_null": "nullValues", "unique": "duplicateValues",
              "accepted_values": "invalidValues", "relationships": "relationships"}


def _read_json(path: Path, what: str) -> dict:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise _Refused(_diag("error", "results/unreadable", f"{path}: {what}: {exc}"))
    if not isinstance(doc, dict):
        raise _Refused(_diag("error", "results/unreadable", f"{path}: {what} is not an object"))
    return doc


def _tested_relation(manifest: dict, test: dict) -> tuple[str, str, str] | None:
    """The `(schema, table, kind)` a test is attached to, from the manifest's node or source."""
    attached = test.get("attached_node") or next(
        (node for node in (test.get("depends_on") or {}).get("nodes") or []
         if not node.startswith("test.")), None)
    if not attached:
        return None
    node = (manifest.get("nodes") or {}).get(attached) or (manifest.get("sources") or {}).get(
        attached)
    if not node:
        return None
    if attached.startswith("source."):
        return str(node.get("schema")), str(node.get("identifier") or node.get("name")), "source"
    return str(node.get("schema")), str(node.get("alias") or node.get("name")), "node"


def _contract_for(contracts: dict[tuple[str, str], Path], schema: str,
                  table: str) -> tuple[str, str] | None:
    """The contract a built relation answers to.

    A dbt target may prefix the schema a layer declares (a developer's or a CI schema), so the
    table decides and the schema only breaks a tie.
    """
    schema, table = schema.lower(), table.lower()
    candidates = [relation for relation in contracts if relation[1].lower() == table]
    exact = [relation for relation in candidates
             if schema == relation[0].lower() or schema.endswith(f"_{relation[0].lower()}")]
    # A relation outside the boundary may share a table name with a contracted one; the
    # schema must agree, so its tests are never credited to the other's contract.
    return exact[0] if len(exact) == 1 else None


REF = re.compile(r"""^\s*ref\(\s*(?:['"][^'"]*['"]\s*,\s*)?['"]([^'"]+)['"]\s*\)\s*$""")


def _asserts(rule: str, prop: dict, kwargs: dict) -> bool:
    """Whether a test asserts what the property's contract states, not merely the same kind.

    A test whose arguments cannot be read against the contract is not credited: crediting it
    would record a rule as tested by a test that checks something else.
    """
    if rule == "relationships":
        model = REF.match(str(kwargs.get("to") or ""))
        field = str(kwargs.get("field") or "").lower()
        if not model or not field:
            return False
        for relationship in prop.get("relationships") or []:
            target = str((relationship or {}).get("to") or "") if isinstance(
                relationship, dict) else ""
            relation, _, column = target.rpartition(".")
            if (relation.rpartition(".")[2].lower() == model.group(1).lower()
                    and column.lower() == field):
                return True
        return False
    if rule == "invalidValues":
        values = kwargs.get("values")
        if not isinstance(values, list):
            return False
        tested = {str(value) for value in values}
        return any(
            isinstance(rule_doc, dict) and rule_doc.get("type") == "library"
            and rule_doc.get("metric") == "invalidValues" and rule_doc.get("mustBe") == 0
            and {str(v) for v in (rule_doc.get("arguments") or {}).get("valid_values") or []}
            == tested
            for rule_doc in prop.get("quality") or [])
    return rule in _settled_metrics(prop)


def _implied(rule: str, prop: dict, contract: dict) -> str | None:
    """The property claim a `not_null` or `unique` test renders when no quality rule does:
    `required`, or a single-column `primaryKey`."""
    if rule == "nullValues" and prop.get("required") is True:
        return "required"
    if rule == "duplicateValues" and _sole_key(contract) == prop.get("name"):
        return "primaryKey"
    return None


def _executed_at(result: dict, run_results: dict) -> str | None:
    for timing in result.get("timing") or []:
        if timing.get("name") == "execute" and timing.get("completed_at"):
            return timing["completed_at"]
    return (run_results.get("metadata") or {}).get("generated_at")


@app.command()
@_guarded
def results(run_results: str = typer.Argument(..., help="dbt's run_results.json"),
            manifest: str = typer.Option("", help="dbt's manifest.json; default: beside "
                                                  "run_results.json"),
            root: str = ""):
    """Print one JSON record per contract assertion a dbt run executed, one per line.

    Each test result is traced through the manifest to the relation and column it is attached
    to, then to the contract and the rule (or relationship) that rendered it. A test no contract
    rule accounts for (hand-written, or on a relation with no contract) is skipped. Nothing is
    stored: the caller keeps the records.
    """
    repo_root = _repo_root(root)
    repo_map = _repo_map(repo_root)
    contracts = _committed_contracts(repo_root, repo_map)
    results_path = Path(run_results).resolve()
    manifest_path = Path(manifest).resolve() if manifest else results_path.with_name(
        "manifest.json")
    if not manifest_path.is_file():
        raise _Refused(_diag("error", "results/no-manifest",
                             f"{manifest_path}: no manifest to trace tests to their columns; pass "
                             f"--manifest"))
    run = _read_json(results_path, "run results")
    graph = _read_json(manifest_path, "manifest")
    docs: dict[tuple[str, str], dict | None] = {}
    records = []
    for result in run.get("results") or []:
        test = (graph.get("nodes") or {}).get(str(result.get("unique_id")))
        if not test or test.get("resource_type") != "test":
            continue
        meta = test.get("test_metadata") or {}
        rule = TEST_RULES.get(str(meta.get("name")))
        column = test.get("column_name") or (meta.get("kwargs") or {}).get("column_name")
        built = _tested_relation(graph, test)
        if rule is None or not column or built is None:
            continue
        relation = _contract_for(contracts, built[0], built[1])
        if relation is None:
            continue
        if relation not in docs:
            docs[relation] = _loaded(contracts[relation].read_text(encoding="utf-8"))
        doc = docs[relation]
        prop = _properties(doc or {}).get(str(column))
        if prop is None:
            continue
        kwargs = meta.get("kwargs") or {}
        # dbt 1.10 nests a test's own arguments under `arguments`; earlier versions flatten them.
        if isinstance(kwargs.get("arguments"), dict):
            kwargs = {**kwargs, **kwargs["arguments"]}
        if not _asserts(rule, prop, kwargs):
            rule = _implied(rule, prop, doc or {})
            if rule is None:
                continue
        records.append({
            "contract_id": doc.get("id"),
            "contract_version": doc.get("version"),
            "relation": f"{relation[0]}.{relation[1]}",
            "property": str(column),
            "rule": rule,
            "status": result.get("status"),
            "failures": result.get("failures"),
            "executed_at": _executed_at(result, run),
        })
    for record in records:
        print(json.dumps(record))


if __name__ == "__main__":
    app()
