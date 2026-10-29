"""Checks over ontology and mapping documents that the vendored schemas cannot make.

The vendored validator already proves an ontology's shape: unique concept names, and
`extends`, `identify_by`, `roles` and QName prefixes that resolve. This module adds:

- per ontology: an `EntityType` with no `identify_by` (error), and the declarations
  nothing evaluates — `requires`, `derived_by`, `prefixes`, `custom_extensions` and a
  concept's or relationship's `iri` (warnings, never errors);
- per mapping, against the ontology and the model document it names: every concept and
  relationship it maps is declared, every expression is a `dataset.field` the model
  declares, and the identity lint (`identity_checks`).

Every check compares two declarations. None judges meaning: whether a term denotes
identity, or whether a mapping's population is honest, is a review.

Pure functions: input is parsed documents, output is a list of diagnostics.
"""

import json
import re

VENDOR = "VIBEDATA"
DECLARATION_KEYS = ("requires", "derived_by")
DATASET_FIELD = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*$")


def _diag(severity, code, message, file=None):
    diag = {"severity": severity, "code": code, "message": message}
    if file:
        diag["file"] = file
    return diag


def walk(node):
    """Every dict in a document tree, whatever shape the caller's schema allows."""
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from walk(value)


def declaration_warnings(data) -> list:
    """Report hidden semantics: fields that look executable and are not."""
    used = sorted({key for node in walk(data) for key in DECLARATION_KEYS if node.get(key)})
    if data.get("prefixes"):
        used.append("prefixes")
    if data.get("custom_extensions"):
        used.append("custom_extensions")
    if not used:
        return []
    return [_diag("warning", "declaration-only",
                  f"uses {', '.join(used)}, which no evaluator reads; this document is "
                  f"declaration-only and must not be described as executable")]


def ontology_checks(data) -> list:
    """Identity is declared; an external vocabulary nothing resolves is a declaration."""
    diags = []
    for component in data.get("ontology") or []:
        if not isinstance(component, dict):
            continue
        if component.get("type") == "EntityType" and not component.get("identify_by"):
            diags.append(_diag("error", "ontology/no-identity",
                               f"EntityType '{component.get('concept')}' has no identify_by; "
                               f"identity is a declared relationship to a value type, never a "
                               f"surrogate"))
        for kind, node in [("concept", component)] + [
                ("relationship", r) for r in component.get("relationships") or []
                if isinstance(r, dict)]:
            if node.get("iri"):
                diags.append(_diag("warning", "declaration-only",
                                   f"{kind} '{node.get('concept') or node.get('name')}' carries "
                                   f"an iri, which names an external vocabulary nothing "
                                   f"resolves; drop it or declare it as prose"))
    return diags + declaration_warnings(data)


class Ontology:
    """The concepts of one ontology document, with relationships inherited through extends."""

    def __init__(self, doc):
        self.components = {}
        for component in doc.get("ontology") or []:
            if isinstance(component, dict) and component.get("concept"):
                self.components.setdefault(component["concept"], component)
        self.relationship_names = {name for concept in self.components
                                   for name in self._own(concept)}

    def _own(self, concept):
        component = self.components.get(concept) or {}
        return {r["name"]: r for r in component.get("relationships") or []
                if isinstance(r, dict) and r.get("name")}

    def relationships(self, concept) -> dict:
        out, pending, seen = {}, [concept], set()
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            for name, relationship in self._own(current).items():
                out.setdefault(name, relationship)
            pending.extend(e for e in (self.components.get(current) or {}).get("extends") or []
                           if isinstance(e, str))
        return out

    def identify_by(self, concept) -> list:
        return [r for r in (self.components.get(concept) or {}).get("identify_by") or []
                if isinstance(r, str)]

    def is_entity(self, concept) -> bool:
        return (self.components.get(concept) or {}).get("type") == "EntityType"

    def target(self, concept, relationship) -> str | None:
        """The concept at the other end of a relationship: its first role that is not CONCEPT."""
        roles = [r.get("concept") for r in
                 self.relationships(concept).get(relationship, {}).get("roles") or []
                 if isinstance(r, dict) and r.get("concept")]
        others = [r for r in roles if r != concept]
        return (others or roles or [None])[0]


def _payload(node):
    payload = {}
    for ext in node.get("custom_extensions") or []:
        if isinstance(ext, dict) and ext.get("vendor_name") == VENDOR:
            try:
                decoded = json.loads(ext.get("data") or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(decoded, dict):
                payload.update(decoded)
    return payload


class Model:
    """What a model document declares about each dataset's keys, and its foreign keys."""

    def __init__(self, doc):
        self.datasets = {}
        for dataset in doc.get("datasets") or []:
            if not isinstance(dataset, dict) or not dataset.get("name"):
                continue
            payload = _payload(dataset)
            scd = payload.get("scd") if isinstance(payload.get("scd"), dict) else {}
            row = payload.get("row_level") if isinstance(payload.get("row_level"), dict) else {}
            level_keys = set()
            for level in payload.get("levels") or []:
                if not isinstance(level, dict):
                    continue
                for key in level.get("keys") or []:
                    if isinstance(key, dict) and key.get("primary") is True:
                        level_keys.update(f for f in key.get("fields") or []
                                          if isinstance(f, str))
            self.datasets[dataset["name"]] = {
                "fields": {f.get("name") for f in dataset.get("fields") or []
                           if isinstance(f, dict)},
                "primary_key": list(dataset.get("primary_key") or []),
                "natural_key": scd.get("natural_key"),
                "versioned": scd.get("type") == 2 or row.get("versioned") is True,
                "level_keys": level_keys,
            }
        self.foreign_keys = {}
        for relationship in doc.get("relationships") or []:
            if not isinstance(relationship, dict):
                continue
            pairs = zip(relationship.get("from_columns") or [], relationship.get("to_columns") or [])
            for from_column, to_column in pairs:
                self.foreign_keys.setdefault((relationship.get("from"), from_column), []).append(
                    (relationship.get("to"), to_column))

    def declares(self, dataset, field) -> bool:
        return field in (self.datasets.get(dataset) or {}).get("fields", set())

    def is_surrogate(self, dataset, field) -> bool:
        """A versioned dataset's primary key that is not its natural key names a version."""
        declared = self.datasets.get(dataset)
        return bool(declared and declared["versioned"] and field in declared["primary_key"]
                    and field != declared["natural_key"])

    def natural_keys(self, dataset) -> set:
        """The fields that name a member of DATASET, or of one of its levels, never a version."""
        declared = self.datasets.get(dataset) or {}
        keys = set(declared.get("level_keys") or ())
        if declared.get("natural_key"):
            keys.add(declared["natural_key"])
        if declared and not declared["versioned"]:
            keys.update(declared["primary_key"])
        return keys

    def surrogate_target(self, dataset, field):
        """The surrogate a foreign key on DATASET.FIELD references, if any."""
        for target, column in self.foreign_keys.get((dataset, field), []):
            if self.is_surrogate(target, column):
                return f"{target}.{column}"
        return None

    def carries_natural_key(self, dataset, field) -> bool:
        """A foreign key onto its target's natural key carries that member's identity."""
        return any(column in self.natural_keys(target)
                   for target, column in self.foreign_keys.get((dataset, field), []))


def reference_checks(mapping, ontology, model, ontology_name, model_name, file) -> list:
    """Every concept and relationship is declared; every expression is a declared dataset.field."""
    diags = []
    for node in walk(mapping.get("concept_mappings") or []):
        concept = node.get("concept")
        if concept and ontology and concept not in ontology.components:
            diags.append(_diag("error", "mapping/unknown-concept",
                               f"maps concept '{concept}', which '{ontology_name}' does not "
                               f"declare", file))
        relationship = node.get("relationship")
        if relationship and ontology and relationship not in ontology.relationship_names:
            diags.append(_diag("error", "mapping/unknown-relationship",
                               f"maps relationship '{relationship}', which '{ontology_name}' "
                               f"does not declare", file))
        expression = node.get("expression")
        if not expression:
            continue
        match = DATASET_FIELD.match(str(expression))
        if not match:
            diags.append(_diag("error", "mapping/unqualified-expression",
                               f"expression '{expression}' is not dataset.field, so it cannot "
                               f"be checked", file))
        elif model and not model.declares(*match.groups()):
            diags.append(_diag("error", "mapping/unknown-field",
                               f"expression '{expression}' names no dataset.field that "
                               f"'{model_name}' declares", file))
    return diags


def identity_checks(mapping, ontology, model, file) -> list:
    """A mapping identifies each member by its natural key, completely, never by a version.

    - `refs/identity-version-surrogate` (error): an identifying expression names a
      versioned dataset's surrogate (primary key, not `scd.natural_key`), or a foreign
      key whose relationship targets one. A link's referent that is an entity counts:
      the expression stands for the target's identity.
    - `refs/identity-not-natural-key`: an object mapping's own identity names a field
      that is not its dataset's `scd.natural_key`, a level's primary key (the row level's
      or a coarser one's: a member of that level) or, when unversioned, its primary key
      — nor a foreign key onto one of those. Always a warning: a concept may be populated
      from a natural code held on another dataset (a degenerate key on a fact, a code
      held as an attribute), and no declaration says which dataset is the concept's.
    - `refs/partial-identity` (error): a referent list resolving a concept maps only some
      of the relationships its `identify_by` names.
    """
    diags = []

    def field_of(expression):
        match = DATASET_FIELD.match(str(expression))
        return match.groups() if match and model.declares(*match.groups()) else None

    def surrogate(expression, concept, where):
        resolved = field_of(expression)
        if not resolved:
            return False
        dataset, field = resolved
        target = (f"{dataset}.{field}" if model.is_surrogate(dataset, field)
                  else model.surrogate_target(dataset, field))
        if not target:
            return False
        diags.append(_diag("error", "refs/identity-version-surrogate",
                           f"{where}: '{expression}' identifies {concept} by '{target}', the "
                           f"surrogate of a versioned dataset, which names one version of the "
                           f"member, not the member; map its natural key", file))
        return True

    def natural(expression, concept, where):
        resolved = field_of(expression)
        if not resolved:
            return
        dataset, field = resolved
        keys = model.natural_keys(dataset)
        if not keys:
            diags.append(_diag("warning", "refs/identity-not-natural-key",
                               f"{where}: '{dataset}' declares no scd.natural_key, level key "
                               f"or unversioned primary key, so '{expression}' cannot be "
                               f"compared with one", file))
        elif field not in keys and not model.carries_natural_key(dataset, field):
            diags.append(_diag("warning", "refs/identity-not-natural-key",
                               f"{where}: '{expression}' identifies {concept} by a field that "
                               f"is not a key of '{dataset}' (declared: "
                               f"{', '.join(sorted(keys))})", file))

    def resolve(entries, concept, where, own):
        """Check a referent list that resolves CONCEPT; OWN when it is the mapped dataset's."""
        if not ontology.is_entity(concept):
            return
        identity = ontology.identify_by(concept)
        relationships = ontology.relationships(concept)
        named = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            name = entry.get("relationship")
            if name not in relationships:
                if name in ontology.relationship_names:
                    diags.append(_diag("error", "mapping/foreign-relationship",
                                       f"{where}: '{name}' is not a relationship of "
                                       f"'{concept}' or a concept it extends", file))
                continue
            named.add(name)
            target = ontology.target(concept, name)
            here = f"{where} -> {name}"
            if isinstance(entry.get("referent_mappings"), list):
                resolve(entry["referent_mappings"], target, here, own=False)
            expression = entry.get("expression")
            if not expression:
                continue
            identifying = name in identity
            if identifying or ontology.is_entity(target):
                described = concept if identifying else target
                if not surrogate(expression, described, here) and identifying and own:
                    natural(expression, concept, here)
        missing = [name for name in identity if name not in named]
        if missing:
            diags.append(_diag("error", "refs/partial-identity",
                               f"{where}: {concept} is identified by {identity}, but "
                               f"{missing} {'is' if len(missing) == 1 else 'are'} not mapped",
                               file))

    for block in mapping.get("concept_mappings") or []:
        if not isinstance(block, dict) or block.get("concept") not in ontology.components:
            continue
        for index, object_mapping in enumerate(block.get("object_mappings") or []):
            if not isinstance(object_mapping, dict):
                continue
            concept = object_mapping.get("concept") or block["concept"]
            if concept not in ontology.components:
                continue
            where = f"{concept} object_mappings[{index}]"
            if object_mapping.get("expression") and ontology.is_entity(concept):
                if not surrogate(object_mapping["expression"], concept, where):
                    natural(object_mapping["expression"], concept, where)
            if isinstance(object_mapping.get("referent_mappings"), list):
                resolve(object_mapping["referent_mappings"], concept, where, own=True)
    return diags



def _single_field_keys(model, dataset) -> set:
    """Keys that name a member on their own: a composite key's columns are not identities."""
    declared = model.datasets.get(dataset) or {}
    keys = set()
    if declared.get("natural_key"):
        keys.add(declared["natural_key"])
    if not declared.get("versioned") and len(declared.get("primary_key") or []) == 1:
        keys.update(declared["primary_key"])
    return keys


def unmapped_identity_checks(mapping, ontology, model, file) -> list:
    """`refs/unmapped-identity-dataset` (warning): a dataset keyed by a concept's identity.

    A concept identified by one relationship, mapped on dataset D by `D.field`, is also
    carried by any other dataset of the document whose own natural key or single-field
    unversioned primary key is a field of that name; a column of a composite key is not. When no mapping of the concept names that
    dataset, the mapping understates the model. A foreign key is not a key of its
    dataset, so a fact holding the concept's key is never flagged.
    """
    identity_fields, mapped = {}, {}
    for block in mapping.get("concept_mappings") or []:
        if not isinstance(block, dict):
            continue
        for object_mapping in block.get("object_mappings") or []:
            if not isinstance(object_mapping, dict):
                continue
            concept = object_mapping.get("concept") or block.get("concept")
            if concept not in ontology.components or not ontology.is_entity(concept):
                continue
            identity = ontology.identify_by(concept)
            for entry in object_mapping.get("referent_mappings") or []:
                if not isinstance(entry, dict):
                    continue
                match = DATASET_FIELD.match(str(entry.get("expression") or ""))
                if not match:
                    continue
                dataset, field = match.groups()
                mapped.setdefault(concept, set()).add(dataset)
                if len(identity) == 1 and entry.get("relationship") == identity[0]:
                    identity_fields.setdefault(concept, set()).add(field)
            match = DATASET_FIELD.match(str(object_mapping.get("expression") or ""))
            if match:
                mapped.setdefault(concept, set()).add(match.group(1))
    diags = []
    for concept, fields in sorted(identity_fields.items()):
        for dataset in sorted(model.datasets):
            if dataset in mapped.get(concept, set()):
                continue
            shared = sorted(fields & _single_field_keys(model, dataset))
            if shared:
                diags.append(_diag("warning", "refs/unmapped-identity-dataset",
                                   f"'{dataset}' is keyed by '{shared[0]}', the field that "
                                   f"identifies {concept} elsewhere in this mapping, but no "
                                   f"mapping of {concept} names '{dataset}'; map it, or say "
                                   f"in its description why it is not {concept}", file))
    return diags

TERM_HEADING = re.compile(r"^\*\*([^*]+)\*\*:", re.MULTILINE)


def glossary_drift(ontologies, context_text) -> dict:
    """Concept names against `**Term**:` headings, both ways, split by concept type."""
    terms = {name.strip().casefold(): name.strip() for name in TERM_HEADING.findall(context_text)}
    concepts = {}
    for doc in ontologies:
        for component in doc.get("ontology") or []:
            if isinstance(component, dict) and component.get("concept"):
                concepts[component["concept"]] = component.get("type")
    represented = {name.casefold() for name in concepts}
    return {
        "entity_types_without_term": sorted(
            n for n, kind in concepts.items()
            if kind == "EntityType" and n.casefold() not in terms),
        "terms_without_concept": sorted(
            shown for key, shown in terms.items() if key not in represented),
        "value_types_without_term": sorted(
            n for n, kind in concepts.items()
            if kind != "EntityType" and n.casefold() not in terms),
    }
