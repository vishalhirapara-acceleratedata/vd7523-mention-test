# Vendored Apache Ossie sources

Pinned to upstream commit `c5cb931d72029dc523a5dd18d267f083403786f2`
(2026-10-14, `apache/ossie`). Do not edit these files: `ossie.py` imports
`validation/validate.py` and reads the three schemas as-is.

The directory tree mirrors upstream, because the validator resolves a schema
`$ref` that names `core-spec/ossie-schema.json` or `ontology/ontology.json` onto
the same relative path under this directory. Moving a file breaks resolution
offline, and the mapping schema then fails with an unresolvable reference.

Canonical definitions (upstream source of truth; these files are copies):
- Spec: https://github.com/apache/ossie/blob/main/core-spec/spec.md
- Machine schema: https://github.com/apache/ossie/blob/main/core-spec/ossie-schema.json
- Ontology schema: https://github.com/apache/ossie/blob/main/ontology/ontology.json
- Mapping schema: https://github.com/apache/ossie/blob/main/ontology/mapping.json
- Pinned tree: https://github.com/apache/ossie/tree/c5cb931d72029dc523a5dd18d267f083403786f2
- Pinned validator: https://github.com/apache/ossie/blob/c5cb931d72029dc523a5dd18d267f083403786f2/validation/validate.py

To re-pin: download the four files at the new commit, update this note, and
re-run `python3 -m pytest skills/semantic-model/scripts/__tests__ -q` from the plugin root. Any upstream
behavior change must surface as a test failure here, never silently.

- `validation/validate.py` — `validation/validate.py` (Apache-2.0, header intact)
- `core-spec/ossie-schema.json` — `core-spec/ossie-schema.json`, spec `0.2.0.dev0`
- `ontology/ontology.json` — `ontology/ontology.json`, spec `0.2.0.dev0`
- `ontology/mapping.json` — `ontology/mapping.json`, spec `0.2.0.dev0`
