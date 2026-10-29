# Vendored Open Data Contract Standard schema

Never edit a file here. A re-pin replaces the file and updates this README in the same commit.

| | |
| --- | --- |
| Standard | Open Data Contract Standard (ODCS) |
| Standard version | `v3.2.0` |
| Upstream repository | https://github.com/bitol-io/open-data-contract-standard |
| Upstream path | `schema/odcs-json-schema-v3.2.0.json` |
| Pinned commit | `8b551607aad6801979bf4c209385e78fc4d029e5` |
| Bytes / sha256 | 110505 / `edb41f33ec46e847…` |

`odcs.py check` loads this file and nothing else: it never fetches, so a validation run is offline by construction.

The published standard takes precedence over this schema where the two disagree. `logicalTypeOptions` is
the case that matters: the schema already constrains its keys per logical type — `timeZone` is rejected and
`timezone`, `defaultTimezone` and `format` are accepted — so `check` keeps no allow-list of its own and trusts
the schema to reject an undocumented key.
