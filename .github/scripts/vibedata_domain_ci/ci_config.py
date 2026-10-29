"""Pure helpers for ci-config.yml loading and intent-slug validation.

Extracted from preflight.py so these functions are unit-testable in isolation
and reusable without going through the CLI shell.
"""
import os
import re

from vibedata_domain_ci.platform_enum import (
    DATABRICKS,
    DUCKDB_LOCAL,
    FABRIC_LAKEHOUSE,
    FABRIC_WAREHOUSE,
    MOTHERDUCK,
    PLATFORMS,
    REDSHIFT,
    SNOWFLAKE,
    VALID_PLATFORMS,
)

try:
    import yaml
except ImportError:
    yaml = None


def locate_ci_config(root: str = ".") -> str:
    """Return the ci-config.yml path: always .workflow/ci-config.yml."""
    return ".workflow/ci-config.yml"


def locate_project_dir(root: str = ".") -> str:
    """Return C6 dbt project path: transformation/ with root fallback."""
    candidate = os.path.join(root, "transformation", "dbt_project.yml")
    return "transformation" if os.path.isfile(candidate) else "."

INTENT_SLUG_RE = re.compile(r"^intent/[a-z0-9][a-z0-9\-]+$")

# The two `*_NAME` coordinates are deliberately absent: no workflow consumes them
# (ci.yml re-exports them as preflight outputs that nothing reads downstream), and
# both the /install-ci skill and the plugin's runtime-location-contract.json declare
# them optional. Requiring them here failed an install that legitimately omitted them.
_FABRIC_REQUIRED_KEYS = [
    "VD_DOMAIN_SLUG",
    "VD_DOMAIN_FABRIC_LAKEHOUSE_WORKSPACE_ID",
    "VD_DOMAIN_FABRIC_LAKEHOUSE_ID",
]

# A Warehouse's coordinates are warehouse-shaped, not lakehouse-shaped, and the NAME
# is load-bearing rather than cosmetic: the Intent's bridge Lakehouse — and the CI
# bridge that mirrors it — must carry the Domain Warehouse's name verbatim, because a
# three-part TDS name records no workspace and only resolves against an item of
# exactly that name.
_FABRIC_WAREHOUSE_REQUIRED_KEYS = [
    "VD_DOMAIN_SLUG",
    "VD_DOMAIN_FABRIC_WAREHOUSE_WORKSPACE_ID",
    "VD_DOMAIN_FABRIC_WAREHOUSE_WORKSPACE_NAME",
    "VD_DOMAIN_FABRIC_WAREHOUSE_ID",
    "VD_DOMAIN_FABRIC_WAREHOUSE_NAME",
]

# Redshift's coordinates: the Domain's endpoint and database, as the plugin's install-ci
# renders them. The bundle publishes the production manifest only, which reads the
# database and VD_DOMAIN_SCHEMA (redshift_prod_profile.py).
_REDSHIFT_REQUIRED_KEYS = [
    "VD_DOMAIN_SLUG",
    "VD_DOMAIN_REDSHIFT_HOST",
    "VD_DOMAIN_REDSHIFT_PORT",
    "VD_DOMAIN_REDSHIFT_DATABASE",
]

_MOTHERDUCK_REQUIRED_KEYS = [
    "VD_DOMAIN_SLUG",
    "VD_DOMAIN_MOTHERDUCK_DATABASE",
]

# LocalDuck runs no CI against a target platform, so it has no credential/coordinate
# keys to validate — just the domain slug every platform's config carries.
_DUCKDB_LOCAL_REQUIRED_KEYS = [
    "VD_DOMAIN_SLUG",
]

# Total over platform_enum.VALID_PLATFORMS — every member has its own entry, so a
# future platform added to VALID_PLATFORMS without a matching entry here raises
# (via the dict indexing below) instead of silently inheriting Fabric's keys.
# Snowflake and Databricks publish only the production manifest, whose offline compile
# renders the database or catalog into every relation name. The Primary Schema is optional,
# as the Domain binding's is: an absent one falls back to the platform's default schema in
# the publish profile.
_SNOWFLAKE_REQUIRED_KEYS = [
    "VD_DOMAIN_SLUG",
    "VD_DOMAIN_SNOWFLAKE_DATABASE",
]

_DATABRICKS_REQUIRED_KEYS = [
    "VD_DOMAIN_SLUG",
    "VD_DOMAIN_DATABRICKS_CATALOG",
]

_REQUIRED_KEYS_BY_PLATFORM = {
    FABRIC_LAKEHOUSE: _FABRIC_REQUIRED_KEYS,
    FABRIC_WAREHOUSE: _FABRIC_WAREHOUSE_REQUIRED_KEYS,
    MOTHERDUCK: _MOTHERDUCK_REQUIRED_KEYS,
    DUCKDB_LOCAL: _DUCKDB_LOCAL_REQUIRED_KEYS,
    REDSHIFT: _REDSHIFT_REQUIRED_KEYS,
    SNOWFLAKE: _SNOWFLAKE_REQUIRED_KEYS,
    DATABRICKS: _DATABRICKS_REQUIRED_KEYS,
}

# The indirection block is validated permissively on purpose: an
# unrecognised key passes through so an older repository does not break on a
# stale one. No `*_VAR` key is required here — a mapping can
# be present and correct while the Variable it names is unset, so requiring the
# key would not catch the real failure anyway. Validating that a named Variable
# actually resolves needs the workflow to pass `${{ vars[...] }}` in, which is a
# separate change across the bundles.


def _is_blank(value) -> bool:
    """True when a declared key carries no usable value.

    `KEY:` with nothing after it parses as None, and `KEY: ""` as the empty
    string. Both satisfy a presence check while breaking every consumer, which
    is the same failure seen from the other direction.
    """
    return value is None or (isinstance(value, str) and value.strip() == "")


_PLACEHOLDER = re.compile(r"<[^<>]+>")


def _is_placeholder(value) -> bool:
    """True when a value is still the bundle template's `<...>` token.

    A leftover `<string>` satisfies both the presence and the blank check, so
    without this an unfilled template line reaches a consumer as real data —
    the prod-manifest publish once recorded `<string>` as the schema.
    """
    return isinstance(value, str) and _PLACEHOLDER.fullmatch(value.strip()) is not None


def require_indirection_key(config: dict, key: str) -> dict:
    """Return the GitHub Actions name a `*_VAR` indirection key holds, or an error naming the key.

    Pure and deliberately not part of `parse_ci_config`'s validation: that stays permissive about
    `*_VAR` keys. Callers with no safe default name (the MotherDuck cleanup) use
    this to fail closed instead of falling back to a literal secret name.
    """
    value = config.get(key)
    if not isinstance(value, str) or _is_blank(value):
        return {"ok": False, "value": None, "error": f"ci-config.yml must set {key} to a GitHub Actions secret or variable name"}
    return {"ok": True, "value": value.strip(), "error": None}


# Committed coordinate key -> legacy lowercase key every existing downstream
# consumer (preflight.py's GITHUB_OUTPUT writer, ci.yml's `outputs.*` refs,
# fabric_api.py) already expects. VD_DOMAIN_SCHEMA has no entry here —
# confirmed dead, dropped rather than renamed.
_KEY_ALIASES = {
    "VD_DOMAIN_DATA_PLATFORM": "platform",
    "VD_DOMAIN_SLUG": "domain",
    "VD_DOMAIN_FABRIC_LAKEHOUSE_WORKSPACE_ID": "prod_workspace_id",
    "VD_DOMAIN_FABRIC_LAKEHOUSE_WORKSPACE_NAME": "prod_workspace_name",
    "VD_DOMAIN_FABRIC_LAKEHOUSE_ID": "prod_lakehouse_id",
    "VD_DOMAIN_FABRIC_LAKEHOUSE_NAME": "prod_lakehouse_name",
    # No Warehouse entries, on purpose. The Warehouse preflight job reads its
    # coordinates back under their own VD_DOMAIN_FABRIC_WAREHOUSE_* names (an
    # untranslated key passes through to GITHUB_OUTPUT unchanged), and its publish
    # workflow reads ci-config.yml directly, so no consumer needs a legacy name.
    # A shared alias would also make `_translate_config_keys` lossy for a malformed
    # config carrying both Fabric kinds' coordinates — two source keys mapping to
    # one `prod_workspace_id` silently drops whichever comes first.
    "VD_DOMAIN_MOTHERDUCK_DATABASE": "prod_db_name",
    "VD_DOMAIN_SNOWFLAKE_DATABASE": "prod_db_name",
    "VD_DOMAIN_DATABRICKS_CATALOG": "prod_db_name",
    "VD_DOMAIN_SNOWFLAKE_PRIMARY_SCHEMA": "prod_schema_name",
    "VD_DOMAIN_DATABRICKS_PRIMARY_SCHEMA": "prod_schema_name",
    "VD_DOMAIN_CI_DBT_PROFILE": "ci_target",
    "VD_DOMAIN_CI_SPARK_COMPUTE": "spark_compute",
    "VD_DOMAIN_PROD_MANIFEST_SOURCE": "prod_manifest_source",
}

# duckdb-quack is handled below with a specific migration message before this check.
# The accepted values are owned by platform_enum so every consumer that branches
# on platform reads the same set.
_VALID_PLATFORMS = VALID_PLATFORMS


def _translate_config_keys(config: dict) -> dict:
    """Rename recognized VD_DOMAIN_* keys to the legacy lowercase names every
    existing consumer expects. All non-coordinate indirection values pass through
    unchanged so repository owners control their committed mapping."""
    return {_KEY_ALIASES.get(k, k): v for k, v in config.items()}


def validate_intent_slug(branch_name: str) -> tuple[bool, str | None]:
    """Return (valid, branch_name_if_valid_else_None)."""
    if INTENT_SLUG_RE.match(branch_name):
        return True, branch_name
    return False, None


def parse_ci_config(yaml_str: str) -> dict:
    """Parse ci-config.yml content.

    Returns:
        {
            "ok": bool,
            "config": dict,
            "error": str | None,
            "line_number": int | None,
            "missing_keys": list[str],
        }
    """
    if yaml is None:
        return {"ok": False, "config": {}, "error": "pyyaml not installed", "line_number": None, "missing_keys": []}

    try:
        config = yaml.safe_load(yaml_str)
    except yaml.YAMLError as exc:
        line_number = None
        if hasattr(exc, "problem_mark") and exc.problem_mark is not None:
            line_number = exc.problem_mark.line + 1
        return {"ok": False, "config": {}, "error": str(exc), "line_number": line_number, "missing_keys": []}

    if config is None:
        config = {}

    if not isinstance(config, dict):
        return {
            "ok": False,
            "config": {},
            "error": "ci-config.yml must be a YAML mapping (key: value pairs), not a list or scalar",
            "line_number": None,
            "missing_keys": [],
        }

    platform = config.get("VD_DOMAIN_DATA_PLATFORM")

    if platform == "duckdb-quack":
        return {
            "ok": False,
            "config": _translate_config_keys(config),
            "error": "platform: duckdb-quack is no longer supported. Use platform: motherduck instead.",
            "line_number": None,
            "missing_keys": [],
        }

    if platform is not None and platform not in _VALID_PLATFORMS:
        return {
            "ok": False,
            "config": _translate_config_keys(config),
            "error": f"Unknown platform: {platform!r}. Valid values: {', '.join(PLATFORMS)}.",
            "line_number": None,
            "missing_keys": [],
        }

    # platform is None (VD_DOMAIN_DATA_PLATFORM omitted) is the one deliberate
    # exception that still defaults to Fabric's keys, matching prior behavior.
    # Any non-None, recognized-but-unmapped platform raises KeyError rather than
    # silently inheriting Fabric's required keys.
    required_keys = _FABRIC_REQUIRED_KEYS if platform is None else _REQUIRED_KEYS_BY_PLATFORM[platform]
    absent = [k for k in required_keys if k not in config]
    blank = [k for k in required_keys if k in config and _is_blank(config[k])]
    # Every key, not only required ones: an optional line left as `<string>`
    # is just as wrong as a required one.
    placeholder = [k for k, v in config.items() if _is_placeholder(v)]

    if absent or blank or placeholder:
        parts = []
        if absent:
            parts.append(f"Missing required keys: {absent}")
        if blank:
            parts.append(f"Required keys present but empty: {blank}")
        if placeholder:
            parts.append(f"Template placeholders left unfilled: {placeholder}")
        return {
            "ok": False,
            "config": _translate_config_keys(config),
            "error": ". ".join(parts),
            "line_number": None,
            "missing_keys": absent + blank + placeholder,
        }

    return {"ok": True, "config": _translate_config_keys(config), "error": None, "line_number": None, "missing_keys": []}


def resolve_prod_manifest_mode(yaml_str: str) -> str:
    """Return VD_DOMAIN_PROD_MANIFEST_SOURCE.mode, or "artifact" when it can't be read.

    Lenient by design: Gate 1 must still pick a prod-state source when the
    config is absent or malformed — preflight is the gate that rejects it.
    """
    try:
        c = yaml.safe_load(yaml_str) or {}
        return (c.get("VD_DOMAIN_PROD_MANIFEST_SOURCE") or {}).get("mode", "artifact")
    except Exception:
        return "artifact"
