"""Iceberg REST catalog helpers.

Used by scripts/iceberg_to_s3.py (publish) and scripts/run_queries.py / run_rest_query
to resolve tables in the Apache Iceberg REST catalog backed by MinIO object storage
(s3://warehouse). Env comes from iceberg_rest/env.rest.

The old *local* SQLite catalog build (`bench.py iceberg-build`, flat-bbox copies under
data/iceberg/) was removed — the lakehouse now lives entirely on MinIO + the REST catalog.
"""
from __future__ import annotations

import os

from pyiceberg.catalog import load_catalog

from bench.config import LayoutSpec

CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "ais_bench")
CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "rest").lower()
NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "ais")


def _extra_catalog_props() -> dict[str, str]:
    """Pass-through for ICEBERG_CATALOG_PROP__* env vars (e.g. the MinIO S3 endpoint)."""
    props: dict[str, str] = {}
    prefix = "ICEBERG_CATALOG_PROP__"
    for key, value in os.environ.items():
        if key.startswith(prefix):
            prop = key[len(prefix):].lower().replace("__", ".").replace("_", "-")
            props[prop] = value
    return props


def catalog():
    """Load the Iceberg REST catalog (MinIO warehouse). Requires the REST env to be
    sourced: `source iceberg_rest/env.rest`."""
    if CATALOG_TYPE != "rest":
        raise ValueError(
            f"ICEBERG_CATALOG_TYPE must be 'rest' (the local SQLite catalog was "
            f"removed), got {CATALOG_TYPE!r}. Did you `source iceberg_rest/env.rest`?"
        )
    rest_uri = os.getenv("ICEBERG_REST_URI")
    if not rest_uri:
        # Fall back to PyIceberg's native config (.pyiceberg.yaml / PYICEBERG_CATALOG__*).
        return load_catalog(CATALOG_NAME)

    props = {
        "type": "rest",
        "uri": rest_uri,
        "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/"),
    }
    optional_env = {
        "credential": "ICEBERG_REST_CREDENTIAL",
        "token": "ICEBERG_REST_TOKEN",
        "oauth2-server-uri": "ICEBERG_REST_OAUTH2_SERVER_URI",
        "scope": "ICEBERG_REST_SCOPE",
    }
    for prop, env_name in optional_env.items():
        if value := os.getenv(env_name):
            props[prop] = value
    props.update(_extra_catalog_props())
    return load_catalog(CATALOG_NAME, **props)


def table_name(ls: LayoutSpec) -> str:
    return f"{NAMESPACE}.{ls.key}"
