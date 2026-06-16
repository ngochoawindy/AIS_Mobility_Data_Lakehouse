"""S3 / MinIO helpers shared by the build scripts and the registrar so the `trips`
layouts can be written and read directly on object storage (TRIPS_DEST=s3://warehouse/...).

DuckDB writes/reads via httpfs (attach_s3); listing/overwrite use pyarrow's S3FileSystem.
Credentials come from the iceberg_rest/env.rest environment (ICEBERG_CATALOG_PROP__S3__*).
"""
from __future__ import annotations

import os

from pyarrow import fs


def is_s3(path: str) -> bool:
    return str(path).startswith("s3://")


def _endpoint() -> str:
    return (os.environ["ICEBERG_CATALOG_PROP__S3__ENDPOINT"]
            .removeprefix("http://").removeprefix("https://"))


def _key(env: str, default: str) -> str:
    return os.environ.get(env, default)


def attach_s3(con) -> None:
    """Create the DuckDB S3 secret (MinIO) so `COPY … TO 's3://…'` and
    `read_parquet('s3://…')` work. Same pattern as run_queries.connect()."""
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE OR REPLACE SECRET s3warehouse (
            TYPE S3,
            KEY_ID '{_key("ICEBERG_CATALOG_PROP__S3__ACCESS_KEY_ID", "admin")}',
            SECRET '{_key("ICEBERG_CATALOG_PROP__S3__SECRET_ACCESS_KEY", "password")}',
            ENDPOINT '{_endpoint()}',
            URL_STYLE 'path', USE_SSL false,
            REGION '{_key("ICEBERG_CATALOG_PROP__S3__REGION", "us-east-1")}'
        );
    """)


def s3_fs() -> fs.S3FileSystem:
    """pyarrow S3FileSystem for MinIO (listing, delete, schema reads)."""
    return fs.S3FileSystem(
        access_key=_key("ICEBERG_CATALOG_PROP__S3__ACCESS_KEY_ID", "admin"),
        secret_key=_key("ICEBERG_CATALOG_PROP__S3__SECRET_ACCESS_KEY", "password"),
        endpoint_override=_endpoint(), scheme="http",
        region=_key("ICEBERG_CATALOG_PROP__S3__REGION", "us-east-1"),
    )


def _prefix(uri: str) -> str:
    """s3://bucket/key… -> bucket/key…  (pyarrow paths carry no scheme)."""
    return uri.removeprefix("s3://").rstrip("/")


def clear_prefix(uri: str) -> None:
    """Delete every object under an s3 prefix (overwrite helper — replaces rmtree)."""
    s3 = s3_fs()
    sel = fs.FileSelector(_prefix(uri), recursive=True, allow_not_found=True)
    for info in s3.get_file_info(sel):
        if info.type == fs.FileType.File:
            s3.delete_file(info.path)


def list_parquet(uri: str) -> list[str]:
    """List `s3://…/*.parquet` object URIs under a prefix (recursive, sorted)."""
    s3 = s3_fs()
    sel = fs.FileSelector(_prefix(uri), recursive=True, allow_not_found=True)
    return sorted(
        "s3://" + i.path for i in s3.get_file_info(sel)
        if i.type == fs.FileType.File and i.path.endswith(".parquet")
    )
