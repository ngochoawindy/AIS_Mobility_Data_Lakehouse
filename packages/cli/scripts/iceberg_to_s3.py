import os
import sys
from glob import glob
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, ".")
from bench import s3io
from bench.config import DATA, TRIPS_DEST, layouts, trips_root
from bench.iceberg_build import catalog, table_name, NAMESPACE

TRIPS_ROOT = DATA / "trips"
BUCKET = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/").removeprefix("s3://").rstrip("/")
DEFAULT = {"L0", "L1s", "L2s", "L3s", "L4s", "LHs"}


def upload(s3, ls) -> tuple[list[str], list[str]]:
    """Local-trips path: copy each local parquet to s3 and return (files, s3 uris)."""
    root = TRIPS_ROOT / ls.subdir
    files = sorted(glob((root / "**" / "*.parquet").as_posix(), recursive=True))
    uris = []
    for f in files:
        rel = Path(f).relative_to(DATA)              # trips/<subdir>/<...>
        key = f"{BUCKET}/{rel.as_posix()}"
        with open(f, "rb") as src, s3.open_output_stream(key) as dst:
            dst.write(src.read())
        uris.append(f"s3://{key}")
    return files, uris


def register(cat, ls, schema, uris: list[str]) -> None:
    cat.create_namespace_if_not_exists((NAMESPACE,))
    ident = table_name(ls)
    try:
        cat.drop_table(ident)
    except Exception:
        pass
    tbl = cat.create_table(ident, schema=schema)
    tbl.add_files(uris)


def main() -> None:
    args = set(sys.argv[1:])
    all_layouts = list(layouts())
    keys = {l.key for l in layouts()} if args == {"all"} else (args or DEFAULT)
    if os.getenv("ICEBERG_CATALOG_TYPE") != "rest":
        sys.exit("Set REST env first:  source iceberg_rest/env.rest")
    cat = catalog()
    on_s3 = s3io.is_s3(TRIPS_DEST)        # trips already on object storage -> register in place
    s3 = None if on_s3 else s3io.s3_fs()
    for ls in all_layouts:
        if ls.key not in keys:
            continue
        if on_s3:                          # no upload - list the s3 parquet + add_files
            uris = s3io.list_parquet(trips_root(ls))
            if not uris:
                print(f"  {ls.key:<12} no trips parquet on s3; skipped", flush=True)
                continue
            schema = pq.ParquetFile(uris[0].removeprefix("s3://"),
                                    filesystem=s3io.s3_fs()).schema_arrow
            register(cat, ls, schema, uris)
            verb = "registered (in place)"
        else:
            files, uris = upload(s3, ls)
            if not files:
                print(f"  {ls.key:<12} no trips parquet found; skipped", flush=True)
                continue
            schema = pq.ParquetFile(files[0]).schema_arrow
            register(cat, ls, schema, uris)
            verb = "uploaded+registered"
        print(f"  {ls.key:<12} {verb} {len(uris):>5} files  -> {table_name(ls)}", flush=True)
    print(f"\nDone. Tables under REST catalog namespace {NAMESPACE!r} on s3://{BUCKET}/")


if __name__ == "__main__":
    main()
