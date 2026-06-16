import os
import sys
from glob import glob
from pathlib import Path

import duckdb

sys.path.insert(0, ".")
from bench import s3io
from bench.config import MOBILITYDUCK_EXT, TRIPS_DEST, layouts, trips_root

SOURCE_TZ = os.getenv("TRIPS_SOURCE_TZ", "Europe/Rome")
PROJECT = """
    mmsi,
    vessel_type AS ship_type,
    segment_type,
    asEWKB(tgeompointFromBinary(traj_wkb))::BLOB AS traj,
    {tmin} AS tmin,
    {tmax} AS tmax,
    bbox.xmin AS xmin, bbox.xmax AS xmax,
    bbox.ymin AS ymin, bbox.ymax AS ymax,
    ({tmin})::DATE AS dt
"""


def _norm(col: str, is_tz: bool) -> str:
    if is_tz:
        return f"({col} AT TIME ZONE '{SOURCE_TZ}') AT TIME ZONE 'UTC'"
    return f"{col} AT TIME ZONE 'UTC'"


def _project(is_tz: bool) -> str:
    return PROJECT.format(tmin=_norm("start_time", is_tz), tmax=_norm("end_time", is_tz))


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{MOBILITYDUCK_EXT}';")
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SET TimeZone='UTC';")
    if s3io.is_s3(TRIPS_DEST):
        s3io.attach_s3(con)            # so COPY can write trips straight to MinIO
    return con


def project_layout(con: duckdb.DuckDBPyConnection, ls) -> tuple[int, int, int]:
    src_root = ls.root                 # source layout stays LOCAL (the build input)
    out_root = trips_root(ls)          # local dir or s3:// prefix
    on_s3 = s3io.is_s3(out_root)
    files = sorted(glob((src_root / "**" / "*.parquet").as_posix(), recursive=True))
    if on_s3:
        s3io.clear_prefix(out_root)    # overwrite (no per-file resume on object storage)
    src_bytes = dst_bytes = done = 0
    select = None
    for f in files:
        if select is None:
            ty = con.execute(
                f"SELECT typeof(start_time) FROM read_parquet('{f}') LIMIT 1"
            ).fetchone()[0]
            select = _project("TIME ZONE" in ty)
        rel = Path(f).relative_to(src_root).as_posix()
        src_bytes += os.path.getsize(f)
        if on_s3:
            con.execute(
                f"COPY (SELECT {select} FROM read_parquet('{f}')) "
                f"TO '{out_root}/{rel}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            done += 1
            continue
        dst = Path(out_root) / rel
        if dst.exists():
            dst_bytes += os.path.getsize(dst)
            done += 1
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_suffix(".parquet.tmp")
        con.execute(
            f"COPY (SELECT {select} FROM read_parquet('{f}')) "
            f"TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        os.replace(tmp, dst)
        dst_bytes += os.path.getsize(dst)
        done += 1
    return len(files), src_bytes, dst_bytes


def main() -> None:
    only = set(sys.argv[1:])  
    con = _con()
    tot_src = tot_dst = 0
    for ls in layouts(names=only or None):
        if only and ls.key not in only:
            continue
        n, sb, db = project_layout(con, ls)
        tot_src += sb
        tot_dst += db
        pct = 100 * db / sb if sb else 0
        print(f"  {ls.key:<12} {n:>5} files  {sb/1e6:8.1f} -> {db/1e6:8.1f} MB "
              f"({pct:5.1f}%)  -> {trips_root(ls)}", flush=True)
    print(f"TOTAL  {tot_src/1e9:.2f} -> {tot_dst/1e9:.2f} GB "
          f"({100*tot_dst/tot_src:.1f}% of original)")

if __name__ == "__main__":
    main()
