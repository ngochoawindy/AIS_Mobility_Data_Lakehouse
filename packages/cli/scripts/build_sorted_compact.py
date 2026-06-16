from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, ".")
from bench import s3io
from bench.config import TRIPS_DEST

COMPACT = f"{TRIPS_DEST.rstrip('/')}/layout_compact"
# source compact layout name -> sorted-compact output name
VARIANT = {"L1": "L1s", "L2": "L2s", "L3": "L3s", "L4": "L4s", "LH": "LHs"}


def _list_parquet(root: str) -> list[str]:
    if s3io.is_s3(root):
        return s3io.list_parquet(root)
    return [p.as_posix() for p in sorted(Path(root).rglob("*.parquet"))]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--layouts", default="L2,L3")
    ap.add_argument("--cell-m", type=float, default=5000.0,
                    help="sub-tile spatial cell size (m) for the primary sort key")
    ap.add_argument("--row-group", type=int, default=1024,
                    help="Parquet row-group size (rows) — smaller = finer pruning")
    ap.add_argument("--threads", type=int, default=4)
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute(f"SET threads={args.threads}")
    on_s3 = s3io.is_s3(COMPACT)
    if on_s3:
        s3io.attach_s3(con)
    cell, rg = args.cell_m, args.row_group

    for src_name in (s.strip() for s in args.layouts.split(",")):
        out_name = VARIANT[src_name]
        src_root = f"{COMPACT}/{src_name}"
        out_root = f"{COMPACT}/{out_name}"
        files = _list_parquet(src_root)
        if not files:
            print(f"{src_name}: no source files under {src_root}; skipped")
            continue
        if on_s3:
            s3io.clear_prefix(out_root)                       
        else:
            shutil.rmtree(out_root, ignore_errors=True)
        print(f"{src_name} -> {out_name}: {len(files)} tiles  (cell={cell:.0f}m, rg={rg})",
              flush=True)
        _t0 = time.perf_counter()
        for src in files:
            rel = src[len(src_root) + 1:]                     
            dst = f"{out_root}/{rel}"
            if not on_s3:
                Path(dst).parent.mkdir(parents=True, exist_ok=True)
            con.execute(f"""
                COPY (
                    SELECT mmsi, ship_type, segment_type, traj, tmin, tmax,
                           xmin, xmax, ymin, ymax, dt
                    FROM read_parquet('{src}')
                    ORDER BY floor(((xmin+xmax)/2)/{cell}),
                             floor(((ymin+ymax)/2)/{cell}),
                             tmin
                ) TO '{dst}'
                (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {rg})
            """)
        nrg = con.execute(f"""
            SELECT avg(num_row_groups), max(num_row_groups)
            FROM parquet_file_metadata('{out_root}/**/*.parquet')""").fetchone()
        print(f"  {out_name}: row-groups/file avg={nrg[0]:.1f} max={nrg[1]}  "
              f"total_s={time.perf_counter() - _t0:.1f}", flush=True)


if __name__ == "__main__":
    main()
