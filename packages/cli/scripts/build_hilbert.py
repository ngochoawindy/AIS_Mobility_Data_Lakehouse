"""Hilbert-curve-ordered layout (LH) from L0 segments, both granularities.
Assign each segment a Hilbert bucket via precomputed quantile cut-points (no global sort
of the heavy traj blobs), then partition into files. Each file covers a contiguous Hilbert
range -> tight bbox bounds -> more pruning.

  LH compact : 64 month-wide Hilbert buckets (files).
  LH daily   : per day x 8 Hilbert buckets.

Reads the flat-schema L0 trips projection under config.TRIPS_DEST (local `data/trips/…` by
default, or s3://warehouse/… when TRIPS_DEST is set — then reads L0 from and writes LH to
object storage directly). Output roots override via $LH_OUT_COMPACT / $LH_OUT_DAILY (e.g. a
temp dir to measure build time). Prints `total_s` for the build-cost figure.
"""
import bisect, os, shutil, sys, time
from glob import glob
from pathlib import Path
import duckdb

sys.path.insert(0, ".")
from bench import s3io
from bench.config import TRIPS_DEST

N_COMPACT = 64
N_DAILY = 8
_T = TRIPS_DEST.rstrip("/")
L0_GLOB = f"{_T}/L0/L0/**/*.parquet"                       # DuckDB glob (local or s3://)
CEN = "(xmin+xmax)/2.0, (ymin+ymax)/2.0"
OUT_COMPACT = os.getenv("LH_OUT_COMPACT", f"{_T}/layout_compact/LH")
OUT_DAILY = os.getenv("LH_OUT_DAILY", f"{_T}/layouts_daily/LH")


def xy2d(n, x, y):
    d = 0; s = n // 2
    while s > 0:
        rx = 1 if (x & s) > 0 else 0
        ry = 1 if (y & s) > 0 else 0
        d += s * s * ((3 * rx) ^ ry)
        if ry == 0:
            if rx == 1:
                x = s - 1 - x; y = s - 1 - y
            x, y = y, x
        s //= 2
    return d


def _reset(out):                                           # overwrite the output root
    if s3io.is_s3(out):
        s3io.clear_prefix(out)
    else:
        p = Path(out); shutil.rmtree(p, ignore_errors=True); p.mkdir(parents=True)


def _nfiles(out):
    return len(s3io.list_parquet(out)) if s3io.is_s3(out) \
        else len(glob(f"{out}/**/*.parquet", recursive=True))


Path(".tmp/duckdb_spill").mkdir(parents=True, exist_ok=True)
c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
for s in ("SET memory_limit='10GB'", "SET temp_directory='.tmp/duckdb_spill'",
          "SET preserve_insertion_order=false", "SET threads=2"):
    c.execute(s)
if any(s3io.is_s3(p) for p in (TRIPS_DEST, OUT_COMPACT, OUT_DAILY)):
    s3io.attach_s3(c)

_t0 = time.perf_counter()
x0, x1, y0, y1 = c.execute(f"""
    SELECT quantile_cont((xmin+xmax)/2, 0.005),
           quantile_cont((xmin+xmax)/2, 0.995),
           quantile_cont((ymin+ymax)/2, 0.005),
           quantile_cont((ymin+ymax)/2, 0.995)
    FROM read_parquet('{L0_GLOB}')""").fetchone()
print(f"hilbert extent x:[{x0:.0f},{x1:.0f}] y:[{y0:.0f},{y1:.0f}]")
N = 1 << 16

def hkey(cx, cy):
    ix = min(N - 1, max(0, int((cx - x0) / (x1 - x0) * (N - 1))))
    iy = min(N - 1, max(0, int((cy - y0) / (y1 - y0) * (N - 1))))
    return xy2d(N, ix, iy)

c.create_function("hkey", hkey, ["DOUBLE", "DOUBLE"], "BIGINT")

# Hilbert quantile cut-points (computed on the lightweight key only — no sort of blobs)
qs = [i / N_COMPACT for i in range(1, N_COMPACT)]
cuts = c.execute(f"SELECT quantile_cont(hkey({CEN}), {qs}) FROM read_parquet('{L0_GLOB}')").fetchone()[0]
def binof(h): return bisect.bisect_right(cuts, h) + 1
c.create_function("binof", binof, ["BIGINT"], "INTEGER")

# --- compact: 64 Hilbert buckets -------------------------------------------- #
_reset(OUT_COMPACT)
c.execute(f"""
    COPY (SELECT *, binof(hkey({CEN})) AS hbucket FROM read_parquet('{L0_GLOB}'))
    TO '{OUT_COMPACT}' (FORMAT PARQUET, PARTITION_BY (hbucket),
         COMPRESSION ZSTD, FILENAME_PATTERN 'data_{{i}}', OVERWRITE_OR_IGNORE)""")

# --- daily: per day x 8 coarse Hilbert buckets ------------------------------ #
_reset(OUT_DAILY)
step = -(-N_COMPACT // N_DAILY)   # ceil
c.execute(f"""
    COPY (SELECT * EXCLUDE (_b), ((_b - 1) / {step} + 1)::INTEGER AS hbucket
          FROM (SELECT *, binof(hkey({CEN})) AS _b FROM read_parquet('{L0_GLOB}')))
    TO '{OUT_DAILY}' (FORMAT PARQUET, PARTITION_BY (dt, hbucket),
         COMPRESSION ZSTD, FILENAME_PATTERN 'data_{{i}}', OVERWRITE_OR_IGNORE)""")

print(f"[LH] built compact+daily total_s={time.perf_counter() - _t0:.3f}")

for tag, p, grp in (("compact", OUT_COMPACT, "hbucket"), ("daily", OUT_DAILY, "dt, hbucket")):
    rows, mw, mh = c.execute(f"""
        SELECT (SELECT count(*) FROM read_parquet('{p}/**/*.parquet', hive_partitioning=true)),
               avg(fw), avg(fh) FROM (
          SELECT max(xmax)-min(xmin) fw, max(ymax)-min(ymin) fh
          FROM read_parquet('{p}/**/*.parquet', hive_partitioning=true) GROUP BY {grp})""").fetchone()
    print(f"LH {tag:7}: {_nfiles(p):>5} files, {rows:,} rows, "
          f"mean file bbox {mw/1000:5.0f} x {mh/1000:5.0f} km")
print(f"(dense extent ~{(x1-x0)/1000:.0f} x {(y1-y0)/1000:.0f} km)")
