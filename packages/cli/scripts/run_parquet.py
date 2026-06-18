from __future__ import annotations

import argparse
import os
import re
import statistics
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, ".")
from bench.config import MOBILITYDUCK_EXT, RESULTS, layouts
from run_queries import WINDOWS, files_read, run_pipeline, set_window, statements, trimmed

WAREHOUSE = "s3://warehouse/trips"
PARQUET_DIR = Path("queries/parquet")
BOOK_QUERIES = ["q57", "q58", "q59", "q510", "q511"]


def connect() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.execute("INSTALL httpfs; LOAD httpfs;")
    c.execute(f"LOAD '{MOBILITYDUCK_EXT}';")
    c.execute("INSTALL spatial; LOAD spatial;")
    ep = os.getenv("ICEBERG_CATALOG_PROP__S3__ENDPOINT",
                   "http://localhost:9000").removeprefix("http://")
    c.execute(f"CREATE SECRET (TYPE S3, KEY_ID '{os.getenv('AWS_ACCESS_KEY_ID','admin')}', "
              f"SECRET '{os.getenv('AWS_SECRET_ACCESS_KEY','password')}', ENDPOINT '{ep}', "
              f"URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');")
    return c


def measure_files(con, stmts: list[str]) -> int | None:
    """Sum 'Total Files Read' over the read_parquet scan statement(s) (a query may scan
    trips more than once; handles a CREATE ... AS SELECT prefix). Also warms the scan."""
    opened = None
    for s in stmts:
        if "read_parquet" not in s.lower():
            continue
        sel = s
        if s.lstrip().upper().startswith("CREATE"):
            m = re.search(r"\bAS\b\s+(SELECT|WITH)", s, re.IGNORECASE)
            if m:
                sel = s[m.start(1):]
        plan = con.execute(f"EXPLAIN ANALYZE {sel}").fetchall()[0][1]
        fr = files_read(plan)
        if fr is not None:
            opened = (opened or 0) + fr
    return opened


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument("--sels", default="hour,day,week")
    ap.add_argument("--layouts", default="", help="L0,L2s,... (default: all)")
    ap.add_argument("--kinds", default="book,app", help="book,app")
    args = ap.parse_args()

    sels = [s.strip() for s in args.sels.split(",") if s.strip()]
    kinds = {s.strip() for s in args.kinds.split(",")}
    names = {s.strip() for s in args.layouts.split(",")} if args.layouts else None

    book_text = {q: (PARQUET_DIR / f"{q}.sql").read_text() for q in BOOK_QUERIES}
    app_text = {p.stem.removeprefix("app_"): p.read_text()
                for p in sorted(PARQUET_DIR.glob("app_*.sql"))}

    con = connect()
    sub = {ls.key: ls.subdir for ls in layouts()}
    book_rows, app_rows = [], []
    for ls in layouts(names=names):
        key = ls.key
        glob = f"{WAREHOUSE}/{sub[key]}/**/*.parquet"
        ntot = con.execute(f"SELECT count(*) FROM glob('{glob}')").fetchone()[0]
        if not ntot:
            print(f"\n--- skip {key}: no parquet at {glob} ---", flush=True)
            continue
        con.execute(f"SET VARIABLE trips_glob = '{glob}'")
        print(f"\n=== {key} ({ls.desc}) · files_total={ntot} ===", flush=True)

        for sel in sels:
            set_window(con, sel)
            if "book" in kinds:
                for q, raw in book_text.items():
                    stmts = statements(raw)
                    opened = measure_files(con, stmts)
                    times, ans = [], None
                    for _ in range(args.iters):
                        t = time.perf_counter()
                        ans = run_pipeline(con, stmts)
                        times.append(time.perf_counter() - t)
                    book_rows.append({
                        "system": "duckdb_parquet",
                        "layout": ls.name, "gran": ls.gran, "query": q,
                        "selectivity": sel, "span": WINDOWS[sel][5],
                        "files_total": ntot, "files_read": opened,
                        "files_pct": round(100 * opened / ntot, 1) if opened else None,
                        "n_iters": args.iters,
                        "runtime_trimmed_s": trimmed(times),
                        "runtime_median_s": statistics.median(times),
                        "runtime_min_s": min(times),
                        "answer": ans[0] if ans else None,
                    })
                    print(f"  book {q:<6} {sel:<5} files {opened}/{ntot}  "
                          f"ans={ans[0] if ans else None}  {trimmed(times):.3f}s", flush=True)

            if "app" in kinds:
                for q, raw in app_text.items():
                    stmts = statements(raw)
                    opened = measure_files(con, stmts)
                    times, ans = [], None
                    for _ in range(args.iters):
                        t = time.perf_counter()
                        ans = run_pipeline(con, stmts)
                        times.append(time.perf_counter() - t)
                    app_rows.append({
                        "query": q, "layout": key, "selectivity": sel,
                        "files_total": ntot, "files_opened": opened,
                        "files_pct": round(100 * opened / ntot, 1) if opened else None,
                        "answer": ans[0] if ans else None,
                        "runtime_s": round(trimmed(times), 3),
                    })
                    print(f"  app  {q:<22} {sel:<5} files {opened}/{ntot}  "
                          f"ans={ans[0] if ans else None}  {trimmed(times):.3f}s", flush=True)
    con.close()

    RESULTS.mkdir(exist_ok=True)
    if "book" in kinds:
        p = RESULTS / "parquet_all.csv"
        pd.DataFrame(book_rows).to_csv(p, index=False)
        print(f"\nwrote {p}  ({len(book_rows)} rows)")
    if "app" in kinds:
        p = RESULTS / "parquet_app_compare.csv"
        pd.DataFrame(app_rows).to_csv(p, index=False)
        print(f"wrote {p}  ({len(app_rows)} rows)")


if __name__ == "__main__":
    main()
