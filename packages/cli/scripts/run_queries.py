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
from bench.iceberg_build import NAMESPACE, catalog, table_name

BOOK_DIR = Path("queries/iceberg")
APP_DIR = Path("queries/app")
BOOK_QUERIES = ["q57", "q58", "q59", "q510", "q511"]

WINDOWS: dict[str, tuple[str, str, str, str, str, str]] = {
    "hour": ("2026-01-15 08:00:00", "2026-01-15 09:00:00",
             "2026-01-15 08:30:00", "2026-01-15", "2026-01-15", "1 hour"),
    "day":  ("2026-01-15 08:00:00", "2026-01-16 08:00:00",
             "2026-01-15 20:00:00", "2026-01-15", "2026-01-16", "1 day"),
    "week": ("2026-01-15 08:00:00", "2026-01-22 08:00:00",
             "2026-01-18 20:00:00", "2026-01-15", "2026-01-22", "1 week"),
    "month": ("2026-01-01 00:00:00", "2026-02-01 00:00:00",
              "2026-01-16 00:00:00", "2026-01-01", "2026-01-31", "1 month"),
}


def set_window(con, sel: str) -> None:
    t0, t1, tmid, d0, d1, _ = WINDOWS[sel]
    con.execute(f"SET VARIABLE t0   = TIMESTAMP '{t0}'")
    con.execute(f"SET VARIABLE t1   = TIMESTAMP '{t1}'")
    con.execute(f"SET VARIABLE tmid = TIMESTAMP '{tmid}'")
    con.execute(f"SET VARIABLE d0   = DATE '{d0}'")
    con.execute(f"SET VARIABLE d1   = DATE '{d1}'")


def connect() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.execute("INSTALL iceberg; LOAD iceberg;")
    c.execute("INSTALL httpfs; LOAD httpfs;")
    c.execute(f"LOAD '{MOBILITYDUCK_EXT}';")
    c.execute("INSTALL spatial; LOAD spatial;")
    ep = os.getenv("ICEBERG_CATALOG_PROP__S3__ENDPOINT",
                   "http://localhost:9000").removeprefix("http://")
    c.execute(f"CREATE SECRET (TYPE S3, KEY_ID '{os.getenv('AWS_ACCESS_KEY_ID','admin')}', "
              f"SECRET '{os.getenv('AWS_SECRET_ACCESS_KEY','password')}', ENDPOINT '{ep}', "
              f"URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');")
    rest = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")
    c.execute(f"ATTACH '' AS lake (TYPE ICEBERG, ENDPOINT '{rest}', "
              f"AUTHORIZATION_TYPE 'none');")
    return c


def statements(text: str) -> list[str]:
    body = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--"))
    return [s.strip() for s in body.split(";") if s.strip()]


def files_read(plan: str) -> int | None:
    """Sum every 'Total Files Read: N' in an EXPLAIN ANALYZE plan (a query may scan
    `trips` more than once, e.g. q57's two per-port branches)."""
    total, found = 0, False
    lines = plan.splitlines()
    for i, l in enumerate(lines):
        if "Total Files Read" in l:
            for cand in [l] + lines[i + 1:i + 4]:
                m = re.search(r"(\d[\d,]*)", cand)
                if m:
                    total += int(m.group(1).replace(",", ""))
                    found = True
                    break
    return total if found else None


def trimmed(xs: list[float]) -> float:
    if len(xs) >= 5:
        s = sorted(xs)[1:-1]
        return statistics.mean(s)
    return statistics.median(xs)


def measure_files(con, stmts: list[str]) -> int | None:
    """EXPLAIN ANALYZE the trips-touching statement(s); returns files opened. Doubles
    as a warm-up (it executes the scan)."""
    opened = None
    for s in stmts:
        if "from trips" not in s.lower():
            continue
        sel = s
        if s.lstrip().upper().startswith("CREATE"):  # drop CREATE ... AS prefix
            m = re.search(r"\bAS\b\s+(SELECT|WITH)", s, re.IGNORECASE)
            if m:
                sel = s[m.start(1):]
        plan = con.execute(f"EXPLAIN ANALYZE {sel}").fetchall()[0][1]
        fr = files_read(plan)
        if fr is not None:
            opened = (opened or 0) + fr
    return opened


def run_pipeline(con, stmts: list[str]):
    for s in stmts[:-1]:
        con.execute(s)
    return con.execute(stmts[-1]).fetchone()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument("--sels", default="hour,day,week")
    ap.add_argument("--layouts", default="", help="L0,L2,... (default: all)")
    ap.add_argument("--kinds", default="book,app", help="book,app")
    args = ap.parse_args()

    if os.getenv("ICEBERG_CATALOG_TYPE") != "rest":
        sys.exit("Set REST env first:  source iceberg_rest/env.rest")

    sels = [s.strip() for s in args.sels.split(",") if s.strip()]
    kinds = {s.strip() for s in args.kinds.split(",")}
    names = {s.strip() for s in args.layouts.split(",")} if args.layouts else None

    book_text = {q: (BOOK_DIR / f"{q}.sql").read_text() for q in BOOK_QUERIES}
    app_text = {p.stem: p.read_text() for p in sorted(APP_DIR.glob("*.sql"))}
    cat = catalog()

    book_rows, app_rows = [], []
    for ls in layouts(names=names):
        try:
            ntot = len(list(cat.load_table(table_name(ls)).scan().plan_files()))
        except Exception as e:  # layout not registered in the catalog -> skip
            print(f"\n--- skip {ls.key}: {type(e).__name__} ({e}) ---", flush=True)
            continue
        con = connect()
        con.execute(f"CREATE OR REPLACE VIEW trips AS "
                    f"SELECT * FROM lake.{NAMESPACE}.{ls.key}")
        print(f"\n=== {ls.key} ({ls.desc}) · files_total={ntot} ===", flush=True)

        for sel in sels:
            set_window(con, sel)   # selectivity = window width, via session vars
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
                        "system": "duckdb_iceberg_rest",
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
                          f"ans={ans[0] if ans else None}  "
                          f"{trimmed(times):.3f}s", flush=True)

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
                        "query": q, "layout": ls.key, "selectivity": sel,
                        "files_total": ntot, "files_opened": opened,
                        "files_pct": round(100 * opened / ntot, 1) if opened else None,
                        "answer": ans[0] if ans else None,
                        "runtime_s": round(trimmed(times), 3),
                    })
                    print(f"  app  {q:<22} {sel:<5} files {opened}/{ntot}  "
                          f"ans={ans[0] if ans else None}  "
                          f"{trimmed(times):.3f}s", flush=True)
        con.close()

    RESULTS.mkdir(exist_ok=True)
    if "book" in kinds:
        p = RESULTS / "rest_all.csv"
        pd.DataFrame(book_rows).to_csv(p, index=False)
        print(f"\nwrote {p}  ({len(book_rows)} rows)")
    if "app" in kinds:
        p = RESULTS / "app_compare.csv"
        pd.DataFrame(app_rows).to_csv(p, index=False)
        print(f"wrote {p}  ({len(app_rows)} rows)")


if __name__ == "__main__":
    main()
