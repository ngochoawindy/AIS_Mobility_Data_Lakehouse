from datetime import date, timedelta
from pathlib import Path
from typing import Annotated

from rich import print
from typer import Argument, Option, Typer

from lakehouse.settings import Settings
from lakehouse_pipeline.ingest import (
    ingest_dma_day,
    ingest_dma_range,
    ingest_dma_source,
)
from lakehouse_pipeline.compact import compact_daily_to_monthly
from lakehouse_pipeline.runner import (
    DuckdbRuntime,
    LAYOUTS,
    LayoutParams,
    build_l0_day,
    build_layout_month,
    parse_day,
    parse_month,
    raw_path_for_day,
)


cli = Typer()


@cli.command("ingest")
def ingest(
    source: Annotated[str, Argument(help="DMA URL or local CSV/ZIP/Parquet path")],
    filename: Annotated[str | None, Option(help="Override output filename")] = None,
    keep_zip: Annotated[bool, Option()] = False,
    keep_csv: Annotated[bool, Option()] = False,
) -> None:
    settings = Settings.create()
    out = ingest_dma_source(
        source, settings.pipeline.raw_dir,
        filename=filename, keep_zip=keep_zip, keep_csv=keep_csv,
    )
    print(f"[green]Ingest complete:[/green] {out}")


@cli.command("ingest-day")
def ingest_day(
    day: Annotated[str, Argument(help="YYYY-MM-DD (becomes aisdk-YYYY-MM-DD.zip)")],
    keep_zip: Annotated[bool, Option()] = False,
    keep_csv: Annotated[bool, Option()] = False,
) -> None:
    settings = Settings.create()
    out = ingest_dma_day(
        parse_day(day), settings.pipeline.raw_dir,
        keep_zip=keep_zip, keep_csv=keep_csv,
    )
    print(f"[green]Ingested:[/green] {out}")


@cli.command("ingest-range")
def ingest_range(
    start_day: Annotated[str, Argument(help="Start YYYY-MM-DD (inclusive)")],
    end_day: Annotated[str, Argument(help="End YYYY-MM-DD (inclusive)")],
    keep_zip: Annotated[bool, Option()] = False,
    keep_csv: Annotated[bool, Option()] = False,
) -> None:
    settings = Settings.create()
    outs = ingest_dma_range(
        parse_day(start_day), parse_day(end_day), settings.pipeline.raw_dir,
        keep_zip=keep_zip, keep_csv=keep_csv,
    )
    for o in outs:
        print(f"[green]Ingested:[/green] {o}")
    print(f"[green]Total days:[/green] {len(outs)}")


def _daterange(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end_day must be on or after start_day")
    out: list[date] = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def _resolve_l0_days(
    *, day: str | None, start_day: str | None, end_day: str | None, raw_dir: Path,
) -> list[date]:
    if day:
        return [parse_day(day)]
    if start_day and end_day:
        return _daterange(parse_day(start_day), parse_day(end_day))
    days = []
    for p in sorted(raw_dir.glob("aisdk-*.parquet")):
        stem = p.stem.removeprefix("aisdk-")
        try:
            days.append(parse_day(stem))
        except ValueError:
            continue
    if not days:
        raise SystemExit(
            f"No --day, no --start-day/--end-day, and no aisdk-*.parquet under {raw_dir}"
        )
    return days


def _runtime(settings: Settings, memory_limit_override: str | None) -> DuckdbRuntime:
    return DuckdbRuntime(
        extension_path=settings.mobilityduck.extension_path,
        extension_name=settings.mobilityduck.extension_name,
        allow_unsigned_extensions=settings.mobilityduck.allow_unsigned_extensions,
        memory_limit=memory_limit_override or settings.pipeline.duckdb_memory_limit,
        threads=settings.pipeline.duckdb_threads,
        max_temp_directory_size=settings.pipeline.duckdb_max_temp_directory_size,
        spill_dir=settings.pipeline.duckdb_temp_directory,
    )


@cli.command("build-l0")
def build_l0(
    day: Annotated[str | None, Option(help="Single day YYYY-MM-DD")] = None,
    start_day: Annotated[str | None, Option(help="Range start YYYY-MM-DD")] = None,
    end_day: Annotated[str | None, Option(help="Range end YYYY-MM-DD")] = None,
    l0_dir: Annotated[str | None, Option(help="Override settings.pipeline.l0_dir")] = None,
    memory_limit: Annotated[str | None, Option(help="DuckDB memory_limit, e.g. '10GB'")] = None,
) -> None:
    settings = Settings.create()
    raw_dir = settings.pipeline.raw_dir
    l0_root = Path(l0_dir) if l0_dir else settings.pipeline.l0_dir
    params = settings.pipeline.to_params()
    runtime = _runtime(settings, memory_limit)

    days = _resolve_l0_days(day=day, start_day=start_day, end_day=end_day, raw_dir=raw_dir)
    print(f"[cyan]Building L0 for {len(days)} day(s) under {l0_root}[/cyan]")
    for d in days:
        raw = raw_path_for_day(raw_dir, d)
        if not raw.exists():
            print(f"[yellow]skip {d}: missing {raw.name}[/yellow]")
            continue
        try:
            build_l0_day(
                raw_parquet=raw, day=d, l0_root=l0_root,
                params=params, runtime=runtime,
            )
        except Exception as e:  # noqa: BLE001
            print(f"[red]L0 FAILED {d}: {type(e).__name__}: {e}[/red]")


@cli.command("build-layout")
def build_layout(
    layout: Annotated[str, Argument(help="One of L1, L2, L3, L4")],
    month: Annotated[str, Option(help="Month YYYY-MM (required)")],
    l0_dir: Annotated[str | None, Option(help="Override settings.pipeline.l0_dir")] = None,
    layout_dir: Annotated[str | None, Option(help="Override settings.pipeline.layout_dir")] = None,
    num_shards: Annotated[int, Option(help="L1 hash buckets")] = 16,
    tile_size_m: Annotated[float, Option(help="L2 spaceSplit tile size, metres")] = 500.0,
    region_size_m: Annotated[float, Option(help="L2/L3 region bucket size, metres")] = 50_000.0,
    segs_per_box: Annotated[int, Option(help="L3 MEST segs-per-box")] = 16,
    time_bin: Annotated[str, Option(help="L4 time bin (DuckDB INTERVAL syntax)")] = "1 hour",
    granularity: Annotated[str, Option(help="L1/L2/L3/L4 output granularity: 'monthly' (one file per partition-month) or 'daily' (one file per partition-day, supports incremental ingest)")] = "monthly",
    memory_limit: Annotated[str | None, Option(help="DuckDB memory_limit, e.g. '10GB'")] = None,
) -> None:
    if layout not in LAYOUTS or layout == "L0":
        raise SystemExit(f"layout must be one of L1..L4, got {layout!r}")
    year, mo = parse_month(month)

    settings = Settings.create()
    l0_root = Path(l0_dir) if l0_dir else settings.pipeline.l0_dir
    layout_root = Path(layout_dir) if layout_dir else settings.pipeline.layout_dir
    params = settings.pipeline.to_params()
    runtime = _runtime(settings, memory_limit)
    if granularity not in ("monthly", "daily"):
        raise SystemExit(
            f"--granularity must be 'monthly' or 'daily', got {granularity!r}"
        )
    layout_params = LayoutParams(
        num_shards=num_shards,
        tile_size_m=tile_size_m,
        region_size_m=region_size_m,
        segs_per_box=segs_per_box,
        time_bin=time_bin,
        granularity=granularity,
    )

    print(
        f"[cyan]Building {layout} for {year}-{mo:02d} from L0={l0_root} "
        f"→ {layout_root}[/cyan]"
    )
    try:
        build_layout_month(
            layout=layout, year=year, month=mo,
            l0_root=l0_root, layout_root=layout_root,
            params=params, layout_params=layout_params, runtime=runtime,
        )
    except Exception as e:  # noqa: BLE001
        print(f"[red]{layout} FAILED {year}-{mo:02d}: {type(e).__name__}: {e}[/red]")
        raise


@cli.command("compact-layout")
def compact_layout(
    layout: Annotated[str, Argument(help="One of L1, L2, L3, L4")],
    month: Annotated[str, Option(help="Month YYYY-MM (required)")],
    src_dir: Annotated[str, Option(help="Daily layout root (the --layout-dir used at build time)")],
    dst_dir: Annotated[str, Option(help="Where to write the compacted monthly output")],
    delete_sources: Annotated[bool, Option(help="Remove per-day source files after a successful concat")] = False,
    memory_limit: Annotated[str | None, Option(help="DuckDB memory_limit, e.g. '10GB'")] = None,
) -> None:
    if layout not in ("L1", "L2", "L3", "L4"):
        raise SystemExit(f"layout must be one of L1, L2, L3, L4, got {layout!r}")
    year, mo = parse_month(month)

    settings = Settings.create()
    src_root = Path(src_dir)
    dst_root = Path(dst_dir)
    runtime = _runtime(settings, memory_limit)

    print(f"[cyan]Compacting {layout} for {year}-{mo:02d}: "
          f"{src_root} → {dst_root}[/cyan]")
    try:
        compact_daily_to_monthly(
            layout=layout, year=year, month=mo,
            src_root=src_root, dst_root=dst_root,
            runtime=runtime, delete_sources=delete_sources,
        )
    except Exception as e:  # noqa: BLE001
        print(f"[red]{layout} compact FAILED {year}-{mo:02d}: "
              f"{type(e).__name__}: {e}[/red]")
        raise
