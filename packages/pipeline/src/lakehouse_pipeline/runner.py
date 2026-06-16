"""Per-day L0 build + per-month L1..L4 build, each in a fresh DuckDB connection."""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from time import perf_counter

from lakehouse_pipeline._duckdb import open_pipeline_conn
from lakehouse_pipeline._params import PipelineParams
from lakehouse_pipeline.base import build_base_segments_from_raw
from lakehouse_pipeline.layouts import (
    _month_dir,
    l0_month_glob,
    list_l0_days,
    register_base_segments_view,
    write_l0,
    write_l1_hash,
    write_l2_spacesplit,
    write_l3_mest,
    write_l4_timesplit,
)

LAYOUTS = ("L0", "L1", "L2", "L3", "L4")


@dataclass(frozen=True)
class DuckdbRuntime:
    extension_path: str | None
    extension_name: str = "mobilityduck"
    allow_unsigned_extensions: bool = True
    memory_limit: str | None = None
    threads: int = 2
    max_temp_directory_size: str | None = None
    spill_dir: Path | None = None


@dataclass(frozen=True)
class LayoutParams:
    num_shards: int = 16
    tile_size_m: float = 500.0
    region_size_m: float = 50_000.0
    segs_per_box: int = 16
    time_bin: str = "1 hour"
    granularity: str = "monthly"


def _spill_for(runtime: DuckdbRuntime, fallback: Path) -> Path:
    return runtime.spill_dir or fallback


def build_l0_day(
    *,
    raw_parquet: Path,
    day: date,
    l0_root: Path,
    params: PipelineParams,
    runtime: DuckdbRuntime,
) -> tuple[Path, int]:
    spill = _spill_for(runtime, l0_root / ".spill")
    t0 = perf_counter()
    with open_pipeline_conn(
        spill_dir=spill,
        extension_path=runtime.extension_path,
        extension_name=runtime.extension_name,
        allow_unsigned_extensions=runtime.allow_unsigned_extensions,
        memory_limit=runtime.memory_limit,
        threads=runtime.threads,
        max_temp_directory_size=runtime.max_temp_directory_size,
    ) as con:
        raw_count, base_count = build_base_segments_from_raw(
            con, raw_parquet, params=params
        )
        out, rows = write_l0(con, root=l0_root, day=day)
    print(
        f"[L0] day={day} raw={raw_count} base_segs={base_count} "
        f"rows={rows} out={out.relative_to(l0_root).as_posix()} "
        f"total_s={perf_counter() - t0:.3f}"
    )
    return out, rows


def build_layout_month(
    *,
    layout: str,
    year: int,
    month: int,
    l0_root: Path,
    layout_root: Path,
    params: PipelineParams,
    layout_params: LayoutParams,
    runtime: DuckdbRuntime,
) -> int:
    if layout not in LAYOUTS or layout == "L0":
        raise ValueError(f"layout must be one of L1..L4, got {layout!r}")

    glob = l0_month_glob(l0_root, year, month)
    days = list_l0_days(l0_root, year, month)
    spill = _spill_for(runtime, layout_root / ".spill")
    t0 = perf_counter()
    with open_pipeline_conn(
        spill_dir=spill,
        extension_path=runtime.extension_path,
        extension_name=runtime.extension_name,
        allow_unsigned_extensions=runtime.allow_unsigned_extensions,
        memory_limit=runtime.memory_limit,
        threads=runtime.threads,
        max_temp_directory_size=runtime.max_temp_directory_size,
    ) as con:
        base_rows = register_base_segments_view(con, glob)
        if base_rows == 0:
            print(f"[{layout}] {year}-{month:02d}: empty L0 under {glob}")
            return 0

        if layout == "L1":
            rows = write_l1_hash(
                con, root=layout_root, year=year, month=month,
                num_shards=layout_params.num_shards,
                l0_glob=glob,
                l0_days=days,
                granularity=layout_params.granularity,
            )
        elif layout == "L2":
            def _l2_cell_conn():
                return open_pipeline_conn(
                    spill_dir=spill,
                    extension_path=runtime.extension_path,
                    extension_name=runtime.extension_name,
                    allow_unsigned_extensions=runtime.allow_unsigned_extensions,
                    memory_limit=runtime.memory_limit,
                    threads=runtime.threads,
                    max_temp_directory_size=runtime.max_temp_directory_size,
                )
            rows = write_l2_spacesplit(
                con, root=layout_root, year=year, month=month,
                tile_size_m=layout_params.tile_size_m,
                region_size_m=layout_params.region_size_m,
                params=params, l0_days=days, l0_glob=glob,
                cell_conn_factory=_l2_cell_conn,
                granularity=layout_params.granularity,
            )
        elif layout == "L3":
            def _l3_cell_conn():
                return open_pipeline_conn(
                    spill_dir=spill,
                    extension_path=runtime.extension_path,
                    extension_name=runtime.extension_name,
                    allow_unsigned_extensions=runtime.allow_unsigned_extensions,
                    memory_limit=runtime.memory_limit,
                    threads=runtime.threads,
                    max_temp_directory_size=runtime.max_temp_directory_size,
                )
            rows = write_l3_mest(
                con, root=layout_root, year=year, month=month,
                segs_per_box=layout_params.segs_per_box,
                region_size_m=layout_params.region_size_m,
                params=params, l0_days=days, l0_glob=glob,
                cell_conn_factory=_l3_cell_conn,
                granularity=layout_params.granularity,
            )
        elif layout == "L4":
            def _l4_cell_conn():
                return open_pipeline_conn(
                    spill_dir=spill,
                    extension_path=runtime.extension_path,
                    extension_name=runtime.extension_name,
                    allow_unsigned_extensions=runtime.allow_unsigned_extensions,
                    memory_limit=runtime.memory_limit,
                    threads=runtime.threads,
                    max_temp_directory_size=runtime.max_temp_directory_size,
                )
            rows = write_l4_timesplit(
                con, root=layout_root, year=year, month=month,
                time_bin=layout_params.time_bin, params=params,
                l0_days=days,
                cell_conn_factory=_l4_cell_conn,
                granularity=layout_params.granularity,
            )
        else:  # pragma: no cover
            raise AssertionError(layout)

    print(
        f"[{layout}] {year}-{month:02d} base_rows={base_rows} rows={rows} "
        f"out={_month_dir(layout_root, layout, year, month).as_posix()} "
        f"total_s={perf_counter() - t0:.3f}"
    )
    return rows


def parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def parse_month(s: str) -> tuple[int, int]:
    dt = datetime.strptime(s, "%Y-%m")
    return dt.year, dt.month


def raw_path_for_day(raw_dir: Path, day: date) -> Path:
    return raw_dir / f"aisdk-{day.isoformat()}.parquet"
