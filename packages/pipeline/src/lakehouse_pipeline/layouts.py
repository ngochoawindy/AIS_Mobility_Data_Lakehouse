"""The L0–L4 layout writers from the benchmark plan."""

import shutil
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import date
from pathlib import Path

import duckdb

CellConnFactory = Callable[[], AbstractContextManager[duckdb.DuckDBPyConnection]]

from lakehouse_pipeline._geoparquet import (
    GEOPARQUET_EXCLUDE,
    GEOPARQUET_PROJECTION,
    kv_metadata_clause,
)
from lakehouse_pipeline._params import PipelineParams
from lakehouse_pipeline._utils import _sql_str
from lakehouse_pipeline.tile import (
    mest_split_motion_sql,
    parse_time_bin_hours,
    passthrough_select_sql,
    space_split_motion_sql,
    time_split_motion_sql,
)


def _month_dir(root: Path, layout: str, year: int, month: int) -> Path:
    return root / layout / f"year={year:04d}" / f"month={month:02d}"


def _day_dir(root: Path, layout: str, day: date) -> Path:
    return root / layout / f"day={day.isoformat()}"


def l0_file(root: Path, day: date) -> Path:
    return _month_dir(root, "L0", day.year, day.month) / f"day={day.isoformat()}.parquet"


def l0_month_glob(root: Path, year: int, month: int) -> str:
    return (_month_dir(root, "L0", year, month) / "*.parquet").as_posix()


def list_l0_days(root: Path, year: int, month: int) -> list[tuple[date, Path]]:
    md = _month_dir(root, "L0", year, month)
    if not md.exists():
        return []
    out: list[tuple[date, Path]] = []
    for p in sorted(md.glob("day=*.parquet")):
        stem = p.stem.removeprefix("day=")
        try:
            out.append((date.fromisoformat(stem), p))
        except ValueError:
            continue
    return out


def _ensure_empty_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _wrap_geoparquet(inner_sql: str) -> str:
    return f"""
        SELECT
            * EXCLUDE ({GEOPARQUET_EXCLUDE}),
            {GEOPARQUET_PROJECTION}
        FROM ({inner_sql})
    """


def _concat_parquet(
    con: duckdb.DuckDBPyConnection, *, sources: list[Path], target: Path
) -> int:
    if not sources:
        return 0
    target.parent.mkdir(parents=True, exist_ok=True)
    target.unlink(missing_ok=True)
    srcs_sql = "[" + ", ".join(_sql_str(p.as_posix()) for p in sources) + "]"
    options = ["FORMAT PARQUET", "COMPRESSION ZSTD", kv_metadata_clause()]
    con.execute(
        f"""
        COPY (SELECT * FROM read_parquet({srcs_sql}))
        TO {_sql_str(target.as_posix())}
        ({', '.join(options)});
        """
    )
    return int(
        con.execute(
            f"SELECT COUNT(*) FROM read_parquet({_sql_str(target.as_posix())})"
        ).fetchone()[0]
    )


def _copy_single(
    con: duckdb.DuckDBPyConnection, *, select_sql: str, target: Path
) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    target.unlink(missing_ok=True)
    options = ["FORMAT PARQUET", "COMPRESSION ZSTD", kv_metadata_clause()]
    con.execute(
        f"""
        COPY (
            {_wrap_geoparquet(select_sql)}
        )
        TO {_sql_str(target.as_posix())}
        ({', '.join(options)});
        """
    )
    rows = int(
        con.execute(
            f"SELECT COUNT(*) FROM read_parquet({_sql_str(target.as_posix())})"
        ).fetchone()[0]
    )
    if rows == 0:
        target.unlink(missing_ok=True)
        d = target.parent
        while d.is_dir() and not any(d.iterdir()):
            d.rmdir()
            d = d.parent
    return rows


def register_base_segments_view(
    con: duckdb.DuckDBPyConnection, source: str | Path
) -> int:
    src_sql = _sql_str(source.as_posix() if isinstance(source, Path) else source)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW base_segments AS
        SELECT
            * EXCLUDE (traj_wkb, geometry, bbox),
            bbox.xmin AS bbox_min_x,
            bbox.ymin AS bbox_min_y,
            bbox.xmax AS bbox_max_x,
            bbox.ymax AS bbox_max_y,
            tgeompointFromBinary(traj_wkb) AS traj
        FROM read_parquet({src_sql}, hive_partitioning=true);
        """
    )
    return int(con.execute("SELECT COUNT(*) FROM base_segments").fetchone()[0])


def enumerate_region_cells(
    con: duckdb.DuckDBPyConnection,
    region_size_m: float,
    l0_days: list[tuple[date, Path]],
    min_motion_points: int,
) -> list[tuple[int, int]]:
    cells: set[tuple[int, int]] = set()
    for d, l0_path in l0_days:
        register_base_segments_view(con, l0_path)
        rows = con.execute(
            f"""
            SELECT DISTINCT rx, ry FROM (
                SELECT
                    floor(ST_X(sp.spaceBin) / {region_size_m})::INTEGER AS rx,
                    floor(ST_Y(sp.spaceBin) / {region_size_m})::INTEGER AS ry
                FROM (
                    -- Skip rows that would crash MEOS spaceSplit with
                    -- "Instant sequence must have inclusive bounds":
                    -- zero spatial extent or zero temporal extent.
                    SELECT *
                    FROM base_segments
                    WHERE segment_type = 'in motion'
                      AND duration_s > 0
                      AND (bbox_max_x > bbox_min_x OR bbox_max_y > bbox_min_y)
                ) bs,
                LATERAL spaceSplit(
                    bs.traj,
                    {region_size_m}, {region_size_m}, 1.0,
                    ST_Point(0, 0), FALSE
                ) sp(spaceBin, tpoint)
                WHERE numInstants(sp.tpoint) >= {min_motion_points}
                UNION
                SELECT
                    floor(bbox_min_x / {region_size_m})::INTEGER AS rx,
                    floor(bbox_min_y / {region_size_m})::INTEGER AS ry
                FROM base_segments
                WHERE segment_type = 'stationary'
            ) AS _per_day
            """
        ).fetchall()
        cells.update((int(r), int(y)) for r, y in rows)
    return sorted(cells)


def _region_box(rx: int, ry: int, region_size_m: float) -> tuple[float, float, float, float]:
    x0 = rx * region_size_m
    y0 = ry * region_size_m
    return (x0, x0 + region_size_m, y0, y0 + region_size_m)


def write_l0(
    con: duckdb.DuckDBPyConnection, *, root: Path, day: date,
) -> tuple[Path, int]:
    out = l0_file(root, day)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.unlink(missing_ok=True)
    options = ["FORMAT PARQUET", "COMPRESSION ZSTD", kv_metadata_clause()]
    con.execute(
        f"""
        COPY (
            {_wrap_geoparquet("SELECT * FROM base_segments")}
        )
        TO {_sql_str(out.as_posix())}
        ({', '.join(options)});
        """
    )
    rows = int(
        con.execute(
            f"SELECT COUNT(*) FROM read_parquet({_sql_str(out.as_posix())})"
        ).fetchone()[0]
    )
    return out, rows


def write_l1_hash(
    con: duckdb.DuckDBPyConnection,
    *, root: Path, year: int, month: int, num_shards: int, l0_glob: str,
    l0_days: list[tuple[date, Path]] | None = None,
    granularity: str = "monthly",
) -> int:
    if num_shards < 1:
        raise ValueError("num_shards must be >= 1")
    if granularity not in ("monthly", "daily"):
        raise ValueError(
            f"granularity must be 'monthly' or 'daily', got {granularity!r}"
        )
    if granularity == "daily" and not l0_days:
        raise ValueError("l0_days is required for daily granularity")

    options = ["FORMAT PARQUET", "COMPRESSION ZSTD", kv_metadata_clause()]
    print(f"  L1: {num_shards} shards (granularity={granularity})")

    if granularity == "monthly":
        month_dir = _month_dir(root, "L1", year, month)
        _ensure_empty_dir(month_dir)
        total = 0
        for s in range(num_shards):
            out = month_dir / f"shard={s}" / "data.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_parquet({_sql_str(l0_glob)}, hive_partitioning=true)
                    WHERE (abs(hash(mmsi)) % {num_shards}) = {s}
                )
                TO {_sql_str(out.as_posix())}
                ({', '.join(options)});
                """
            )
            rows = int(
                con.execute(
                    f"SELECT COUNT(*) FROM read_parquet({_sql_str(out.as_posix())})"
                ).fetchone()[0]
            )
            total += rows
            print(f"  L1 shard={s} rows={rows}")
    else:
        total = 0
        for day, l0_path in l0_days:
            day_dir = _day_dir(root, "L1", day)
            day_dir.mkdir(parents=True, exist_ok=True)
            src = _sql_str(l0_path.as_posix())
            for s in range(num_shards):
                out = day_dir / f"shard={s}" / "data.parquet"
                if out.exists():
                    total += int(
                        con.execute(
                            f"SELECT COUNT(*) FROM read_parquet({_sql_str(out.as_posix())})"
                        ).fetchone()[0]
                    )
                    continue
                out.parent.mkdir(parents=True, exist_ok=True)
                con.execute(
                    f"""
                    COPY (
                        SELECT *
                        FROM read_parquet({src}, hive_partitioning=true)
                        WHERE (abs(hash(mmsi)) % {num_shards}) = {s}
                    )
                    TO {_sql_str(out.as_posix())}
                    ({', '.join(options)});
                    """
                )
                rows = int(
                    con.execute(
                        f"SELECT COUNT(*) FROM read_parquet({_sql_str(out.as_posix())})"
                    ).fetchone()[0]
                )
                if rows == 0:
                    out.unlink(missing_ok=True)
                    d = out.parent
                    while d.is_dir() and not any(d.iterdir()):
                        d.rmdir()
                        d = d.parent
                else:
                    total += rows
                    print(f"  L1 day={day} shard={s} rows={rows}")
    return total


def write_l2_spacesplit(
    con: duckdb.DuckDBPyConnection,
    *,
    root: Path, year: int, month: int,
    tile_size_m: float, region_size_m: float,
    params: PipelineParams,
    l0_days: list[tuple[date, Path]],
    l0_glob: str,
    cell_conn_factory: CellConnFactory | None = None,
    granularity: str = "monthly",
) -> int:
    if tile_size_m <= 0 or region_size_m <= 0:
        raise ValueError("tile_size_m and region_size_m must be > 0")
    if region_size_m < tile_size_m:
        raise ValueError("region_size_m must be >= tile_size_m")
    if granularity not in ("monthly", "daily"):
        raise ValueError(
            f"granularity must be 'monthly' or 'daily', got {granularity!r}"
        )

    cells = enumerate_region_cells(con, region_size_m, l0_days, params.min_motion_points)
    print(
        f"  L2: {len(cells)} candidate region cells × {len(l0_days)} day(s) "
        f"(granularity={granularity})"
    )

    if granularity == "monthly":
        month_dir = _month_dir(root, "L2", year, month)
        _ensure_empty_dir(month_dir)
        register_base_segments_view(con, l0_glob)
        total = 0
        for i, (rx, ry) in enumerate(cells, start=1):
            box = _region_box(rx, ry, region_size_m)
            motion_sql = space_split_motion_sql(
                tile_size_m=tile_size_m,
                min_motion_points=params.min_motion_points,
                region_box=box,
            )
            ps = passthrough_select_sql(region_box=box)
            out = month_dir / f"region_x={rx}" / f"region_y={ry}" / "data.parquet"
            rows = _copy_single(
                con,
                select_sql=f"{motion_sql} UNION ALL {ps}",
                target=out,
            )
            total += rows
            if rows > 0 or i % 25 == 0:
                print(f"  L2 [{i}/{len(cells)}] region=({rx},{ry}) rows={rows}")
    else:
        for day, _ in l0_days:
            _day_dir(root, "L2", day).mkdir(parents=True, exist_ok=True)
        total = 0
        for i, (rx, ry) in enumerate(cells, start=1):
            box = _region_box(rx, ry, region_size_m)
            motion_sql = space_split_motion_sql(
                tile_size_m=tile_size_m,
                min_motion_points=params.min_motion_points,
                region_box=box,
            )
            ps = passthrough_select_sql(region_box=box)
            select_sql = f"{motion_sql} UNION ALL {ps}"
            cell_rows = _build_cell_daily(
                con, root=root, layout="L2", rx=rx, ry=ry,
                l0_days=l0_days, select_sql=select_sql,
                cell_conn_factory=cell_conn_factory,
            )
            total += cell_rows
            if cell_rows > 0 or i % 25 == 0:
                print(f"  L2 [{i}/{len(cells)}] region=({rx},{ry}) rows={cell_rows}")
    return total


def _count_rows(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    return int(
        con.execute(
            f"SELECT COUNT(*) FROM read_parquet({_sql_str(path.as_posix())})"
        ).fetchone()[0]
    )


def _conn_ctx(
    factory: CellConnFactory | None, fallback: duckdb.DuckDBPyConnection
) -> AbstractContextManager[duckdb.DuckDBPyConnection]:
    if factory is None:
        from contextlib import nullcontext
        return nullcontext(fallback)
    return factory()


def _build_cell_monthly(
    con: duckdb.DuckDBPyConnection, *,
    month_dir: Path, rx: int, ry: int,
    l0_days: list[tuple[date, Path]], select_sql: str,
    cell_conn_factory: CellConnFactory | None,
) -> int:
    cell_dir = month_dir / f"region_x={rx}" / f"region_y={ry}"
    final = cell_dir / "data.parquet"
    if final.exists():
        return _count_rows(con, final)
    if cell_dir.exists():
        for stale in cell_dir.glob("_day-*.parquet"):
            stale.unlink()

    daily_tmps: list[Path] = []
    for day, l0_path in l0_days:
        tmp = cell_dir / f"_day-{day.isoformat()}.parquet"
        with _conn_ctx(cell_conn_factory, con) as work:
            register_base_segments_view(work, l0_path)
            rows = _copy_single(work, select_sql=select_sql, target=tmp)
        if rows > 0 and tmp.exists():
            daily_tmps.append(tmp)

    if not daily_tmps:
        if cell_dir.is_dir() and not any(cell_dir.iterdir()):
            cell_dir.rmdir()
        return 0

    cell_rows = _concat_parquet(con, sources=daily_tmps, target=final)
    for tmp in daily_tmps:
        tmp.unlink(missing_ok=True)
    return cell_rows


def _build_cell_daily(
    con: duckdb.DuckDBPyConnection, *,
    root: Path, layout: str, rx: int, ry: int,
    l0_days: list[tuple[date, Path]], select_sql: str,
    cell_conn_factory: CellConnFactory | None,
) -> int:
    total = 0
    for day, l0_path in l0_days:
        day_cell = _day_dir(root, layout, day) / f"region_x={rx}" / f"region_y={ry}"
        final = day_cell / "data.parquet"
        if final.exists():
            total += _count_rows(con, final)
            continue
        with _conn_ctx(cell_conn_factory, con) as work:
            register_base_segments_view(work, l0_path)
            rows = _copy_single(work, select_sql=select_sql, target=final)
        total += rows
    return total


def write_l3_mest(
    con: duckdb.DuckDBPyConnection,
    *,
    root: Path, year: int, month: int,
    segs_per_box: int, region_size_m: float,
    params: PipelineParams,
    l0_days: list[tuple[date, Path]],
    l0_glob: str,
    cell_conn_factory: CellConnFactory | None = None,
    granularity: str = "monthly",
) -> int:
    del l0_glob
    if segs_per_box < 1:
        raise ValueError("segs_per_box must be >= 1")
    if region_size_m <= 0:
        raise ValueError("region_size_m must be > 0")
    if granularity not in ("monthly", "daily"):
        raise ValueError(
            f"granularity must be 'monthly' or 'daily', got {granularity!r}"
        )

    cells = enumerate_region_cells(con, region_size_m, l0_days, params.min_motion_points)
    print(
        f"  L3: {len(cells)} candidate region cells × {len(l0_days)} day(s) "
        f"(granularity={granularity})"
    )

    if granularity == "monthly":
        month_dir = _month_dir(root, "L3", year, month)
        month_dir.mkdir(parents=True, exist_ok=True)
    else:
        for day, _ in l0_days:
            _day_dir(root, "L3", day).mkdir(parents=True, exist_ok=True)

    total = 0
    for i, (rx, ry) in enumerate(cells, start=1):
        box = _region_box(rx, ry, region_size_m)
        motion_sql = mest_split_motion_sql(
            segs_per_box=segs_per_box,
            min_motion_points=params.min_motion_points,
            region_box=box,
        )
        ps = passthrough_select_sql(region_box=box)
        select_sql = f"{motion_sql} UNION ALL {ps}"

        if granularity == "monthly":
            cell_rows = _build_cell_monthly(
                con, month_dir=month_dir, rx=rx, ry=ry,
                l0_days=l0_days, select_sql=select_sql,
                cell_conn_factory=cell_conn_factory,
            )
        else:
            cell_rows = _build_cell_daily(
                con, root=root, layout="L3", rx=rx, ry=ry,
                l0_days=l0_days, select_sql=select_sql,
                cell_conn_factory=cell_conn_factory,
            )

        total += cell_rows
        if cell_rows > 0 or i % 25 == 0:
            print(f"  L3 [{i}/{len(cells)}] region=({rx},{ry}) rows={cell_rows}")
    return total


def write_l4_timesplit(
    con: duckdb.DuckDBPyConnection,
    *, root: Path, year: int, month: int,
    time_bin: str, params: PipelineParams,
    l0_days: list[tuple[date, Path]] | None = None,
    cell_conn_factory: CellConnFactory | None = None,
    granularity: str = "monthly",
) -> int:
    if granularity not in ("monthly", "daily"):
        raise ValueError(
            f"granularity must be 'monthly' or 'daily', got {granularity!r}"
        )
    if granularity == "daily" and not l0_days:
        raise ValueError("l0_days is required for daily granularity")

    bin_hours = parse_time_bin_hours(time_bin)
    num_bins = 24 // bin_hours
    print(f"  L4: {num_bins} bins × {bin_hours}h (granularity={granularity})")

    if granularity == "monthly":
        month_dir = _month_dir(root, "L4", year, month)
        _ensure_empty_dir(month_dir)
        total = 0
        for h in range(0, 24, bin_hours):
            motion_sql = time_split_motion_sql(
                time_bin=time_bin,
                min_motion_points=params.min_motion_points,
                hour=h, bin_hours=bin_hours,
            )
            ps = passthrough_select_sql(hour=h, bin_hours=bin_hours)
            out = month_dir / f"hour={h}" / "data.parquet"
            rows = _copy_single(
                con,
                select_sql=f"{motion_sql} UNION ALL {ps}",
                target=out,
            )
            total += rows
            print(f"  L4 hour={h} rows={rows}")
    else:
        expected_hours = set(range(0, 24, bin_hours))
        total = 0
        for day, l0_path in l0_days:
            day_dir = _day_dir(root, "L4", day)
            day_dir.mkdir(parents=True, exist_ok=True)
            present_hours = {
                int(p.name.removeprefix("hour="))
                for p in day_dir.glob("hour=*")
                if p.is_dir() and p.name.removeprefix("hour=").isdigit()
            }
            stale = present_hours - expected_hours
            if stale:
                raise ValueError(
                    f"L4 daily build: {day_dir} has hour partitions {sorted(stale)} "
                    f"outside the current bin_hours={bin_hours} grid. Likely a "
                    f"previous build used a different --time-bin. Remove "
                    f"{day_dir.as_posix()} and rerun."
                )
            if len(present_hours) >= 2:
                sorted_p = sorted(present_hours)
                implied_stride = min(b - a for a, b in zip(sorted_p, sorted_p[1:]))
                if implied_stride != bin_hours:
                    raise ValueError(
                        f"L4 daily build: {day_dir} has hour partitions "
                        f"{sorted_p} with stride {implied_stride}h, but current "
                        f"bin_hours={bin_hours}h. Previous build used a different "
                        f"--time-bin. Remove {day_dir.as_posix()} and rerun."
                    )
            for h in range(0, 24, bin_hours):
                final = day_dir / f"hour={h}" / "data.parquet"
                if final.exists():
                    total += _count_rows(con, final)
                    continue
                motion_sql = time_split_motion_sql(
                    time_bin=time_bin,
                    min_motion_points=params.min_motion_points,
                    hour=h, bin_hours=bin_hours,
                )
                ps = passthrough_select_sql(hour=h, bin_hours=bin_hours)
                with _conn_ctx(cell_conn_factory, con) as work:
                    register_base_segments_view(work, l0_path)
                    rows = _copy_single(
                        work,
                        select_sql=f"{motion_sql} UNION ALL {ps}",
                        target=final,
                    )
                total += rows
                if rows > 0:
                    print(f"  L4 day={day} hour={h} rows={rows}")
    return total
