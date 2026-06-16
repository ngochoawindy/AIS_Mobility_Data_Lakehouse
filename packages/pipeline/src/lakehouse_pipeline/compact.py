"""Compaction: turn an existing daily-granularity L1..L4 layout into a"""

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import duckdb

from lakehouse_pipeline._duckdb import open_pipeline_conn
from lakehouse_pipeline.layouts import _concat_parquet
from lakehouse_pipeline.runner import DuckdbRuntime


def _month_day_dirs(layout_src: Path, year: int, month: int) -> list[Path]:
    prefix = f"day={year:04d}-{month:02d}-"
    return sorted(p for p in layout_src.glob(f"{prefix}*") if p.is_dir())


def _month_dir(dst_root: Path, layout: str, year: int, month: int) -> Path:
    return dst_root / layout / f"year={year:04d}" / f"month={month:02d}"


@dataclass(frozen=True)
class PartitionKeys:
    parts: tuple[str, ...]

    @property
    def subpath(self) -> Path:
        return Path(*self.parts)


def _discover_partitions(
    day_dirs: list[Path], pattern: tuple[str, ...]
) -> list[PartitionKeys]:
    keys: set[PartitionKeys] = set()
    for day_dir in day_dirs:
        for combo in _walk_pattern(day_dir, pattern):
            keys.add(PartitionKeys(tuple(combo)))
    return sorted(keys, key=lambda k: k.parts)


def _walk_pattern(base: Path, pattern: tuple[str, ...]) -> Iterable[list[str]]:
    if not pattern:
        yield []
        return
    head, *tail = pattern
    for child in sorted(base.glob(head)):
        if not child.is_dir():
            continue
        for sub in _walk_pattern(child, tuple(tail)):
            yield [child.name, *sub]


PARTITION_PATTERN: dict[str, tuple[str, ...]] = {
    "L1": ("shard=*",),
    "L2": ("region_x=*", "region_y=*"),
    "L3": ("region_x=*", "region_y=*"),
    "L4": ("hour=*",),
}


def compact_daily_to_monthly(
    *,
    layout: str,
    year: int,
    month: int,
    src_root: Path,
    dst_root: Path,
    runtime: DuckdbRuntime,
    delete_sources: bool = False,
) -> int:
    if layout not in PARTITION_PATTERN:
        raise ValueError(
            f"layout must be one of {sorted(PARTITION_PATTERN)}, got {layout!r}"
        )

    layout_src = src_root / layout
    if not layout_src.is_dir():
        raise FileNotFoundError(f"No daily source directory at {layout_src}")

    day_dirs = _month_day_dirs(layout_src, year, month)
    if not day_dirs:
        print(f"  no day=YYYY-MM-DD dirs under {layout_src} for {year}-{month:02d}")
        return 0

    pattern = PARTITION_PATTERN[layout]
    partitions = _discover_partitions(day_dirs, pattern)
    print(
        f"  {layout}: {len(day_dirs)} day(s) × {len(partitions)} partition(s) "
        f"→ {dst_root}"
    )

    month_dir = _month_dir(dst_root, layout, year, month)
    month_dir.mkdir(parents=True, exist_ok=True)

    t0 = perf_counter()
    total_rows = 0
    spill = (dst_root / ".spill").resolve()
    with open_pipeline_conn(
        spill_dir=spill,
        extension_path=runtime.extension_path,
        extension_name=runtime.extension_name,
        allow_unsigned_extensions=runtime.allow_unsigned_extensions,
        memory_limit=runtime.memory_limit,
        threads=runtime.threads,
        max_temp_directory_size=runtime.max_temp_directory_size,
    ) as con:
        for i, pkeys in enumerate(partitions, start=1):
            target = month_dir / pkeys.subpath / "data.parquet"
            if target.exists():
                rows = _count_rows(con, target)
                total_rows += rows
                print(f"  {layout} [{i}/{len(partitions)}] {pkeys.subpath} "
                      f"already exists, rows={rows}")
                continue

            sources = [
                d / pkeys.subpath / "data.parquet" for d in day_dirs
                if (d / pkeys.subpath / "data.parquet").is_file()
            ]
            if not sources:
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            rows = _concat_parquet(con, sources=sources, target=target)
            total_rows += rows
            if delete_sources and rows > 0:
                for s in sources:
                    s.unlink(missing_ok=True)
                    d = s.parent
                    while d.is_dir() and not any(d.iterdir()) and d != layout_src:
                        d.rmdir()
                        d = d.parent
            if rows > 0 or i % 25 == 0:
                print(f"  {layout} [{i}/{len(partitions)}] {pkeys.subpath} "
                      f"rows={rows}  sources={len(sources)}")

    print(
        f"  {layout} {year}-{month:02d} compacted: rows={total_rows} "
        f"out={month_dir.as_posix()} total_s={perf_counter() - t0:.3f}"
    )
    return total_rows


def _count_rows(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    return int(
        con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path.as_posix()}')"
        ).fetchone()[0]
    )
