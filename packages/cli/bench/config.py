"""Evaluation config: layouts, query areas/windows, schema columns."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

CLI_ROOT = Path(__file__).resolve().parent.parent
DATA = CLI_ROOT / "data"
RESULTS = CLI_ROOT / "results"

# Where the `trips` layouts are written/read. Local by default; set
# TRIPS_DEST=s3://warehouse/trips to build + register directly on object storage
# (MinIO) with no local staging. The raw/L0/daily/compact *source* layouts always
# stay local (data/…) — only this trips layer is redirectable.
TRIPS_DEST = os.getenv("TRIPS_DEST", (DATA / "trips").as_posix())

try:
    from lakehouse.settings import Settings

    _S = Settings.create()
    MOBILITYDUCK_EXT = _S.mobilityduck.extension_path
    METRIC_EPSG = _S.pipeline.metric_epsg
except Exception:
    MOBILITYDUCK_EXT = (
        "/Users/ngochoapham/Thesis/MobilityDuck/build/release/extension"
        "/mobilityduck/mobilityduck.duckdb_extension"
    )
    METRIC_EPSG = 32632

N_ITERS = 10
TRIM = 2
WARMUP = 1


@dataclass(frozen=True)
class LayoutSpec:
    name: str
    gran: str
    subdir: str
    partition_keys: tuple[str, ...]
    region_size_m: float | None
    desc: str

    @property
    def glob(self) -> str:
        return (DATA / self.subdir / "**" / "*.parquet").as_posix()

    @property
    def trips_glob(self) -> str:
        """Glob over the clean flat-schema `trips` projection (true-UTC
        timestamptz, top-level xmin/xmax/ymin/ymax, EWKB `traj`). Mirrors the
        source layout's partition tree file-for-file, so pruning is identical."""
        return (DATA / "trips" / self.subdir / "**" / "*.parquet").as_posix()

    @property
    def root(self) -> Path:
        return DATA / self.subdir

    @property
    def key(self) -> str:
        # Single-granularity layouts use a bare name (no redundant suffix).
        return self.name if self.name in ("L0", "L1s", "L2s", "L3s", "L4s", "LHs") \
            else f"{self.name}_{self.gran}"


def trips_root(ls: "LayoutSpec") -> str:
    """The layout's `trips` location under TRIPS_DEST — a local dir or an s3:// prefix."""
    return f"{TRIPS_DEST.rstrip('/')}/{ls.subdir}"


def trips_glob(ls: "LayoutSpec") -> str:
    """Recursive parquet glob for the layout's trips files (local path or s3:// glob)."""
    return f"{trips_root(ls)}/**/*.parquet"


# Daily family (date-partitioned baseline) + sorted-compact family (compact tiles, rows
# clustered by sub-tile cell+time at a small row-group size). The plain unsorted compacts
# were dropped: sorted-compact dominates them and isolates the partition scheme.
BASE_LAYOUTS: tuple[LayoutSpec, ...] = (
    LayoutSpec("L0", "daily", "L0/L0", ("year", "month", "day"), None,
               "base segments, daily"),
    LayoutSpec("L1", "daily", "layouts_daily/L1", ("shard",), None,
               "hash(mmsi), daily"),
    LayoutSpec("L2", "daily", "layouts_daily/L2", ("region_x", "region_y"),
               50_000, "spaceSplit tiles, daily"),
    LayoutSpec("L3", "daily", "layouts_daily/L3", ("region_x", "region_y"),
               50_000, "MEST tiles, daily"),
    LayoutSpec("L4", "daily", "layouts_daily/L4", ("hour",), None,
               "timeSplit(hour), daily"),
    LayoutSpec("LH", "daily", "layouts_daily/LH", ("hbucket",), None,
               "Hilbert order, daily"),
    # sorted-compact family (all use the same sort+small-row-group; differ by partitioning)
    LayoutSpec("L1s", "compact", "layout_compact/L1s", ("shard",), None,
               "hash(mmsi), compact, sorted (cell+time) small row-groups"),
    LayoutSpec("L2s", "compact", "layout_compact/L2s",
               ("region_x", "region_y"), 50_000,
               "spaceSplit tiles, compact, sorted (cell+time) small row-groups"),
    LayoutSpec("L3s", "compact", "layout_compact/L3s",
               ("region_x", "region_y"), 50_000,
               "MEST tiles, compact, sorted (cell+time) small row-groups"),
    LayoutSpec("L4s", "compact", "layout_compact/L4s", ("hour",), None,
               "timeSplit(hour), compact, sorted (cell+time) small row-groups"),
    LayoutSpec("LHs", "compact", "layout_compact/LHs",
               ("hbucket",), None,
               "Hilbert buckets, compact, sorted (cell+time) small row-groups"),
)

LAYOUTS: tuple[LayoutSpec, ...] = BASE_LAYOUTS


def _layout_matches(ls: LayoutSpec, names: set[str] | None) -> bool:
    if not names:
        return True
    return ls.name in names or ls.key in names


def layouts(names: set[str] | None = None, grans: set[str] | None = None):
    for ls in BASE_LAYOUTS:
        if not _layout_matches(ls, names):
            continue
        if grans and ls.gran not in grans:
            continue
        yield ls


@dataclass(frozen=True)
class Area:
    name: str
    xmin: float
    ymin: float
    xmax: float
    ymax: float
    desc: str

    @property
    def bbox(self) -> tuple[float, float, float, float]:
        return (self.xmin, self.ymin, self.xmax, self.ymax)

    @property
    def area_km2(self) -> float:
        return (self.xmax - self.xmin) * (self.ymax - self.ymin) / 1e6


RODBY = Area("rodby", 651135.0, 6058230.0, 651422.0, 6058548.0,
             "Rødby ferry port (~0.09 km²)")
PUTTGARDEN = Area("puttgarden", 644339.0, 6042108.0, 644896.0, 6042487.0,
                  "Puttgarden ferry port (~0.21 km²)")
GOTEBORG = Area("goteborg", 666538.0, 6392057.0, 679171.0, 6403745.0,
                "Port of Göteborg (book Q5.8)")
ALERT_BELT = Area("belt", 640730.0, 6042487.0, 654100.0, 6058230.0,
                  "Rødby–Puttgarden alert belt (book Q5.9–5.11)")


@dataclass(frozen=True)
class Window:
    name: str
    start: str
    end: str
    desc: str


WINDOWS: tuple[Window, ...] = (
    Window("day", "2026-01-15 00:00:00", "2026-01-16 00:00:00", "1 day"),
    Window("week", "2026-01-12 00:00:00", "2026-01-19 00:00:00", "1 week")
    )
