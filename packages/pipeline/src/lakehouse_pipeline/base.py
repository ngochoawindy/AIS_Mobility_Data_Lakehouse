"""Stages that produce `base_segments` from raw AIS parquet:"""

from pathlib import Path

import duckdb

from lakehouse_pipeline._params import PipelineParams
from lakehouse_pipeline._utils import _sql_str
from lakehouse_pipeline.segment import segment_clean_points
from lakehouse_pipeline.trajectory import build_base_segments


def filter_bad_vessels(
    con: duckdb.DuckDBPyConnection, *, params: PipelineParams
) -> int:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _bad_vessels AS
        SELECT mmsi
        FROM (
            SELECT mmsi,
                   SUM(CASE WHEN is_outlier_point THEN 1 ELSE 0 END) * 1.0
                       / COUNT(*) AS outlier_pct
            FROM clean_points
            GROUP BY mmsi
        )
        WHERE outlier_pct > {params.max_vessel_outlier_pct};
        """
    )
    dropped = con.execute("SELECT COUNT(*) FROM _bad_vessels").fetchone()[0]
    if dropped > 0:
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE clean_points AS
            SELECT * FROM clean_points
            WHERE mmsi NOT IN (SELECT mmsi FROM _bad_vessels);
            """
        )
    con.execute("DROP TABLE IF EXISTS _bad_vessels;")
    return int(dropped)


def build_base_segments_from_raw(
    con: duckdb.DuckDBPyConnection,
    parquet_file: Path,
    *,
    params: PipelineParams,
) -> tuple[int, int]:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW normalized AS
        SELECT * FROM read_parquet({_sql_str(parquet_file.as_posix())});
        """
    )
    raw_count = con.execute("SELECT COUNT(*) FROM normalized").fetchone()[0]

    if params.study_area_lonlat_bbox is not None:
        lon_min, lon_max, lat_min, lat_max = params.study_area_lonlat_bbox
        study_area_clip = (
            f" AND longitude BETWEEN {lon_min} AND {lon_max}"
            f" AND latitude  BETWEEN {lat_min} AND {lat_max}"
        )
    else:
        study_area_clip = ""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW filtered AS
        SELECT * FROM normalized
        WHERE mmsi IS NOT NULL
          AND event_time IS NOT NULL
          AND latitude  IS NOT NULL
          AND longitude IS NOT NULL
          AND latitude  BETWEEN -90  AND 90
          AND longitude BETWEEN -180 AND 180
          AND (draught < 28.5 OR draught IS NULL)
          AND (width   < 75   OR width   IS NULL)
          AND (length  < 488  OR length  IS NULL)
          AND mmsi < 990000000
          AND mmsi > 99999999
          AND (mmsi <= 111000000 OR mmsi >= 112000000)
          AND mobile_type = 'Class A'
          {study_area_clip}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY mmsi, event_time
            ORDER BY latitude, longitude
        ) = 1;
        """
    )

    segment_clean_points(con, parquet_file=parquet_file, params=params)
    filter_bad_vessels(con, params=params)
    base_count = build_base_segments(con, params=params)
    con.execute("DROP TABLE IF EXISTS clean_points;")
    return int(raw_count), int(base_count)
