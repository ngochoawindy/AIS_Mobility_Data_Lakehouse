"""Tiling SELECTs over `base_segments`."""

import re


def parse_time_bin_hours(time_bin: str) -> int:
    m = re.match(r"^\s*(\d+)\s+hours?\s*$", time_bin, re.IGNORECASE)
    if not m:
        raise ValueError(
            f"time_bin must be '<N> hour(s)', got {time_bin!r}"
        )
    h = int(m.group(1))
    if h < 1 or 24 % h != 0:
        raise ValueError(
            f"time_bin hours must evenly divide 24, got {h}"
        )
    return h


_OUTPUT_COLS = """
    mmsi, segment_type, vessel_name, imo, callsign, vessel_type,
    start_time, end_time, duration_s,
    bbox_min_x, bbox_min_y, bbox_max_x, bbox_max_y,
    point_count, track_length_m,
    quality_flags, source_file,
    traj
"""


RegionBox = tuple[float, float, float, float]


def _bbox_contains_where(region_box: RegionBox | None) -> str:
    if region_box is None:
        return ""
    x0, x1, y0, y1 = region_box
    return (
        f" AND bbox_min_x >= {x0} AND bbox_min_x < {x1}"
        f" AND bbox_min_y >= {y0} AND bbox_min_y < {y1}"
    )


def _spacebin_in_region_where(region_box: RegionBox | None) -> str:
    if region_box is None:
        return ""
    x0, x1, y0, y1 = region_box
    return (
        f" AND ST_X(sp.spaceBin) >= {x0} AND ST_X(sp.spaceBin) < {x1}"
        f" AND ST_Y(sp.spaceBin) >= {y0} AND ST_Y(sp.spaceBin) < {y1}"
    )


def _stbox_in_region_where(region_box: RegionBox | None) -> str:
    if region_box is None:
        return ""
    x0, x1, y0, y1 = region_box
    return (
        f" AND Xmin(mb.box) >= {x0} AND Xmin(mb.box) < {x1}"
        f" AND Ymin(mb.box) >= {y0} AND Ymin(mb.box) < {y1}"
    )


def _temporal_within_parent(out_traj: str) -> str:
    """Guard against degenerate MEOS split output.

    `spaceSplit`/`atStbox` can emit a corrupt sub-trajectory anchored at a
    phantom instant (observed: a 2000-01-01 start on a 15-point, ~26-year,
    8.4e9 m "in motion" segment for one vessel whose raw points are all in
    range). A legitimate split only selects instants that already exist in the
    parent segment, so the output's temporal extent must lie within the
    parent's [start_time, end_time]. This drops only such corrupt rows and is
    date-independent (a speed/length bound does not catch it — the phantom
    span makes the implied speed look plausible)."""
    return (
        f" AND startTimestamp({out_traj}) >= bs.start_time"
        f" AND endTimestamp({out_traj}) <= bs.end_time"
    )


def _time_bin_where(bin_start: int | None, bin_hours: int, col_sql: str) -> str:
    if bin_start is None:
        return ""
    return f" AND extract(hour FROM {col_sql})::INTEGER = {bin_start}"


def _hour_range_where(
    bin_start: int | None, bin_hours: int, col_sql: str
) -> str:
    if bin_start is None:
        return ""
    if bin_hours == 1:
        return f" AND extract(hour FROM {col_sql})::INTEGER = {bin_start}"
    bin_end = bin_start + bin_hours
    return (
        f" AND extract(hour FROM {col_sql})::INTEGER >= {bin_start}"
        f" AND extract(hour FROM {col_sql})::INTEGER < {bin_end}"
    )


def passthrough_select_sql(
    *,
    region_box: RegionBox | None = None,
    hour: int | None = None,
    bin_hours: int = 1,
) -> str:
    where = (
        " AND segment_type = 'stationary'"
        + _bbox_contains_where(region_box)
        + _hour_range_where(hour, bin_hours, "start_time")
    )
    return f"""
        SELECT {_OUTPUT_COLS}
        FROM base_segments
        WHERE 1=1 {where}
    """


def _motion_subquery_sql(*, min_motion_points: int, region_box: RegionBox | None) -> str:
    raw_input_filter = (
        f" AND segment_type = 'in motion'"
        f" AND point_count >= {min_motion_points}"
        f" AND duration_s > 0"
        f" AND (bbox_max_x > bbox_min_x OR bbox_max_y > bbox_min_y)"
    )
    if region_box is None:
        return f"""
            SELECT {_OUTPUT_COLS}
            FROM base_segments
            WHERE 1=1 {raw_input_filter}
        """
    x0, x1, y0, y1 = region_box
    return f"""
        SELECT * FROM (
            SELECT
                mmsi, segment_type, vessel_name, imo, callsign, vessel_type,
                start_time, end_time, duration_s,
                bbox_min_x, bbox_min_y, bbox_max_x, bbox_max_y,
                point_count, track_length_m, quality_flags, source_file,
                UNNEST(sequences(
                    atStbox(
                        traj,
                        ST_MakeEnvelope({x0}, {y0}, {x1}, {y1})::STBOX,
                        true
                    )
                )) AS traj
            FROM base_segments
            WHERE 1=1 {raw_input_filter}
              AND bbox_max_x >= {x0} AND bbox_min_x < {x1}
              AND bbox_max_y >= {y0} AND bbox_min_y < {y1}
        ) AS _exploded
        WHERE traj IS NOT NULL
          AND numInstants(traj) >= {min_motion_points}
          AND length(traj) > 0
          AND DATE_DIFF('second',
                startTimestamp(traj), endTimestamp(traj)) > 0
    """


def space_split_motion_sql(
    *,
    tile_size_m: float,
    min_motion_points: int,
    region_box: RegionBox | None = None,
) -> str:
    sub = _motion_subquery_sql(min_motion_points=min_motion_points, region_box=region_box)
    return f"""
        SELECT
            bs.mmsi,
            'in motion'                                   AS segment_type,
            bs.vessel_name, bs.imo, bs.callsign, bs.vessel_type,

            startTimestamp(sp.tpoint)                     AS start_time,
            endTimestamp(sp.tpoint)                       AS end_time,
            DATE_DIFF('second',
                startTimestamp(sp.tpoint),
                endTimestamp(sp.tpoint))::INTEGER         AS duration_s,

            ST_X(sp.spaceBin)::DOUBLE                     AS bbox_min_x,
            ST_Y(sp.spaceBin)::DOUBLE                     AS bbox_min_y,
            (ST_X(sp.spaceBin) + {tile_size_m})::DOUBLE   AS bbox_max_x,
            (ST_Y(sp.spaceBin) + {tile_size_m})::DOUBLE   AS bbox_max_y,

            numInstants(sp.tpoint)::INTEGER               AS point_count,
            length(sp.tpoint)::DOUBLE                     AS track_length_m,

            bs.quality_flags, bs.source_file,
            sp.tpoint                                     AS traj
        FROM ({sub}) bs,
             LATERAL spaceSplit(
                 bs.traj,
                 {tile_size_m}, {tile_size_m}, 1.0,
                 ST_Point(0, 0),
                 FALSE
             ) sp(spaceBin, tpoint)
        WHERE numInstants(sp.tpoint) >= {min_motion_points}
          {_spacebin_in_region_where(region_box)}
          {_temporal_within_parent("sp.tpoint")}
    """


def mest_split_motion_sql(
    *,
    segs_per_box: int,
    min_motion_points: int,
    region_box: RegionBox | None = None,
) -> str:
    sub = _motion_subquery_sql(min_motion_points=min_motion_points, region_box=region_box)
    return f"""
        SELECT
            bs.mmsi,
            'in motion'                                  AS segment_type,
            bs.vessel_name, bs.imo, bs.callsign, bs.vessel_type,

            startTimestamp(clipped.traj)                 AS start_time,
            endTimestamp(clipped.traj)                   AS end_time,
            DATE_DIFF('second',
                startTimestamp(clipped.traj),
                endTimestamp(clipped.traj))::INTEGER     AS duration_s,

            Xmin(mb.box)::DOUBLE                         AS bbox_min_x,
            Ymin(mb.box)::DOUBLE                         AS bbox_min_y,
            Xmax(mb.box)::DOUBLE                         AS bbox_max_x,
            Ymax(mb.box)::DOUBLE                         AS bbox_max_y,

            numInstants(clipped.traj)::INTEGER           AS point_count,
            length(clipped.traj)::DOUBLE                 AS track_length_m,

            bs.quality_flags, bs.source_file,
            clipped.traj                                 AS traj

        FROM ({sub}) bs,
             LATERAL UNNEST(splitEachNStboxes(bs.traj, {segs_per_box})) AS mb(box),
             LATERAL (SELECT atStbox(bs.traj, mb.box, true) AS traj) AS clipped
        WHERE clipped.traj IS NOT NULL
          AND numInstants(clipped.traj) >= 2
          {_stbox_in_region_where(region_box)}
          {_temporal_within_parent("clipped.traj")}
    """


_TIME_ONLY_HUGE_TILE = 1.0e12


def time_split_motion_sql(
    *,
    time_bin: str,
    min_motion_points: int,
    hour: int | None = None,
    bin_hours: int = 1,
) -> str:
    sub = _motion_subquery_sql(min_motion_points=min_motion_points, region_box=None)
    return f"""
        SELECT
            bs.mmsi,
            'in motion'                                   AS segment_type,
            bs.vessel_name, bs.imo, bs.callsign, bs.vessel_type,

            startTimestamp(sp.tpoint)                     AS start_time,
            endTimestamp(sp.tpoint)                       AS end_time,
            DATE_DIFF('second',
                startTimestamp(sp.tpoint),
                endTimestamp(sp.tpoint))::INTEGER         AS duration_s,

            Xmin(stbox(sp.tpoint))::DOUBLE                AS bbox_min_x,
            Ymin(stbox(sp.tpoint))::DOUBLE                AS bbox_min_y,
            Xmax(stbox(sp.tpoint))::DOUBLE                AS bbox_max_x,
            Ymax(stbox(sp.tpoint))::DOUBLE                AS bbox_max_y,

            numInstants(sp.tpoint)::INTEGER               AS point_count,
            length(sp.tpoint)::DOUBLE                     AS track_length_m,

            bs.quality_flags, bs.source_file,
            sp.tpoint                                     AS traj
        FROM ({sub}) bs,
             LATERAL spaceTimeSplit(
                 bs.traj,
                 {_TIME_ONLY_HUGE_TILE}, {_TIME_ONLY_HUGE_TILE}, {_TIME_ONLY_HUGE_TILE},
                 INTERVAL '{time_bin}',
                 ST_Point(0, 0),
                 TIMESTAMP '2000-01-01 00:00:00',
                 FALSE
             ) sp(spaceBin, timeBin, tpoint)
        WHERE numInstants(sp.tpoint) >= {min_motion_points}
          {_time_bin_where(hour, bin_hours, "sp.timeBin")}
          {_temporal_within_parent("sp.tpoint")}
    """
