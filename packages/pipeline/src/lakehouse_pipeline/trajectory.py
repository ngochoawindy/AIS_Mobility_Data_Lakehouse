import duckdb

from lakehouse_pipeline._params import PipelineParams
from lakehouse_pipeline._utils import _sql_str


def build_base_segments(
    con: duckdb.DuckDBPyConnection,
    *,
    params: PipelineParams,
) -> int:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE base_segments AS
        WITH
        pts_metric AS (
            SELECT
                *,
                ST_Transform(geom_4326, '+proj=longlat +datum=WGS84', {_sql_str(params.metric_srid)}) AS geom_metric
            FROM clean_points
        ),

        motion_segs AS (
            SELECT
                mmsi,
                'in motion'                                     AS segment_type,
                any_value(name)                                 AS vessel_name,
                any_value(imo)::UINTEGER                        AS imo,
                any_value(callsign)                             AS callsign,
                any_value(ship_type)                            AS vessel_type,

                MIN(event_time)                                 AS start_time,
                MAX(event_time)                                 AS end_time,
                DATE_DIFF('second',
                    MIN(event_time), MAX(event_time))::INTEGER  AS duration_s,

                MIN(ST_X(geom_metric))::DOUBLE                  AS bbox_min_x,
                MIN(ST_Y(geom_metric))::DOUBLE                  AS bbox_min_y,
                MAX(ST_X(geom_metric))::DOUBLE                  AS bbox_max_x,
                MAX(ST_Y(geom_metric))::DOUBLE                  AS bbox_max_y,

                COUNT(*)::INTEGER                               AS point_count,
                SUM(CASE WHEN NOT is_new_segment
                         THEN COALESCE(meters_to_prev, 0.0)
                         ELSE 0.0
                    END)::DOUBLE                                AS track_length_m,

                NULLIF(STRING_AGG(DISTINCT quality_flags, '|')
                       FILTER (WHERE quality_flags IS NOT NULL), '') AS quality_flags,
                any_value(source_file)                          AS source_file,

                YEAR(MIN(event_time))::INTEGER                  AS year,
                MONTH(MIN(event_time))::INTEGER                 AS month,
                DAY(MIN(event_time))::INTEGER                   AS day,

                tgeompointSeq(
                    list_transform(
                        list(struct_pack(ts := event_time, geom := geom_metric)
                             ORDER BY event_time),
                        t -> tgeompoint(t.geom, t.ts)
                    )
                )                                               AS traj

            FROM pts_metric
            WHERE object_type = 'in motion' AND NOT is_outlier_point
            GROUP BY mmsi, object_id
            HAVING COUNT(*) >= {params.min_motion_points}
               -- spaceSplit / spaceTimeSplit reject zero-extent trajectories:
               -- vessels reporting SOG above the stop threshold while their GPS
               -- position doesn't change show up here. Drop them — they aren't
               -- meaningful "in motion" segments and they crash MEOS tilers.
               AND (MAX(ST_X(geom_metric)) > MIN(ST_X(geom_metric))
                 OR MAX(ST_Y(geom_metric)) > MIN(ST_Y(geom_metric)))
               AND DATE_DIFF('second', MIN(event_time), MAX(event_time)) > 0
        ),

        stationary_segs AS (
            SELECT
                mmsi,
                'stationary'                                    AS segment_type,
                any_value(name)                                 AS vessel_name,
                any_value(imo)::UINTEGER                        AS imo,
                any_value(callsign)                             AS callsign,
                any_value(ship_type)                            AS vessel_type,

                MIN(event_time)                                 AS start_time,
                MAX(event_time)                                 AS end_time,
                DATE_DIFF('second',
                    MIN(event_time), MAX(event_time))::INTEGER  AS duration_s,

                MIN(ST_X(geom_metric))::DOUBLE                  AS bbox_min_x,
                MIN(ST_Y(geom_metric))::DOUBLE                  AS bbox_min_y,
                MAX(ST_X(geom_metric))::DOUBLE                  AS bbox_max_x,
                MAX(ST_Y(geom_metric))::DOUBLE                  AS bbox_max_y,

                COUNT(*)::INTEGER                               AS point_count,
                SUM(CASE WHEN NOT is_new_segment
                         THEN COALESCE(meters_to_prev, 0.0)
                         ELSE 0.0
                    END)::DOUBLE                                AS track_length_m,

                NULLIF(STRING_AGG(DISTINCT quality_flags, '|')
                       FILTER (WHERE quality_flags IS NOT NULL), '') AS quality_flags,
                any_value(source_file)                          AS source_file,

                YEAR(MIN(event_time))::INTEGER                  AS year,
                MONTH(MIN(event_time))::INTEGER                 AS month,
                DAY(MIN(event_time))::INTEGER                   AS day,

                tgeompointSeq(
                    list_transform(
                        list(struct_pack(ts := event_time, geom := geom_metric)
                             ORDER BY event_time),
                        t -> tgeompoint(t.geom, t.ts)
                    )
                )                                               AS traj

            FROM pts_metric
            WHERE object_type = 'stationary' AND NOT is_outlier_point
            GROUP BY mmsi, object_id
        )

        SELECT * FROM motion_segs
        UNION ALL
        SELECT * FROM stationary_segs;
        """
    )
    return con.execute("SELECT COUNT(*) FROM base_segments").fetchone()[0]
