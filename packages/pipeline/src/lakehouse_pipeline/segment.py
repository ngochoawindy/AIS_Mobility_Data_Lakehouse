from pathlib import Path

import duckdb

from lakehouse_pipeline._params import PipelineParams
from lakehouse_pipeline._utils import _sql_str


def segment_clean_points(
    con: duckdb.DuckDBPyConnection,
    *,
    parquet_file: Path,
    params: PipelineParams,
) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE clean_points AS
        WITH base AS (
            SELECT
                *,
                CAST(strftime(event_time, '%Y%m%d') AS UINTEGER) AS date_id,
                CAST(strftime(event_time, '%H%M%S') AS UINTEGER) AS time_id,

                ST_Point(longitude, latitude) AS geom_4326,

                regexp_matches(CAST(mmsi AS VARCHAR), '^[0-9]{{9}}$') AS is_valid_mmsi,

                CASE
                    WHEN imo IS NULL THEN NULL
                    ELSE regexp_matches(CAST(imo AS VARCHAR), '^[0-9]{{7}}$')
                END AS is_valid_imo,

                {_sql_str(parquet_file.name)} AS source_file
            FROM filtered
        ),

        -- Bring in the previous point's timestamp and position for each MMSI.
        with_lag AS (
            SELECT
                *,
                LAG(event_time) OVER w AS prev_event_time,
                LAG(latitude)    OVER w AS prev_latitude,
                LAG(longitude)   OVER w AS prev_longitude,
                LAG(geom_4326)  OVER w AS prev_geom_4326
            FROM base
            WINDOW w AS (
                PARTITION BY mmsi
                ORDER BY event_time, latitude, longitude
            )
        ),

        -- Compute all scalar path metrics in one pass.
        -- ST_Distance_Sphere returns geodetic metres directly from WGS-84 points.
        -- Keep geom_4326 as lon/lat for downstream geometry output, but pass
        -- lat/lon points here because DuckDB's spherical helper interprets the
        -- first coordinate as latitude.
        -- DuckDB lateral column aliases let each expression reference aliases
        -- defined earlier in the same SELECT list (seconds_to_prev, meters_to_prev,
        -- calc_speed_knots_to_prev), avoiding repeated sub-expressions.
        metrics AS (
            SELECT
                *,
                epoch(event_time) - epoch(prev_event_time)          AS seconds_to_prev,

                CASE
                    WHEN prev_geom_4326 IS NULL THEN NULL
                    ELSE ST_Distance_Sphere(
                        ST_Point(prev_latitude, prev_longitude),
                        ST_Point(latitude, longitude)
                    )
                END                                                  AS meters_to_prev,

                CASE
                    WHEN meters_to_prev IS NULL OR seconds_to_prev <= 0 THEN NULL
                    ELSE meters_to_prev / seconds_to_prev
                END                                                  AS calc_speed_mps_to_prev,

                CASE
                    WHEN meters_to_prev IS NULL OR seconds_to_prev <= 0 THEN NULL
                    ELSE meters_to_prev / seconds_to_prev * 1.943844
                END                                                  AS calc_speed_knots_to_prev,

                CASE
                    WHEN sog IS NOT NULL
                         AND calc_speed_knots_to_prev IS NOT NULL
                         AND ABS(sog - calc_speed_knots_to_prev) < 2.0
                    THEN sog
                    ELSE calc_speed_knots_to_prev
                END                                                  AS speed_to_determine_outlier
            FROM with_lag
        ),

        classified AS (
            SELECT
                *,
                CASE
                    WHEN seconds_to_prev IS NULL                          THEN 'first_point'
                    WHEN seconds_to_prev <= 0                             THEN 'impossible_jump'
                    WHEN speed_to_determine_outlier IS NOT NULL
                         AND speed_to_determine_outlier > {params.max_implied_speed_knots}
                                                                          THEN 'impossible_jump'
                    WHEN seconds_to_prev > {params.time_gap_seconds}             THEN 'temporal_gap'
                    ELSE 'continuous'
                END AS transition_class,

                CASE
                    WHEN sog IS NOT NULL
                         AND sog >= 0
                         AND sog <= {params.max_speed_knots}
                         AND sog < {params.stop_speed_knots}
                    THEN 'stationary'
                    WHEN calc_speed_knots_to_prev IS NOT NULL
                         AND calc_speed_knots_to_prev >= 0
                         AND calc_speed_knots_to_prev <= {params.max_implied_speed_knots}
                         AND calc_speed_knots_to_prev < {params.stop_speed_knots}
                    THEN 'stationary'
                    ELSE 'in motion'
                END AS motion_label,

                NULLIF(
                    TRIM(BOTH '|' FROM CONCAT(
                        CASE WHEN seconds_to_prev > {params.time_gap_seconds}
                             THEN 'temporal_gap|' ELSE '' END,
                        CASE WHEN speed_to_determine_outlier IS NOT NULL
                                  AND speed_to_determine_outlier > {params.max_implied_speed_knots}
                             THEN 'impossible_speed|' ELSE '' END,
                        CASE WHEN sog IS NULL
                             THEN 'missing_sog|' ELSE '' END,
                        CASE WHEN sog IS NOT NULL AND (sog < 0 OR sog > {params.max_speed_knots})
                             THEN 'invalid_sog|' ELSE '' END,
                        CASE WHEN cog IS NOT NULL AND (cog < 0 OR cog > 360)
                             THEN 'invalid_cog|' ELSE '' END,
                        CASE WHEN heading IS NOT NULL AND heading != 511
                                  AND (heading < 0 OR heading > 360)
                             THEN 'invalid_heading|' ELSE '' END
                    )),
                    ''
                ) AS quality_flags
            FROM metrics
        ),

        point_flags AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY mmsi
                    ORDER BY event_time, latitude, longitude
                ) AS point_order,
                transition_class = 'impossible_jump' AS is_outlier_point,
                CASE
                    WHEN transition_class = 'impossible_jump' THEN 'outlier'
                    WHEN motion_label = 'stationary'          THEN 'stationary'
                    ELSE 'in motion'
                END AS candidate_object_type
            FROM classified
        ),

        candidate_run_flags AS (
            SELECT
                *,
                -- Skip outlier points when looking back for the previous motion label.
                -- Outliers are transparent: a SOG spike inside a stationary block must
                -- not shatter that block into tiny sub-runs.
                arg_max(
                    CASE WHEN NOT is_outlier_point THEN candidate_object_type ELSE NULL END,
                    CASE WHEN NOT is_outlier_point THEN point_order ELSE NULL END
                ) OVER (
                    PARTITION BY mmsi
                    ORDER BY point_order
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS prev_candidate_object_type
            FROM point_flags
        ),

        candidate_runs AS (
            SELECT
                *,
                SUM(
                    CASE
                        -- Outlier points never start a new candidate run.
                        WHEN is_outlier_point                                          THEN 0
                        WHEN prev_candidate_object_type IS NULL                        THEN 1
                        -- Only a true temporal gap (not an impossible-speed jump) splits a run;
                        -- outlier-induced impossible_jump is handled by transparency above.
                        WHEN transition_class = 'temporal_gap'                         THEN 1
                        WHEN candidate_object_type != prev_candidate_object_type       THEN 1
                        ELSE 0
                    END
                ) OVER (
                    PARTITION BY mmsi
                    ORDER BY event_time, latitude, longitude
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS candidate_run_id
            FROM candidate_run_flags
        ),

        run_stats AS (
            SELECT
                *,
                -- Exclude outlier points from run-type and duration aggregates so a
                -- single bad ping inside a long stop doesn't reclassify the whole run.
                any_value(CASE WHEN NOT is_outlier_point THEN candidate_object_type END)
                    OVER w_run AS run_candidate_type,
                date_diff('second',
                    min(CASE WHEN NOT is_outlier_point THEN event_time END) OVER w_run,
                    max(CASE WHEN NOT is_outlier_point THEN event_time END) OVER w_run
                ) AS run_seconds
            FROM candidate_runs
            WINDOW w_run AS (PARTITION BY mmsi, candidate_run_id)
        ),

        point_objects AS (
            SELECT
                * EXCLUDE (run_candidate_type, run_seconds),
                CASE
                    WHEN is_outlier_point                                        THEN 'outlier'
                    WHEN run_candidate_type = 'stationary'
                         AND run_seconds >= {params.min_stop_seconds}
                    THEN 'stationary'
                    ELSE 'in motion'
                END AS object_type
            FROM run_stats
        ),

        with_previous_flags AS (
            SELECT
                *,
                -- Skip outlier points when looking back for the previous segment type.
                arg_max(
                    CASE WHEN NOT is_outlier_point THEN object_type ELSE NULL END,
                    CASE WHEN NOT is_outlier_point THEN point_order ELSE NULL END
                ) OVER (
                    PARTITION BY mmsi
                    ORDER BY point_order
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS prev_object_type
            FROM point_objects
        ),

        object_flags AS (
            SELECT
                *,
                CASE
                    WHEN transition_class = 'first_point'   THEN 'first_point'
                    WHEN transition_class = 'temporal_gap'  THEN 'temporal_gap'
                    WHEN is_outlier_point                   THEN 'outlier_point'
                    WHEN object_type != prev_object_type    THEN object_type
                    ELSE 'same_object_type'
                END AS break_reason,

                CASE
                    WHEN transition_class = 'first_point'                             THEN TRUE
                    WHEN transition_class = 'temporal_gap'                            THEN TRUE
                    -- Outliers are transparent: they never start a new segment.
                    -- Only a real type transition (stationary ↔ in motion) on a
                    -- non-outlier point creates a boundary.
                    WHEN NOT is_outlier_point
                         AND object_type IS DISTINCT FROM prev_object_type            THEN TRUE
                    ELSE FALSE
                END AS is_new_segment
            FROM with_previous_flags
        ),

        object_ids AS (
            SELECT
                *,
                SUM(CASE WHEN is_new_segment THEN 1 ELSE 0 END) OVER (
                    PARTITION BY mmsi
                    ORDER BY event_time, latitude, longitude
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS object_id
            FROM object_flags
        )

        SELECT
            event_time,
            mobile_type,
            date_id,
            time_id,

            mmsi,
            is_valid_mmsi,

            geom_4326,

            navigational_status,
            rot,
            sog,
            cog,
            heading,

            imo,
            is_valid_imo,
            callsign,
            name,
            ship_type,
            cargo_type,

            width,
            length,
            pos_fixing_device,
            draught,
            destination,
            eta,
            data_source_type,

            size_a,
            size_b,
            size_c,
            size_d,

            prev_event_time,
            seconds_to_prev,
            meters_to_prev,
            calc_speed_mps_to_prev,
            calc_speed_knots_to_prev,
            speed_to_determine_outlier,

            transition_class,
            motion_label,
            quality_flags,
            is_outlier_point,
            object_type,
            prev_object_type,
            break_reason,
            is_new_segment,
            object_id,

            source_file
        FROM object_ids

        UNION ALL

        -- Shared boundary points: genuine motion-type transitions (stationary ↔ in motion)
        -- are appended a second time to close the previous segment. Temporal-gap and
        -- first-point boundaries are excluded — those segments are disconnected.
        SELECT
            event_time,
            mobile_type,
            date_id,
            time_id,

            mmsi,
            is_valid_mmsi,

            geom_4326,

            navigational_status,
            rot,
            sog,
            cog,
            heading,

            imo,
            is_valid_imo,
            callsign,
            name,
            ship_type,
            cargo_type,

            width,
            length,
            pos_fixing_device,
            draught,
            destination,
            eta,
            data_source_type,

            size_a,
            size_b,
            size_c,
            size_d,

            prev_event_time,
            seconds_to_prev,
            meters_to_prev,
            calc_speed_mps_to_prev,
            calc_speed_knots_to_prev,
            speed_to_determine_outlier,

            transition_class,
            motion_label,
            quality_flags,
            is_outlier_point,
            prev_object_type        AS object_type,
            prev_object_type,
            'same_object_type'      AS break_reason,
            FALSE                   AS is_new_segment,
            object_id - 1           AS object_id,

            source_file
        FROM object_ids
        WHERE is_new_segment = TRUE
          AND transition_class NOT IN ('first_point', 'temporal_gap')
          AND NOT is_outlier_point;
        """
    )
