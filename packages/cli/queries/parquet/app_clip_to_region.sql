CREATE OR REPLACE TEMP TABLE mt AS
SELECT mmsi, ship_type, traj FROM read_parquet(getvariable('trips_glob')) AS trips
WHERE dt BETWEEN getvariable('d0') AND getvariable('d1')
  AND tmax >= getvariable('t0') AND tmin <= getvariable('t1')
  AND xmax >= 640730.0 AND xmin <= 654100.0
  AND ymax >= 6042487.0 AND ymin <= 6058230.0;

WITH clipped AS (
    SELECT mmsi, ship_type, g FROM (
        SELECT mmsi, ship_type,
               atStbox(tgeompointFromEWKB(traj),
                       stbox(ST_MakeEnvelope(640730.0, 6042487.0, 654100.0, 6058230.0), span(getvariable('t0'), getvariable('t1'), true, false))) AS g
        FROM mt
    ) WHERE g IS NOT NULL
)
SELECT count(DISTINCT mmsi) AS n_in_belt FROM clipped;
