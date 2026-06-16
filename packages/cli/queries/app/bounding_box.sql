CREATE OR REPLACE TEMP TABLE mt AS
SELECT mmsi, ship_type, traj FROM trips
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
SELECT round(avg((x1 - x0) * (y1 - y0) / 1e6), 2) AS avg_bbox_km2 FROM (
    SELECT mmsi, min(x0) x0, max(x1) x1, min(y0) y0, max(y1) y1 FROM (
        SELECT mmsi, ST_XMin(trajectory(g)) x0, ST_XMax(trajectory(g)) x1,
               ST_YMin(trajectory(g)) y0, ST_YMax(trajectory(g)) y1 FROM clipped
    ) GROUP BY mmsi
);
