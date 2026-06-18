WITH belt AS (
    SELECT ST_MakeEnvelope(640730.0, 6042487.0, 654100.0, 6058230.0) AS g
),
clipped AS (
    SELECT mmsi,
           atGeometry(
               atTime(tgeompointFromEWKB(traj),
                      span(getvariable('t0'),
                           getvariable('t1'), true, false)),
               belt.g) AS seg
    FROM trips, belt
    WHERE xmin <= 654100.0 AND xmax >= 640730.0
      AND ymin <= 6058230.0 AND ymax >= 6042487.0
      AND tmin < getvariable('t1') AND tmax > getvariable('t0')
      AND dt BETWEEN getvariable('d0') AND getvariable('d1')
)
SELECT COUNT(DISTINCT mmsi)                   AS n_vessels,
       COUNT(*) FILTER (WHERE seg IS NOT NULL) AS n_clips
FROM clipped
WHERE seg IS NOT NULL
;
