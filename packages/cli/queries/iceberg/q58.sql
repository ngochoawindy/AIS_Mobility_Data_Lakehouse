WITH port AS (
    SELECT ST_MakeEnvelope(666538.0, 6392057.0, 679171.0, 6403745.0) AS g
),
cand AS (
    SELECT mmsi,
           atTime(tgeompointFromEWKB(traj),
                  span(getvariable('t0'),
                       getvariable('t1'), true, false)) AS trip
    FROM trips
    WHERE xmin <= 679171.0 AND xmax >= 666538.0
      AND ymin <= 6403745.0 AND ymax >= 6392057.0
      AND tmin < getvariable('t1') AND tmax > getvariable('t0')
)
SELECT COUNT(DISTINCT mmsi) AS n_vessels
FROM cand, port
WHERE trip IS NOT NULL AND eIntersects(trip, port.g)
;
