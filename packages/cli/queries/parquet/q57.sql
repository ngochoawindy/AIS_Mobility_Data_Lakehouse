WITH rodby AS (
    SELECT DISTINCT mmsi
    FROM read_parquet(getvariable('trips_glob')) AS trips
    WHERE xmin <= 651422.0 AND xmax >= 651135.0
      AND ymin <= 6058548.0 AND ymax >= 6058230.0
      AND tmin < getvariable('t1') AND tmax > getvariable('t0')
      AND dt BETWEEN getvariable('d0') AND getvariable('d1')
      AND eIntersects(tgeompointFromEWKB(traj),
                      ST_MakeEnvelope(651135.0, 6058230.0, 651422.0, 6058548.0))
),
puttgarden AS (
    SELECT DISTINCT mmsi
    FROM read_parquet(getvariable('trips_glob')) AS trips
    WHERE xmin <= 644896.0 AND xmax >= 644339.0
      AND ymin <= 6042487.0 AND ymax >= 6042108.0
      AND tmin < getvariable('t1') AND tmax > getvariable('t0')
      AND dt BETWEEN getvariable('d0') AND getvariable('d1')
      AND eIntersects(tgeompointFromEWKB(traj),
                      ST_MakeEnvelope(644339.0, 6042108.0, 644896.0, 6042487.0))
)
SELECT COUNT(*) AS n_both
FROM rodby r JOIN puttgarden pg USING (mmsi)
;
