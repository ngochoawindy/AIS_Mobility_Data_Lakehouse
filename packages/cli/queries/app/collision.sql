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
),
ext AS (SELECT mmsi, g, ST_XMin(e) x0, ST_XMax(e) x1, ST_YMin(e) y0, ST_YMax(e) y1
        FROM (SELECT mmsi, g, ST_Extent(trajectory(g)) e FROM clipped) WHERE e IS NOT NULL),
cand AS (SELECT a.mmsi m1, b.mmsi m2, a.g t1, b.g t2 FROM ext a JOIN ext b
         ON a.mmsi < b.mmsi
        AND a.x0 <= b.x1 + 300 AND b.x0 <= a.x1 + 300
        AND a.y0 <= b.y1 + 300 AND b.y0 <= a.y1 + 300)
SELECT count(DISTINCT (m1, m2)) AS n_pairs_300m FROM cand
WHERE nearestApproachDistance(t1, t2) < 300;
