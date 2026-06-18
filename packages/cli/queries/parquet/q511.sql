WITH sub AS (
    SELECT mmsi,
           unnest(sequences(atStbox(
               atTime(tgeompointFromEWKB(traj),
                      span(getvariable('t0'),
                           getvariable('t1'), true, false)),
               ST_MakeEnvelope(640730.0, 6042487.0, 654100.0, 6058230.0)::STBOX, true))) AS trip
    FROM read_parquet(getvariable('trips_glob')) AS trips
    WHERE xmin <= 654100.0 AND xmax >= 640730.0
      AND ymin <= 6058230.0 AND ymax >= 6042487.0
      AND tmin < getvariable('t1') AND tmax > getvariable('t0')
      AND dt BETWEEN getvariable('d0') AND getvariable('d1')
),
cand AS (
    SELECT mmsi, trip,
           startTimestamp(trip) AS t0, endTimestamp(trip) AS t1,
           Xmin(stbox(trip)) AS xmin, Ymin(stbox(trip)) AS ymin,
           Xmax(stbox(trip)) AS xmax, Ymax(stbox(trip)) AS ymax
    FROM sub WHERE trip IS NOT NULL
)
SELECT COUNT(*) AS n_pairs FROM (
    SELECT DISTINCT a.mmsi AS m1, b.mmsi AS m2
    FROM cand a JOIN cand b
      ON a.mmsi < b.mmsi AND a.t0 < b.t1 AND a.t1 > b.t0
     AND a.xmin <= b.xmax + 300 AND a.xmax >= b.xmin - 300
     AND a.ymin <= b.ymax + 300 AND a.ymax >= b.ymin - 300
     AND eDwithin(a.trip, b.trip, 300)
)
;
