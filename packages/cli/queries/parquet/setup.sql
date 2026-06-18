INSTALL httpfs;   LOAD httpfs;
LOAD '<path-to>/mobilityduck.duckdb_extension';
INSTALL spatial;  LOAD spatial;

-- Credentials for the data files on MinIO (s3://warehouse).
CREATE SECRET (
    TYPE S3, KEY_ID 'admin', SECRET 'password',
    ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1'
);

SET VARIABLE trips_glob = 's3://warehouse/trips/layout_compact/L2s/**/*.parquet';
SET VARIABLE t0   = TIMESTAMP '2026-01-15 08:00:00';   -- window start
SET VARIABLE t1   = TIMESTAMP '2026-01-16 08:00:00';   -- window end (day)
SET VARIABLE tmid = TIMESTAMP '2026-01-15 20:00:00';   -- window midpoint
SET VARIABLE d0   = DATE '2026-01-15';                 -- first date partition
SET VARIABLE d1   = DATE '2026-01-16';                 -- last  date partition (day)
