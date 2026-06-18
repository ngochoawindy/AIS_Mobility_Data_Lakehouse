INSTALL iceberg;  LOAD iceberg;
INSTALL httpfs;   LOAD httpfs;
LOAD '<path-to>/mobilityduck.duckdb_extension';
INSTALL spatial;  LOAD spatial;

-- Credentials for the data files on MinIO (s3://warehouse).
CREATE SECRET (
    TYPE S3, KEY_ID 'admin', SECRET 'password',
    ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1'
);

-- Attach the Iceberg REST catalog: tables now resolve by NAME (lake.ais.<layout>),
-- the catalog handles file resolution + manifest pruning.
ATTACH '' AS lake (
    TYPE ICEBERG, ENDPOINT 'http://localhost:8181', AUTHORIZATION_TYPE 'none'
);

-- Expose the chosen layout as `trips`. Swap the layout to compare pruning, e.g.
--   CREATE OR REPLACE VIEW trips AS SELECT * FROM lake.ais.L3s;
CREATE OR REPLACE VIEW trips AS SELECT * FROM lake.ais.L2s;

-- These are the DAY defaults (so the .sql run standalone); scripts/run_queries.py
-- overrides them per selectivity. Windows are anchored at 2026-01-15 08:00 
-- and extend by the duration: hour=+1h, day=+1day, week=+7days. tmid = midpoint.
SET VARIABLE t0   = TIMESTAMP '2026-01-15 08:00:00';   -- window start
SET VARIABLE t1   = TIMESTAMP '2026-01-16 08:00:00';   -- window end (day)
SET VARIABLE tmid = TIMESTAMP '2026-01-15 20:00:00';   -- window midpoint
SET VARIABLE d0   = DATE '2026-01-15';                 -- first date partition
SET VARIABLE d1   = DATE '2026-01-16';                 -- last  date partition (day)
