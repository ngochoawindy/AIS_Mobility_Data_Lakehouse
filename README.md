# AIS Mobility Data lakehouse 

## Install

```bash
uv venv packages/cli/.venv
uv pip install --python packages/cli/.venv/bin/python -e packages/pipeline -e packages/cli
```

`packages/cli/.env` sets `MOBILITYDUCK__EXTENSION_PATH` (a local unsigned MobilityDuck build). The
`lakehouse` console command and the `scripts/` are run from `packages/cli/`.

## How it works

### 1. Build: `../pipeline/run_pipeline.sh` (raw -> all layouts -> Parquet in MinIO)

One script runs these stages in order. The raw / L0 / daily / compact source layouts stay local (build inputs); the final `trips` layers are written to MinIO
(`TRIPS_DEST=s3://warehouse/trips`):

| stage | command / file | output |
|---|---|---|
| ingest | `lakehouse pipeline ingest-range` | `data/raw/aisdk-*.parquet` (raw AIS, local) |
| build L0 | `lakehouse pipeline build-l0` | `data/L0/…` (base trajectory segments, local) |
| daily layouts | `lakehouse pipeline build-layout L1..L4` | `data/layouts_daily/L{1..4}/` (local) |
| compact | `lakehouse pipeline compact-layout` | `data/layout_compact/L{1..4}/` (local) |
| trips schema | `scripts/build_trips.py` | `$TRIPS_DEST/<layout>/` (the 11-col query schema) |
| Hilbert | `scripts/build_hilbert.py` | `$TRIPS_DEST/.../LH` |
| sorted-compact | `scripts/build_sorted_compact.py` | `$TRIPS_DEST/.../{L1s,L2s,L3s,L4s,LHs}` |

### 2. Register in the Iceberg catalog (`scripts/iceberg_to_s3.py`)

pyiceberg writes each layout's table metadata
(manifests + the min/max stats used for pruning) into object storage and updates the catalog pointer.
So the data + metadata live on MinIO; the REST catalog is a thin pointer service.

```bash
docker compose -f iceberg_rest/docker-compose.yml up -d   # MinIO + Iceberg REST
source iceberg_rest/env.rest
python scripts/iceberg_to_s3.py all                       # register every layout as ais.<layout>
```

### 3. Query + measure `scripts/run_queries.py`

```bash
source iceberg_rest/env.rest
python scripts/run_queries.py --iters 3 --sels hour,day,week
```
### 4. The analysis `evaluation.ipynb`