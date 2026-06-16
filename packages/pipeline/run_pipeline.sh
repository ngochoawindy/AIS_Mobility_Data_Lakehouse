set -euo pipefail

START="${START:-2026-01-01}"     # first ingest day (inclusive)
END="${END:-2026-01-31}"         # last  ingest day (inclusive)
MONTH="${MONTH:-2026-01}"        # month to build the L1..L4 layouts for

export TRIPS_DEST="${TRIPS_DEST:-s3://warehouse/trips}"

DAILY_DIR="data/layouts_daily"   
COMPACT_DIR="data/layout_compact"

NUM_SHARDS=16                    # L1 hash buckets
L2_REGION_M=50000                # L2 spaceSplit region bucket (m)  
L2_TILE_M=1000                   # L2 spaceSplit tile size (m)
L3_REGION_M=50000                # L3 MEST region bucket (m)        
L3_SEGS_PER_BOX=16               # L3 MEST segments per box
L4_TIME_BIN="1 hour"            # L4 timeSplit bin

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO/packages/cli"

LAKEHOUSE=".venv/bin/lakehouse"
PYTHON=".venv/bin/python"
[ -x "$LAKEHOUSE" ] || LAKEHOUSE="lakehouse"      # fall back to an active venv
[ -x "$PYTHON" ]    || PYTHON="python"

stage() { echo; echo "==================== $* ===================="; echo; }

if [[ "$TRIPS_DEST" == s3://* ]]; then
    # shellcheck disable=SC1091
    source iceberg_rest/env.rest
    echo "trips dest = $TRIPS_DEST  (writing to object storage; MinIO+REST must be up)"
fi

############################ DATA BUILD  ############################

stage "ingest raw  ($START .. $END)"
"$LAKEHOUSE" pipeline ingest-range "$START" "$END"

stage "build L0 base segments"
"$LAKEHOUSE" pipeline build-l0

stage "build daily layouts L1..L4 ($MONTH)"
"$LAKEHOUSE" pipeline build-layout L1 --month "$MONTH" --granularity daily \
    --layout-dir "$DAILY_DIR" --num-shards "$NUM_SHARDS"
"$LAKEHOUSE" pipeline build-layout L2 --month "$MONTH" --granularity daily \
    --layout-dir "$DAILY_DIR" --region-size-m "$L2_REGION_M" --tile-size-m "$L2_TILE_M"
"$LAKEHOUSE" pipeline build-layout L3 --month "$MONTH" --granularity daily \
    --layout-dir "$DAILY_DIR" --region-size-m "$L3_REGION_M" --segs-per-box "$L3_SEGS_PER_BOX"
"$LAKEHOUSE" pipeline build-layout L4 --month "$MONTH" --granularity daily \
    --layout-dir "$DAILY_DIR" --time-bin "$L4_TIME_BIN"

stage "compact daily -> compact layouts L1..L4 ($MONTH)"
for L in L1 L2 L3 L4; do
    "$LAKEHOUSE" pipeline compact-layout "$L" --month "$MONTH" \
        --src-dir "$DAILY_DIR" --dst-dir "$COMPACT_DIR"
done

stage "project layouts -> trips schema ($TRIPS_DEST)"
"$PYTHON" scripts/build_trips.py

stage "build Hilbert layout LH"
"$PYTHON" scripts/build_hilbert.py

stage "build sorted-compact layouts (L1s..L4s, LHs)"
"$PYTHON" scripts/build_sorted_compact.py

stage "done, trips layers built at $TRIPS_DEST"
echo "raw     : data/raw/aisdk-*.parquet            (local)"
echo "sources : data/L0 , $DAILY_DIR , $COMPACT_DIR  (local, build inputs)"
echo "trips   : $TRIPS_DEST/<layout>/   (11-col schema: the query input, Parquet in MinIO)"
 