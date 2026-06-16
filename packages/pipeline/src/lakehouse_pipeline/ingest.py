from datetime import date, timedelta
from pathlib import Path
from shutil import copy2
from urllib.parse import urlparse
from zipfile import ZipFile

import duckdb
import requests
from time import perf_counter

from lakehouse_pipeline._utils import _sql_str

DMA_BASE_URL = "http://aisdata.ais.dk/"

REQUIRED_COLUMNS: dict[str, str] = {
    "timestamp": "event_time",
    "basedatetime": "event_time",
    "base_datetime": "event_time",
    "type of mobile": "mobile_type",
    "mmsi": "mmsi",
    "latitude": "latitude",
    "lat": "latitude",
    "longitude": "longitude",
    "lon": "longitude",
    "navigational status": "navigational_status",
    "status": "navigational_status",
    "rot": "rot",
    "sog": "sog",
    "cog": "cog",
    "heading": "heading",
    "imo": "imo",
    "callsign": "callsign",
    "call sign": "callsign",
    "vesselname": "name",
    "vessel name": "name",
    "name": "name",
    "ship type": "ship_type",
    "vesseltype": "ship_type",
    "vessel type": "ship_type",
    "cargo type": "cargo_type",
    "cargo": "cargo_type",
    "width": "width",
    "length": "length",
    "draft": "draught",
    "type of position fixing device": "pos_fixing_device",
    "draught": "draught",
    "destination": "destination",
    "eta": "eta",
    "data source type": "data_source_type",
    "transceiverclass": "data_source_type",
    "transceiver class": "data_source_type",
    "a": "size_a",
    "size a": "size_a",
    "b": "size_b",
    "size b": "size_b",
    "c": "size_c",
    "size c": "size_c",
    "d": "size_d",
    "size d": "size_d",
}

CANONICAL_COLUMNS: list[str] = [
    "event_time",
    "mobile_type",
    "mmsi",
    "latitude",
    "longitude",
    "navigational_status",
    "rot",
    "sog",
    "cog",
    "heading",
    "imo",
    "callsign",
    "name",
    "ship_type",
    "cargo_type",
    "width",
    "length",
    "pos_fixing_device",
    "draught",
    "destination",
    "eta",
    "data_source_type",
    "size_a",
    "size_b",
    "size_c",
    "size_d",
]

_TIMESTAMP_COLS = {"event_time", "eta"}
_UBIGINT_COLS   = {"mmsi", "imo"}
_DOUBLE_COLS    = {
    "latitude", "longitude", "rot", "sog", "cog", "heading",
    "width", "length", "draught", "size_a", "size_b", "size_c", "size_d",
}


def _norm_col(name: str) -> str:
    return str(name).strip().lower().lstrip("#").strip()


def _source_expr(column_lookup: dict[str, str], logical: str) -> str:
    for src, dst in REQUIRED_COLUMNS.items():
        if dst == logical and src in column_lookup:
            raw = column_lookup[src]
            return '"' + raw.replace('"', '""') + '"'
    return "NULL"


def _ts_expr(raw: str) -> str:
    return (
        f"COALESCE("
        f"TRY_STRPTIME(NULLIF(TRIM({raw}), ''), '%d/%m/%Y %H:%M:%S'), "
        f"TRY_STRPTIME(NULLIF(TRIM({raw}), ''), '%Y-%m-%dT%H:%M:%S'), "
        f"TRY_STRPTIME(NULLIF(TRIM({raw}), ''), '%Y-%m-%d %H:%M:%S'), "
        f"TRY_CAST(NULLIF(TRIM({raw}), '') AS TIMESTAMP)"
        f")"
    )


def _double_expr(raw: str) -> str:
    return f"TRY_CAST(REPLACE(NULLIF(TRIM({raw}), ''), ',', '.') AS DOUBLE)"


def _ubigint_expr(raw: str) -> str:
    return f"TRY_CAST(REPLACE(NULLIF(TRIM({raw}), ''), ',', '.') AS UBIGINT)"


def _string_expr(raw: str) -> str:
    return f"NULLIF(TRIM({raw}), '')"


def _csv_to_raw_parquet(csv_path: Path, *, keep_csv: bool = False) -> Path:
    parquet_path = csv_path.with_suffix(".parquet")

    with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
        header = f.readline()
    delimiter = ";" if header.count(";") > header.count(",") else ","

    with duckdb.connect(":memory:") as con:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW src AS
            SELECT * FROM read_csv(
                {_sql_str(csv_path.as_posix())},
                header        = true,
                delim         = {_sql_str(delimiter)},
                all_varchar   = true,
                ignore_errors = true
            );
            """
        )

        columns = [row[0] for row in con.execute("DESCRIBE src").fetchall()]
        column_lookup = {_norm_col(c): c for c in columns}

        missing = [
            c for c in ["event_time", "mmsi", "latitude", "longitude"]
            if _source_expr(column_lookup, c) == "NULL"
        ]
        if missing:
            raise ValueError(
                f"{csv_path.name}: missing required AIS columns after normalization: {missing}"
            )

        exprs: list[str] = []
        for col in CANONICAL_COLUMNS:
            src = _source_expr(column_lookup, col)
            if src == "NULL":
                exprs.append(f"NULL AS {col}")
            elif col in _TIMESTAMP_COLS:
                exprs.append(f"{_ts_expr(src)} AS {col}")
            elif col in _UBIGINT_COLS:
                exprs.append(f"{_ubigint_expr(src)} AS {col}")
            elif col in _DOUBLE_COLS:
                exprs.append(f"{_double_expr(src)} AS {col}")
            else:
                exprs.append(f"{_string_expr(src)} AS {col}")

        con.execute(
            f"""
            COPY (SELECT {", ".join(exprs)} FROM src)
            TO {_sql_str(parquet_path.as_posix())}
            (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )

    if not keep_csv:
        csv_path.unlink(missing_ok=True)

    return parquet_path


def _download_file(url: str, target: Path) -> Path:
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with target.open("wb") as out:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    out.write(chunk)
    return target


def _extract_csv_from_zip(zip_path: Path, raw_dir: Path) -> Path:
    with ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV file found in zip archive: {zip_path}")
        out_path = raw_dir / Path(csv_names[0]).name
        with zf.open(csv_names[0]) as src, out_path.open("wb") as dst:
            dst.write(src.read())
    return out_path


def _handle_zip(
    zip_path: Path, raw_dir: Path, *, keep_zip: bool, keep_csv: bool
) -> Path:
    csv_path = _extract_csv_from_zip(zip_path, raw_dir)
    if not keep_zip:
        zip_path.unlink(missing_ok=True)
    return _csv_to_raw_parquet(csv_path, keep_csv=keep_csv)


def ingest_dma_day(
    day: date,
    raw_dir: Path,
    *,
    keep_zip: bool = False,
    keep_csv: bool = False,
) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    zip_name = f"aisdk-{day.isoformat()}.zip"
    zip_path = raw_dir / zip_name

    t0 = perf_counter()
    _download_file(f"{DMA_BASE_URL}{zip_name}", zip_path)
    t_download_s = perf_counter() - t0

    t_extract = perf_counter()
    csv_path = _extract_csv_from_zip(zip_path, raw_dir)
    t_extract_s = perf_counter() - t_extract

    if not keep_zip:
        zip_path.unlink(missing_ok=True)

    t_convert = perf_counter()
    parquet_path = _csv_to_raw_parquet(csv_path, keep_csv=keep_csv)
    t_convert_s = perf_counter() - t_convert

    t_total_s = perf_counter() - t0
    print(
        f"[ingest-timing] file={zip_name} "
        f"download_s={t_download_s:.3f} extract_s={t_extract_s:.3f} "
        f"convert_s={t_convert_s:.3f} total_s={t_total_s:.3f}"
    )
    return parquet_path


def ingest_dma_range(
    start_day: date,
    end_day: date,
    raw_dir: Path,
    *,
    keep_zip: bool = False,
    keep_csv: bool = False,
) -> list[Path]:
    if end_day < start_day:
        raise ValueError("end_day must be on or after start_day.")
    outputs: list[Path] = []
    current = start_day
    while current <= end_day:
        outputs.append(
            ingest_dma_day(current, raw_dir, keep_zip=keep_zip, keep_csv=keep_csv)
        )
        current += timedelta(days=1)
    return outputs


def _dispatch_target(
    target: Path,
    raw_dir: Path,
    *,
    keep_zip: bool,
    keep_csv: bool,
) -> Path:
    suffix = target.suffix.lower()
    if suffix == ".zip":
        return _handle_zip(target, raw_dir, keep_zip=keep_zip, keep_csv=keep_csv)
    if suffix == ".csv":
        return _csv_to_raw_parquet(target, keep_csv=keep_csv)
    return target


def ingest_dma_source(
    source: str,
    raw_dir: Path,
    filename: str | None = None,
    *,
    keep_zip: bool = False,
    keep_csv: bool = False,
) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)

    if source.startswith("http://") or source.startswith("https://"):
        parsed = urlparse(source)
        inferred_name = Path(parsed.path).name or "ais_input"
        target = raw_dir / (filename or inferred_name)
        _download_file(source, target)
        return _dispatch_target(target, raw_dir, keep_zip=keep_zip, keep_csv=keep_csv)

    source_path = Path(source)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file does not exist: {source}")

    target = raw_dir / (filename or source_path.name)
    if source_path.suffix.lower() == ".parquet":
        copy2(source_path, target)
        return target

    copy2(source_path, target)
    return _dispatch_target(target, raw_dir, keep_zip=keep_zip, keep_csv=keep_csv)
