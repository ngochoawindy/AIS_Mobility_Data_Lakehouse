"""DuckDB connection helper: fresh per-day connection with spatial + MobilityDuck"""

from contextlib import contextmanager
from pathlib import Path

import duckdb

from lakehouse_pipeline._utils import _sql_str


@contextmanager
def open_pipeline_conn(
    *,
    spill_dir: Path,
    extension_path: str | None,
    extension_name: str = "mobilityduck",
    allow_unsigned_extensions: bool = True,
    memory_limit: str | None = None,
    threads: int = 2,
    max_temp_directory_size: str | None = None,
):
    spill_dir.mkdir(parents=True, exist_ok=True)

    config: dict = {}
    if allow_unsigned_extensions:
        config["allow_unsigned_extensions"] = "true"

    con = duckdb.connect(":memory:", config=config)
    try:
        con.execute(f"PRAGMA threads={max(1, int(threads))};")
        con.execute("SET preserve_insertion_order = false;")
        con.execute(f"SET temp_directory = {_sql_str(spill_dir.as_posix())};")
        if max_temp_directory_size:
            con.execute(
                f"SET max_temp_directory_size = {_sql_str(max_temp_directory_size)};"
            )
        if memory_limit:
            con.execute(f"SET memory_limit = {_sql_str(memory_limit)};")

        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        if extension_path:
            con.execute(f"LOAD {_sql_str(Path(extension_path).as_posix())};")
        else:
            con.execute(f"INSTALL {extension_name};")
            con.execute(f"LOAD {extension_name};")
        yield con
    finally:
        con.close()
