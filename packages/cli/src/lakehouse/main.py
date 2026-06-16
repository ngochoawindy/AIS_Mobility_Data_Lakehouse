from typer import Typer

from lakehouse import pipeline

app = Typer()

app.add_typer(
    pipeline.cli,
    name="pipeline",
    help="Ingest raw DMA parquet, then build L0–L4 GeoParquet layouts.",
)
