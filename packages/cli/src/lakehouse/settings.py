from functools import cache
from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from lakehouse_pipeline._params import PipelineParams


_DEFAULTS = PipelineParams()


class MobilityDuckSettings(BaseModel):
    extension_name: str = "mobilityduck"
    extension_path: str | None = None
    allow_unsigned_extensions: bool = True


class PipelineSettings(BaseModel):
    raw_dir: Path = Path("./data/raw")
    l0_dir: Path = Path("./data/L0")
    layout_dir: Path = Path("./data/layouts")

    metric_srid: str = _DEFAULTS.metric_srid
    metric_epsg: int = _DEFAULTS.metric_epsg
    time_gap_seconds: int = _DEFAULTS.time_gap_seconds
    max_speed_knots: float = _DEFAULTS.max_speed_knots
    max_implied_speed_knots: float = _DEFAULTS.max_implied_speed_knots
    stop_speed_knots: float = _DEFAULTS.stop_speed_knots
    min_stop_seconds: int = _DEFAULTS.min_stop_seconds
    min_motion_points: int = _DEFAULTS.min_motion_points
    max_vessel_outlier_pct: float = _DEFAULTS.max_vessel_outlier_pct
    study_area_lonlat_bbox: tuple[float, float, float, float] | None = (
        _DEFAULTS.study_area_lonlat_bbox
    )

    duckdb_memory_limit: str | None = None
    duckdb_temp_directory: Path | None = None
    duckdb_max_temp_directory_size: str | None = None
    duckdb_threads: int = 2

    def to_params(self) -> PipelineParams:
        return PipelineParams(
            metric_srid=self.metric_srid,
            metric_epsg=self.metric_epsg,
            time_gap_seconds=self.time_gap_seconds,
            max_speed_knots=self.max_speed_knots,
            max_implied_speed_knots=self.max_implied_speed_knots,
            stop_speed_knots=self.stop_speed_knots,
            min_stop_seconds=self.min_stop_seconds,
            min_motion_points=self.min_motion_points,
            max_vessel_outlier_pct=self.max_vessel_outlier_pct,
            study_area_lonlat_bbox=self.study_area_lonlat_bbox,
        )


class Settings(BaseSettings):
    mobilityduck: MobilityDuckSettings = MobilityDuckSettings()
    pipeline: PipelineSettings = PipelineSettings()

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=".env",
        case_sensitive=False,
        env_nested_delimiter="__",
    )

    @staticmethod
    @cache
    def create() -> "Settings":
        return Settings()  # type: ignore
