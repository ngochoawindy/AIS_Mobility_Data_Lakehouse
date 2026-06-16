from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineParams:

    metric_srid: str = "+proj=utm +zone=32 +datum=WGS84 +units=m +no_defs"
    metric_epsg: int = 32632
    time_gap_seconds: int = 600
    max_speed_knots: float = 100.0
    max_implied_speed_knots: float = 100.0
    stop_speed_knots: float = 1.0
    min_stop_seconds: int = 300
    min_motion_points: int = 3
    max_vessel_outlier_pct: float = 0.5

    study_area_lonlat_bbox: tuple[float, float, float, float] | None = (
        -16.1, 32.88, 40.18, 84.17,
    )
