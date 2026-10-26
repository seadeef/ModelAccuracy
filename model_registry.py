#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta


# Shared CONUS crop bounds (west, south, east, north).
# Used by downloaders to crop model output to CONUS before saving .npy files.
# compute_tiles uses a narrower US_BOUNDS for visible-CONUS rendering.
US_CROP_BOUNDS = (-130.0, 20.0, -60.0, 55.0)


@dataclass(frozen=True)
class VersionWindow:
    """One model-version era, bounded by *init* (cycle) dates, inclusive.

    ``start``/``end`` of None mean open-ended. ``role``:
      excluded — runs never used in pooled products (e.g. AIFS pre-v1.1);
      clean    — the era pooled products are built from while it is primary;
      current  — the operational era (may equal the primary window).
    Windows bound init dates because a version change swaps the *run*; the
    valid-date filter for pooled products is derived (see
    ``ModelConfig.valid_date_in_primary_window``).
    """

    label: str
    start: date | None
    end: date | None
    role: str  # "excluded" | "clean" | "current"

    def contains(self, day: date) -> bool:
        return ((self.start is None or day >= self.start)
                and (self.end is None or day <= self.end))

    def as_json(self) -> dict:
        return {
            "label": self.label,
            "start": self.start.isoformat() if self.start else None,
            "end": self.end.isoformat() if self.end else None,
            "role": self.role,
        }


@dataclass(frozen=True)
class ModelConfig:
    key: str
    label: str
    downloader_class_path: str  # e.g. "downloaders.gfs_downloader.GFSFilteredDownloaderParallel"
    ident: str = ""  # optional 4-char UI identifier; empty → derived from label in frontend
    downloader_defaults: dict = field(default_factory=dict)
    lead_days_min: int = 1
    lead_days_max: int = 14
    # Forecast variables this model exposes (keys into FORECAST_VARIABLES).
    # The base resolves these in extract_forecast to emit forecast/{key}/ folders.
    variables: tuple[str, ...] = ("precip",)
    cycle_hour: int = 12
    data_dir: str = "model_data/gfs"
    # Hours after the cycle hour at which the full forecast is available to
    # download. Used by --forecast mode to pick today vs. yesterday.
    publish_delay_hours: int = 14
    default: bool = False
    # Model-version eras (init-date bounded) and the label of the window all
    # pooled products are built from. Per-date artifacts (consistency_daily,
    # the basin history npz) stay full-record and carry these as metadata.
    version_windows: tuple[VersionWindow, ...] = ()
    primary_window: str = ""

    @property
    def forecast_hours(self) -> list[int]:
        return [d * 24 for d in range(self.lead_days_min, self.lead_days_max + 1)]

    def window_for(self, init_date: date) -> VersionWindow | None:
        for window in self.version_windows:
            if window.contains(init_date):
                return window
        return None

    @property
    def primary(self) -> VersionWindow | None:
        for window in self.version_windows:
            if window.label == self.primary_window:
                return window
        if self.version_windows:
            raise ValueError(
                f"{self.key}: primary_window {self.primary_window!r} names no "
                "declared version window")
        return None

    def valid_date_in_primary_window(self, valid_date: date) -> bool:
        """True when *every* contributing init falls in the primary window.

        A valid date V pools runs initialized V-lead_max .. V-lead_min, so V
        is version-clean only if the whole init span is inside the window —
        pooled products never mix runs across a version switch.
        """
        window = self.primary
        if window is None:
            return True
        return (window.contains(valid_date - timedelta(days=self.lead_days_max))
                and window.contains(valid_date - timedelta(days=self.lead_days_min)))

    def primary_valid_range(self) -> tuple[date | None, date | None]:
        """Derived inclusive valid-date span of the primary window (or opens)."""
        window = self.primary
        if window is None:
            return None, None
        start = (window.start + timedelta(days=self.lead_days_max)
                 if window.start else None)
        end = (window.end + timedelta(days=self.lead_days_min)
               if window.end else None)
        return start, end

    def window_block(self) -> dict:
        """The metadata block pooled artifacts and exports record (W1)."""
        window = self.primary
        valid_start, valid_end = self.primary_valid_range()
        return {
            "windows": [w.as_json() for w in self.version_windows],
            "primary": window.as_json() if window else None,
            "primary_valid_range": [
                valid_start.isoformat() if valid_start else None,
                valid_end.isoformat() if valid_end else None,
            ],
        }

    def get_downloader_class(self) -> type:
        module_path, class_name = self.downloader_class_path.rsplit(".", 1)
        import importlib
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)


MODEL_REGISTRY: dict[str, ModelConfig] = {
    "gfs": ModelConfig(
        key="gfs",
        label="GFS",
        downloader_class_path="downloaders.gfs_downloader.GFSFilteredDownloaderParallel",
        downloader_defaults={
            "output_dir": "model_data/gfs",
            "max_workers": 16,
            "max_retries": 3,
            "timeout_seconds": 120,
        },
        lead_days_min=1,
        lead_days_max=14,
        variables=("precip", "temp", "wind", "humidity"),
        cycle_hour=12,
        data_dir="model_data/gfs",
        publish_delay_hours=14,
        default=True,
        # GFSv16 operational since 2021; no upgrade inside the record.
        version_windows=(VersionWindow("v16", None, None, "current"),),
        primary_window="v16",
    ),
    "nbm": ModelConfig(
        key="nbm",
        label="NBM",
        downloader_class_path="downloaders.nbm_downloader.NBMDownloaderParallel",
        downloader_defaults={
            "output_dir": "model_data/nbm",
            "max_workers": 16,
            "max_retries": 3,
            "timeout_seconds": 120,
        },
        lead_days_min=1,
        lead_days_max=11,
        variables=("precip", "temp", "wind", "humidity"),
        cycle_hour=12,
        data_dir="model_data/nbm",
        publish_delay_hours=5,
        # NBM v5.0 went operational with the 2026-05-05 13Z run (SCN 26-24
        # scheduled "on or about" Apr 30 with a Critical Weather Day fallback;
        # MDL's version history records May 5, 2026 — pinned 2026-07-20).
        # This archive is the 12Z cycle: the 05-05 12Z run is still v4.3, so
        # the last v4.3 init is 2026-05-05 and the first v5.0 init 2026-05-06.
        # v5.0 ingests ECMWF AIFS (ECAIFS) and AIGFS — cross-model IC coupling
        # disclosed in the typology figure.
        version_windows=(
            VersionWindow("v4.3", None, date(2026, 5, 5), "clean"),
            VersionWindow("v5.0", date(2026, 5, 6), None, "current"),
        ),
        primary_window="v4.3",
    ),
    "weathernext2": ModelConfig(
        key="weathernext2",
        label="WeatherNext 2",
        ident="WNXT",
        downloader_class_path="downloaders.weathernext2_downloader.WeatherNext2DownloaderParallel",
        downloader_defaults={
            "output_dir": "model_data/weathernext2",
            "max_workers": 8,
            "max_retries": 3,
            "timeout_seconds": 300,
        },
        lead_days_min=1,
        lead_days_max=10,
        variables=("precip", "temp", "wind", "humidity"),
        cycle_hour=12,
        data_dir="model_data/weathernext2",
        publish_delay_hours=16,
        # WeatherNext changelog checked 2026-07-20: no WN2 model update inside
        # the record (the 2026-07-15 deprecation retires the *older* WeatherNext
        # Gen/Graph datasets, not WN2). Single window until Google says otherwise.
        version_windows=(VersionWindow("wn2", None, None, "current"),),
        primary_window="wn2",
    ),
    "aifs": ModelConfig(
        key="aifs",
        label="AIFS",
        downloader_class_path="downloaders.aifs_downloader.AIFSDownloaderParallel",
        downloader_defaults={
            "output_dir": "model_data/aifs",
            # ECMWF's public S3 bucket throttles aggressively (503 "Slow Down");
            # keep concurrency modest. Base downloader handles 429/5xx backoff.
            "max_workers": 8,
            "max_retries": 3,
            "timeout_seconds": 120,
        },
        lead_days_min=1,
        lead_days_max=14,
        variables=("precip", "temp", "wind", "humidity"),
        cycle_hour=12,
        data_dir="model_data/aifs",
        publish_delay_hours=6,
        # pre-v1.1 runs (before 2025-08-27) are excluded
        # from all pooled products; v1.1 is the clean window until v2 (first
        # init 2026-05-12) accumulates a comparable record. Never pooled across
        # the switch.
        version_windows=(
            VersionWindow("pre-v1.1", None, date(2025, 8, 26), "excluded"),
            VersionWindow("v1.1", date(2025, 8, 27), date(2026, 5, 11), "clean"),
            VersionWindow("v2", date(2026, 5, 12), None, "current"),
        ),
        primary_window="v1.1",
    ),
    # Registered after aifs: the frontend assigns model hues positionally
    # from registry order, so appending keeps every existing color identity.
    "ifs": ModelConfig(
        key="ifs",
        label="IFS",
        ident="IFS",
        downloader_class_path="downloaders.ifs_downloader.IFSControlDownloaderParallel",
        downloader_defaults={
            "output_dir": "model_data/ifs",
            # Same throttle-prone ECMWF bucket as AIFS; keep concurrency modest.
            "max_workers": 8,
            "max_retries": 3,
            "timeout_seconds": 120,
        },
        lead_days_min=1,
        lead_days_max=14,
        variables=("precip", "temp", "wind", "humidity"),
        cycle_hour=12,
        data_dir="model_data/ifs",
        publish_delay_hours=9,
        # IFS Cycle 49r1 operational 2024-11-12; Cycle 50r1 went live
        # 2026-05-12 together with AIFS v2 (ECMWF announcement, pinned
        # 2026-07-24), so the 49r1→50r1 boundary mirrors the AIFS v1.1→v2
        # boundary exactly: last 49r1 12Z init 2026-05-11, first 50r1 init
        # 2026-05-12. Never pooled across the switch. 50r1 also removed the
        # control member from the open-data ENS, so the 50r1 era is the HRES
        # deterministic run (downloaders.ifs_downloader.IFS_OPER_FIRST_DATE).
        version_windows=(
            VersionWindow("49r1", None, date(2026, 5, 11), "clean"),
            VersionWindow("50r1", date(2026, 5, 12), None, "current"),
        ),
        primary_window="49r1",
    ),
}

DEFAULT_MODEL = next(
    (k for k, v in MODEL_REGISTRY.items() if v.default),
    next(iter(MODEL_REGISTRY)),  # fallback to first if none marked
)
