#!/usr/bin/env python3
"""Shared forecast-variable vocabulary and physical derivations.

Every downloader emits the same set of quantities per ``(variable, lead-day)``:
a 2-D CONUS array. This module is the single source of truth for what those
quantities are (key, label, units, aggregation kind, colormap) and for the
pure math that converts raw model fields into them.

Two aggregation kinds:
  * ``accumulation`` — precip: summed/subtracted into a per-day total.
  * ``instant``      — temp/wind/humidity: a snapshot at the lead's valid time.

The ``kind`` only affects how a downloader assembles the daily intermediate
``.npy`` (stage a). Lead-window combination (stage b) is a mean for every
variable (mean conditions over the window).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class ForecastVariable:
    key: str          # folder + identifier, e.g. "temp"
    label: str        # display label, e.g. "2 m Temperature"
    units: str        # canonical units of the stored array
    kind: str         # "accumulation" | "instant"
    colormap: str     # suggested colormap name (consumed by the next step)


PRECIP = ForecastVariable(
    key="precip", label="Precipitation", units="mm",
    kind="accumulation", colormap="Blues",
)
TEMP = ForecastVariable(
    key="temp", label="2 m Temperature", units="°C",
    kind="instant", colormap="RdYlBu_r",
)
WIND = ForecastVariable(
    key="wind", label="10 m Wind Speed", units="m/s",
    kind="instant", colormap="viridis",
)
HUMIDITY = ForecastVariable(
    key="humidity", label="2 m Relative Humidity", units="%",
    kind="instant", colormap="YlGnBu",
)

# Lookup by key. The registry stores keys per model; the base resolves them
# into ForecastVariable instances via this dict.
FORECAST_VARIABLES: dict[str, ForecastVariable] = {
    v.key: v for v in (PRECIP, TEMP, WIND, HUMIDITY)
}


# ── Pure derivation helpers (reused by the downloaders) ──────────────────

def wind_speed(u: np.ndarray, v: np.ndarray) -> np.ndarray:
    """10 m wind speed from u/v components (m/s). Used by GFS/AIFS."""
    return np.hypot(u, v).astype(np.float32)


def kelvin_to_celsius(t_k: np.ndarray) -> np.ndarray:
    """Kelvin → degrees Celsius."""
    return (np.asarray(t_k, dtype=np.float32) - 273.15).astype(np.float32)


def rh_from_dewpoint(t_k: np.ndarray, td_k: np.ndarray) -> np.ndarray:
    """Relative humidity (%) from temperature and dewpoint (both Kelvin).

    Magnus formula with constants a=17.625, b=243.04 °C; RH = 100·e(Td)/e_s(T).
    Used by AIFS, whose surface humidity is published as 2 m dewpoint (``2d``).
    """
    a, b = 17.625, 243.04
    t_c = np.asarray(t_k, dtype=np.float64) - 273.15
    td_c = np.asarray(td_k, dtype=np.float64) - 273.15
    e = np.exp(a * td_c / (b + td_c))
    es = np.exp(a * t_c / (b + t_c))
    rh = 100.0 * e / es
    return np.clip(rh, 0.0, 100.0).astype(np.float32)


def rh_from_specific_humidity(
    t_k: np.ndarray, q: np.ndarray, p_pa: float | np.ndarray
) -> np.ndarray:
    """Relative humidity (%) from temperature (K), specific humidity (kg/kg),
    and pressure (Pa).

    Vapor pressure e = q·p / (0.622 + 0.378·q); saturation e_s via Bolton
    (1980); RH = 100·e/e_s, clipped to 0–100. General helper for models that
    expose specific humidity rather than RH or dewpoint.
    """
    t_k = np.asarray(t_k, dtype=np.float64)
    q = np.asarray(q, dtype=np.float64)
    es = 611.2 * np.exp(17.67 * (t_k - 273.15) / (t_k - 29.65))  # Pa
    e = q * p_pa / (0.622 + 0.378 * q)                            # Pa
    rh = 100.0 * e / es
    return np.clip(rh, 0.0, 100.0).astype(np.float32)
