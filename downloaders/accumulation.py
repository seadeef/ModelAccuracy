"""Pure daily-precipitation recovery cores (no I/O, no GRIB, no reprojection).

Each model publishes precipitation in a different native convention; these
array→array functions are the single implementation of the recovery math,
called by the downloaders on real data.

Conventions:
  GFS / AIFS  cumulative-from-start buckets: daily(D) = cum(24·D) − cum(24·(D−1));
              the first lead may lack a predecessor record (GFS has no f000
              cumulative), in which case the cumulative *is* the daily total.
              Tiny negative noise from the subtraction is clipped to zero.
  IFS ctrl    cumulative-from-start buckets in meters: same difference as
              GFS/AIFS, then ×1000 → mm (clip and scale commute).
  NBM         four 6 h buckets (QPF06, mm) summed per day. No clip — buckets
              are non-negative by construction and the sum stays in mm.
  WN2         four 6 h buckets in meters summed per day, ×1000 → mm, clipped.

Reordering/cropping to the output grid commutes with this elementwise math,
so the downloaders may apply their geometry steps before or after calling in
(they keep their existing order; results are byte-identical).
"""

from __future__ import annotations

import numpy as np


def daily_from_cumulative(cumulative_end: np.ndarray,
                          cumulative_start: np.ndarray | None = None) -> np.ndarray:
    """GFS/AIFS: difference two cumulative-precip fields into a daily total (mm)."""
    if cumulative_start is not None:
        daily = cumulative_end - cumulative_start
    else:
        daily = np.array(cumulative_end, copy=True)
    np.clip(daily, 0.0, None, out=daily)
    return daily


def daily_from_cumulative_meters(cumulative_end: np.ndarray,
                                 cumulative_start: np.ndarray | None = None) -> np.ndarray:
    """IFS control: difference cumulative tp fields in meters into a daily total (mm)."""
    daily = daily_from_cumulative(cumulative_end, cumulative_start)
    daily *= 1000.0
    return daily


def daily_from_buckets(buckets: np.ndarray) -> np.ndarray:
    """NBM: sum 6 h precipitation buckets (mm) along axis 0 into a daily total."""
    buckets = np.asarray(buckets)
    total = buckets[0].copy()
    for bucket in buckets[1:]:
        total += bucket
    return total


def daily_from_buckets_meters(buckets_m: np.ndarray) -> np.ndarray:
    """WN2: sum 6 h buckets (meters) along axis 0, convert to mm, clip noise."""
    daily = daily_from_buckets(buckets_m) * 1000.0
    np.clip(daily, 0.0, None, out=daily)
    return daily
