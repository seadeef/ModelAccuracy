"""Python port of the tARget v4 per-timestep AR shape detection.

Faithful to target.m (doi:10.25346/S6/SF4AFW, v4, 2024-05-05) for the
parts that determine the binary shape mask consumed by the episode
pipeline: whirl (tropical-cyclone) exclusion, the 85th->95th percentile
ladder, area / direction-spread / axis / poleward-segment / geometry
criteria.  Tracking (kidmap), transects and landfall locations are not
ported — no consumer in this repository reads them (episodes use
``shapemap > 0`` only).

Known deliberate deviations, all validated frame-by-frame against the
canonical catalog by tarv4.validate:
  - regional detection domain (10S-70N, 140E-50W) instead of the globe,
    so objects crossing the domain edge may differ near the boundary;
  - MATLAB ``smoothdata(...,'sgolay')`` picks its window heuristically;
    we use a fixed Savitzky-Golay window (SGOLAY_WINDOW, degree 2).

Line references below are to target.m.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

import numpy as np
import scipy.ndimage as ndi
from scipy.signal import savgol_filter

from atmospheric_rivers.tarv4.climatology import (
    DOMAIN_LATS, DOMAIN_LONS, THRESHOLDS_PATH, pixel_areas,
)

EARTH_RADIUS = 6371e3
LENGTH_LIMIT = 2e6
LENWID_RATIO_LIMIT = 2.0
OBJECT_AREA_LIMIT = LENGTH_LIMIT * 100e3          # line 115
OBJECT_IVTY_LIMIT = 0.25                          # line 116
OBJECT_IVTDIRSPREAD_LIMIT = 67.5                  # line 117
UNIVERSAL_IVT_LIMITS = (0.0, np.inf)              # run_example.m
AXIS_MAX_POINTS = 1440       # canonical nrow (global 0.25-deg lon count)
CRESSMAN_AREA_LIMIT = 0.25                        # line 1607
CRESSMAN_RADIUS_LIMIT = 500e3                     # line 1608
# MATLAB smoothdata(...,'sgolay') picks its window by an undocumented
# heuristic; ours is fixed and calibrated by frame agreement against the
# canonical catalog over 2025-10-01..2025-12-31 (window sweep 3..19:
# cellwise agreement and pooled Jaccard peak at 15, and CA basin
# active-day mismatches bottom out at 16/1380; see
# target_ar_data/tarv4_local/validation/report_w*.json).
SGOLAY_WINDOW = int(os.environ.get("TARV4_SGOLAY_WINDOW", "15"))
SGOLAY_DEGREE = 2

_STRUCT8 = np.ones((3, 3), dtype=bool)


# ---------------------------------------------------------------------------
# Spherical geometry (mapping-toolbox equivalents, sphere of EARTH_RADIUS)
# ---------------------------------------------------------------------------

def gc_distance(lat1, lon1, lat2, lon2):
    """Great-circle distance in meters (haversine, float64)."""
    p1 = np.radians(np.asarray(lat1, dtype=np.float64))
    p2 = np.radians(np.asarray(lat2, dtype=np.float64))
    dp = p2 - p1
    dl = np.radians(np.asarray(lon2, dtype=np.float64)
                    - np.asarray(lon1, dtype=np.float64))
    h = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS * np.arcsin(np.minimum(1.0, np.sqrt(h)))


def gc_azimuth(lat1, lon1, lat2, lon2):
    """Initial great-circle bearing, degrees clockwise from north [0, 360)."""
    p1 = np.radians(np.asarray(lat1, dtype=np.float64))
    p2 = np.radians(np.asarray(lat2, dtype=np.float64))
    dl = np.radians(np.asarray(lon2, dtype=np.float64)
                    - np.asarray(lon1, dtype=np.float64))
    y = np.sin(dl) * np.cos(p2)
    x = np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(dl)
    return np.mod(np.degrees(np.arctan2(y, x)), 360.0)


def atan_in_azimuth(y, x):
    """target.m line 1402: IVT direction as azimuth; NaN where both zero."""
    with np.errstate(invalid="ignore"):
        degree = np.mod(90.0 - np.degrees(np.arctan2(y, x)), 360.0)
    both_zero = (np.asarray(x) == 0) & (np.asarray(y) == 0)
    return np.where(both_zero, np.nan, degree)


def _fold180(degree):
    """mod 360 then reflect >180 to 360-x (target.m degree_diff idiom)."""
    degree = np.mod(degree, 360.0)
    return np.where(degree > 180.0, 360.0 - degree, degree)


# ---------------------------------------------------------------------------
# Morphology (image-toolbox equivalents)
# ---------------------------------------------------------------------------

def fill_holes(mask: np.ndarray) -> np.ndarray:
    """imfill(BW,'holes'): 4-connected background, 8-connected foreground."""
    return ndi.binary_fill_holes(mask)


def label8(mask: np.ndarray):
    """bwlabel default 8-connectivity."""
    return ndi.label(mask, structure=_STRUCT8)


def fill_label_holes(labels: np.ndarray) -> np.ndarray:
    """imfill on a label map (line 625): holes take an enclosing label.

    MATLAB grayscale imfill raises interior basins to the lowest
    surrounding intensity; for a label map whose holes are enclosed by a
    single object that is exactly "fill with the enclosing label", and for
    the (rare) multi-label enclosure it is the minimum adjacent label.
    """
    filled = fill_holes(labels > 0)
    holes = filled & (labels == 0)
    if not holes.any():
        return labels
    out = labels.copy()
    hole_labels, count = label8(holes)
    for hole in range(1, count + 1):
        cells = hole_labels == hole
        ring = ndi.binary_dilation(cells, structure=_STRUCT8) & (labels > 0)
        if ring.any():
            out[cells] = labels[ring].min()
    return out


# ---------------------------------------------------------------------------
# Axis walk (target.m object_axis, lines 1606-1704)
# ---------------------------------------------------------------------------

# Neighbor order replicates the MATLAB double loop (lon offset outer, lat
# offset inner) because first-maximum tie-breaking depends on it.
_NEIGHBOR_OFFSETS = [(dlat, dlon) for dlon in (-1, 0, 1)
                     for dlat in (-1, 0, 1) if not (dlat == 0 and dlon == 0)]


def object_axis(grid, ivt, ivtx, ivty, mask):
    """Return (axis flat indices, iswhirl) for one object mask."""
    nlat, nlon = grid.lat2d.shape
    mask_idx = np.flatnonzero(mask)
    mask_lat = grid.lat2d.ravel()[mask_idx]
    mask_lon = grid.lon2d.ravel()[mask_idx]
    mask_area = grid.area2d.ravel()[mask_idx]
    mask_ivtx = ivtx.ravel()[mask_idx]
    mask_ivty = ivty.ravel()[mask_idx]
    total_area = np.nansum(mask_area)
    ivt_flat = ivt.ravel()
    mask_flat = mask.ravel()

    masked = np.where(mask_flat, ivt_flat, np.nan)
    axis_idx = [int(np.nanargmax(masked))]
    axis_quality = [1]
    iswhirl = False

    for fly_direction in ("downstream", "upstream"):
        while True:
            current = axis_idx[-1]
            crow, ccol = divmod(current, nlon)
            clat = grid.lat2d[crow, ccol]
            clon = grid.lon2d[crow, ccol]
            dist = gc_distance(clat, clon, mask_lat, mask_lon)
            order = np.argsort(dist, kind="stable")
            cum = np.nancumsum(mask_area[order]) / total_area
            within = np.flatnonzero(cum <= CRESSMAN_AREA_LIMIT)
            if within.size:
                radius = min(float(dist[order[within[-1]]]),
                             CRESSMAN_RADIUS_LIMIT)
            else:
                radius = 0.001
            with np.errstate(invalid="ignore", divide="ignore"):
                weight = (radius ** 2 - dist ** 2) / (radius ** 2 + dist ** 2)
            weight = np.where(weight <= 0, np.nan, weight)
            wsum = np.nansum(mask_area * weight)
            cx = np.nansum(mask_ivtx * mask_area * weight) / wsum
            cy = np.nansum(mask_ivty * mask_area * weight) / wsum
            cressman_dir = atan_in_azimuth(cy, cx)

            neighbors = []
            for dlat, dlon in _NEIGHBOR_OFFSETS:
                nrow, ncol_ = crow + dlat, ccol + dlon
                if 0 <= nrow < nlat and 0 <= ncol_ < nlon:
                    neighbors.append(nrow * nlon + ncol_)
            neighbors = np.asarray(neighbors, dtype=int)
            nlat_vals = grid.lat2d.ravel()[neighbors]
            nlon_vals = grid.lon2d.ravel()[neighbors]
            azi = gc_azimuth(clat, clon, nlat_vals, nlon_vals)
            heading = cressman_dir if fly_direction == "downstream" \
                else cressman_dir + 180.0
            degree_diff = np.round(_fold180(heading - azi) / 45.0) * 45.0
            if len(axis_idx) >= 2:
                prow, pcol = divmod(axis_idx[-2], nlon)
                azimuth_prev = gc_azimuth(grid.lat2d[prow, pcol],
                                          grid.lon2d[prow, pcol], clat, clon)
                degree_diff2 = np.round(
                    _fold180(azimuth_prev - azi) / 45.0) * 45.0
                keep = (degree_diff <= 45.0) & (degree_diff2 <= 90.0)
            else:
                degree_diff2 = None
                keep = degree_diff <= 45.0

            next_idx = None
            next_quality = 1
            candidates = neighbors[keep]
            if candidates.size:
                next_idx = int(candidates[np.argmax(ivt_flat[candidates])])
            if next_idx is not None and not mask_flat[next_idx]:
                in_axis = np.isin(neighbors, axis_idx)
                fallback = (mask_flat[neighbors] & (degree_diff <= 90.0)
                            & ~in_axis)
                if degree_diff2 is not None:
                    fallback &= degree_diff2 <= 90.0
                candidates = neighbors[fallback]
                next_idx = (int(candidates[np.argmax(ivt_flat[candidates])])
                            if candidates.size else None)
                next_quality = 0
            if (next_idx is None or not mask_flat[next_idx]
                    or next_idx in axis_idx):
                if (next_idx is not None and next_idx in axis_idx
                        and len(axis_idx) - axis_idx.index(next_idx) >= 4):
                    iswhirl = True
                    position = axis_idx.index(next_idx)
                    axis_idx = axis_idx[position:]
                    axis_quality = axis_quality[position:]
                break
            axis_idx.append(next_idx)
            axis_quality.append(next_quality)

        # Trim a trailing low-quality run (lines 1693-1697).
        if not iswhirl:
            end = len(axis_quality)
            start = end
            while start > 0 and axis_quality[start - 1] == 0:
                start -= 1
            if start < end and end == len(axis_idx):
                axis_idx = axis_idx[:start]
                axis_quality = axis_quality[:start]
        if fly_direction == "downstream" and iswhirl:
            break
        axis_idx.reverse()
        axis_quality.reverse()
    return axis_idx, iswhirl


# ---------------------------------------------------------------------------
# Thresholds and grid
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Grid:
    lats: np.ndarray
    lons: np.ndarray
    lat2d: np.ndarray
    lon2d: np.ndarray
    area2d: np.ndarray
    border: np.ndarray  # outer perimeter ring (axis trimming, line 659)


def build_grid(lats: np.ndarray = DOMAIN_LATS,
               lons: np.ndarray = DOMAIN_LONS) -> Grid:
    lat2d = np.broadcast_to(lats[:, None], (lats.size, lons.size)).copy()
    lon2d = np.broadcast_to(lons[None, :], (lats.size, lons.size)).copy()
    area2d = np.broadcast_to(pixel_areas(lats, 0.25, 0.25)[:, None],
                             lat2d.shape).copy()
    border = np.zeros(lat2d.shape, dtype=bool)
    border[0, :] = border[-1, :] = True
    border[:, 0] = border[:, -1] = True
    return Grid(lats, lons, lat2d, lon2d, area2d, border.ravel())


class Thresholds:
    """Effective per-month pixel limits: max(pixel, hemispheric floor)."""

    def __init__(self, path=THRESHOLDS_PATH):
        with np.load(path) as payload:
            dom_pct = payload["dom_pct"]            # (P, 12, nlat, nlon)
            floors = payload["floors_nh_sh"]        # (P, 12, 2)
            self.lats = payload["lat"]
            self.lons = payload["lon"]
            self.meta = json.loads(str(payload["meta"]))
        floor_field = np.empty_like(dom_pct)
        north = self.lats > 0
        south = self.lats < 0
        equator = self.lats == 0
        floor_field[:, :, north, :] = floors[:, :, 0, None, None]
        floor_field[:, :, south, :] = floors[:, :, 1, None, None]
        if equator.any():  # line 470: midpoint of the two hemispheric floors
            floor_field[:, :, equator, :] = (
                floors.mean(axis=2)[:, :, None, None])
        self.effective = np.maximum(dom_pct, floor_field)   # line 473
        self.levels = self.effective.shape[0]

    def level_field(self, level: int, month: int) -> np.ndarray:
        low, high = UNIVERSAL_IVT_LIMITS
        field = np.maximum(self.effective[level, month - 1], low)
        if level == 0:      # line 535: the 85th-pct pass caps at uni high
            field = np.minimum(field, high)
        return field


# ---------------------------------------------------------------------------
# Per-timestep detection
# ---------------------------------------------------------------------------

def _object_slices(labels, count):
    slices = ndi.find_objects(labels, count)
    for index, region in enumerate(slices, start=1):
        if region is not None:
            yield index, region


def _area_filter(labels, count, area2d):
    """Drop objects below OBJECT_AREA_LIMIT (lines 550-554 / 618-622)."""
    if count == 0:
        return labels
    sums = ndi.sum_labels(area2d, labels, index=np.arange(1, count + 1))
    small = np.flatnonzero(sums < OBJECT_AREA_LIMIT) + 1
    if small.size:
        labels[np.isin(labels, small)] = 0
    return labels


def _threshold_objects(ivt_reduced, threshold_field, grid):
    """Threshold -> fill -> label -> area filter -> fill label holes."""
    with np.errstate(invalid="ignore"):
        shape = ivt_reduced >= threshold_field
    shape = fill_holes(shape)
    labels, count = label8(shape)
    labels = _area_filter(labels, count, grid.area2d)
    labels = fill_label_holes(labels)
    return labels


def _smooth_axis(axis_lat, axis_lon):
    """smoothdata(...,'sgolay') stand-in: fixed-window Savitzky-Golay."""
    n = axis_lat.size
    window = min(SGOLAY_WINDOW, n if n % 2 else n - 1)
    if window <= SGOLAY_DEGREE:
        return axis_lat.copy(), axis_lon.copy()
    unwrapped = np.degrees(np.unwrap(np.radians(axis_lon)))
    smooth_lon = np.mod(
        savgol_filter(unwrapped, window, SGOLAY_DEGREE, mode="interp"), 360.0)
    smooth_lat = savgol_filter(axis_lat, window, SGOLAY_DEGREE, mode="interp")
    return smooth_lat, smooth_lon


def _whirl_map(ivt, ivtx, ivty, labels, grid):
    """Tropical-cyclone exclusion (lines 556-584)."""
    whirl = np.zeros(ivt.shape, dtype=bool)
    direction = atan_in_azimuth(ivty, ivtx)
    quadrant = np.round(direction / 90.0) * 90.0
    for index, region in _object_slices(*labels):
        obj = labels[0] == index
        present = quadrant[obj]
        if not all(np.any(present == q) for q in (0.0, 90.0, 180.0, 270.0)):
            continue
        axis_idx, iswhirl = object_axis(grid, ivt, ivtx, ivty, obj)
        if not iswhirl:
            continue
        axis_idx = np.asarray(axis_idx)
        alat = grid.lat2d.ravel()[axis_idx]
        alon = grid.lon2d.ravel()[axis_idx]
        aarea = grid.area2d.ravel()[axis_idx]
        eye_lon = np.mod(np.degrees(np.angle(
            np.nansum(np.exp(1j * np.radians(alon)) * aarea)
            / np.nansum(aarea))), 360.0)
        eye_lat = np.nansum(alat * aarea) / np.nansum(aarea)
        ring1 = gc_azimuth(eye_lat, eye_lon, alat[0], alon[0])
        quarter = min(axis_idx.size - 1, 1 + round(axis_idx.size / 4) - 1)
        ring2 = gc_azimuth(eye_lat, eye_lon, alat[quarter], alon[quarter])
        degree_diff = np.mod(ring2 - ring1, 360.0)
        if degree_diff > 180.0:
            degree_diff -= 360.0
        clock = np.sign(degree_diff)
        obj_idx = np.flatnonzero(obj.ravel())
        olat = grid.lat2d.ravel()[obj_idx]
        olon = grid.lon2d.ravel()[obj_idx]
        azimuth_to_shape = gc_azimuth(eye_lat, eye_lon, olat, olon)
        diff = np.mod(atan_in_azimuth(ivty.ravel()[obj_idx],
                                      ivtx.ravel()[obj_idx])
                      - azimuth_to_shape, 360.0)
        diff = np.where(diff > 180.0, diff - 360.0, diff)
        selected = obj_idx[(diff > clock * 90 - 45) & (diff < clock * 90 + 45)]
        flat = whirl.ravel()
        flat[selected] = True
        whirl = fill_holes(flat.reshape(whirl.shape))
    return whirl


def detect_step(ivtx, ivty, month, thresholds, grid):
    """One 6-hourly step -> (union AR mask, per-level survivor counts)."""
    ivt = np.hypot(ivtx, ivty).astype(np.float64)
    ivtx = ivtx.astype(np.float64)
    ivty = ivty.astype(np.float64)
    ivt_reduced = ivt.copy()

    # Whirl pre-pass at the primary threshold (lines 534-584).
    pre_labels = _threshold_pre(ivt_reduced, thresholds.level_field(0, month),
                                grid)
    whirl = _whirl_map(ivt, ivtx, ivty, pre_labels, grid)
    ivt_reduced[whirl] = np.nan

    union = np.zeros(ivt.shape, dtype=bool)
    survivor_counts = []
    previous_survivors = None
    for level in range(thresholds.levels):
        if level > 0 and previous_survivors is not None:
            ivt_reduced[previous_survivors] = np.nan
        labels = _threshold_objects(
            ivt_reduced, thresholds.level_field(level, month), grid)
        count = labels.max()
        survivors = _apply_object_criteria(
            labels, int(count), ivt, ivtx, ivty, grid)
        previous_survivors = survivors
        union |= survivors
        survivor_counts.append(int(label8(survivors)[1]))
    return union, survivor_counts


def _threshold_pre(ivt_reduced, threshold_field, grid):
    """Pre-pass objects for whirl detection (lines 534-554)."""
    with np.errstate(invalid="ignore"):
        shape = ivt_reduced >= threshold_field
    shape = fill_holes(shape)
    labels, count = label8(shape)
    labels = _area_filter(labels, count, grid.area2d)
    return labels, int(labels.max())


def _apply_object_criteria(labels, count, ivt, ivtx, ivty, grid):
    """Direction spread, axis, poleward segments, geometry (lines 618-714)."""
    survivors = np.zeros(labels.shape, dtype=bool)
    if count == 0:
        return survivors
    area = grid.area2d
    for index, region in _object_slices(labels, count):
        obj = labels == index
        if not obj.any():
            continue
        oarea = area[obj]
        oivt = ivt[obj]
        oivtx = ivtx[obj]
        oivty = ivty[obj]

        # Direction coherence (lines 636-649).
        object_ivtx = np.nansum(oivtx * oarea) / np.nansum(oarea)
        object_ivty = np.nansum(oivty * oarea) / np.nansum(oarea)
        object_dir = atan_in_azimuth(object_ivty, object_ivtx)
        diff = _fold180(atan_in_azimuth(oivty, oivtx) - object_dir)
        weight = oarea * oivt
        spread = np.sqrt(np.nansum(diff ** 2 * weight) / np.nansum(weight))
        if spread > OBJECT_IVTDIRSPREAD_LIMIT:
            continue

        # Axis (lines 652-674).
        axis_idx, _ = object_axis(grid, ivt, ivtx, ivty, obj)
        axis_idx = [i for i in axis_idx if not grid.border[i]]
        if len(axis_idx) > AXIS_MAX_POINTS:
            step = (len(axis_idx) - 1) / (AXIS_MAX_POINTS - 1)
            positions = np.round(np.arange(AXIS_MAX_POINTS) * step).astype(int)
            axis_idx = [axis_idx[p] for p in positions]
        if len(axis_idx) < 2:
            continue
        axis_idx = np.asarray(axis_idx)
        raw_lat = grid.lat2d.ravel()[axis_idx]
        raw_lon = grid.lon2d.ravel()[axis_idx]
        smooth_lat, smooth_lon = _smooth_axis(raw_lat, raw_lon)

        # Centroid + poleward segments (lines 679-698).
        olat = grid.lat2d[obj]
        straddling = float(olat.min()) * float(olat.max()) < 0
        weight = oarea * oivt
        centroid_lat = np.nansum(olat * weight) / np.nansum(weight)
        axis_ivtx = ivtx.ravel()[axis_idx]
        axis_ivty = ivty.ravel()[axis_idx].copy()
        axis_ivt = np.hypot(axis_ivtx, axis_ivty)
        if centroid_lat < 0:
            axis_ivty = -axis_ivty
        poleward = axis_ivty >= OBJECT_IVTY_LIMIT * axis_ivt
        plat = np.where(poleward, smooth_lat, np.nan)
        plon = np.where(poleward, smooth_lon, np.nan)
        segments = gc_distance(plat[:-1], plon[:-1], plat[1:], plon[1:])
        poleward_length = np.nansum(segments)
        if straddling or poleward_length < 0.5 * LENGTH_LIMIT:
            continue

        # Length / width (lines 703-713).
        segments = gc_distance(smooth_lat[:-1], smooth_lon[:-1],
                               smooth_lat[1:], smooth_lon[1:])
        nseg = segments.size
        object_length = np.nansum(segments) / nseg * (nseg + 1)
        object_width = np.nansum(oarea) / object_length
        if (object_length < LENGTH_LIMIT
                or object_length / object_width < LENWID_RATIO_LIMIT):
            continue
        survivors |= obj
    return survivors
