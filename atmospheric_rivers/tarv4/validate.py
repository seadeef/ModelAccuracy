"""Frame-level validation of the local tARget port against the canonical
catalog on an overlap period.

Two layers of comparison, strictest first:
  - cellwise mask agreement per 6-hourly frame on the pipeline's
    consumption crop (the canonical index's ``available_bounds``);
  - the quantities episodes actually consume: per-HUC6 daily area-weighted
    coverage fractions and the resulting consecutive-day episode windows.

Writes a JSON report under target_ar_data/tarv4_local/validation/ and
prints a summary.  The local masks used here are written to the same
validation directory — never to the discovery path — so a validation run
can never leak into the production catalog.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from atmospheric_rivers import discover_target_ar_sources, iter_normalized_frames
from atmospheric_rivers.remote import read_index
from atmospheric_rivers.tarv4.climatology import LOCAL_DIR
from atmospheric_rivers.tarv4.extend import detect_range, write_extension
from stats_pipeline.basin import basin_weights, load_basins

VALIDATION_DIR = LOCAL_DIR / "validation"


def _canonical_index() -> Path:
    sources = discover_target_ar_sources()
    if not sources:
        raise FileNotFoundError("no canonical tARget index with cached ranges")
    return sources[0]


def _frames_by_time(source, start, end, bounds):
    frames = {}
    for frame in iter_normalized_frames([source], start_date=start,
                                        end_date=end, bounds=bounds):
        frames[frame.timestamp] = frame
    return frames


def _daily_fractions(frames, huc2="18"):
    """Per-basin daily max area fraction (find_basin_episodes core)."""
    basins = load_basins(huc2)
    daily = {huc: defaultdict(float) for huc, _, _ in basins}
    weights = None
    signature = None
    for timestamp in sorted(frames):
        frame = frames[timestamp]
        key = (frame.lats.tobytes(), frame.lons.tobytes())
        if key != signature:
            weights, _ = basin_weights(frame.lats, frame.lons, basins)
            signature = key
        fractions = weights @ frame.mask.reshape(-1).astype(np.float64)
        day = timestamp[:10]
        for position, (huc, _, _) in enumerate(basins):
            value = float(fractions[position])
            if value > daily[huc][day]:
                daily[huc][day] = value
    return {huc: dict(days) for huc, days in daily.items()}


def _episode_days(daily, min_area_fraction=0.0):
    return {huc: sorted(day for day, value in days.items()
                        if value > min_area_fraction)
            for huc, days in daily.items()}


def run(start: str, end: str, *, workers: int = 24) -> dict:
    index = _canonical_index()
    bounds = read_index(index).get("available_bounds")
    bounds = tuple(float(v) for v in bounds) if bounds else None

    print(f"detecting {start}..{end} with the local port")
    steps, masks, lats, lons = detect_range(start, end, workers=workers)
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    local_path = write_extension(
        steps, masks, lats, lons, out_dir=VALIDATION_DIR,
        provenance={"kind": "validation_run", "start": start, "end": end})

    print("reading canonical frames")
    canonical = _frames_by_time(index, start, end, bounds)
    local = _frames_by_time(local_path, start, end, bounds)
    shared = sorted(set(canonical) & set(local))
    missing = sorted(set(canonical) ^ set(local))
    if not shared:
        raise RuntimeError("no shared timestamps between catalog and port")

    per_frame = []
    agree_cells = 0
    union_cells = 0
    intersect_cells = 0
    total_cells = 0
    for timestamp in shared:
        a = canonical[timestamp].mask
        b = local[timestamp].mask
        if a.shape != b.shape:
            raise RuntimeError(
                f"grid mismatch at {timestamp}: {a.shape} vs {b.shape}")
        both = int((a & b).sum())
        either = int((a | b).sum())
        agree_cells += int((a == b).sum())
        total_cells += a.size
        union_cells += either
        intersect_cells += both
        per_frame.append({
            "time": timestamp,
            "canonical_ar": int(a.sum()), "local_ar": int(b.sum()),
            "jaccard": (both / either) if either else 1.0,
        })
    jaccards = [f["jaccard"] for f in per_frame]

    daily_canonical = _daily_fractions(canonical)
    daily_local = _daily_fractions(local)
    fraction_deltas = []
    for huc in daily_canonical:
        days = set(daily_canonical[huc]) | set(daily_local.get(huc, {}))
        for day in days:
            fraction_deltas.append(abs(
                daily_canonical[huc].get(day, 0.0)
                - daily_local.get(huc, {}).get(day, 0.0)))
    days_canonical = _episode_days(daily_canonical)
    days_local = _episode_days(daily_local)
    day_mismatches = {
        huc: sorted(set(days_canonical[huc]) ^ set(days_local.get(huc, [])))
        for huc in days_canonical
        if set(days_canonical[huc]) != set(days_local.get(huc, []))}

    report = {
        "start": start, "end": end, "bounds": bounds,
        "frames_compared": len(shared),
        "timestamps_unmatched": missing,
        "cellwise_agreement": agree_cells / total_cells,
        "pooled_jaccard": (intersect_cells / union_cells
                           if union_cells else 1.0),
        "frame_jaccard_min": min(jaccards),
        "frame_jaccard_median": float(np.median(jaccards)),
        "ar_cellhours_canonical": int(sum(f["canonical_ar"] for f in per_frame)),
        "ar_cellhours_local": int(sum(f["local_ar"] for f in per_frame)),
        "basin_daily_fraction_max_abs_delta": max(fraction_deltas, default=0.0),
        "basin_active_day_mismatches": day_mismatches,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "worst_frames": sorted(per_frame, key=lambda f: f["jaccard"])[:12],
    }
    out = VALIDATION_DIR / f"report_{start}_{end}.json"
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n",
                   encoding="utf-8")
    print(json.dumps({k: report[k] for k in (
        "frames_compared", "cellwise_agreement", "pooled_jaccard",
        "frame_jaccard_min", "frame_jaccard_median",
        "ar_cellhours_canonical", "ar_cellhours_local",
        "basin_daily_fraction_max_abs_delta")}, indent=2))
    if day_mismatches:
        print("active-day mismatches:", json.dumps(day_mismatches, indent=1))
    else:
        print("active-day sets identical for every CA HUC6 basin")
    print(f"report: {out}")
    return report
