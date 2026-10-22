"""Local tARget v4 AR-detection extension (non-canonical).

Generates 6-hourly AR shape masks beyond the canonical UCLA Dataverse
release (which lags the model record by up to ~14 months) by running a
Python port of the published tARget v4 algorithm (Guan & Waliser 2024,
doi:10.1038/s41597-024-03258-4; code doi:10.25346/S6/SF4AFW) on ERA5/ERA5T
IVT read from the public ARCO-ERA5 store.

Outputs are deliberately labeled as a locally generated extension — they
are NOT the canonical catalog and every consumer surface that reports AR
coverage must carry that provenance.
"""

from atmospheric_rivers.tarv4.arco import (  # noqa: F401
    IVT_EAST, IVT_NORTH, hour_index, iter_ivt_steps, store_time_coverage,
)
