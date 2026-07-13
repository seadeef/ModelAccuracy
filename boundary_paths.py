"""Paths for the boundary data store.

``boundary_data/`` holds only data (like ``model_data/``): normalized outputs at
the top level and a non-hidden ``cache/`` for the large transient WBD GDB
download. All of it is written by ``load_boundaries.py`` and read by
``export_static.py``.

    boundary_data/
        zip/zip_lookup.csv             ← ZIP centroid/bounds lookup
        admin/states-10m.json          ← us-atlas TopoJSON sources
        admin/counties-10m.json
        wbd/layers/hu{2,4,…}.json      ← normalized WBD HU layers
        military/sites.json            ← normalized DISDI MIRTA installation sites
        cache/                         ← transient raw downloads (gitignored anyway)
            cb_2020_us_zcta520_500k.zip   ← raw ZCTA boundary shapefile (zip source)
            WBD_National_GDB.zip
            WBD_National_GDB.gdb/
            installations_ranges.zip      ← raw DISDI MIRTA GDB zip (military source)
            MIRTA.gdb/
"""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
BOUNDARY_DATA = PROJECT_ROOT / "boundary_data"

ZIP_DIR = BOUNDARY_DATA / "zip"
ADMIN_DIR = BOUNDARY_DATA / "admin"
WBD_DIR = BOUNDARY_DATA / "wbd"
WBD_LAYERS_DIR = WBD_DIR / "layers"

# Transient heavy intermediates (raw national GDB download + extraction).
WBD_CACHE = BOUNDARY_DATA / "cache"
WBD_GDB_ZIP = WBD_CACHE / "WBD_National_GDB.zip"
WBD_GDB_DIR = WBD_CACHE / "WBD_National_GDB.gdb"

ZIP_LOOKUP_CSV = ZIP_DIR / "zip_lookup.csv"

# Raw ZIP source: US Census ZCTA cartographic-boundary shapefile (cached zip).
# Both the fly-to centroid and the zoom bbox are derived from its polygons.
ZIP_ZCTA_ZIP = WBD_CACHE / "cb_2020_us_zcta520_500k.zip"
ZIP_ZCTA_SHP_NAME = "cb_2020_us_zcta520_500k.shp"
ZIP_ZCTA_CODE_FIELD = "ZCTA5CE20"
ZIP_ZCTA_URL = (
    "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_zcta520_500k.zip"
)
ZIP_SOURCE_LABEL = (
    "US Census 2020 ZCTA Cartographic Boundaries (GENZ2020 cb_2020_us_zcta520_500k)"
)

MILITARY_DIR = BOUNDARY_DATA / "military"
MILITARY_SITES_JSON = MILITARY_DIR / "sites.json"
MILITARY_ZIP = WBD_CACHE / "installations_ranges.zip"
MILITARY_GDB_DIR = WBD_CACHE / "MIRTA.gdb"
MILITARY_URL = (
    "https://www.acq.osd.mil/eie/imr/rpid/disdi/Downloads/installations_ranges.zip"
)
MILITARY_SOURCE_LABEL = (
    "DoD DISDI MIRTA (Military Installations, Ranges, and Training Areas), FY24 release"
)
# www.acq.osd.mil serves its leaf certificate without the IdenTrust intermediate,
# so default verification fails; this pinned chain (intermediate + root) completes it.
MILITARY_CA_PEM = PROJECT_ROOT / "certs" / "identrust_trustid_server_ca_o1.pem"

ADMIN_STATES_TOPOJSON = ADMIN_DIR / "states-10m.json"
ADMIN_COUNTIES_TOPOJSON = ADMIN_DIR / "counties-10m.json"

US_ATLAS_CDN = "https://cdn.jsdelivr.net/npm/us-atlas@3"
ADMIN_SOURCE_LABEL = "us-atlas@3 (US Census Cartographic Boundaries, 2017)"
WBD_SOURCE_LABEL = "USGS WBD National GDB (TNM StagedProducts/Hydrography/WBD/National/GDB)"
WBD_SCIENCEBASE_URL = (
    "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/GDB/"
    "WBD_National_GDB.zip"
)
