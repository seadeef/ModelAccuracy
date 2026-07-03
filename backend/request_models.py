from __future__ import annotations

from pydantic import BaseModel, Field, field_validator, model_validator


def _reject_non_digit_lead(v):
    """A lead string must be plain digits. Guards the Python quirk where
    ``int("1_7") == 17`` would let a legacy window key coerce to a bogus lead."""
    if isinstance(v, str) and not v.isdigit():
        raise ValueError("lead must be a non-negative integer")
    return v

VALID_REGION_TYPES = {"point", "rectangle", "polygon", "admin", "huc"}
VALID_ADMIN_LEVELS = {"state", "county"}
VALID_HUC_LEVELS = {2, 4, 6, 8, 10, 12, 14, 16}
VALID_PERIODS = {"yearly", "monthly", "seasonal"}
VALID_SEASONS = {"djf", "mam", "jja", "son"}
VALID_MONTHS = {f"{m:02d}" for m in range(1, 13)}

# Caps on request-driven fan-out so an unauthenticated caller can't force the
# backend to read tens of thousands of layers or walk a giant polygon. Any real
# model's deepest lead is 14; 31 is a hard ceiling. Invalid values raise a
# pydantic ValidationError → FastAPI 422, never a 500.
MAX_LEAD_DAYS = 31
MAX_POLYGON_POINTS = 1000


def _validate_lon_lat(lon: float, lat: float) -> None:
    if not (-180.0 <= lon <= 180.0) or not (-90.0 <= lat <= 90.0):
        raise ValueError("coordinates out of range (lon -180..180, lat -90..90)")


def _validate_month_season(month, season) -> None:
    """Whitelist path-building fields so ``period``/``month``/``season`` can only
    map to existing directories (no traversal)."""
    if month is not None and str(month) not in VALID_MONTHS:
        raise ValueError("month must be a two-digit value 01..12")
    if season is not None and str(season) not in VALID_SEASONS:
        raise ValueError(f"season must be one of {sorted(VALID_SEASONS)}")


class StatsRegion(BaseModel):
    type: str
    coordinates: list | None = None
    bounds: list[float] | None = None
    # admin type only:
    level: str | int | None = None
    fips: str | None = None
    # huc type only:
    huc: str | None = None

    @model_validator(mode="after")
    def validate_shape(self) -> "StatsRegion":
        if self.type not in VALID_REGION_TYPES:
            raise ValueError(f"region.type must be one of {sorted(VALID_REGION_TYPES)}")

        if self.type == "point":
            if not self.coordinates or len(self.coordinates) < 2:
                raise ValueError("point region requires [lon, lat] coordinates")
            _validate_lon_lat(float(self.coordinates[0]), float(self.coordinates[1]))

        if self.type == "rectangle":
            if not self.bounds or len(self.bounds) != 4:
                raise ValueError("rectangle region requires bounds [west, south, east, north]")
            west, south, east, north = (float(b) for b in self.bounds)
            _validate_lon_lat(west, south)
            _validate_lon_lat(east, north)
            if west > east or south > north:
                raise ValueError("rectangle bounds must satisfy west<=east and south<=north")

        if self.type == "polygon":
            coords = self.coordinates or []
            if len(coords) < 3:
                raise ValueError("polygon region requires at least 3 coordinates")
            if len(coords) > MAX_POLYGON_POINTS:
                raise ValueError(f"polygon has too many points (max {MAX_POLYGON_POINTS})")

        if self.type == "admin":
            if self.level not in VALID_ADMIN_LEVELS:
                raise ValueError(f"region.level must be one of {sorted(VALID_ADMIN_LEVELS)}")
            if not self.fips or not self.fips.isdigit():
                raise ValueError("region.fips must be a numeric FIPS code")
            expected = 2 if self.level == "state" else 5
            if len(self.fips) != expected:
                raise ValueError(f"region.fips for {self.level} must be {expected} digits")

        if self.type == "huc":
            if not self.huc or not str(self.huc).isdigit():
                raise ValueError("region.huc must be a numeric HUC code")
            huc_len = len(str(self.huc))
            if huc_len not in VALID_HUC_LEVELS:
                raise ValueError(
                    f"region.huc length must be one of {sorted(VALID_HUC_LEVELS)}"
                )
            if self.level is not None and int(self.level) != huc_len:
                raise ValueError("region.level must match the digit count of region.huc")
        return self


class StatsQueryRequest(BaseModel):
    model: str | None = None
    # Post-Phase-2 there are no string window leads — a lead is a bounded int.
    lead: int | None = Field(default=None, ge=0, le=MAX_LEAD_DAYS)
    minLead: int | None = Field(default=None, ge=0, le=MAX_LEAD_DAYS)
    maxLead: int | None = Field(default=None, ge=0, le=MAX_LEAD_DAYS)
    region: StatsRegion
    period: str = "yearly"
    month: str | None = None
    season: str | None = None
    statistics: list[str] | None = None

    _lead_digits = field_validator("lead", mode="before")(_reject_non_digit_lead)

    @model_validator(mode="after")
    def validate_request(self) -> "StatsQueryRequest":
        if self.period not in VALID_PERIODS:
            raise ValueError("period must be yearly, monthly, or seasonal")
        _validate_month_season(self.month, self.season)
        has_single = self.lead is not None
        has_range = self.minLead is not None or self.maxLead is not None
        if has_single == has_range:
            raise ValueError("Provide either lead or minLead/maxLead")
        if has_range and (self.minLead is None or self.maxLead is None):
            raise ValueError("Both minLead and maxLead are required for range queries")
        if has_range and self.minLead > self.maxLead:
            raise ValueError("minLead must be <= maxLead")
        return self


class LeadWinnersRequest(BaseModel):
    """Cross-model best model per lead for a geographic region (verification stats only)."""

    region: StatsRegion
    statistic: str
    period: str = "yearly"
    month: str | None = None
    season: str | None = None
    minLead: int = Field(..., ge=0, le=MAX_LEAD_DAYS)
    maxLead: int = Field(..., ge=0, le=MAX_LEAD_DAYS)

    @model_validator(mode="after")
    def validate_lead_winners(self) -> "LeadWinnersRequest":
        if self.period not in VALID_PERIODS:
            raise ValueError("period must be yearly, monthly, or seasonal")
        _validate_month_season(self.month, self.season)
        if self.minLead > self.maxLead:
            raise ValueError("minLead must be <= maxLead")
        return self


class ForecastAllModelsRequest(BaseModel):
    """Forecast values for all models across all leads for a given region."""

    region: StatsRegion
    # Which forecast variable to return (precip/temp/wind/humidity). Defaults to
    # precip so older clients that omit it keep their existing behavior.
    variable: str = "precip"


class ExportImageRequest(BaseModel):
    model: str
    statistic: str
    lead: int = Field(..., ge=0, le=MAX_LEAD_DAYS)
    period: str = "yearly"
    month: str | None = None
    season: str | None = None

    _lead_digits = field_validator("lead", mode="before")(_reject_non_digit_lead)

    @model_validator(mode="after")
    def validate_export(self) -> "ExportImageRequest":
        if self.period not in VALID_PERIODS:
            raise ValueError("period must be yearly, monthly, or seasonal")
        _validate_month_season(self.month, self.season)
        return self
