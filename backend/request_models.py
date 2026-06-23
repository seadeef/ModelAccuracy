from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

VALID_REGION_TYPES = {"point", "rectangle", "polygon", "admin", "huc"}
VALID_ADMIN_LEVELS = {"state", "county"}
VALID_HUC_LEVELS = {2, 4, 6, 8, 10, 12, 14, 16}


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
    lead: str | int | None = None
    minLead: int | None = Field(default=None, ge=0)
    maxLead: int | None = Field(default=None, ge=0)
    region: StatsRegion
    period: str = "yearly"
    month: str | None = None
    season: str | None = None
    statistics: list[str] | None = None

    @model_validator(mode="after")
    def validate_request(self) -> "StatsQueryRequest":
        if self.period not in {"yearly", "monthly", "seasonal"}:
            raise ValueError("period must be yearly, monthly, or seasonal")
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
    minLead: int = Field(..., ge=0)
    maxLead: int = Field(..., ge=0)

    @model_validator(mode="after")
    def validate_lead_winners(self) -> "LeadWinnersRequest":
        if self.period not in {"yearly", "monthly", "seasonal"}:
            raise ValueError("period must be yearly, monthly, or seasonal")
        if self.minLead > self.maxLead:
            raise ValueError("minLead must be <= maxLead")
        return self


class ForecastAllModelsRequest(BaseModel):
    """Forecast values for all models across all leads for a given region."""

    region: StatsRegion


class ExportImageRequest(BaseModel):
    model: str
    statistic: str
    lead: str
    period: str = "yearly"
    month: str | None = None
    season: str | None = None
