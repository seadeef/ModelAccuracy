"""Pydantic request/response models for saved-shapes CRUD."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

from backend.request_models import StatsRegion

_VALID_SHAPE_TYPES = {"point", "rectangle", "polygon"}


def _validate_region(region: StatsRegion) -> StatsRegion:
    if region.type not in _VALID_SHAPE_TYPES:
        raise ValueError(f"region.type must be one of {_VALID_SHAPE_TYPES}")
    return region


class SavedShapeCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    region: StatsRegion

    @model_validator(mode="after")
    def validate_region(self) -> SavedShapeCreate:
        _validate_region(self.region)
        return self


class SavedShapeUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=200)
    region: StatsRegion | None = None

    @model_validator(mode="after")
    def validate_update(self) -> SavedShapeUpdate:
        if self.name is None and self.region is None:
            raise ValueError("At least one of name or region must be provided")
        if self.region is not None:
            _validate_region(self.region)
        return self


class SavedShapeResponse(BaseModel):
    id: str
    name: str
    region: StatsRegion
    created_at: str
    updated_at: str


class SavedShapeListResponse(BaseModel):
    shapes: list[SavedShapeResponse]
