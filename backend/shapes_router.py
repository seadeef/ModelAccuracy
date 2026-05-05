"""FastAPI router for saved-shapes CRUD (requires authentication)."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Response
from ulid import ULID

from backend.auth import CognitoJWTVerifier, build_get_current_user
from backend.dynamo_store import UserItemStore, user_item_store_from_env
from backend.user_models import (
    SavedShapeCreate,
    SavedShapeListResponse,
    SavedShapeResponse,
    SavedShapeUpdate,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_shapes_router(
    store: UserItemStore | None = None,
    verifier: CognitoJWTVerifier | None = ...,  # type: ignore[assignment]
) -> APIRouter:
    """Build the ``/api/shapes`` router.

    Parameters are injectable for testing; production uses env-based defaults.
    """
    if store is None:
        store = user_item_store_from_env()
    if verifier is ...:
        verifier = CognitoJWTVerifier.from_env()

    get_current_user = build_get_current_user(verifier)

    router = APIRouter(prefix="/api/shapes", tags=["shapes"])

    @router.post("", status_code=201, response_model=SavedShapeResponse)
    async def create_shape(
        body: SavedShapeCreate,
        user_id: str = Depends(get_current_user),
    ):
        shape_id = str(ULID())
        now = _now_iso()
        return store.put_shape(
            user_id=user_id,
            shape_id=shape_id,
            name=body.name,
            region=body.region.model_dump(),
            now=now,
        )

    @router.get("", response_model=SavedShapeListResponse)
    async def list_shapes(user_id: str = Depends(get_current_user)):
        shapes = store.list_shapes(user_id)
        return SavedShapeListResponse(shapes=shapes)

    @router.get("/{shape_id}", response_model=SavedShapeResponse)
    async def get_shape(shape_id: str, user_id: str = Depends(get_current_user)):
        shape = store.get_shape(user_id, shape_id)
        if shape is None:
            raise HTTPException(status_code=404, detail="Shape not found")
        return shape

    @router.put("/{shape_id}", response_model=SavedShapeResponse)
    async def update_shape(
        shape_id: str,
        body: SavedShapeUpdate,
        user_id: str = Depends(get_current_user),
    ):
        updates: dict = {}
        if body.name is not None:
            updates["name"] = body.name
        if body.region is not None:
            updates["region"] = body.region.model_dump()
        result = store.update_shape(user_id, shape_id, updates, _now_iso())
        if result is None:
            raise HTTPException(status_code=404, detail="Shape not found")
        return result

    @router.delete("/{shape_id}", status_code=204, response_class=Response)
    async def delete_shape(shape_id: str, user_id: str = Depends(get_current_user)):
        deleted = store.delete_shape(user_id, shape_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Shape not found")

    return router
