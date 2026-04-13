"""DynamoDB storage for user-owned items (saved shapes, etc.)."""

from __future__ import annotations

import os
from typing import Any, Protocol

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover - optional for local dev
    boto3 = None
    ClientError = Exception


class UserItemStore(Protocol):
    """Abstract interface for user-item persistence (mirrors StaticStore pattern)."""

    def put_shape(
        self, user_id: str, shape_id: str, name: str, region: dict[str, Any], now: str
    ) -> dict[str, Any]: ...

    def get_shape(self, user_id: str, shape_id: str) -> dict[str, Any] | None: ...

    def list_shapes(self, user_id: str) -> list[dict[str, Any]]: ...

    def update_shape(
        self, user_id: str, shape_id: str, updates: dict[str, Any], now: str
    ) -> dict[str, Any] | None: ...

    def delete_shape(self, user_id: str, shape_id: str) -> bool: ...


def _pk(user_id: str) -> str:
    return f"USER#{user_id}"


def _shape_sk(shape_id: str) -> str:
    return f"SHAPE#{shape_id}"


def _shape_from_item(item: dict[str, Any]) -> dict[str, Any]:
    """Convert a raw DynamoDB item to a shape response dict."""
    sk: str = item["SK"]
    return {
        "id": sk.removeprefix("SHAPE#"),
        "name": item["name"],
        "region": item["shape"],
        "created_at": item["created_at"],
        "updated_at": item["updated_at"],
    }


class DynamoUserItemStore:
    """DynamoDB-backed implementation of :class:`UserItemStore`."""

    def __init__(self, table_name: str, resource=None):
        if resource is None:
            if boto3 is None:
                raise RuntimeError("boto3 is required for DynamoUserItemStore")
            resource = boto3.resource("dynamodb")
        self._table = resource.Table(table_name)

    def put_shape(
        self, user_id: str, shape_id: str, name: str, region: dict[str, Any], now: str
    ) -> dict[str, Any]:
        item = {
            "PK": _pk(user_id),
            "SK": _shape_sk(shape_id),
            "item_type": "shape",
            "name": name,
            "shape": region,
            "created_at": now,
            "updated_at": now,
        }
        self._table.put_item(Item=item)
        return _shape_from_item(item)

    def get_shape(self, user_id: str, shape_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(
            Key={"PK": _pk(user_id), "SK": _shape_sk(shape_id)}
        )
        item = resp.get("Item")
        if item is None:
            return None
        return _shape_from_item(item)

    def list_shapes(self, user_id: str) -> list[dict[str, Any]]:
        resp = self._table.query(
            KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
            ExpressionAttributeValues={":pk": _pk(user_id), ":prefix": "SHAPE#"},
        )
        return [_shape_from_item(item) for item in resp.get("Items", [])]

    def update_shape(
        self, user_id: str, shape_id: str, updates: dict[str, Any], now: str
    ) -> dict[str, Any] | None:
        expr_parts: list[str] = ["#updated_at = :now"]
        attr_names: dict[str, str] = {"#updated_at": "updated_at"}
        attr_values: dict[str, Any] = {":now": now}

        if "name" in updates:
            expr_parts.append("#name = :name")
            attr_names["#name"] = "name"
            attr_values[":name"] = updates["name"]

        if "region" in updates:
            expr_parts.append("#shape = :shape")
            attr_names["#shape"] = "shape"
            attr_values[":shape"] = updates["region"]

        try:
            resp = self._table.update_item(
                Key={"PK": _pk(user_id), "SK": _shape_sk(shape_id)},
                UpdateExpression="SET " + ", ".join(expr_parts),
                ExpressionAttributeNames=attr_names,
                ExpressionAttributeValues=attr_values,
                ConditionExpression="attribute_exists(PK)",
                ReturnValues="ALL_NEW",
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return None
            raise
        return _shape_from_item(resp["Attributes"])

    def delete_shape(self, user_id: str, shape_id: str) -> bool:
        try:
            self._table.delete_item(
                Key={"PK": _pk(user_id), "SK": _shape_sk(shape_id)},
                ConditionExpression="attribute_exists(PK)",
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise
        return True


def user_item_store_from_env(resource=None) -> UserItemStore:
    """Build a :class:`DynamoUserItemStore` from environment variables."""
    table_name = os.getenv("DYNAMODB_USER_ITEMS_TABLE", "ModelAccuracy-UserItems").strip()
    return DynamoUserItemStore(table_name=table_name, resource=resource)
