"""Cognito JWT verification and dev-mode auth fallback."""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any

from fastapi import HTTPException, Header


class CognitoJWTVerifier:
    """Verify Cognito-issued JWTs against the User Pool's public JWKS."""

    def __init__(self, pool_id: str, client_id: str, region: str):
        self.pool_id = pool_id
        self.client_id = client_id
        self.region = region
        self.issuer = f"https://cognito-idp.{region}.amazonaws.com/{pool_id}"
        self._jwks: dict[str, Any] | None = None

    @classmethod
    def from_env(cls) -> CognitoJWTVerifier | None:
        """Return a verifier if Cognito env vars are set, else ``None``."""
        pool_id = os.getenv("COGNITO_USER_POOL_ID", "").strip()
        if not pool_id:
            return None
        client_id = os.getenv("COGNITO_APP_CLIENT_ID", "").strip()
        if not client_id:
            raise RuntimeError(
                "COGNITO_USER_POOL_ID is set but COGNITO_APP_CLIENT_ID is missing"
            )
        region = os.getenv(
            "COGNITO_REGION", os.getenv("AWS_REGION", "us-west-1")
        ).strip()
        return cls(pool_id=pool_id, client_id=client_id, region=region)

    def _fetch_jwks(self) -> dict[str, Any]:
        url = f"{self.issuer}/.well-known/jwks.json"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read())

    @property
    def jwks(self) -> dict[str, Any]:
        if self._jwks is None:
            self._jwks = self._fetch_jwks()
        return self._jwks

    def _refresh_jwks(self) -> dict[str, Any]:
        """Force-refresh JWKS (handles key rotation)."""
        self._jwks = self._fetch_jwks()
        return self._jwks

    def verify_token(self, token: str) -> dict[str, Any]:
        """Decode and validate a Cognito JWT. Returns the claims dict."""
        from jose import jwt as jose_jwt, JWTError, jwk

        # Decode header to find the signing key
        try:
            unverified_header = jose_jwt.get_unverified_header(token)
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token header")

        kid = unverified_header.get("kid")
        if not kid:
            raise HTTPException(status_code=401, detail="Token missing kid")

        # Find the matching key in JWKS
        key = self._find_key(kid)
        if key is None:
            # Key rotation may have happened — refresh once and retry
            self._refresh_jwks()
            key = self._find_key(kid)
        if key is None:
            raise HTTPException(status_code=401, detail="Token signing key not found")

        try:
            claims = jose_jwt.decode(
                token,
                key,
                algorithms=["RS256"],
                audience=self.client_id,
                issuer=self.issuer,
            )
        except JWTError as exc:
            raise HTTPException(status_code=401, detail=f"Token verification failed: {exc}")

        # Cognito access tokens use "client_id" instead of "aud"
        token_use = claims.get("token_use", "")
        if token_use == "access":
            if claims.get("client_id") != self.client_id:
                raise HTTPException(status_code=401, detail="Token client_id mismatch")
        elif token_use != "id":
            raise HTTPException(status_code=401, detail="Unexpected token_use")

        return claims

    def _find_key(self, kid: str) -> dict[str, Any] | None:
        for key in self.jwks.get("keys", []):
            if key.get("kid") == kid:
                return key
        return None

    def get_user_id(self, authorization: str) -> str:
        """Extract and verify Bearer token, return the Cognito ``sub``."""
        if not authorization.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="Expected Bearer token")
        token = authorization[7:].strip()
        if not token:
            raise HTTPException(status_code=401, detail="Empty Bearer token")
        claims = self.verify_token(token)
        sub = claims.get("sub")
        if not sub:
            raise HTTPException(status_code=401, detail="Token missing sub claim")
        return sub


def _is_dev_auth_enabled() -> bool:
    return os.getenv("MODELACCURACY_DEV_AUTH", "").strip().lower() in ("1", "true", "yes")


def build_get_current_user(
    verifier: CognitoJWTVerifier | None,
):
    """Build a FastAPI dependency that returns the authenticated user ID.

    When *verifier* is ``None`` and dev auth is enabled, the ``X-Dev-User-Id``
    header is accepted instead of a real JWT.
    """

    async def get_current_user(
        authorization: str | None = Header(None),
        x_dev_user_id: str | None = Header(None, alias="X-Dev-User-Id"),
    ) -> str:
        # Cognito path
        if verifier is not None:
            if not authorization:
                raise HTTPException(status_code=401, detail="Missing Authorization header")
            return verifier.get_user_id(authorization)

        # Dev-mode path
        if _is_dev_auth_enabled():
            if x_dev_user_id:
                return x_dev_user_id
            if authorization:
                # Allow Bearer tokens in dev mode too (pass-through, no verification)
                if authorization.lower().startswith("bearer "):
                    token = authorization[7:].strip()
                    if token:
                        return token
            raise HTTPException(
                status_code=401,
                detail="Dev auth: provide X-Dev-User-Id header",
            )

        # Auth not configured at all
        raise HTTPException(
            status_code=503,
            detail="Authentication is not configured on this server",
        )

    return get_current_user
