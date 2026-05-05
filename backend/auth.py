"""Cognito JWT verification."""

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
                options={"verify_at_hash": False},
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


def _cognito_oauth_base_url(region: str, domain_prefix: str) -> str | None:
    """HTTPS origin for Hosted UI OAuth endpoints (no trailing slash)."""
    p = domain_prefix.strip()
    if not p:
        return None
    r = region.strip()
    return f"https://{p}.auth.{r}.amazoncognito.com"


def public_auth_config() -> dict[str, Any]:
    """Public SPA settings for Cognito Hosted UI / OAuth (no secrets).

    Hosted UI base URL is built from ``COGNITO_DOMAIN_PREFIX`` + region, or set
    explicitly via ``COGNITO_OAUTH_BASE_URL`` (e.g.
    ``https://your-domain-prefix.auth.us-west-1.amazoncognito.com``) for
    localhost demos when you already have the full URL from the Cognito console.
    """
    pool_id = os.getenv("COGNITO_USER_POOL_ID", "").strip()
    client_id = os.getenv("COGNITO_APP_CLIENT_ID", "").strip()
    if not pool_id or not client_id:
        return {"mode": "none"}
    region = os.getenv(
        "COGNITO_REGION", os.getenv("AWS_REGION", "us-west-1")
    ).strip()
    domain_prefix = os.getenv("COGNITO_DOMAIN_PREFIX", "").strip()
    oauth_override = os.getenv("COGNITO_OAUTH_BASE_URL", "").strip().rstrip("/")
    oauth_base = oauth_override or _cognito_oauth_base_url(region, domain_prefix)
    return {
        "mode": "cognito",
        "region": region,
        "clientId": client_id,
        "domainPrefix": domain_prefix or None,
        "oauthBase": oauth_base,
    }


def build_get_current_user(
    verifier: CognitoJWTVerifier | None,
):
    """Build a FastAPI dependency that returns the authenticated user ID.

    When *verifier* is ``None`` (Cognito env vars missing), returns 503.
    """

    async def get_current_user(
        authorization: str | None = Header(None),
    ) -> str:
        if verifier is None:
            raise HTTPException(
                status_code=503,
                detail="Authentication is not configured on this server",
            )
        if not authorization:
            print(f"[AUTH DEBUG] No Authorization header received")
            raise HTTPException(status_code=401, detail="Missing Authorization header")
        print(f"[AUTH DEBUG] Authorization header present, length={len(authorization)}")
        try:
            return verifier.get_user_id(authorization)
        except HTTPException as exc:
            print(f"[AUTH DEBUG] Verification failed: {exc.detail}")
            raise

    return get_current_user
