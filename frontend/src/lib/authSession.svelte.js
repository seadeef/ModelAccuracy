// Cognito Hosted UI (PKCE) auth for the same Bearer tokens the API verifies.

const LS_ACCESS = 'modelaccuracy_cognito_access';
const LS_ID = 'modelaccuracy_cognito_id';
const SS_STATE = 'modelaccuracy_oauth_state';
const SS_VERIFIER = 'modelaccuracy_pkce_verifier';
const SS_LOGIN_CTX = 'modelaccuracy_oauth_login_ctx';

/** @type {{ ready: boolean, mode: string, region: string, clientId: string, domainPrefix: string | null, oauthBase: string | null, userLabel: string | null, hasSession: boolean }} */
export const authSession = $state({
  ready: false,
  mode: 'unknown',
  region: '',
  clientId: '',
  domainPrefix: null,
  oauthBase: null,
  userLabel: null,
  hasSession: false,
});

function b64urlFromBytes(bytes) {
  let bin = '';
  bytes.forEach((b) => {
    bin += String.fromCharCode(b);
  });
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/u, '');
}

function randomB64urlUrl(len) {
  const a = new Uint8Array(len);
  crypto.getRandomValues(a);
  return b64urlFromBytes(a);
}

async function sha256B64url(ascii) {
  const data = new TextEncoder().encode(ascii);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return b64urlFromBytes(new Uint8Array(hash));
}

/** @param {string} jwt */
function decodeJwtPayload(jwt) {
  try {
    const part = jwt.split('.')[1];
    if (!part) return null;
    const b64 = part.replace(/-/g, '+').replace(/_/g, '/');
    const pad = b64.length % 4 === 0 ? '' : '='.repeat(4 - (b64.length % 4));
    const json = atob(b64 + pad);
    return JSON.parse(json);
  } catch {
    return null;
  }
}

function oauthBaseUrl(region, domainPrefix) {
  return `https://${domainPrefix}.auth.${region}.amazoncognito.com`;
}

/** Normalized Hosted UI origin (no trailing slash), or null. */
function getCognitoOAuthBase() {
  const raw = authSession.oauthBase?.trim().replace(/\/+$/u, '');
  if (raw) return raw;
  const { region, domainPrefix } = authSession;
  if (region?.trim() && domainPrefix?.trim()) {
    return oauthBaseUrl(region.trim(), domainPrefix.trim());
  }
  return null;
}

function redirectUri() {
  return `${window.location.origin}${window.location.pathname}`;
}

function stripOAuthParamsFromUrl() {
  const u = new URL(window.location.href);
  if (!u.searchParams.has('code') && !u.searchParams.has('state')) return;
  u.searchParams.delete('code');
  u.searchParams.delete('state');
  const q = u.searchParams.toString();
  u.search = q ? `?${q}` : '';
  window.history.replaceState({}, '', u.pathname + u.search + u.hash);
}

/**
 * @param {object} ctx
 * @param {string} ctx.region
 * @param {string} ctx.clientId
 * @param {string} ctx.domainPrefix
 */
async function exchangeCodeForTokens(code, ctx) {
  const base = ctx.oauthBase?.replace(/\/+$/u, '');
  if (!base) throw new Error('Missing OAuth base URL');
  const verifier = sessionStorage.getItem(SS_VERIFIER);
  sessionStorage.removeItem(SS_VERIFIER);
  sessionStorage.removeItem(SS_STATE);
  sessionStorage.removeItem(SS_LOGIN_CTX);
  if (!verifier) throw new Error('Missing PKCE verifier');

  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: ctx.clientId,
    code,
    redirect_uri: redirectUri(),
    code_verifier: verifier,
  });
  const resp = await fetch(`${base}/oauth2/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });
  if (!resp.ok) {
    const t = await resp.text();
    throw new Error(`Token exchange failed (${resp.status}): ${t.slice(0, 200)}`);
  }
  return resp.json();
}

function applyAuthSessionFromConfig(data) {
  authSession.mode = data.mode ?? 'none';
  authSession.region = data.region ?? '';
  authSession.clientId = data.clientId ?? '';
  authSession.domainPrefix = data.domainPrefix ?? null;
  const ob = data.oauthBase;
  authSession.oauthBase =
    typeof ob === 'string' && ob.trim() ? ob.trim().replace(/\/+$/u, '') : null;
}

function syncUserLabelFromStorage() {
  authSession.hasSession = Boolean(
    localStorage.getItem(LS_ID) || localStorage.getItem(LS_ACCESS),
  );
  const idTok = localStorage.getItem(LS_ID);
  if (!idTok) {
    authSession.userLabel = null;
    return;
  }
  const claims = decodeJwtPayload(idTok);
  if (!claims) {
    authSession.userLabel = null;
    return;
  }
  authSession.userLabel =
    claims.email ||
    claims['cognito:username'] ||
    claims.preferred_username ||
    claims.sub ||
    null;
}

async function fetchAuthConfig() {
  try {
    const resp = await fetch('/api/auth/config');
    if (!resp.ok) {
      applyAuthSessionFromConfig({ mode: 'none' });
      return;
    }
    const data = await resp.json();
    applyAuthSessionFromConfig(data);
  } catch {
    applyAuthSessionFromConfig({ mode: 'none' });
  }
}

/**
 * Optional Vite env override for Hosted UI when /api/auth/config is unavailable:
 * VITE_COGNITO_CLIENT_ID plus either VITE_COGNITO_OAUTH_BASE_URL or
 * (VITE_COGNITO_DOMAIN_PREFIX + VITE_COGNITO_REGION). Values are public.
 */
function mergeVitePublicCognitoConfig() {
  const clientId = import.meta.env.VITE_COGNITO_CLIENT_ID;
  const oauthBaseRaw = import.meta.env.VITE_COGNITO_OAUTH_BASE_URL;
  const domainPrefix = import.meta.env.VITE_COGNITO_DOMAIN_PREFIX;
  const region = import.meta.env.VITE_COGNITO_REGION;
  const cid = typeof clientId === 'string' ? clientId.trim() : '';
  if (!cid) return;
  const oauthFromEnv =
    typeof oauthBaseRaw === 'string' && oauthBaseRaw.trim()
      ? oauthBaseRaw.trim().replace(/\/+$/u, '')
      : '';
  const dp = typeof domainPrefix === 'string' ? domainPrefix.trim() : '';
  const reg = typeof region === 'string' ? region.trim() : '';
  const oauthComputed = dp && reg ? oauthBaseUrl(reg, dp) : '';
  const oauthBase = oauthFromEnv || oauthComputed;
  if (!oauthBase) return;
  authSession.mode = 'cognito';
  authSession.clientId = cid;
  authSession.region = reg;
  authSession.domainPrefix = dp || null;
  authSession.oauthBase = oauthBase;
}

async function completeOAuthCallbackIfPresent() {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  const state = params.get('state');
  if (!code || !state) return;

  const expected = sessionStorage.getItem(SS_STATE);
  const ctxRaw = sessionStorage.getItem(SS_LOGIN_CTX);
  stripOAuthParamsFromUrl();

  if (!expected || state !== expected || !ctxRaw) return;

  let ctx;
  try {
    ctx = JSON.parse(ctxRaw);
  } catch {
    return;
  }
  if (!ctx.clientId || !ctx.oauthBase) return;

  try {
    const tok = await exchangeCodeForTokens(code, ctx);
    if (tok.access_token) localStorage.setItem(LS_ACCESS, tok.access_token);
    if (tok.id_token) localStorage.setItem(LS_ID, tok.id_token);
    else localStorage.removeItem(LS_ID);
  } catch (e) {
    console.error(e);
    localStorage.removeItem(LS_ACCESS);
    localStorage.removeItem(LS_ID);
  }
}

/** Call once at startup (handles OAuth redirect, loads /api/auth/config). */
export async function initAuth() {
  try {
    await completeOAuthCallbackIfPresent();
  } catch (e) {
    console.warn('[auth] OAuth callback failed', e);
  }
  await fetchAuthConfig();
  mergeVitePublicCognitoConfig();
  syncUserLabelFromStorage();
  authSession.ready = true;
}

export function isSignedIn() {
  return authSession.hasSession;
}

/** Headers to merge into fetch() for authenticated API routes (e.g. saved shapes). */
export function authHeaders() {
  const t = localStorage.getItem(LS_ID) || localStorage.getItem(LS_ACCESS);
  if (!t) return {};
  const claims = decodeJwtPayload(t);
  const exp = claims?.exp;
  if (typeof exp === 'number' && Date.now() / 1000 > exp - 30) {
    localStorage.removeItem(LS_ACCESS);
    localStorage.removeItem(LS_ID);
    authSession.userLabel = null;
    authSession.hasSession = false;
    return {};
  }
  return { Authorization: `Bearer ${t}` };
}

export function beginCognitoSignIn() {
  const clientId = authSession.clientId?.trim();
  const base = getCognitoOAuthBase();
  if (!clientId || !base) return;

  const state = randomB64urlUrl(32);
  const verifier = randomB64urlUrl(64);
  sessionStorage.setItem(SS_STATE, state);
  sessionStorage.setItem(SS_VERIFIER, verifier);
  sessionStorage.setItem(
    SS_LOGIN_CTX,
    JSON.stringify({ clientId, oauthBase: base }),
  );

  sha256B64url(verifier).then((challenge) => {
    const u = new URL(`${base}/oauth2/authorize`);
    u.searchParams.set('client_id', clientId);
    u.searchParams.set('response_type', 'code');
    u.searchParams.set('scope', 'openid email phone');
    u.searchParams.set('redirect_uri', redirectUri());
    u.searchParams.set('state', state);
    u.searchParams.set('code_challenge_method', 'S256');
    u.searchParams.set('code_challenge', challenge);
    window.location.assign(u.toString());
  });
}

export function signOutCognito() {
  localStorage.removeItem(LS_ACCESS);
  localStorage.removeItem(LS_ID);
  authSession.userLabel = null;
  authSession.hasSession = false;
  const clientId = authSession.clientId?.trim();
  const base = getCognitoOAuthBase();
  if (clientId && base) {
    const logout = new URL(`${base}/logout`);
    logout.searchParams.set('client_id', clientId);
    logout.searchParams.set('logout_uri', redirectUri());
    window.location.href = logout.toString();
    return;
  }
  window.location.reload();
}
