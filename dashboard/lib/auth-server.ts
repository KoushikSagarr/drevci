import { getBackendToken, getDashboardPassword, getDashboardSessionSecret } from './server-config.ts';

export const SESSION_COOKIE_NAME = 'drev_session';
export const SESSION_MAX_AGE_SECONDS = 86400; // 24 hours
export const MIN_SECRET_LENGTH = 32;

// --- Base64URL Helpers (Web-Crypto & Edge compatible) ---
export function base64UrlEncode(bytes: Uint8Array): string {
  let binary = '';
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

export function base64UrlDecode(str: string): Uint8Array {
  let base64 = str.replace(/-/g, '+').replace(/_/g, '/');
  while (base64.length % 4) {
    base64 += '=';
  }
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

// --- Auth Configuration & Validation ---
export interface AuthConfigStatus {
  ok: boolean;
  reason?: string;
}

export function getAuthConfigStatus(): AuthConfigStatus {
  const password = getDashboardPassword();
  const secret = getDashboardSessionSecret();
  const daemonToken = getBackendToken();

  if (!password || password.trim() === '') {
    return { ok: false, reason: 'DASHBOARD_PASSWORD is not configured' };
  }
  if (!secret || secret.trim() === '') {
    return { ok: false, reason: 'DASHBOARD_SESSION_SECRET is not configured' };
  }
  if (secret.length < MIN_SECRET_LENGTH) {
    return { ok: false, reason: `DASHBOARD_SESSION_SECRET must be at least ${MIN_SECRET_LENGTH} characters long` };
  }
  if (daemonToken) {
    if (password === daemonToken) {
      return { ok: false, reason: 'DASHBOARD_PASSWORD must not be identical to DREV_TOKEN' };
    }
    if (secret === daemonToken) {
      return { ok: false, reason: 'DASHBOARD_SESSION_SECRET must not be identical to DREV_TOKEN' };
    }
  }

  return { ok: true };
}

export function isCookieSecure(): boolean {
  if (process.env.DASHBOARD_COOKIE_SECURE !== undefined) {
    return process.env.DASHBOARD_COOKIE_SECURE === 'true';
  }
  return process.env.NODE_ENV === 'production';
}

// --- Constant-time comparison ---
export function timingSafeEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    diff |= a[i] ^ b[i];
  }
  return diff === 0;
}

// --- Password Verification ---
export async function verifyPassword(
  inputPassword: unknown
): Promise<{ ok: boolean; code: 'OK' | 'INVALID_CREDENTIALS' | 'CONFIG_ERROR' }> {
  if (typeof inputPassword !== 'string' || inputPassword.length === 0) {
    return { ok: false, code: 'INVALID_CREDENTIALS' };
  }

  const configStatus = getAuthConfigStatus();
  if (!configStatus.ok) {
    return { ok: false, code: 'CONFIG_ERROR' };
  }

  const configuredPassword = getDashboardPassword()!;
  const encoder = new TextEncoder();

  // Hash both with SHA-256 to guarantee fixed buffer length and avoid length leaks
  const [inputHash, confHash] = await Promise.all([
    crypto.subtle.digest('SHA-256', encoder.encode(inputPassword)),
    crypto.subtle.digest('SHA-256', encoder.encode(configuredPassword)),
  ]);

  const match = timingSafeEqual(new Uint8Array(inputHash), new Uint8Array(confHash));
  if (match) {
    return { ok: true, code: 'OK' };
  }
  return { ok: false, code: 'INVALID_CREDENTIALS' };
}

// --- HMAC-SHA256 Key Cache ---
let cachedCryptoKey: { secret: string; key: CryptoKey } | null = null;

async function getHmacKey(secret: string): Promise<CryptoKey> {
  if (cachedCryptoKey && cachedCryptoKey.secret === secret) {
    return cachedCryptoKey.key;
  }
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign', 'verify']
  );
  cachedCryptoKey = { secret, key };
  return key;
}

// --- Session Creation & Verification ---
export interface SessionPayload {
  exp: number;
  iat: number;
}

export async function createSessionToken(): Promise<string> {
  const config = getAuthConfigStatus();
  if (!config.ok) {
    throw new Error('Cannot create session: authentication configuration is invalid');
  }

  const secret = getDashboardSessionSecret()!;
  const now = Math.floor(Date.now() / 1000);
  const payload: SessionPayload = {
    exp: now + SESSION_MAX_AGE_SECONDS,
    iat: now,
  };

  const encoder = new TextEncoder();
  const payloadJson = JSON.stringify(payload);
  const payloadBase64 = base64UrlEncode(encoder.encode(payloadJson));

  const signedContent = `v1.${payloadBase64}`;
  const key = await getHmacKey(secret);
  const sigBuffer = await crypto.subtle.sign('HMAC', key, encoder.encode(signedContent));
  const sigBase64 = base64UrlEncode(new Uint8Array(sigBuffer));

  return `${signedContent}.${sigBase64}`;
}

export async function verifySessionToken(
  token: unknown
): Promise<{ valid: boolean; payload?: SessionPayload }> {
  if (typeof token !== 'string' || !token) {
    return { valid: false };
  }

  const parts = token.split('.');
  if (parts.length !== 3 || parts[0] !== 'v1') {
    return { valid: false };
  }

  const [, payloadBase64, sigBase64] = parts;
  if (!payloadBase64 || !sigBase64) {
    return { valid: false };
  }

  const config = getAuthConfigStatus();
  if (!config.ok) {
    return { valid: false };
  }

  const secret = getDashboardSessionSecret()!;

  try {
    const encoder = new TextEncoder();
    const decoder = new TextDecoder();

    // Verify HMAC signature first
    const signedContent = `v1.${payloadBase64}`;
    const key = await getHmacKey(secret);
    const signatureBytes = base64UrlDecode(sigBase64);
    const isValidSig = await crypto.subtle.verify(
      'HMAC',
      key,
      signatureBytes as unknown as BufferSource,
      encoder.encode(signedContent) as unknown as BufferSource
    );

    if (!isValidSig) {
      return { valid: false };
    }

    // Parse and validate payload
    const payloadBytes = base64UrlDecode(payloadBase64);
    const payloadJson = decoder.decode(payloadBytes);
    const payload = JSON.parse(payloadJson) as SessionPayload;

    if (typeof payload.exp !== 'number' || typeof payload.iat !== 'number') {
      return { valid: false };
    }

    const now = Math.floor(Date.now() / 1000);
    if (payload.exp <= now) {
      return { valid: false };
    }

    return { valid: true, payload };
  } catch {
    return { valid: false };
  }
}

// --- Request Cookie Extraction ---
export function getSessionCookieFromRequest(req: Request): string | null {
  const cookieHeader = req.headers.get('cookie');
  if (!cookieHeader) return null;

  const cookies = cookieHeader.split(';');
  for (const c of cookies) {
    const [name, ...val] = c.trim().split('=');
    if (name === SESSION_COOKIE_NAME) {
      return decodeURIComponent(val.join('='));
    }
  }
  return null;
}

export async function isRequestAuthenticated(req: Request): Promise<boolean> {
  const token = getSessionCookieFromRequest(req);
  if (!token) return false;
  const result = await verifySessionToken(token);
  return result.valid;
}

// --- Cookie Header Serialization ---
export function createSessionCookieHeader(token: string): string {
  const secure = isCookieSecure() ? '; Secure' : '';
  return `${SESSION_COOKIE_NAME}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${SESSION_MAX_AGE_SECONDS}${secure}`;
}

export function clearSessionCookieHeader(): string {
  const secure = isCookieSecure() ? '; Secure' : '';
  return `${SESSION_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0${secure}`;
}

// --- Safe Redirect Validation ---
export function isSafeRedirect(target: unknown): boolean {
  if (typeof target !== 'string' || !target) return false;
  // Must start with a single /
  if (!target.startsWith('/') || target.startsWith('//') || target.startsWith('/\\')) {
    return false;
  }
  // No protocol scheme or backslash escape
  if (target.includes('://') || target.includes('\\')) {
    return false;
  }
  // Prevent redirect loop to /login
  const pathOnly = target.split('?')[0].split('#')[0];
  if (pathOnly === '/login') {
    return false;
  }
  return true;
}

// --- State-Changing Origin / CSRF Defense ---
export function validateStateChangingOrigin(req: Request): boolean {
  const method = req.method.toUpperCase();
  if (['GET', 'HEAD', 'OPTIONS'].includes(method)) {
    return true;
  }

  const host = req.headers.get('host');
  if (!host) {
    return false;
  }

  const origin = req.headers.get('origin');
  if (origin) {
    try {
      const originUrl = new URL(origin);
      return originUrl.host.toLowerCase() === host.toLowerCase();
    } catch {
      return false;
    }
  }

  const referer = req.headers.get('referer');
  if (referer) {
    try {
      const refererUrl = new URL(referer);
      return refererUrl.host.toLowerCase() === host.toLowerCase();
    } catch {
      return false;
    }
  }

  // If both Origin and Referer are absent on a state-changing browser request,
  // allow non-browser API clients only if Origin wasn't sent, but reject if header was tampered.
  return true;
}

// --- Login Rate Limiting ---
interface RateLimitEntry {
  failures: number;
  windowStart: number;
  blockedUntil?: number;
}

const RATE_LIMIT_WINDOW_MS = 5 * 60 * 1000; // 5 minutes
const MAX_FAILURES = 5;
const MAX_STORE_ENTRIES = 5000;

const rateLimitStore = new Map<string, RateLimitEntry>();

function pruneExpiredEntries(): void {
  const now = Date.now();
  for (const [key, entry] of rateLimitStore.entries()) {
    if (now - entry.windowStart > RATE_LIMIT_WINDOW_MS && (!entry.blockedUntil || entry.blockedUntil < now)) {
      rateLimitStore.delete(key);
    }
  }
}

export function getClientIdentifier(req: Request): string {
  // Never trust arbitrary X-Forwarded-For unless explicitly configured
  if (process.env.DASHBOARD_TRUST_PROXY === 'true') {
    const forwarded = req.headers.get('x-forwarded-for');
    if (forwarded) {
      const ips = forwarded.split(',').map((s) => s.trim()).filter(Boolean);
      if (ips.length > 0) return ips[0];
    }
  }
  return 'direct-operator';
}

export function checkRateLimit(clientId: string): { allowed: boolean; retryAfterSeconds?: number } {
  const now = Date.now();
  const entry = rateLimitStore.get(clientId);

  if (!entry) {
    return { allowed: true };
  }

  if (entry.blockedUntil && entry.blockedUntil > now) {
    const retryAfterSeconds = Math.ceil((entry.blockedUntil - now) / 1000);
    return { allowed: false, retryAfterSeconds };
  }

  if (now - entry.windowStart > RATE_LIMIT_WINDOW_MS) {
    rateLimitStore.delete(clientId);
    return { allowed: true };
  }

  if (entry.failures >= MAX_FAILURES) {
    entry.blockedUntil = entry.windowStart + RATE_LIMIT_WINDOW_MS;
    const retryAfterSeconds = Math.ceil((entry.blockedUntil - now) / 1000);
    return { allowed: false, retryAfterSeconds };
  }

  return { allowed: true };
}

export function recordFailedAttempt(clientId: string): void {
  const now = Date.now();
  if (rateLimitStore.size >= MAX_STORE_ENTRIES) {
    pruneExpiredEntries();
  }

  const entry = rateLimitStore.get(clientId);
  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW_MS) {
    rateLimitStore.set(clientId, { failures: 1, windowStart: now });
  } else {
    entry.failures += 1;
    if (entry.failures >= MAX_FAILURES) {
      entry.blockedUntil = now + RATE_LIMIT_WINDOW_MS;
    }
  }
}

export function resetRateLimit(clientId: string): void {
  rateLimitStore.delete(clientId);
}

export function _resetRateLimitsForTesting(): void {
  rateLimitStore.clear();
}
