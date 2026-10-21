import { describe, it, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { NextRequest } from 'next/server.js';
import {
  GET as proxyGET,
  POST as proxyPOST,
  PUT as proxyPUT,
  DELETE as proxyDELETE,
  PATCH as proxyPATCH,
  HEAD as proxyHEAD,
  OPTIONS as proxyOPTIONS,
} from '../app/api/v1/[[...path]]/route.ts';
import { POST as loginPOST } from '../app/api/auth/login/route.ts';
import { POST as logoutPOST } from '../app/api/auth/logout/route.ts';
import { GET as meGET } from '../app/api/auth/me/route.ts';
import { proxy } from '../proxy.ts';
import { getRuns, getRun, getQueueStatus, ApiError } from '../lib/api.ts';
import {
  createSessionToken,
  verifySessionToken,
  isSafeRedirect,
  _resetRateLimitsForTesting,
  base64UrlEncode,
} from '../lib/auth-server.ts';

const TEST_PASSWORD = 'correct-horse-battery-staple-operator';
const TEST_SESSION_SECRET = 'this-is-a-high-entropy-session-secret-for-tests-32';
const TEST_DAEMON_TOKEN = 'secret-test-token-12345';

describe('Dashboard API Proxy & Authentication Integration', () => {
  let mockBackend: http.Server;
  let backendPort: number;
  let receivedHeaders: http.IncomingHttpHeaders[] = [];
  let simulateAuthFailure = false;
  let authCookieHeader = '';

  before(async () => {
    process.env.DASHBOARD_PASSWORD = TEST_PASSWORD;
    process.env.DASHBOARD_SESSION_SECRET = TEST_SESSION_SECRET;
    process.env.DREV_TOKEN = TEST_DAEMON_TOKEN;

    mockBackend = http.createServer((req, res) => {
      receivedHeaders.push(req.headers);

      if (simulateAuthFailure) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'invalid token' }));
        return;
      }

      if (req.url?.startsWith('/api/v1/runs/') && req.url?.endsWith('/logs')) {
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive',
        });
        res.write('data: line 1\n\n');
        res.write('data: line 2\n\n');
        res.end();
        return;
      }

      if (req.url === '/api/v1/queue') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ depth: 5, workers: 2, status: 'healthy' }));
        return;
      }

      if (req.url?.startsWith('/api/v1/runs/notfound')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'run not found' }));
        return;
      }

      if (req.url === '/api/v1/runs/test-run-1' && (req.method === 'PUT' || req.method === 'DELETE' || req.method === 'PATCH')) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ method: req.method, status: 'success' }));
        return;
      }

      if (req.method === 'HEAD' && req.url?.startsWith('/api/v1/runs')) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end();
        return;
      }

      if (req.url?.startsWith('/api/v1/runs')) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify([{ id: 'test-run-1', pipeline_id: 'ci-pipeline', status: 'success' }]));
        return;
      }

      if (req.url === '/api/v1/pipelines/trigger') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ run_id: 'triggered-run-1', status: 'queued' }));
        return;
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok' }));
    });

    await new Promise<void>((resolve) => {
      mockBackend.listen(0, '127.0.0.1', () => {
        const addr = mockBackend.address() as { port: number };
        backendPort = addr.port;
        resolve();
      });
    });

    process.env.DREV_BACKEND_URL = `http://127.0.0.1:${backendPort}`;
    process.env.DREV_NO_ENV_FILE = 'true';

    const validToken = await createSessionToken();
    authCookieHeader = `drev_session=${encodeURIComponent(validToken)}`;
  });

  after(() => {
    delete process.env.DREV_NO_ENV_FILE;
    mockBackend.close();
  });

  beforeEach(() => {
    receivedHeaders = [];
    simulateAuthFailure = false;
    _resetRateLimitsForTesting();
  });

  // 1. Valid login issues the expected HttpOnly cookie
  it('1. Valid login issues the expected HttpOnly cookie', async () => {
    const req = new Request('http://localhost:3000/api/auth/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Host: 'localhost:3000',
        Origin: 'http://localhost:3000',
      },
      body: JSON.stringify({ password: TEST_PASSWORD }),
    });

    const res = await loginPOST(req);
    assert.equal(res.status, 200);

    const data = await res.json();
    assert.equal(data.success, true);

    const setCookie = res.headers.get('set-cookie');
    assert.ok(setCookie, 'Set-Cookie header must be present');
    assert.match(setCookie, /drev_session=/);
    assert.match(setCookie, /HttpOnly/i);
    assert.match(setCookie, /SameSite=Lax/i);
    assert.match(setCookie, /Path=\//);
    assert.match(setCookie, /Max-Age=86400/);

    // Verify token within cookie
    const tokenMatch = setCookie.match(/drev_session=([^;]+)/);
    assert.ok(tokenMatch);
    const token = decodeURIComponent(tokenMatch[1]);
    const verification = await verifySessionToken(token);
    assert.equal(verification.valid, true);
  });

  it('1b. Cookie security settings: local HTTP vs production HTTPS', async () => {
    const oldSecure = process.env.DASHBOARD_COOKIE_SECURE;
    const oldEnv = process.env.NODE_ENV;
    const makeLoginReq = (url: string) =>
      new Request(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: new URL(url).host,
          Origin: new URL(url).origin,
        },
        body: JSON.stringify({ password: TEST_PASSWORD }),
      });

    try {
      // 1. Local HTTP Docker Compose / development (DASHBOARD_COOKIE_SECURE=false)
      process.env.DASHBOARD_COOKIE_SECURE = 'false';
      const resLocal = await loginPOST(makeLoginReq('http://localhost:3000/api/auth/login'));
      const cookieLocal = resLocal.headers.get('set-cookie') || '';
      assert.equal(cookieLocal.includes('Secure'), false, 'Local HTTP cookie must not have Secure flag');

      // 2. Production HTTPS with explicit true (DASHBOARD_COOKIE_SECURE=true)
      process.env.DASHBOARD_COOKIE_SECURE = 'true';
      const resProd = await loginPOST(makeLoginReq('https://ci.example.com/api/auth/login'));
      const cookieProd = resProd.headers.get('set-cookie') || '';
      assert.ok(cookieProd.includes('Secure'), 'Production cookie with explicit true must have Secure flag');

      // 3. Production HTTPS with variable ABSENT (NODE_ENV=production, DASHBOARD_COOKIE_SECURE unset)
      // Must NOT silently weaken to Secure=false!
      (process.env as Record<string, string | undefined>).NODE_ENV = 'production';
      delete process.env.DASHBOARD_COOKIE_SECURE;
      const resProdAbsent = await loginPOST(makeLoginReq('https://ci.example.com/api/auth/login'));
      const cookieProdAbsent = resProdAbsent.headers.get('set-cookie') || '';
      assert.ok(cookieProdAbsent.includes('Secure'), 'Production cookie with absent variable must default to Secure flag');

      // 4. Production HTTPS with variable EMPTY STRING (from Compose ${DASHBOARD_COOKIE_SECURE:-})
      // Must NOT silently weaken to Secure=false!
      process.env.DASHBOARD_COOKIE_SECURE = '';
      const resProdEmpty = await loginPOST(makeLoginReq('https://ci.example.com/api/auth/login'));
      const cookieProdEmpty = resProdEmpty.headers.get('set-cookie') || '';
      assert.ok(cookieProdEmpty.includes('Secure'), 'Production cookie with empty variable must default to Secure flag');

      // 5. Development mode with variable ABSENT (NODE_ENV=development)
      (process.env as Record<string, string | undefined>).NODE_ENV = 'development';
      delete process.env.DASHBOARD_COOKIE_SECURE;
      const resDev = await loginPOST(makeLoginReq('http://localhost:3000/api/auth/login'));
      const cookieDev = resDev.headers.get('set-cookie') || '';
      assert.equal(cookieDev.includes('Secure'), false, 'Development mode cookie without variable must not have Secure flag');
    } finally {
      (process.env as Record<string, string | undefined>).NODE_ENV = oldEnv;
      if (oldSecure !== undefined) {
        process.env.DASHBOARD_COOKIE_SECURE = oldSecure;
      } else {
        delete process.env.DASHBOARD_COOKIE_SECURE;
      }
    }
  });

  // 2. Invalid password is rejected
  it('2. Invalid password is rejected', async () => {
    const req = new Request('http://localhost:3000/api/auth/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Host: 'localhost:3000',
        Origin: 'http://localhost:3000',
      },
      body: JSON.stringify({ password: 'wrong-password-attempt' }),
    });

    const res = await loginPOST(req);
    assert.equal(res.status, 401);

    const data = await res.json();
    assert.equal(data.error, 'Invalid credentials');
    assert.equal(res.headers.get('set-cookie'), null);
  });

  // 3. Missing production configuration and secret reuse combinations fail closed
  it('3. Missing production configuration and secret reuse combinations fail closed', async () => {
    const oldEnv = process.env.NODE_ENV;
    const oldPass = process.env.DASHBOARD_PASSWORD;
    const oldSecret = process.env.DASHBOARD_SESSION_SECRET;
    const oldToken = process.env.DREV_TOKEN;
    (process.env as Record<string, string | undefined>).NODE_ENV = 'production';

    try {
      // 3a. Missing password
      delete process.env.DASHBOARD_PASSWORD;
      const req1 = new Request('http://localhost:3000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: 'localhost:3000',
          Origin: 'http://localhost:3000',
        },
        body: JSON.stringify({ password: 'any' }),
      });
      const res1 = await loginPOST(req1);
      assert.equal(res1.status, 500);
      const data1 = await res1.json();
      assert.equal(data1.error, 'Authentication is unavailable');

      // 3b. Secret too short (< 32 chars)
      process.env.DASHBOARD_PASSWORD = TEST_PASSWORD;
      process.env.DASHBOARD_SESSION_SECRET = 'short';
      const req2 = new Request('http://localhost:3000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: 'localhost:3000',
          Origin: 'http://localhost:3000',
        },
        body: JSON.stringify({ password: TEST_PASSWORD }),
      });
      const res2 = await loginPOST(req2);
      assert.equal(res2.status, 500);

      // 3c. Reuse of DREV_TOKEN as DASHBOARD_PASSWORD fails closed
      process.env.DASHBOARD_PASSWORD = TEST_DAEMON_TOKEN;
      process.env.DASHBOARD_SESSION_SECRET = TEST_SESSION_SECRET;
      const req3 = new Request('http://localhost:3000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: 'localhost:3000',
          Origin: 'http://localhost:3000',
        },
        body: JSON.stringify({ password: TEST_DAEMON_TOKEN }),
      });
      const res3 = await loginPOST(req3);
      assert.equal(res3.status, 500);

      // 3d. Reuse of DREV_TOKEN as DASHBOARD_SESSION_SECRET fails closed
      process.env.DASHBOARD_PASSWORD = TEST_PASSWORD;
      process.env.DASHBOARD_SESSION_SECRET = TEST_DAEMON_TOKEN;
      const req3d = new Request('http://localhost:3000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: 'localhost:3000',
          Origin: 'http://localhost:3000',
        },
        body: JSON.stringify({ password: TEST_PASSWORD }),
      });
      const res3d = await loginPOST(req3d);
      assert.equal(res3d.status, 500);

      // 3e. Reuse of DASHBOARD_PASSWORD as DASHBOARD_SESSION_SECRET fails closed
      const sharedSecret = 'identical-password-and-secret-minimum-32-chars';
      process.env.DASHBOARD_PASSWORD = sharedSecret;
      process.env.DASHBOARD_SESSION_SECRET = sharedSecret;
      const req3e = new Request('http://localhost:3000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: 'localhost:3000',
          Origin: 'http://localhost:3000',
        },
        body: JSON.stringify({ password: sharedSecret }),
      });
      const res3e = await loginPOST(req3e);
      assert.equal(res3e.status, 500);

      // 3f. Multi-token daemon list matching password fails closed
      process.env.DREV_TOKEN = 'tok-1,tok-2,secret-multi-token-3';
      process.env.DASHBOARD_PASSWORD = 'tok-2';
      process.env.DASHBOARD_SESSION_SECRET = TEST_SESSION_SECRET;
      const req3f = new Request('http://localhost:3000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: 'localhost:3000',
          Origin: 'http://localhost:3000',
        },
        body: JSON.stringify({ password: 'tok-2' }),
      });
      const res3f = await loginPOST(req3f);
      assert.equal(res3f.status, 500);
    } finally {
      (process.env as Record<string, string | undefined>).NODE_ENV = oldEnv;
      process.env.DASHBOARD_PASSWORD = oldPass;
      process.env.DASHBOARD_SESSION_SECRET = oldSecret;
      process.env.DREV_TOKEN = oldToken;
    }
  });

  // 4. Missing, malformed, expired, and tampered session cookies are rejected
  it('4. Missing, malformed, expired, and tampered session cookies are rejected', async () => {
    // 4a. Missing / null
    assert.equal((await verifySessionToken(null)).valid, false);
    assert.equal((await verifySessionToken('')).valid, false);

    // 4b. Malformed token format
    assert.equal((await verifySessionToken('not-a-token')).valid, false);
    assert.equal((await verifySessionToken('v2.something.signature')).valid, false);
    assert.equal((await verifySessionToken('v1.invalid_base64.sig')).valid, false);

    // 4c. Tampered signature
    const valid = await createSessionToken();
    const parts = valid.split('.');
    const tamperedSig = `${parts[0]}.${parts[1]}.${parts[2].slice(0, -4)}AAAA`;
    assert.equal((await verifySessionToken(tamperedSig)).valid, false);

    // 4d. Tampered payload
    const fakePayload = base64UrlEncode(new TextEncoder().encode(JSON.stringify({ exp: 9999999999, iat: 1 })));
    const tamperedPayload = `${parts[0]}.${fakePayload}.${parts[2]}`;
    assert.equal((await verifySessionToken(tamperedPayload)).valid, false);

    // 4e. Expired token
    const expiredPayload = base64UrlEncode(
      new TextEncoder().encode(JSON.stringify({ exp: Math.floor(Date.now() / 1000) - 60, iat: Math.floor(Date.now() / 1000) - 120 }))
    );
    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey(
      'raw',
      enc.encode(TEST_SESSION_SECRET),
      { name: 'HMAC', hash: 'SHA-256' },
      false,
      ['sign']
    );
    const sig = await crypto.subtle.sign('HMAC', key, enc.encode(`v1.${expiredPayload}`));
    const expiredToken = `v1.${expiredPayload}.${base64UrlEncode(new Uint8Array(sig))}`;
    assert.equal((await verifySessionToken(expiredToken)).valid, false);
  });

  // 5. Unauthenticated requests on all HTTP methods return 401
  it('5. Unauthenticated requests on all HTTP methods return 401', async () => {
    // GET
    const resGet = await proxyGET(new Request('http://localhost:3000/api/v1/runs'), {
      params: Promise.resolve({ path: ['runs'] }),
    });
    assert.equal(resGet.status, 401);

    // POST
    const resPost = await proxyPOST(
      new Request('http://localhost:3000/api/v1/pipelines/trigger', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Host: 'localhost:3000' },
        body: JSON.stringify({ pipeline_id: 'ci' }),
      }),
      { params: Promise.resolve({ path: ['pipelines', 'trigger'] }) }
    );
    assert.equal(resPost.status, 401);

    // PUT
    const resPut = await proxyPUT(
      new Request('http://localhost:3000/api/v1/runs/test-run-1', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', Host: 'localhost:3000' },
        body: JSON.stringify({ status: 'cancelled' }),
      }),
      { params: Promise.resolve({ path: ['runs', 'test-run-1'] }) }
    );
    assert.equal(resPut.status, 401);

    // DELETE
    const resDelete = await proxyDELETE(
      new Request('http://localhost:3000/api/v1/runs/test-run-1', {
        method: 'DELETE',
        headers: { Host: 'localhost:3000' },
      }),
      { params: Promise.resolve({ path: ['runs', 'test-run-1'] }) }
    );
    assert.equal(resDelete.status, 401);

    // PATCH
    const resPatch = await proxyPATCH(
      new Request('http://localhost:3000/api/v1/runs/test-run-1', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json', Host: 'localhost:3000' },
        body: JSON.stringify({ label: 'urgent' }),
      }),
      { params: Promise.resolve({ path: ['runs', 'test-run-1'] }) }
    );
    assert.equal(resPatch.status, 401);

    // HEAD
    const resHead = await proxyHEAD(
      new Request('http://localhost:3000/api/v1/runs', { method: 'HEAD' }),
      { params: Promise.resolve({ path: ['runs'] }) }
    );
    assert.equal(resHead.status, 401);
  });

  // 6. Authenticated requests forward upstream across supported HTTP methods
  it('6. Authenticated requests forward upstream across supported HTTP methods', async () => {
    receivedHeaders = [];

    // Authenticated PUT
    const putReq = new Request('http://localhost:3000/api/v1/runs/test-run-1', {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        Host: 'localhost:3000',
        Cookie: authCookieHeader,
      },
      body: JSON.stringify({ action: 'cancel' }),
    });
    const putRes = await proxyPUT(putReq, { params: Promise.resolve({ path: ['runs', 'test-run-1'] }) });
    assert.equal(putRes.status, 200);

    // Authenticated DELETE
    const delReq = new Request('http://localhost:3000/api/v1/runs/test-run-1', {
      method: 'DELETE',
      headers: {
        Host: 'localhost:3000',
        Cookie: authCookieHeader,
      },
    });
    const delRes = await proxyDELETE(delReq, { params: Promise.resolve({ path: ['runs', 'test-run-1'] }) });
    assert.equal(delRes.status, 200);

    // Verify upstream received bearer token
    assert.ok(receivedHeaders.length >= 2);
    assert.equal(receivedHeaders[0]['authorization'], `Bearer ${TEST_DAEMON_TOKEN}`);
    assert.equal(receivedHeaders[1]['authorization'], `Bearer ${TEST_DAEMON_TOKEN}`);
  });

  // 7. Unauthenticated OPTIONS behavior cannot bypass authentication or enable wildcard cross-origin access
  it('7. Unauthenticated OPTIONS behavior cannot bypass authentication or enable wildcard cross-origin access', async () => {
    // 7a. Unauthenticated OPTIONS on protected endpoint returns 401
    const req1 = new Request('http://localhost:3000/api/v1/runs', { method: 'OPTIONS' });
    const res1 = await proxyOPTIONS(req1, { params: Promise.resolve({ path: ['runs'] }) });
    assert.equal(res1.status, 401);
    assert.notEqual(res1.headers.get('access-control-allow-origin'), '*');

    // 7b. Public health endpoint OPTIONS does not set wildcard
    const req2 = new Request('http://localhost:3000/api/v1/health', { method: 'OPTIONS' });
    const res2 = await proxyOPTIONS(req2, { params: Promise.resolve({ path: ['health'] }) });
    assert.equal(res2.status, 204);
    assert.notEqual(res2.headers.get('access-control-allow-origin'), '*');

    // 7c. Authenticated OPTIONS sets same-origin when Origin matches Host
    const req3 = new Request('http://localhost:3000/api/v1/runs', {
      method: 'OPTIONS',
      headers: {
        Host: 'localhost:3000',
        Origin: 'http://localhost:3000',
        Cookie: authCookieHeader,
      },
    });
    const res3 = await proxyOPTIONS(req3, { params: Promise.resolve({ path: ['runs'] }) });
    assert.equal(res3.status, 204);
    assert.equal(res3.headers.get('access-control-allow-origin'), 'http://localhost:3000');
    assert.match(res3.headers.get('access-control-allow-methods') || '', /PUT, DELETE/);
  });

  // 8. Rejected proxy requests never call the upstream daemon
  it('8. Rejected proxy requests never call the upstream daemon', async () => {
    receivedHeaders = [];

    const req = new Request('http://localhost:3000/api/v1/runs');
    const res = await proxyGET(req, { params: Promise.resolve({ path: ['runs'] }) });

    assert.equal(res.status, 401);
    assert.equal(receivedHeaders.length, 0, 'Daemon must never be contacted on authentication rejection');
  });

  // 9. Authenticated proxy requests forward the daemon token only server-side
  it('9. Authenticated proxy requests forward the daemon token only server-side', async () => {
    receivedHeaders = [];

    const req = new Request('http://localhost:3000/api/v1/runs', {
      headers: {
        Cookie: authCookieHeader,
      },
    });
    const res = await proxyGET(req, { params: Promise.resolve({ path: ['runs'] }) });

    assert.equal(res.status, 200);
    const body = await res.text();

    // Verify upstream received token
    assert.equal(receivedHeaders.length, 1);
    assert.equal(receivedHeaders[0]['authorization'], `Bearer ${TEST_DAEMON_TOKEN}`);

    // Verify token never appears in client response body or headers
    assert.equal(body.includes(TEST_DAEMON_TOKEN), false);
    assert.equal(res.headers.get('authorization'), null);
  });

  // 10. Logout clears the session with consistent attributes
  it('10. Logout clears the session with consistent attributes and invalidates access', async () => {
    // 10a. Logout request
    const req = new Request('http://localhost:3000/api/auth/logout', {
      method: 'POST',
      headers: {
        Host: 'localhost:3000',
        Origin: 'http://localhost:3000',
        Cookie: authCookieHeader,
      },
    });

    const res = await logoutPOST(req);
    assert.equal(res.status, 200);

    const setCookie = res.headers.get('set-cookie');
    assert.ok(setCookie);
    assert.match(setCookie, /drev_session=;/);
    assert.match(setCookie, /Max-Age=0/);
    assert.match(setCookie, /Expires=Thu, 01 Jan 1970 00:00:00 GMT/);
    assert.match(setCookie, /HttpOnly/i);
    assert.match(setCookie, /SameSite=Lax/i);
    assert.match(setCookie, /Path=\//);

    // 10b. Subsequent access with empty/cleared cookie fails
    const subsequentReq = new Request('http://localhost:3000/api/v1/runs', {
      headers: {
        Cookie: 'drev_session=',
      },
    });
    const subsequentRes = await proxyGET(subsequentReq, { params: Promise.resolve({ path: ['runs'] }) });
    assert.equal(subsequentRes.status, 401);
  });

  // 11. Login rate limiting and untrusted forwarded header behavior
  it('11. Login rate limiting: spoofed X-Forwarded-For cannot bypass 429 threshold', async () => {
    _resetRateLimitsForTesting();
    const oldTrust = process.env.DASHBOARD_TRUST_PROXY;
    delete process.env.DASHBOARD_TRUST_PROXY;

    try {
      // Attacker rotates X-Forwarded-For across 5 failed attempts
      for (let i = 0; i < 5; i++) {
        const spoofedReq = new Request('http://localhost:3000/api/auth/login', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Host: 'localhost:3000',
            Origin: 'http://localhost:3000',
            'X-Forwarded-For': `192.0.2.${i + 1}`,
          },
          body: JSON.stringify({ password: 'bad-password' }),
        });
        const res = await loginPOST(spoofedReq);
        assert.equal(res.status, 401);
      }

      // 6th attempt with yet another spoofed IP MUST be rate-limited (429)
      // because untrusted forwarded headers are ignored without DASHBOARD_TRUST_PROXY=true
      const rateLimitedReq = new Request('http://localhost:3000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: 'localhost:3000',
          Origin: 'http://localhost:3000',
          'X-Forwarded-For': '198.51.100.99',
        },
        body: JSON.stringify({ password: TEST_PASSWORD }),
      });
      const rateLimitedRes = await loginPOST(rateLimitedReq);
      assert.equal(rateLimitedRes.status, 429);
      assert.ok(rateLimitedRes.headers.get('retry-after'));
    } finally {
      if (oldTrust !== undefined) process.env.DASHBOARD_TRUST_PROXY = oldTrust;
    }
  });

  // 11b. Trusted proxy rate limiting mode uses verified client IP
  it('11b. Trusted proxy rate limiting mode extracts verified client IP', async () => {
    _resetRateLimitsForTesting();
    process.env.DASHBOARD_TRUST_PROXY = 'true';

    try {
      // 5 failures from client A (203.0.113.10)
      for (let i = 0; i < 5; i++) {
        const reqA = new Request('http://localhost:3000/api/auth/login', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Host: 'localhost:3000',
            Origin: 'http://localhost:3000',
            'X-Real-IP': '203.0.113.10',
          },
          body: JSON.stringify({ password: 'bad-password' }),
        });
        await loginPOST(reqA);
      }

      // 6th attempt from client A is rate-limited (429)
      const reqABlocked = new Request('http://localhost:3000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: 'localhost:3000',
          Origin: 'http://localhost:3000',
          'X-Real-IP': '203.0.113.10',
        },
        body: JSON.stringify({ password: TEST_PASSWORD }),
      });
      const resABlocked = await loginPOST(reqABlocked);
      assert.equal(resABlocked.status, 429);

      // Client B (203.0.113.20) is NOT blocked
      const reqB = new Request('http://localhost:3000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: 'localhost:3000',
          Origin: 'http://localhost:3000',
          'X-Real-IP': '203.0.113.20',
        },
        body: JSON.stringify({ password: TEST_PASSWORD }),
      });
      const resB = await loginPOST(reqB);
      assert.equal(resB.status, 200);
    } finally {
      delete process.env.DASHBOARD_TRUST_PROXY;
    }
  });

  // 12. Origin validation and CSRF defenses
  it('12. Origin validation rejects cross-origin state-changing requests and null origins', async () => {
    // 12a. Cross-origin login attempt
    const evilLoginReq = new Request('http://localhost:3000/api/auth/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Host: 'localhost:3000',
        Origin: 'https://evil-attacker.com',
      },
      body: JSON.stringify({ password: TEST_PASSWORD }),
    });
    const evilLoginRes = await loginPOST(evilLoginReq);
    assert.equal(evilLoginRes.status, 403);

    // 12b. Cross-origin state change on /api/v1/pipelines/trigger
    const evilTriggerReq = new Request('http://localhost:3000/api/v1/pipelines/trigger', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Host: 'localhost:3000',
        Origin: 'https://evil-attacker.com',
        Cookie: authCookieHeader,
      },
      body: JSON.stringify({ pipeline_id: 'ci' }),
    });
    const evilTriggerRes = await proxyPOST(evilTriggerReq, { params: Promise.resolve({ path: ['pipelines', 'trigger'] }) });
    assert.equal(evilTriggerRes.status, 403);

    // 12c. Origin: null (sandboxed iframe / privacy proxy) rejected on state-changing request
    const nullOriginReq = new Request('http://localhost:3000/api/auth/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Host: 'localhost:3000',
        Origin: 'null',
      },
      body: JSON.stringify({ password: TEST_PASSWORD }),
    });
    const nullOriginRes = await loginPOST(nullOriginReq);
    assert.equal(nullOriginRes.status, 403);

    // 12d. Sec-Fetch-Site: cross-site rejected
    const crossSiteReq = new Request('http://localhost:3000/api/auth/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Host: 'localhost:3000',
        'Sec-Fetch-Site': 'cross-site',
      },
      body: JSON.stringify({ password: TEST_PASSWORD }),
    });
    const crossSiteRes = await loginPOST(crossSiteReq);
    assert.equal(crossSiteRes.status, 403);

    // 12e. Direct CLI request without Origin header and without Sec-Fetch-Site is allowed
    const cliLoginReq = new Request('http://localhost:3000/api/auth/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Host: 'localhost:3000',
      },
      body: JSON.stringify({ password: TEST_PASSWORD }),
    });
    const cliLoginRes = await loginPOST(cliLoginReq);
    assert.equal(cliLoginRes.status, 200);
  });

  // 13. Redirect handling rejects external destinations
  it('13. Redirect handling rejects external destinations and control characters', async () => {
    // Safe destinations
    assert.equal(isSafeRedirect('/'), true);
    assert.equal(isSafeRedirect('/runs'), true);
    assert.equal(isSafeRedirect('/runs/test-run-123'), true);
    assert.equal(isSafeRedirect('/runs?status=failed'), true);

    // Dangerous / Open Redirect targets
    assert.equal(isSafeRedirect('https://evil.com'), false);
    assert.equal(isSafeRedirect('http://evil.com/runs'), false);
    assert.equal(isSafeRedirect('//evil.com'), false);
    assert.equal(isSafeRedirect('/\\evil.com'), false);
    assert.equal(isSafeRedirect('javascript:alert(1)'), false);
    assert.equal(isSafeRedirect('/login'), false); // prevent redirect loop
    assert.equal(isSafeRedirect('/runs\r\nSet-Cookie: evil=1'), false); // control characters

    // Proxy NextRequest navigation redirect test
    const unauthedReq = new NextRequest('http://localhost:3000/runs/run-99');
    const proxyRes1 = await proxy(unauthedReq);
    assert.equal(proxyRes1.status, 307);
    const location1 = proxyRes1.headers.get('location');
    assert.ok(location1?.includes('/login?from=%2Fruns%2Frun-99'));

    // Authenticated on /login with safe ?from=%2F redirects to / (Overview landing page)
    const overviewRedirectReq = new NextRequest('http://localhost:3000/login?from=%2F', {
      headers: {
        Cookie: authCookieHeader,
      },
    });
    const proxyResOverview = await proxy(overviewRedirectReq);
    assert.equal(proxyResOverview.status, 307);
    assert.equal(proxyResOverview.headers.get('location'), 'http://localhost:3000/');

    // Authenticated on /login with malicious ?from=https://evil.com
    const evilRedirectReq = new NextRequest('http://localhost:3000/login?from=https://evil.com', {
      headers: {
        Cookie: authCookieHeader,
      },
    });
    const proxyRes2 = await proxy(evilRedirectReq);
    assert.equal(proxyRes2.status, 307);
    const location2 = proxyRes2.headers.get('location');
    assert.equal(location2, 'http://localhost:3000/runs'); // safely defaulted to /runs
  });

  // 14. Session secrets and daemon tokens never appear in client-visible output
  it('14. Session secrets and daemon tokens never appear in client-visible output', async () => {
    // Check /api/auth/me
    const meReq = new Request('http://localhost:3000/api/auth/me', {
      headers: { Cookie: authCookieHeader },
    });
    const meRes = await meGET(meReq);
    assert.equal(meRes.status, 200);
    const meText = await meRes.text();
    assert.equal(meText.includes(TEST_SESSION_SECRET), false);
    assert.equal(meText.includes(TEST_DAEMON_TOKEN), false);
    assert.equal(meText.includes(TEST_PASSWORD), false);

    // Check proxy error output
    const errReq = new Request('http://localhost:3000/api/v1/runs/notfound', {
      headers: { Cookie: authCookieHeader },
    });
    const errRes = await proxyGET(errReq, { params: Promise.resolve({ path: ['runs', 'notfound'] }) });
    const errText = await errRes.text();
    assert.equal(errText.includes(TEST_SESSION_SECRET), false);
    assert.equal(errText.includes(TEST_DAEMON_TOKEN), false);
  });

  // 15. Stream logs with SSE
  it('proxy/log-stream behavior: streams SSE logs with correct headers', async () => {
    const req = new Request('http://localhost:3000/api/v1/runs/test-run-1/logs', {
      headers: { Cookie: authCookieHeader },
    });
    const res = await proxyGET(req, { params: Promise.resolve({ path: ['runs', 'test-run-1', 'logs'] }) });

    assert.equal(res.status, 200);
    assert.match(res.headers.get('content-type') || '', /text\/event-stream/);
    assert.match(res.headers.get('cache-control') || '', /no-cache/);

    const bodyText = await res.text();
    assert.match(bodyText, /data: line 1/);
    assert.match(bodyText, /data: line 2/);
  });

  // 16. Client API integration
  it('client API: handles authenticated responses and throws ApiError on failure', async () => {
    const proxyServer = http.createServer(async (req, res) => {
      const url = new URL(req.url || '/', 'http://localhost:3000');
      const segments = url.pathname.replace(/^\/api\/v1\/?/, '').split('/').filter(Boolean);
      const headers = new Headers();
      for (const [k, v] of Object.entries(req.headers)) {
        if (typeof v === 'string') headers.set(k, v);
      }
      headers.set('Cookie', authCookieHeader);
      const nextReq = new Request(`http://localhost:3000${url.pathname}${url.search}`, {
        headers,
      });
      const proxyRes = await proxyGET(nextReq, { params: Promise.resolve({ path: segments }) });

      res.writeHead(proxyRes.status, Object.fromEntries(proxyRes.headers.entries()));
      const buf = Buffer.from(await proxyRes.arrayBuffer());
      res.end(buf);
    });

    await new Promise<void>((resolve) => proxyServer.listen(0, '127.0.0.1', resolve));
    const proxyPort = (proxyServer.address() as { port: number }).port;
    process.env.NEXT_PUBLIC_SITE_URL = `http://127.0.0.1:${proxyPort}`;

    try {
      // 1. Success case
      const runs = await getRuns(10);
      assert.equal(runs.length, 1);
      assert.equal(runs[0].id, 'test-run-1');

      const queue = await getQueueStatus();
      assert.equal(queue.depth, 5);

      // 2. Auth failure case - MUST throw ApiError, not return empty []!
      simulateAuthFailure = true;
      await assert.rejects(
        async () => {
          await getRuns(10);
        },
        (err: unknown) => {
          assert.equal(err instanceof ApiError, true);
          assert.equal((err as ApiError).status, 401);
          assert.equal((err as ApiError).message, 'invalid token');
          return true;
        }
      );

      // 3. Not found error
      simulateAuthFailure = false;
      await assert.rejects(
        async () => {
          await getRun('notfound');
        },
        (err: unknown) => {
          assert.equal(err instanceof ApiError, true);
          assert.equal((err as ApiError).status, 404);
          return true;
        }
      );
    } finally {
      delete process.env.NEXT_PUBLIC_SITE_URL;
      proxyServer.close();
    }
  });

  // 17. Stateless token lifecycle: verification until expiry and emergency secret rotation
  it('17. Stateless token lifecycle: verification until expiry and emergency secret rotation', async () => {
    // 17a. Token copied before logout remains valid until expiry
    const copiedToken = await createSessionToken();
    const verifiedBefore = await verifySessionToken(copiedToken);
    assert.equal(verifiedBefore.valid, true);

    // Logout only clears cookie on client; token signature is still mathematically valid
    await logoutPOST(
      new Request('http://localhost:3000/api/auth/logout', {
        method: 'POST',
        headers: { Host: 'localhost:3000', Origin: 'http://localhost:3000' },
      })
    );
    const verifiedAfter = await verifySessionToken(copiedToken);
    assert.equal(verifiedAfter.valid, true);

    // 17b. Rotating DASHBOARD_SESSION_SECRET immediately and globally invalidates all previously issued tokens
    const oldSecret = process.env.DASHBOARD_SESSION_SECRET;
    try {
      process.env.DASHBOARD_SESSION_SECRET = 'new-rotated-secret-key-that-is-at-least-32-chars-long';
      const verifiedRotated = await verifySessionToken(copiedToken);
      assert.equal(verifiedRotated.valid, false, 'Rotated secret must invalidate old session tokens');
    } finally {
      process.env.DASHBOARD_SESSION_SECRET = oldSecret;
    }
  });

  // 18. Next.js Proxy middleware integration: real proxy() function guards pages, APIs, and public assets
  it('18. Next.js Proxy middleware integration: real proxy() function guards pages, APIs, and public assets', async () => {
    // 18a. Unauthenticated API requests receive 401 JSON directly from proxy middleware
    const apiReq1 = new NextRequest('http://localhost:3000/api/v1/runs', { method: 'GET' });
    const apiRes1 = await proxy(apiReq1);
    assert.equal(apiRes1.status, 401);
    const apiData1 = await apiRes1.json();
    assert.equal(apiData1.error, 'Unauthorized');

    const apiReq2 = new NextRequest('http://localhost:3000/api/v1/pipelines/trigger', { method: 'POST' });
    const apiRes2 = await proxy(apiReq2);
    assert.equal(apiRes2.status, 401);
    const apiData2 = await apiRes2.json();
    assert.equal(apiData2.error, 'Unauthorized');

    // 18b. Public health endpoint passes through proxy without auth
    const healthReq = new NextRequest('http://localhost:3000/api/v1/health');
    const healthRes = await proxy(healthReq);
    assert.equal(healthRes.status, 200);

    // 18c. Public auth endpoints pass through proxy without auth
    const authReq = new NextRequest('http://localhost:3000/api/auth/me');
    const authRes = await proxy(authReq);
    assert.equal(authRes.status, 200);

    // 18d. Unauthenticated page navigations redirect to /login with encoded from destination
    const pageReq1 = new NextRequest('http://localhost:3000/runs');
    const pageRes1 = await proxy(pageReq1);
    assert.equal(pageRes1.status, 307);
    assert.ok(pageRes1.headers.get('location')?.includes('/login?from=%2Fruns'));

    const pageReq2 = new NextRequest('http://localhost:3000/runs/pipeline-xyz');
    const pageRes2 = await proxy(pageReq2);
    assert.equal(pageRes2.status, 307);
    assert.ok(pageRes2.headers.get('location')?.includes('/login?from=%2Fruns%2Fpipeline-xyz'));

    // 18e. Authenticated page navigations pass through
    const authedPageReq = new NextRequest('http://localhost:3000/runs', {
      headers: { Cookie: authCookieHeader },
    });
    const authedPageRes = await proxy(authedPageReq);
    assert.equal(authedPageRes.status, 200);

    // 18f. Root path / is a protected route: redirects to /login?from=%2F when unauthenticated
    const rootUnauthed = new NextRequest('http://localhost:3000/');
    const rootUnauthedRes = await proxy(rootUnauthed);
    assert.equal(rootUnauthedRes.status, 307);
    assert.ok(rootUnauthedRes.headers.get('location')?.includes('/login?from=%2F'));

    // Authenticated request to / is served directly (Overview landing page, not redirected to /runs)
    const rootAuthed = new NextRequest('http://localhost:3000/', {
      headers: { Cookie: authCookieHeader },
    });
    const rootAuthedRes = await proxy(rootAuthed);
    assert.equal(rootAuthedRes.status, 200);
    assert.equal(rootAuthedRes.headers.get('location'), null);

    // 18g. Framework assets pass through
    const staticReq = new NextRequest('http://localhost:3000/_next/static/chunks/app.js');
    const staticRes = await proxy(staticReq);
    assert.equal(staticRes.status, 200);

    const faviconReq = new NextRequest('http://localhost:3000/favicon.ico');
    const faviconRes = await proxy(faviconReq);
    assert.equal(faviconRes.status, 200);
  });

  // 19. Health check endpoint security: minimal public status with zero secret exposure
  it('19. Health check endpoint security: minimal public status with zero secret exposure', async () => {
    // 19a. GET /api/v1/health is accessible unauthenticated
    const healthReq = new Request('http://localhost:3000/api/v1/health');
    const healthRes = await proxyGET(healthReq, { params: Promise.resolve({ path: ['health'] }) });
    assert.equal(healthRes.status, 200);

    const healthText = await healthRes.text();
    const healthJson = JSON.parse(healthText);
    assert.equal(healthJson.status, 'ok');

    // 19b. Verify no daemon tokens, operator passwords, or session secrets are exposed
    assert.equal(healthText.includes(TEST_DAEMON_TOKEN), false);
    assert.equal(healthText.includes(TEST_PASSWORD), false);
    assert.equal(healthText.includes(TEST_SESSION_SECRET), false);

    // 19c. OPTIONS on /api/v1/health has no wildcard CORS header
    const optReq = new Request('http://localhost:3000/api/v1/health', { method: 'OPTIONS' });
    const optRes = await proxyOPTIONS(optReq, { params: Promise.resolve({ path: ['health'] }) });
    assert.equal(optRes.status, 204);
    assert.notEqual(optRes.headers.get('access-control-allow-origin'), '*');
  });
});
