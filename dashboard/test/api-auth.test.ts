import { describe, it, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { NextRequest } from 'next/server.js';
import { GET as proxyGET, POST as proxyPOST, OPTIONS as proxyOPTIONS } from '../app/api/v1/[[...path]]/route.ts';
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

    const validToken = await createSessionToken();
    authCookieHeader = `drev_session=${encodeURIComponent(validToken)}`;
  });

  after(() => {
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

  // 3. Missing production configuration fails closed
  it('3. Missing production configuration fails closed', async () => {
    const oldEnv = process.env.NODE_ENV;
    const oldPass = process.env.DASHBOARD_PASSWORD;
    const oldSecret = process.env.DASHBOARD_SESSION_SECRET;
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
    } finally {
      (process.env as Record<string, string | undefined>).NODE_ENV = oldEnv;
      process.env.DASHBOARD_PASSWORD = oldPass;
      process.env.DASHBOARD_SESSION_SECRET = oldSecret;
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

  // 5. Unauthenticated GET /api/v1/runs returns 401
  it('5. Unauthenticated GET /api/v1/runs returns 401', async () => {
    const req = new Request('http://localhost:3000/api/v1/runs');
    const res = await proxyGET(req, { params: Promise.resolve({ path: ['runs'] }) });

    assert.equal(res.status, 401);
    const data = await res.json();
    assert.equal(data.error, 'Unauthorized');
  });

  // 6. Unauthenticated POST /api/v1/pipelines/trigger returns 401
  it('6. Unauthenticated POST /api/v1/pipelines/trigger returns 401', async () => {
    const req = new Request('http://localhost:3000/api/v1/pipelines/trigger', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Host: 'localhost:3000',
        Origin: 'http://localhost:3000',
      },
      body: JSON.stringify({ pipeline_id: 'ci' }),
    });
    const res = await proxyPOST(req, { params: Promise.resolve({ path: ['pipelines', 'trigger'] }) });

    assert.equal(res.status, 401);
    const data = await res.json();
    assert.equal(data.error, 'Unauthorized');
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

  // 10. Logout clears the session and invalidates subsequent access
  it('10. Logout clears the session and invalidates subsequent access', async () => {
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

    // 10b. Subsequent access with empty/cleared cookie fails
    const subsequentReq = new Request('http://localhost:3000/api/v1/runs', {
      headers: {
        Cookie: 'drev_session=',
      },
    });
    const subsequentRes = await proxyGET(subsequentReq, { params: Promise.resolve({ path: ['runs'] }) });
    assert.equal(subsequentRes.status, 401);
  });

  // 11. Login rate limiting returns 429 after the configured threshold
  it('11. Login rate limiting returns 429 after the configured threshold', async () => {
    _resetRateLimitsForTesting();

    // Perform 5 failed attempts
    for (let i = 0; i < 5; i++) {
      const badReq = new Request('http://localhost:3000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Host: 'localhost:3000',
          Origin: 'http://localhost:3000',
        },
        body: JSON.stringify({ password: 'bad-password' }),
      });
      const res = await loginPOST(badReq);
      assert.equal(res.status, 401);
    }

    // 6th attempt must be rate-limited (429)
    const rateLimitedReq = new Request('http://localhost:3000/api/auth/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Host: 'localhost:3000',
        Origin: 'http://localhost:3000',
      },
      body: JSON.stringify({ password: TEST_PASSWORD }),
    });
    const rateLimitedRes = await loginPOST(rateLimitedReq);
    assert.equal(rateLimitedRes.status, 429);

    const data = await rateLimitedRes.json();
    assert.match(data.error, /Too many login attempts/);
    assert.ok(rateLimitedRes.headers.get('retry-after'));
  });

  // 12. Origin validation rejects cross-origin state-changing requests
  it('12. Origin validation rejects cross-origin state-changing requests', async () => {
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
  });

  // 13. Redirect handling rejects external destinations
  it('13. Redirect handling rejects external destinations', async () => {
    // Safe destinations
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

    // Proxy NextRequest navigation redirect test
    const unauthedReq = new NextRequest('http://localhost:3000/runs/run-99');
    const proxyRes1 = await proxy(unauthedReq);
    assert.equal(proxyRes1.status, 307);
    const location1 = proxyRes1.headers.get('location');
    assert.ok(location1?.includes('/login?from=%2Fruns%2Frun-99'));

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
});
