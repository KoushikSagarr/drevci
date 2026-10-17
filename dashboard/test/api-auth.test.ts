import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { GET } from '../app/api/v1/[[...path]]/route.ts';
import { getRuns, getRun, getQueueStatus, ApiError } from '../lib/api.ts';

describe('Dashboard API Proxy & Authentication Integration', () => {
  let mockBackend: http.Server;
  let backendPort: number;
  let receivedHeaders: http.IncomingHttpHeaders[] = [];
  let simulateAuthFailure = false;

  before(async () => {
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
    process.env.DREV_TOKEN = 'secret-test-token-12345';
  });

  after(() => {
    mockBackend.close();
  });

  it('successful requests: attaches bearer token on server and returns data', async () => {
    receivedHeaders = [];
    simulateAuthFailure = false;

    const req = new Request(`http://localhost:3000/api/v1/runs?limit=100`);
    const res = await GET(req, { params: Promise.resolve({ path: ['runs'] }) });

    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(Array.isArray(data), true);
    assert.equal(data[0].id, 'test-run-1');

    // Upstream must have received Authorization header from server config
    assert.equal(receivedHeaders.length, 1);
    assert.equal(receivedHeaders[0]['authorization'], 'Bearer secret-test-token-12345');
  });

  it('missing credentials: fails with 500 when DREV_TOKEN is unset', async () => {
    const savedToken = process.env.DREV_TOKEN;
    delete process.env.DREV_TOKEN;
    delete process.env.DREV_API_TOKEN;
    delete process.env.DREV_API_TOKENS;
    process.env.DREV_NO_ENV_FILE = 'true';

    try {
      const req = new Request(`http://localhost:3000/api/v1/runs`);
      const res = await GET(req, { params: Promise.resolve({ path: ['runs'] }) });

      assert.equal(res.status, 500);
      const data = await res.json();
      assert.match(data.error, /Server authentication credential missing/);
    } finally {
      process.env.DREV_TOKEN = savedToken;
      delete process.env.DREV_NO_ENV_FILE;
    }
  });

  it('backend authentication failures: forwards 401 when backend rejects token', async () => {
    simulateAuthFailure = true;

    try {
      const req = new Request(`http://localhost:3000/api/v1/runs`);
      const res = await GET(req, { params: Promise.resolve({ path: ['runs'] }) });

      assert.equal(res.status, 401);
      const data = await res.json();
      assert.equal(data.error, 'invalid token');
    } finally {
      simulateAuthFailure = false;
    }
  });

  it('proxy/log-stream behavior: streams SSE logs with correct headers', async () => {
    receivedHeaders = [];
    const req = new Request(`http://localhost:3000/api/v1/runs/test-run-1/logs`);
    const res = await GET(req, { params: Promise.resolve({ path: ['runs', 'test-run-1', 'logs'] }) });

    assert.equal(res.status, 200);
    assert.match(res.headers.get('content-type') || '', /text\/event-stream/);
    assert.match(res.headers.get('cache-control') || '', /no-cache/);

    const bodyText = await res.text();
    assert.match(bodyText, /data: line 1/);
    assert.match(bodyText, /data: line 2/);
  });

  it('client API: throws ApiError on failure and does not silently return empty array', async () => {
    const proxyServer = http.createServer(async (req, res) => {
      const url = new URL(req.url || '/', 'http://localhost:3000');
      const segments = url.pathname.replace(/^\/api\/v1\/?/, '').split('/').filter(Boolean);
      const nextReq = new Request(`http://localhost:3000${url.pathname}${url.search}`);
      const proxyRes = await GET(nextReq, { params: Promise.resolve({ path: segments }) });

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
