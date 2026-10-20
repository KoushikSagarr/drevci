import {
  isRequestAuthenticated,
  validateStateChangingOrigin,
} from '../../../../lib/auth-server.ts';
import { getBackendToken, getBackendUrl } from '../../../../lib/server-config.ts';

export const dynamic = 'force-dynamic';

async function proxyRequest(
  request: Request,
  context: { params: Promise<{ path?: string[] }> }
): Promise<Response> {
  const { path } = await context.params;
  const subpath = Array.isArray(path) ? path.map(encodeURIComponent).join('/') : '';
  const reqUrl = new URL(request.url);

  // 1. Authentication boundary: /api/v1/health is public, all other endpoints require authenticated session
  if (subpath !== 'health') {
    // Defense-in-depth: Reject unauthenticated requests immediately. Never contact daemon.
    const authenticated = await isRequestAuthenticated(request);
    if (!authenticated) {
      return Response.json(
        { error: 'Unauthorized' },
        { status: 401, headers: { 'Cache-Control': 'no-store' } }
      );
    }

    // Origin validation for state-changing operations
    if (!validateStateChangingOrigin(request)) {
      return Response.json(
        { error: 'Forbidden: cross-origin request rejected' },
        { status: 403, headers: { 'Cache-Control': 'no-store' } }
      );
    }
  }

  const token = getBackendToken();

  // Fail early with explicit 500 status if server credentials are not configured (unless health check)
  if (!token && subpath !== 'health') {
    return Response.json(
      { error: 'Server authentication credential missing: DREV_TOKEN is not configured' },
      { status: 500, headers: { 'Cache-Control': 'no-store' } }
    );
  }

  const backendBase = getBackendUrl();
  const targetUrl = new URL(`/api/v1/${subpath}${reqUrl.search}`, backendBase);

  const upstreamHeaders = new Headers();
  if (token) {
    upstreamHeaders.set('Authorization', `Bearer ${token}`);
  }

  const contentType = request.headers.get('content-type');
  if (contentType) {
    upstreamHeaders.set('Content-Type', contentType);
  }

  const accept = request.headers.get('accept');
  if (accept) {
    upstreamHeaders.set('Accept', accept);
  }

  let body: ArrayBuffer | undefined = undefined;
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    body = await request.arrayBuffer();
  }

  try {
    const upstreamRes = await fetch(targetUrl.toString(), {
      method: request.method,
      headers: upstreamHeaders,
      body,
      cache: 'no-store',
      signal: request.signal,
    });

    const resContentType = upstreamRes.headers.get('content-type');
    const isSSE = resContentType?.includes('text/event-stream');

    const responseHeaders = new Headers();
    if (resContentType) {
      responseHeaders.set('Content-Type', resContentType);
    }

    if (isSSE) {
      responseHeaders.set('Cache-Control', 'no-cache, no-transform');
      responseHeaders.set('Connection', 'keep-alive');
      responseHeaders.set('X-Accel-Buffering', 'no');
    } else {
      responseHeaders.set('Cache-Control', 'no-store');
    }

    return new Response(upstreamRes.body, {
      status: upstreamRes.status,
      headers: responseHeaders,
    });
  } catch (err: unknown) {
    if (err instanceof Error && err.name === 'AbortError') {
      return new Response(null, { status: 499 });
    }
    return Response.json(
      { error: 'Failed to connect to backend server. Ensure drevd is running.' },
      { status: 502, headers: { 'Cache-Control': 'no-store' } }
    );
  }
}

export async function GET(
  request: Request,
  context: { params: Promise<{ path?: string[] }> }
) {
  return proxyRequest(request, context);
}

export async function POST(
  request: Request,
  context: { params: Promise<{ path?: string[] }> }
) {
  return proxyRequest(request, context);
}

export async function OPTIONS(
  request: Request,
  context: { params: Promise<{ path?: string[] }> }
) {
  const { path } = await context.params;
  const subpath = Array.isArray(path) ? path.map(encodeURIComponent).join('/') : '';

  if (subpath !== 'health') {
    const authenticated = await isRequestAuthenticated(request);
    if (!authenticated) {
      return Response.json(
        { error: 'Unauthorized' },
        { status: 401, headers: { 'Cache-Control': 'no-store' } }
      );
    }
  }

  const host = request.headers.get('host');
  const origin = request.headers.get('origin');
  const headers = new Headers();
  headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  headers.set('Access-Control-Allow-Headers', 'Content-Type');

  if (origin && host) {
    try {
      const originUrl = new URL(origin);
      if (originUrl.host.toLowerCase() === host.toLowerCase()) {
        headers.set('Access-Control-Allow-Origin', origin);
      }
    } catch {
      // ignore invalid origin header
    }
  }

  return new Response(null, {
    status: 204,
    headers,
  });
}
