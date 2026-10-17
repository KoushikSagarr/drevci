import { getBackendToken, getBackendUrl } from '../../../../lib/server-config.ts';

export const dynamic = 'force-dynamic';

async function proxyRequest(
  request: Request,
  context: { params: Promise<{ path?: string[] }> }
): Promise<Response> {
  const { path } = await context.params;
  const subpath = Array.isArray(path) ? path.map(encodeURIComponent).join('/') : '';
  const reqUrl = new URL(request.url);

  const token = getBackendToken();

  // Fail early with explicit 500 status if server credentials are not configured (unless health check)
  if (!token && subpath !== 'health') {
    return Response.json(
      { error: 'Server authentication credential missing: DREV_TOKEN is not configured' },
      { status: 500 }
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
      { status: 502 }
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

export async function OPTIONS() {
  return new Response(null, {
    status: 204,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Authorization, Content-Type',
    },
  });
}
