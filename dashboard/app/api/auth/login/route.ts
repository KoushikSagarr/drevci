import {
  checkRateLimit,
  createSessionCookieHeader,
  createSessionToken,
  getClientIdentifier,
  recordFailedAttempt,
  resetRateLimit,
  validateStateChangingOrigin,
  verifyPassword,
} from '../../../../lib/auth-server.ts';

export const dynamic = 'force-dynamic';

export async function POST(request: Request): Promise<Response> {
  // 1. Origin validation against login CSRF
  if (!validateStateChangingOrigin(request)) {
    return Response.json(
      { error: 'Forbidden: cross-origin request rejected' },
      { status: 403 }
    );
  }

  // 2. Client rate-limit check
  const clientId = getClientIdentifier(request);
  const rateLimit = checkRateLimit(clientId);
  if (!rateLimit.allowed) {
    return Response.json(
      { error: 'Too many login attempts. Please try again later.' },
      {
        status: 429,
        headers: {
          'Retry-After': String(rateLimit.retryAfterSeconds || 60),
          'Cache-Control': 'no-store',
        },
      }
    );
  }

  // 3. Body validation
  let password: unknown;
  try {
    const body = await request.json();
    password = body?.password;
  } catch {
    recordFailedAttempt(clientId);
    return Response.json(
      { error: 'Invalid request body' },
      { status: 400, headers: { 'Cache-Control': 'no-store' } }
    );
  }

  if (typeof password !== 'string' || password.length === 0) {
    recordFailedAttempt(clientId);
    return Response.json(
      { error: 'Invalid credentials' },
      { status: 401, headers: { 'Cache-Control': 'no-store' } }
    );
  }

  // 4. Verify password with constant-time comparison
  const authResult = await verifyPassword(password);
  if (authResult.code === 'CONFIG_ERROR') {
    return Response.json(
      { error: 'Authentication is unavailable' },
      { status: 500, headers: { 'Cache-Control': 'no-store' } }
    );
  }

  if (!authResult.ok) {
    recordFailedAttempt(clientId);
    return Response.json(
      { error: 'Invalid credentials' },
      { status: 401, headers: { 'Cache-Control': 'no-store' } }
    );
  }

  // 5. Success: clear rate limit and issue signed session cookie
  resetRateLimit(clientId);

  let sessionToken: string;
  try {
    sessionToken = await createSessionToken();
  } catch {
    return Response.json(
      { error: 'Authentication is unavailable' },
      { status: 500, headers: { 'Cache-Control': 'no-store' } }
    );
  }

  const cookieHeader = createSessionCookieHeader(sessionToken);
  return Response.json(
    { success: true },
    {
      status: 200,
      headers: {
        'Set-Cookie': cookieHeader,
        'Cache-Control': 'no-store',
      },
    }
  );
}
