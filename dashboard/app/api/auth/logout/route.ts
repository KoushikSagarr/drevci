import {
  clearSessionCookieHeader,
  validateStateChangingOrigin,
} from '../../../../lib/auth-server.ts';

export const dynamic = 'force-dynamic';

export async function POST(request: Request): Promise<Response> {
  // Origin validation against logout CSRF
  if (!validateStateChangingOrigin(request)) {
    return Response.json(
      { error: 'Forbidden: cross-origin request rejected' },
      { status: 403 }
    );
  }

  const clearCookieHeader = clearSessionCookieHeader();
  return Response.json(
    { success: true },
    {
      status: 200,
      headers: {
        'Set-Cookie': clearCookieHeader,
        'Cache-Control': 'no-store',
      },
    }
  );
}
