import { NextResponse } from 'next/server.js';
import type { NextRequest } from 'next/server.js';
import { isRequestAuthenticated, isSafeRedirect } from './lib/auth-server.ts';

export async function proxy(request: NextRequest) {
  const { pathname, search } = request.nextUrl;

  // 1. Static and internal Next.js assets
  if (
    pathname.startsWith('/_next') ||
    pathname === '/favicon.ico' ||
    pathname.startsWith('/static')
  ) {
    return NextResponse.next();
  }

  // 2. Auth endpoints are public (protected by rate limiting / csrf within routes)
  if (pathname.startsWith('/api/auth/')) {
    return NextResponse.next();
  }

  // 3. Explicit minimal public healthcheck endpoint
  if (pathname === '/api/v1/health') {
    return NextResponse.next();
  }

  // Check session validity
  const authenticated = await isRequestAuthenticated(request);

  // 4. All other API endpoints on /api/v1/* require authentication
  if (pathname.startsWith('/api/v1/')) {
    if (!authenticated) {
      return NextResponse.json(
        { error: 'Unauthorized' },
        { status: 401, headers: { 'Cache-Control': 'no-store' } }
      );
    }
    return NextResponse.next();
  }

  // 5. Login page handling
  if (pathname === '/login') {
    if (authenticated) {
      const from = request.nextUrl.searchParams.get('from');
      const target = isSafeRedirect(from) ? from! : '/runs';
      return NextResponse.redirect(new URL(target, request.url));
    }
    return NextResponse.next();
  }

  // 6. Root page handling
  if (pathname === '/') {
    if (authenticated) {
      return NextResponse.redirect(new URL('/runs', request.url));
    }
    return NextResponse.redirect(new URL('/login?from=/runs', request.url));
  }

  // 7. Protected pages (/runs, /runs/[runId], etc.)
  if (!authenticated) {
    const fromPath = pathname + (search || '');
    const redirectTarget = `/login?from=${encodeURIComponent(fromPath)}`;
    return NextResponse.redirect(new URL(redirectTarget, request.url));
  }

  return NextResponse.next();
}

export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
};
