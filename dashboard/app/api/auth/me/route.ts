import { isRequestAuthenticated } from '../../../../lib/auth-server.ts';

export const dynamic = 'force-dynamic';

export async function GET(request: Request): Promise<Response> {
  const authenticated = await isRequestAuthenticated(request);
  return Response.json(
    { authenticated },
    {
      status: 200,
      headers: {
        'Cache-Control': 'no-store',
      },
    }
  );
}
