import { Run, RunJob } from './types';

const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:9090';
const TOKEN = process.env.NEXT_PUBLIC_API_TOKEN ?? '';

const headers = () => ({
  'Authorization': `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
});

export async function getRuns(limit = 20): Promise<Run[]> {
  try {
    const res = await fetch(`${BASE}/api/v1/runs?limit=${limit}`, { headers: headers(), cache: 'no-store' });
    if (!res.ok) return [];
    return res.json();
  } catch (err) {
    console.error('Network error fetching runs:', err);
    return [];
  }
}

export async function getRun(id: string): Promise<Run> {
  const res = await fetch(`${BASE}/api/v1/runs/${id}`, { headers: headers(), cache: 'no-store' });
  if (!res.ok) throw new Error('Failed to fetch run');
  return res.json();
}

export async function getRunJobs(id: string): Promise<RunJob[]> {
  const res = await fetch(`${BASE}/api/v1/runs/${id}/jobs`, { headers: headers(), cache: 'no-store' });
  if (!res.ok) throw new Error('Failed to fetch run jobs');
  return res.json();
}

export function streamLogs(
  runId: string,
  onLine: (line: string) => void,
  onDone: () => void
): () => void {
  const url = `${BASE}/api/v1/runs/${runId}/logs`;
  
  // Using native EventSource. Note: EventSource doesn't support custom headers (like Authorization) natively in browsers.
  // Since we use a proxy via rewrites in next.config.ts, we can point to the local route if needed,
  // but for external APIs we might need to pass token in query params or rely on proxying.
  // We'll append token to URL if needed or use the proxy route.
  // To keep it simple and standard:
  const es = new EventSource(`${url}?token=${TOKEN}`);

  es.onmessage = (event) => {
    onLine(event.data);
  };

  es.onerror = (err) => {
    onDone();
    es.close();
  };

  return () => {
    es.close();
  };
}
