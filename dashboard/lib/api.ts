import type { Run, RunJob } from './types.ts';

export class ApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

export interface QueueStatus {
  depth: number;
  workers: number;
  status: string;
}

function getBaseUrl(): string {
  if (typeof window !== 'undefined') {
    return '';
  }
  return process.env.NEXT_PUBLIC_SITE_URL || process.env.SITE_URL || 'http://localhost:3000';
}

async function extractErrorMessage(res: Response): Promise<string> {
  try {
    const data = await res.json();
    if (data && typeof data === 'object') {
      if (typeof data.error === 'string' && data.error) {
        return data.error;
      }
      if (typeof data.message === 'string' && data.message) {
        return data.message;
      }
    }
  } catch {
    // Ignore JSON parse errors
  }
  return `Request failed with status ${res.status}`;
}

async function handleApiResponse(res: Response): Promise<Response> {
  if (!res.ok) {
    if (res.status === 401 && typeof window !== 'undefined' && !window.location.pathname.startsWith('/login')) {
      const from = encodeURIComponent(window.location.pathname + window.location.search);
      // eslint-disable-next-line @next/next/no-location-assign-relative-destination
      window.location.href = `/login?from=${from}`;
    }
    const errorMsg = await extractErrorMessage(res);
    throw new ApiError(errorMsg, res.status);
  }
  return res;
}

export async function getRuns(limit = 20): Promise<Run[]> {
  const res = await fetch(`${getBaseUrl()}/api/v1/runs?limit=${limit}`, {
    headers: { 'Content-Type': 'application/json' },
    cache: 'no-store',
  });
  await handleApiResponse(res);
  return res.json();
}

export async function getRun(id: string): Promise<Run> {
  const res = await fetch(`${getBaseUrl()}/api/v1/runs/${id}`, {
    headers: { 'Content-Type': 'application/json' },
    cache: 'no-store',
  });
  await handleApiResponse(res);
  return res.json();
}

export async function getRunJobs(id: string): Promise<RunJob[]> {
  const res = await fetch(`${getBaseUrl()}/api/v1/runs/${id}/jobs`, {
    headers: { 'Content-Type': 'application/json' },
    cache: 'no-store',
  });
  await handleApiResponse(res);
  return res.json();
}

export async function getQueueStatus(): Promise<QueueStatus> {
  const res = await fetch(`${getBaseUrl()}/api/v1/queue`, {
    headers: { 'Content-Type': 'application/json' },
    cache: 'no-store',
  });
  await handleApiResponse(res);
  return res.json();
}

export function streamLogs(
  runId: string,
  onLine: (line: string) => void,
  onDone: () => void,
  onOpen?: () => void
): () => void {
  const url = `${getBaseUrl()}/api/v1/runs/${runId}/logs`;
  const es = new EventSource(url);

  if (onOpen) {
    es.onopen = () => {
      onOpen();
    };
  }

  es.onmessage = (event) => {
    onLine(event.data);
  };

  es.onerror = () => {
    onDone();
    es.close();
  };

  return () => {
    es.close();
  };
}
