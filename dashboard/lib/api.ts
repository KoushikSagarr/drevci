import type { Run, RunJob, OverviewStats } from './types.ts';
export type { Run, RunJob, OverviewStats };

export class ApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

export function isNotFoundError(err: unknown): boolean {
  if (err instanceof ApiError) return err.status === 404;
  if (err instanceof Error) {
    return err.message.includes('404') || err.message.toLowerCase().includes('not found');
  }
  if (typeof err === 'string') {
    return err.includes('404') || err.toLowerCase().includes('not found');
  }
  return false;
}

export interface QueueStatus {
  depth: number;
  workers: number;
  status: string;
}

export interface HealthStatus {
  status: 'ok' | string;
  version: string;
  notifications_enabled?: boolean;
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

export async function getStats(limit = 25): Promise<OverviewStats> {
  const res = await fetch(`${getBaseUrl()}/api/v1/stats?limit=${limit}`, {
    headers: { 'Content-Type': 'application/json' },
    cache: 'no-store',
  });
  await handleApiResponse(res);
  return res.json();
}

export async function getHealth(): Promise<HealthStatus> {
  const res = await fetch(`${getBaseUrl()}/api/v1/health`, {
    headers: { 'Content-Type': 'application/json' },
    cache: 'no-store',
  });
  await handleApiResponse(res);
  return res.json();
}

export interface StreamLogsOptions {
  isLive?: () => boolean;
  maxRetries?: number;
  retryDelayMs?: number;
  getRunStatus?: (runId: string) => Promise<string | null>;
}

export function streamLogs(
  runId: string,
  onLine: (line: string) => void,
  onDone: () => void,
  onOpen?: () => void,
  options?: StreamLogsOptions
): () => void {
  const url = `${getBaseUrl()}/api/v1/runs/${runId}/logs`;
  let currentEs: EventSource | null = null;
  let isTeardown = false;
  let retryCount = 0;
  let reconnectTimer: NodeJS.Timeout | null = null;
  const maxRetries = options?.maxRetries ?? 3;
  const retryDelayMs = options?.retryDelayMs ?? 1000;

  const cleanupCurrentEs = () => {
    if (currentEs) {
      currentEs.onopen = null;
      currentEs.onmessage = null;
      currentEs.onerror = null;
      currentEs.close();
      currentEs = null;
    }
  };

  const connect = () => {
    if (isTeardown) return;

    const es = new EventSource(url);
    currentEs = es;

    if (onOpen) {
      es.onopen = () => {
        if (!isTeardown) {
          onOpen();
        }
      };
    }

    es.onmessage = (event) => {
      if (!isTeardown) {
        onLine(event.data);
      }
    };

    es.onerror = async () => {
      if (isTeardown) return;
      cleanupCurrentEs();

      if (options?.isLive && !options.isLive()) {
        // Caller explicitly knows the run is already completed
        if (!isTeardown) {
          onDone();
        }
        return;
      }

      if (retryCount >= maxRetries) {
        // Retry limit reached: terminate cleanly without further retries
        if (!isTeardown) {
          onDone();
        }
        return;
      }

      // Verify whether the run is still active or completed on the backend
      let isStillActive = false;
      try {
        if (options?.getRunStatus) {
          const status = await options.getRunStatus(runId);
          isStillActive = status === 'running' || status === 'pending';
        } else {
          const run = await getRun(runId);
          isStillActive = run.status === 'running' || run.status === 'pending';
        }
      } catch {
        // If status query fails due to network outage, attempt retry since under limit
        isStillActive = true;
      }

      if (isTeardown) return;

      if (!isStillActive) {
        if (!isTeardown) {
          onDone();
        }
        return;
      }

      // Transient disconnection during live run: attempt recovery
      retryCount++;
      if (!isTeardown) {
        reconnectTimer = setTimeout(() => {
          if (!isTeardown) {
            connect();
          }
        }, retryDelayMs);
      }
    };
  };

  connect();

  return () => {
    isTeardown = true;
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    cleanupCurrentEs();
  };
}
