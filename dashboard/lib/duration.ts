/**
 * Duration formatting utilities for pipeline runs and jobs.
 */

export function formatDuration(started?: string, finished?: string, nowMs?: number): string {
  if (!started || started === '0001-01-01T00:00:00Z') return 'Pending...';
  const start = new Date(started).getTime();
  if (isNaN(start) || start <= 0) return 'Pending...';

  const hasFinished = finished && finished !== '0001-01-01T00:00:00Z';
  const end = hasFinished ? new Date(finished).getTime() : (nowMs || Date.now());
  const diff = Math.max(0, Math.floor((end - start) / 1000));

  if (diff < 60) return `${diff}s`;
  const m = Math.floor(diff / 60);
  const s = diff % 60;
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}
