import type { RunJob } from './types';

export interface JobTimelineMetrics {
  job: RunJob;
  jobStartMs: number | null;
  jobEndMs: number | null;
  jobDurationMs: number;
  offsetMs: number;
  offsetPercent: number; // 0 - 100 (relative to run start)
  durationPercent: number; // 0 - 100 (clamped width on track)
  wallClockSharePercent: number; // 0 - 100 (jobDuration / totalRunDuration)
  isLive: boolean;
  isStarted: boolean;
  isFinished: boolean;
  formattedDuration: string;
  formattedOffset: string;
}

export interface RunTimelineSummary {
  runStartMs: number | null;
  runEndMs: number | null;
  totalRunDurationMs: number;
  totalComputeMs: number;
  formattedRunDuration: string;
  formattedTotalCompute: string;
  isLive: boolean;
  hasValidTimeWindow: boolean;
  jobMetrics: JobTimelineMetrics[];
  maxParallelJobs: number;
}

/**
 * Checks whether an ISO timestamp string represents a real, non-zero date.
 */
export function isValidTimestamp(ts?: string | null): boolean {
  if (!ts || ts === '0001-01-01T00:00:00Z' || ts === '0001-01-01T00:00:00') {
    return false;
  }
  const ms = new Date(ts).getTime();
  return !isNaN(ms) && ms > 0;
}

/**
 * Formats a duration in milliseconds to human-readable format.
 */
export function formatMilliseconds(ms: number): string {
  if (!ms || ms <= 0 || isNaN(ms)) return '0s';
  const totalSec = Math.floor(ms / 1000);
  if (totalSec < 60) return `${totalSec}s`;
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  if (m < 60) return s > 0 ? `${m}m ${s}s` : `${m}m`;
  const h = Math.floor(m / 60);
  const remainingM = m % 60;
  return remainingM > 0 ? `${h}h ${remainingM}m` : `${h}h`;
}

/**
 * Formats an offset from run start (in milliseconds).
 */
export function formatOffset(offsetMs: number): string {
  if (!offsetMs || offsetMs <= 0 || isNaN(offsetMs)) return '+0s';
  return `+${formatMilliseconds(offsetMs)}`;
}

/**
 * Calculates timeline metrics for a single job against the run's time window.
 */
export function calculateJobTimeline(
  job: RunJob,
  runStartMs: number | null,
  totalRunDurationMs: number,
  nowMs?: number
): JobTimelineMetrics {
  const isStarted = isValidTimestamp(job.started_at);
  const isFinished = isValidTimestamp(job.finished_at);
  const isLive = job.status === 'running' && isStarted && !isFinished;

  if (!isStarted) {
    return {
      job,
      jobStartMs: null,
      jobEndMs: null,
      jobDurationMs: 0,
      offsetMs: 0,
      offsetPercent: 0,
      durationPercent: 0,
      wallClockSharePercent: 0,
      isLive: false,
      isStarted: false,
      isFinished: false,
      formattedDuration: 'Pending',
      formattedOffset: '—',
    };
  }

  const jobStartMs = new Date(job.started_at).getTime();
  const currentNow = nowMs || Date.now();
  const jobEndMs = isFinished
    ? new Date(job.finished_at).getTime()
    : isLive
    ? currentNow
    : jobStartMs;

  const jobDurationMs = Math.max(0, jobEndMs - jobStartMs);

  let offsetMs = 0;
  let offsetPercent = 0;
  let durationPercent = 0;
  let wallClockSharePercent = 0;

  if (runStartMs !== null && totalRunDurationMs > 0) {
    offsetMs = Math.max(0, jobStartMs - runStartMs);
    offsetPercent = Math.min(100, Math.max(0, (offsetMs / totalRunDurationMs) * 100));

    const rawWallClockShare = (jobDurationMs / totalRunDurationMs) * 100;
    wallClockSharePercent = Math.min(100, Math.max(0, Math.round(rawWallClockShare)));

    // Clamped width on track so offset + width <= 100%
    const maxAvailableWidth = Math.max(0, 100 - offsetPercent);
    durationPercent = Math.min(maxAvailableWidth, Math.max(0, rawWallClockShare));
  }

  return {
    job,
    jobStartMs,
    jobEndMs,
    jobDurationMs,
    offsetMs,
    offsetPercent,
    durationPercent,
    wallClockSharePercent,
    isLive,
    isStarted: true,
    isFinished,
    formattedDuration: formatMilliseconds(jobDurationMs),
    formattedOffset: formatOffset(offsetMs),
  };
}

/**
 * Calculates aggregated timeline summary across the pipeline run and all jobs.
 */
export function calculateRunTimelineSummary(
  run: { started_at?: string; finished_at?: string; status?: string },
  jobs: RunJob[],
  nowMs?: number
): RunTimelineSummary {
  const currentNow = nowMs || Date.now();
  const hasValidStart = isValidTimestamp(run.started_at);
  const hasValidFinish = isValidTimestamp(run.finished_at);
  const isLive = (run.status === 'running' || run.status === 'pending') && !hasValidFinish;

  const runStartMs = hasValidStart ? new Date(run.started_at!).getTime() : null;
  const runEndMs = hasValidFinish
    ? new Date(run.finished_at!).getTime()
    : hasValidStart && isLive
    ? currentNow
    : runStartMs;

  let totalRunDurationMs = 0;
  if (runStartMs !== null && runEndMs !== null) {
    totalRunDurationMs = Math.max(0, runEndMs - runStartMs);
  }

  // If run duration is 0 but jobs have timestamps that extend past runEnd, expand window to encompass jobs
  let effectiveRunStartMs = runStartMs;
  let effectiveRunEndMs = runEndMs;

  for (const job of jobs) {
    if (isValidTimestamp(job.started_at)) {
      const s = new Date(job.started_at).getTime();
      if (effectiveRunStartMs === null || s < effectiveRunStartMs) {
        effectiveRunStartMs = s;
      }
    }
    if (isValidTimestamp(job.finished_at)) {
      const e = new Date(job.finished_at).getTime();
      if (effectiveRunEndMs === null || e > effectiveRunEndMs) {
        effectiveRunEndMs = e;
      }
    }
  }

  if (effectiveRunStartMs !== null && effectiveRunEndMs !== null) {
    totalRunDurationMs = Math.max(totalRunDurationMs, effectiveRunEndMs - effectiveRunStartMs);
  }

  const hasValidTimeWindow = effectiveRunStartMs !== null && totalRunDurationMs > 0;

  let totalComputeMs = 0;
  const jobMetrics: JobTimelineMetrics[] = [];

  for (const job of jobs) {
    const metric = calculateJobTimeline(
      job,
      effectiveRunStartMs,
      totalRunDurationMs,
      currentNow
    );
    jobMetrics.push(metric);
    totalComputeMs += metric.jobDurationMs;
  }

  // Calculate max concurrency / parallel execution
  let maxParallelJobs = 0;
  if (jobMetrics.length > 0) {
    // Collect discrete time boundaries
    const timePoints: { time: number; type: 'start' | 'end' }[] = [];
    for (const m of jobMetrics) {
      if (m.isStarted && m.jobStartMs !== null) {
        timePoints.push({ time: m.jobStartMs, type: 'start' });
        timePoints.push({ time: m.jobEndMs || m.jobStartMs, type: 'end' });
      }
    }
    timePoints.sort((a, b) => a.time - b.time || (a.type === 'end' ? -1 : 1));

    let activeCount = 0;
    for (const pt of timePoints) {
      if (pt.type === 'start') {
        activeCount++;
        if (activeCount > maxParallelJobs) maxParallelJobs = activeCount;
      } else {
        activeCount = Math.max(0, activeCount - 1);
      }
    }
  }

  return {
    runStartMs: effectiveRunStartMs,
    runEndMs: effectiveRunEndMs,
    totalRunDurationMs,
    totalComputeMs,
    formattedRunDuration: formatMilliseconds(totalRunDurationMs),
    formattedTotalCompute: formatMilliseconds(totalComputeMs),
    isLive,
    hasValidTimeWindow,
    jobMetrics,
    maxParallelJobs,
  };
}
