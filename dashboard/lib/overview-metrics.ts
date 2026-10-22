import type { Run } from './types.ts';

export interface RecentMetrics {
  sampleLimit: number;
  completedSampleCount: number;
  durationSampleCount: number;
  succeededCount: number;
  failedCount: number;
  cancelledCount: number;
  passRate: number | null; // percentage 0-100, or null if no completed runs
  meanDurationSec: number | null;
  meanDurationFormatted: string;
  recentFailures: Run[];
  activeInBatch: number;
}

/**
 * Calculates operational metrics over the most recent runs.
 *
 * Terminal status accounting (success, failed, cancelled) determines the pass-rate
 * sample, regardless of whether a run has valid timestamps.
 *
 * Mean duration calculations strictly require valid start and finish timestamps
 * with a non-negative delta, and only average over runs with valid timing.
 */
export function calculateRecentMetrics(runs: Run[], sampleLimit = 25): RecentMetrics {
  if (!runs || runs.length === 0) {
    return {
      sampleLimit,
      completedSampleCount: 0,
      durationSampleCount: 0,
      succeededCount: 0,
      failedCount: 0,
      cancelledCount: 0,
      passRate: null,
      meanDurationSec: null,
      meanDurationFormatted: '—',
      recentFailures: [],
      activeInBatch: 0,
    };
  }

  // Active runs in this batch (pending or running)
  const activeInBatch = runs.filter(
    (r) => r.status === 'running' || r.status === 'pending'
  ).length;

  // Recent failures across the loaded batch (up to 5 most recent)
  const recentFailures = runs.filter((r) => r.status === 'failed').slice(0, 5);

  // 1. Terminal status accounting: all completed runs (success, failed, cancelled)
  const terminalRuns = runs.filter((r) => {
    return r.status === 'success' || r.status === 'failed' || r.status === 'cancelled';
  });

  // Take up to sampleLimit terminal runs as the documented recent sample
  const sample = terminalRuns.slice(0, sampleLimit);
  const completedSampleCount = sample.length;

  if (completedSampleCount === 0) {
    return {
      sampleLimit,
      completedSampleCount: 0,
      durationSampleCount: 0,
      succeededCount: 0,
      failedCount: 0,
      cancelledCount: 0,
      passRate: null,
      meanDurationSec: null,
      meanDurationFormatted: '—',
      recentFailures,
      activeInBatch,
    };
  }

  const succeededCount = sample.filter((r) => r.status === 'success').length;
  const failedCount = sample.filter((r) => r.status === 'failed').length;
  const cancelledCount = sample.filter((r) => r.status === 'cancelled').length;

  // Succeeded count over all terminal runs in sample (failed and cancelled count against pass rate)
  const passRate = Math.round((succeededCount / completedSampleCount) * 100);

  // 2. Duration validity: only runs in sample with valid non-zero timestamps and end >= start
  const timedRuns = sample.filter((r) => {
    if (!r.started_at || r.started_at === '0001-01-01T00:00:00Z') return false;
    if (!r.finished_at || r.finished_at === '0001-01-01T00:00:00Z') return false;

    const start = new Date(r.started_at).getTime();
    const end = new Date(r.finished_at).getTime();
    return !isNaN(start) && !isNaN(end) && end >= start;
  });

  const durationSampleCount = timedRuns.length;
  let meanDurationSec: number | null = null;
  let meanDurationFormatted = '—';

  if (durationSampleCount > 0) {
    const totalDurationMs = timedRuns.reduce((sum, r) => {
      const start = new Date(r.started_at).getTime();
      const end = new Date(r.finished_at).getTime();
      return sum + (end - start);
    }, 0);

    meanDurationSec = Math.max(0, Math.round(totalDurationMs / durationSampleCount / 1000));
    meanDurationFormatted = formatSeconds(meanDurationSec);
  }

  return {
    sampleLimit,
    completedSampleCount,
    durationSampleCount,
    succeededCount,
    failedCount,
    cancelledCount,
    passRate,
    meanDurationSec,
    meanDurationFormatted,
    recentFailures,
    activeInBatch,
  };
}

export function formatSeconds(sec: number): string {
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

export function formatDurationSec(sec: number | null | undefined): string {
  if (sec === null || sec === undefined || isNaN(sec)) return '—';
  return formatSeconds(sec);
}
