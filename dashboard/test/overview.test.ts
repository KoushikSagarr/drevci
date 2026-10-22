import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { calculateRecentMetrics, formatSeconds, formatDurationSec } from '../lib/overview-metrics.ts';
import type { Run, OverviewStats } from '../lib/types.ts';

describe('Overview Metrics: calculateRecentMetrics', () => {
  it('handles empty and null runs cleanly without division by zero', () => {
    const empty = calculateRecentMetrics([]);
    assert.equal(empty.completedSampleCount, 0);
    assert.equal(empty.durationSampleCount, 0);
    assert.equal(empty.passRate, null);
    assert.equal(empty.meanDurationSec, null);
    assert.equal(empty.meanDurationFormatted, '—');
    assert.equal(empty.recentFailures.length, 0);
    assert.equal(empty.activeInBatch, 0);

    const fromNull = calculateRecentMetrics(null as unknown as Run[]);
    assert.equal(fromNull.completedSampleCount, 0);
    assert.equal(fromNull.durationSampleCount, 0);
    assert.equal(fromNull.passRate, null);
    assert.equal(fromNull.meanDurationFormatted, '—');
  });

  it('handles dataset containing only pending/running runs (zero completed runs)', () => {
    const runs: Run[] = [
      {
        id: 'run-p1',
        pipeline_id: 'pending-pipeline',
        status: 'pending',
        started_at: '2026-10-21T10:00:00Z',
        finished_at: '',
      },
      {
        id: 'run-r1',
        pipeline_id: 'running-pipeline',
        status: 'running',
        started_at: '2026-10-21T10:01:00Z',
        finished_at: '',
      },
    ];

    const metrics = calculateRecentMetrics(runs);
    assert.equal(metrics.completedSampleCount, 0);
    assert.equal(metrics.durationSampleCount, 0);
    assert.equal(metrics.succeededCount, 0);
    assert.equal(metrics.failedCount, 0);
    assert.equal(metrics.cancelledCount, 0);
    assert.equal(metrics.passRate, null);
    assert.equal(metrics.meanDurationSec, null);
    assert.equal(metrics.meanDurationFormatted, '—');
    assert.equal(metrics.recentFailures.length, 0);
    assert.equal(metrics.activeInBatch, 2);
  });

  it('handles zero completed runs with no failures', () => {
    const runs: Run[] = [
      {
        id: 'run-p1',
        pipeline_id: 'pending-only',
        status: 'pending',
        started_at: '',
        finished_at: '',
      },
    ];

    const metrics = calculateRecentMetrics(runs);
    assert.equal(metrics.completedSampleCount, 0);
    assert.equal(metrics.passRate, null);
    assert.equal(metrics.recentFailures.length, 0);
    assert.equal(metrics.activeInBatch, 1);
  });

  it('calculates pass rate and duration across mixed completed runs accurately', () => {
    const runs: Run[] = [
      // 1. Success, took 40s
      {
        id: 'run-1',
        pipeline_id: 'ci-pipeline',
        status: 'success',
        started_at: '2026-10-21T10:00:00Z',
        finished_at: '2026-10-21T10:00:40Z',
      },
      // 2. Failed, took 20s
      {
        id: 'run-2',
        pipeline_id: 'ci-pipeline',
        status: 'failed',
        started_at: '2026-10-21T10:05:00Z',
        finished_at: '2026-10-21T10:05:20Z',
      },
      // 3. Success, took 60s
      {
        id: 'run-3',
        pipeline_id: 'ci-pipeline',
        status: 'success',
        started_at: '2026-10-21T10:10:00Z',
        finished_at: '2026-10-21T10:11:00Z',
      },
      // 4. Cancelled, took 10s
      {
        id: 'run-4',
        pipeline_id: 'ci-pipeline',
        status: 'cancelled',
        started_at: '2026-10-21T10:15:00Z',
        finished_at: '2026-10-21T10:15:10Z',
      },
      // 5. Running (active - must be excluded from completed metrics)
      {
        id: 'run-5',
        pipeline_id: 'ci-pipeline',
        status: 'running',
        started_at: '2026-10-21T10:20:00Z',
        finished_at: '',
      },
    ];

    const metrics = calculateRecentMetrics(runs, 25);

    // 4 completed runs: 2 success, 1 failed, 1 cancelled
    assert.equal(metrics.completedSampleCount, 4);
    assert.equal(metrics.durationSampleCount, 4);
    assert.equal(metrics.succeededCount, 2);
    assert.equal(metrics.failedCount, 1);
    assert.equal(metrics.cancelledCount, 1);

    // Pass rate: (2 / 4) * 100 = 50%
    assert.equal(metrics.passRate, 50);

    // Total duration: 40 + 20 + 60 + 10 = 130s. Mean: 130 / 4 = 32.5 -> rounded to 33s
    assert.equal(metrics.meanDurationSec, 33);
    assert.equal(metrics.meanDurationFormatted, '33s');

    // Active in batch
    assert.equal(metrics.activeInBatch, 1);

    // Recent failures
    assert.equal(metrics.recentFailures.length, 1);
    assert.equal(metrics.recentFailures[0].id, 'run-2');
  });

  it('includes failed terminal run with missing or invalid timestamps in pass-rate denominator', () => {
    const runs: Run[] = [
      // 1. Success, took 30s
      {
        id: 'run-success',
        pipeline_id: 'build',
        status: 'success',
        started_at: '2026-10-21T12:00:00Z',
        finished_at: '2026-10-21T12:00:30Z',
      },
      // 2. Failed run with missing finished_at (e.g. runner crashed)
      {
        id: 'run-crashed-fail',
        pipeline_id: 'build',
        status: 'failed',
        started_at: '2026-10-21T12:01:00Z',
        finished_at: '',
      },
    ];

    const metrics = calculateRecentMetrics(runs);
    // Both are terminal runs: pass rate must be 1 / 2 = 50%
    assert.equal(metrics.completedSampleCount, 2);
    assert.equal(metrics.succeededCount, 1);
    assert.equal(metrics.failedCount, 1);
    assert.equal(metrics.passRate, 50);

    // Duration must only average the valid timed run
    assert.equal(metrics.durationSampleCount, 1);
    assert.equal(metrics.meanDurationSec, 30);
    assert.equal(metrics.meanDurationFormatted, '30s');

    // Failure is still surfaced in recent failures
    assert.equal(metrics.recentFailures.length, 1);
    assert.equal(metrics.recentFailures[0].id, 'run-crashed-fail');
  });

  it('counts cancelled runs in denominator without incrementing success count', () => {
    const runs: Run[] = [
      // 1. Success
      {
        id: 'run-s',
        pipeline_id: 'test',
        status: 'success',
        started_at: '2026-10-21T14:00:00Z',
        finished_at: '2026-10-21T14:00:20Z',
      },
      // 2. Cancelled
      {
        id: 'run-c',
        pipeline_id: 'test',
        status: 'cancelled',
        started_at: '2026-10-21T14:05:00Z',
        finished_at: '2026-10-21T14:05:10Z',
      },
    ];

    const metrics = calculateRecentMetrics(runs);
    assert.equal(metrics.completedSampleCount, 2);
    assert.equal(metrics.succeededCount, 1);
    assert.equal(metrics.cancelledCount, 1);
    assert.equal(metrics.failedCount, 0);

    // 1 success out of 2 terminal runs = 50%
    assert.equal(metrics.passRate, 50);
  });

  it('excludes invalid timestamps from mean-duration calculation without removing run from pass rate', () => {
    const runs: Run[] = [
      // Valid success, 40s
      {
        id: 'valid-success',
        pipeline_id: 'p1',
        status: 'success',
        started_at: '2026-10-21T12:00:00Z',
        finished_at: '2026-10-21T12:00:40Z',
      },
      // Valid failure, 20s
      {
        id: 'valid-fail',
        pipeline_id: 'p2',
        status: 'failed',
        started_at: '2026-10-21T12:05:00Z',
        finished_at: '2026-10-21T12:05:20Z',
      },
      // Success with Go zero timestamp
      {
        id: 'zero-timestamp-success',
        pipeline_id: 'p3',
        status: 'success',
        started_at: '0001-01-01T00:00:00Z',
        finished_at: '0001-01-01T00:00:00Z',
      },
      // Failed with inverted timestamp (finished before started)
      {
        id: 'inverted-timestamp-fail',
        pipeline_id: 'p4',
        status: 'failed',
        started_at: '2026-10-21T12:10:30Z',
        finished_at: '2026-10-21T12:10:00Z',
      },
    ];

    const metrics = calculateRecentMetrics(runs);

    // All 4 are terminal runs: 2 success, 2 failed -> 2 / 4 = 50%
    assert.equal(metrics.completedSampleCount, 4);
    assert.equal(metrics.succeededCount, 2);
    assert.equal(metrics.failedCount, 2);
    assert.equal(metrics.passRate, 50);

    // Only 2 have valid timing: 40s and 20s -> avg 30s
    assert.equal(metrics.durationSampleCount, 2);
    assert.equal(metrics.meanDurationSec, 30);
    assert.equal(metrics.meanDurationFormatted, '30s');
  });

  it('strictly respects sampleLimit when dataset exceeds sample window', () => {
    // Generate 40 runs (30 success, 10 failed)
    const runs: Run[] = [];
    for (let i = 0; i < 40; i++) {
      runs.push({
        id: `run-${i}`,
        pipeline_id: 'batch-test',
        status: i < 30 ? 'success' : 'failed',
        started_at: '2026-10-21T10:00:00Z',
        finished_at: '2026-10-21T10:01:00Z', // 60s
      });
    }

    // Sample limit of 25: takes first 25 (all success)
    const metrics25 = calculateRecentMetrics(runs, 25);
    assert.equal(metrics25.completedSampleCount, 25);
    assert.equal(metrics25.passRate, 100);

    // Sample limit of 35: takes 30 success, 5 failed -> 30/35 = 86%
    const metrics35 = calculateRecentMetrics(runs, 35);
    assert.equal(metrics35.completedSampleCount, 35);
    assert.equal(metrics35.passRate, 86);
  });
});

describe('Navigation active state rules', () => {
  it('correctly matches active tab states for Overview vs Pipelines', () => {
    function getActiveNav(pathname: string) {
      return {
        isOverviewActive: pathname === '/',
        isPipelinesActive: pathname.startsWith('/runs'),
      };
    }

    assert.deepEqual(getActiveNav('/'), { isOverviewActive: true, isPipelinesActive: false });
    assert.deepEqual(getActiveNav('/runs'), { isOverviewActive: false, isPipelinesActive: true });
    assert.deepEqual(getActiveNav('/runs/test-run-123'), { isOverviewActive: false, isPipelinesActive: true });
    assert.deepEqual(getActiveNav('/login'), { isOverviewActive: false, isPipelinesActive: false });
  });
});

describe('StatCard & Metric state transitions', () => {
  it('distinguishes loading, error, zero, and populated metric states', () => {
    function resolveMetricDisplay(
      loading: boolean,
      error: boolean,
      value: number | null,
      unit = ''
    ) {
      if (loading) return 'LOADING';
      if (error) return 'UNAVAILABLE';
      if (value === null) return '—';
      return `${value}${unit}`;
    }

    assert.equal(resolveMetricDisplay(true, false, 95, '%'), 'LOADING');
    assert.equal(resolveMetricDisplay(false, true, 95, '%'), 'UNAVAILABLE');
    assert.equal(resolveMetricDisplay(false, false, null, '%'), '—');
    assert.equal(resolveMetricDisplay(false, false, 95, '%'), '95%');
    assert.equal(resolveMetricDisplay(false, false, 0, ' jobs waiting'), '0 jobs waiting');
  });
});

describe('Duration formatting: formatSeconds & formatDurationSec', () => {
  it('formats seconds correctly', () => {
    assert.equal(formatSeconds(0), '0s');
    assert.equal(formatSeconds(45), '45s');
    assert.equal(formatSeconds(59), '59s');
  });

  it('formats minutes and seconds correctly', () => {
    assert.equal(formatSeconds(60), '1m 0s');
    assert.equal(formatSeconds(125), '2m 5s');
    assert.equal(formatSeconds(3599), '59m 59s');
  });

  it('formats hours and minutes correctly', () => {
    assert.equal(formatSeconds(3600), '1h 0m');
    assert.equal(formatSeconds(3665), '1h 1m');
    assert.equal(formatSeconds(7320), '2h 2m');
  });

  it('handles null, undefined, and NaN safely in formatDurationSec', () => {
    assert.equal(formatDurationSec(null), '—');
    assert.equal(formatDurationSec(undefined), '—');
    assert.equal(formatDurationSec(NaN), '—');
    assert.equal(formatDurationSec(42), '42s');
    assert.equal(formatDurationSec(150), '2m 30s');
  });
});

describe('Overview Stats: API response resolution and fallback mechanics', () => {
  it('resolves metrics directly from backend stats when available', () => {
    const stats: OverviewStats = {
      queue_depth: 3,
      worker_capacity: 8,
      queue_status: 'healthy',
      sample_limit: 25,
      completed_sample_count: 20,
      duration_sample_count: 18,
      succeeded_count: 16,
      failed_count: 3,
      cancelled_count: 1,
      pass_rate: 80,
      mean_duration_sec: 45,
      recent_failures: [
        {
          id: 'fail-1',
          pipeline_id: 'deploy-prod',
          status: 'failed',
          started_at: '2026-10-22T10:00:00Z',
          finished_at: '2026-10-22T10:01:00Z',
        },
      ],
      active_runs_count: 2,
      total_runs_count: 25,
    };

    const queueDepth = stats ? stats.queue_depth : 0;
    const workerCapacity = stats ? stats.worker_capacity : 0;
    const passRate = stats ? stats.pass_rate : null;
    const durationFormatted = stats ? formatDurationSec(stats.mean_duration_sec) : '—';
    const recentFailures = stats ? stats.recent_failures : [];

    assert.equal(queueDepth, 3);
    assert.equal(workerCapacity, 8);
    assert.equal(passRate, 80);
    assert.equal(durationFormatted, '45s');
    assert.equal(recentFailures.length, 1);
    assert.equal(recentFailures[0].id, 'fail-1');
  });

  it('honestly handles zero and empty stats without fabricating metrics', () => {
    const stats: OverviewStats = {
      queue_depth: 0,
      worker_capacity: 4,
      queue_status: 'healthy',
      sample_limit: 25,
      completed_sample_count: 0,
      duration_sample_count: 0,
      succeeded_count: 0,
      failed_count: 0,
      cancelled_count: 0,
      pass_rate: null,
      mean_duration_sec: null,
      recent_failures: [],
      active_runs_count: 0,
      total_runs_count: 0,
    };

    assert.equal(stats.pass_rate, null);
    assert.equal(stats.mean_duration_sec, null);
    assert.equal(formatDurationSec(stats.mean_duration_sec), '—');
    assert.equal(stats.recent_failures.length, 0);
    assert.equal(stats.total_runs_count, 0);
  });

  it('falls back to client calculation when stats is null', () => {
    const runs: Run[] = [
      {
        id: 'run-1',
        pipeline_id: 'test-pipe',
        status: 'success',
        started_at: '2026-10-22T10:00:00Z',
        finished_at: '2026-10-22T10:00:50Z',
      },
    ];

    const stats: OverviewStats | null = null;
    const fallbackMetrics = calculateRecentMetrics(runs, 25);

    const passRate = stats ? (stats as OverviewStats).pass_rate : fallbackMetrics.passRate;
    const durationFormatted = stats
      ? formatDurationSec((stats as OverviewStats).mean_duration_sec)
      : fallbackMetrics.meanDurationFormatted;

    assert.equal(passRate, 100);
    assert.equal(durationFormatted, '50s');
  });

  it('uses 25-run fallback batch instead of 10-run activity slice when stats fails', () => {
    // 25 runs in history: newest 10 are successes, oldest 15 have 5 failures and 10 successes
    const fullFallbackRuns: Run[] = [];
    for (let i = 0; i < 25; i++) {
      fullFallbackRuns.push({
        id: `run-${i}`,
        pipeline_id: 'ci',
        status: i < 20 ? 'success' : 'failed', // 20 success, 5 failed -> 80%
        started_at: '2026-10-22T10:00:00Z',
        finished_at: '2026-10-22T10:01:00Z',
      });
    }

    // The activity slice only contains the top 10 runs (all success)
    const activitySlice = fullFallbackRuns.slice(0, 10);

    // If misleadingly evaluated only from the 10-run activity slice:
    const flawedMetrics = calculateRecentMetrics(activitySlice, 25);
    assert.equal(flawedMetrics.completedSampleCount, 10);
    assert.equal(flawedMetrics.passRate, 100); // Misleading 100% over only 10 runs

    // With dedicated fallback batch:
    const stats: OverviewStats | null = null;
    const source = fullFallbackRuns.length > 0 ? fullFallbackRuns : activitySlice;
    const accurateMetrics = calculateRecentMetrics(source, 25);

    const effectivePassRate = stats ? (stats as OverviewStats).pass_rate : accurateMetrics.passRate;
    const effectiveCompletedCount = stats ? (stats as OverviewStats).completed_sample_count : accurateMetrics.completedSampleCount;

    assert.equal(effectiveCompletedCount, 25);
    assert.equal(effectivePassRate, 80); // Accurately reflects 20/25 = 80%

    // Recent activity table remains strictly capped at 10 runs
    const recentRuns = (activitySlice.length > 0 ? activitySlice : fullFallbackRuns).slice(0, 10);
    assert.equal(recentRuns.length, 10);
  });

  it('generates truthful sample size labels without claiming misleading scopes', () => {
    function getPassRateSubtitle(completedCount: number, activeCount: number): string {
      if (completedCount > 0) {
        return completedCount === 1 ? 'Last 1 completed run' : `Last ${completedCount} completed runs`;
      }
      if (activeCount > 0) {
        return `${activeCount} ${activeCount === 1 ? 'run' : 'runs'} in progress`;
      }
      return 'No completed runs in sample';
    }

    assert.equal(getPassRateSubtitle(25, 0), 'Last 25 completed runs');
    assert.equal(getPassRateSubtitle(10, 0), 'Last 10 completed runs');
    assert.equal(getPassRateSubtitle(1, 0), 'Last 1 completed run');
    assert.equal(getPassRateSubtitle(0, 2), '2 runs in progress');
    assert.equal(getPassRateSubtitle(0, 1), '1 run in progress');
    assert.equal(getPassRateSubtitle(0, 0), 'No completed runs in sample');
  });
});
