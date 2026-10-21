import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { calculateRecentMetrics } from '../lib/overview-metrics.ts';
import type { Run } from '../lib/types.ts';

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
