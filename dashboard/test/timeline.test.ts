import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  isValidTimestamp,
  formatMilliseconds,
  formatOffset,
  calculateJobTimeline,
  calculateRunTimelineSummary,
} from '../lib/timeline.ts';
import type { RunJob } from '../lib/types.ts';

describe('Timeline Timestamp Validation and Formatting', () => {
  it('validates real ISO timestamps and rejects zero or missing values', () => {
    assert.equal(isValidTimestamp('2026-10-24T12:00:00Z'), true);
    assert.equal(isValidTimestamp('2026-10-24T12:05:30.123Z'), true);
    assert.equal(isValidTimestamp(''), false);
    assert.equal(isValidTimestamp(undefined), false);
    assert.equal(isValidTimestamp(null), false);
    assert.equal(isValidTimestamp('0001-01-01T00:00:00Z'), false);
    assert.equal(isValidTimestamp('invalid-date'), false);
  });

  it('formats millisecond durations cleanly without fractional seconds', () => {
    assert.equal(formatMilliseconds(0), '0s');
    assert.equal(formatMilliseconds(800), '0s');
    assert.equal(formatMilliseconds(45000), '45s');
    assert.equal(formatMilliseconds(135000), '2m 15s');
    assert.equal(formatMilliseconds(120000), '2m');
    assert.equal(formatMilliseconds(3600000), '1h');
    assert.equal(formatMilliseconds(3900000), '1h 5m');
  });

  it('formats start offsets from run origin', () => {
    assert.equal(formatOffset(0), '+0s');
    assert.equal(formatOffset(15000), '+15s');
    assert.equal(formatOffset(90000), '+1m 30s');
  });
});

describe('Sequential Job Timeline Calculations', () => {
  const baseStart = new Date('2026-10-24T10:00:00Z').getTime();
  const runDurationMs = 60000; // 60s total

  const job1: RunJob = {
    id: 'job-1',
    run_id: 'run-1',
    job_name: 'test',
    status: 'success',
    started_at: '2026-10-24T10:00:00Z',
    finished_at: '2026-10-24T10:00:20Z', // 20s
  };

  const job2: RunJob = {
    id: 'job-2',
    run_id: 'run-1',
    job_name: 'build',
    status: 'success',
    started_at: '2026-10-24T10:00:20Z', // starts at +20s
    finished_at: '2026-10-24T10:01:00Z', // finishes at +60s (40s duration)
  };

  it('calculates proportional start offsets and duration percentages for sequential jobs', () => {
    const m1 = calculateJobTimeline(job1, baseStart, runDurationMs);
    assert.equal(m1.isStarted, true);
    assert.equal(m1.isFinished, true);
    assert.equal(m1.jobDurationMs, 20000);
    assert.equal(m1.offsetMs, 0);
    assert.equal(m1.offsetPercent, 0);
    assert.equal(Math.round(m1.durationPercent), 33);
    assert.equal(m1.wallClockSharePercent, 33);
    assert.equal(m1.formattedDuration, '20s');
    assert.equal(m1.formattedOffset, '+0s');

    const m2 = calculateJobTimeline(job2, baseStart, runDurationMs);
    assert.equal(m2.isStarted, true);
    assert.equal(m2.isFinished, true);
    assert.equal(m2.jobDurationMs, 40000);
    assert.equal(m2.offsetMs, 20000);
    assert.equal(Math.round(m2.offsetPercent), 33);
    assert.equal(Math.round(m2.durationPercent), 67);
    assert.equal(m2.wallClockSharePercent, 67);
    assert.equal(m2.formattedDuration, '40s');
    assert.equal(m2.formattedOffset, '+20s');
  });

  it('calculates run summary for sequential execution', () => {
    const summary = calculateRunTimelineSummary(
      {
        started_at: '2026-10-24T10:00:00Z',
        finished_at: '2026-10-24T10:01:00Z',
        status: 'success',
      },
      [job1, job2]
    );

    assert.equal(summary.totalRunDurationMs, 60000);
    assert.equal(summary.totalComputeMs, 60000); // 20s + 40s
    assert.equal(summary.formattedRunDuration, '1m');
    assert.equal(summary.formattedTotalCompute, '1m');
    assert.equal(summary.maxParallelJobs, 1); // purely sequential
    assert.equal(summary.jobMetrics.length, 2);
  });
});

describe('Parallel and Concurrent Job Calculations', () => {
  const jobA: RunJob = {
    id: 'job-a',
    run_id: 'run-parallel',
    job_name: 'test-backend',
    status: 'success',
    started_at: '2026-10-24T11:00:00Z',
    finished_at: '2026-10-24T11:00:30Z', // 30s
  };

  const jobB: RunJob = {
    id: 'job-b',
    run_id: 'run-parallel',
    job_name: 'test-frontend',
    status: 'success',
    started_at: '2026-10-24T11:00:00Z',
    finished_at: '2026-10-24T11:00:30Z', // 30s concurrent
  };

  it('distinguishes individual job duration from total wall-clock duration without false summation', () => {
    const summary = calculateRunTimelineSummary(
      {
        started_at: '2026-10-24T11:00:00Z',
        finished_at: '2026-10-24T11:00:30Z',
        status: 'success',
      },
      [jobA, jobB]
    );

    // Run wall-clock duration is 30s
    assert.equal(summary.totalRunDurationMs, 30000);
    assert.equal(summary.formattedRunDuration, '30s');

    // Total compute time is 60s (30s + 30s)
    assert.equal(summary.totalComputeMs, 60000);
    assert.equal(summary.formattedTotalCompute, '1m');

    // Concurrency peak was 2 parallel jobs
    assert.equal(summary.maxParallelJobs, 2);

    // Both jobs ran 100% of the wall-clock window starting at 0%
    assert.equal(summary.jobMetrics[0].offsetPercent, 0);
    assert.equal(summary.jobMetrics[0].durationPercent, 100);
    assert.equal(summary.jobMetrics[0].wallClockSharePercent, 100);

    assert.equal(summary.jobMetrics[1].offsetPercent, 0);
    assert.equal(summary.jobMetrics[1].durationPercent, 100);
    assert.equal(summary.jobMetrics[1].wallClockSharePercent, 100);
  });
});

describe('Edge Cases and Fallback Safety', () => {
  it('handles pending / unstarted runs safely without division by zero', () => {
    const pendingJob: RunJob = {
      id: 'job-p',
      run_id: 'run-pending',
      job_name: 'init',
      status: 'pending',
      started_at: '',
      finished_at: '',
    };

    const summary = calculateRunTimelineSummary(
      {
        started_at: '0001-01-01T00:00:00Z',
        finished_at: '0001-01-01T00:00:00Z',
        status: 'pending',
      },
      [pendingJob]
    );

    assert.equal(summary.hasValidTimeWindow, false);
    assert.equal(summary.totalRunDurationMs, 0);
    assert.equal(summary.totalComputeMs, 0);
    assert.equal(summary.jobMetrics[0].isStarted, false);
    assert.equal(summary.jobMetrics[0].offsetPercent, 0);
    assert.equal(summary.jobMetrics[0].durationPercent, 0);
    assert.equal(summary.jobMetrics[0].formattedDuration, 'Pending');
    assert.equal(summary.jobMetrics[0].formattedOffset, '—');
  });

  it('handles instantaneous / zero-duration jobs cleanly', () => {
    const instantJob: RunJob = {
      id: 'job-instant',
      run_id: 'run-1',
      job_name: 'quick-check',
      status: 'success',
      started_at: '2026-10-24T12:00:10Z',
      finished_at: '2026-10-24T12:00:10Z', // 0s
    };

    const runStart = new Date('2026-10-24T12:00:00Z').getTime();
    const m = calculateJobTimeline(instantJob, runStart, 20000);

    assert.equal(m.isStarted, true);
    assert.equal(m.jobDurationMs, 0);
    assert.equal(m.offsetMs, 10000);
    assert.equal(m.offsetPercent, 50);
    assert.equal(m.durationPercent, 0);
    assert.equal(m.formattedDuration, '0s');
    assert.equal(m.formattedOffset, '+10s');
  });

  it('calculates live running jobs dynamically using provided nowMs', () => {
    const startMs = new Date('2026-10-24T13:00:00Z').getTime();
    const nowMs = startMs + 45000; // 45 seconds live

    const liveJob: RunJob = {
      id: 'job-live',
      run_id: 'run-live',
      job_name: 'compile',
      status: 'running',
      started_at: '2026-10-24T13:00:15Z', // started +15s into run
      finished_at: '',
    };

    const m = calculateJobTimeline(liveJob, startMs, 45000, nowMs);

    assert.equal(m.isLive, true);
    assert.equal(m.isStarted, true);
    assert.equal(m.isFinished, false);
    assert.equal(m.jobDurationMs, 30000); // 45s - 15s = 30s live
    assert.equal(m.offsetMs, 15000);
    assert.equal(Math.round(m.offsetPercent), 33);
    assert.equal(Math.round(m.durationPercent), 67);
    assert.equal(m.formattedDuration, '30s');
    assert.equal(m.formattedOffset, '+15s');
  });

  it('clamps track duration so offsetPercent + durationPercent does not exceed 100%', () => {
    const skewJob: RunJob = {
      id: 'job-skew',
      run_id: 'run-skew',
      job_name: 'long-job',
      status: 'success',
      started_at: '2026-10-24T14:00:10Z', // +10s
      finished_at: '2026-10-24T14:00:30Z', // +30s (duration 20s in a 20s run)
    };

    const runStart = new Date('2026-10-24T14:00:00Z').getTime();
    const m = calculateJobTimeline(skewJob, runStart, 20000);

    // offset is 10s / 20s = 50%
    assert.equal(m.offsetPercent, 50);
    // durationPercent must clamp to max 50% so 50% + 50% = 100%
    assert.equal(m.durationPercent, 50);
    assert.equal(m.offsetPercent + m.durationPercent <= 100, true);
  });

  it('expands effective run window when job timestamps extend beyond nominal run start or finish', () => {
    // Nominal run window: 15:00:10 to 15:00:40 (30s)
    // Job 1 starts early at 15:00:00 (-10s before nominal run start)
    // Job 2 finishes late at 15:01:00 (+20s after nominal run finish)
    const earlyJob: RunJob = {
      id: 'job-early',
      run_id: 'run-skew',
      job_name: 'prepare',
      status: 'success',
      started_at: '2026-10-24T15:00:00Z',
      finished_at: '2026-10-24T15:00:20Z',
    };

    const lateJob: RunJob = {
      id: 'job-late',
      run_id: 'run-skew',
      job_name: 'teardown',
      status: 'success',
      started_at: '2026-10-24T15:00:30Z',
      finished_at: '2026-10-24T15:01:00Z',
    };

    const summary = calculateRunTimelineSummary(
      {
        started_at: '2026-10-24T15:00:10Z',
        finished_at: '2026-10-24T15:00:40Z',
        status: 'success',
      },
      [earlyJob, lateJob]
    );

    // Effective start must expand backwards to 15:00:00
    const expectedStartMs = new Date('2026-10-24T15:00:00Z').getTime();
    assert.equal(summary.runStartMs, expectedStartMs);

    // Effective end must expand forwards to 15:01:00
    const expectedEndMs = new Date('2026-10-24T15:01:00Z').getTime();
    assert.equal(summary.runEndMs, expectedEndMs);

    // Total wall-clock duration must reflect the full 60s expanded window (15:00:00 to 15:01:00)
    assert.equal(summary.totalRunDurationMs, 60000);
    assert.equal(summary.formattedRunDuration, '1m');

    // Early job offset from expanded origin (15:00:00) is 0s (0%)
    assert.equal(summary.jobMetrics[0].offsetMs, 0);
    assert.equal(summary.jobMetrics[0].offsetPercent, 0);
    assert.equal(Math.round(summary.jobMetrics[0].durationPercent), 33);

    // Late job offset from expanded origin (15:00:00 to 15:00:30) is 30s (50%)
    assert.equal(summary.jobMetrics[1].offsetMs, 30000);
    assert.equal(summary.jobMetrics[1].offsetPercent, 50);
    assert.equal(Math.round(summary.jobMetrics[1].durationPercent), 50);
  });
});
