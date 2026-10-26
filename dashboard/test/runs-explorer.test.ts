import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import type { Run } from '../lib/types.ts';
import type { RunsFilterState } from '../lib/runs-filter.ts';
import {
  matchRunSearch,
  extractUniqueBranches,
  getRunCompletedDurationMs,
  getRunStartTimeMs,
  sortRuns,
  filterAndSortRuns,
  parseRunsUrlParams,
  serializeRunsUrlParams,
  calculateRunsPageMetrics,
} from '../lib/runs-filter.ts';

describe('Runs Explorer: matchRunSearch', () => {
  const sampleRun: Run = {
    id: 'run-abc-12345678',
    pipeline_id: 'payment-service',
    pipeline_name: 'Payment Service CI',
    status: 'success',
    branch: 'feat/checkout-flow',
    commit_sha: '7f9a2b1c8e3d',
    commit_msg: 'refactor: simplify Stripe webhook processing',
    triggered_by: 'alice@example.com',
    started_at: '2026-10-25T10:00:00Z',
    finished_at: '2026-10-25T10:02:30Z',
  };

  it('matches case-insensitively by run ID or prefix', () => {
    assert.equal(matchRunSearch(sampleRun, 'ABC-123'), true);
    assert.equal(matchRunSearch(sampleRun, '12345678'), true);
    assert.equal(matchRunSearch(sampleRun, 'xyz'), false);
  });

  it('matches case-insensitively by pipeline ID or name', () => {
    assert.equal(matchRunSearch(sampleRun, 'PAYMENT'), true);
    assert.equal(matchRunSearch(sampleRun, 'service ci'), true);
    assert.equal(matchRunSearch(sampleRun, 'auth-service'), false);
  });

  it('matches case-insensitively by branch name', () => {
    assert.equal(matchRunSearch(sampleRun, 'CHECKOUT'), true);
    assert.equal(matchRunSearch(sampleRun, 'feat/checkout'), true);
    assert.equal(matchRunSearch(sampleRun, 'main'), false);
  });

  it('matches by commit SHA or prefix', () => {
    assert.equal(matchRunSearch(sampleRun, '7f9a2b1'), true);
    assert.equal(matchRunSearch(sampleRun, '7F9A2B1'), true);
    assert.equal(matchRunSearch(sampleRun, 'deadbeef'), false);
  });

  it('matches by commit message contents', () => {
    assert.equal(matchRunSearch(sampleRun, 'stripe webhook'), true);
    assert.equal(matchRunSearch(sampleRun, 'STRIPE'), true);
    assert.equal(matchRunSearch(sampleRun, 'unrelated commit'), false);
  });

  it('matches by author or triggered_by', () => {
    assert.equal(matchRunSearch(sampleRun, 'alice'), true);
    assert.equal(matchRunSearch(sampleRun, 'ALICE@EXAMPLE'), true);
    assert.equal(matchRunSearch(sampleRun, 'bob'), false);
  });

  it('handles runs with missing or undefined Git metadata without throwing', () => {
    const minimalRun: Run = {
      id: 'run-min-001',
      pipeline_id: 'minimal-pipe',
      status: 'pending',
      started_at: '',
      finished_at: '',
    };
    assert.equal(matchRunSearch(minimalRun, 'min'), true);
    assert.equal(matchRunSearch(minimalRun, 'main'), false);
    assert.equal(matchRunSearch(minimalRun, ''), true);
  });

  it('returns true for empty or whitespace query', () => {
    assert.equal(matchRunSearch(sampleRun, ''), true);
    assert.equal(matchRunSearch(sampleRun, '   '), true);
  });

  it('safely handles special regex characters in search query', () => {
    assert.equal(matchRunSearch(sampleRun, '[payment]*'), false);
    assert.equal(matchRunSearch(sampleRun, 'feat/'), true);
  });
});

describe('Runs Explorer: extractUniqueBranches', () => {
  it('extracts, trims, deduplicates, and alphabetizes branches', () => {
    const runs: Run[] = [
      { id: '1', pipeline_id: 'p', status: 'success', branch: 'main', started_at: '', finished_at: '' },
      { id: '2', pipeline_id: 'p', status: 'success', branch: 'staging', started_at: '', finished_at: '' },
      { id: '3', pipeline_id: 'p', status: 'success', branch: 'main', started_at: '', finished_at: '' },
      { id: '4', pipeline_id: 'p', status: 'success', branch: 'feat/login', started_at: '', finished_at: '' },
      { id: '5', pipeline_id: 'p', status: 'success', branch: '  ', started_at: '', finished_at: '' },
      { id: '6', pipeline_id: 'p', status: 'success', started_at: '', finished_at: '' },
    ];

    const branches = extractUniqueBranches(runs);
    assert.deepEqual(branches, ['feat/login', 'main', 'staging']);
  });

  it('returns empty array for empty runs list or runs without branches', () => {
    assert.deepEqual(extractUniqueBranches([]), []);
    assert.deepEqual(
      extractUniqueBranches([
        { id: '1', pipeline_id: 'p', status: 'success', started_at: '', finished_at: '' },
      ]),
      []
    );
  });
});

describe('Runs Explorer: Duration and Timestamp Calculations', () => {
  it('calculates duration for completed runs with valid timestamps', () => {
    const run: Run = {
      id: '1',
      pipeline_id: 'p',
      status: 'success',
      started_at: '2026-10-25T10:00:00Z',
      finished_at: '2026-10-25T10:01:30Z',
    };
    assert.equal(getRunCompletedDurationMs(run), 90000);
  });

  it('returns null duration for active running and pending runs (does not fabricate completed duration)', () => {
    const runningRun: Run = {
      id: '2',
      pipeline_id: 'p',
      status: 'running',
      started_at: '2026-10-25T10:00:00Z',
      finished_at: '',
    };
    const pendingRun: Run = {
      id: '3',
      pipeline_id: 'p',
      status: 'pending',
      started_at: '',
      finished_at: '',
    };
    assert.equal(getRunCompletedDurationMs(runningRun), null);
    assert.equal(getRunCompletedDurationMs(pendingRun), null);
  });

  it('returns null duration for missing, zero, or inverted timestamps', () => {
    const zeroRun: Run = {
      id: '4',
      pipeline_id: 'p',
      status: 'failed',
      started_at: '0001-01-01T00:00:00Z',
      finished_at: '0001-01-01T00:00:00Z',
    };
    const invertedRun: Run = {
      id: '5',
      pipeline_id: 'p',
      status: 'failed',
      started_at: '2026-10-25T10:05:00Z',
      finished_at: '2026-10-25T10:00:00Z',
    };
    assert.equal(getRunCompletedDurationMs(zeroRun), null);
    assert.equal(getRunCompletedDurationMs(invertedRun), null);
  });

  it('returns start timestamp safely with getRunStartTimeMs', () => {
    const valid: Run = {
      id: '6',
      pipeline_id: 'p',
      status: 'success',
      started_at: '2026-10-25T12:00:00Z',
      finished_at: '',
    };
    const zero: Run = {
      id: '7',
      pipeline_id: 'p',
      status: 'success',
      started_at: '0001-01-01T00:00:00Z',
      finished_at: '',
    };
    assert.equal(getRunStartTimeMs(valid), new Date('2026-10-25T12:00:00Z').getTime());
    assert.equal(getRunStartTimeMs(zero), 0);
  });
});

describe('Runs Explorer: Deterministic Sorting', () => {
  const r1: Run = {
    id: 'run-01',
    pipeline_id: 'p',
    status: 'success',
    started_at: '2026-10-25T10:00:00Z',
    finished_at: '2026-10-25T10:01:00Z', // 60s
  };
  const r2: Run = {
    id: 'run-02',
    pipeline_id: 'p',
    status: 'failed',
    started_at: '2026-10-25T11:00:00Z',
    finished_at: '2026-10-25T11:05:00Z', // 300s
  };
  const r3: Run = {
    id: 'run-03',
    pipeline_id: 'p',
    status: 'running',
    started_at: '2026-10-25T12:00:00Z',
    finished_at: '', // running (untimed)
  };
  const r4: Run = {
    id: 'run-04',
    pipeline_id: 'p',
    status: 'success',
    started_at: '2026-10-25T11:00:00Z', // same start as r2
    finished_at: '2026-10-25T11:00:30Z', // 30s
  };

  it('sorts newest first with deterministic tie-breaking by ID', () => {
    const sorted = sortRuns([r1, r2, r3, r4], 'newest');
    // r3 (12:00), then r4/r2 (11:00 - tie break 'run-04' > 'run-02' desc), then r1 (10:00)
    assert.deepEqual(
      sorted.map((r) => r.id),
      ['run-03', 'run-04', 'run-02', 'run-01']
    );
  });

  it('sorts oldest first with valid runs before untimed runs and deterministic tie-breaking', () => {
    const untimed: Run = { id: 'run-00', pipeline_id: 'p', status: 'pending', started_at: '', finished_at: '' };
    const sorted = sortRuns([r2, untimed, r1], 'oldest');
    // r1 (10:00), r2 (11:00), untimed at end
    assert.deepEqual(
      sorted.map((r) => r.id),
      ['run-01', 'run-02', 'run-00']
    );
  });

  it('sorts slowest duration first with untimed runs at the end', () => {
    const sorted = sortRuns([r1, r2, r3, r4], 'duration-desc');
    // Durations: r2 (300s), r1 (60s), r4 (30s), r3 (untimed -> end)
    assert.deepEqual(
      sorted.map((r) => r.id),
      ['run-02', 'run-01', 'run-04', 'run-03']
    );
  });

  it('sorts fastest duration first with untimed runs at the end', () => {
    const sorted = sortRuns([r1, r2, r3, r4], 'duration-asc');
    // Durations: r4 (30s), r1 (60s), r2 (300s), r3 (untimed -> end)
    assert.deepEqual(
      sorted.map((r) => r.id),
      ['run-04', 'run-01', 'run-02', 'run-03']
    );
  });

  it('sorts failed first, ordering failed runs newest-first and non-failed runs newest-first', () => {
    const failedNewer: Run = {
      id: 'run-failed-newer',
      pipeline_id: 'p',
      status: 'failed',
      started_at: '2026-10-25T13:00:00Z',
      finished_at: '2026-10-25T13:01:00Z',
    };
    const sorted = sortRuns([r1, r2, r3, failedNewer], 'failed-first');
    // Failed: failedNewer (13:00), r2 (11:00)
    // Non-failed: r3 (12:00), r1 (10:00)
    assert.deepEqual(
      sorted.map((r) => r.id),
      ['run-failed-newer', 'run-02', 'run-03', 'run-01']
    );
  });
});

describe('Runs Explorer: Composition (Filter & Sort)', () => {
  const dataset: Run[] = [
    {
      id: 'run-101',
      pipeline_id: 'api-server',
      status: 'success',
      branch: 'main',
      commit_sha: 'a1b2c3d',
      commit_msg: 'fix: handle nil context in auth handler',
      started_at: '2026-10-25T10:00:00Z',
      finished_at: '2026-10-25T10:01:00Z',
    },
    {
      id: 'run-102',
      pipeline_id: 'api-server',
      status: 'failed',
      branch: 'feat/payments',
      commit_sha: 'b2c3d4e',
      commit_msg: 'feat: add stripe checkout webhook',
      started_at: '2026-10-25T11:00:00Z',
      finished_at: '2026-10-25T11:02:00Z',
    },
    {
      id: 'run-103',
      pipeline_id: 'web-client',
      status: 'success',
      branch: 'main',
      commit_sha: 'c3d4e5f',
      commit_msg: 'chore: update Tailwind typography',
      started_at: '2026-10-25T12:00:00Z',
      finished_at: '2026-10-25T12:00:45Z',
    },
    {
      id: 'run-104',
      pipeline_id: 'api-server',
      status: 'running',
      branch: 'main',
      commit_sha: 'd4e5f6a',
      commit_msg: 'test: add integration test suite',
      started_at: '2026-10-25T13:00:00Z',
      finished_at: '',
    },
  ];

  it('filters by branch and pipeline simultaneously', () => {
    const state: RunsFilterState = {
      search: '',
      status: 'all',
      branch: 'main',
      pipeline: 'api-server',
      sort: 'newest',
    };
    const res = filterAndSortRuns(dataset, state);
    assert.deepEqual(
      res.map((r) => r.id),
      ['run-104', 'run-101']
    );
  });

  it('filters by status and commit search query', () => {
    const state: RunsFilterState = {
      search: 'stripe',
      status: 'failed',
      branch: 'all',
      pipeline: '',
      sort: 'newest',
    };
    const res = filterAndSortRuns(dataset, state);
    assert.deepEqual(
      res.map((r) => r.id),
      ['run-102']
    );
  });

  it('returns empty array when search does not match any run', () => {
    const state: RunsFilterState = {
      search: 'nonexistent-query-string',
      status: 'all',
      branch: 'all',
      pipeline: '',
      sort: 'newest',
    };
    const res = filterAndSortRuns(dataset, state);
    assert.equal(res.length, 0);
  });

  it('treats pipeline "all" and empty string equivalently as matching all pipelines', () => {
    const emptyPipeline = filterAndSortRuns(dataset, {
      search: '',
      status: 'all',
      branch: 'all',
      pipeline: '',
      sort: 'newest',
    });
    const allPipeline = filterAndSortRuns(dataset, {
      search: '',
      status: 'all',
      branch: 'all',
      pipeline: 'all',
      sort: 'newest',
    });
    assert.equal(emptyPipeline.length, dataset.length);
    assert.equal(allPipeline.length, dataset.length);
  });
});

describe('Runs Explorer: URL Parameters Serialization and Parsing', () => {
  it('parses empty or missing parameters into safe defaults', () => {
    const emptyParams = new URLSearchParams('');
    const parsed = parseRunsUrlParams(emptyParams);
    assert.deepEqual(parsed, {
      search: '',
      status: 'all',
      branch: 'all',
      pipeline: '',
      sort: 'newest',
    });
  });

  it('parses valid search query, status, branch, pipeline, and sort', () => {
    const params = new URLSearchParams('q=auth&status=failed&branch=main&pipeline=api-server&sort=duration-desc');
    const parsed = parseRunsUrlParams(params);
    assert.deepEqual(parsed, {
      search: 'auth',
      status: 'failed',
      branch: 'main',
      pipeline: 'api-server',
      sort: 'duration-desc',
    });
  });

  it('sanitizes unknown status and sort values back to defaults', () => {
    const badParams = new URLSearchParams('status=exploding&sort=random-order');
    const parsed = parseRunsUrlParams(badParams);
    assert.equal(parsed.status, 'all');
    assert.equal(parsed.sort, 'newest');
  });

  it('serializes state into URL parameters, omitting default values', () => {
    const defaultState: RunsFilterState = {
      search: '',
      status: 'all',
      branch: 'all',
      pipeline: '',
      sort: 'newest',
    };
    const serialized = serializeRunsUrlParams(defaultState);
    assert.equal(serialized.toString(), '');
  });

  it('serializes custom state into URL parameters while preserving unrelated parameters', () => {
    const customState: RunsFilterState = {
      search: 'checkout',
      status: 'running',
      branch: 'feat/pay',
      pipeline: 'payment-svc',
      sort: 'failed-first',
    };
    const existing = new URLSearchParams('theme=dark&sidebar=open');
    const serialized = serializeRunsUrlParams(customState, existing);

    assert.equal(serialized.get('theme'), 'dark');
    assert.equal(serialized.get('sidebar'), 'open');
    assert.equal(serialized.get('q'), 'checkout');
    assert.equal(serialized.get('status'), 'running');
    assert.equal(serialized.get('branch'), 'feat/pay');
    assert.equal(serialized.get('pipeline'), 'payment-svc');
    assert.equal(serialized.get('sort'), 'failed-first');
  });

  it('normalizes pipeline="all" in query params to empty string and omits from serialization', () => {
    const parsed = parseRunsUrlParams(new URLSearchParams('pipeline=all'));
    assert.equal(parsed.pipeline, '');

    const serialized = serializeRunsUrlParams({
      search: '',
      status: 'all',
      branch: 'all',
      pipeline: 'all',
      sort: 'newest',
    });
    assert.equal(serialized.get('pipeline'), null);
  });

  it('performs lossless URL round-tripping with special characters, branch slashes, and complex queries', () => {
    const originalState: RunsFilterState = {
      search: 'fix: #1234 (login/error) & token',
      status: 'failed',
      branch: 'feature/auth-v2/oauth-flow',
      pipeline: 'api.server-auth',
      sort: 'duration-desc',
    };

    const serialized = serializeRunsUrlParams(originalState);
    const parsed = parseRunsUrlParams(serialized);

    assert.deepEqual(parsed, originalState);
  });
});

describe('Runs Explorer: 4-Way Filter Composition & Deterministic Edge Cases', () => {
  const comprehensiveDataset: Run[] = [
    {
      id: 'run-alpha-1',
      pipeline_id: 'api-server',
      status: 'failed',
      branch: 'feature/auth-v2',
      commit_msg: 'fix: #1234 login error',
      started_at: '2026-10-25T10:00:00Z',
      finished_at: '2026-10-25T10:05:00Z', // 300s
    },
    {
      id: 'run-alpha-2',
      pipeline_id: 'api-server',
      status: 'failed',
      branch: 'feature/auth-v2',
      commit_msg: 'fix: #1234 login error retry',
      started_at: '2026-10-25T10:10:00Z',
      finished_at: '2026-10-25T10:12:00Z', // 120s
    },
    {
      id: 'run-beta-1',
      pipeline_id: 'api-server',
      status: 'success', // wrong status
      branch: 'feature/auth-v2',
      commit_msg: 'fix: #1234 login error',
      started_at: '2026-10-25T10:00:00Z',
      finished_at: '2026-10-25T10:01:00Z',
    },
    {
      id: 'run-gamma-1',
      pipeline_id: 'api-server',
      status: 'failed',
      branch: 'main', // wrong branch
      commit_msg: 'fix: #1234 login error',
      started_at: '2026-10-25T10:00:00Z',
      finished_at: '2026-10-25T10:01:00Z',
    },
    {
      id: 'run-delta-1',
      pipeline_id: 'web-frontend', // wrong pipeline
      status: 'failed',
      branch: 'feature/auth-v2',
      commit_msg: 'fix: #1234 login error',
      started_at: '2026-10-25T10:00:00Z',
      finished_at: '2026-10-25T10:01:00Z',
    },
  ];

  it('filters simultaneously by search, status, branch, and pipeline with duration-desc sort', () => {
    const state: RunsFilterState = {
      search: '#1234',
      status: 'failed',
      branch: 'feature/auth-v2',
      pipeline: 'api-server',
      sort: 'duration-desc',
    };

    const result = filterAndSortRuns(comprehensiveDataset, state);
    assert.equal(result.length, 2);
    // alpha-1 (300s) should precede alpha-2 (120s) under duration-desc
    assert.deepEqual(
      result.map((r) => r.id),
      ['run-alpha-1', 'run-alpha-2']
    );
  });

  it('returns empty array cleanly when no runs match all criteria, distinct from error state', () => {
    const state: RunsFilterState = {
      search: 'nonexistent',
      status: 'failed',
      branch: 'feature/auth-v2',
      pipeline: 'api-server',
      sort: 'newest',
    };

    const result = filterAndSortRuns(comprehensiveDataset, state);
    assert.deepEqual(result, []);
  });
});

describe('Runs Explorer: calculateRunsPageMetrics', () => {
  it('handles empty runs array honestly', () => {
    const metrics = calculateRunsPageMetrics([]);
    assert.deepEqual(metrics, {
      total: 0,
      completedCount: 0,
      succeeded: 0,
      failed: 0,
      active: 0,
      passRate: null,
      avgDurationFormatted: '—',
    });
  });

  it('handles active-only runs without division-by-zero or depressing pass-rate', () => {
    const activeRuns: Run[] = [
      { id: '1', pipeline_id: 'p', status: 'running', started_at: '2026-10-25T10:00:00Z', finished_at: '' },
      { id: '2', pipeline_id: 'p', status: 'pending', started_at: '', finished_at: '' },
    ];
    const metrics = calculateRunsPageMetrics(activeRuns);
    assert.equal(metrics.total, 2);
    assert.equal(metrics.active, 2);
    assert.equal(metrics.completedCount, 0);
    assert.equal(metrics.passRate, null);
    assert.equal(metrics.avgDurationFormatted, '—');
  });

  it('calculates pass-rate exclusively over completed runs (success, failed, cancelled)', () => {
    const mixedRuns: Run[] = [
      { id: '1', pipeline_id: 'p', status: 'success', started_at: '2026-10-25T10:00:00Z', finished_at: '2026-10-25T10:01:00Z' },
      { id: '2', pipeline_id: 'p', status: 'failed', started_at: '2026-10-25T10:00:00Z', finished_at: '2026-10-25T10:02:00Z' },
      { id: '3', pipeline_id: 'p', status: 'running', started_at: '2026-10-25T10:00:00Z', finished_at: '' }, // active
      { id: '4', pipeline_id: 'p', status: 'cancelled', started_at: '2026-10-25T10:00:00Z', finished_at: '2026-10-25T10:00:30Z' },
    ];
    // Completed: 3 (1 success, 1 failed, 1 cancelled). Active: 1. Total: 4.
    // Pass rate: 1 / 3 = 33%
    const metrics = calculateRunsPageMetrics(mixedRuns);
    assert.equal(metrics.total, 4);
    assert.equal(metrics.active, 1);
    assert.equal(metrics.completedCount, 3);
    assert.equal(metrics.succeeded, 1);
    assert.equal(metrics.failed, 1);
    assert.equal(metrics.passRate, 33);
    // Timed completed runs: r1 (60s), r2 (120s), r4 (30s) -> avg = 210/3 = 70s = 1m 10s
    assert.equal(metrics.avgDurationFormatted, '1m 10s');
  });
});
