import type { Run, RunStatus } from './types.ts';

export type RunSortOption =
  | 'newest'
  | 'oldest'
  | 'duration-desc'
  | 'duration-asc'
  | 'failed-first';

export interface RunsFilterState {
  search: string;
  status: RunStatus | 'all';
  branch: string; // 'all' or specific branch name
  pipeline: string; // specific pipeline ID or empty string
  sort: RunSortOption;
}

export interface RunsPageMetrics {
  total: number;
  completedCount: number;
  succeeded: number;
  failed: number;
  active: number;
  passRate: number | null; // percentage 0-100 or null if no completed runs
  avgDurationFormatted: string;
}

export interface SearchParamsGetter {
  get(name: string): string | null;
}

/**
 * Returns the effective start timestamp in milliseconds, or 0 if missing or invalid.
 */
export function getRunStartTimeMs(run: Run): number {
  const ts = run.started_at || run.created_at;
  if (!ts || ts === '0001-01-01T00:00:00Z') return 0;
  const time = new Date(ts).getTime();
  return isNaN(time) ? 0 : time;
}

/**
 * Returns the completed wall-clock duration in milliseconds for finished runs with valid timestamps.
 * Returns null for active, pending, unstarted, or invalid runs.
 */
export function getRunCompletedDurationMs(run: Run): number | null {
  if (run.status === 'running' || run.status === 'pending') {
    return null;
  }
  if (!run.started_at || run.started_at === '0001-01-01T00:00:00Z') return null;
  if (!run.finished_at || run.finished_at === '0001-01-01T00:00:00Z') return null;
  const start = new Date(run.started_at).getTime();
  const end = new Date(run.finished_at).getTime();
  if (isNaN(start) || isNaN(end) || end < start) return null;
  return end - start;
}

/**
 * Case-insensitive search across pipeline ID, run ID, branch, commit SHA, commit message, and author.
 */
export function matchRunSearch(run: Run, query: string): boolean {
  if (!query || !query.trim()) return true;
  const q = query.trim().toLowerCase();

  if (run.id && run.id.toLowerCase().includes(q)) return true;
  if (run.pipeline_id && run.pipeline_id.toLowerCase().includes(q)) return true;
  if (run.pipeline_name && run.pipeline_name.toLowerCase().includes(q)) return true;
  if (run.branch && run.branch.toLowerCase().includes(q)) return true;
  if (run.commit_sha && run.commit_sha.toLowerCase().includes(q)) return true;
  if (run.commit_msg && run.commit_msg.toLowerCase().includes(q)) return true;
  if (run.triggered_by && run.triggered_by.toLowerCase().includes(q)) return true;
  if (run.author && run.author.toLowerCase().includes(q)) return true;

  return false;
}

/**
 * Extracts unique non-empty branch names in alphabetical order.
 */
export function extractUniqueBranches(runs: Run[]): string[] {
  if (!runs || runs.length === 0) return [];
  const set = new Set<string>();
  for (const r of runs) {
    if (r.branch && r.branch.trim() !== '') {
      set.add(r.branch.trim());
    }
  }
  return Array.from(set).sort((a, b) => a.localeCompare(b));
}

/**
 * Deterministically sorts runs based on the selected sort option.
 */
export function sortRuns(runs: Run[], sortOption: RunSortOption): Run[] {
  if (!runs || runs.length <= 1) return [...(runs || [])];
  const list = [...runs];

  switch (sortOption) {
    case 'newest':
      return list.sort((a, b) => {
        const aTime = getRunStartTimeMs(a);
        const bTime = getRunStartTimeMs(b);
        if (aTime !== bTime) return bTime - aTime;
        return b.id.localeCompare(a.id);
      });

    case 'oldest':
      return list.sort((a, b) => {
        const aTime = getRunStartTimeMs(a);
        const bTime = getRunStartTimeMs(b);
        if (aTime > 0 && bTime > 0) {
          if (aTime !== bTime) return aTime - bTime;
          return a.id.localeCompare(b.id);
        }
        if (aTime > 0 && bTime === 0) return -1;
        if (aTime === 0 && bTime > 0) return 1;
        return a.id.localeCompare(b.id);
      });

    case 'duration-desc':
      return list.sort((a, b) => {
        const aDur = getRunCompletedDurationMs(a);
        const bDur = getRunCompletedDurationMs(b);
        if (aDur !== null && bDur !== null) {
          if (aDur !== bDur) return bDur - aDur;
          return b.id.localeCompare(a.id);
        }
        if (aDur !== null && bDur === null) return -1;
        if (aDur === null && bDur !== null) return 1;
        const aTime = getRunStartTimeMs(a);
        const bTime = getRunStartTimeMs(b);
        if (aTime !== bTime) return bTime - aTime;
        return b.id.localeCompare(a.id);
      });

    case 'duration-asc':
      return list.sort((a, b) => {
        const aDur = getRunCompletedDurationMs(a);
        const bDur = getRunCompletedDurationMs(b);
        if (aDur !== null && bDur !== null) {
          if (aDur !== bDur) return aDur - bDur;
          return a.id.localeCompare(b.id);
        }
        if (aDur !== null && bDur === null) return -1;
        if (aDur === null && bDur !== null) return 1;
        const aTime = getRunStartTimeMs(a);
        const bTime = getRunStartTimeMs(b);
        if (aTime !== bTime) return bTime - aTime;
        return a.id.localeCompare(b.id);
      });

    case 'failed-first':
      return list.sort((a, b) => {
        const aFailed = a.status === 'failed' ? 1 : 0;
        const bFailed = b.status === 'failed' ? 1 : 0;
        if (aFailed !== bFailed) {
          return bFailed - aFailed;
        }
        const aTime = getRunStartTimeMs(a);
        const bTime = getRunStartTimeMs(b);
        if (aTime !== bTime) return bTime - aTime;
        return b.id.localeCompare(a.id);
      });

    default:
      return list;
  }
}

/**
 * Filters and deterministically sorts a collection of runs.
 */
export function filterAndSortRuns(runs: Run[], state: RunsFilterState): Run[] {
  if (!runs || runs.length === 0) return [];

  const filtered = runs.filter((run) => {
    // Pipeline filter
    if (state.pipeline && state.pipeline.trim() && state.pipeline.trim().toLowerCase() !== 'all') {
      const p = state.pipeline.trim().toLowerCase();
      const matchPipe =
        (run.pipeline_id && run.pipeline_id.toLowerCase() === p) ||
        (run.pipeline_name && run.pipeline_name.toLowerCase() === p);
      if (!matchPipe) return false;
    }

    // Status filter
    if (state.status !== 'all' && run.status !== state.status) {
      return false;
    }

    // Branch filter
    if (state.branch !== 'all') {
      const runBranch = (run.branch || '').trim().toLowerCase();
      if (runBranch !== state.branch.trim().toLowerCase()) {
        return false;
      }
    }

    // Search query
    if (state.search && state.search.trim()) {
      if (!matchRunSearch(run, state.search)) {
        return false;
      }
    }

    return true;
  });

  return sortRuns(filtered, state.sort);
}

/**
 * Parses URL query parameters into a validated RunsFilterState.
 */
export function parseRunsUrlParams(params: SearchParamsGetter): RunsFilterState {
  const q = params.get('q') || '';

  const rawStatus = params.get('status') || 'all';
  const validStatuses: Array<RunStatus | 'all'> = [
    'all',
    'running',
    'success',
    'failed',
    'pending',
    'cancelled',
  ];
  const status: RunStatus | 'all' = (validStatuses as readonly string[]).includes(rawStatus)
    ? (rawStatus as RunStatus | 'all')
    : 'all';

  const branch = params.get('branch') || 'all';
  const rawPipeline = params.get('pipeline') || '';
  const pipeline = rawPipeline.trim().toLowerCase() === 'all' ? '' : rawPipeline.trim();

  const rawSort = params.get('sort') || 'newest';
  const validSorts: RunSortOption[] = [
    'newest',
    'oldest',
    'duration-desc',
    'duration-asc',
    'failed-first',
  ];
  const sort: RunSortOption = (validSorts as readonly string[]).includes(rawSort)
    ? (rawSort as RunSortOption)
    : 'newest';

  return {
    search: q,
    status,
    branch,
    pipeline,
    sort,
  };
}

/**
 * Serializes a RunsFilterState into a URLSearchParams instance, preserving unrelated existing parameters.
 */
export function serializeRunsUrlParams(
  state: RunsFilterState,
  currentParams?: URLSearchParams
): URLSearchParams {
  const next = new URLSearchParams(currentParams ? currentParams.toString() : '');

  if (state.search && state.search.trim()) {
    next.set('q', state.search.trim());
  } else {
    next.delete('q');
  }

  if (state.status && state.status !== 'all') {
    next.set('status', state.status);
  } else {
    next.delete('status');
  }

  if (state.branch && state.branch !== 'all') {
    next.set('branch', state.branch.trim());
  } else {
    next.delete('branch');
  }

  if (state.pipeline && state.pipeline.trim() && state.pipeline.trim().toLowerCase() !== 'all') {
    next.set('pipeline', state.pipeline.trim());
  } else {
    next.delete('pipeline');
  }

  if (state.sort && state.sort !== 'newest') {
    next.set('sort', state.sort);
  } else {
    next.delete('sort');
  }

  return next;
}

/**
 * Formats a duration in seconds cleanly.
 */
function formatSeconds(sec: number): string {
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return s > 0 ? `${m}m ${s}s` : `${m}m`;
}

/**
 * Computes metrics for the Runs page with honest completed-run semantics.
 * Running and pending runs are tracked in active and excluded from the pass-rate denominator.
 */
export function calculateRunsPageMetrics(runs: Run[]): RunsPageMetrics {
  if (!runs || runs.length === 0) {
    return {
      total: 0,
      completedCount: 0,
      succeeded: 0,
      failed: 0,
      active: 0,
      passRate: null,
      avgDurationFormatted: '—',
    };
  }

  const total = runs.length;
  const active = runs.filter((r) => r.status === 'running' || r.status === 'pending').length;

  // Completed runs: terminal statuses only
  const completedRuns = runs.filter(
    (r) => r.status === 'success' || r.status === 'failed' || r.status === 'cancelled'
  );
  const completedCount = completedRuns.length;
  const succeeded = completedRuns.filter((r) => r.status === 'success').length;
  const failed = completedRuns.filter((r) => r.status === 'failed').length;

  const passRate = completedCount > 0 ? Math.round((succeeded / completedCount) * 100) : null;

  // Average duration: only finished runs with valid timestamps
  const timedRuns = completedRuns.filter((r) => {
    if (!r.started_at || r.started_at === '0001-01-01T00:00:00Z') return false;
    if (!r.finished_at || r.finished_at === '0001-01-01T00:00:00Z') return false;
    const start = new Date(r.started_at).getTime();
    const end = new Date(r.finished_at).getTime();
    return !isNaN(start) && !isNaN(end) && end >= start;
  });

  let avgDurationFormatted = '—';
  if (timedRuns.length > 0) {
    const totalMs = timedRuns.reduce((sum, r) => {
      return sum + (new Date(r.finished_at).getTime() - new Date(r.started_at).getTime());
    }, 0);
    const avgSec = Math.max(0, Math.round(totalMs / timedRuns.length / 1000));
    avgDurationFormatted = formatSeconds(avgSec);
  }

  return {
    total,
    completedCount,
    succeeded,
    failed,
    active,
    passRate,
    avgDurationFormatted,
  };
}
