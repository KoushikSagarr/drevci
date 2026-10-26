'use client';

import React, { useEffect, useState, useMemo, useCallback, Suspense } from 'react';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { getRuns, getQueueStatus, QueueStatus } from '../../lib/api';
import { Run, RunStatus } from '../../lib/types';
import StatusBadge from '../../components/StatusBadge';
import LiveDuration from '../../components/LiveDuration';
import {
  RunSortOption,
  RunsFilterState,
  extractUniqueBranches,
  filterAndSortRuns,
  parseRunsUrlParams,
  serializeRunsUrlParams,
  calculateRunsPageMetrics,
} from '../../lib/runs-filter';

function timeAgo(dateStr: string) {
  if (!dateStr || dateStr === '0001-01-01T00:00:00Z') return '—';
  const diff = Math.floor((Date.now() - new Date(dateStr).getTime()) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function StatCard({
  label,
  value,
  sub,
  color,
}: {
  label: string;
  value: string | number;
  sub?: string;
  color?: string;
}) {
  return (
    <div className="flex flex-col rounded-lg border border-border bg-surface p-4 min-w-0">
      <span className="text-xs font-medium text-text-muted uppercase tracking-wider mb-1">
        {label}
      </span>
      <span className={`text-2xl font-bold tabular-nums ${color || 'text-text-primary'}`}>
        {value}
      </span>
      {sub && <span className="text-xs text-text-muted mt-0.5 truncate">{sub}</span>}
    </div>
  );
}

function QueueBar({ qs }: { qs: QueueStatus | null }) {
  if (!qs) return null;
  const dotColor =
    qs.depth > 50
      ? 'bg-status-failed'
      : qs.depth >= 10
      ? 'bg-[#d29922]'
      : 'bg-status-success';
  return (
    <div className="flex items-center gap-4 border-b border-border bg-surface/50 px-6 py-2 text-xs font-mono text-text-muted">
      <span>
        Workers: <span className="text-text-secondary">{qs.workers}</span>
      </span>
      <span className="text-border">│</span>
      <span>
        Queue: <span className="text-text-secondary">{qs.depth}</span>
      </span>
      <span className="text-border">│</span>
      <span className="flex items-center gap-1.5">
        <span className={`inline-block h-1.5 w-1.5 rounded-full ${dotColor}`} />
        <span className="text-text-secondary">{qs.status}</span>
      </span>
    </div>
  );
}

function RunsContent() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [runs, setRuns] = useState<Run[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [queueStatus, setQueueStatus] = useState<QueueStatus | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  // Initial filter state from URL query parameters
  const initialParams = useMemo(() => parseRunsUrlParams(searchParams), [searchParams]);
  const [search, setSearch] = useState(initialParams.search);
  const [status, setStatus] = useState<RunStatus | 'all'>(initialParams.status);
  const [branch, setBranch] = useState(initialParams.branch);
  const [pipeline, setPipeline] = useState(initialParams.pipeline);
  const [sort, setSort] = useState<RunSortOption>(initialParams.sort);

  // Adjust state when searchParams changes (e.g. navigation via in-app links)
  const [prevParamsString, setPrevParamsString] = useState(searchParams.toString());
  const currentParamsString = searchParams.toString();
  if (currentParamsString !== prevParamsString) {
    setPrevParamsString(currentParamsString);
    const parsed = parseRunsUrlParams(searchParams);
    setSearch(parsed.search);
    setStatus(parsed.status);
    setBranch(parsed.branch);
    setPipeline(parsed.pipeline);
    setSort(parsed.sort);
  }

  // Sync state when browser back/forward navigation occurs
  useEffect(() => {
    const handlePopState = () => {
      const parsed = parseRunsUrlParams(new URLSearchParams(window.location.search));
      setSearch(parsed.search);
      setStatus(parsed.status);
      setBranch(parsed.branch);
      setPipeline(parsed.pipeline);
      setSort(parsed.sort);
    };
    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  // Synchronize user controls back to the URL query string
  const updateUrl = useCallback((nextState: RunsFilterState) => {
    if (typeof window === 'undefined') return;
    const currentParams = new URLSearchParams(window.location.search);
    const updatedParams = serializeRunsUrlParams(nextState, currentParams);
    const newQuery = updatedParams.toString();
    const currentQuery = currentParams.toString();
    if (newQuery === currentQuery) return;
    const newUrl = newQuery ? `${window.location.pathname}?${newQuery}` : window.location.pathname;
    window.history.replaceState(null, '', newUrl);
  }, []);

  // Debounce search update to avoid history clutter, but update other controls immediately
  useEffect(() => {
    const timer = setTimeout(() => {
      updateUrl({ search, status, branch, pipeline, sort });
    }, 250);
    return () => clearTimeout(timer);
  }, [search, status, branch, pipeline, sort, updateUrl]);

  // Fetch runs and queue status
  useEffect(() => {
    let isCancelled = false;

    const fetchRuns = async () => {
      try {
        const data = await getRuns(100);
        if (!isCancelled) {
          setRuns(data || []);
          setError(null);
        }
      } catch (err) {
        if (!isCancelled) {
          console.error('Failed to fetch runs:', err);
          setError(err instanceof Error ? err.message : 'Failed to fetch runs');
        }
      } finally {
        if (!isCancelled) {
          setLoading(false);
        }
      }
    };

    const fetchQueue = async () => {
      try {
        const qs = await getQueueStatus();
        if (!isCancelled) {
          setQueueStatus(qs);
        }
      } catch (err) {
        if (!isCancelled) {
          console.error('Failed to fetch queue:', err);
        }
      }
    };

    fetchRuns();
    fetchQueue();
    const r = setInterval(fetchRuns, 5000);
    const q = setInterval(fetchQueue, 10000);
    return () => {
      isCancelled = true;
      clearInterval(r);
      clearInterval(q);
    };
  }, [reloadKey]);

  // Dynamic branch list derived from current runs
  const uniqueBranches = useMemo(() => extractUniqueBranches(runs), [runs]);

  // Filtered and sorted runs
  const filteredRuns = useMemo(() => {
    return filterAndSortRuns(runs, { search, status, branch, pipeline, sort });
  }, [runs, search, status, branch, pipeline, sort]);

  // Operational metrics calculated over full runs collection
  const metrics = useMemo(() => calculateRunsPageMetrics(runs), [runs]);

  const hasActiveFilters =
    search.trim() !== '' ||
    status !== 'all' ||
    branch !== 'all' ||
    pipeline.trim() !== '' ||
    sort !== 'newest';

  const resetFilters = () => {
    setSearch('');
    setStatus('all');
    setBranch('all');
    setPipeline('');
    setSort('newest');
    updateUrl({ search: '', status: 'all', branch: 'all', pipeline: '', sort: 'newest' });
  };

  return (
    <div className="min-h-screen bg-background">
      <QueueBar qs={queueStatus} />

      <div className="mx-auto max-w-6xl px-6 py-8">
        {/* Page Header */}
        <div className="mb-8 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-text-primary tracking-tight">Pipeline Runs</h1>
            <p className="text-sm text-text-muted mt-1">
              Explore, filter, and inspect CI/CD executions across branches and commits
            </p>
          </div>
          <button
            disabled
            className="inline-flex items-center gap-2 rounded-md bg-accent px-4 py-2 text-sm font-semibold text-white opacity-50 cursor-not-allowed"
            title="Manual trigger coming soon"
          >
            <span>+</span> New Run
          </button>
        </div>

        {/* Error Banner when cached runs exist but a refresh failed */}
        {error && runs.length > 0 && (
          <div className="mb-6 flex items-center justify-between rounded-lg border border-status-failed/30 bg-status-failed/10 px-4 py-3 text-sm text-text-primary">
            <div className="flex items-center gap-2">
              <span className="text-status-failed font-bold">⚠</span>
              <span>Failed to refresh runs: {error}</span>
            </div>
            <button
              onClick={() => {
                setLoading(true);
                setReloadKey((k) => k + 1);
              }}
              className="text-xs font-medium text-text-secondary hover:text-text-primary underline ml-4"
            >
              Retry
            </button>
          </div>
        )}

        {/* Metrics Cards with truthful completed-run denominator */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-8">
          <StatCard label="Total Runs" value={metrics.total} />
          <StatCard
            label="Pass Rate"
            value={metrics.passRate !== null ? `${metrics.passRate}%` : '—'}
            color={
              metrics.passRate !== null
                ? metrics.passRate >= 80
                  ? 'text-status-success'
                  : metrics.passRate >= 50
                  ? 'text-[#d29922]'
                  : 'text-status-failed'
                : 'text-text-muted'
            }
            sub={
              metrics.completedCount > 0
                ? `${metrics.completedCount} completed`
                : 'No completed runs'
            }
          />
          <StatCard
            label="Active"
            value={metrics.active}
            color={metrics.active > 0 ? 'text-status-running' : 'text-text-primary'}
            sub={metrics.active > 0 ? 'running / pending' : undefined}
          />
          <StatCard
            label="Failed"
            value={metrics.failed}
            color={metrics.failed > 0 ? 'text-status-failed' : 'text-text-primary'}
          />
          <StatCard
            label="Avg Duration"
            value={metrics.avgDurationFormatted}
            sub="completed runs"
          />
        </div>

        {/* Search & Filter Toolbar */}
        <div className="mb-4 space-y-3">
          <div className="flex flex-col md:flex-row items-stretch md:items-center gap-3">
            {/* Multi-field search */}
            <div className="relative flex-1">
              <span className="absolute left-3 top-1/2 -translate-y-1/2 text-text-muted opacity-50 text-sm">
                ⌕
              </span>
              <input
                type="text"
                aria-label="Search runs by commit, author, branch, or ID"
                placeholder="Search pipeline, run ID, branch, commit, author..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="w-full rounded-md border border-border bg-surface pl-9 pr-8 py-2 text-sm text-text-primary focus:border-accent focus:outline-none transition-all placeholder:text-text-muted/60"
              />
              {search && (
                <button
                  type="button"
                  onClick={() => setSearch('')}
                  aria-label="Clear search"
                  className="absolute right-2.5 top-1/2 -translate-y-1/2 text-xs text-text-muted hover:text-text-primary"
                >
                  ✕
                </button>
              )}
            </div>

            {/* Branch selector */}
            <div className="w-full md:w-48">
              <select
                value={branch}
                onChange={(e) => setBranch(e.target.value)}
                aria-label="Filter by branch"
                className="w-full rounded-md border border-border bg-surface px-3 py-2 text-xs font-mono text-text-primary focus:border-accent focus:outline-none transition-all"
              >
                <option value="all">Branch: All ({uniqueBranches.length})</option>
                {uniqueBranches.map((b) => (
                  <option key={b} value={b}>
                    {b}
                  </option>
                ))}
              </select>
            </div>

            {/* Sort selector */}
            <div className="w-full md:w-48">
              <select
                value={sort}
                onChange={(e) => setSort(e.target.value as RunSortOption)}
                aria-label="Sort runs"
                className="w-full rounded-md border border-border bg-surface px-3 py-2 text-xs font-mono text-text-primary focus:border-accent focus:outline-none transition-all"
              >
                <option value="newest">Sort: Newest First</option>
                <option value="oldest">Sort: Oldest First</option>
                <option value="duration-desc">Sort: Slowest Duration</option>
                <option value="duration-asc">Sort: Fastest Duration</option>
                <option value="failed-first">Sort: Failed First</option>
              </select>
            </div>
          </div>

          {/* Status filter pills & active chips */}
          <div className="flex flex-wrap items-center justify-between gap-3 pt-1">
            <div className="flex items-center gap-1.5 overflow-x-auto pb-1 md:pb-0">
              {(['all', 'running', 'success', 'failed', 'pending'] as const).map((s) => (
                <button
                  key={s}
                  onClick={() => setStatus(s)}
                  className={`whitespace-nowrap rounded-md border px-3 py-1.5 text-xs font-medium transition-all ${
                    status === s
                      ? 'border-accent bg-accent/10 text-accent font-semibold'
                      : 'border-border bg-surface text-text-secondary hover:text-text-primary'
                  }`}
                >
                  {s.charAt(0).toUpperCase() + s.slice(1)}
                </button>
              ))}
            </div>

            <div className="flex items-center gap-3 text-xs text-text-muted">
              {pipeline && (
                <span className="inline-flex items-center gap-1.5 rounded-full border border-accent/40 bg-accent/10 px-2.5 py-0.5 text-accent font-medium">
                  Pipeline: <span className="font-mono">{pipeline}</span>
                  <button
                    onClick={() => setPipeline('')}
                    title="Clear pipeline filter"
                    className="hover:text-white"
                  >
                    ✕
                  </button>
                </span>
              )}

              <span>
                Showing <strong className="text-text-primary">{filteredRuns.length}</strong> of{' '}
                {runs.length} runs
              </span>

              {hasActiveFilters && (
                <button
                  onClick={resetFilters}
                  className="text-xs text-accent hover:underline font-medium"
                >
                  Reset filters
                </button>
              )}
            </div>
          </div>
        </div>

        {/* Runs Table */}
        <div className="overflow-hidden rounded-lg border border-border bg-surface">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-border bg-background/50">
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider">
                  Run
                </th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider">
                  Pipeline & Branch
                </th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider hidden md:table-cell">
                  Commit
                </th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider">
                  Status
                </th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider">
                  Triggered
                </th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider">
                  Duration
                </th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider text-right">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {loading && runs.length === 0 ? (
                [...Array(5)].map((_, i) => (
                  <tr key={i} className="border-b border-border">
                    <td className="px-5 py-4">
                      <div className="skeleton h-4 w-20" />
                    </td>
                    <td className="px-5 py-4">
                      <div className="skeleton h-4 w-32" />
                    </td>
                    <td className="px-5 py-4 hidden md:table-cell">
                      <div className="skeleton h-4 w-24" />
                    </td>
                    <td className="px-5 py-4">
                      <div className="skeleton h-5 w-20 rounded-full" />
                    </td>
                    <td className="px-5 py-4">
                      <div className="skeleton h-4 w-16" />
                    </td>
                    <td className="px-5 py-4">
                      <div className="skeleton h-4 w-14" />
                    </td>
                    <td className="px-5 py-4">
                      <div className="skeleton h-7 w-16 ml-auto" />
                    </td>
                  </tr>
                ))
              ) : error && runs.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-5 py-16 text-center">
                    <div className="flex flex-col items-center gap-3">
                      <div className="text-4xl text-status-failed">⚠</div>
                      <p className="text-text-primary font-medium text-base">
                        Authentication or Network Error
                      </p>
                      <p className="text-text-muted text-xs max-w-md">{error}</p>
                      <button
                        onClick={() => {
                          setLoading(true);
                          setReloadKey((k) => k + 1);
                        }}
                        className="mt-2 inline-flex items-center gap-1.5 rounded-md border border-border bg-surface px-4 py-1.5 text-xs font-semibold text-text-primary hover:bg-surface-hover hover:border-accent/40 transition-colors"
                      >
                        ↻ Retry
                      </button>
                    </div>
                  </td>
                </tr>
              ) : filteredRuns.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-5 py-16 text-center">
                    <div className="flex flex-col items-center gap-3">
                      <div className="text-4xl opacity-30">⚡</div>
                      <p className="text-text-secondary font-medium">No matching runs found</p>
                      <p className="text-text-muted text-xs">
                        Try adjusting your search query, branch filter, or status selection
                      </p>
                      {hasActiveFilters && (
                        <button
                          onClick={resetFilters}
                          className="mt-2 inline-flex items-center gap-1 rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium text-accent hover:border-accent transition-colors"
                        >
                          Clear all filters
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ) : (
                filteredRuns.map((run) => (
                  <tr
                    key={run.id}
                    className={`border-b border-border transition-colors hover:bg-surface-hover cursor-pointer group ${
                      run.status === 'running' ? 'glow-running' : ''
                    }`}
                    onClick={() => router.push(`/runs/${run.id}`)}
                  >
                    {/* Run ID */}
                    <td className="px-5 py-3.5">
                      <span className="font-mono text-xs text-text-secondary group-hover:text-accent font-medium">
                        {run.id.substring(0, 8)}
                      </span>
                    </td>

                    {/* Pipeline & Branch */}
                    <td className="px-5 py-3.5">
                      <div className="flex flex-col gap-1">
                        <div className="flex items-center gap-2 flex-wrap">
                          <span className="font-medium text-text-primary text-sm">
                            {run.pipeline_id}
                          </span>
                          {run.branch ? (
                            <span className="inline-flex items-center gap-1 font-mono text-[11px] px-1.5 py-0.5 rounded border border-border bg-background text-text-secondary">
                              <span className="opacity-50 text-[10px]">⎇</span>
                              <span>{run.branch}</span>
                            </span>
                          ) : null}
                        </div>
                        {/* Compact mobile commit preview */}
                        {run.commit_msg && (
                          <span className="text-xs text-text-muted truncate max-w-[220px] md:hidden">
                            {run.commit_msg}
                          </span>
                        )}
                      </div>
                    </td>

                    {/* Commit metadata (Desktop) */}
                    <td className="px-5 py-3.5 hidden md:table-cell">
                      {run.commit_sha || run.commit_msg ? (
                        <div className="flex flex-col gap-0.5 max-w-xs">
                          <div className="flex items-center gap-1.5">
                            {run.commit_sha && (
                              <span className="font-mono text-xs text-accent bg-accent/10 px-1.5 py-0.5 rounded border border-accent/20">
                                {run.commit_sha.substring(0, 7)}
                              </span>
                            )}
                            {(run.triggered_by || run.author) && (
                              <span className="text-xs text-text-muted truncate">
                                by {run.triggered_by || run.author}
                              </span>
                            )}
                          </div>
                          {run.commit_msg && (
                            <span
                              className="text-xs text-text-secondary truncate mt-0.5"
                              title={run.commit_msg}
                            >
                              {run.commit_msg}
                            </span>
                          )}
                        </div>
                      ) : (
                        <span className="text-xs text-text-muted font-mono">—</span>
                      )}
                    </td>

                    {/* Status */}
                    <td className="px-5 py-3.5">
                      <StatusBadge status={run.status} pulse={run.status === 'running'} />
                    </td>

                    {/* Triggered */}
                    <td className="px-5 py-3.5 text-text-muted text-xs font-mono">
                      {timeAgo(run.started_at)}
                    </td>

                    {/* Duration */}
                    <td className="px-5 py-3.5 font-mono text-xs text-text-secondary">
                      <LiveDuration
                        startedAt={run.started_at}
                        finishedAt={run.finished_at}
                        isLive={run.status === 'running' || run.status === 'pending'}
                      />
                    </td>

                    {/* Actions */}
                    <td
                      className="px-5 py-3.5 text-right"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <Link
                        href={`/runs/${run.id}`}
                        className="inline-flex items-center gap-1 rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium text-text-secondary transition-all hover:bg-surface-hover hover:text-text-primary hover:border-accent/30"
                      >
                        View →
                      </Link>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function RunsLoadingSkeleton() {
  return (
    <div className="min-h-screen bg-background">
      <div className="mx-auto max-w-6xl px-6 py-8">
        <div className="mb-8 flex flex-col md:flex-row md:items-center justify-between gap-4 animate-pulse">
          <div className="h-8 w-48 bg-border/40 rounded" />
          <div className="h-9 w-28 bg-border/30 rounded" />
        </div>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-8">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="h-20 bg-surface border border-border rounded-lg animate-pulse" />
          ))}
        </div>
        <div className="h-64 bg-surface border border-border rounded-lg animate-pulse" />
      </div>
    </div>
  );
}

export default function RunsPage() {
  return (
    <Suspense fallback={<RunsLoadingSkeleton />}>
      <RunsContent />
    </Suspense>
  );
}
