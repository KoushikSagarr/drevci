'use client';

import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import { getRuns, getStats, getQueueStatus } from '../lib/api';
import type { OverviewStats, QueueStatus } from '../lib/api';
import type { Run } from '../lib/types';
import StatusBadge from '../components/StatusBadge';
import LiveDuration from '../components/LiveDuration';
import { calculateRecentMetrics, formatDurationSec } from '../lib/overview-metrics';

function timeAgo(dateStr?: string) {
  if (!dateStr || dateStr === '0001-01-01T00:00:00Z') return '—';
  const diff = Math.floor((Date.now() - new Date(dateStr).getTime()) / 1000);
  if (isNaN(diff) || diff < 0) return '—';
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
  loading = false,
  error = false,
}: {
  label: string;
  value: React.ReactNode;
  sub?: string;
  color?: string;
  loading?: boolean;
  error?: boolean;
}) {
  return (
    <div className="flex flex-col rounded-lg border border-border bg-surface p-4 min-w-0">
      <span className="text-xs font-medium text-text-muted uppercase tracking-wider mb-1">
        {label}
      </span>
      {loading ? (
        <div className="skeleton h-8 w-24 my-0.5" />
      ) : error ? (
        <span className="text-xl font-bold text-text-muted">Unavailable</span>
      ) : (
        <span className={`text-2xl font-bold tabular-nums ${color || 'text-text-primary'}`}>
          {value}
        </span>
      )}
      {sub && <span className="text-xs text-text-muted mt-0.5">{sub}</span>}
    </div>
  );
}

export default function OverviewPage() {
  const [stats, setStats] = useState<OverviewStats | null>(null);
  const [statsLoading, setStatsLoading] = useState(true);
  const [statsError, setStatsError] = useState<string | null>(null);

  const [runs, setRuns] = useState<Run[] | null>(null);
  const [runsLoading, setRunsLoading] = useState(true);
  const [runsError, setRunsError] = useState<string | null>(null);

  const [queueFallback, setQueueFallback] = useState<QueueStatus | null>(null);
  const [fallbackRuns, setFallbackRuns] = useState<Run[] | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  // Independent fetch logic: failure in one endpoint does not break the other
  useEffect(() => {
    let isCancelled = false;

    const fetchStats = async () => {
      try {
        const s = await getStats(25);
        if (!isCancelled) {
          setStats(s);
          setStatsError(null);
          setQueueFallback(null);
          setFallbackRuns(null);
        }
      } catch (err) {
        if (!isCancelled) {
          setStatsError(err instanceof Error ? err.message : 'Stats unavailable');
          try {
            const qs = await getQueueStatus();
            if (!isCancelled) setQueueFallback(qs);
          } catch {
            // Ignore secondary fallback error
          }
          // Fetch bounded 25-run sample specifically for fallback metrics only when stats is unavailable
          try {
            const fr = await getRuns(25);
            if (!isCancelled) setFallbackRuns(fr || []);
          } catch {
            // Ignore secondary fallback error
          }
        }
      } finally {
        if (!isCancelled) {
          setStatsLoading(false);
        }
      }
    };

    const fetchRuns = async () => {
      try {
        const data = await getRuns(10);
        if (!isCancelled) {
          setRuns(data || []);
          setRunsError(null);
        }
      } catch (err) {
        if (!isCancelled) {
          setRunsError(err instanceof Error ? err.message : 'Runs unavailable');
        }
      } finally {
        if (!isCancelled) {
          setRunsLoading(false);
        }
      }
    };

    fetchStats();
    fetchRuns();

    const sInterval = setInterval(fetchStats, 15000);
    const rInterval = setInterval(fetchRuns, 15000);

    return () => {
      isCancelled = true;
      clearInterval(sInterval);
      clearInterval(rInterval);
    };
  }, [reloadKey]);

  const fallbackMetrics = useMemo(() => {
    // When stats is unavailable, prefer the dedicated 25-run fallback batch if loaded; otherwise fall back to runs
    const source = fallbackRuns && fallbackRuns.length > 0 ? fallbackRuns : (runs || []);
    return calculateRecentMetrics(source, 25);
  }, [fallbackRuns, runs]);

  // Sourced from dedicated backend stats API when available; falls back to client calculations
  const effectiveQueueDepth = stats ? stats.queue_depth : (queueFallback?.depth ?? 0);
  const effectiveWorkers = stats ? stats.worker_capacity : (queueFallback?.workers ?? '—');
  const effectivePassRate = stats ? stats.pass_rate : fallbackMetrics.passRate;
  const effectiveCompletedSampleCount = stats ? stats.completed_sample_count : fallbackMetrics.completedSampleCount;
  const effectiveDurationSampleCount = stats ? stats.duration_sample_count : fallbackMetrics.durationSampleCount;
  const effectiveMeanDurationFormatted = stats
    ? formatDurationSec(stats.mean_duration_sec)
    : fallbackMetrics.meanDurationFormatted;
  const effectiveActiveCount = stats ? stats.active_runs_count : fallbackMetrics.activeInBatch;
  const effectiveRecentFailures: Run[] = stats ? stats.recent_failures : fallbackMetrics.recentFailures;

  const recentRuns = useMemo(() => {
    // Recent Activity always displays at most 10 runs
    const source = (runs && runs.length > 0) ? runs : (fallbackRuns || []);
    return source.slice(0, 10);
  }, [runs, fallbackRuns]);

  const hasAnyData = stats
    ? stats.total_runs_count > 0
    : ((runs !== null && runs.length > 0) || (fallbackRuns !== null && fallbackRuns.length > 0));

  const isFreshZeroRuns = stats
    ? (!statsLoading && stats.total_runs_count === 0 && !statsError)
    : (!runsLoading && runs !== null && runs.length === 0 && (!fallbackRuns || fallbackRuns.length === 0) && !runsError);

  return (
    <main className="min-h-screen bg-background">
      <div className="mx-auto max-w-6xl px-6 py-8 space-y-8">
        {/* Page Header */}
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-text-primary tracking-tight">System Overview</h1>
            <p className="text-sm text-text-muted mt-1">
              Operational pulse, runner queue, and recent execution health
            </p>
          </div>
          <div className="flex items-center gap-3">
            <Link
              href="/runs"
              className="inline-flex items-center gap-1.5 rounded-md border border-border bg-surface px-4 py-2 text-xs font-semibold text-text-primary hover:bg-surface-hover hover:border-accent/40 transition-colors"
            >
              <span>Explore All Pipelines</span>
              <span>→</span>
            </Link>
          </div>
        </div>

        {/* Partial Error Notification Banner */}
        {(statsError || runsError) && (
          <div
            role="alert"
            className="flex items-center justify-between rounded-lg border border-status-failed/30 bg-status-failed/10 px-4 py-3 text-sm text-text-primary"
          >
            <div className="flex items-center gap-2">
              <span className="text-status-failed font-bold">⚠</span>
              <span>
                {statsError && runsError
                  ? `Failed to load dashboard data: ${statsError}`
                  : statsError
                  ? `Unable to refresh system statistics: ${statsError}`
                  : `Unable to refresh recent runs: ${runsError}`}
              </span>
            </div>
            <button
              onClick={() => {
                setStatsLoading(true);
                setRunsLoading(true);
                setReloadKey((k) => k + 1);
              }}
              className="text-xs font-semibold text-text-primary hover:underline ml-4 cursor-pointer"
            >
              Retry
            </button>
          </div>
        )}

        {/* Operational Pulse: 4 Key Metric Cards */}
        <section aria-labelledby="pulse-heading">
          <h2 id="pulse-heading" className="sr-only">Operational Metrics</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {/* 1. Queue Depth */}
            <StatCard
              label="Queue Depth"
              value={effectiveQueueDepth}
              sub={effectiveQueueDepth === 1 ? '1 job waiting' : `${effectiveQueueDepth} jobs waiting`}
              color={effectiveQueueDepth > 10 ? 'text-[#d29922]' : 'text-text-primary'}
              loading={statsLoading && !stats && !queueFallback}
              error={!!statsError && !stats && !queueFallback}
            />

            {/* 2. Configured Workers (Capacity) */}
            <StatCard
              label="Worker Capacity"
              value={effectiveWorkers}
              sub="Configured concurrency"
              loading={statsLoading && !stats && !queueFallback}
              error={!!statsError && !stats && !queueFallback}
            />

            {/* 3. Recent Pass Rate (Explicit sample scope) */}
            <StatCard
              label="Recent Pass Rate"
              value={effectivePassRate !== null ? `${effectivePassRate}%` : '—'}
              sub={
                effectiveCompletedSampleCount > 0
                  ? effectiveCompletedSampleCount === 1
                    ? 'Last 1 completed run'
                    : `Last ${effectiveCompletedSampleCount} completed runs`
                  : effectiveActiveCount > 0
                  ? `${effectiveActiveCount} ${effectiveActiveCount === 1 ? 'run' : 'runs'} in progress`
                  : 'No completed runs in sample'
              }
              color={
                effectivePassRate === null
                  ? 'text-text-muted'
                  : effectivePassRate >= 80
                  ? 'text-status-success'
                  : effectivePassRate >= 50
                  ? 'text-[#d29922]'
                  : 'text-status-failed'
              }
              loading={statsLoading && !stats && runsLoading}
              error={!!statsError && !stats && !!runsError && !runs}
            />

            {/* 4. Mean Duration (Explicit sample scope) */}
            <StatCard
              label="Mean Duration"
              value={effectiveMeanDurationFormatted}
              sub={
                effectiveDurationSampleCount > 0
                  ? effectiveDurationSampleCount === 1
                    ? 'From 1 timed completed run'
                    : `Avg of ${effectiveDurationSampleCount} timed runs`
                  : 'No timed runs in sample'
              }
              loading={statsLoading && !stats && runsLoading}
              error={!!statsError && !stats && !!runsError && !runs}
            />
          </div>
        </section>

        {/* Zero-Data / Onboarding State for Fresh Installation */}
        {isFreshZeroRuns && (
          <div className="rounded-xl border border-dashed border-border bg-surface/50 p-10 text-center">
            <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-xl border border-border bg-background text-2xl text-accent mb-4">
              ⚡
            </div>
            <h3 className="text-lg font-bold text-text-primary">No pipeline runs recorded yet</h3>
            <p className="mt-1 text-sm text-text-muted max-w-md mx-auto">
              Drev CI engine is online and ready to execute pipelines. Trigger your first run
              using the CLI or push a commit with a configured webhook.
            </p>
            <div className="mt-6 flex flex-wrap items-center justify-center gap-3">
              <code className="rounded bg-background border border-border px-3 py-1.5 text-xs font-mono text-text-secondary select-all">
                ./bin/drev trigger --pipeline configs/webhooks/example/.drev.yml
              </code>
            </div>
          </div>
        )}

        {/* Main Grid: Failures Panel & Activity Stream */}
        {hasAnyData && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            {/* Left: Recent Failures Panel */}
            <div className="lg:col-span-1 space-y-4">
              <div className="flex items-center justify-between px-1">
                <h3 className="text-xs font-bold text-text-muted uppercase tracking-widest">
                  Recent Failures
                </h3>
                {effectiveRecentFailures.length > 0 && (
                  <span className="rounded-full bg-status-failed/20 px-2 py-0.5 text-[11px] font-semibold text-status-failed">
                    {effectiveRecentFailures.length} attention needed
                  </span>
                )}
              </div>

              {effectiveRecentFailures.length === 0 ? (
                <div className="rounded-lg border border-border bg-surface p-6 text-center space-y-2">
                  {effectiveCompletedSampleCount > 0 ? (
                    <>
                      <span className="inline-block text-xl text-status-success">✓</span>
                      <h4 className="text-sm font-semibold text-text-primary">
                        Zero pipeline failures
                      </h4>
                      <p className="text-xs text-text-muted">
                        {effectiveCompletedSampleCount === 1
                          ? 'Zero failures across the last completed run in this sample.'
                          : `Zero failures across the last ${effectiveCompletedSampleCount} completed runs in this sample.`}
                      </p>
                    </>
                  ) : effectiveActiveCount > 0 ? (
                    <>
                      <div className="flex justify-center items-center py-1">
                        <span className="relative flex h-3 w-3">
                          <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-status-running opacity-75" />
                          <span className="relative inline-flex h-3 w-3 rounded-full bg-status-running" />
                        </span>
                      </div>
                      <h4 className="text-sm font-semibold text-text-primary">
                        Pipelines in progress
                      </h4>
                      <p className="text-xs text-text-muted">
                        {effectiveActiveCount === 1
                          ? '1 pipeline run currently executing. Results will update upon completion.'
                          : `${effectiveActiveCount} pipeline runs currently executing. Results will update upon completion.`}
                      </p>
                    </>
                  ) : (
                    <>
                      <span className="inline-block text-xl text-text-muted">—</span>
                      <h4 className="text-sm font-semibold text-text-primary">
                        No completed runs
                      </h4>
                      <p className="text-xs text-text-muted">
                        No completed pipeline runs available to report failures.
                      </p>
                    </>
                  )}
                </div>
              ) : (
                <div className="space-y-3">
                  {effectiveRecentFailures.map((failure: Run) => (
                    <div
                      key={failure.id}
                      className="rounded-lg border border-status-failed/30 bg-surface p-4 space-y-2 transition-colors hover:border-status-failed/60"
                    >
                      <div className="flex items-center justify-between gap-2">
                        <span className="font-bold text-text-primary text-sm truncate">
                          {failure.pipeline_id}
                        </span>
                        <span className="text-[11px] font-mono text-text-muted shrink-0">
                          {timeAgo(failure.finished_at || failure.started_at)}
                        </span>
                      </div>
                      <div className="flex items-center gap-3 text-xs text-text-muted font-mono">
                        <span>{failure.branch || 'Unknown branch'}</span>
                        {failure.commit_sha && (
                          <>
                            <span>•</span>
                            <span>{failure.commit_sha.substring(0, 7)}</span>
                          </>
                        )}
                      </div>
                      <div className="pt-1">
                        <Link
                          href={`/runs/${failure.id}`}
                          className="inline-flex items-center gap-1 text-xs font-semibold text-status-failed hover:underline"
                        >
                          <span>Inspect failure in log</span>
                          <span>→</span>
                        </Link>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Right: Recent Pipeline Activity */}
            <div className="lg:col-span-2 space-y-4">
              <div className="flex items-center justify-between px-1">
                <h3 className="text-xs font-bold text-text-muted uppercase tracking-widest">
                  Recent Activity (Last {recentRuns.length})
                </h3>
                <Link
                  href="/runs"
                  className="text-xs font-medium text-accent hover:underline"
                >
                  View all runs →
                </Link>
              </div>

              <div className="overflow-hidden rounded-lg border border-border bg-surface">
                <table className="w-full text-left text-sm" aria-label="Recent Pipeline Runs">
                  <thead>
                    <tr className="border-b border-border bg-background/50 text-xs font-medium text-text-muted uppercase tracking-wider">
                      <th scope="col" className="px-4 py-3">Run</th>
                      <th scope="col" className="px-4 py-3">Pipeline</th>
                      <th scope="col" className="px-4 py-3">Status</th>
                      <th scope="col" className="px-4 py-3">Branch</th>
                      <th scope="col" className="px-4 py-3">Duration</th>
                      <th scope="col" className="px-4 py-3 text-right">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recentRuns.map((run) => (
                      <tr
                        key={run.id}
                        className="border-b border-border/60 hover:bg-surface-hover transition-colors"
                      >
                        <td className="px-4 py-3 font-mono text-xs text-text-secondary">
                          <Link
                            href={`/runs/${run.id}`}
                            className="hover:text-accent font-semibold"
                          >
                            {run.id.substring(0, 8)}
                          </Link>
                        </td>
                        <td className="px-4 py-3 font-medium text-text-primary text-sm truncate max-w-[160px]">
                          {run.pipeline_id}
                        </td>
                        <td className="px-4 py-3">
                          <StatusBadge status={run.status} pulse={run.status === 'running'} />
                        </td>
                        <td className="px-4 py-3 font-mono text-xs text-text-muted truncate max-w-[120px]">
                          {run.branch || '—'}
                        </td>
                        <td className="px-4 py-3 font-mono text-xs text-text-secondary">
                          <LiveDuration
                            startedAt={run.started_at}
                            finishedAt={run.finished_at}
                            isLive={run.status === 'running' || run.status === 'pending'}
                          />
                        </td>
                        <td className="px-4 py-3 text-right">
                          <Link
                            href={`/runs/${run.id}`}
                            className="inline-flex items-center gap-1 rounded border border-border bg-background px-2.5 py-1 text-xs font-medium text-text-secondary hover:text-text-primary hover:border-accent/30 transition-colors"
                          >
                            View →
                          </Link>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </div>
    </main>
  );
}
