'use client';

import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import { getRuns, getStats, getQueueStatus } from '../lib/api';
import type { OverviewStats, QueueStatus } from '../lib/api';
import type { Run } from '../lib/types';
import StatusBadge from '../components/StatusBadge';
import LiveDuration from '../components/LiveDuration';
import {
  calculateRecentMetrics,
  formatDurationSec,
  resolveRefreshStatus,
  resolveErrorBanner,
} from '../lib/overview-metrics';

function timeAgo(dateStr?: string) {
  if (!dateStr || dateStr === '0001-01-01T00:00:00Z') return '—';
  const diff = Math.floor((Date.now() - new Date(dateStr).getTime()) / 1000);
  if (isNaN(diff) || diff < 0) return '—';
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

/* ─── Stat Card Icons (inline SVG for zero-dep rendering) ─── */

function QueueIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className={className}>
      <rect x="3" y="4" width="18" height="4" rx="1" />
      <rect x="3" y="10" width="18" height="4" rx="1" />
      <rect x="3" y="16" width="18" height="4" rx="1" />
    </svg>
  );
}

function WorkersIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className={className}>
      <rect x="4" y="4" width="16" height="16" rx="2" />
      <line x1="9" y1="4" x2="9" y2="20" />
      <line x1="15" y1="4" x2="15" y2="20" />
      <line x1="4" y1="9" x2="20" y2="9" />
      <line x1="4" y1="15" x2="20" y2="15" />
    </svg>
  );
}

function PassRateIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className={className}>
      <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
      <polyline points="22 4 12 14.01 9 11.01" />
    </svg>
  );
}

function DurationIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className={className}>
      <circle cx="12" cy="12" r="10" />
      <polyline points="12 6 12 12 16 14" />
    </svg>
  );
}

/* ─── Stat Card ─── */

function StatCard({
  label,
  value,
  sub,
  color,
  glowColor,
  icon,
  loading = false,
  error = false,
}: {
  label: string;
  value: React.ReactNode;
  sub?: string;
  color?: string;
  glowColor?: string;
  icon?: React.ReactNode;
  loading?: boolean;
  error?: boolean;
}) {
  return (
    <div
      className="stat-card-glow flex flex-col rounded-xl border border-border/80 bg-surface p-3.5 sm:p-5 min-w-0 transition-colors hover:border-border"
      style={{ '--glow-color': glowColor || '#F97316' } as React.CSSProperties}
    >
      <div className="flex items-center justify-between mb-2 sm:mb-3">
        <span className="text-[10px] sm:text-[11px] font-semibold text-text-muted uppercase tracking-wider">
          {label}
        </span>
        {icon && (
          <span className="text-text-muted/50">
            {icon}
          </span>
        )}
      </div>
      {loading ? (
        <div className="skeleton h-7 sm:h-8 w-20 sm:w-24 my-0.5" />
      ) : error ? (
        <span className="text-base sm:text-lg font-bold text-text-muted">Unavailable</span>
      ) : (
        <span className={`text-xl sm:text-2xl font-bold tabular-nums tracking-tight ${color || 'text-text-primary'}`}>
          {value}
        </span>
      )}
      {sub && (
        <span className="text-[10px] sm:text-[11px] text-text-muted mt-1 sm:mt-1.5 leading-tight">{sub}</span>
      )}
    </div>
  );
}

/* ─── Overview Page ─── */

export default function OverviewPage() {
  const [stats, setStats] = useState<OverviewStats | null>(null);
  const [statsLoading, setStatsLoading] = useState(true);
  const [statsError, setStatsError] = useState<string | null>(null);

  const [runs, setRuns] = useState<Run[] | null>(null);
  const [runsLoading, setRunsLoading] = useState(true);
  const [runsError, setRunsError] = useState<string | null>(null);

  const [queueFallback, setQueueFallback] = useState<QueueStatus | null>(null);
  const [fallbackRuns, setFallbackRuns] = useState<Run[] | null>(null);
  const [fallbackActive, setFallbackActive] = useState(false);
  const [reloadKey, setReloadKey] = useState(0);

  const [statsUpdatedAt, setStatsUpdatedAt] = useState<Date | null>(null);
  const [runsUpdatedAt, setRunsUpdatedAt] = useState<Date | null>(null);

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
          setFallbackActive(false);
          setStatsUpdatedAt(new Date());
        }
      } catch (err) {
        if (!isCancelled) {
          setStatsError(err instanceof Error ? err.message : 'Stats unavailable');
          let hasFallbackData = false;
          try {
            const qs = await getQueueStatus();
            if (!isCancelled && qs) {
              setQueueFallback(qs);
              hasFallbackData = true;
            }
          } catch {
            // Ignore secondary fallback error
          }
          // Fetch bounded 25-run sample specifically for fallback metrics only when stats is unavailable
          try {
            const fr = await getRuns(25);
            if (!isCancelled && fr) {
              setFallbackRuns(fr);
              hasFallbackData = true;
            }
          } catch {
            // Ignore secondary fallback error
          }
          if (!isCancelled) {
            if (hasFallbackData) {
              setFallbackActive(true);
              setStatsUpdatedAt(new Date());
            } else {
              setFallbackActive(false);
            }
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
          setRunsUpdatedAt(new Date());
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
  const effectiveRecentFailures: Run[] = (stats?.recent_failures && Array.isArray(stats.recent_failures))
    ? stats.recent_failures
    : (fallbackMetrics.recentFailures || []);

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

  const isInitialLoading = (statsLoading && !stats && !queueFallback) && (runsLoading && !runs);

  const refreshStatus = useMemo(() => {
    return resolveRefreshStatus({
      isInitialLoading,
      statsError,
      runsError,
      fallbackActive,
      queueFallback,
      fallbackRuns,
      statsUpdatedAt,
      runsUpdatedAt,
    });
  }, [isInitialLoading, statsError, runsError, fallbackActive, queueFallback, fallbackRuns, statsUpdatedAt, runsUpdatedAt]);

  const errorBanner = useMemo(() => {
    return resolveErrorBanner({
      statsError,
      runsError,
      fallbackActive,
    });
  }, [statsError, runsError, fallbackActive]);

  return (
    <main className="min-h-screen bg-background">
      <div className="mx-auto max-w-6xl px-4 sm:px-6 py-8 space-y-8">
        {/* Page Header */}
        <div className="flex flex-col md:flex-row md:items-end justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-text-primary tracking-tight">
              System Overview
            </h1>
            <p className="text-sm text-text-muted mt-1">
              Operational pulse, runner queue, and recent execution health
            </p>
          </div>
          <div className="flex items-center gap-3">
            {/* Truthful live polling & status indicator */}
            <div
              role="status"
              aria-live="polite"
              className="flex items-center gap-1.5 text-[11px] font-mono text-text-secondary"
            >
              <span className="relative flex h-1.5 w-1.5">
                <span className={`inline-flex h-1.5 w-1.5 rounded-full ${refreshStatus.color}`} />
              </span>
              <span>{refreshStatus.label}</span>
            </div>

            <Link
              href="/runs"
              className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-surface px-4 py-2 text-xs font-semibold text-text-primary hover:bg-surface-hover hover:border-accent/40 transition-all focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-accent"
            >
              <span>Explore All Pipelines</span>
              <span className="text-text-muted">→</span>
            </Link>
          </div>
        </div>

        {/* Partial Error Notification Banner */}
        {errorBanner.visible && (
          <div
            role="alert"
            className="flex items-center justify-between rounded-lg border border-status-failed/30 bg-status-failed/10 px-4 py-3 text-sm text-text-primary animate-fade-in-up"
          >
            <div className="flex items-center gap-2">
              <span className="text-status-failed font-bold">⚠</span>
              <span>{errorBanner.message}</span>
            </div>
            <button
              onClick={() => {
                setStatsLoading(true);
                setRunsLoading(true);
                setReloadKey((k) => k + 1);
              }}
              className="text-xs font-semibold text-text-primary hover:underline ml-4 cursor-pointer focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-accent rounded px-1"
            >
              Retry
            </button>
          </div>
        )}

        {/* Operational Pulse: 4 Key Metric Cards */}
        <section aria-labelledby="pulse-heading">
          <h2 id="pulse-heading" className="sr-only">Operational Metrics</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 stagger-children">
            {/* 1. Queue Depth */}
            <StatCard
              label="Queue Depth"
              value={effectiveQueueDepth}
              sub={effectiveQueueDepth === 1 ? '1 job waiting' : `${effectiveQueueDepth} jobs waiting`}
              color={effectiveQueueDepth > 10 ? 'text-[#d29922]' : 'text-text-primary'}
              glowColor={effectiveQueueDepth > 10 ? '#d29922' : '#3fb950'}
              icon={<QueueIcon className="w-4 h-4" />}
              loading={statsLoading && !stats && !queueFallback}
              error={!!statsError && !stats && !queueFallback}
            />

            {/* 2. Configured Workers (Capacity) */}
            <StatCard
              label="Worker Capacity"
              value={effectiveWorkers}
              sub="Configured concurrency"
              glowColor="#388bfd"
              icon={<WorkersIcon className="w-4 h-4" />}
              loading={statsLoading && !stats && !queueFallback}
              error={!!statsError && !stats && !queueFallback}
            />

            {/* 3. Recent Pass Rate (Explicit sample scope) */}
            <StatCard
              label="Recent Pass Rate"
              value={typeof effectivePassRate === 'number' ? `${effectivePassRate}%` : '—'}
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
                typeof effectivePassRate !== 'number'
                  ? 'text-text-muted'
                  : effectivePassRate >= 80
                  ? 'text-status-success'
                  : effectivePassRate >= 50
                  ? 'text-[#d29922]'
                  : 'text-status-failed'
              }
              glowColor={
                typeof effectivePassRate !== 'number'
                  ? '#71717A'
                  : effectivePassRate >= 80
                  ? '#3fb950'
                  : effectivePassRate >= 50
                  ? '#d29922'
                  : '#f85149'
              }
              icon={<PassRateIcon className="w-4 h-4" />}
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
              glowColor="#F97316"
              icon={<DurationIcon className="w-4 h-4" />}
              loading={statsLoading && !stats && runsLoading}
              error={!!statsError && !stats && !!runsError && !runs}
            />
          </div>
        </section>

        {/* Zero-Data / Onboarding State for Fresh Installation */}
        {isFreshZeroRuns && (
          <div className="rounded-xl border border-dashed border-border bg-surface/50 p-10 text-center animate-fade-in-up">
            <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-2xl border border-accent/30 bg-accent/10 text-2xl text-accent mb-4">
              ⚡
            </div>
            <h3 className="text-lg font-bold text-text-primary">No pipeline runs recorded yet</h3>
            <p className="mt-1 text-sm text-text-muted max-w-md mx-auto">
              Drev CI engine is online and ready to execute pipelines. Trigger your first run
              using the CLI or push a commit with a configured webhook.
            </p>
            <div className="mt-6 flex flex-wrap items-center justify-center gap-3">
              <code className="rounded-lg bg-background border border-border px-4 py-2 text-xs font-mono text-text-secondary select-all">
                ./bin/drev trigger --pipeline configs/webhooks/example/.drev.yml
              </code>
            </div>
          </div>
        )}

        {/* Main Grid: Failures Panel & Activity Stream */}
        {hasAnyData && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            {/* Left: Recent Failures Panel */}
            <div className="lg:col-span-1 space-y-4 animate-fade-in-up" style={{ animationDelay: '100ms' }}>
              <div className="flex items-center justify-between px-1">
                <h3 className="text-xs font-bold text-text-muted uppercase tracking-widest">
                  Recent Failures
                </h3>
                {effectiveRecentFailures.length > 0 && (
                  <span className="rounded-full bg-status-failed/15 border border-status-failed/20 px-2.5 py-0.5 text-[11px] font-semibold text-status-failed">
                    {effectiveRecentFailures.length} attention needed
                  </span>
                )}
              </div>

              {effectiveRecentFailures.length === 0 ? (
                <div className="rounded-xl border border-border bg-surface p-6 text-center space-y-2">
                  {effectiveCompletedSampleCount > 0 ? (
                    <>
                      <div className="mx-auto flex h-10 w-10 items-center justify-center rounded-full bg-status-success/10 border border-status-success/20 text-status-success mb-2">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="w-5 h-5">
                          <polyline points="20 6 9 17 4 12" />
                        </svg>
                      </div>
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
                      className="group rounded-xl border border-status-failed/20 bg-surface p-4 space-y-2 transition-all hover:border-status-failed/50 hover:bg-status-failed/[0.03] row-hover-lift"
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
                            <span className="text-border">·</span>
                            <span>{failure.commit_sha.substring(0, 7)}</span>
                          </>
                        )}
                      </div>
                      <div className="pt-1">
                        <Link
                          href={`/runs/${failure.id}`}
                          className="inline-flex items-center gap-1 text-xs font-semibold text-status-failed hover:underline group-hover:text-status-failed/80"
                        >
                          <span>Inspect failure in log</span>
                          <span className="transition-transform group-hover:translate-x-0.5">→</span>
                        </Link>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Right: Recent Pipeline Activity */}
            <div className="lg:col-span-2 space-y-4 animate-fade-in-up" style={{ animationDelay: '150ms' }}>
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

              <div className="overflow-x-auto rounded-xl border border-border bg-surface">
                <table className="w-full text-left text-sm" aria-label="Recent Pipeline Runs">
                  <thead>
                    <tr className="border-b border-border bg-background/50 text-[11px] font-semibold text-text-muted uppercase tracking-wider">
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
                        className="group border-b border-border/50 hover:bg-surface-hover/50 transition-colors"
                      >
                        <td className="px-4 py-3 font-mono text-xs text-text-secondary">
                          <Link
                            href={`/runs/${run.id}`}
                            className="hover:text-accent font-semibold transition-colors"
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
                            className="inline-flex items-center gap-1 rounded-md border border-border/70 bg-background px-2.5 py-1 text-xs font-medium text-text-secondary hover:text-text-primary hover:border-accent/30 transition-all"
                          >
                            View <span className="text-text-muted/60 transition-transform group-hover:translate-x-0.5">→</span>
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

        {/* Footer */}
        <footer className="border-t border-border/40 pt-6 pb-4">
          <div className="flex items-center justify-between text-[11px] text-text-muted">
            <span>Drev CI Dashboard</span>
            <span>Auto-refresh every 15s</span>
          </div>
        </footer>
      </div>
    </main>
  );
}
