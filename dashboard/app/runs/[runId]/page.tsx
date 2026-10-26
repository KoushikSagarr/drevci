'use client';

import { useEffect, useState, useRef, use } from 'react';
import Link from 'next/link';
import { getRun, getRunJobs, isNotFoundError } from '../../../lib/api';
import { Run, RunJob } from '../../../lib/types';
import StatusBadge from '../../../components/StatusBadge';
import LogViewer, { LogViewerRef } from '../../../components/LogViewer';
import LiveDuration from '../../../components/LiveDuration';
import JobTimeline from '../../../components/JobTimeline';

function formatTimestamp(ts?: string) {
  if (!ts || ts === '0001-01-01T00:00:00Z') return '—';
  const d = new Date(ts);
  if (isNaN(d.getTime())) return '—';
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

// --- Detail Card ---
function DetailCard({
  label,
  value,
  sub,
  color,
}: {
  label: string;
  value: React.ReactNode;
  sub?: string;
  color?: string;
}) {
  return (
    <div className="flex flex-col rounded-lg border border-border bg-surface p-4">
      <span className="text-[10px] font-bold text-text-muted uppercase tracking-widest mb-1">{label}</span>
      <span className={`text-lg font-bold tabular-nums ${color || 'text-text-primary'}`}>{value}</span>
      {sub && <span className="text-[10px] text-text-muted mt-0.5">{sub}</span>}
    </div>
  );
}

export default function RunDetailPage({ params }: { params: Promise<{ runId: string }> }) {
  const { runId } = use(params);
  const [run, setRun] = useState<Run | null>(null);
  const [jobs, setJobs] = useState<RunJob[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);
  const [copiedSha, setCopiedSha] = useState(false);
  const [firstError, setFirstError] = useState<{ lineNum: number; snippet: string } | null>(null);
  const logViewerRef = useRef<LogViewerRef>(null);

  const status = run?.status;

  useEffect(() => {
    let isCancelled = false;

    const fetchData = async () => {
      try {
        const r = await getRun(runId);
        const j = await getRunJobs(runId);
        if (!isCancelled) {
          setRun(r);
          setJobs(j || []);
          setError(null);
        }
      } catch (err) {
        if (!isCancelled) {
          if (isNotFoundError(err)) {
            console.warn(`Run ${runId} not found (404)`);
          } else {
            console.error('Failed to fetch run data:', err);
          }
          setError(err instanceof Error ? err.message : 'Failed to fetch run details');
        }
      }
    };

    fetchData();
    const isActive = !status || status === 'pending' || status === 'running';
    let interval: NodeJS.Timeout | undefined;
    if (isActive) interval = setInterval(fetchData, 3000);
    return () => {
      isCancelled = true;
      if (interval) clearInterval(interval);
    };
  }, [runId, status, reloadKey]);

  const copyCommitSha = (sha: string) => {
    navigator.clipboard.writeText(sha);
    setCopiedSha(true);
    setTimeout(() => setCopiedSha(false), 2000);
  };

  if (error && !run) {
    const isNotFound = isNotFoundError(error);
    return (
      <div className="mx-auto max-w-6xl px-6 py-16 text-center">
        <div className="flex flex-col items-center gap-4 rounded-xl border border-border bg-surface p-8 max-w-lg mx-auto animate-fade-in-up">
          <div className="text-4xl">{isNotFound ? '🔍' : '⚠'}</div>
          <h2 className="text-lg font-bold text-text-primary">
            {isNotFound ? 'Run Not Found' : 'Unable to load run details'}
          </h2>
          <p className="text-sm text-text-muted max-w-md">
            {isNotFound
              ? `The requested pipeline run "${runId}" does not exist or may have expired.`
              : error}
          </p>
          <div className="flex gap-3 mt-2">
            {!isNotFound && (
              <button
                onClick={() => setReloadKey((k) => k + 1)}
                className="rounded-md border border-border bg-background px-4 py-2 text-xs font-semibold text-text-primary hover:bg-surface-hover transition-colors"
              >
                ↻ Try Again
              </button>
            )}
            <Link
              href="/runs"
              className="rounded-md bg-accent px-4 py-2 text-xs font-semibold text-white hover:bg-accent/90 transition-colors"
            >
              ← Back to Pipeline Runs
            </Link>
          </div>
        </div>
      </div>
    );
  }

  if (!run) {
    return (
      <div className="mx-auto max-w-6xl p-6 space-y-4">
        <div className="skeleton h-6 w-48" />
        <div className="skeleton h-40 w-full rounded-lg" />
        <div className="skeleton h-60 w-full rounded-lg" />
      </div>
    );
  }

  const isLive = run.status === 'pending' || run.status === 'running';
  const succeededCount = jobs.filter((j) => j.status === 'success').length;
  const failedCount = jobs.filter((j) => j.status === 'failed').length;

  return (
    <div className="mx-auto max-w-6xl px-6 py-8 space-y-8">
      {/* Top Nav / Breadcrumbs */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-2 text-sm">
          <Link href="/runs" className="text-text-muted hover:text-accent font-medium transition-colors">
            Pipelines
          </Link>
          <span className="text-text-muted">/</span>
          <Link
            href={`/runs?pipeline=${encodeURIComponent(run.pipeline_id)}`}
            className="text-text-muted hover:text-accent font-medium transition-colors"
            title={`View all runs for ${run.pipeline_id}`}
          >
            {run.pipeline_id}
          </Link>
          <span className="text-text-muted">/</span>
          <span className="text-text-primary font-mono bg-surface px-2 py-0.5 rounded border border-border">
            {run.id.substring(0, 8)}
          </span>
        </div>
        <div className="flex items-center gap-3">
          {/* Restart Run button clearly disabled with explanatory state */}
          <button
            type="button"
            disabled
            aria-disabled="true"
            title="Pipeline re-runs are not yet supported in this version"
            className="inline-flex items-center gap-1.5 text-xs font-medium text-text-muted border border-border/60 bg-surface/40 px-3 py-1.5 rounded-md opacity-50 cursor-not-allowed select-none"
          >
            <span>↻</span>
            <span>Restart Run</span>
            <span className="text-[10px] text-text-muted/70 bg-background/50 px-1 rounded border border-border/40">
              Not yet supported
            </span>
          </button>
        </div>
      </div>

      {/* Header */}
      <div className="rounded-xl border border-border bg-surface overflow-hidden shadow-xl">
        <div className="p-8">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-8">
            <div className="space-y-4">
              <div className="flex items-center gap-4">
                <h1 className="text-3xl font-black text-text-primary tracking-tight uppercase">
                  {run.pipeline_id}
                </h1>
                <StatusBadge status={run.status} pulse={isLive} />
              </div>

              {/* Real Run Metadata — no fabricated fields */}
              <div className="flex flex-wrap items-center gap-6">
                <div className="flex flex-col">
                  <span className="text-[10px] font-bold text-text-muted uppercase tracking-widest">
                    Triggered
                  </span>
                  <span className="text-sm font-medium text-text-secondary">
                    {formatTimestamp(run.started_at)}
                  </span>
                </div>

                <div className="h-8 w-px bg-border hidden sm:block" />

                <div className="flex flex-col">
                  <span className="text-[10px] font-bold text-text-muted uppercase tracking-widest">
                    Trigger Source
                  </span>
                  <span className="text-sm font-medium text-text-secondary">
                    {run.triggered_by || 'Not specified'}
                  </span>
                </div>

                <div className="h-8 w-px bg-border hidden sm:block" />

                <div className="flex flex-col">
                  <span className="text-[10px] font-bold text-text-muted uppercase tracking-widest">
                    Branch
                  </span>
                  <span className="text-sm font-mono text-text-secondary">
                    {run.branch || 'Unknown branch'}
                  </span>
                </div>

                <div className="h-8 w-px bg-border hidden sm:block" />

                <div className="flex flex-col">
                  <span className="text-[10px] font-bold text-text-muted uppercase tracking-widest">
                    Commit
                  </span>
                  {run.commit_sha ? (
                    <div className="flex items-center gap-1.5" title={run.commit_msg || run.commit_sha}>
                      <span className="text-sm font-mono text-text-secondary">
                        {run.commit_sha.substring(0, 7)}
                      </span>
                      <button
                        type="button"
                        onClick={() => copyCommitSha(run.commit_sha!)}
                        title="Copy full commit SHA"
                        className="text-[11px] text-text-muted hover:text-text-primary px-1 py-0.5 rounded bg-background border border-border/60 transition-colors"
                      >
                        {copiedSha ? '✓' : '⎘'}
                      </button>
                    </div>
                  ) : (
                    <span className="text-sm font-mono text-text-muted">Not available</span>
                  )}
                </div>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4 shrink-0">
              <DetailCard
                label="Run Duration"
                value={
                  <LiveDuration
                    startedAt={run.started_at}
                    finishedAt={run.finished_at}
                    isLive={isLive}
                    className="text-accent"
                  />
                }
              />
              <DetailCard
                label="Job Success"
                value={`${succeededCount}/${jobs.length}`}
                color={failedCount > 0 ? 'text-status-failed' : 'text-status-success'}
                sub="jobs passed"
              />
            </div>
          </div>
        </div>
      </div>

      {/* Failure Callout Banner when pipeline failed */}
      {run.status === 'failed' && (
        <div className="rounded-lg border border-status-failed/40 bg-status-failed/10 p-4 text-sm text-text-primary flex flex-col sm:flex-row sm:items-start justify-between gap-4">
          <div className="flex items-start gap-3 min-w-0 flex-1">
            <span className="text-status-failed text-lg leading-none font-bold shrink-0 mt-0.5" aria-hidden="true">
              ⚠
            </span>
            <div className="min-w-0 space-y-1.5 flex-1">
              <div className="flex items-center gap-2">
                <h4 className="font-semibold text-status-failed">Pipeline run failed</h4>
                {failedCount > 0 && (
                  <span className="text-[11px] font-mono text-status-failed/80 bg-status-failed/15 px-1.5 py-0.5 rounded">
                    {failedCount} of {jobs.length} jobs failed
                  </span>
                )}
              </div>
              {firstError ? (
                <div className="space-y-1">
                  <div className="text-xs text-text-muted flex items-center gap-2">
                    <span>First error in console log:</span>
                    <span className="font-mono text-[11px] text-text-secondary bg-background/60 px-1 rounded border border-border/40">
                      Line {firstError.lineNum}
                    </span>
                  </div>
                  <pre className="font-mono text-xs text-status-failed bg-background/80 border border-status-failed/20 rounded px-2.5 py-1.5 overflow-x-auto whitespace-pre-wrap break-all max-h-20">
                    {firstError.snippet}
                  </pre>
                </div>
              ) : (
                <p className="text-xs text-text-muted">
                  {failedCount > 0
                    ? `${failedCount} of ${jobs.length} jobs failed. Check console output below for error details.`
                    : 'Execution failed. Check console output below for failure details.'}
                </p>
              )}
            </div>
          </div>

          {firstError && (
            <button
              type="button"
              onClick={() => logViewerRef.current?.jumpToError()}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-semibold bg-status-failed/20 text-status-failed hover:bg-status-failed/30 border border-status-failed/40 transition-colors shrink-0 self-start focus:outline-none focus:ring-1 focus:ring-status-failed cursor-pointer"
              title={`Jump to line ${firstError.lineNum} in console logs`}
            >
              <span>Jump to Error</span>
              <span className="font-mono text-[10px] opacity-75">(L{firstError.lineNum})</span>
              <span aria-hidden="true">↓</span>
            </button>
          )}
        </div>
      )}

      {/* Content Layout */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Left: Job Timeline */}
        <div className="lg:col-span-1">
          <JobTimeline
            run={run}
            jobs={jobs}
            isLive={isLive}
            onJumpToError={firstError ? () => logViewerRef.current?.jumpToError() : undefined}
          />
        </div>

        {/* Right: Console Logs */}
        <div className="lg:col-span-2 space-y-4">
          <h3 className="text-xs font-bold text-text-muted uppercase tracking-widest px-1">
            Console Output
          </h3>
          <LogViewer
            ref={logViewerRef}
            runId={runId}
            isLive={isLive}
            onFirstErrorDetected={setFirstError}
          />
        </div>
      </div>
    </div>
  );
}
