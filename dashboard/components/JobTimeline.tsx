'use client';

import { useState, useEffect, useMemo } from 'react';
import type { Run, RunJob } from '../lib/types';
import StatusBadge from './StatusBadge';
import LiveDuration from './LiveDuration';
import {
  calculateRunTimelineSummary,
  JobTimelineMetrics,
} from '../lib/timeline';

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

interface JobTimelineProps {
  run: Run;
  jobs: RunJob[];
  isLive: boolean;
  onJumpToError?: () => void;
}

export default function JobTimeline({
  run,
  jobs,
  isLive,
  onJumpToError,
}: JobTimelineProps) {
  const [now, setNow] = useState<number>(() => Date.now());

  // Dynamically tick live runs so timeline bars grow in real time
  useEffect(() => {
    if (!isLive) return;
    const interval = setInterval(() => {
      setNow(Date.now());
    }, 1000);
    return () => clearInterval(interval);
  }, [isLive]);

  const summary = useMemo(() => {
    return calculateRunTimelineSummary(run, jobs, now);
  }, [run, jobs, now]);

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'running':
        return '◉';
      case 'success':
        return '✓';
      case 'failed':
        return '✗';
      case 'cancelled':
        return '⊘';
      case 'pending':
      default:
        return '○';
    }
  };

  const getStatusNodeColor = (status: string) => {
    switch (status) {
      case 'running':
        return 'border-status-running text-status-running bg-status-running/10 shadow-[0_0_10px_rgba(56,139,253,0.15)]';
      case 'success':
        return 'border-status-success text-status-success bg-status-success/5';
      case 'failed':
        return 'border-status-failed text-status-failed bg-status-failed/5';
      case 'cancelled':
        return 'border-status-cancelled text-status-cancelled bg-status-cancelled/5';
      case 'pending':
      default:
        return 'border-status-pending text-status-pending bg-status-pending/5';
    }
  };

  const getProgressBarColor = (status: string) => {
    switch (status) {
      case 'running':
        return 'bg-status-running';
      case 'success':
        return 'bg-status-success';
      case 'failed':
        return 'bg-status-failed';
      case 'cancelled':
        return 'bg-status-cancelled';
      case 'pending':
      default:
        return 'bg-status-pending';
    }
  };

  return (
    <div className="space-y-4">
      {/* Header and Summary Bar */}
      <div className="flex flex-col gap-2">
        <div className="flex items-center justify-between px-1">
          <h3 className="text-xs font-bold text-text-muted uppercase tracking-widest">
            Pipeline Jobs ({jobs.length})
          </h3>
          {summary.hasValidTimeWindow && (
            <div className="flex items-center gap-3 text-[11px] font-mono text-text-muted">
              <span>
                Wall-Clock: <span className="text-text-primary font-semibold">{summary.formattedRunDuration}</span>
              </span>
              {summary.maxParallelJobs > 1 && (
                <span className="text-accent bg-accent/10 border border-accent/20 px-1.5 py-0.5 rounded text-[10px]">
                  Peak Parallelism: {summary.maxParallelJobs}x
                </span>
              )}
            </div>
          )}
        </div>

        {/* Global Pipeline Execution Ruler & Summary Overview */}
        {summary.hasValidTimeWindow && jobs.length > 0 && (
          <div className="rounded-lg border border-border bg-surface/60 p-3 space-y-2">
            <div className="flex items-center justify-between text-[10px] font-mono text-text-muted select-none">
              <span>0s</span>
              <span>Execution Timeline</span>
              <span>{summary.formattedRunDuration}</span>
            </div>

            {/* Aggregated multi-job timeline track */}
            <div
              className="relative h-2 w-full rounded bg-background border border-border/80 overflow-hidden"
              aria-hidden="true"
            >
              {/* Ruler graduation marks at 25%, 50%, 75% */}
              <div className="absolute left-1/4 top-0 bottom-0 w-px bg-border/40" />
              <div className="absolute left-1/2 top-0 bottom-0 w-px bg-border/40" />
              <div className="absolute left-3/4 top-0 bottom-0 w-px bg-border/40" />

              {summary.jobMetrics.map((m) => {
                if (!m.isStarted) return null;
                const barColor = getProgressBarColor(m.job.status);
                const isRunning = m.job.status === 'running';

                return (
                  <div
                    key={m.job.id}
                    title={`${m.job.job_name}: ${m.formattedDuration} (${m.wallClockSharePercent}% of run)`}
                    style={{
                      left: `${m.offsetPercent}%`,
                      width: `${Math.max(1.5, m.durationPercent)}%`,
                    }}
                    className={`absolute top-0 bottom-0 rounded-xs transition-all duration-300 ${barColor} ${
                      isRunning ? 'opacity-90' : 'opacity-80'
                    }`}
                  />
                );
              })}
            </div>

            <div className="flex items-center justify-between text-[10px] font-mono text-text-muted pt-0.5">
              <span>
                Compute: <span className="text-text-secondary">{summary.formattedTotalCompute}</span>
              </span>
              <span className="text-[10px] text-text-muted/80">
                Gantt span relative to run origin
              </span>
            </div>
          </div>
        )}
      </div>

      {/* Jobs List */}
      <div className="space-y-0">
        {jobs.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-48 border border-dashed border-border rounded-lg text-text-muted text-sm italic">
            No jobs registered for this run.
          </div>
        ) : (
          summary.jobMetrics.map((metric: JobTimelineMetrics, index: number) => {
            const { job } = metric;
            const isJobLive = job.status === 'running';
            const nodeClass = getStatusNodeColor(job.status);
            const statusIcon = getStatusIcon(job.status);
            const barColor = getProgressBarColor(job.status);

            return (
              <div key={job.id} className="flex gap-4">
                {/* Visual node and connecting line */}
                <div className="flex flex-col items-center w-8">
                  <div
                    className={`w-8 h-8 rounded-full border-2 flex items-center justify-center font-bold shrink-0 transition-colors ${nodeClass}`}
                    aria-hidden="true"
                  >
                    {statusIcon}
                  </div>
                  {index < summary.jobMetrics.length - 1 && (
                    <div className="w-0.5 flex-1 bg-border/30 my-1" />
                  )}
                </div>

                {/* Job Card Details */}
                <div
                  className={`flex-1 rounded-lg border p-4 mb-4 transition-colors ${
                    isJobLive
                      ? 'border-status-running/30 bg-status-running/5'
                      : 'border-border bg-surface'
                  }`}
                >
                  {/* Job Header */}
                  <div className="flex items-center justify-between gap-4 mb-2">
                    <div className="flex items-center gap-3 min-w-0">
                      <span className="font-bold text-text-primary text-sm truncate">
                        {job.job_name}
                      </span>
                      <StatusBadge status={job.status} pulse={isJobLive} />
                    </div>
                    <span className="font-mono text-xs text-text-muted bg-background px-2 py-1 rounded shrink-0">
                      <LiveDuration
                        startedAt={job.started_at}
                        finishedAt={job.finished_at}
                        isLive={isJobLive}
                      />
                    </span>
                  </div>

                  {/* Metadata Row */}
                  <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-text-muted font-mono uppercase tracking-tight">
                    <span>Job {index + 1}</span>
                    {job.started_at && job.started_at !== '0001-01-01T00:00:00Z' && (
                      <span>Started: {formatTimestamp(job.started_at)}</span>
                    )}
                    {job.finished_at && job.finished_at !== '0001-01-01T00:00:00Z' && (
                      <span>Ended: {formatTimestamp(job.finished_at)}</span>
                    )}
                  </div>

                  {/* Gantt Timeline Bar */}
                  {summary.hasValidTimeWindow && (
                    <div className="mt-3 pt-2.5 border-t border-border/50">
                      {metric.isStarted ? (
                        <div>
                          {/* Proportional track */}
                          <div
                            role="img"
                            aria-label={`${job.job_name} timeline span: ${metric.formattedDuration}, started ${metric.formattedOffset} from run origin (${metric.wallClockSharePercent}% of run duration)`}
                            className="relative h-2 w-full rounded bg-background/80 border border-border/60 overflow-hidden"
                          >
                            {/* Graduation lines */}
                            <div className="absolute left-1/4 top-0 bottom-0 w-px bg-border/30" aria-hidden="true" />
                            <div className="absolute left-1/2 top-0 bottom-0 w-px bg-border/30" aria-hidden="true" />
                            <div className="absolute left-3/4 top-0 bottom-0 w-px bg-border/30" aria-hidden="true" />

                            <div
                              aria-hidden="true"
                              style={{
                                left: `${metric.offsetPercent}%`,
                                width: `${Math.max(1.5, metric.durationPercent)}%`,
                              }}
                              className={`absolute top-0 bottom-0 rounded-xs transition-all duration-300 ${barColor}`}
                            />
                          </div>

                          {/* Track Details Subtitle */}
                          <div className="flex items-center justify-between text-[10px] font-mono text-text-muted mt-1.5 select-none">
                            <span>
                              Offset: <span className="text-text-secondary">{metric.formattedOffset}</span>
                            </span>
                            <span>
                              <span className="text-text-primary font-medium">{metric.formattedDuration}</span>
                              <span className="text-text-muted ml-1">({metric.wallClockSharePercent}% of run)</span>
                            </span>
                          </div>
                        </div>
                      ) : (
                        <div className="flex items-center justify-between text-[10px] font-mono text-text-muted py-0.5">
                          <span>Queued</span>
                          <span>Waiting for runner</span>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Jump to error action button */}
                  {job.status === 'failed' && onJumpToError && (
                    <button
                      type="button"
                      onClick={onJumpToError}
                      className="mt-3 inline-flex items-center gap-1.5 text-xs font-mono text-status-failed bg-status-failed/10 hover:bg-status-failed/20 border border-status-failed/30 px-2 py-1 rounded transition-colors focus:outline-none focus:ring-1 focus:ring-status-failed cursor-pointer"
                    >
                      <span aria-hidden="true">⚠</span>
                      <span>Jump to error in log</span>
                      <span aria-hidden="true">↓</span>
                    </button>
                  )}
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
