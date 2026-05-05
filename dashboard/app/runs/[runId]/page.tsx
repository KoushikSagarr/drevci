'use client';

import { useEffect, useState } from 'react';
import { getRun, getRunJobs } from '../../../lib/api';
import { Run, RunJob } from '../../../lib/types';
import StatusBadge from '../../../components/StatusBadge';
import LogViewer from '../../../components/LogViewer';
import Link from 'next/link';
import { use } from 'react';

function formatDuration(started: string, finished?: string) {
  if (!started || started === '0001-01-01T00:00:00Z') return 'Pending...';
  const start = new Date(started).getTime();
  const end = (finished && finished !== '0001-01-01T00:00:00Z') ? new Date(finished).getTime() : Date.now();
  const diff = Math.max(0, Math.floor((end - start) / 1000));
  if (diff < 60) return `${diff}s`;
  const m = Math.floor(diff / 60);
  const s = diff % 60;
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function formatTimestamp(ts: string) {
  if (!ts || ts === '0001-01-01T00:00:00Z') return '-';
  const d = new Date(ts);
  return d.toLocaleString('en-US', { 
    month: 'short', day: 'numeric', 
    hour: '2-digit', minute: '2-digit', second: '2-digit', 
    hour12: false 
  });
}

// --- Detail Card ---
function DetailCard({ label, value, sub, color }: { label: string; value: string | number; sub?: string; color?: string }) {
  return (
    <div className="flex flex-col rounded-lg border border-border bg-surface p-4">
      <span className="text-[10px] font-bold text-text-muted uppercase tracking-widest mb-1">{label}</span>
      <span className={`text-lg font-bold tabular-nums ${color || 'text-text-primary'}`}>{value}</span>
      {sub && <span className="text-[10px] text-text-muted mt-0.5">{sub}</span>}
    </div>
  );
}

// --- Job Timeline Item ---
function JobItem({ job, index }: { job: RunJob; index: number }) {
  const statusIcon = {
    pending: '○',
    running: '◉',
    success: '✓',
    failed: '✗',
    cancelled: '⊘',
  }[job.status] || '○';

  const colorClass = {
    pending: 'border-status-pending text-status-pending bg-status-pending/5',
    running: 'border-status-running text-status-running bg-status-running/10 shadow-[0_0_10px_rgba(56,139,253,0.1)]',
    success: 'border-status-success text-status-success bg-status-success/5',
    failed: 'border-status-failed text-status-failed bg-status-failed/5',
    cancelled: 'border-status-cancelled text-status-cancelled bg-status-cancelled/5',
  }[job.status] || 'border-border text-text-muted bg-surface';

  return (
    <div className="flex gap-4">
      <div className="flex flex-col items-center w-8">
        <div className={`w-8 h-8 rounded-full border-2 flex items-center justify-center font-bold shrink-0 transition-all ${colorClass}`}>
          {statusIcon}
        </div>
        <div className="w-0.5 flex-1 bg-border/30 my-1" />
      </div>

      <div className={`flex-1 rounded-lg border p-4 mb-4 transition-all ${
        job.status === 'running' ? 'border-status-running/30 bg-status-running/5' : 'border-border bg-surface'
      }`}>
        <div className="flex items-center justify-between gap-4 mb-2">
          <div className="flex items-center gap-3">
            <span className="font-bold text-text-primary text-sm">{job.job_name}</span>
            <StatusBadge status={job.status} pulse={job.status === 'running'} />
          </div>
          <span className="font-mono text-xs text-text-muted bg-background px-2 py-1 rounded">
            {formatDuration(job.started_at, job.finished_at)}
          </span>
        </div>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-text-muted font-mono uppercase tracking-tight">
          <span>Step {index + 1}</span>
          {job.started_at && <span>Started: {formatTimestamp(job.started_at)}</span>}
          {job.finished_at && <span>Ended: {formatTimestamp(job.finished_at)}</span>}
        </div>
      </div>
    </div>
  );
}

export default function RunDetailPage({ params }: { params: Promise<{ runId: string }> }) {
  const { runId } = use(params);
  const [run, setRun] = useState<Run | null>(null);
  const [jobs, setJobs] = useState<RunJob[]>([]);
  const [, setNow] = useState(Date.now());

  useEffect(() => {
    const fetchData = async () => {
      try {
        const r = await getRun(runId);
        setRun(r);
        const j = await getRunJobs(runId);
        setJobs(j || []);
      } catch (err) { console.error(err); }
    };
    fetchData();
    const isActive = run?.status === 'pending' || run?.status === 'running';
    let interval: NodeJS.Timeout;
    if (isActive || !run) interval = setInterval(fetchData, 3000);
    return () => { if (interval) clearInterval(interval); };
  }, [runId, run?.status]);

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);

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
  const succeededCount = jobs.filter(j => j.status === 'success').length;
  const failedCount = jobs.filter(j => j.status === 'failed').length;
  const progress = jobs.length > 0 ? Math.round(((succeededCount + failedCount) / jobs.length) * 100) : 0;

  return (
    <div className="mx-auto max-w-6xl px-6 py-8 space-y-8">

      {/* Top Nav / Breadcrumbs */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm">
          <Link href="/runs" className="text-text-muted hover:text-accent font-medium">Pipelines</Link>
          <span className="text-text-muted">/</span>
          <span className="text-text-primary font-mono bg-surface px-2 py-0.5 rounded border border-border">{run.id.substring(0, 8)}</span>
        </div>
        <div className="flex items-center gap-3">
          <button className="text-xs font-medium text-text-muted hover:text-text-primary border border-border bg-surface px-3 py-1.5 rounded-md transition-colors">
            ↻ Restart Run
          </button>
        </div>
      </div>

      {/* Main Stats Banner */}
      <div className="rounded-xl border border-border bg-surface overflow-hidden shadow-xl">
        {isLive && (
          <div className="h-1.5 bg-background">
            <div
              className={`h-full transition-all duration-1000 ${failedCount > 0 ? 'bg-status-failed' : 'bg-status-running'}`}
              style={{ width: `${Math.max(progress, 2)}%` }}
            />
          </div>
        )}
        <div className="p-8">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-8">
            <div className="space-y-4">
              <div className="flex items-center gap-4">
                <h1 className="text-3xl font-black text-text-primary tracking-tight uppercase">{run.pipeline_id}</h1>
                <StatusBadge status={run.status} pulse={isLive} />
              </div>
              <div className="flex items-center gap-6">
                <div className="flex flex-col">
                  <span className="text-[10px] font-bold text-text-muted uppercase tracking-widest">Triggered</span>
                  <span className="text-sm font-medium text-text-secondary">{formatTimestamp(run.started_at)}</span>
                </div>
                <div className="h-8 w-px bg-border" />
                <div className="flex flex-col">
                  <span className="text-[10px] font-bold text-text-muted uppercase tracking-widest">Environment</span>
                  <span className="text-sm font-medium text-text-secondary">Production</span>
                </div>
                <div className="h-8 w-px bg-border" />
                <div className="flex flex-col">
                  <span className="text-[10px] font-bold text-text-muted uppercase tracking-widest">Branch</span>
                  <span className="text-sm font-mono text-text-secondary">main</span>
                </div>
              </div>
            </div>
            
            <div className="grid grid-cols-2 gap-4 shrink-0">
              <DetailCard label="Run Duration" value={formatDuration(run.started_at, run.finished_at)} color="text-accent" />
              <DetailCard label="Job Success" value={`${succeededCount}/${jobs.length}`} color={failedCount > 0 ? 'text-status-failed' : 'text-status-success'} sub="jobs passed" />
            </div>
          </div>
        </div>
      </div>

      {/* Content Layout */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Left: Job Timeline */}
        <div className="lg:col-span-1 space-y-4">
          <h3 className="text-xs font-bold text-text-muted uppercase tracking-widest px-1">Pipeline Execution</h3>
          <div className="space-y-0">
            {jobs.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-48 border border-dashed border-border rounded-lg text-text-muted text-sm italic">
                No jobs registered for this run.
              </div>
            ) : (
              jobs.map((job, i) => (
                <JobItem key={job.id} job={job} index={i} />
              ))
            )}
          </div>
        </div>

        {/* Right: Build Output / Logs */}
        <div className="lg:col-span-2 space-y-4">
          <h3 className="text-xs font-bold text-text-muted uppercase tracking-widest px-1">Console Output</h3>
          <LogViewer runId={runId} isLive={isLive} />
          
          <div className="rounded-lg border border-border bg-surface/30 p-4">
            <h4 className="text-[10px] font-bold text-text-muted uppercase tracking-widest mb-2">Technical Details</h4>
            <div className="font-mono text-[11px] text-text-secondary space-y-1">
              <div className="flex justify-between"><span>Runner Version</span> <span>Drev-Runner v0.1.0-alpha</span></div>
              <div className="flex justify-between"><span>Execution Node</span> <span>Local-Daemon-Win64</span></div>
              <div className="flex justify-between"><span>Storage Engine</span> <span>SQLite v3.x (WAL-mode)</span></div>
            </div>
          </div>
        </div>
      </div>

    </div>
  );
}
