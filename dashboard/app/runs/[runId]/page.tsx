'use client';

import { useEffect, useState } from 'react';
import { getRun, getRunJobs } from '../../../lib/api';
import { Run, RunJob } from '../../../lib/types';
import StatusBadge from '../../../components/StatusBadge';
import LogViewer from '../../../components/LogViewer';
import { use } from 'react';

function formatDuration(started: string, finished?: string) {
  if (!started) return '-';
  const start = new Date(started).getTime();
  const end = finished ? new Date(finished).getTime() : Date.now();
  const diff = Math.max(0, Math.floor((end - start) / 1000));
  
  const m = Math.floor(diff / 60);
  const s = diff % 60;
  return `${m}m ${s}s`;
}

export default function RunDetailPage({ params }: { params: Promise<{ runId: string }> }) {
  const { runId } = use(params);
  
  const [run, setRun] = useState<Run | null>(null);
  const [jobs, setJobs] = useState<RunJob[]>([]);
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    const fetchData = async () => {
      try {
        const r = await getRun(runId);
        setRun(r);
        const j = await getRunJobs(runId);
        setJobs(j || []);
      } catch (err) {
        console.error(err);
      }
    };

    fetchData();

    // Poll every 3 seconds if active
    const isActive = run?.status === 'pending' || run?.status === 'running';
    let interval: NodeJS.Timeout;
    if (isActive || !run) {
      interval = setInterval(fetchData, 3000);
    }

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [runId, run?.status]);

  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  if (!run) {
    return (
      <div className="mx-auto max-w-5xl p-6">
        <div className="animate-pulse h-8 w-64 bg-border rounded mb-6"></div>
        <div className="animate-pulse h-32 w-full bg-border rounded mb-6"></div>
      </div>
    );
  }

  const isLive = run.status === 'pending' || run.status === 'running';

  return (
    <div className="mx-auto max-w-5xl p-6 space-y-6">
      
      {/* Top Section */}
      <div className="rounded-lg border border-border bg-surface p-6 shadow-sm">
        <div className="mb-4 flex items-center justify-between">
          <div className="space-y-1">
            <div className="flex items-center gap-3">
              <h1 className="text-2xl font-semibold text-text-primary">
                {run.pipeline_id}
              </h1>
              <StatusBadge status={run.status} pulse={isLive} />
            </div>
            <p className="font-mono text-sm text-text-secondary">
              Run ID: {run.id}
            </p>
          </div>
          <div className="text-right text-sm text-text-secondary">
            <div>Duration: <span className="font-mono text-text-primary">{formatDuration(run.started_at, run.finished_at)}</span></div>
            <div>Started: {run.started_at ? new Date(run.started_at).toLocaleString() : '-'}</div>
            {run.finished_at && <div>Finished: {new Date(run.finished_at).toLocaleString()}</div>}
          </div>
        </div>
      </div>

      {/* Middle Section - Jobs */}
      <div className="rounded-lg border border-border bg-background shadow-sm overflow-hidden">
        <div className="border-b border-border bg-surface px-4 py-3 font-medium text-text-primary">
          Pipeline Jobs
        </div>
        <div className="divide-y divide-border">
          {jobs.length === 0 ? (
            <div className="p-4 text-center text-sm text-text-secondary">No jobs found</div>
          ) : (
            jobs.map(job => (
              <div key={job.id} className="flex items-center justify-between px-4 py-3">
                <div className="flex items-center gap-4">
                  <StatusBadge status={job.status} pulse={job.status === 'running'} />
                  <span className="font-medium text-text-primary">{job.job_name}</span>
                </div>
                <div className="font-mono text-sm text-text-secondary">
                  {formatDuration(job.started_at, job.finished_at)}
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Bottom Section - Logs */}
      <LogViewer runId={runId} isLive={isLive} />

    </div>
  );
}
