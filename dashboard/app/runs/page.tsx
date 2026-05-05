'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { getRuns } from '../../lib/api';
import { Run } from '../../lib/types';
import StatusBadge from '../../components/StatusBadge';

function formatDuration(started: string, finished?: string) {
  if (!started) return '-';
  const start = new Date(started).getTime();
  const end = finished ? new Date(finished).getTime() : Date.now();
  const diff = Math.max(0, Math.floor((end - start) / 1000));
  
  const m = Math.floor(diff / 60);
  const s = diff % 60;
  return `${m}m ${s}s`;
}

export default function RunsPage() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [loading, setLoading] = useState(true);
  const [now, setNow] = useState(Date.now()); // trigger re-renders for live duration

  const fetchRuns = async () => {
    try {
      const data = await getRuns(50);
      setRuns(data || []);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRuns();

    const hasActive = runs.some(r => r.status === 'pending' || r.status === 'running');
    
    // Poll every 5s if active
    let interval: NodeJS.Timeout;
    if (hasActive) {
      interval = setInterval(fetchRuns, 5000);
    }

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [runs.map(r => r.status).join(',')]); // simple dependency to recalculate active check

  // Tick every second for live durations
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  return (
    <div className="mx-auto max-w-5xl p-6">
      <div className="mb-6 flex items-center justify-between">
        <h1 className="text-2xl font-semibold text-text-primary">Pipeline Runs</h1>
      </div>

      <div className="overflow-hidden rounded-lg border border-border bg-background shadow-sm">
        <table className="w-full text-left text-sm text-text-primary">
          <thead className="bg-surface text-text-secondary border-b border-border">
            <tr>
              <th className="px-6 py-3 font-medium">Run ID</th>
              <th className="px-6 py-3 font-medium">Pipeline</th>
              <th className="px-6 py-3 font-medium">Status</th>
              <th className="px-6 py-3 font-medium">Started</th>
              <th className="px-6 py-3 font-medium">Duration</th>
              <th className="px-6 py-3 font-medium text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {loading && runs.length === 0 ? (
              [...Array(3)].map((_, i) => (
                <tr key={i} className="animate-pulse bg-background">
                  <td className="px-6 py-4"><div className="h-4 w-20 rounded bg-border"></div></td>
                  <td className="px-6 py-4"><div className="h-4 w-32 rounded bg-border"></div></td>
                  <td className="px-6 py-4"><div className="h-4 w-24 rounded bg-border"></div></td>
                  <td className="px-6 py-4"><div className="h-4 w-24 rounded bg-border"></div></td>
                  <td className="px-6 py-4"><div className="h-4 w-16 rounded bg-border"></div></td>
                  <td className="px-6 py-4"><div className="h-4 w-12 ml-auto rounded bg-border"></div></td>
                </tr>
              ))
            ) : runs.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-6 py-12 text-center text-text-secondary">
                  <div className="flex flex-col items-center justify-center">
                    <span className="mb-2 text-4xl">💻</span>
                    <p>No pipeline runs yet</p>
                  </div>
                </td>
              </tr>
            ) : (
              runs.map((run) => (
                <tr key={run.id} className="transition-colors hover:bg-surface/50">
                  <td className="px-6 py-4 font-mono text-text-secondary">
                    {run.id.substring(0, 8)}
                  </td>
                  <td className="px-6 py-4 font-medium">
                    {run.pipeline_id}
                  </td>
                  <td className="px-6 py-4">
                    <StatusBadge status={run.status} pulse={run.status === 'running'} />
                  </td>
                  <td className="px-6 py-4 text-text-secondary">
                    {run.started_at ? new Date(run.started_at).toLocaleString() : '-'}
                  </td>
                  <td className="px-6 py-4 font-mono text-text-secondary">
                    {formatDuration(run.started_at, run.finished_at)}
                  </td>
                  <td className="px-6 py-4 text-right">
                    <Link
                      href={`/runs/${run.id}`}
                      className="rounded bg-surface px-3 py-1.5 text-sm font-medium text-text-primary border border-border transition-colors hover:bg-border hover:text-white"
                    >
                      View
                    </Link>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
