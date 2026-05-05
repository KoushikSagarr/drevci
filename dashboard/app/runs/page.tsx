'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { getRuns } from '../../lib/api';
import { Run } from '../../lib/types';
import StatusBadge from '../../components/StatusBadge';

interface QueueStatus {
  depth: number;
  workers: number;
  status: string;
}

function formatDuration(started: string, finished?: string) {
  if (!started) return '-';
  const start = new Date(started).getTime();
  const end = finished ? new Date(finished).getTime() : Date.now();
  const diff = Math.max(0, Math.floor((end - start) / 1000));
  const m = Math.floor(diff / 60);
  const s = diff % 60;
  return `${m}m ${s}s`;
}

function QueueBar({ qs }: { qs: QueueStatus | null }) {
  if (!qs) return null;

  const dotColor =
    qs.depth > 50
      ? 'bg-[#f85149]'
      : qs.depth >= 10
      ? 'bg-[#d29922]'
      : 'bg-[#3fb950]';

  return (
    <div
      className="flex items-center gap-4 border-b px-6 py-2 text-xs font-mono"
      style={{ backgroundColor: '#0d1117', borderColor: '#30363d', color: '#8b949e' }}
    >
      <span>Workers: <span style={{ color: '#e6edf3' }}>{qs.workers}</span></span>
      <span style={{ color: '#30363d' }}>|</span>
      <span>Queue depth: <span style={{ color: '#e6edf3' }}>{qs.depth}</span></span>
      <span style={{ color: '#30363d' }}>|</span>
      <span className="flex items-center gap-1.5">
        <span className={`inline-block h-2 w-2 rounded-full ${dotColor}`} />
        <span style={{ color: '#e6edf3' }}>Healthy</span>
      </span>
    </div>
  );
}

export default function RunsPage() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [loading, setLoading] = useState(true);
  const [queueStatus, setQueueStatus] = useState<QueueStatus | null>(null);
  const [now, setNow] = useState(Date.now());

  const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:9090';

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

  const fetchQueue = async () => {
    try {
      const res = await fetch(`${BASE}/api/v1/queue`);
      if (res.ok) {
        const data = await res.json();
        setQueueStatus(data);
      }
    } catch (_) {}
  };

  useEffect(() => {
    fetchRuns();
    fetchQueue();

    const runInterval = setInterval(fetchRuns, 5000);
    const queueInterval = setInterval(fetchQueue, 10000);

    return () => {
      clearInterval(runInterval);
      clearInterval(queueInterval);
    };
  }, []);

  // Tick every second for live durations
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  return (
    <div style={{ backgroundColor: '#0d1117', minHeight: '100vh' }}>
      <QueueBar qs={queueStatus} />
      <div className="mx-auto max-w-5xl p-6">
        <div className="mb-6 flex items-center justify-between">
          <h1 className="text-2xl font-semibold" style={{ color: '#e6edf3' }}>Pipeline Runs</h1>
        </div>

        <div
          className="overflow-hidden rounded-lg shadow-sm"
          style={{ border: '1px solid #30363d', backgroundColor: '#0d1117' }}
        >
          <table className="w-full text-left text-sm" style={{ color: '#e6edf3' }}>
            <thead style={{ backgroundColor: '#161b22', borderBottom: '1px solid #30363d' }}>
              <tr>
                <th className="px-6 py-3 font-medium" style={{ color: '#8b949e' }}>Run ID</th>
                <th className="px-6 py-3 font-medium" style={{ color: '#8b949e' }}>Pipeline</th>
                <th className="px-6 py-3 font-medium" style={{ color: '#8b949e' }}>Status</th>
                <th className="px-6 py-3 font-medium" style={{ color: '#8b949e' }}>Started</th>
                <th className="px-6 py-3 font-medium" style={{ color: '#8b949e' }}>Duration</th>
                <th className="px-6 py-3 font-medium text-right" style={{ color: '#8b949e' }}>Actions</th>
              </tr>
            </thead>
            <tbody style={{ borderTop: '1px solid #30363d' }}>
              {loading && runs.length === 0 ? (
                [...Array(3)].map((_, i) => (
                  <tr key={i} className="animate-pulse">
                    {[...Array(6)].map((_, j) => (
                      <td key={j} className="px-6 py-4">
                        <div className="h-4 rounded" style={{ backgroundColor: '#30363d', width: j === 5 ? '48px' : '80px', marginLeft: j === 5 ? 'auto' : undefined }} />
                      </td>
                    ))}
                  </tr>
                ))
              ) : runs.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-6 py-12 text-center" style={{ color: '#8b949e' }}>
                    <div className="flex flex-col items-center justify-center gap-2">
                      <span className="text-4xl">💻</span>
                      <p>No pipeline runs yet</p>
                    </div>
                  </td>
                </tr>
              ) : (
                runs.map((run) => (
                  <tr
                    key={run.id}
                    style={{ borderTop: '1px solid #30363d' }}
                    className="transition-colors hover:bg-[#161b22]"
                  >
                    <td className="px-6 py-4 font-mono" style={{ color: '#8b949e' }}>
                      {run.id.substring(0, 8)}
                    </td>
                    <td className="px-6 py-4 font-medium">{run.pipeline_id}</td>
                    <td className="px-6 py-4">
                      <StatusBadge status={run.status} pulse={run.status === 'running'} />
                    </td>
                    <td className="px-6 py-4" style={{ color: '#8b949e' }}>
                      {run.started_at ? new Date(run.started_at).toLocaleString() : '-'}
                    </td>
                    <td className="px-6 py-4 font-mono" style={{ color: '#8b949e' }}>
                      {formatDuration(run.started_at, run.finished_at)}
                    </td>
                    <td className="px-6 py-4 text-right">
                      <Link
                        href={`/runs/${run.id}`}
                        className="rounded px-3 py-1.5 text-sm font-medium transition-colors"
                        style={{
                          backgroundColor: '#161b22',
                          border: '1px solid #30363d',
                          color: '#e6edf3',
                        }}
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
    </div>
  );
}
