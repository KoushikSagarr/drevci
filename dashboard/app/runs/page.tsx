'use client';

import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { getRuns } from '../../lib/api';
import { Run, RunStatus } from '../../lib/types';
import StatusBadge from '../../components/StatusBadge';

interface QueueStatus {
  depth: number;
  workers: number;
  status: string;
}

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

function timeAgo(dateStr: string) {
  if (!dateStr || dateStr === '0001-01-01T00:00:00Z') return '-';
  const diff = Math.floor((Date.now() - new Date(dateStr).getTime()) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function StatCard({ label, value, sub, color }: { label: string; value: string | number; sub?: string; color?: string }) {
  return (
    <div className="flex flex-col rounded-lg border border-border bg-surface p-4 min-w-0">
      <span className="text-xs font-medium text-text-muted uppercase tracking-wider mb-1">{label}</span>
      <span className={`text-2xl font-bold tabular-nums ${color || 'text-text-primary'}`}>{value}</span>
      {sub && <span className="text-xs text-text-muted mt-0.5">{sub}</span>}
    </div>
  );
}

function QueueBar({ qs }: { qs: QueueStatus | null }) {
  if (!qs) return null;
  const dotColor = qs.depth > 50 ? 'bg-status-failed' : qs.depth >= 10 ? 'bg-[#d29922]' : 'bg-status-success';
  return (
    <div className="flex items-center gap-4 border-b border-border bg-surface/50 px-6 py-2 text-xs font-mono text-text-muted">
      <span>Workers: <span className="text-text-secondary">{qs.workers}</span></span>
      <span className="text-border">│</span>
      <span>Queue: <span className="text-text-secondary">{qs.depth}</span></span>
      <span className="text-border">│</span>
      <span className="flex items-center gap-1.5">
        <span className={`inline-block h-1.5 w-1.5 rounded-full ${dotColor}`} />
        <span className="text-text-secondary">{qs.status}</span>
      </span>
    </div>
  );
}

export default function RunsPage() {
  const router = useRouter();
  const [runs, setRuns] = useState<Run[]>([]);
  const [loading, setLoading] = useState(true);
  const [queueStatus, setQueueStatus] = useState<QueueStatus | null>(null);
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState<RunStatus | 'all'>('all');
  const [, setNow] = useState(Date.now());

  const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:9090';

  const fetchRuns = async () => {
    try {
      const data = await getRuns(100);
      setRuns(data || []);
    } catch (err) { console.error(err); }
    finally { setLoading(false); }
  };

  const fetchQueue = async () => {
    try {
      const res = await fetch(`${BASE}/api/v1/queue`);
      if (res.ok) setQueueStatus(await res.json());
    } catch (_) {}
  };

  useEffect(() => {
    fetchRuns();
    fetchQueue();
    const r = setInterval(fetchRuns, 5000);
    const q = setInterval(fetchQueue, 10000);
    return () => { clearInterval(r); clearInterval(q); };
  }, []);

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);

  // --- Filtering ---
  const filteredRuns = useMemo(() => {
    return runs.filter(run => {
      const matchesSearch = run.pipeline_id.toLowerCase().includes(search.toLowerCase()) || 
                           run.id.toLowerCase().includes(search.toLowerCase());
      const matchesFilter = filter === 'all' || run.status === filter;
      return matchesSearch && matchesFilter;
    });
  }, [runs, search, filter]);

  // --- Metrics ---
  const validRuns = runs.filter(r => r.started_at && r.started_at !== '0001-01-01T00:00:00Z');
  const total = validRuns.length;
  const succeeded = validRuns.filter(r => r.status === 'success').length;
  const failed = validRuns.filter(r => r.status === 'failed').length;
  const active = validRuns.filter(r => r.status === 'running' || r.status === 'pending').length;
  const successRate = total > 0 ? Math.round((succeeded / total) * 100) : 0;

  const avgDuration = (() => {
    const completed = validRuns.filter(r => 
      r.finished_at && r.finished_at !== '0001-01-01T00:00:00Z' &&
      r.started_at && r.started_at !== '0001-01-01T00:00:00Z'
    );
    if (completed.length === 0) return '-';
    const totalMs = completed.reduce((sum, r) => {
      return sum + (new Date(r.finished_at).getTime() - new Date(r.started_at).getTime());
    }, 0);
    const avgSec = Math.max(0, Math.round(totalMs / completed.length / 1000));
    if (avgSec < 60) return `${avgSec}s`;
    return `${Math.floor(avgSec / 60)}m ${avgSec % 60}s`;
  })();

  return (
    <div className="min-h-screen bg-background">
      <QueueBar qs={queueStatus} />

      <div className="mx-auto max-w-6xl px-6 py-8">

        {/* Page Header */}
        <div className="mb-8 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-text-primary tracking-tight">Pipeline Runs</h1>
            <p className="text-sm text-text-muted mt-1">Monitor and inspect your CI/CD pipeline executions</p>
          </div>
          <button 
            disabled 
            className="inline-flex items-center gap-2 rounded-md bg-accent px-4 py-2 text-sm font-semibold text-white opacity-50 cursor-not-allowed"
            title="Manual trigger coming soon"
          >
            <span>+</span> New Run
          </button>
        </div>

        {/* Metrics Cards */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-8">
          <StatCard label="Total Runs" value={total} />
          <StatCard label="Success Rate" value={`${successRate}%`} color={successRate >= 80 ? 'text-status-success' : successRate >= 50 ? 'text-[#d29922]' : 'text-status-failed'} />
          <StatCard label="Active" value={active} color={active > 0 ? 'text-status-running' : 'text-text-primary'} sub={active > 0 ? 'pipelines running' : undefined} />
          <StatCard label="Failed" value={failed} color={failed > 0 ? 'text-status-failed' : 'text-text-primary'} />
          <StatCard label="Avg Duration" value={avgDuration} sub="per pipeline" />
        </div>

        {/* Search & Filters */}
        <div className="mb-6 flex flex-col md:flex-row items-center gap-4">
          <div className="relative flex-1 w-full">
            <span className="absolute left-3 top-1/2 -translate-y-1/2 text-text-muted opacity-50 text-lg">⌕</span>
            <input 
              type="text" 
              placeholder="Search by Run ID or Pipeline Name..." 
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full rounded-md border border-border bg-surface px-10 py-2 text-sm text-text-primary focus:border-accent focus:outline-none transition-all"
            />
          </div>
          <div className="flex items-center gap-2 w-full md:w-auto overflow-x-auto pb-2 md:pb-0">
            {(['all', 'running', 'success', 'failed', 'pending'] as const).map((s) => (
              <button
                key={s}
                onClick={() => setFilter(s)}
                className={`whitespace-nowrap rounded-md border px-3 py-1.5 text-xs font-medium transition-all ${
                  filter === s 
                    ? 'border-accent bg-accent/10 text-accent' 
                    : 'border-border bg-surface text-text-secondary hover:text-text-primary'
                }`}
              >
                {s.charAt(0).toUpperCase() + s.slice(1)}
              </button>
            ))}
          </div>
        </div>

        {/* Runs Table */}
        <div className="overflow-hidden rounded-lg border border-border bg-surface">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-border bg-background/50">
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider">Run</th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider">Pipeline</th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider">Status</th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider">Triggered</th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider">Duration</th>
                <th className="px-5 py-3 font-medium text-text-muted text-xs uppercase tracking-wider text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading && runs.length === 0 ? (
                [...Array(5)].map((_, i) => (
                  <tr key={i} className="border-b border-border">
                    <td className="px-5 py-4"><div className="skeleton h-4 w-20" /></td>
                    <td className="px-5 py-4"><div className="skeleton h-4 w-32" /></td>
                    <td className="px-5 py-4"><div className="skeleton h-5 w-20 rounded-full" /></td>
                    <td className="px-5 py-4"><div className="skeleton h-4 w-16" /></td>
                    <td className="px-5 py-4"><div className="skeleton h-4 w-14" /></td>
                    <td className="px-5 py-4"><div className="skeleton h-7 w-16 ml-auto" /></td>
                  </tr>
                ))
              ) : filteredRuns.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-5 py-16 text-center">
                    <div className="flex flex-col items-center gap-3">
                      <div className="text-4xl opacity-30">⚡</div>
                      <p className="text-text-secondary font-medium">No matching runs found</p>
                      <p className="text-text-muted text-xs">Try adjusting your search or filters</p>
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
                    <td className="px-5 py-3.5">
                      <span className="font-mono text-xs text-text-secondary group-hover:text-accent">
                        {run.id.substring(0, 8)}
                      </span>
                    </td>
                    <td className="px-5 py-3.5">
                      <span className="font-medium text-text-primary text-sm">{run.pipeline_id}</span>
                    </td>
                    <td className="px-5 py-3.5">
                      <StatusBadge status={run.status} pulse={run.status === 'running'} />
                    </td>
                    <td className="px-5 py-3.5 text-text-muted text-xs font-mono">
                      {timeAgo(run.started_at)}
                    </td>
                    <td className="px-5 py-3.5 font-mono text-xs text-text-secondary">
                      {formatDuration(run.started_at, run.finished_at)}
                    </td>
                    <td className="px-5 py-3.5 text-right" onClick={(e) => e.stopPropagation()}>
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
