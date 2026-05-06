'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useEffect, useState } from 'react';

interface HealthStatus {
  status: string;
  version: string;
  notifications_enabled: boolean;
}

export default function Navbar() {
  const pathname = usePathname();
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:9090';

  useEffect(() => {
    const fetchHealth = async () => {
      try {
        const res = await fetch(`${BASE}/api/v1/health`);
        if (res.ok) setHealth(await res.json());
      } catch (_) {}
    };
    fetchHealth();
    const interval = setInterval(fetchHealth, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <nav className="fixed top-0 left-0 right-0 z-50 flex h-14 items-center justify-between border-b border-border bg-surface/80 backdrop-blur-md px-6">
      <div className="flex items-center gap-6">
        <Link href="/runs" className="flex items-center gap-2 font-bold text-white transition-opacity hover:opacity-80">
          <span className="text-lg">⚡</span>
          <span className="text-base tracking-tight">Drev CI</span>
        </Link>
        <div className="h-5 w-px bg-border" />
        <div className="flex items-center gap-1">
          <Link
            href="/runs"
            className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
              pathname?.startsWith('/runs')
                ? 'bg-border/50 text-text-primary'
                : 'text-text-secondary hover:text-text-primary hover:bg-border/30'
            }`}
          >
            Pipelines
          </Link>
        </div>
      </div>
      <div className="flex items-center gap-3">
        {/* Notification indicator */}
        {health && (
          <div className={`flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium ${
            health.notifications_enabled
              ? 'border-status-success/30 bg-status-success/5 text-status-success'
              : 'border-border bg-background text-text-muted'
          }`}>
            <span>{health.notifications_enabled ? '🔔' : '🔕'}</span>
            <span className="hidden sm:inline">
              {health.notifications_enabled ? 'Notifications' : 'Notifications off'}
            </span>
          </div>
        )}
        {/* Online status */}
        <div className="flex items-center gap-1.5 rounded-full border border-border bg-background px-2.5 py-1">
          <span className={`inline-block h-2 w-2 rounded-full ${health ? 'bg-status-success' : 'bg-status-failed'}`} />
          <span className="text-xs font-medium text-text-secondary">{health ? 'Online' : 'Offline'}</span>
        </div>
        <span className="rounded-md border border-border bg-background px-2 py-1 text-xs font-mono text-text-muted">
          v0.1.0
        </span>
      </div>
    </nav>
  );
}
