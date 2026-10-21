'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { getHealth } from '../lib/api';

type EngineHealth = 'loading' | 'healthy' | 'unavailable';

export default function Navbar() {
  const pathname = usePathname();
  const router = useRouter();
  const isLoginPage = pathname === '/login';

  const [healthStatus, setHealthStatus] = useState<EngineHealth>('loading');
  const [engineVersion, setEngineVersion] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;

    async function checkHealth() {
      try {
        const data = await getHealth();
        if (isMounted) {
          if (data && data.status === 'ok') {
            setHealthStatus('healthy');
            setEngineVersion(data.version || null);
          } else {
            setHealthStatus('unavailable');
          }
        }
      } catch {
        if (isMounted) {
          setHealthStatus('unavailable');
        }
      }
    }

    checkHealth();
    // Poll conservatively every 20 seconds
    const interval = setInterval(checkHealth, 20000);

    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  async function handleLogout() {
    try {
      await fetch('/api/auth/logout', { method: 'POST' });
    } finally {
      router.push('/login');
      router.refresh();
    }
  }
  
  return (
    <nav className="fixed top-0 left-0 right-0 z-50 flex h-14 items-center justify-between border-b border-border bg-surface/80 backdrop-blur-md px-6">
      <div className="flex items-center gap-6">
        <Link href="/" className="flex items-center gap-2 font-bold text-white transition-opacity hover:opacity-80">
          <svg viewBox="0 0 24 24" fill="none" className="w-4 h-4 text-[#F97316]" aria-hidden="true">
            <line x1="5" y1="6" x2="19" y2="12" stroke="#F97316" strokeWidth="2" strokeLinecap="round" />
            <line x1="5" y1="18" x2="19" y2="12" stroke="#F97316" strokeWidth="2" strokeLinecap="round" />
            <circle cx="5" cy="6" r="3" fill="#F97316" />
            <circle cx="5" cy="18" r="3" fill="#F97316" />
            <circle cx="19" cy="12" r="3" fill="#F97316" />
          </svg>
          <span className="text-base tracking-tight">Drev CI</span>
        </Link>
        <div className="h-5 w-px bg-border" />
        <div className="flex items-center gap-1">
          <Link
            href="/"
            className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
              pathname === '/'
                ? 'bg-border/50 text-text-primary'
                : 'text-text-secondary hover:text-text-primary hover:bg-border/30'
            }`}
          >
            Overview
          </Link>
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
        <div
          className="flex items-center gap-1.5 rounded-full border border-border bg-background px-2.5 py-1"
          title={
            healthStatus === 'healthy'
              ? 'Drev engine is healthy and reachable'
              : healthStatus === 'unavailable'
              ? 'Drev engine is unreachable or unhealthy'
              : 'Checking engine health...'
          }
        >
          <span
            className={`inline-block h-2 w-2 rounded-full ${
              healthStatus === 'healthy'
                ? 'bg-status-success'
                : healthStatus === 'unavailable'
                ? 'bg-status-failed'
                : 'bg-status-pending animate-pulse'
            }`}
          />
          <span className="text-xs font-medium text-text-secondary">
            {healthStatus === 'healthy'
              ? 'Online'
              : healthStatus === 'unavailable'
              ? 'Offline'
              : 'Checking...'}
          </span>
        </div>
        <span
          className="rounded-md border border-border bg-background px-2 py-1 text-xs font-mono text-text-muted"
          title="Drev Engine Version"
        >
          {engineVersion ? `v${engineVersion}` : '—'}
        </span>
        {!isLoginPage && (
          <button
            onClick={handleLogout}
            title="Sign out of dashboard"
            className="flex items-center gap-1.5 rounded-md border border-border/80 bg-background px-2.5 py-1 text-xs font-medium text-text-secondary hover:text-text-primary hover:border-border transition-colors cursor-pointer"
          >
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="w-3.5 h-3.5 text-text-muted"
            >
              <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
              <polyline points="16 17 21 12 16 7" />
              <line x1="21" y1="12" x2="9" y2="12" />
            </svg>
            <span>Sign out</span>
          </button>
        )}
      </div>
    </nav>
  );
}

