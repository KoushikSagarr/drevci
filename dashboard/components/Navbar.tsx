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

  const navItems = [
    { href: '/', label: 'Overview', isActive: pathname === '/' },
    { href: '/runs', label: 'Pipelines', isActive: pathname?.startsWith('/runs') ?? false },
  ];

  return (
    <nav className="fixed top-0 left-0 right-0 z-50 flex h-14 items-center justify-between border-b border-border bg-surface/80 backdrop-blur-xl navbar-glow px-3 sm:px-6">
      {/* Left section: Brand + Navigation */}
      <div className="flex items-center gap-2.5 sm:gap-6">
        {/* Brand mark */}
        <Link
          href="/"
          className="group flex items-center gap-2.5 font-bold text-white transition-opacity hover:opacity-90 rounded-lg focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-accent"
        >
          <div className="relative flex h-7 w-7 items-center justify-center rounded-lg bg-accent/10 border border-accent/20 transition-colors group-hover:bg-accent/20">
            <svg viewBox="0 0 24 24" fill="none" className="w-3.5 h-3.5" aria-hidden="true">
              <line x1="5" y1="6" x2="19" y2="12" stroke="#F97316" strokeWidth="2" strokeLinecap="round" />
              <line x1="5" y1="18" x2="19" y2="12" stroke="#F97316" strokeWidth="2" strokeLinecap="round" />
              <circle cx="5" cy="6" r="3" fill="#F97316" />
              <circle cx="5" cy="18" r="3" fill="#F97316" />
              <circle cx="19" cy="12" r="3" fill="#F97316" />
            </svg>
          </div>
          <span className="text-sm font-semibold tracking-tight">Drev CI</span>
        </Link>

        <div className="h-4 w-px bg-border/60 hidden sm:block" />

        {/* Navigation tabs */}
        <div className="flex items-center gap-0.5">
          {navItems.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className={`relative rounded-md px-2.5 sm:px-3 py-1 sm:py-1.5 text-xs sm:text-[13px] font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-accent ${
                item.isActive
                  ? 'text-text-primary'
                  : 'text-text-muted hover:text-text-secondary'
              }`}
            >
              {item.label}
              {/* Active indicator underline */}
              {item.isActive && (
                <span className="absolute -bottom-[13px] sm:-bottom-[11px] left-1 right-1 h-[2px] rounded-full bg-accent" />
              )}
            </Link>
          ))}
        </div>
      </div>

      {/* Right section: Status indicators + Actions */}
      <div className="flex items-center gap-1.5 sm:gap-2.5">
        {/* Health status pill */}
        <div
          role="status"
          aria-live="polite"
          className="flex items-center gap-1.5 rounded-full border border-border/60 bg-background/60 px-2 sm:px-2.5 py-1 backdrop-blur-sm"
          title={
            healthStatus === 'healthy'
              ? 'Drev engine is healthy and reachable'
              : healthStatus === 'unavailable'
              ? 'Drev engine is unreachable or unhealthy'
              : 'Checking engine health...'
          }
        >
          <span className="relative flex h-2 w-2">
            <span
              className={`inline-flex h-2 w-2 rounded-full ${
                healthStatus === 'healthy'
                  ? 'bg-status-success'
                  : healthStatus === 'unavailable'
                  ? 'bg-status-failed'
                  : 'bg-status-pending'
              }`}
            />
          </span>
          <span className="text-[11px] font-medium text-text-secondary">
            {healthStatus === 'healthy'
              ? 'Online'
              : healthStatus === 'unavailable'
              ? 'Offline'
              : 'Checking…'}
          </span>
        </div>

        {/* Engine version (compacted on narrow screens) */}
        <span
          className="hidden sm:inline-block rounded-md border border-border/50 bg-background/40 px-2 py-1 text-[11px] font-mono text-text-secondary"
          title="Drev Engine Version"
        >
          {engineVersion ? `v${engineVersion}` : '—'}
        </span>

        {/* Logout button */}
        {!isLoginPage && (
          <button
            onClick={handleLogout}
            title="Sign out of dashboard"
            aria-label="Sign out of dashboard"
            className="flex items-center gap-1.5 rounded-md border border-border/60 bg-background/40 px-2 sm:px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:text-text-primary hover:border-border hover:bg-surface-hover transition-colors cursor-pointer focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-accent"
          >
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="w-3 h-3"
              aria-hidden="true"
            >
              <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
              <polyline points="16 17 21 12 16 7" />
              <line x1="21" y1="12" x2="9" y2="12" />
            </svg>
            <span className="hidden sm:inline">Sign out</span>
          </button>
        )}
      </div>
    </nav>
  );
}
