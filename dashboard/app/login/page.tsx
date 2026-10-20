'use client';

import React, { useState, Suspense } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';

function LoginForm() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const rawFrom = searchParams.get('from');

  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  // Safe redirect helper
  function getSafeDestination(target: string | null): string {
    if (!target) return '/runs';
    if (!target.startsWith('/') || target.startsWith('//') || target.startsWith('/\\')) {
      return '/runs';
    }
    if (target.includes('://') || target.includes('\\')) {
      return '/runs';
    }
    const pathOnly = target.split('?')[0].split('#')[0];
    if (pathOnly === '/login') {
      return '/runs';
    }
    return target;
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!password || loading) return;

    setLoading(true);
    setErrorMessage(null);

    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ password }),
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        if (res.status === 429) {
          setErrorMessage(data.error || 'Too many login attempts. Please try again later.');
        } else if (res.status === 500) {
          setErrorMessage('Authentication service is unavailable. Verify server configuration.');
        } else {
          setErrorMessage('Invalid password. Please check your credentials.');
        }
        setLoading(false);
        return;
      }

      const destination = getSafeDestination(rawFrom);
      router.push(destination);
      router.refresh();
    } catch {
      setErrorMessage('Network error. Unable to contact authentication endpoint.');
      setLoading(false);
    }
  }

  return (
    <div className="w-full max-w-sm rounded-xl border border-border bg-surface p-8 shadow-2xl">
      <div className="mb-6 flex flex-col items-center text-center">
        <div className="mb-3 flex h-12 w-12 items-center justify-center rounded-xl border border-border bg-background shadow-inner">
          <svg viewBox="0 0 24 24" fill="none" className="h-6 w-6 text-[#F97316]" aria-hidden="true">
            <line x1="5" y1="6" x2="19" y2="12" stroke="#F97316" strokeWidth="2" strokeLinecap="round" />
            <line x1="5" y1="18" x2="19" y2="12" stroke="#F97316" strokeWidth="2" strokeLinecap="round" />
            <circle cx="5" cy="6" r="3" fill="#F97316" />
            <circle cx="5" cy="18" r="3" fill="#F97316" />
            <circle cx="19" cy="12" r="3" fill="#F97316" />
          </svg>
        </div>
        <h1 className="text-xl font-bold tracking-tight text-text-primary">Drev CI Console</h1>
        <p className="mt-1 text-xs text-text-secondary">Enter the operator password to access the dashboard</p>
      </div>

      {errorMessage && (
        <div
          role="alert"
          className="mb-4 rounded-lg border border-status-failed/40 bg-status-failed/10 px-3.5 py-2.5 text-xs font-medium text-status-failed"
        >
          {errorMessage}
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="dashboard-password" className="block text-xs font-medium text-text-secondary mb-1.5">
            Operator Password
          </label>
          <input
            id="dashboard-password"
            name="password"
            type="password"
            autoComplete="current-password"
            required
            disabled={loading}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="••••••••••••"
            className="w-full rounded-lg border border-border bg-background px-3.5 py-2 text-sm text-text-primary placeholder:text-text-muted focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent disabled:opacity-50"
          />
        </div>

        <button
          type="submit"
          disabled={loading || !password}
          className="flex w-full items-center justify-center rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-accent-hover focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent disabled:cursor-not-allowed disabled:opacity-50 transition-colors"
        >
          {loading ? (
            <span className="flex items-center gap-2">
              <svg className="h-4 w-4 animate-spin text-white" viewBox="0 0 24 24" fill="none">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" />
              </svg>
              <span>Verifying...</span>
            </span>
          ) : (
            'Sign In'
          )}
        </button>
      </form>

      <div className="mt-6 border-t border-border/50 pt-4 text-center">
        <span className="text-[11px] text-text-muted">Self-hosted session protected by HMAC-SHA256</span>
      </div>
    </div>
  );
}

export default function LoginPage() {
  return (
    <main className="flex min-h-[calc(100vh-3.5rem)] flex-col items-center justify-center px-4 py-12">
      <Suspense
        fallback={
          <div className="w-full max-w-sm rounded-xl border border-border bg-surface p-8 shadow-2xl animate-pulse">
            <div className="h-8 w-24 bg-border/40 rounded mx-auto mb-4" />
            <div className="h-4 w-48 bg-border/20 rounded mx-auto mb-6" />
            <div className="h-10 bg-border/30 rounded mb-4" />
            <div className="h-10 bg-border/30 rounded" />
          </div>
        }
      >
        <LoginForm />
      </Suspense>
    </main>
  );
}
