'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

export default function Navbar() {
  const pathname = usePathname();
  
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
        <div className="flex items-center gap-1.5 rounded-full border border-border bg-background px-2.5 py-1">
          <span className="inline-block h-2 w-2 rounded-full bg-status-success" />
          <span className="text-xs font-medium text-text-secondary">Online</span>
        </div>
        <span className="rounded-md border border-border bg-background px-2 py-1 text-xs font-mono text-text-muted">
          v0.1.0
        </span>
      </div>
    </nav>
  );
}
