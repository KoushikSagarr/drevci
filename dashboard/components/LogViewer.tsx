'use client';

import { useEffect, useRef, useState } from 'react';
import { streamLogs } from '../lib/api';

export default function LogViewer({ runId, isLive }: { runId: string; isLive: boolean }) {
  const [lines, setLines] = useState<string[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [copied, setCopied] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let cleanup = () => {};
    if (isLive || lines.length === 0) {
      setIsConnected(true);
      cleanup = streamLogs(
        runId,
        (line) => setLines((prev) => [...prev, line]),
        () => setIsConnected(false)
      );
    } else {
      setIsConnected(false);
    }
    return () => cleanup();
  }, [runId, isLive]);

  useEffect(() => {
    if (bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [lines]);

  const copyLogs = () => {
    navigator.clipboard.writeText(lines.join('\n'));
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="flex flex-col rounded-lg border border-border overflow-hidden">
      {/* Terminal header */}
      <div className="flex items-center justify-between border-b border-border bg-surface px-4 py-2.5">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <span className="h-3 w-3 rounded-full bg-[#f85149]/70" />
            <span className="h-3 w-3 rounded-full bg-[#d29922]/70" />
            <span className="h-3 w-3 rounded-full bg-[#3fb950]/70" />
          </div>
          <span className="text-xs font-mono text-text-muted">
            {isConnected ? '● live' : '○ closed'} — {lines.length} lines
          </span>
        </div>
        <button
          onClick={copyLogs}
          className="flex items-center gap-1.5 rounded-md border border-border bg-background px-2.5 py-1 text-xs font-medium text-text-secondary transition-colors hover:bg-surface-hover hover:text-text-primary"
        >
          {copied ? '✓ Copied' : '⎘ Copy'}
        </button>
      </div>

      {/* Terminal body */}
      <div className="max-h-[500px] overflow-y-auto bg-background p-4 font-mono text-[13px] leading-5">
        {lines.length === 0 && isConnected && (
          <div className="text-text-muted animate-pulse">$ waiting for logs...</div>
        )}
        {lines.length === 0 && !isConnected && !isLive && (
          <div className="text-text-muted">No log output captured.</div>
        )}

        {lines.map((line, i) => (
          <div key={i} className="log-line flex gap-3 rounded-sm px-1 -mx-1">
            <span className="select-none text-text-muted w-8 text-right shrink-0">{i + 1}</span>
            <span className="text-[#3fb950] whitespace-pre-wrap break-all">{line}</span>
          </div>
        ))}

        {!isConnected && lines.length > 0 && !isLive && (
          <div className="mt-3 pt-3 border-t border-border text-text-muted text-center text-xs">
            ── pipeline complete ──
          </div>
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
