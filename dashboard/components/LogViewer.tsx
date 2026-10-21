'use client';

import { useEffect, useRef, useState } from 'react';
import { streamLogs } from '../lib/api';
import { parseAnsi, stripAnsi, classifyLogLine, LogLineKind } from '../lib/ansi';

export default function LogViewer({ runId, isLive }: { runId: string; isLive: boolean }) {
  const [lines, setLines] = useState<string[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [copied, setCopied] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);

  const containerRef = useRef<HTMLDivElement>(null);
  const lastRunIdRef = useRef<string | null>(null);
  const hasFetchedRef = useRef(false);

  // Clear lines when switching runs
  useEffect(() => {
    if (lastRunIdRef.current !== runId) {
      setLines([]);
      lastRunIdRef.current = runId;
      hasFetchedRef.current = false;
      setAutoScroll(true);
    }
  }, [runId]);

  useEffect(() => {
    let cleanup = () => {};
    let isSubscribed = true;

    if (isLive || !hasFetchedRef.current) {
      cleanup = streamLogs(
        runId,
        (line) => {
          if (isSubscribed) {
            hasFetchedRef.current = true;
            setLines((prev) => [...prev, line]);
          }
        },
        () => {
          if (isSubscribed) {
            hasFetchedRef.current = true;
            setIsConnected(false);
          }
        },
        () => {
          if (isSubscribed) {
            setIsConnected(true);
          }
        }
      );
    }

    return () => {
      isSubscribed = false;
      cleanup();
    };
  }, [runId, isLive]);

  // Handle scroll detection: pause auto-scroll if user scrolls up
  const handleScroll = () => {
    if (!containerRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = containerRef.current;
    // Considered near bottom if within 40px of bottom
    const isNearBottom = scrollHeight - scrollTop - clientHeight < 40;
    setAutoScroll(isNearBottom);
  };

  // Auto-scroll when new lines arrive if autoScroll is enabled
  useEffect(() => {
    if (autoScroll && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [lines, autoScroll]);

  const copyLogs = () => {
    const rawText = lines.map(stripAnsi).join('\n');
    navigator.clipboard.writeText(rawText);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const toggleAutoScroll = () => {
    const next = !autoScroll;
    setAutoScroll(next);
    if (next && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  };

  const renderLineContent = (rawLine: string) => {
    const spans = parseAnsi(rawLine);

    // If ANSI parsing yielded no styled spans, render classified text directly
    if (spans.length === 0 || (spans.length === 1 && !spans[0].className)) {
      const clean = stripAnsi(rawLine);
      return <span className="whitespace-pre-wrap break-all">{clean}</span>;
    }

    return (
      <span className="whitespace-pre-wrap break-all">
        {spans.map((span, idx) => (
          <span key={idx} className={span.className}>
            {span.text}
          </span>
        ))}
      </span>
    );
  };

  const getLineContainerClass = (kind: LogLineKind) => {
    switch (kind) {
      case 'step':
        return 'bg-accent/10 border-l-2 border-accent text-accent font-semibold px-2 my-1';
      case 'error':
        return 'bg-status-failed/10 border-l-2 border-status-failed text-status-failed px-2 my-0.5';
      case 'warning':
        return 'text-[#d29922] px-1';
      case 'command':
        return 'text-[#39c5cf] font-medium px-1';
      case 'drev':
        return 'text-[#58a6ff] px-1';
      case 'normal':
      default:
        return 'text-zinc-200 px-1';
    }
  };

  return (
    <div className="flex flex-col rounded-lg border border-border overflow-hidden bg-background">
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
        <div className="flex items-center gap-2">
          {/* Follow output toggle button */}
          <button
            type="button"
            onClick={toggleAutoScroll}
            aria-pressed={autoScroll}
            title={autoScroll ? 'Auto-scroll is following output (click to pause)' : 'Auto-scroll paused (click to follow bottom)'}
            className={`flex items-center gap-1 rounded-md border px-2 py-1 text-xs font-mono transition-colors ${
              autoScroll
                ? 'border-accent/40 bg-accent/10 text-accent font-semibold'
                : 'border-border bg-background text-text-muted hover:text-text-primary'
            }`}
          >
            <span>↓</span>
            <span>{autoScroll ? 'Following' : 'Follow Paused'}</span>
          </button>

          <button
            onClick={copyLogs}
            className="flex items-center gap-1.5 rounded-md border border-border bg-background px-2.5 py-1 text-xs font-medium text-text-secondary transition-colors hover:bg-surface-hover hover:text-text-primary"
          >
            {copied ? '✓ Copied' : '⎘ Copy'}
          </button>
        </div>
      </div>

      {/* Terminal body */}
      <div
        ref={containerRef}
        onScroll={handleScroll}
        className="max-h-[520px] overflow-y-auto bg-background p-4 font-mono text-[13px] leading-5 select-text"
      >
        {lines.length === 0 && isConnected && (
          <div className="text-text-muted animate-pulse">$ waiting for logs...</div>
        )}
        {lines.length === 0 && !isConnected && !isLive && (
          <div className="text-text-muted">No log output captured.</div>
        )}

        {lines.map((line, i) => {
          const cleanText = stripAnsi(line);
          const kind = classifyLogLine(cleanText);
          const lineClass = getLineContainerClass(kind);

          return (
            <div key={i} className={`log-line flex gap-3 rounded-sm -mx-1 ${lineClass}`}>
              <span className="select-none text-text-muted/60 w-8 text-right shrink-0 font-mono text-xs pt-0.5">
                {i + 1}
              </span>
              <div className="min-w-0 flex-1">
                {renderLineContent(line)}
              </div>
            </div>
          );
        })}

        {!isConnected && lines.length > 0 && !isLive && (
          <div className="mt-3 pt-3 border-t border-border/60 text-text-muted text-center text-xs">
            ── pipeline complete ──
          </div>
        )}
      </div>
    </div>
  );
}
