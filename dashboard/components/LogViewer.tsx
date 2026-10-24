'use client';

import { useEffect, useRef, useState, useMemo, useImperativeHandle } from 'react';
import { streamLogs } from '../lib/api';
import {
  parseAnsi,
  stripAnsi,
  classifyLogLine,
  extractStepName,
  extractFirstError,
  filterLogLines,
  highlightMatchInSpan,
  LogLineKind,
  LogFilterMode,
} from '../lib/ansi';

export interface LogViewerRef {
  jumpToError: () => void;
  jumpToLine: (lineNum: number) => void;
}

export interface LogViewerProps {
  runId: string;
  isLive: boolean;
  onFirstErrorDetected?: (error: { lineNum: number; snippet: string } | null) => void;
  ref?: React.Ref<LogViewerRef>;
}

export default function LogViewer({
  runId,
  isLive,
  onFirstErrorDetected,
  ref,
}: LogViewerProps) {
  const [lines, setLines] = useState<string[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [copied, setCopied] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);

  // Search & Filter state
  const [searchQuery, setSearchQuery] = useState('');
  const [activeMatchIdx, setActiveMatchIdx] = useState(0);
  const [logFilter, setLogFilter] = useState<LogFilterMode>('all');
  const [targetHighlightLine, setTargetHighlightLine] = useState<number | null>(null);

  const containerRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const lastRunIdRef = useRef<string | null>(null);
  const hasFetchedRef = useRef(false);
  const highlightTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Clear lines and reset state when switching runs
  useEffect(() => {
    if (lastRunIdRef.current !== runId) {
      setLines([]);
      lastRunIdRef.current = runId;
      hasFetchedRef.current = false;
      setAutoScroll(true);
      setSearchQuery('');
      setActiveMatchIdx(0);
      setLogFilter('all');
      setTargetHighlightLine(null);
    }
  }, [runId]);

  // SSE log streaming
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

  // Error, warning, and step counts across all raw lines
  const { errorCount, warningCount, stepCount } = useMemo(() => {
    let errors = 0;
    let warnings = 0;
    let steps = 0;
    for (let i = 0; i < lines.length; i++) {
      const kind = classifyLogLine(stripAnsi(lines[i]));
      if (kind === 'error') errors++;
      else if (kind === 'warning') warnings++;
      else if (kind === 'step') steps++;
    }
    return { errorCount: errors, warningCount: warnings, stepCount: steps };
  }, [lines]);

  // First error extraction
  const firstError = useMemo(() => {
    return extractFirstError(lines);
  }, [lines]);

  // Notify parent of first detected error safely
  const onFirstErrorDetectedRef = useRef(onFirstErrorDetected);
  useEffect(() => {
    onFirstErrorDetectedRef.current = onFirstErrorDetected;
  });

  useEffect(() => {
    if (onFirstErrorDetectedRef.current) {
      onFirstErrorDetectedRef.current(
        firstError ? { lineNum: firstError.lineNum, snippet: firstError.snippet } : null
      );
    }
  }, [firstError]);

  // Filtered lines based on current severity/step filter
  const filteredLines = useMemo(() => {
    return filterLogLines(lines, logFilter);
  }, [lines, logFilter]);

  // Lines matching search query
  const searchMatches = useMemo(() => {
    if (!searchQuery.trim()) return [];
    const query = searchQuery.toLowerCase();
    return filteredLines.filter((item) =>
      item.cleanText.toLowerCase().includes(query)
    );
  }, [filteredLines, searchQuery]);

  // Handle scroll detection: pause auto-scroll if user scrolls up
  const handleScroll = () => {
    if (!containerRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = containerRef.current;
    // Considered near bottom if within 40px of bottom
    const isNearBottom = scrollHeight - scrollTop - clientHeight < 40;
    setAutoScroll(isNearBottom);
  };

  // Auto-scroll when new lines arrive if autoScroll is enabled and no search query active
  useEffect(() => {
    if (autoScroll && !searchQuery.trim() && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [lines, autoScroll, searchQuery]);

  // Jump to specific line number (1-indexed)
  const jumpToLine = (lineNum: number) => {
    // If line is hidden under current filter, switch to 'all'
    if (logFilter !== 'all') {
      const isVisibleInFilter = filteredLines.some(
        (item) => item.originalIndex + 1 === lineNum
      );
      if (!isVisibleInFilter) {
        setLogFilter('all');
      }
    }

    setAutoScroll(false);
    setTargetHighlightLine(lineNum);

    if (highlightTimeoutRef.current) {
      clearTimeout(highlightTimeoutRef.current);
    }
    highlightTimeoutRef.current = setTimeout(() => {
      setTargetHighlightLine(null);
    }, 2500);

    // Scroll element into view with reduced-motion respect
    setTimeout(() => {
      const el = document.getElementById(`log-line-${lineNum}`);
      if (el) {
        const prefersReducedMotion =
          typeof window !== 'undefined' &&
          window.matchMedia('(prefers-reduced-motion: reduce)').matches;
        el.scrollIntoView({
          behavior: prefersReducedMotion ? 'auto' : 'smooth',
          block: 'center',
        });
      }
    }, 40);
  };

  const jumpToFirstError = () => {
    if (firstError) {
      jumpToLine(firstError.lineNum);
    }
  };

  // Expose methods via ref
  useImperativeHandle(ref, () => ({
    jumpToError: jumpToFirstError,
    jumpToLine,
  }));

  // Match navigation
  const nextMatch = () => {
    if (searchMatches.length === 0) return;
    const nextIdx = (activeMatchIdx + 1) % searchMatches.length;
    setActiveMatchIdx(nextIdx);
    const target = searchMatches[nextIdx];
    jumpToLine(target.originalIndex + 1);
  };

  const prevMatch = () => {
    if (searchMatches.length === 0) return;
    const prevIdx = (activeMatchIdx - 1 + searchMatches.length) % searchMatches.length;
    setActiveMatchIdx(prevIdx);
    const target = searchMatches[prevIdx];
    jumpToLine(target.originalIndex + 1);
  };

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

  // Render line content with ANSI spans and search match highlights
  const renderLineContent = (rawLine: string, query: string) => {
    const spans = parseAnsi(rawLine);
    const trimmedQuery = query.trim();

    // Plain text rendering without SGR ANSI spans
    if (spans.length === 0 || (spans.length === 1 && !spans[0].className)) {
      const clean = stripAnsi(rawLine);
      if (!trimmedQuery) {
        return <span className="whitespace-pre-wrap break-all">{clean}</span>;
      }
      const parts = highlightMatchInSpan(clean, trimmedQuery);
      return (
        <span className="whitespace-pre-wrap break-all">
          {parts.map((p, idx) =>
            p.isMatch ? (
              <mark
                key={idx}
                className="bg-accent/40 text-white font-bold rounded-xs px-0.5"
              >
                {p.text}
              </mark>
            ) : (
              <span key={idx}>{p.text}</span>
            )
          )}
        </span>
      );
    }

    // Rich ANSI spans with embedded search match highlights
    return (
      <span className="whitespace-pre-wrap break-all">
        {spans.map((span, spanIdx) => {
          if (!trimmedQuery) {
            return (
              <span key={spanIdx} className={span.className}>
                {span.text}
              </span>
            );
          }
          const parts = highlightMatchInSpan(span.text, trimmedQuery);
          return (
            <span key={spanIdx} className={span.className}>
              {parts.map((p, partIdx) =>
                p.isMatch ? (
                  <mark
                    key={partIdx}
                    className="bg-accent/40 text-white font-bold rounded-xs px-0.5"
                  >
                    {p.text}
                  </mark>
                ) : (
                  <span key={partIdx}>{p.text}</span>
                )
              )}
            </span>
          );
        })}
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
      {/* Primary Terminal Header */}
      <div className="flex flex-wrap items-center justify-between border-b border-border bg-surface px-4 py-2.5 gap-2">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5" aria-hidden="true">
            <span className="h-3 w-3 rounded-full bg-[#f85149]/70" />
            <span className="h-3 w-3 rounded-full bg-[#d29922]/70" />
            <span className="h-3 w-3 rounded-full bg-[#3fb950]/70" />
          </div>
          <span className="text-xs font-mono text-text-muted">
            {isConnected ? '● live' : '○ closed'} — {lines.length} lines
          </span>

          {errorCount > 0 && (
            <span className="rounded-full bg-status-failed/15 border border-status-failed/30 px-2 py-0.5 text-[11px] font-mono text-status-failed font-semibold">
              {errorCount === 1 ? '1 error' : `${errorCount} errors`}
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          {/* Jump to first error button */}
          {firstError && (
            <button
              type="button"
              onClick={jumpToFirstError}
              title={`Jump to first error at line ${firstError.lineNum}`}
              className="flex items-center gap-1 rounded-md border border-status-failed/40 bg-status-failed/15 px-2.5 py-1 text-xs font-mono text-status-failed hover:bg-status-failed/25 transition-colors cursor-pointer focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-status-failed font-semibold"
            >
              <span aria-hidden="true">⚠</span>
              <span>Jump to Error</span>
            </button>
          )}

          {/* Follow output toggle button */}
          <button
            type="button"
            onClick={toggleAutoScroll}
            aria-pressed={autoScroll}
            title={
              autoScroll
                ? 'Auto-scroll is following output (click to pause)'
                : 'Auto-scroll paused (click to follow bottom)'
            }
            className={`flex items-center gap-1 rounded-md border px-2 py-1 text-xs font-mono transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-accent cursor-pointer ${
              autoScroll
                ? 'border-accent/40 bg-accent/10 text-accent font-semibold'
                : 'border-border bg-background text-text-muted hover:text-text-primary'
            }`}
          >
            <span aria-hidden="true">↓</span>
            <span>{autoScroll ? 'Following' : 'Follow Paused'}</span>
          </button>

          {/* Copy all logs button */}
          <button
            type="button"
            onClick={copyLogs}
            title="Copy full raw logs to clipboard"
            className="flex items-center gap-1.5 rounded-md border border-border bg-background px-2.5 py-1 text-xs font-medium text-text-secondary transition-colors hover:bg-surface-hover hover:text-text-primary cursor-pointer focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-accent"
          >
            <span aria-hidden="true">{copied ? '✓' : '⎘'}</span>
            <span>{copied ? 'Copied' : 'Copy'}</span>
          </button>
        </div>
      </div>

      {/* Secondary Toolbar: Log Filters + In-Log Search */}
      <div className="flex flex-wrap items-center justify-between border-b border-border/80 bg-surface/50 px-4 py-2 gap-2 text-xs">
        {/* Severity / Step Filter Buttons */}
        <div className="flex items-center gap-1" role="tablist" aria-label="Log filter modes">
          {(['all', 'errors', 'warnings', 'steps'] as const).map((mode) => {
            const label = {
              all: 'All',
              errors: 'Errors',
              warnings: 'Warnings',
              steps: 'Steps',
            }[mode];
            const count = {
              all: lines.length,
              errors: errorCount,
              warnings: warningCount,
              steps: stepCount,
            }[mode];
            const isActive = logFilter === mode;

            return (
              <button
                key={mode}
                type="button"
                role="tab"
                aria-selected={isActive}
                onClick={() => {
                  setLogFilter(mode);
                  setActiveMatchIdx(0);
                }}
                className={`flex items-center gap-1 rounded-md px-2 py-1 text-xs font-mono transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-accent cursor-pointer ${
                  isActive
                    ? 'bg-accent/15 border border-accent/40 text-accent font-semibold'
                    : 'border border-transparent text-text-muted hover:text-text-secondary hover:bg-surface-hover'
                }`}
              >
                <span>{label}</span>
                <span
                  className={`text-[10px] px-1.5 rounded-full ${
                    isActive
                      ? 'bg-accent/20 text-accent'
                      : 'bg-background/80 text-text-muted'
                  }`}
                >
                  {count}
                </span>
              </button>
            );
          })}
        </div>

        {/* Search input with match counter & match navigation */}
        <div className="relative flex items-center">
          <span
            className="absolute left-2.5 text-text-muted select-none text-xs pointer-events-none"
            aria-hidden="true"
          >
            ⌕
          </span>
          <input
            ref={searchInputRef}
            type="text"
            value={searchQuery}
            onChange={(e) => {
              setSearchQuery(e.target.value);
              setActiveMatchIdx(0);
            }}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                if (e.shiftKey) prevMatch();
                else nextMatch();
              } else if (e.key === 'Escape') {
                setSearchQuery('');
              }
            }}
            placeholder="Search in logs..."
            aria-label="Search logs"
            className="rounded-md border border-border bg-background pl-7 pr-20 py-1 text-xs font-mono text-text-primary placeholder:text-text-muted focus-visible:outline-none focus-visible:border-accent w-48 sm:w-60 transition-colors"
          />
          {searchQuery && (
            <div className="absolute right-1.5 flex items-center gap-1 text-[10px] font-mono text-text-muted">
              <span className="select-none px-1 text-text-secondary">
                {searchMatches.length > 0
                  ? `${activeMatchIdx + 1}/${searchMatches.length}`
                  : '0/0'}
              </span>
              <button
                type="button"
                onClick={prevMatch}
                title="Previous match (Shift+Enter)"
                aria-label="Previous match"
                disabled={searchMatches.length === 0}
                className="hover:text-text-primary px-0.5 cursor-pointer disabled:opacity-30 disabled:cursor-not-allowed"
              >
                ↑
              </button>
              <button
                type="button"
                onClick={nextMatch}
                title="Next match (Enter)"
                aria-label="Next match"
                disabled={searchMatches.length === 0}
                className="hover:text-text-primary px-0.5 cursor-pointer disabled:opacity-30 disabled:cursor-not-allowed"
              >
                ↓
              </button>
              <button
                type="button"
                onClick={() => setSearchQuery('')}
                title="Clear search (Escape)"
                aria-label="Clear search"
                className="hover:text-text-primary px-0.5 cursor-pointer"
              >
                ✕
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Terminal Body */}
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

        {lines.length > 0 && filteredLines.length === 0 && (
          <div className="text-text-muted text-xs py-4 text-center">
            No lines match filter &quot;{logFilter}&quot;.
          </div>
        )}

        {filteredLines.map((item) => {
          const lineNum = item.originalIndex + 1;
          const isTargetLine = targetHighlightLine === lineNum;
          const stepName = item.kind === 'step' ? extractStepName(item.cleanText) : null;

          // Visually distinct Step boundary divider
          if (stepName) {
            return (
              <div
                key={item.originalIndex}
                id={`log-line-${lineNum}`}
                className={`log-line flex items-center gap-3 my-2 px-3 py-1.5 rounded-md border border-accent/30 bg-accent/5 font-mono text-xs transition-all ${
                  isTargetLine ? 'ring-2 ring-status-failed bg-status-failed/20' : ''
                }`}
              >
                <span className="select-none text-accent/60 w-8 text-right shrink-0 font-mono text-xs">
                  {lineNum}
                </span>
                <div className="flex items-center gap-2 min-w-0 flex-1 text-accent font-semibold truncate">
                  <span className="text-accent/80 select-none" aria-hidden="true">▸</span>
                  <span className="text-[10px] uppercase font-bold tracking-wider text-text-muted">
                    Step
                  </span>
                  <span className="text-accent truncate">{stepName}</span>
                </div>
              </div>
            );
          }

          const lineClass = getLineContainerClass(item.kind);

          return (
            <div
              key={item.originalIndex}
              id={`log-line-${lineNum}`}
              className={`log-line flex gap-3 rounded-sm -mx-1 transition-colors ${lineClass} ${
                isTargetLine ? 'ring-2 ring-status-failed bg-status-failed/20' : ''
              }`}
            >
              <span className="select-none text-text-muted/60 w-8 text-right shrink-0 font-mono text-xs pt-0.5">
                {lineNum}
              </span>
              <div className="min-w-0 flex-1">
                {renderLineContent(item.line, searchQuery)}
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
