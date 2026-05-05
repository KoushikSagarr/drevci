'use client';

import { useEffect, useRef, useState } from 'react';
import { streamLogs } from '../lib/api';

export default function LogViewer({ runId, isLive }: { runId: string; isLive: boolean }) {
  const [lines, setLines] = useState<string[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let cleanup = () => {};

    if (isLive || lines.length === 0) {
      setIsConnected(true);
      cleanup = streamLogs(
        runId,
        (line) => {
          setLines((prev) => [...prev, line]);
        },
        () => {
          setIsConnected(false);
        }
      );
    } else {
       setIsConnected(false);
    }

    return () => {
      cleanup();
    };
  }, [runId, isLive]);

  // Auto-scroll
  useEffect(() => {
    if (bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [lines]);

  const copyLogs = () => {
    navigator.clipboard.writeText(lines.join('\n'));
  };

  return (
    <div className="relative flex flex-col rounded-lg border border-border bg-background overflow-hidden">
      <div className="flex items-center justify-between border-b border-border bg-surface px-4 py-2">
        <span className="text-xs font-mono text-text-secondary">Terminal Output</span>
        <button
          onClick={copyLogs}
          className="rounded px-2 py-1 text-xs font-medium text-text-secondary transition-colors hover:bg-border hover:text-text-primary"
        >
          Copy logs
        </button>
      </div>
      
      <div className="max-h-[600px] overflow-y-auto p-4 font-mono text-sm text-[#3fb950]">
        {lines.length === 0 && isConnected && (
          <div className="text-text-secondary animate-pulse">[waiting for logs...]</div>
        )}
        
        {lines.map((line, i) => (
          <div key={i} className="whitespace-pre-wrap break-words">{line}</div>
        ))}
        
        {!isConnected && lines.length > 0 && !isLive && (
          <div className="mt-4 text-text-secondary">── pipeline complete ──</div>
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
