'use client';

import { useState, useEffect } from 'react';
import { formatDuration } from '../lib/duration';

export { formatDuration };

interface LiveDurationProps {
  startedAt?: string;
  finishedAt?: string;
  isLive?: boolean;
  className?: string;
}

export default function LiveDuration({
  startedAt,
  finishedAt,
  isLive = false,
  className = '',
}: LiveDurationProps) {
  const [now, setNow] = useState<number>(() => Date.now());

  useEffect(() => {
    if (!isLive) return;

    const timer = setInterval(() => {
      setNow(Date.now());
    }, 1000);

    return () => clearInterval(timer);
  }, [isLive]);

  return (
    <span className={`tabular-nums ${className}`}>
      {formatDuration(startedAt, finishedAt, now)}
    </span>
  );
}
