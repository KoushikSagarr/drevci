import { RunStatus } from '../lib/types';

export default function StatusBadge({ status, pulse }: { status: RunStatus, pulse?: boolean }) {
  const colors = {
    pending: 'bg-status-pending text-status-pending',
    running: 'bg-status-running text-status-running',
    success: 'bg-status-success text-status-success',
    failed: 'bg-status-failed text-status-failed',
    cancelled: 'bg-status-cancelled text-status-cancelled',
  };

  const bg = colors[status] || colors.pending;

  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-border bg-surface px-2.5 py-1 text-xs font-medium text-text-primary">
      <span className="relative flex h-2 w-2">
        {status === 'running' && pulse && (
          <span className={`absolute inline-flex h-full w-full animate-ping rounded-full opacity-75 ${bg.split(' ')[0]}`} />
        )}
        <span className={`relative inline-flex h-2 w-2 rounded-full ${bg.split(' ')[0]}`} />
      </span>
      {status.charAt(0).toUpperCase() + status.slice(1)}
    </span>
  );
}
