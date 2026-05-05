import { RunStatus } from '../lib/types';

const config: Record<RunStatus, { bg: string; text: string; label: string }> = {
  pending:   { bg: 'bg-status-pending/20', text: 'text-status-pending',   label: 'Pending' },
  running:   { bg: 'bg-status-running/20', text: 'text-status-running',   label: 'Running' },
  success:   { bg: 'bg-status-success/20', text: 'text-status-success',   label: 'Success' },
  failed:    { bg: 'bg-status-failed/20',  text: 'text-status-failed',    label: 'Failed' },
  cancelled: { bg: 'bg-status-cancelled/20', text: 'text-status-cancelled', label: 'Cancelled' },
};

const dotBg: Record<RunStatus, string> = {
  pending: 'bg-status-pending',
  running: 'bg-status-running',
  success: 'bg-status-success',
  failed: 'bg-status-failed',
  cancelled: 'bg-status-cancelled',
};

export default function StatusBadge({ status, pulse }: { status: RunStatus; pulse?: boolean }) {
  const c = config[status] || config.pending;
  const dot = dotBg[status] || dotBg.pending;

  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-semibold ${c.bg} ${c.text}`}>
      <span className="relative flex h-2 w-2">
        {status === 'running' && pulse && (
          <span className={`absolute inline-flex h-full w-full animate-ping rounded-full opacity-75 ${dot}`} />
        )}
        <span className={`relative inline-flex h-2 w-2 rounded-full ${dot}`} />
      </span>
      {c.label}
    </span>
  );
}
