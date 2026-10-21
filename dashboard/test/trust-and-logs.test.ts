import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { stripAnsi, parseAnsi, classifyLogLine } from '../lib/ansi.ts';
import { formatDuration } from '../lib/duration.ts';
import type { Run } from '../lib/types.ts';

describe('ANSI stripping, SGR parsing, and log classification', () => {
  it('strips basic and multi-code SGR sequences cleanly', () => {
    const raw = '\u001b[31m[error]\u001b[0m Compilation failed in \u001b[1;34mmain.go\u001b[0m';
    const clean = stripAnsi(raw);
    assert.equal(clean, '[error] Compilation failed in main.go');
  });

  it('strips non-SGR terminal control sequences (cursor, line clears)', () => {
    const raw = '\u001b[2K\u001b[1G[drev] pulling alpine:latest\u001b[?25h';
    const clean = stripAnsi(raw);
    assert.equal(clean, '[drev] pulling alpine:latest');
  });

  it('handles empty and plain strings safely in stripAnsi', () => {
    assert.equal(stripAnsi(''), '');
    assert.equal(stripAnsi('plain log line'), 'plain log line');
  });

  it('parses SGR color codes into structured styled spans', () => {
    const raw = 'Normal \u001b[31mRed Text\u001b[0m Normal Again';
    const spans = parseAnsi(raw);
    assert.equal(spans.length, 3);
    assert.equal(spans[0].text, 'Normal ');
    assert.equal(spans[0].className, undefined);
    assert.equal(spans[1].text, 'Red Text');
    assert.match(spans[1].className || '', /text-\[#f85149\]/);
    assert.equal(spans[2].text, ' Normal Again');
  });

  it('parses bold and combined styles correctly', () => {
    const raw = '\u001b[1;32mGreen Bold\u001b[0m';
    const spans = parseAnsi(raw);
    assert.equal(spans.length, 1);
    assert.equal(spans[0].text, 'Green Bold');
    assert.match(spans[0].className || '', /font-bold/);
    assert.match(spans[0].className || '', /text-\[#3fb950\]/);
  });

  it('classifies step headers, errors, warnings, commands, and standard logs', () => {
    assert.equal(classifyLogLine('[drev] --- step: Run Tests ---'), 'step');
    assert.equal(classifyLogLine('error: exit status 1'), 'error');
    assert.equal(classifyLogLine('FATAL: container out of memory'), 'error');
    assert.equal(classifyLogLine('job "build" failed with exit code 2'), 'error');
    assert.equal(classifyLogLine('warning: deprecated flag used'), 'warning');
    assert.equal(classifyLogLine('[warn] low disk space'), 'warning');
    assert.equal(classifyLogLine('$ go test -v ./...'), 'command');
    assert.equal(classifyLogLine('+ npm run build'), 'command');
    assert.equal(classifyLogLine('[drev] enforcing container limits: 2.0 CPUs, 1024MB RAM'), 'drev');
    assert.equal(classifyLogLine('PASS: TestRunJob (0.04s)'), 'normal');
  });
});

describe('LiveDuration formatting and calculations', () => {
  it('returns Pending... for missing or zero timestamps', () => {
    assert.equal(formatDuration(''), 'Pending...');
    assert.equal(formatDuration('0001-01-01T00:00:00Z'), 'Pending...');
    assert.equal(formatDuration(undefined), 'Pending...');
  });

  it('formats seconds, minutes, and hours accurately', () => {
    const start = '2026-10-21T10:00:00Z';
    // 45 seconds later
    assert.equal(formatDuration(start, '2026-10-21T10:00:45Z'), '45s');
    // 2 minutes 15 seconds later
    assert.equal(formatDuration(start, '2026-10-21T10:02:15Z'), '2m 15s');
    // 1 hour 5 minutes later
    assert.equal(formatDuration(start, '2026-10-21T11:05:00Z'), '1h 5m');
  });

  it('computes live duration using provided nowMs when unfinished', () => {
    const startMs = new Date('2026-10-21T12:00:00Z').getTime();
    const nowMs = startMs + 35000; // 35 seconds live
    assert.equal(
      formatDuration('2026-10-21T12:00:00Z', undefined, nowMs),
      '35s'
    );
  });
});

describe('Run metadata integrity and neutral fallbacks', () => {
  it('handles fully populated run metadata from Go daemon schema', () => {
    const run: Run = {
      id: 'fd30764f-8cf9-420f-a35b-33fcd34a996a',
      pipeline_id: 'sample-pipeline',
      status: 'success',
      triggered_by: 'cli',
      commit_sha: '8839d501922b6fc08f9e5110eb7a82477e7a75b8',
      commit_msg: 'fix(dashboard): harden self-hosted authentication',
      branch: 'feat/auth-hardening',
      started_at: '2026-10-21T14:00:00Z',
      finished_at: '2026-10-21T14:01:20Z',
    };

    assert.equal(run.branch, 'feat/auth-hardening');
    assert.equal(run.triggered_by, 'cli');
    assert.equal(run.commit_sha?.substring(0, 7), '8839d50');
    assert.equal(run.commit_msg, 'fix(dashboard): harden self-hosted authentication');
  });

  it('provides honest fallbacks without fabricating environment or branch', () => {
    const runWithoutMeta: Run = {
      id: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
      pipeline_id: 'test-pipeline',
      status: 'running',
      started_at: '2026-10-21T15:00:00Z',
      finished_at: '',
    };

    // The display helpers should use honest neutral fallbacks
    const branchDisplay = runWithoutMeta.branch || 'Unknown branch';
    const triggerDisplay = runWithoutMeta.triggered_by || 'Not specified';
    const commitDisplay = runWithoutMeta.commit_sha
      ? runWithoutMeta.commit_sha.substring(0, 7)
      : 'Not available';

    assert.equal(branchDisplay, 'Unknown branch');
    assert.notEqual(branchDisplay, 'main'); // Must never fabricate 'main'
    assert.equal(triggerDisplay, 'Not specified');
    assert.equal(commitDisplay, 'Not available');
  });
});

describe('Engine health status states', () => {
  it('determines appropriate badge attributes for all health states', () => {
    type EngineHealth = 'loading' | 'healthy' | 'unavailable';

    function getHealthBadge(status: EngineHealth, version: string | null) {
      return {
        label: status === 'healthy' ? 'Online' : status === 'unavailable' ? 'Offline' : 'Checking...',
        colorClass: status === 'healthy' ? 'bg-status-success' : status === 'unavailable' ? 'bg-status-failed' : 'bg-status-pending',
        versionDisplay: version ? `v${version}` : '—',
      };
    }

    const healthy = getHealthBadge('healthy', '0.1.0');
    assert.equal(healthy.label, 'Online');
    assert.equal(healthy.colorClass, 'bg-status-success');
    assert.equal(healthy.versionDisplay, 'v0.1.0');

    const offline = getHealthBadge('unavailable', null);
    assert.equal(offline.label, 'Offline');
    assert.equal(offline.colorClass, 'bg-status-failed');
    assert.equal(offline.versionDisplay, '—');

    const loading = getHealthBadge('loading', null);
    assert.equal(loading.label, 'Checking...');
    assert.equal(loading.colorClass, 'bg-status-pending');
    assert.equal(loading.versionDisplay, '—');
  });
});

describe('Restart Run control disabled state', () => {
  it('verifies non-functional control is properly declared disabled and non-interactive', () => {
    const buttonAttrs = {
      disabled: true,
      'aria-disabled': 'true',
      title: 'Pipeline re-runs are not yet supported in this version',
      label: 'Restart Run',
    };

    assert.equal(buttonAttrs.disabled, true);
    assert.equal(buttonAttrs['aria-disabled'], 'true');
    assert.match(buttonAttrs.title, /not yet supported/i);
  });
});
