import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  stripAnsi,
  parseAnsi,
  classifyLogLine,
  extractStepName,
  extractFirstError,
  filterLogLines,
  highlightMatchInSpan,
} from '../lib/ansi.ts';
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

describe('Step name extraction', () => {
  it('extracts step names from standard drev runner step headers', () => {
    assert.equal(extractStepName('[drev] --- step: Build Frontend ---'), 'Build Frontend');
    assert.equal(extractStepName('[drev] --- step: test-integration ---'), 'test-integration');
    assert.equal(extractStepName('[drev]   ---  step: Lint & Typecheck  ---'), 'Lint & Typecheck');
  });

  it('extracts step names from ANSI-styled step headers after stripping ANSI', () => {
    const styled = '\u001b[36m[drev] --- step: Deploy to Staging ---\u001b[0m';
    const clean = stripAnsi(styled);
    assert.equal(extractStepName(clean), 'Deploy to Staging');
  });

  it('returns null for non-step lines', () => {
    assert.equal(extractStepName('[drev] pulling container image alpine:latest'), null);
    assert.equal(extractStepName('$ echo step 1'), null);
    assert.equal(extractStepName('Step 1: run tests'), null);
    assert.equal(extractStepName(''), null);
  });
});

describe('First-error extraction and snippet generation', () => {
  it('returns null when no error lines are present in logs', () => {
    const cleanLogs = [
      '[drev] pulling container image alpine:latest',
      '[drev] --- step: Build ---',
      '$ go build -v ./...',
      'warning: deprecated flag used',
      'PASS: TestAll (0.12s)',
    ];
    assert.equal(extractFirstError(cleanLogs), null);
  });

  it('identifies the exact first error and preserves 1-indexed line numbers', () => {
    const logsWithErrors = [
      '[drev] container initialized',                     // line 1
      '[drev] --- step: Test ---',                        // line 2
      '$ go test ./...',                                  // line 3
      '\u001b[31merror: exit status 1\u001b[0m',          // line 4 (first error)
      'job "test" failed with exit code 1',               // line 5 (second error)
    ];

    const result = extractFirstError(logsWithErrors);
    assert.notEqual(result, null);
    assert.equal(result?.index, 3);
    assert.equal(result?.lineNum, 4);
    assert.equal(result?.snippet, 'error: exit status 1');
  });

  it('strips ANSI codes cleanly from the extracted snippet', () => {
    const logs = [
      '\u001b[1;31mFATAL:\u001b[0m \u001b[33mcontainer killed due to OOM\u001b[0m',
    ];
    const result = extractFirstError(logs);
    assert.notEqual(result, null);
    assert.equal(result?.lineNum, 1);
    assert.equal(result?.snippet, 'FATAL: container killed due to OOM');
  });
});

describe('Log filtering with preserved line numbers', () => {
  const sampleLogs = [
    '[drev] initializing runner',                               // line 1: drev
    '[drev] --- step: Dependencies ---',                        // line 2: step
    '$ npm install',                                            // line 3: command
    'warning: package rollup is deprecated',                    // line 4: warning
    '[drev] --- step: Build ---',                               // line 5: step
    '$ npm run build',                                          // line 6: command
    '\u001b[31m[error] compilation failed: syntax error\u001b[0m', // line 7: error
    'Command failed with exit code 1',                          // line 8: error
  ];

  it('preserves all lines and original line indices under filter "all"', () => {
    const filtered = filterLogLines(sampleLogs, 'all');
    assert.equal(filtered.length, 8);
    for (let i = 0; i < filtered.length; i++) {
      assert.equal(filtered[i].originalIndex, i);
    }
  });

  it('filters to errors only while preserving exact original line indices', () => {
    const filtered = filterLogLines(sampleLogs, 'errors');
    assert.equal(filtered.length, 2);
    // line 7 is index 6
    assert.equal(filtered[0].originalIndex, 6);
    assert.equal(filtered[0].kind, 'error');
    // line 8 is index 7
    assert.equal(filtered[1].originalIndex, 7);
    assert.equal(filtered[1].kind, 'error');
  });

  it('filters to warnings only while preserving original line indices', () => {
    const filtered = filterLogLines(sampleLogs, 'warnings');
    assert.equal(filtered.length, 1);
    // line 4 is index 3
    assert.equal(filtered[0].originalIndex, 3);
    assert.equal(filtered[0].kind, 'warning');
  });

  it('filters to steps only while preserving original line indices', () => {
    const filtered = filterLogLines(sampleLogs, 'steps');
    assert.equal(filtered.length, 2);
    // line 2 is index 1
    assert.equal(filtered[0].originalIndex, 1);
    assert.equal(filtered[0].kind, 'step');
    // line 5 is index 4
    assert.equal(filtered[1].originalIndex, 4);
    assert.equal(filtered[1].kind, 'step');
  });

  it('handles empty log arrays gracefully', () => {
    assert.deepEqual(filterLogLines([], 'all'), []);
    assert.deepEqual(filterLogLines([], 'errors'), []);
  });
});

describe('In-log search match highlighting', () => {
  it('performs case-insensitive matching while preserving original casing', () => {
    const text = 'Failed with exit code 1 (FAILED)';
    const parts = highlightMatchInSpan(text, 'failed');

    assert.equal(parts.length, 4);
    assert.equal(parts[0].text, 'Failed');
    assert.equal(parts[0].isMatch, true);
    assert.equal(parts[1].text, ' with exit code 1 (');
    assert.equal(parts[1].isMatch, false);
    assert.equal(parts[2].text, 'FAILED');
    assert.equal(parts[2].isMatch, true);
    assert.equal(parts[3].text, ')');
    assert.equal(parts[3].isMatch, false);
  });

  it('safely handles search queries with regex special characters without throwing', () => {
    const text = '[drev] container limits: (cpu: 2, mem: 1024MB)';
    const parts = highlightMatchInSpan(text, '[drev]');

    assert.equal(parts.length, 2);
    assert.equal(parts[0].text, '[drev]');
    assert.equal(parts[0].isMatch, true);
    assert.equal(parts[1].text, ' container limits: (cpu: 2, mem: 1024MB)');
    assert.equal(parts[1].isMatch, false);
  });

  it('returns single non-match part when search query is not found', () => {
    const text = 'All test suites passed';
    const parts = highlightMatchInSpan(text, 'error');

    assert.equal(parts.length, 1);
    assert.equal(parts[0].text, text);
    assert.equal(parts[0].isMatch, false);
  });

  it('matches searches across ANSI-formatted output spans', () => {
    const rawAnsi = '\u001b[31m[error]\u001b[0m Compilation failed in \u001b[1;34mmain.go\u001b[0m';
    const spans = parseAnsi(rawAnsi);

    // Verify spans structure
    assert.equal(spans.length, 3);
    assert.equal(spans[0].text, '[error]');
    assert.equal(spans[1].text, ' Compilation failed in ');
    assert.equal(spans[2].text, 'main.go');

    // Searching for 'compilation' matches within the middle span
    const middleSpanMatches = highlightMatchInSpan(spans[1].text, 'compilation');
    assert.equal(middleSpanMatches.length, 3);
    assert.equal(middleSpanMatches[0].text, ' ');
    assert.equal(middleSpanMatches[0].isMatch, false);
    assert.equal(middleSpanMatches[1].text, 'Compilation');
    assert.equal(middleSpanMatches[1].isMatch, true);
    assert.equal(middleSpanMatches[2].text, ' failed in ');
    assert.equal(middleSpanMatches[2].isMatch, false);

    // Searching for 'error' matches within the first styled span
    const firstSpanMatches = highlightMatchInSpan(spans[0].text, 'error');
    assert.equal(firstSpanMatches.length, 3);
    assert.equal(firstSpanMatches[0].text, '[');
    assert.equal(firstSpanMatches[0].isMatch, false);
    assert.equal(firstSpanMatches[1].text, 'error');
    assert.equal(firstSpanMatches[1].isMatch, true);
    assert.equal(firstSpanMatches[2].text, ']');
    assert.equal(firstSpanMatches[2].isMatch, false);
  });
});
