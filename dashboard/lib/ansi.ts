/**
 * ANSI escape code parsing and log line classification utilities.
 */

// Matches all ANSI escape sequences (CSI, OSC, etc.)
const ANSI_REGEX = /[\u001B\u009B][[\]()#;?]*(?:(?:(?:[a-zA-Z\d]*(?:;[-a-zA-Z\d\/#&.:=?%@~_]*)*)?\u0007)|(?:(?:\d{1,4}(?:;\d{0,4})*)?[A-PR-TZcf-nqr-uy=><~]))/g;

// Matches specifically SGR sequences: ESC [ <params> m
const SGR_REGEX = /\u001B\[([0-9;]*)m/g;

/**
 * Strips all ANSI escape sequences from a string.
 */
export function stripAnsi(str: string): string {
  if (!str) return '';
  return str.replace(ANSI_REGEX, '');
}

export interface AnsiSpan {
  text: string;
  className?: string;
  style?: Record<string, string>;
}

const SGR_COLOR_MAP: Record<number, string> = {
  30: 'text-zinc-500',
  31: 'text-[#f85149]',       // red
  32: 'text-[#3fb950]',       // green
  33: 'text-[#d29922]',       // yellow
  34: 'text-[#58a6ff]',       // blue
  35: 'text-[#bc8cff]',       // magenta / purple
  36: 'text-[#39c5cf]',       // cyan
  37: 'text-zinc-200',        // white
  90: 'text-zinc-400',        // bright black / gray
  91: 'text-[#ff7b72]',       // bright red
  92: 'text-[#56d364]',       // bright green
  93: 'text-[#e3b341]',       // bright yellow
  94: 'text-[#79c0ff]',       // bright blue
  95: 'text-[#d2a8ff]',       // bright magenta
  96: 'text-[#56e3ec]',       // bright cyan
  97: 'text-white',           // bright white
};

/**
 * Parses a string containing ANSI SGR color/style sequences into structured styled spans.
 * Strips any non-SGR ANSI sequences first.
 */
export function parseAnsi(raw: string): AnsiSpan[] {
  if (!raw) return [];

  // Remove non-SGR sequences first
  const cleanOfNonSgr = raw.replace(ANSI_REGEX, (match) => {
    return match.endsWith('m') ? match : '';
  });

  const spans: AnsiSpan[] = [];
  let lastIndex = 0;
  let currentColorClass: string | undefined;
  let isBold = false;
  let isDim = false;

  SGR_REGEX.lastIndex = 0;
  let match: RegExpExecArray | null;

  while ((match = SGR_REGEX.exec(cleanOfNonSgr)) !== null) {
    const textBefore = cleanOfNonSgr.slice(lastIndex, match.index);
    if (textBefore) {
      spans.push(createSpan(textBefore, currentColorClass, isBold, isDim));
    }

    const paramsStr = match[1];
    if (!paramsStr || paramsStr === '' || paramsStr === '0') {
      currentColorClass = undefined;
      isBold = false;
      isDim = false;
    } else {
      const codes = paramsStr.split(';').map((s) => parseInt(s, 10));
      for (const code of codes) {
        if (code === 0) {
          currentColorClass = undefined;
          isBold = false;
          isDim = false;
        } else if (code === 1) {
          isBold = true;
        } else if (code === 2) {
          isDim = true;
        } else if (code === 22) {
          isBold = false;
          isDim = false;
        } else if (code === 39) {
          currentColorClass = undefined;
        } else if (SGR_COLOR_MAP[code]) {
          currentColorClass = SGR_COLOR_MAP[code];
        }
      }
    }

    lastIndex = SGR_REGEX.lastIndex;
  }

  const remaining = cleanOfNonSgr.slice(lastIndex);
  if (remaining) {
    spans.push(createSpan(remaining, currentColorClass, isBold, isDim));
  }

  return spans;
}

function createSpan(
  text: string,
  colorClass?: string,
  bold?: boolean,
  dim?: boolean
): AnsiSpan {
  const classes: string[] = [];
  if (colorClass) classes.push(colorClass);
  if (bold) classes.push('font-bold');
  if (dim) classes.push('opacity-60');

  return {
    text,
    className: classes.length > 0 ? classes.join(' ') : undefined,
  };
}

export type LogLineKind = 'step' | 'error' | 'warning' | 'command' | 'drev' | 'normal';

/**
 * Classifies a log line for semantic highlighting in the terminal log viewer.
 */
export function classifyLogLine(cleanText: string): LogLineKind {
  const trimmed = cleanText.trim();
  if (!trimmed) return 'normal';

  // Drev step boundary marker
  if (/^\[drev\]\s+---\s+step:.*---$/i.test(trimmed)) {
    return 'step';
  }

  // Clear failure signals (exit codes, fatal errors, stack markers)
  if (
    /\bfailed with exit code\b/i.test(trimmed) ||
    /\b(fatal|panic|error):/i.test(trimmed) ||
    /^(error|fatal|panic)[\s:]/i.test(trimmed) ||
    /^\[error\]/i.test(trimmed) ||
    /\bFAIL:\s+/i.test(trimmed) ||
    /\bCommand failed with exit code\b/i.test(trimmed)
  ) {
    return 'error';
  }

  // Warnings
  if (
    /\b(warning|warn):/i.test(trimmed) ||
    /^\[warn(ing)?\]/i.test(trimmed) ||
    /\bWARN:\s+/i.test(trimmed)
  ) {
    return 'warning';
  }

  // Shell / command prompts
  if (trimmed.startsWith('$ ') || trimmed.startsWith('+ ') || trimmed.startsWith('> ')) {
    return 'command';
  }

  // Drev system runner notes (container limits, image pull, etc.)
  if (trimmed.startsWith('[drev]')) {
    return 'drev';
  }

  return 'normal';
}

/**
 * Extracts the step name from a step log line.
 * Matches lines formatted as `[drev] --- step: <name> ---`.
 */
export function extractStepName(cleanText: string): string | null {
  const match = cleanText.trim().match(/^\[drev\]\s+---\s+step:\s*(.*?)\s*---$/i);
  return match ? match[1] : null;
}

/**
 * Finds the first line classified as an error in an array of raw log lines.
 * Returns original 0-indexed index, 1-indexed lineNum, and stripped snippet.
 */
export function extractFirstError(lines: string[]): { index: number; lineNum: number; snippet: string } | null {
  for (let i = 0; i < lines.length; i++) {
    const clean = stripAnsi(lines[i]).trim();
    if (classifyLogLine(clean) === 'error') {
      return {
        index: i,
        lineNum: i + 1,
        snippet: clean,
      };
    }
  }
  return null;
}

export type LogFilterMode = 'all' | 'errors' | 'warnings' | 'steps';

export interface ClassifiedLogLine {
  line: string;
  originalIndex: number;
  kind: LogLineKind;
  cleanText: string;
}

/**
 * Filters an array of raw log lines by severity / step filter, preserving original line indices.
 */
export function filterLogLines(lines: string[], filter: LogFilterMode): ClassifiedLogLine[] {
  const result: ClassifiedLogLine[] = [];
  for (let i = 0; i < lines.length; i++) {
    const cleanText = stripAnsi(lines[i]);
    const kind = classifyLogLine(cleanText);
    if (
      filter === 'all' ||
      (filter === 'errors' && kind === 'error') ||
      (filter === 'warnings' && kind === 'warning') ||
      (filter === 'steps' && kind === 'step')
    ) {
      result.push({ line: lines[i], originalIndex: i, kind, cleanText });
    }
  }
  return result;
}

/**
 * Splits text into matching and non-matching segments for case-insensitive substring search.
 */
export function highlightMatchInSpan(text: string, query: string): { text: string; isMatch: boolean }[] {
  if (!query || !text) return [{ text, isMatch: false }];
  const lowerQuery = query.toLowerCase();
  const lowerText = text.toLowerCase();
  const parts: { text: string; isMatch: boolean }[] = [];
  let start = 0;
  let matchIdx = lowerText.indexOf(lowerQuery, start);
  while (matchIdx !== -1) {
    if (matchIdx > start) {
      parts.push({ text: text.slice(start, matchIdx), isMatch: false });
    }
    parts.push({ text: text.slice(matchIdx, matchIdx + query.length), isMatch: true });
    start = matchIdx + query.length;
    matchIdx = lowerText.indexOf(lowerQuery, start);
  }
  if (start < text.length) {
    parts.push({ text: text.slice(start), isMatch: false });
  }
  return parts;
}
