import fs from 'node:fs';
import path from 'node:path';

function readTokenFromFile(filePath: string): string | null {
  try {
    if (!fs.existsSync(filePath)) return null;
    const content = fs.readFileSync(filePath, 'utf-8');
    const lines = content.split('\n');
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const eqIdx = trimmed.indexOf('=');
      if (eqIdx === -1) continue;
      const key = trimmed.slice(0, eqIdx).trim();
      let value = trimmed.slice(eqIdx + 1).trim();
      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }
      if (key === 'DREV_TOKEN' || key === 'DREV_API_TOKEN' || key === 'DREV_API_TOKENS') {
        if (value) return value;
      }
    }
  } catch {
    // Ignore errors reading files
  }
  return null;
}

export function getBackendToken(): string | null {
  if (process.env.DREV_TOKEN) return process.env.DREV_TOKEN;
  if (process.env.DREV_API_TOKEN) return process.env.DREV_API_TOKEN;
  if (process.env.DREV_API_TOKENS) return process.env.DREV_API_TOKENS;

  if (process.env.DREV_NO_ENV_FILE === 'true') {
    return null;
  }

  // Search candidate paths for .env
  const candidates = [
    path.resolve(process.cwd(), '.env.local'),
    path.resolve(process.cwd(), '.env'),
    path.resolve(process.cwd(), '../.env.local'),
    path.resolve(process.cwd(), '../.env'),
  ];

  for (const candidate of candidates) {
    const token = readTokenFromFile(candidate);
    if (token) return token;
  }

  return null;
}

export function getBackendUrl(): string {
  const url =
    process.env.DREV_BACKEND_URL ||
    process.env.DREV_API_URL ||
    process.env.NEXT_PUBLIC_API_URL ||
    'http://localhost:9090';
  return url.replace(/\/+$/, '');
}
