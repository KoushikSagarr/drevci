import fs from 'node:fs';
import path from 'node:path';

function readKeyFromFile(filePath: string, keys: string[]): string | null {
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
      if (keys.includes(key) && value) {
        return value;
      }
    }
  } catch {
    // Ignore errors reading files
  }
  return null;
}

function resolveEnvKey(envKey: string, fileKeys: string[]): string | null {
  if (process.env[envKey]) return process.env[envKey] as string;

  if (process.env.DREV_NO_ENV_FILE === 'true') {
    return null;
  }

  const candidates = [
    path.resolve(process.cwd(), '.env.local'),
    path.resolve(process.cwd(), '.env'),
    path.resolve(process.cwd(), 'dashboard/.env.local'),
    path.resolve(process.cwd(), 'dashboard/.env'),
    path.resolve(process.cwd(), '../.env.local'),
    path.resolve(process.cwd(), '../.env'),
  ];

  for (const candidate of candidates) {
    const val = readKeyFromFile(candidate, fileKeys);
    if (val) {
      process.env[envKey] = val;
      return val;
    }
  }

  return null;
}

export function getBackendToken(): string | null {
  if (process.env.DREV_TOKEN) return process.env.DREV_TOKEN;
  if (process.env.DREV_API_TOKEN) return process.env.DREV_API_TOKEN;
  if (process.env.DREV_API_TOKENS) return process.env.DREV_API_TOKENS;

  return resolveEnvKey('DREV_TOKEN', ['DREV_TOKEN', 'DREV_API_TOKEN', 'DREV_API_TOKENS']);
}

export function getDashboardPassword(): string | null {
  return resolveEnvKey('DASHBOARD_PASSWORD', ['DASHBOARD_PASSWORD']);
}

export function getDashboardSessionSecret(): string | null {
  return resolveEnvKey('DASHBOARD_SESSION_SECRET', ['DASHBOARD_SESSION_SECRET']);
}

export function getBackendUrl(): string {
  const envVal =
    process.env.DREV_BACKEND_URL ||
    process.env.DREV_API_URL ||
    process.env.NEXT_PUBLIC_API_URL;
  if (envVal) {
    return envVal.replace(/\/+$/, '');
  }

  const fileVal = resolveEnvKey('DREV_BACKEND_URL', [
    'DREV_BACKEND_URL',
    'DREV_API_URL',
    'NEXT_PUBLIC_API_URL',
  ]);
  if (fileVal) {
    return fileVal.replace(/\/+$/, '');
  }

  return 'http://localhost:9090';
}

