import type {
  AppSettings, KafkaAuth, Profile, Protocol, RunConfig, RunRow, RunSummary,
  RunnerAvailability, WindowMetrics,
} from '@shared/types.ts';

export interface MonitorStatus {
  running: boolean;
  bootstrapServers: string | null;
  intervalSec: number;
  auth: (Omit<KafkaAuth, 'password'> & { hasPassword: boolean }) | null;
}

const TOKEN_KEY = 'ltd.token';

export function setToken(token: string): void { localStorage.setItem(TOKEN_KEY, token); }
export function getToken(): string { return localStorage.getItem(TOKEN_KEY) ?? ''; }

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {};
  // Declare a JSON body only when there is one. Fastify rejects an empty body
  // sent with content-type json (FST_ERR_CTP_EMPTY_JSON_BODY -> 400), which is
  // what every bodyless DELETE and stop request used to hit.
  if (init?.body != null) headers['Content-Type'] = 'application/json';
  if (token) headers['x-dashboard-token'] = token;
  Object.assign(headers, init?.headers as Record<string, string> | undefined);

  const res = await fetch(path, { ...init, headers });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json() as { error?: unknown };
      detail = typeof body.error === 'string' ? body.error : JSON.stringify(body.error);
    } catch { /* non-JSON error body */ }
    throw new Error(detail);
  }
  return res.json() as Promise<T>;
}

export interface RunDetail extends RunRow {
  config: RunConfig;
  histogram: string | null;
  samples: WindowMetrics[];
  logs: Array<{ ts: number; level: string; line: string }>;
}

export const api = {
  health: () => req<{ ok: boolean; version: string }>('/api/health'),
  availability: () => req<RunnerAvailability>('/api/availability'),
  meta: () => req<{
    metrics: string[];
    librdkafkaHints: string[];
    headerHints: string[];
    headerValueHints: Record<string, string[]>;
  }>('/api/meta'),

  settings: () => req<AppSettings>('/api/settings'),
  saveSettings: (patch: Partial<AppSettings>) =>
    req<AppSettings>('/api/settings', { method: 'PUT', body: JSON.stringify(patch) }),

  defaults: (protocol: Protocol) => req<RunConfig>(`/api/defaults/${protocol}`),
  importScript: (body: { content: string; filename: string; protocol: Protocol | 'auto' }) =>
    req<{ protocol: Protocol; config: RunConfig; warnings: string[]; detected: boolean }>(
      '/api/import', { method: 'POST', body: JSON.stringify(body) }),

  /** Returns the generated script as a blob so the browser can save it. */
  async exportScript(config: RunConfig, name: string): Promise<{ blob: Blob; filename: string }> {
    const token = getToken();
    const res = await fetch('/api/export/script', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(token ? { 'x-dashboard-token': token } : {}) },
      body: JSON.stringify({ config, name }),
    });
    if (!res.ok) {
      let detail = res.statusText;
      try {
        const body = await res.json() as { error?: unknown };
        detail = typeof body.error === 'string' ? body.error : JSON.stringify(body.error);
      } catch { /* non-JSON error body */ }
      throw new Error(detail);
    }
    const disposition = res.headers.get('Content-Disposition') ?? '';
    const filename = /filename="([^"]+)"/.exec(disposition)?.[1] ?? 'script.txt';
    return { blob: await res.blob(), filename };
  },

  profiles: () => req<Profile[]>('/api/profiles'),
  saveProfile: (p: { id?: string; name: string; protocol: Protocol; config: RunConfig }) =>
    req<Profile>('/api/profiles', { method: 'POST', body: JSON.stringify(p) }),
  deleteProfile: (id: string) => req<{ ok: true }>(`/api/profiles/${id}`, { method: 'DELETE' }),

  runs: () => req<RunRow[]>('/api/runs'),
  activeRuns: () => req<Array<{ runId: string; protocol: Protocol; profileName: string; startedAt: number }>>('/api/runs/active'),
  run: (id: string) => req<RunDetail>(`/api/runs/${id}`),
  startRun: (body: { profileId?: string; profileIds?: string[]; profileName?: string; config?: RunConfig }) =>
    req<{ runId: string | null; target: string; queued: Array<{ queueId: string; profileName: string }> }>(
      '/api/runs', { method: 'POST', body: JSON.stringify(body) }),
  queue: () => req<Array<{ queueId: string; profileName: string; protocol: Protocol }>>('/api/runs/queue'),
  clearQueue: () => req<{ removed: number }>('/api/runs/queue', { method: 'DELETE' }),
  dequeue: (queueId: string) => req<{ ok: true }>(`/api/runs/queue/${queueId}`, { method: 'DELETE' }),
  stopRun: (id: string) => req<{ ok: true }>(`/api/runs/${id}/stop`, { method: 'POST' }),
  deleteRun: (id: string) => req<{ ok: true }>(`/api/runs/${id}`, { method: 'DELETE' }),

  kafkaMonitor: () => req<MonitorStatus>('/api/kafka/monitor'),
  startKafkaMonitor: (bootstrapServers: string, intervalSec: number, auth: KafkaAuth | null) =>
    req<MonitorStatus>('/api/kafka/monitor/start', {
      method: 'POST', body: JSON.stringify({ bootstrapServers, intervalSec, auth }),
    }),
  stopKafkaMonitor: () => req<MonitorStatus>('/api/kafka/monitor/stop', { method: 'POST' }),
  importKafkaAuth: (content: string) => req<{
    auth: KafkaAuth;
    bootstrapServers: string | null;
    format: 'json' | 'yaml' | 'properties';
    warnings: string[];
  }>('/api/kafka/monitor/auth/import', { method: 'POST', body: JSON.stringify({ content }) }),
};

/** Browser downloads must carry the token too, so build the URL with it. */
export function csvUrl(path: string, params: Record<string, string | undefined>): string {
  const url = new URL(path, window.location.origin);
  for (const [k, v] of Object.entries(params)) if (v) url.searchParams.set(k, v);
  const token = getToken();
  if (token) url.searchParams.set('token', token);
  return url.toString();
}

export type { RunSummary };
