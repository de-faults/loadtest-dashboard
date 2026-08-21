import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { DATA_DIR } from '../config.ts';
import type {
  AppSettings, Profile, Protocol, RunConfig, RunRow, RunState, RunSummary, WindowMetrics,
} from '../shared/types.ts';

mkdirSync(DATA_DIR, { recursive: true });
const db = new DatabaseSync(join(DATA_DIR, 'loadtest.db'));

db.exec(`
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS profiles (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  protocol TEXT NOT NULL,
  config TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  profile_id TEXT,
  profile_name TEXT NOT NULL,
  protocol TEXT NOT NULL,
  target TEXT NOT NULL,
  state TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  ended_at INTEGER,
  config_snapshot TEXT NOT NULL,
  summary TEXT,
  histogram TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at DESC);

CREATE TABLE IF NOT EXISTS run_samples (
  run_id TEXT NOT NULL,
  ts INTEGER NOT NULL,
  elapsed REAL NOT NULL,
  requests INTEGER NOT NULL,
  success INTEGER NOT NULL,
  failed INTEGER NOT NULL,
  rps REAL NOT NULL,
  tps REAL NOT NULL,
  vus INTEGER NOT NULL,
  lat_min REAL, lat_avg REAL, lat_p90 REAL, lat_p95 REAL, lat_p99 REAL, lat_max REAL,
  consumer_lag INTEGER
);
CREATE INDEX IF NOT EXISTS idx_samples_run ON run_samples(run_id, ts);

CREATE TABLE IF NOT EXISTS run_logs (
  run_id TEXT NOT NULL,
  ts INTEGER NOT NULL,
  level TEXT NOT NULL,
  line TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_logs_run ON run_logs(run_id, ts);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS secrets (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  value TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
`);

// ─── Settings ────────────────────────────────────────────────────────────────

export const DEFAULT_SETTINGS: AppSettings = {
  language: 'en',
  theme: 'dark',
  csvDelimiter: ',',
  csvLanguage: 'en',
  k6Path: 'k6',
  artilleryPath: 'artillery',
  retentionRuns: 100,
  kafkaMonitorIntervalSec: 3,
};

export function getSettings(): AppSettings {
  const rows = db.prepare('SELECT key, value FROM settings').all() as Array<{ key: string; value: string }>;
  const out = { ...DEFAULT_SETTINGS } as Record<string, unknown>;
  for (const r of rows) {
    try { out[r.key] = JSON.parse(r.value); } catch { out[r.key] = r.value; }
  }
  return out as unknown as AppSettings;
}

export function saveSettings(patch: Partial<AppSettings>): AppSettings {
  const stmt = db.prepare('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value');
  for (const [k, v] of Object.entries(patch)) stmt.run(k, JSON.stringify(v));
  return getSettings();
}

// ─── Profiles ────────────────────────────────────────────────────────────────

interface ProfileRow { id: string; name: string; protocol: string; config: string; created_at: number; updated_at: number }

function toProfile(r: ProfileRow): Profile {
  return {
    id: r.id, name: r.name, protocol: r.protocol as Protocol,
    config: JSON.parse(r.config) as RunConfig,
    createdAt: r.created_at, updatedAt: r.updated_at,
  };
}

export function listProfiles(): Profile[] {
  return (db.prepare('SELECT * FROM profiles ORDER BY updated_at DESC').all() as unknown as ProfileRow[]).map(toProfile);
}

export function getProfile(id: string): Profile | null {
  const r = db.prepare('SELECT * FROM profiles WHERE id=?').get(id) as unknown as ProfileRow | undefined;
  return r ? toProfile(r) : null;
}

export function upsertProfile(p: Profile): Profile {
  db.prepare(`
    INSERT INTO profiles(id,name,protocol,config,created_at,updated_at)
    VALUES(?,?,?,?,?,?)
    ON CONFLICT(id) DO UPDATE SET name=excluded.name, protocol=excluded.protocol,
      config=excluded.config, updated_at=excluded.updated_at
  `).run(p.id, p.name, p.protocol, JSON.stringify(p.config), p.createdAt, p.updatedAt);
  return p;
}

export function deleteProfile(id: string): void {
  db.prepare('DELETE FROM profiles WHERE id=?').run(id);
}

// ─── Runs ────────────────────────────────────────────────────────────────────

interface RunDbRow {
  id: string; profile_id: string | null; profile_name: string; protocol: string; target: string;
  state: string; started_at: number; ended_at: number | null;
  config_snapshot: string; summary: string | null; histogram: string | null;
}

function toRun(r: RunDbRow): RunRow {
  return {
    id: r.id, profileId: r.profile_id, profileName: r.profile_name,
    protocol: r.protocol as Protocol, target: r.target, state: r.state as RunState,
    startedAt: r.started_at, endedAt: r.ended_at,
    summary: r.summary ? (JSON.parse(r.summary) as RunSummary) : null,
  };
}

export function createRun(args: {
  id: string; profileId: string | null; profileName: string; protocol: Protocol;
  target: string; startedAt: number; config: RunConfig;
}): void {
  db.prepare(`
    INSERT INTO runs(id,profile_id,profile_name,protocol,target,state,started_at,config_snapshot)
    VALUES(?,?,?,?,?,'running',?,?)
  `).run(args.id, args.profileId, args.profileName, args.protocol, args.target, args.startedAt,
    JSON.stringify(redactConfig(args.config)));
}

export function finishRun(id: string, state: RunState, endedAt: number, summary: RunSummary | null, histogram: string | null): void {
  db.prepare('UPDATE runs SET state=?, ended_at=?, summary=?, histogram=? WHERE id=?')
    .run(state, endedAt, summary ? JSON.stringify(summary) : null, histogram, id);
}

export function insertSample(runId: string, w: WindowMetrics): void {
  db.prepare(`
    INSERT INTO run_samples(run_id,ts,elapsed,requests,success,failed,rps,tps,vus,
      lat_min,lat_avg,lat_p90,lat_p95,lat_p99,lat_max,consumer_lag)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `).run(runId, w.ts, w.elapsed, w.requests, w.success, w.failed, w.rps, w.tps, w.vus,
    w.latency.min, w.latency.avg, w.latency.p90, w.latency.p95, w.latency.p99, w.latency.max,
    w.consumerLag ?? null);
}

export function insertLog(runId: string, ts: number, level: string, line: string): void {
  db.prepare('INSERT INTO run_logs(run_id,ts,level,line) VALUES(?,?,?,?)').run(runId, ts, level, line);
}

export function listRuns(limit = 200): RunRow[] {
  return (db.prepare('SELECT * FROM runs ORDER BY started_at DESC LIMIT ?').all(limit) as unknown as RunDbRow[]).map(toRun);
}

export function getRun(id: string): (RunRow & { config: RunConfig; histogram: string | null }) | null {
  const r = db.prepare('SELECT * FROM runs WHERE id=?').get(id) as unknown as RunDbRow | undefined;
  if (!r) return null;
  return { ...toRun(r), config: JSON.parse(r.config_snapshot) as RunConfig, histogram: r.histogram };
}

export function getSamples(runId: string): WindowMetrics[] {
  const rows = db.prepare('SELECT * FROM run_samples WHERE run_id=? ORDER BY ts').all(runId) as unknown as Array<Record<string, number | null>>;
  return rows.map((r) => ({
    ts: r.ts as number, elapsed: r.elapsed as number,
    requests: r.requests as number, success: r.success as number, failed: r.failed as number,
    rps: r.rps as number, tps: r.tps as number, vus: r.vus as number,
    latency: {
      min: (r.lat_min ?? 0) as number, avg: (r.lat_avg ?? 0) as number,
      p90: (r.lat_p90 ?? 0) as number, p95: (r.lat_p95 ?? 0) as number,
      p99: (r.lat_p99 ?? 0) as number, max: (r.lat_max ?? 0) as number,
    },
    consumerLag: r.consumer_lag == null ? undefined : (r.consumer_lag as number),
  }));
}

export function getLogs(runId: string, limit = 500): Array<{ ts: number; level: string; line: string }> {
  return db.prepare('SELECT ts,level,line FROM run_logs WHERE run_id=? ORDER BY ts DESC LIMIT ?')
    .all(runId, limit) as unknown as Array<{ ts: number; level: string; line: string }>;
}

export function deleteRun(id: string): void {
  db.prepare('DELETE FROM run_samples WHERE run_id=?').run(id);
  db.prepare('DELETE FROM run_logs WHERE run_id=?').run(id);
  db.prepare('DELETE FROM runs WHERE id=?').run(id);
}

/** Keep the newest N runs; drop the rest with their samples and logs. */
export function applyRetention(keep: number): number {
  const stale = db.prepare('SELECT id FROM runs ORDER BY started_at DESC LIMIT -1 OFFSET ?')
    .all(keep) as unknown as Array<{ id: string }>;
  for (const s of stale) deleteRun(s.id);
  return stale.length;
}

/**
 * Never persist credentials in the run snapshot — the snapshot is served to the
 * UI and written into exports.
 */
export function redactConfig(c: RunConfig): RunConfig {
  const clone = pruneInactive(structuredClone(c));
  if (clone.rest?.auth) {
    if (clone.rest.auth.password) clone.rest.auth.password = '***';
    if (clone.rest.auth.token) clone.rest.auth.token = '***';
  }
  for (const cfg of [clone.rest, clone.socket] as Array<{ headers?: Record<string, string> } | undefined>) {
    if (!cfg?.headers) continue;
    for (const k of Object.keys(cfg.headers)) {
      if (/authorization|api[-_]?key|token|secret|cookie/i.test(k)) cfg.headers[k] = '***';
    }
  }
  if (clone.kafka?.librdkafka) {
    for (const k of Object.keys(clone.kafka.librdkafka)) {
      if (/password|secret|key$/i.test(k)) clone.kafka.librdkafka[k] = '***';
    }
  }
  return clone;
}

/**
 * Drop fields the active mode does not use.
 *
 * A profile deliberately keeps all three load models populated so switching
 * between them does not lose what you typed. A run snapshot is the durable
 * record of what actually executed, so carrying the other two models' numbers
 * there only invites misreading it later.
 */
function pruneInactive(c: RunConfig): RunConfig {
  const rest = c.rest as Record<string, unknown> | undefined;
  if (rest) {
    const model = rest.loadModel;
    if (model !== 'vus') { delete rest.vus; delete rest.vusDurationSec; }
    if (model !== 'stages') delete rest.stages;
    if (model !== 'rate') { delete rest.rate; delete rest.rateDurationSec; delete rest.preAllocatedVUs; }
    const auth = rest.auth as { kind?: string } | undefined;
    if (auth?.kind === 'none') delete rest.auth;
  }

  const kafka = c.kafka as Record<string, unknown> | undefined;
  if (kafka) {
    if (kafka.payloadType === 'random') delete kafka.payload;
    else delete kafka.payloadSizeBytes;
    if (kafka.keyStrategy !== 'fixed') delete kafka.keyValue;
    if (kafka.latencyMode !== 'end-to-end' && !kafka.monitorLag) delete kafka.consumerGroup;
  }

  return c;
}

export default db;
