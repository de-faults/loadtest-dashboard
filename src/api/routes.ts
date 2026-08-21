import { randomUUID } from 'node:crypto';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { bus } from '../bus.ts';
import * as store from '../store/db.ts';
import * as csv from '../export/csv.ts';
import {
  defaultConfig, HTTP_HEADER_HINTS, HTTP_HEADER_VALUE_HINTS, LIBRDKAFKA_HINTS,
} from '../shared/defaults.ts';
import { METRICS } from '../metrics/thresholds.ts';
import { activeRuns, availability, startRun, stopRun, targetOf } from '../runners/manager.ts';
import { ingestArtilleryReport } from '../runners/artillery.runner.ts';
import { monitorStatus, startMonitor, stopMonitor } from '../kafka/monitor.ts';
import { exampleScript } from '../runners/script.ts';
import { profileSchema, runConfigSchema, settingsSchema } from './schema.ts';
import { TOKEN } from '../config.ts';
import type { Profile, Protocol, RunEvent } from '../shared/types.ts';

export function registerRoutes(app: FastifyInstance): void {
  // ─── Auth ─────────────────────────────────────────────────────────────────
  if (TOKEN) {
    app.addHook('onRequest', async (req, reply) => {
      if (req.url.startsWith('/api') || req.url === '/events') {
        const supplied = req.headers['x-dashboard-token'] ?? (req.query as { token?: string })?.token;
        if (supplied !== TOKEN) return reply.code(401).send({ error: 'unauthorized' });
      }
    });
  }

  // The artillery plugin runs as a child process on this host only.
  app.addHook('onRequest', async (req, reply) => {
    if (!req.url.startsWith('/_ingest/')) return;
    const ip = req.ip;
    if (ip !== '127.0.0.1' && ip !== '::1' && ip !== '::ffff:127.0.0.1') {
      return reply.code(403).send({ error: 'loopback only' });
    }
  });

  // ─── Meta ─────────────────────────────────────────────────────────────────
  app.get('/api/health', async () => ({ ok: true, version: '0.1.0' }));
  app.get('/api/availability', async () => availability());
  app.get('/api/meta', async () => ({
    metrics: METRICS,
    librdkafkaHints: LIBRDKAFKA_HINTS,
    headerHints: HTTP_HEADER_HINTS,
    headerValueHints: HTTP_HEADER_VALUE_HINTS,
    csvColumns: { summary: csv.SUMMARY_COLUMNS, timeseries: csv.TIMESERIES_COLUMNS },
  }));
  app.get('/api/defaults/:protocol', async (req, reply) => {
    const { protocol } = req.params as { protocol: string };
    if (!['rest', 'socket', 'kafka'].includes(protocol)) return reply.code(400).send({ error: 'unknown protocol' });
    return defaultConfig(protocol as Protocol);
  });

  app.get('/api/examples/:protocol', async (req, reply) => {
    const { protocol } = req.params as { protocol: string };
    if (!['rest', 'socket', 'kafka'].includes(protocol)) {
      return reply.code(400).send({ error: 'unknown protocol' });
    }
    const content = await exampleScript(protocol as Protocol);
    const filename = { rest: 'script.js', socket: 'script.yml', kafka: 'generator.mjs' }[protocol]!;
    return { filename, content };
  });

  // ─── Settings ─────────────────────────────────────────────────────────────
  app.get('/api/settings', async () => store.getSettings());
  app.put('/api/settings', async (req, reply) => {
    const parsed = settingsSchema.safeParse(req.body);
    if (!parsed.success) return reply.code(400).send({ error: parsed.error.issues });
    return store.saveSettings(parsed.data);
  });

  // ─── Profiles ─────────────────────────────────────────────────────────────
  app.get('/api/profiles', async () => store.listProfiles());

  app.post('/api/profiles', async (req, reply) => {
    const parsed = profileSchema.safeParse(req.body);
    if (!parsed.success) return reply.code(400).send({ error: parsed.error.issues });
    const now = Date.now();
    const existing = parsed.data.id ? store.getProfile(parsed.data.id) : null;
    const profile: Profile = {
      id: parsed.data.id ?? randomUUID(),
      name: parsed.data.name,
      protocol: parsed.data.protocol,
      config: parsed.data.config as Profile['config'],
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    return store.upsertProfile(profile);
  });

  app.get('/api/profiles/:id', async (req, reply) => {
    const p = store.getProfile((req.params as { id: string }).id);
    return p ?? reply.code(404).send({ error: 'not found' });
  });

  app.delete('/api/profiles/:id', async (req) => {
    store.deleteProfile((req.params as { id: string }).id);
    return { ok: true };
  });

  // ─── Runs ─────────────────────────────────────────────────────────────────
  app.post('/api/runs', async (req, reply) => {
    const body = req.body as { profileId?: string; profileName?: string; config?: unknown };
    let config = body.config;
    let profileName = body.profileName ?? 'ad-hoc';
    let profileId: string | null = null;

    if (body.profileId) {
      const p = store.getProfile(body.profileId);
      if (!p) return reply.code(404).send({ error: 'profile not found' });
      config = p.config;
      profileName = p.name;
      profileId = p.id;
    }

    const parsed = runConfigSchema.safeParse(config);
    if (!parsed.success) return reply.code(400).send({ error: parsed.error.issues });

    const avail = await availability();
    const cap = avail[parsed.data.protocol];
    if (!cap.available) return reply.code(409).send({ error: `runner unavailable: ${cap.detail}` });

    try {
      const runId = startRun({ config: parsed.data as never, profileId, profileName });
      return { runId, target: targetOf(parsed.data as never) };
    } catch (err) {
      return reply.code(409).send({ error: (err as Error).message });
    }
  });

  app.get('/api/runs/active', async () => activeRuns());
  app.get('/api/runs', async (req) => {
    const limit = Number((req.query as { limit?: string }).limit ?? 200);
    return store.listRuns(Math.min(Math.max(limit, 1), 1000));
  });

  app.get('/api/runs/:id', async (req, reply) => {
    const id = (req.params as { id: string }).id;
    const run = store.getRun(id);
    if (!run) return reply.code(404).send({ error: 'not found' });
    return { ...run, samples: store.getSamples(id), logs: store.getLogs(id).reverse() };
  });

  app.post('/api/runs/:id/stop', async (req, reply) => {
    const ok = stopRun((req.params as { id: string }).id);
    return ok ? { ok: true } : reply.code(404).send({ error: 'run not active' });
  });

  app.delete('/api/runs/:id', async (req) => {
    store.deleteRun((req.params as { id: string }).id);
    return { ok: true };
  });

  // ─── CSV export ───────────────────────────────────────────────────────────
  app.get('/api/runs/:id/export.csv', async (req, reply) => {
    const id = (req.params as { id: string }).id;
    const q = req.query as { type?: string; delimiter?: string; lang?: string };
    const run = store.getRun(id);
    if (!run?.summary) return reply.code(404).send({ error: 'run has no summary yet' });

    const o = csv.optionsFrom(store.getSettings(), {
      delimiter: q.delimiter,
      language: q.lang === 'th' ? 'th' : q.lang === 'en' ? 'en' : undefined,
    });

    let body: string | null;
    switch (q.type) {
      case 'timeseries': body = csv.timeseriesCsv(store.getSamples(id), o); break;
      case 'checks': body = csv.checksCsv(run.summary, o); break;
      case 'thresholds': body = csv.thresholdsCsv(run.summary, o); break;
      case 'errors': body = csv.errorsCsv(run.summary, o); break;
      case 'all': body = csv.fullReportCsv(id, o); break;
      default: body = csv.summaryCsv([run.summary], o);
    }
    if (body == null) return reply.code(404).send({ error: 'nothing to export' });

    return sendCsv(reply, body, `loadtest-${slug(run.profileName)}-${q.type ?? 'summary'}-${stamp(run.startedAt)}.csv`);
  });

  app.get('/api/export/runs.csv', async (req, reply) => {
    const q = req.query as { ids?: string; delimiter?: string; lang?: string };
    const o = csv.optionsFrom(store.getSettings(), {
      delimiter: q.delimiter,
      language: q.lang === 'th' ? 'th' : q.lang === 'en' ? 'en' : undefined,
    });
    const ids = q.ids ? q.ids.split(',').filter(Boolean) : null;
    const summaries = store.listRuns(1000)
      .filter((r) => r.summary && (!ids || ids.includes(r.id)))
      .map((r) => r.summary!);
    return sendCsv(reply, csv.summaryCsv(summaries, o), `loadtest-runs-${stamp(Date.now())}.csv`);
  });

  // ─── Kafka monitor ────────────────────────────────────────────────────────
  app.get('/api/kafka/monitor', async () => monitorStatus());
  app.post('/api/kafka/monitor/start', async (req, reply) => {
    const body = req.body as { bootstrapServers?: string; intervalSec?: number };
    if (!body?.bootstrapServers) return reply.code(400).send({ error: 'bootstrapServers required' });
    const interval = Math.min(Math.max(body.intervalSec ?? store.getSettings().kafkaMonitorIntervalSec, 1), 300);
    startMonitor(body.bootstrapServers, interval);
    return monitorStatus();
  });
  app.post('/api/kafka/monitor/stop', async () => { stopMonitor(); return monitorStatus(); });

  // ─── Artillery plugin ingest ──────────────────────────────────────────────
  app.post('/_ingest/artillery/:runId', async (req, reply) => {
    const runId = (req.params as { runId: string }).runId;
    const accepted = ingestArtilleryReport(runId, req.body as never);
    return accepted ? { ok: true } : reply.code(404).send({ error: 'run not active' });
  });

  // ─── SSE ──────────────────────────────────────────────────────────────────
  app.get('/events', (req: FastifyRequest, reply: FastifyReply) => {
    reply.raw.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    reply.raw.write('retry: 3000\n\n');

    const q = req.query as { runId?: string; lastEventId?: string };
    const lastId = Number(req.headers['last-event-id'] ?? q.lastEventId ?? 0);

    const write = (id: number, ev: RunEvent): void => {
      reply.raw.write(`id: ${id}\ndata: ${JSON.stringify(ev)}\n\n`);
    };

    // Replay so a refresh mid-run redraws the timeline instead of a blank chart.
    if (q.runId) for (const e of bus.replay(q.runId, lastId)) write(e.id, e.ev);
    const monitor = bus.latestMonitor();
    if (monitor) write(0, monitor);

    const onEvent = (id: number, ev: RunEvent): void => {
      if (q.runId && ev.t !== 'kafka-monitor' && ev.runId !== q.runId) return;
      write(id, ev);
    };
    bus.on('event', onEvent);

    const heartbeat = setInterval(() => reply.raw.write(': ping\n\n'), 15_000);
    const cleanup = (): void => { clearInterval(heartbeat); bus.off('event', onEvent); };
    req.raw.on('close', cleanup);
    req.raw.on('error', cleanup);
  });
}

function sendCsv(reply: FastifyReply, body: string, filename: string): FastifyReply {
  return reply
    .header('Content-Type', 'text/csv; charset=utf-8')
    .header('Content-Disposition', `attachment; filename="${filename}"`)
    .send(body);
}

function slug(s: string): string {
  return s.replace(/[^a-zA-Z0-9_-]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40) || 'run';
}

function stamp(ms: number): string {
  return new Date(ms).toISOString().replace(/[:.]/g, '-').slice(0, 19);
}
