import { spawn } from 'node:child_process';
import { mkdtemp, rm, writeFile, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parse as yamlParse, stringify as yamlStringify } from 'yaml';
import { getSettings } from '../store/db.ts';
import { PORT } from '../config.ts';
import { probe } from './k6.runner.ts';
import { materializeScript, usesCustomScript } from './script.ts';
import type { Runner, RunnerContext, RunnerResult } from './types.ts';
import type { SocketConfig, SocketFlowStep } from '../shared/types.ts';

const HERE = dirname(fileURLToPath(import.meta.url));
const PLUGIN_DIR = join(HERE, 'assets');
const SOCKETIO_GUARD = join(PLUGIN_DIR, 'artillery-socketio-guard.mjs');

/** Interim stats posted by artillery-plugin-ltd-publish. */
export interface ArtilleryReport {
  counters?: Record<string, number>;
  rates?: Record<string, number>;
  summaries?: Record<string, Record<string, number>>;
  period?: number | string;
  firstMetricAt?: number;
  lastMetricAt?: number;
}

/**
 * Registry of live runs waiting on plugin POSTs, keyed by runId.
 *
 * `vus` tracks concurrency, which artillery does not report directly: with
 * LEGACY_METRICS_FORMAT=false its counters are per-period, so concurrent
 * virtual users are the running total of created minus finished.
 */
interface IngestState {
  ctx: RunnerContext;
  created: number;
  finished: number;
  warnedLatency?: boolean;
  /** True once a genuine round-trip metric has been reported. */
  sawRtt?: boolean;
}
const ingest = new Map<string, IngestState>();

export function ingestArtilleryReport(runId: string, body: { kind: string; report: ArtilleryReport }): boolean {
  const state = ingest.get(runId);
  if (!state) return false;
  // The `done` payload is the cumulative aggregate for the whole run. Folding
  // it in on top of the per-period `stats` ticks would double every counter.
  if (body.kind === 'done') return true;
  applyReport(state, body.report);
  return true;
}

export const artilleryRunner: Runner = {
  protocol: 'socket',

  async available() {
    return probe(getSettings().artilleryPath, ['--version']);
  },

  async run(ctx: RunnerContext): Promise<RunnerResult> {
    const cfg = ctx.config.socket;
    if (!cfg) throw new Error('socket config missing');

    const dir = await mkdtemp(join(tmpdir(), 'ltd-art-'));
    const scriptPath = join(dir, 'script.yml');
    const outPath = join(dir, 'report.json');

    // yaml.stringify, never string templating — user input must not be able to
    // break out of a scalar and inject YAML structure.
    let runScript = scriptPath;
    // Total scripted phase time, used by the stall watchdog below. Zero means
    // "unknown", and an unknown-length script is never killed.
    let phasesSec = 0;
    let scenarioBudgetSec = 0;
    if (usesCustomScript(ctx.config.script)) {
      const userScript = await materializeScript(ctx.config.script, dir, 'script.yml');
      // Re-emit their YAML with our live-stats plugin merged in, so custom
      // scripts still stream to the charts instead of only reporting at the end.
      const merged = await injectPlugin(userScript, ctx.runId);
      if (merged) {
        await writeFile(scriptPath, merged.yaml, 'utf8');
        phasesSec = merged.phasesSec;
        scenarioBudgetSec = merged.scenarioSec;
        ctx.log('info', `custom artillery script: ${userScript}`);
      } else {
        runScript = userScript;
        ctx.log('warn', `custom artillery script could not be parsed as YAML — running as-is, live metrics unavailable until it finishes`);
      }
    } else {
      await writeFile(scriptPath, yamlStringify(buildScript(cfg, ctx.runId)), 'utf8');
      phasesSec = cfg.phases.reduce((sum, p) => sum + (p.durationSec || 0), 0);
      scenarioBudgetSec = flowBudgetSec(
        cfg.flow.map((step) => ({
          think: step.kind === 'think' ? Number(step.value) || 0 : 0,
          waits: step.kind === 'expect' || (step.kind === 'emit' && step.acknowledge === true),
        })),
      );
    }
    const state: IngestState = { ctx, created: 0, finished: 0 };
    ingest.set(ctx.runId, state);

    const bin = getSettings().artilleryPath;
    const args = ['run', '--output', outPath, runScript];
    ctx.log('info', `artillery ${args.join(' ')}`);

    const child = spawn(bin, args, {
      shell: false,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        ARTILLERY_PLUGIN_PATH: PLUGIN_DIR,
        ARTILLERY_DISABLE_TELEMETRY: 'true',
        // Inherited by artillery's worker processes, which is where the engine
        // actually runs — see the guard's own comment for what it fixes.
        NODE_OPTIONS: `${process.env.NODE_OPTIONS ?? ''} --import "${pathToFileURL(SOCKETIO_GUARD).href}"`.trim(),
        ...(bin.includes('/') ? { LTD_ARTILLERY_MAIN: bin } : {}),
      },
    });

    const onAbort = () => { child.kill('SIGINT'); };
    ctx.signal.addEventListener('abort', onAbort, { once: true });

    // A dead worker takes its virtual users and the final report with it, so
    // the run must say that plainly rather than reporting a short test.
    let workerDied = false;
    const watchLine = (line: string): void => {
      if (workerDied || !/^worker error|Callback was already called/.test(line)) return;
      workerDied = true;
      ctx.error('artillery-worker', line.trim());
      ctx.log('error',
        'an artillery worker crashed — the virtual users it was running stopped early. '
        + 'This usually follows a connection dropped by the target mid-scenario.');
    };

    child.stdout.on('data', (b: Buffer) => splitLines(b).forEach((l) => { ctx.log('info', l); watchLine(l); }));
    child.stderr.on('data', (b: Buffer) => splitLines(b).forEach((l) => { ctx.log('warn', l); watchLine(l); }));

    /**
     * Stall watchdog.
     *
     * A Socket.IO `emit` that waits for an acknowledgement has no timeout in
     * artillery's engine: if the ack never arrives — the target dropped the
     * connection, or simply never answered — that virtual user's scenario never
     * ends and artillery waits for it forever. Without this, a 30-second test
     * sits "running" indefinitely and writes no report.
     */
    let stalled = false;
    let exited = false;
    // Virtual users created in the final second still have their whole scenario
    // to run — think time included — so that budget is added to the grace period
    // rather than being mistaken for a stall.
    const graceSec = Math.max(60, Math.round(phasesSec * 0.25)) + scenarioBudgetSec;
    const watchdog = phasesSec > 0
      ? setTimeout(() => {
          if (exited) return;
          stalled = true;
          ctx.error('artillery-stalled', `no exit ${graceSec}s after the last phase ended`);
          ctx.log('error',
            `artillery is still running ${graceSec}s after its ${phasesSec}s of phases ended and is being stopped. `
            + 'A virtual user is most likely waiting on an acknowledgement that never arrived.');
          child.kill('SIGINT');
          setTimeout(() => { if (!exited) child.kill('SIGKILL'); }, 10_000).unref();
        }, (phasesSec + graceSec) * 1000)
      : null;
    watchdog?.unref();

    const exitCode = await new Promise<number | null>((resolve) => {
      child.on('close', (code) => { exited = true; resolve(code); });
      child.on('error', (err) => { exited = true; ctx.error('spawn', `${bin}: ${err.message}`); resolve(null); });
    });

    if (watchdog) clearTimeout(watchdog);
    ctx.signal.removeEventListener('abort', onAbort);
    ingest.delete(ctx.runId);

    // Final report is authoritative if the plugin never reached us.
    try {
      const raw = JSON.parse(await readFile(outPath, 'utf8')) as { aggregate?: ArtilleryReport };
      // Only used when the plugin never reached us; otherwise it would double-count.
      if (raw.aggregate && ctx.agg.totalRequests === 0) applyReport(state, raw.aggregate);
    } catch {
      // Say what it means for the numbers, not just that a file was missing.
      ctx.log('warn', stalled
        ? 'artillery wrote no final report because it had to be stopped — the figures shown come from the metrics streamed while it ran'
        : ctx.agg.totalRequests > 0
          ? 'artillery wrote no final report — the figures shown come from the metrics streamed while it ran'
          : 'artillery wrote no final report and streamed no metrics — this run has no results');
    }

    await rm(dir, { recursive: true, force: true });
    return { nativeVerdict: exitCode === 0 ? 'pass' : exitCode === null ? undefined : 'fail' };
  },
};

/**
 * Merge the dashboard's output plugin into a user-supplied Artillery script.
 * Returns null when the file is not parseable YAML, so the caller can fall back
 * to running it untouched rather than failing the run.
 */
async function injectPlugin(
  file: string, runId: string,
): Promise<{ yaml: string; phasesSec: number; scenarioSec: number } | null> {
  try {
    const doc = yamlParse(await readFile(file, 'utf8')) as Record<string, unknown> | null;
    if (!doc || typeof doc !== 'object') return null;
    const config = (doc.config ??= {}) as Record<string, unknown>;
    const plugins = (config.plugins ??= {}) as Record<string, unknown>;
    plugins['ltd-publish'] = { url: `http://127.0.0.1:${PORT}/_ingest/artillery/${runId}`, runId };
    const phases = Array.isArray(config.phases) ? config.phases as Array<Record<string, unknown>> : [];
    const phasesSec = phases.reduce((sum, ph) => sum + (Number(ph.duration) || 0), 0);

    const scenarios = Array.isArray(doc.scenarios) ? doc.scenarios as Array<Record<string, unknown>> : [];
    const scenarioSec = Math.max(0, ...scenarios.map((sc) => {
      const flow = Array.isArray(sc.flow) ? sc.flow as Array<Record<string, unknown>> : [];
      return flowBudgetSec(flow.map((step) => ({
        think: Number(step.think) || 0,
        waits: 'response' in step || Boolean((step.emit as { acknowledge?: unknown } | undefined)?.acknowledge)
          || 'acknowledge' in step,
      })));
    }));
    return { yaml: yamlStringify(doc), phasesSec, scenarioSec };
  } catch {
    return null;
  }
}

/**
 * Longest a single virtual user may legitimately take: its think time plus the
 * engine's own 10s timeout for every step that waits on the server.
 */
function flowBudgetSec(steps: Array<{ think: number; waits: boolean }>): number {
  const think = steps.reduce((sum, s) => sum + s.think, 0);
  const waits = steps.filter((s) => s.waits).length * 10;
  return Math.min(think + waits, 3600);
}

function splitLines(b: Buffer): string[] {
  return b.toString('utf8').split('\n').map((s) => s.trimEnd()).filter(Boolean);
}

/**
 * Fold one artillery stats tick into the aggregator.
 *
 * Artillery reports quantiles, not raw samples, so latency goes through the
 * piecewise reconstruction path.
 */
function applyReport(state: IngestState, report: ArtilleryReport): void {
  const ctx = state.ctx;
  const c = report.counters ?? {};
  const s = report.summaries ?? {};

  const sent = c['socketio.emit'] ?? c['websocket.messages_sent'] ?? c['http.requests'] ?? c['vusers.created'] ?? 0;
  const failed = (c['vusers.failed'] ?? 0) + (c['websocket.send_errors'] ?? 0);

  state.created += c['vusers.created'] ?? 0;
  state.finished += (c['vusers.completed'] ?? 0) + (c['vusers.failed'] ?? 0);
  ctx.agg.setVus(Math.max(0, state.created - state.finished));

  // Prefer true round-trip time when the flow measured it. `vusers.session_length`
  // is the fallback and measures something else — whole-scenario duration,
  // think time included — so say so rather than passing it off as latency.
  // socketio.response_time is a genuine emit→ack/response round trip.
  const rtt = s['socketio.response_time'] ?? s['websocket.response_time'] ?? s['http.response_time'];
  if (rtt) state.sawRtt = true;
  // Never mix the two: substituting session length for periods that reported no
  // round-trip time folds two different quantities into one distribution and
  // drags the percentiles towards the much larger session numbers.
  const lat = rtt ?? (state.sawRtt ? undefined : s['vusers.session_length']);
  if (!state.warnedLatency && (rtt || s['vusers.session_length'])) {
    state.warnedLatency = true;
    ctx.log(rtt ? 'info' : 'warn', rtt
      ? 'latency = socket round-trip time (emit to ack/response)'
      : 'latency = vusers.session_length (whole scenario incl. think time) — no round-trip metric reported');
  }
  const count = sent > 0 ? sent : (lat?.count ?? 0);
  const success = Math.max(0, count - failed);

  if (count > 0) {
    if (lat) {
      const q: Array<[number, number]> = [];
      if (lat.min != null) q.push([0, lat.min]);
      if (lat.p50 != null || lat.median != null) q.push([50, lat.p50 ?? lat.median]);
      if (lat.p75 != null) q.push([75, lat.p75]);
      if (lat.p90 != null) q.push([90, lat.p90]);
      if (lat.p95 != null) q.push([95, lat.p95]);
      if (lat.p99 != null) q.push([99, lat.p99]);
      if (lat.max != null) q.push([100, lat.max]);
      ctx.agg.recordDistribution(count, success, q);
    } else {
      ctx.agg.recordDistribution(count, success, []);
    }
  }

  for (const [k, v] of Object.entries(c)) {
    if (k.startsWith('errors.') && v > 0) ctx.error(k.slice('errors.'.length), k);
  }
  const ok = c['plugins.expect.ok'];
  const notOk = c['plugins.expect.failed'];
  if (ok != null || notOk != null) ctx.agg.addCheck('expect', ok ?? 0, notOk ?? 0);
}

export function buildFlow(cfg: SocketConfig): Array<Record<string, unknown>> {
  return cfg.engine === 'socketio' ? buildSocketIoFlow(cfg) : buildWsFlow(cfg);
}

/**
 * Socket.IO flow: named events, optional acknowledgement callbacks, and
 * server-pushed events to wait for.
 */
function buildSocketIoFlow(cfg: SocketConfig): Array<Record<string, unknown>> {
  const flow: Array<Record<string, unknown>> = [];
  for (const step of cfg.flow) {
    if (step.kind === 'think') {
      flow.push({ think: Number(step.value) || 1 });
      continue;
    }
    if (step.kind === 'listen') {
      // Deliberately dropped. Artillery's socketio engine records a flat ~10s
      // for every `response` step whether or not the event arrives — verified
      // against a server that replies in ~15ms — so keeping it would poison the
      // latency distribution. It also cannot coexist with `acknowledge`, which
      // fails every iteration with "Failed match". Acknowledgements give a real
      // round trip, so that is the supported path.
      continue;
    }
    // 'emit' — and 'send' too, so switching engines keeps existing steps usable.
    const entry: Record<string, unknown> = {
      emit: { channel: step.event ?? 'message', data: parseData(step.value) },
    };
    if (step.namespace || cfg.namespace) entry.namespace = step.namespace || cfg.namespace;
    if (step.acknowledge) entry.acknowledge = matchSpec(step);
    flow.push(entry);
  }
  return flow.length ? flow : [{ think: 1 }];
}

/** Shared by acknowledge and response: assert on a JSON path, or accept anything. */
function matchSpec(step: SocketFlowStep): Record<string, unknown> {
  if (!step.matchPath) return {};
  return { match: { json: normalizeJsonPath(step.matchPath), value: step.matchValue ?? '' } };
}

/**
 * Point a user-written path at the acknowledgement payload.
 *
 * The engine matches against `JSON.stringify(args)` — the array of callback
 * arguments — so a plain `$.status` can never match. Paths are rewritten to
 * address the first argument, which is where a Socket.IO ack payload lives.
 * An explicit `$[...]` is left alone so raw addressing stays possible.
 */
export function normalizeJsonPath(path: string): string {
  const p = path.trim();
  if (p.startsWith('$[')) return p;
  const bare = p.replace(/^\$\.?/, '').replace(/^\./, '');
  return bare ? `$[0].${bare}` : '$[0]';
}

/** Send objects as objects so Socket.IO delivers structured data, not a string. */
function parseData(value: string): unknown {
  const t = value.trim();
  if (!t) return '';
  if ((t.startsWith('{') && t.endsWith('}')) || (t.startsWith('[') && t.endsWith(']'))) {
    try { return JSON.parse(t); } catch { return value; }
  }
  return value;
}

function buildWsFlow(cfg: SocketConfig): Array<Record<string, unknown>> {
  const flow: Array<Record<string, unknown>> = [];
  for (const step of cfg.flow) {
    if (step.kind === 'send' || step.kind === 'emit') {
      flow.push({ send: step.value });
    } else if (step.kind === 'think') {
      flow.push({ think: Number(step.value) || 1 });
    } else if (step.kind === 'expect' || step.kind === 'listen') {
      // The ws engine reads match specs from inside the send step, beside
      // `payload`. Attaching a sibling key makes Artillery reject the scenario
      // with "must have 1 key" before the run starts.
      const prev = flow[flow.length - 1];
      const payload = prev && typeof prev.send === 'string' ? prev.send : undefined;
      if (prev && payload !== undefined) {
        prev.send = { payload, match: { regexp: escapeRegex(step.value) } };
      }
    }
  }
  if (flow.length === 0) flow.push({ think: 1 });
  return flow;
}

function buildScript(cfg: SocketConfig, runId: string): Record<string, unknown> {
  const flow = buildFlow(cfg);
  const engineConfig = cfg.engine === 'socketio'
    ? {
        socketio: {
          ...(cfg.transports.length ? { transports: cfg.transports } : {}),
          ...(Object.keys(cfg.query).length ? { query: cfg.query } : {}),
          ...(Object.keys(cfg.headers).length ? { extraHeaders: cfg.headers } : {}),
        },
      }
    : {
        ws: {
          headers: cfg.headers,
          ...(cfg.subprotocols.length ? { subprotocols: cfg.subprotocols } : {}),
        },
      };

  return {
    config: {
      target: cfg.url,
      ...engineConfig,
      phases: cfg.phases.map((p) => ({
        name: p.name,
        duration: p.durationSec,
        arrivalRate: p.arrivalRate,
        ...(p.rampTo ? { rampTo: p.rampTo } : {}),
      })),
      plugins: {
        'ltd-publish': {
          url: `http://127.0.0.1:${PORT}/_ingest/artillery/${runId}`,
          runId,
        },
      },
    },
    scenarios: [{ engine: cfg.engine, name: 'socket', flow }],
  };
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
