import { spawn } from 'node:child_process';
import { mkdtemp, rm, writeFile, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { getSettings } from '../store/db.ts';
import { toK6Thresholds } from '../metrics/thresholds.ts';
import { tailLines } from './tail.ts';
import { materializeScript, usesCustomScript } from './script.ts';
import type { Runner, RunnerContext, RunnerResult } from './types.ts';
import type { RestConfig } from '../shared/types.ts';

const HERE = dirname(fileURLToPath(import.meta.url));
const SCRIPT = join(HERE, 'assets', 'k6-script.js');

/** k6 exits 99 when a threshold was crossed. */
const K6_THRESHOLD_EXIT = 99;

interface K6Point {
  type: string;
  metric: string;
  data?: { time: string; value: number; tags?: Record<string, string> };
}

export const k6Runner: Runner = {
  protocol: 'rest',

  async available() {
    const bin = getSettings().k6Path;
    return probe(bin, ['version']);
  },

  async run(ctx: RunnerContext): Promise<RunnerResult> {
    const cfg = ctx.config.rest;
    if (!cfg) throw new Error('rest config missing');

    const dir = await mkdtemp(join(tmpdir(), 'ltd-k6-'));
    const cfgPath = join(dir, 'config.json');
    const summaryPath = join(dir, 'summary.json');
    const outPath = join(dir, 'metrics.ndjson');

    const { thresholds } = toK6Thresholds(ctx.config.thresholds);
    await writeFile(cfgPath, JSON.stringify(buildScriptConfig(cfg, ctx, thresholds)), 'utf8');

    // A custom script owns its own options and thresholds; the config file is
    // still handed over via __ENV.CFG in case the script wants to read it.
    let scriptPath = SCRIPT;
    if (usesCustomScript(ctx.config.script)) {
      scriptPath = await materializeScript(ctx.config.script, dir, 'script.js');
      ctx.log('info', `custom k6 script: ${scriptPath}`);
    }

    const bin = getSettings().k6Path;
    // argv array + shell:false — user input never reaches a shell.
    const args = [
      'run',
      '--out', `json=${outPath}`,
      '--quiet',
      '--no-color',
      '--env', `CFG=${cfgPath}`,
      '--env', `SUMMARY_OUT=${summaryPath}`,
      scriptPath,
    ];
    ctx.log('info', `k6 ${args.join(' ')}`);

    const child = spawn(bin, args, { shell: false, stdio: ['ignore', 'pipe', 'pipe'] });
    let exited = false;
    let exitCode: number | null = null;

    const onAbort = () => { child.kill('SIGINT'); };
    ctx.signal.addEventListener('abort', onAbort, { once: true });

    child.stdout.on('data', (b: Buffer) => lines(b).forEach((l) => ctx.log('info', l)));
    child.stderr.on('data', (b: Buffer) => lines(b).forEach((l) => ctx.log('warn', l)));

    const exitPromise = new Promise<number | null>((resolve) => {
      child.on('close', (code) => { exited = true; exitCode = code; resolve(code); });
      child.on('error', (err) => {
        exited = true;
        ctx.error('spawn', `${bin}: ${err.message}`);
        resolve(null);
      });
    });

    const tailPromise = tailLines(outPath, (line) => consumePoint(line, ctx), { done: () => exited });

    await exitPromise;
    await tailPromise;
    ctx.signal.removeEventListener('abort', onAbort);

    const result: RunnerResult = {
      nativeVerdict: exitCode === 0 ? 'pass' : exitCode === K6_THRESHOLD_EXIT ? 'fail' : undefined,
    };

    try {
      const raw = await readFile(summaryPath, 'utf8');
      applyNativeSummary(JSON.parse(raw) as K6Summary, ctx, result);
    } catch {
      ctx.log('warn', 'k6 summary not produced — live metrics only');
    }

    await rm(dir, { recursive: true, force: true });
    return result;
  },
};

function lines(b: Buffer): string[] {
  return b.toString('utf8').split('\n').map((s) => s.trimEnd()).filter(Boolean);
}

function consumePoint(line: string, ctx: RunnerContext): void {
  let p: K6Point;
  try { p = JSON.parse(line) as K6Point; } catch { return; }
  if (p.type !== 'Point' || !p.data) return;
  const { value, tags } = p.data;
  const ts = Date.parse(p.data.time) || Date.now();

  switch (p.metric) {
    case 'http_req_duration': {
      // k6 tags each duration point with expected_response, so one point
      // carries both the latency and the pass/fail verdict.
      const ok = tags?.expected_response !== 'false';
      ctx.agg.record({ ts, latencyMs: value, ok });
      if (!ok) {
        const kind = tags?.error_code ? `http_${tags.error_code}` : `status_${tags?.status ?? 'unknown'}`;
        ctx.error(kind, tags?.error || `unexpected response status ${tags?.status ?? '?'}`);
      }
      break;
    }
    case 'vus':
      ctx.agg.setVus(Math.round(value));
      break;
    case 'checks': {
      const name = tags?.check ?? 'check';
      if (value === 1) ctx.agg.addCheck(name, 1, 0);
      else ctx.agg.addCheck(name, 0, 1);
      break;
    }
    default:
      break;
  }
}

interface K6Summary {
  metrics?: Record<string, {
    values?: Record<string, number>;
    thresholds?: Record<string, { ok: boolean }>;
  }>;
}

/** k6's own summary is authoritative for thresholds it evaluated. */
function applyNativeSummary(sum: K6Summary, ctx: RunnerContext, result: RunnerResult): void {
  const native: RunnerResult['nativeThresholds'] = [];
  for (const [metric, m] of Object.entries(sum.metrics ?? {})) {
    for (const [expr, r] of Object.entries(m.thresholds ?? {})) {
      native.push({ expr: `${metric}: ${expr}`, metric, actual: NaN, passed: r.ok });
    }
  }
  if (native.length) result.nativeThresholds = native;
  const reqs = sum.metrics?.http_reqs?.values?.count;
  if (typeof reqs === 'number') ctx.log('info', `k6 reported ${reqs} http requests`);
}

function buildScriptConfig(cfg: RestConfig, ctx: RunnerContext, thresholds: Record<string, string[]>) {
  const headers: Record<string, string> = { ...cfg.headers };
  if (cfg.auth.kind === 'bearer' && cfg.auth.token) headers.Authorization = `Bearer ${cfg.auth.token}`;
  if (cfg.auth.kind === 'basic' && cfg.auth.username) {
    headers.Authorization = `Basic ${Buffer.from(`${cfg.auth.username}:${cfg.auth.password ?? ''}`).toString('base64')}`;
  }
  if (cfg.bodyType === 'json' && !headers['Content-Type']) headers['Content-Type'] = 'application/json';
  if (cfg.bodyType === 'form' && !headers['Content-Type']) headers['Content-Type'] = 'application/x-www-form-urlencoded';

  const k6Options: Record<string, unknown> = {
    insecureSkipTLSVerify: cfg.insecureSkipTlsVerify,
    thresholds,
    summaryTrendStats: ['min', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max'],
  };

  if (cfg.loadModel === 'rate') {
    k6Options.scenarios = {
      constant_rate: {
        executor: 'constant-arrival-rate',
        rate: cfg.rate,
        timeUnit: '1s',
        duration: `${cfg.rateDurationSec}s`,
        preAllocatedVUs: cfg.preAllocatedVUs,
        maxVUs: Math.max(cfg.preAllocatedVUs * 4, cfg.rate),
      },
    };
  } else {
    k6Options.stages = cfg.stages.map((s) => ({ duration: `${s.duration}s`, target: s.target }));
  }

  return {
    url: cfg.url,
    method: cfg.method,
    headers,
    body: cfg.bodyType === 'none' ? '' : cfg.body,
    timeoutSec: cfg.timeoutSec,
    followRedirects: cfg.followRedirects,
    thinkTimeMs: cfg.thinkTimeMs,
    checks: ctx.config.checks,
    k6Options,
  };
}

export async function probe(bin: string, args: string[]): Promise<{ available: boolean; detail: string }> {
  return new Promise((resolve) => {
    const child = spawn(bin, args, { shell: false, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    child.stdout.on('data', (b: Buffer) => { out += b.toString(); });
    child.stderr.on('data', (b: Buffer) => { out += b.toString(); });
    child.on('error', (err) => resolve({ available: false, detail: `${bin} not found (${err.message})` }));
    child.on('close', (code) => resolve(
      code === 0
        ? { available: true, detail: out.split('\n')[0].trim() || bin }
        : { available: false, detail: `${bin} exited ${code}` },
    ));
  });
}
