import { randomUUID } from 'node:crypto';
import { bus } from '../bus.ts';
import { Aggregator } from '../metrics/aggregator.ts';
import { evaluate } from '../metrics/thresholds.ts';
import * as store from '../store/db.ts';
import { MAX_CONCURRENT_RUNS } from '../config.ts';
import { k6Runner } from './k6.runner.ts';
import { artilleryRunner } from './artillery.runner.ts';
import { kafkaRunner } from './kafka.runner.ts';
import type { Runner, RunnerContext } from './types.ts';
import type {
  Protocol, RunConfig, RunnerAvailability, RunState, RunSummary, ThresholdResult,
} from '../shared/types.ts';

const RUNNERS: Record<Protocol, Runner> = {
  rest: k6Runner,
  socket: artilleryRunner,
  kafka: kafkaRunner,
};

interface ActiveRun {
  runId: string;
  protocol: Protocol;
  profileName: string;
  target: string;
  startedAt: number;
  abort: AbortController;
  agg: Aggregator;
  ticker: NodeJS.Timeout;
}

const active = new Map<string, ActiveRun>();

/**
 * Pending runs.
 *
 * Selecting several profiles runs them one after another rather than at once:
 * concurrent load generators on one host compete for CPU and sockets and skew
 * each other's latency numbers, which is exactly what the tool is measuring.
 */
interface QueuedRun {
  queueId: string;
  profileId: string | null;
  profileName: string;
  config: RunConfig;
}

const queue: QueuedRun[] = [];

export function queuedRuns(): Array<{ queueId: string; profileName: string; protocol: Protocol }> {
  return queue.map((q) => ({ queueId: q.queueId, profileName: q.profileName, protocol: q.config.protocol }));
}

function publishQueue(): void {
  bus.publish({ t: 'queue', runId: null, pending: queuedRuns() });
}

export function clearQueue(): number {
  const n = queue.length;
  queue.length = 0;
  publishQueue();
  return n;
}

export function dequeue(queueId: string): boolean {
  const i = queue.findIndex((q) => q.queueId === queueId);
  if (i < 0) return false;
  queue.splice(i, 1);
  publishQueue();
  return true;
}

/** Start the next queued run, if nothing is in flight. */
function startNext(): void {
  if (active.size >= MAX_CONCURRENT_RUNS) return;
  const next = queue.shift();
  if (!next) return;
  publishQueue();
  try {
    launch(next.config, next.profileId, next.profileName);
  } catch {
    // A queued profile that cannot start must not strand the rest of the batch.
    startNext();
  }
}

export function activeRuns(): Array<{ runId: string; protocol: Protocol; profileName: string; startedAt: number }> {
  return [...active.values()].map((r) => ({
    runId: r.runId, protocol: r.protocol, profileName: r.profileName, startedAt: r.startedAt,
  }));
}

export async function availability(): Promise<RunnerAvailability> {
  const [rest, socket, kafka] = await Promise.all([
    k6Runner.available(), artilleryRunner.available(), kafkaRunner.available(),
  ]);
  return { rest, socket, kafka };
}

export function stopRun(runId: string): boolean {
  const run = active.get(runId);
  if (!run) return false;
  run.abort.abort();
  return true;
}

export function stopAll(): void {
  // Clear the queue first, or aborting the active run just starts the next one.
  queue.length = 0;
  for (const r of active.values()) r.abort.abort();
}

export function startRun(args: {
  config: RunConfig;
  profileId: string | null;
  profileName: string;
}): string {
  if (active.size >= MAX_CONCURRENT_RUNS) {
    throw new Error(`a run is already in progress (limit ${MAX_CONCURRENT_RUNS}) — parallel runs skew each other's numbers`);
  }
  return launch(args.config, args.profileId, args.profileName);
}

/**
 * Queue a batch. The first entry starts immediately when nothing is running;
 * the rest wait their turn.
 */
export function startBatch(
  entries: Array<{ config: RunConfig; profileId: string | null; profileName: string }>,
): { runId: string | null; queued: Array<{ queueId: string; profileName: string }> } {
  for (const e of entries) {
    if (!RUNNERS[e.config.protocol]) throw new Error(`unknown protocol ${e.config.protocol}`);
    queue.push({ queueId: randomUUID(), profileId: e.profileId, profileName: e.profileName, config: e.config });
  }
  publishQueue();

  let runId: string | null = null;
  if (active.size < MAX_CONCURRENT_RUNS) {
    const next = queue.shift();
    if (next) {
      publishQueue();
      runId = launch(next.config, next.profileId, next.profileName);
    }
  }
  return { runId, queued: queue.map((q) => ({ queueId: q.queueId, profileName: q.profileName })) };
}

function launch(config: RunConfig, profileId: string | null, profileName: string): string {
  const args = { config, profileId, profileName };
  const runner = RUNNERS[args.config.protocol];
  if (!runner) throw new Error(`unknown protocol ${args.config.protocol}`);

  const runId = randomUUID();
  const startedAt = Date.now();
  const target = targetOf(args.config);
  const agg = new Aggregator(startedAt);
  const abort = new AbortController();

  store.createRun({
    id: runId, profileId: args.profileId, profileName: args.profileName,
    protocol: args.config.protocol, target, startedAt, config: args.config,
  });

  bus.publish({
    t: 'start', runId, startedAt,
    protocol: args.config.protocol, profileName: args.profileName, target,
  });

  const ticker = setInterval(() => {
    const w = agg.closeWindow();
    if (!w) return;
    store.insertSample(runId, w);
    bus.publish({ t: 'tick', runId, window: w });
  }, 250);

  const ctx: RunnerContext = {
    runId,
    config: args.config,
    agg,
    signal: abort.signal,
    log(level, line) {
      const ts = Date.now();
      store.insertLog(runId, ts, level, line);
      bus.publish({ t: 'log', runId, ts, level, line });
    },
    error(kind, message) {
      agg.addError(kind, message);
      bus.publish({ t: 'error', runId, ts: Date.now(), kind, message, count: 1 });
    },
  };

  active.set(runId, {
    runId, protocol: args.config.protocol, profileName: args.profileName,
    target, startedAt, abort, agg, ticker,
  });

  void execute(runner, ctx, args.profileName, target).catch(() => { /* handled inside */ });
  return runId;
}

async function execute(runner: Runner, ctx: RunnerContext, profileName: string, target: string): Promise<void> {
  const run = active.get(ctx.runId)!;
  let state: RunState = 'passed';
  let nativeThresholds: ThresholdResult[] = [];
  let nativeVerdict: 'pass' | 'fail' | undefined;

  try {
    const result = await runner.run(ctx);
    nativeVerdict = result.nativeVerdict;
    nativeThresholds = result.nativeThresholds ?? [];
  } catch (err) {
    state = 'error';
    ctx.error('runner', (err as Error).message);
    ctx.log('error', (err as Error).message);
  }

  clearInterval(run.ticker);
  const endedAt = Date.now();

  // Flush the partial trailing window so short runs are not silently dropped.
  const tail = ctx.agg.closeWindow(endedAt, true);
  if (tail && tail.requests > 0) {
    store.insertSample(ctx.runId, tail);
    bus.publish({ t: 'tick', runId: ctx.runId, window: tail });
  }

  if (ctx.signal.aborted && state !== 'error') state = 'stopped';

  const summary = buildSummary(ctx, profileName, target, endedAt);
  const own = evaluate(ctx.config.thresholds, summary);
  summary.thresholds = [...own, ...nativeThresholds];

  const checksFailed = summary.checks.some((c) => c.passRatePct < minPassRate(ctx, c.name));
  const thresholdsFailed = summary.thresholds.some((t) => !t.passed);
  summary.verdict = thresholdsFailed || checksFailed || nativeVerdict === 'fail' ? 'fail' : 'pass';

  if (state === 'passed') state = summary.verdict === 'pass' ? 'passed' : 'failed';

  store.finishRun(ctx.runId, state, endedAt, summary, ctx.agg.histogram().serialize());
  bus.publish({ t: 'end', runId: ctx.runId, endedAt, state, summary });

  active.delete(ctx.runId);
  store.applyRetention(store.getSettings().retentionRuns);
  startNext();
}

function minPassRate(ctx: RunnerContext, name: string): number {
  return ctx.config.checks.find((c) => c.name === name)?.minPassRatePct ?? 100;
}

function buildSummary(ctx: RunnerContext, profileName: string, target: string, endedAt: number): RunSummary {
  const agg = ctx.agg;
  const durationMs = endedAt - agg.startedAt;
  // Derived from totals, not the mean of per-second windows: a partial trailing
  // window would otherwise drag the average below the real throughput.
  const seconds = durationMs / 1000;
  const throughput = seconds > 0 ? Math.round((agg.totalRequests / seconds) * 10) / 10 : 0;
  const transactionRate = seconds > 0 && agg.totalTransactions > 0
    ? Math.round((agg.totalTransactions / seconds) * 10) / 10
    : throughput;
  return {
    runId: ctx.runId,
    protocol: ctx.config.protocol,
    profileName,
    target,
    startedAt: agg.startedAt,
    endedAt,
    durationMs,
    totalRequests: agg.totalRequests,
    totalSuccess: agg.totalSuccess,
    totalFailed: agg.totalFailed,
    successRatePct: agg.successRatePct,
    rpsAvg: throughput,
    rpsPeak: agg.rpsPeak,
    tpsAvg: transactionRate,
    tpsPeak: agg.tpsPeak,
    vusMax: agg.vusMax,
    latency: agg.latencyProfile(),
    checks: agg.checkResults(),
    thresholds: [],
    errors: agg.errorBuckets(),
    verdict: 'pass',
  };
}

export function targetOf(c: RunConfig): string {
  switch (c.protocol) {
    case 'rest': return c.rest?.url ?? '';
    case 'socket': return c.socket?.url ?? '';
    case 'kafka': return `${c.kafka?.bootstrapServers ?? ''}/${c.kafka?.topic ?? ''}`;
  }
}
