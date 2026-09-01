/**
 * Turns a raw stream of per-request samples into 1-second windows plus a
 * whole-run rollup. Shared by all three runners so their numbers are comparable.
 */

import { Histogram } from './histogram.ts';
import type {
  CheckResult, ErrorBucket, ErrorOrigin, LatencyProfile, ScenarioStat, WindowMetrics,
} from '../shared/types.ts';

/** Upper bound on distinct scenario names kept, so a stray tag cannot grow the map without limit. */
const MAX_SCENARIOS = 50;

interface ScenarioAcc {
  vusers: number;
  requests: number;
  success: number;
  hist: Histogram;
}

/** One captured error payload, as reported by the runner. */
export interface ErrorBody {
  body: string;
  contentType?: string;
  /** Response headers, already redacted by the runner. */
  headers?: Record<string, string>;
  /** Characters the target sent, counted before truncation. */
  chars?: number;
  /** `body` is only the head of what the target sent. */
  truncated?: boolean;
  /** Which hop produced the response, as classified by the runner. */
  origin?: ErrorOrigin;
}

export interface Sample {
  ts: number;
  latencyMs: number;
  ok: boolean;
}

export class Aggregator {
  startedAt: number;
  private total = new Histogram();
  private window = new Histogram();
  private windowStart: number;
  private wRequests = 0;
  private wTransactions = 0;
  private wSuccess = 0;
  private wFailed = 0;
  private wLag: number | undefined;

  totalRequests = 0;
  totalSuccess = 0;
  totalFailed = 0;
  /**
   * Completed transactions (k6 iterations). One transaction can issue several
   * requests, so TPS is genuinely different from RPS and must be counted, not
   * copied from it.
   */
  totalTransactions = 0;
  vus = 0;
  vusMax = 0;
  rpsPeak = 0;
  tpsPeak = 0;
  private rpsSum = 0;
  private windows = 0;

  readonly checks = new Map<string, { passed: number; failed: number }>();
  readonly errors = new Map<string, { count: number; sample: string }>();
  /**
   * First response body seen per error kind. Kept apart from `errors` because
   * the body arrives on a different path than the count (k6 streams the failed
   * sample, the script logs the payload) and may land either side of it.
   */
  private readonly errorBodies = new Map<string, ErrorBody>();
  /**
   * Per-scenario totals, for runners that attribute their work to a named
   * scenario. Capped so an unexpected tag cardinality cannot grow without
   * bound — a script has scenarios in the tens, never the thousands.
   */
  readonly scenarios = new Map<string, ScenarioAcc>();

  constructor(startedAt = Date.now()) {
    this.startedAt = startedAt;
    this.windowStart = startedAt;
  }

  /**
   * Restart the timeline after a setup phase (e.g. the Kafka end-to-end
   * consumer handshake) so warm-up seconds are not billed as test duration.
   */
  resetClock(now = Date.now()): void {
    this.startedAt = now;
    this.windowStart = now;
  }

  record(s: Sample): void {
    this.total.record(s.latencyMs);
    this.window.record(s.latencyMs);
    this.wRequests++;
    this.totalRequests++;
    if (s.ok) { this.wSuccess++; this.totalSuccess++; }
    else { this.wFailed++; this.totalFailed++; }
  }

  /** Bulk path for runners that report pre-aggregated counts (artillery, k6 summary). */
  recordBatch(count: number, success: number, latencies: number[]): void {
    this.wRequests += count;
    this.totalRequests += count;
    this.wSuccess += success;
    this.totalSuccess += success;
    this.wFailed += count - success;
    this.totalFailed += count - success;
    for (const l of latencies) { this.total.record(l); this.window.record(l); }
  }

  /**
   * Path for runners that only report quantiles (Artillery). The distribution
   * is reconstructed piecewise-uniformly between the reported quantiles, so
   * whole-run percentiles are approximate — accurate to the reported points,
   * interpolated between them.
   */
  recordDistribution(count: number, success: number, q: Array<[number, number]>): void {
    this.wRequests += count;
    this.totalRequests += count;
    this.wSuccess += success;
    this.totalSuccess += success;
    this.wFailed += count - success;
    this.totalFailed += count - success;
    if (count <= 0 || q.length === 0) return;

    const sorted = [...q].sort((a, b) => a[0] - b[0]);
    let prevPct = 0;
    for (const [pct, value] of sorted) {
      const share = Math.max(0, pct - prevPct) / 100;
      const weight = Math.round(count * share);
      if (weight > 0) {
        this.total.recordWeighted(value, weight);
        this.window.recordWeighted(value, weight);
      }
      prevPct = pct;
    }
  }

  /** One completed iteration / transaction. */
  recordTransaction(count = 1): void {
    this.wTransactions += count;
    this.totalTransactions += count;
  }

  setVus(n: number): void {
    this.vus = n;
    if (n > this.vusMax) this.vusMax = n;
  }

  setLag(lag: number): void { this.wLag = lag; }

  addCheck(name: string, passed: number, failed: number): void {
    const c = this.checks.get(name) ?? { passed: 0, failed: 0 };
    c.passed += passed;
    c.failed += failed;
    this.checks.set(name, c);
  }

  addScenarioVus(name: string, count: number): void {
    if (count <= 0) return;
    const acc = this.scenarioAcc(name);
    if (acc) acc.vusers += count;
  }

  /** One request, attributed to the scenario that issued it. */
  recordScenarioSample(name: string, latencyMs: number, ok: boolean): void {
    const acc = this.scenarioAcc(name);
    if (!acc) return;
    acc.requests++;
    if (ok) acc.success++;
    acc.hist.record(latencyMs);
  }

  private scenarioAcc(name: string): ScenarioAcc | null {
    const found = this.scenarios.get(name);
    if (found) return found;
    if (this.scenarios.size >= MAX_SCENARIOS) return null;
    const acc: ScenarioAcc = { vusers: 0, requests: 0, success: 0, hist: new Histogram() };
    this.scenarios.set(name, acc);
    return acc;
  }

  addError(kind: string, message: string): void {
    const e = this.errors.get(kind) ?? { count: 0, sample: message };
    e.count++;
    this.errors.set(kind, e);
  }

  /**
   * Attach the response payload of a failed request to its bucket. Never
   * creates a bucket and never counts: the failure itself is counted by
   * `addError`, which may run before or after this.
   */
  attachErrorBody(kind: string, body: ErrorBody): void {
    if (this.errorBodies.has(kind)) return;
    this.errorBodies.set(kind, body);
  }

  /**
   * Close the current 1s window. Returns null when no window boundary has been
   * crossed yet, so callers can poll on any cadence.
   */
  closeWindow(now = Date.now(), force = false): WindowMetrics | null {
    const elapsedMs = now - this.windowStart;
    if (!force && elapsedMs < 1000) return null;
    if (elapsedMs <= 0) return null;

    const seconds = elapsedMs / 1000;
    const rps = this.wRequests / seconds;
    // Runners that have no separate notion of a transaction (Kafka messages,
    // socket emits) report the same number for both, rather than a hollow zero.
    const tps = this.totalTransactions > 0 ? this.wTransactions / seconds : rps;
    const w: WindowMetrics = {
      ts: now,
      elapsed: Math.round((now - this.startedAt) / 100) / 10,
      requests: this.wRequests,
      success: this.wSuccess,
      failed: this.wFailed,
      rps: Math.round(rps * 10) / 10,
      tps: Math.round(tps * 10) / 10,
      vus: this.vus,
      latency: this.window.profile(),
      consumerLag: this.wLag,
    };

    if (w.rps > this.rpsPeak) this.rpsPeak = w.rps;
    if (w.tps > this.tpsPeak) this.tpsPeak = w.tps;
    this.rpsSum += w.rps;
    this.windows++;

    this.window = new Histogram();
    this.wRequests = 0;
    this.wTransactions = 0;
    this.wSuccess = 0;
    this.wFailed = 0;
    this.wLag = undefined;
    this.windowStart = now;
    return w;
  }

  get rpsAvg(): number {
    return this.windows ? Math.round((this.rpsSum / this.windows) * 10) / 10 : 0;
  }

  latencyProfile(): LatencyProfile { return this.total.profile(); }
  histogram(): Histogram { return this.total; }

  checkResults(): CheckResult[] {
    return [...this.checks.entries()].map(([name, c]) => ({
      name,
      passed: c.passed,
      failed: c.failed,
      passRatePct: c.passed + c.failed ? round2((c.passed / (c.passed + c.failed)) * 100) : 0,
    }));
  }

  /**
   * Share is of whichever unit was actually counted — requests when the runner
   * attributes them, virtual users otherwise. Never of `vusMax`, which answers
   * a different question and cannot be trusted to add up to 100%.
   */
  scenarioStats(): ScenarioStat[] {
    const entries = [...this.scenarios.entries()];
    const totalRequests = entries.reduce((a, [, s]) => a + s.requests, 0);
    const totalVus = entries.reduce((a, [, s]) => a + s.vusers, 0);
    const byRequests = totalRequests > 0;
    return entries
      .map(([name, s]) => ({
        name,
        ...(s.vusers ? { vusers: s.vusers } : {}),
        ...(s.requests
          ? {
              requests: s.requests,
              successRatePct: round2((s.success / s.requests) * 100),
              p95: round2(s.hist.profile().p95),
            }
          : {}),
        sharePct: byRequests
          ? round2((s.requests / totalRequests) * 100)
          : totalVus
            ? round2((s.vusers / totalVus) * 100)
            : 0,
      }))
      .sort((a, b) => (b.requests ?? b.vusers ?? 0) - (a.requests ?? a.vusers ?? 0));
  }

  errorBuckets(): ErrorBucket[] {
    return [...this.errors.entries()]
      .map(([kind, e]) => {
        const b = this.errorBodies.get(kind);
        return {
          kind,
          count: e.count,
          sample: e.sample,
          ...(b
            ? {
                body: b.body,
                ...(b.contentType ? { bodyContentType: b.contentType } : {}),
                ...(b.headers && Object.keys(b.headers).length
                  ? { responseHeaders: b.headers }
                  : {}),
                ...(b.chars != null ? { bodyChars: b.chars } : {}),
                ...(b.origin ? { origin: b.origin } : {}),
                ...(b.truncated ? { bodyTruncated: true } : {}),
              }
            : {}),
        };
      })
      .sort((a, b) => b.count - a.count);
  }

  get successRatePct(): number {
    return this.totalRequests ? round2((this.totalSuccess / this.totalRequests) * 100) : 0;
  }
}

function round2(n: number): number { return Math.round(n * 100) / 100; }
