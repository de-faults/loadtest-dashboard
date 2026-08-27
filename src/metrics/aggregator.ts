/**
 * Turns a raw stream of per-request samples into 1-second windows plus a
 * whole-run rollup. Shared by all three runners so their numbers are comparable.
 */

import { Histogram } from './histogram.ts';
import type { CheckResult, ErrorBucket, LatencyProfile, WindowMetrics } from '../shared/types.ts';

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

  addError(kind: string, message: string): void {
    const e = this.errors.get(kind) ?? { count: 0, sample: message };
    e.count++;
    this.errors.set(kind, e);
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

  errorBuckets(): ErrorBucket[] {
    return [...this.errors.entries()]
      .map(([kind, e]) => ({ kind, count: e.count, sample: e.sample }))
      .sort((a, b) => b.count - a.count);
  }

  get successRatePct(): number {
    return this.totalRequests ? round2((this.totalSuccess / this.totalRequests) * 100) : 0;
  }
}

function round2(n: number): number { return Math.round(n * 100) / 100; }
