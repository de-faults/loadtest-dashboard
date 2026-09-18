/**
 * Capacity (ramp / step-load) analysis.
 *
 * A capacity test walks the load up in steps and holds each step long enough
 * for the system to settle. The number worth taking away is not the run's
 * average — it is the *last step the system still served inside its SLO*, and
 * the step where extra virtual users stopped buying extra throughput. Those two
 * numbers are what a follow-up performance test is dimensioned from.
 *
 * The analysis reads the recorded 1-second windows, not the run configuration,
 * so it works the same for a UI-built ramp, a bring-your-own k6 script with its
 * own `stages`, and an Artillery arrival-rate ramp.
 *
 * No Node import: `web/` compiles this file directly and runs it live, on the
 * windows already streamed into the chart.
 */

import type { Stage, ThresholdSpec, WindowMetrics } from './types.ts';
import { compare, parseThreshold, type MetricName } from './thresholdExpr.ts';

/** What "the system still coped" means for this run. */
export interface CapacitySlo {
  maxAvgMs?: number;
  maxP90Ms?: number;
  maxP95Ms?: number;
  maxP99Ms?: number;
  maxErrorRatePct?: number;
}

/**
 * One of the profile's thresholds, evaluated against a single step rather than
 * the whole run.
 *
 * `gates` separates the two kinds of rule a profile carries. An upper bound on
 * latency or errors describes what the system must do *at every load level*, so
 * it decides whether the step held. A goal like `rps > 1000` describes the run
 * as a whole — the first step of a ramp is supposed to be below it — so it is
 * reported per step but never counted against one.
 */
export interface CapacityThreshold {
  expr: string;
  metric: string;
  actual: number;
  passed: boolean;
  gates: boolean;
}

export interface CapacityStep {
  /** 1-based, in the order the steps were held. */
  index: number;
  /** Virtual users held during the step (median of the plateau). */
  vus: number;
  /** Seconds since run start. */
  startElapsed: number;
  endElapsed: number;
  /**
   * Wall-clock bounds of the measured part, so a step can be lined up against
   * the service's own logs, APM traces and dashboards — elapsed seconds are
   * only meaningful next to the run's start time.
   */
  startTs: number;
  endTs: number;
  /** Seconds actually measured — the settling head of the plateau is excluded. */
  durationSec: number;
  requests: number;
  success: number;
  failed: number;
  errorRatePct: number;
  /** Requests per second, over the measured part of the step. */
  rps: number;
  /** Best single second of the step. */
  rpsPeak: number;
  /**
   * Completed transactions per second. One transaction can issue several
   * requests — an async submit-then-poll journey is the usual case — so this is
   * the rate of finished business operations, not of HTTP calls.
   */
  tps: number;
  tpsPeak: number;
  latencyMin: number;
  latencyAvg: number;
  /**
   * Request-weighted mean of the per-second percentiles. The raw distribution
   * is not kept per step, so this is close to but not exactly the percentile of
   * the merged sample — treat it as a step-to-step comparison, not an SLA number.
   */
  p90: number;
  p95: number;
  p99: number;
  /** Worst single second's p95 — where a plateau hides a stall, this shows it. */
  p95Worst: number;
  max: number;
  /** The step stayed inside every gating rule. */
  passed: boolean;
  /** Gating rules the step broke, as their original expressions. */
  breaches: string[];
  /** Every threshold of the profile, measured against this step alone. */
  thresholds: CapacityThreshold[];
}

export interface CapacityReport {
  steps: CapacityStep[];
  slo: CapacitySlo;
  /** Highest step that met the SLO with no failing step below it. */
  knee: CapacityStep | null;
  /** First step that broke the SLO. */
  breaking: CapacityStep | null;
  /** Last step before added VUs stopped translating into throughput. */
  saturation: CapacityStep | null;
  /**
   * What to carry into the performance test: the capacity found, and a headroom
   * figure at `SAFE_FACTOR` of it — the level a soak or steady-state test is
   * normally driven at.
   */
  recommended: {
    vus: number;
    rps: number;
    safeVus: number;
    safeRps: number;
    /** 'slo' = limited by the SLO, 'saturation' = limited by throughput, 'max' = never limited. */
    limitedBy: 'slo' | 'saturation' | 'max';
  } | null;
}

export interface CapacityOptions {
  /** Ignore plateaus shorter than this — ramp transitions, not held load. */
  minHoldSec?: number;
  /** Head of each plateau dropped so the ramp-in transient is not measured. */
  settleSec?: number;
  /** The profile's thresholds, evaluated step by step. */
  thresholds?: ThresholdSpec[];
}

/** Headroom kept between measured capacity and the level a steady test drives. */
export const SAFE_FACTOR = 0.8;

const DEFAULT_MIN_HOLD_SEC = 10;
const DEFAULT_SETTLE_SEC = 5;
/**
 * Below this, an extra virtual user buys almost no extra throughput: the
 * system is queueing rather than serving, which is the definition of saturation.
 * Ratio of relative throughput gain to relative VU gain.
 */
const SATURATION_EFFICIENCY = 0.3;

/**
 * The SLO a capacity run is judged by, read from the profile's own thresholds.
 * Only the rules that bound a single step are used — `total_requests` or
 * `duration_s` describe the whole run and say nothing about one plateau.
 */
export function sloFromThresholds(specs: ThresholdSpec[] | undefined): CapacitySlo {
  const slo: CapacitySlo = {};
  for (const spec of specs ?? []) {
    const p = parseThreshold(spec.expr);
    // Only upper bounds constrain a step; `p95 > 100` is not an SLO.
    if (!p) continue;
    const upper = p.op === '<' || p.op === '<=';
    const lower = p.op === '>' || p.op === '>=';
    switch (p.metric) {
      case 'avg': if (upper) slo.maxAvgMs = p.value; break;
      case 'p90': if (upper) slo.maxP90Ms = p.value; break;
      case 'p95': if (upper) slo.maxP95Ms = p.value; break;
      case 'p99': if (upper) slo.maxP99Ms = p.value; break;
      case 'error_rate': if (upper) slo.maxErrorRatePct = p.value; break;
      // success_rate > 99 is the same statement as error_rate < 1.
      case 'success_rate': if (lower) slo.maxErrorRatePct = round(100 - p.value, 4); break;
      default: break;
    }
  }
  return slo;
}

/**
 * Whether a rule judges a single step. Only an upper bound on latency or on the
 * error rate does: it must hold at every load level. A throughput or volume
 * goal is a statement about the run, and a ramp's early steps are below it by
 * design — gating on it would call every capacity test a failure at 1 VU.
 */
function gatesStep(metric: MetricName, op: string): boolean {
  const upper = op === '<' || op === '<=';
  const lower = op === '>' || op === '>=';
  switch (metric) {
    case 'min': case 'avg': case 'p90': case 'p95': case 'p99': case 'max':
    case 'error_rate':
      return upper;
    case 'success_rate':
      return lower;
    default:
      return false;
  }
}

/** The step's own value for a threshold metric. */
function stepMetric(metric: MetricName, step: CapacityStep): number {
  switch (metric) {
    case 'min': return step.latencyMin;
    case 'avg': return step.latencyAvg;
    case 'p90': return step.p90;
    case 'p95': return step.p95;
    case 'p99': return step.p99;
    case 'max': return step.max;
    case 'rps': return step.rps;
    case 'tps': return step.tps;
    case 'vus': return step.vus;
    case 'success_rate': return round(100 - step.errorRatePct, 3);
    case 'error_rate': return step.errorRatePct;
    case 'total_requests': return step.requests;
    case 'duration_s': return step.durationSec;
    // Whole-run Kafka rollups: a single step has no value for them.
    case 'lag_max': case 'lag_avg': case 'lag_end': case 'lag_final':
    case 'lag_growth': case 'lag_drain_s': case 'total_mb': case 'mb_per_s':
    case 'msg_lag_avg': case 'msg_lag_p50': case 'msg_lag_p95': case 'msg_lag_p99': case 'msg_lag_max':
      return NaN;
  }
}

/**
 * Evaluate every threshold of the profile against one step, and decide from the
 * gating subset whether the step held.
 */
function judge(step: CapacityStep, specs: ThresholdSpec[]): void {
  for (const spec of specs) {
    const p = parseThreshold(spec.expr);
    if (!p) {
      step.thresholds.push({ expr: spec.expr, metric: 'invalid', actual: NaN, passed: false, gates: false });
      continue;
    }
    const actual = stepMetric(p.metric, step);
    // A whole-run rollup (consumer lag, volume) says nothing about one step —
    // listing it as a per-step miss would read as a failure that never happened.
    if (Number.isNaN(actual)) continue;
    const passed = compare(actual, p.op, p.value);
    const gates = gatesStep(p.metric, p.op);
    step.thresholds.push({ expr: spec.expr, metric: p.metric, actual, passed, gates });
    if (gates && !passed) step.breaches.push(spec.expr);
  }
  step.passed = step.breaches.length === 0;
}

interface Segment {
  vus: number;
  windows: WindowMetrics[];
}

/**
 * Split the timeline into held plateaus.
 *
 * A window joins the current plateau while its VU count stays within tolerance
 * of the level; a ramp therefore closes one plateau per value it passes through
 * and those short segments are dropped by the hold filter. Grouping on exact
 * equality instead would split a plateau in two every time the VU gauge
 * flickered by one.
 */
function segment(windows: WindowMetrics[], minHoldSec: number): Segment[] {
  const out: Segment[] = [];
  let cur: WindowMetrics[] = [];

  const level = (ws: WindowMetrics[]): number => {
    const vs = ws.map((w) => w.vus).sort((a, b) => a - b);
    return vs[Math.floor(vs.length / 2)];
  };
  const close = (): void => {
    if (!cur.length) return;
    const vus = level(cur);
    // A plateau at zero VUs is the cool-down, not a load step.
    if (cur.length >= minHoldSec && vus > 0) out.push({ vus, windows: cur });
    cur = [];
  };

  for (const w of windows) {
    if (!cur.length) { cur = [w]; continue; }
    const lvl = level(cur);
    const tol = Math.max(1, lvl * 0.05);
    if (Math.abs(w.vus - lvl) <= tol) cur.push(w);
    else { close(); cur = [w]; }
  }
  close();
  return out;
}

function analyzeSegment(seg: Segment, index: number, settleSec: number, specs: ThresholdSpec[]): CapacityStep {
  // Drop the settling head, but never more than a quarter of the plateau — a
  // short step would otherwise be measured on almost nothing.
  const skip = Math.min(settleSec, Math.floor(seg.windows.length / 4));
  const ws = seg.windows.slice(skip);

  let requests = 0, success = 0, failed = 0, rpsPeak = 0, tpsSum = 0, tpsPeak = 0;
  let min = Number.POSITIVE_INFINITY, max = 0, p95Worst = 0;
  let latSum = 0, p90Sum = 0, p95Sum = 0, p99Sum = 0, weight = 0;
  for (const w of ws) {
    requests += w.requests;
    success += w.success;
    failed += w.failed;
    if (w.rps > rpsPeak) rpsPeak = w.rps;
    // TPS is already a per-second rate in the window, so the step's figure is
    // the mean of the seconds — requests can be summed, a rate cannot.
    tpsSum += w.tps;
    if (w.tps > tpsPeak) tpsPeak = w.tps;
    if (w.latency.max > max) max = w.latency.max;
    if (w.latency.p95 > p95Worst) p95Worst = w.latency.p95;
    // A second with no traffic has no latency to average in.
    if (w.requests > 0) {
      if (w.latency.min < min) min = w.latency.min;
      weight += w.requests;
      latSum += w.latency.avg * w.requests;
      p90Sum += w.latency.p90 * w.requests;
      p95Sum += w.latency.p95 * w.requests;
      p99Sum += w.latency.p99 * w.requests;
    }
  }

  const durationSec = ws.length;
  const errorRatePct = requests ? (failed / requests) * 100 : 0;
  const step: CapacityStep = {
    index,
    vus: seg.vus,
    startElapsed: ws[0]?.elapsed ?? seg.windows[0].elapsed,
    endElapsed: ws[ws.length - 1]?.elapsed ?? seg.windows[seg.windows.length - 1].elapsed,
    startTs: ws[0]?.ts ?? seg.windows[0].ts,
    endTs: ws[ws.length - 1]?.ts ?? seg.windows[seg.windows.length - 1].ts,
    durationSec,
    requests,
    success,
    failed,
    errorRatePct: round(errorRatePct, 3),
    rps: round(durationSec ? requests / durationSec : 0, 2),
    rpsPeak: round(rpsPeak, 2),
    tps: round(durationSec ? tpsSum / durationSec : 0, 2),
    tpsPeak: round(tpsPeak, 2),
    latencyMin: round(Number.isFinite(min) ? min : 0, 2),
    latencyAvg: round(weight ? latSum / weight : 0, 2),
    p90: round(weight ? p90Sum / weight : 0, 2),
    p95: round(weight ? p95Sum / weight : 0, 2),
    p99: round(weight ? p99Sum / weight : 0, 2),
    p95Worst: round(p95Worst, 2),
    max: round(max, 2),
    passed: true,
    breaches: [],
    thresholds: [],
  };

  judge(step, specs);
  return step;
}

/**
 * Analyze a ramp. Returns `null` when the run never held more than one load
 * level — a flat run has no capacity curve, and reporting one from a single
 * point would be an invention.
 */
export function analyzeCapacity(
  windows: WindowMetrics[],
  opts: CapacityOptions = {},
): CapacityReport | null {
  const minHoldSec = opts.minHoldSec ?? DEFAULT_MIN_HOLD_SEC;
  const settleSec = opts.settleSec ?? DEFAULT_SETTLE_SEC;
  const specs = opts.thresholds ?? [];
  const slo = sloFromThresholds(specs);

  const segments = segment(windows, minHoldSec);
  if (segments.length < 2) return null;

  const steps = segments.map((s, i) => analyzeSegment(s, i + 1, settleSec, specs));

  const firstBad = steps.findIndex((s) => !s.passed);
  const breaking = firstBad === -1 ? null : steps[firstBad];
  // The knee is the last good step *below the first failure*: a step that
  // recovers after the system broke is not capacity, it is noise.
  const knee = firstBad === -1
    ? (steps[steps.length - 1] ?? null)
    : (firstBad > 0 ? steps[firstBad - 1] : null);

  let saturation: CapacityStep | null = null;
  for (let i = 1; i < steps.length; i++) {
    const prev = steps[i - 1], cur = steps[i];
    if (cur.vus <= prev.vus || prev.rps <= 0) continue;
    const vusGain = (cur.vus - prev.vus) / prev.vus;
    const rpsGain = (cur.rps - prev.rps) / prev.rps;
    if (vusGain > 0 && rpsGain / vusGain < SATURATION_EFFICIENCY) { saturation = prev; break; }
  }

  // Capacity is whichever ceiling was hit first — the SLO or the throughput
  // curve flattening. Both are real limits; the lower one is the one that binds.
  const candidates: Array<{ step: CapacityStep; by: 'slo' | 'saturation' | 'max' }> = [];
  if (knee) candidates.push({ step: knee, by: breaking ? 'slo' : 'max' });
  if (saturation) candidates.push({ step: saturation, by: 'saturation' });
  candidates.sort((a, b) => a.step.vus - b.step.vus);
  const best = candidates[0] ?? null;

  const recommended = best
    ? {
        vus: best.step.vus,
        rps: best.step.rps,
        safeVus: Math.max(1, Math.floor(best.step.vus * SAFE_FACTOR)),
        safeRps: round(best.step.rps * SAFE_FACTOR, 2),
        limitedBy: best.by,
      }
    : null;

  return { steps, slo, knee, breaking, saturation, recommended };
}

function round(n: number, digits: number): number {
  const f = 10 ** digits;
  return Math.round(n * f) / f;
}


// ─── Building a ramp ─────────────────────────────────────────────────────────

/**
 * A stepped ramp, described the way a capacity test is planned rather than as
 * the stage list it compiles to: walk from `startVus` to `maxVus` in `stepVus`
 * increments, taking `rampSec` to reach each level and holding it `holdSec`.
 */
export interface RampPlan {
  startVus: number;
  stepVus: number;
  maxVus: number;
  /** Seconds spent climbing to each new level. */
  rampSec: number;
  /** Seconds the level is held — the part that is actually measured. */
  holdSec: number;
  /** Seconds spent returning to zero at the end. */
  coolDownSec: number;
}

/**
 * Levels the plan visits, in order: `startVus` first, then the multiples of
 * `stepVus`, and `maxVus` always last.
 *
 * The steps are multiples of the step size rather than `start + n × step`, so a
 * ramp that opens on a single-user smoke level still walks 10 / 20 / 30 instead
 * of 11 / 21 / 31. A start that is already a multiple loses nothing by it.
 */
export function rampLevels(plan: RampPlan): number[] {
  const start = Math.max(1, Math.floor(plan.startVus) || 1);
  const max = Math.max(start, Math.floor(plan.maxVus) || start);
  const step = Math.max(1, Math.floor(plan.stepVus) || 1);
  const levels: number[] = [start];
  // Guard the loop rather than trusting the inputs: a UI mid-edit can hand us
  // a plan that would otherwise generate thousands of stages.
  const first = Math.ceil((start + 1) / step) * step;
  for (let v = first; v < max && levels.length < MAX_RAMP_LEVELS; v += step) levels.push(v);
  if (levels[levels.length - 1] !== max) levels.push(max);
  return levels;
}

const MAX_RAMP_LEVELS = 100;

/** Compile a plan into k6-shaped stages: ramp, hold, ramp, hold, …, cool-down. */
export function rampStages(plan: RampPlan): Stage[] {
  const ramp = Math.max(0, Math.floor(plan.rampSec));
  const hold = Math.max(1, Math.floor(plan.holdSec));
  const stages: Stage[] = [];
  for (const target of rampLevels(plan)) {
    if (ramp > 0) stages.push({ duration: ramp, target });
    stages.push({ duration: hold, target });
  }
  const cool = Math.max(0, Math.floor(plan.coolDownSec));
  if (cool > 0) stages.push({ duration: cool, target: 0 });
  return stages;
}

/** Total seconds a stage list runs for — what the UI shows before you start. */
export function stagesDurationSec(stages: Stage[]): number {
  return stages.reduce((a, s) => a + (Number(s.duration) || 0), 0);
}
