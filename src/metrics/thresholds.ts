/**
 * One threshold DSL for all three protocols.
 *
 *   p95 < 500          success_rate > 99      rps > 1000
 *   p99 <= 1000        error_rate < 1         tps >= 5000
 *   avg < 200          max < 5000             total_requests > 10000
 *
 * REST thresholds are additionally compiled into k6 `options.thresholds` so
 * k6's own exit code agrees with ours; if either side fails, the run fails.
 */

import type { RunSummary, ThresholdResult, ThresholdSpec } from '../shared/types.ts';

export const METRICS = [
  'min', 'avg', 'p90', 'p95', 'p99', 'max',
  'rps', 'tps', 'vus',
  'success_rate', 'error_rate',
  'total_requests', 'duration_s',
] as const;

export type MetricName = (typeof METRICS)[number];

const OPS = ['<=', '>=', '<', '>', '==', '!='] as const;
type Op = (typeof OPS)[number];

export interface ParsedThreshold {
  metric: MetricName;
  op: Op;
  value: number;
}

const EXPR_RE = /^\s*([a-z_0-9]+)\s*(<=|>=|<|>|==|!=)\s*(-?[0-9]*\.?[0-9]+)\s*$/i;

export function parseThreshold(expr: string): ParsedThreshold | null {
  const m = EXPR_RE.exec(expr);
  if (!m) return null;
  const metric = m[1].toLowerCase() as MetricName;
  if (!(METRICS as readonly string[]).includes(metric)) return null;
  return { metric, op: m[2] as Op, value: Number(m[3]) };
}

function compare(actual: number, op: Op, expected: number): boolean {
  switch (op) {
    case '<': return actual < expected;
    case '<=': return actual <= expected;
    case '>': return actual > expected;
    case '>=': return actual >= expected;
    case '==': return actual === expected;
    case '!=': return actual !== expected;
  }
}

/** Pull the metric's actual value out of a finished run summary. */
export function metricValue(metric: MetricName, s: RunSummary): number {
  switch (metric) {
    case 'min': return s.latency.min;
    case 'avg': return s.latency.avg;
    case 'p90': return s.latency.p90;
    case 'p95': return s.latency.p95;
    case 'p99': return s.latency.p99;
    case 'max': return s.latency.max;
    case 'rps': return s.rpsAvg;
    case 'tps': return s.tpsAvg;
    case 'vus': return s.vusMax;
    case 'success_rate': return s.successRatePct;
    case 'error_rate': return round2(100 - s.successRatePct);
    case 'total_requests': return s.totalRequests;
    case 'duration_s': return round2(s.durationMs / 1000);
  }
}

export function evaluate(specs: ThresholdSpec[], summary: RunSummary): ThresholdResult[] {
  return specs.map((spec) => {
    const p = parseThreshold(spec.expr);
    if (!p) {
      return { expr: spec.expr, metric: 'invalid', actual: NaN, passed: false };
    }
    const actual = metricValue(p.metric, summary);
    return { expr: spec.expr, metric: p.metric, actual, passed: compare(actual, p.op, p.value) };
  });
}

/**
 * Compile to k6 threshold syntax. Only latency and rate metrics have a direct
 * k6 equivalent; the rest stay server-side (returned as `unmapped`).
 */
export function toK6Thresholds(specs: ThresholdSpec[]): {
  thresholds: Record<string, string[]>;
  unmapped: string[];
} {
  const thresholds: Record<string, string[]> = {};
  const unmapped: string[] = [];
  const push = (key: string, cond: string) => {
    (thresholds[key] ??= []).push(cond);
  };

  for (const spec of specs) {
    const p = parseThreshold(spec.expr);
    if (!p) { unmapped.push(spec.expr); continue; }
    switch (p.metric) {
      case 'min': push('http_req_duration', `min${p.op}${p.value}`); break;
      case 'avg': push('http_req_duration', `avg${p.op}${p.value}`); break;
      case 'p90': push('http_req_duration', `p(90)${p.op}${p.value}`); break;
      case 'p95': push('http_req_duration', `p(95)${p.op}${p.value}`); break;
      case 'p99': push('http_req_duration', `p(99)${p.op}${p.value}`); break;
      case 'max': push('http_req_duration', `max${p.op}${p.value}`); break;
      // k6's http_req_failed is a rate in 0..1; our DSL speaks percent.
      case 'error_rate': push('http_req_failed', `rate${p.op}${p.value / 100}`); break;
      case 'success_rate': push('http_req_failed', `rate${flip(p.op)}${(100 - p.value) / 100}`); break;
      case 'rps': push('http_reqs', `rate${p.op}${p.value}`); break;
      default: unmapped.push(spec.expr);
    }
  }
  return { thresholds, unmapped };
}

/** success_rate > 99  ⇒  failure rate < 0.01 */
function flip(op: Op): Op {
  switch (op) {
    case '<': return '>';
    case '<=': return '>=';
    case '>': return '<';
    case '>=': return '<=';
    default: return op;
  }
}

function round2(n: number): number { return Math.round(n * 100) / 100; }
