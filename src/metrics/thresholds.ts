/**
 * One threshold DSL for all three protocols.
 *
 *   p95 < 500          success_rate > 99      rps > 1000
 *   p99 <= 1000        error_rate < 1         tps >= 5000
 *   avg < 200          max < 5000             total_requests > 10000
 *
 * Kafka adds consumer lag and volume:
 *
 *   lag_max < 50000    lag_end < 1000         lag_drain_s < 60
 *   lag_growth <= 0    total_mb >= 10000      mb_per_s > 50
 *   msg_lag_p95 < 2000 msg_lag_max < 10000    (per-message lag, ms)
 *
 * REST thresholds are additionally compiled into k6 `options.thresholds` so
 * k6's own exit code agrees with ours; if either side fails, the run fails.
 */

import type { RunSummary, ThresholdResult, ThresholdSpec } from '../shared/types.ts';
import { compare, parseThreshold, type MetricName, type Op } from '../shared/thresholdExpr.ts';

export {
  METRICS,
  OPS,
  parseThreshold,
} from '../shared/thresholdExpr.ts';
export type { MetricName, Op, ParsedThreshold } from '../shared/thresholdExpr.ts';

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
    case 'lag_max': return s.kafka?.lag?.max ?? NaN;
    case 'lag_avg': return s.kafka?.lag?.avg ?? NaN;
    case 'lag_end': return s.kafka?.lag?.endOfLoad ?? NaN;
    case 'lag_final': return s.kafka?.lag?.final ?? NaN;
    case 'lag_growth': return s.kafka?.lag?.growthPerSec ?? NaN;
    // A group that never caught up has no drain time, and must fail any bound on it.
    case 'lag_drain_s': {
      const drain = s.kafka?.lag?.drain;
      if (!drain) return NaN;
      return drain.seconds ?? Number.POSITIVE_INFINITY;
    }
    case 'total_mb': return s.kafka ? round2(s.kafka.volume.bytesAcked / 1e6) : NaN;
    case 'mb_per_s': return s.kafka?.volume.mbPerSecAvg ?? NaN;
    case 'msg_lag_avg': return worstMessageLag(s, 'avg');
    case 'msg_lag_p50': return worstMessageLag(s, 'p50');
    case 'msg_lag_p95': return worstMessageLag(s, 'p95');
    case 'msg_lag_p99': return worstMessageLag(s, 'p99');
    case 'msg_lag_max': return worstMessageLag(s, 'max');
  }
}

/**
 * Per-message lag across groups: the worst group decides. A group that left
 * messages unconsumed has no upper bound on their lag, so it fails any limit.
 */
function worstMessageLag(s: RunSummary, key: 'avg' | 'p50' | 'p95' | 'p99' | 'max'): number {
  const groups = s.kafka?.messageLag ?? [];
  if (groups.length === 0) return NaN;
  if (key === 'max' && groups.some((g) => g.pending > 0)) return Number.POSITIVE_INFINITY;
  return Math.max(...groups.map((g) => g[key]));
}

export function evaluate(specs: ThresholdSpec[], summary: RunSummary): ThresholdResult[] {
  return specs.map((spec) => {
    const p = parseThreshold(spec.expr);
    if (!p) {
      return { expr: spec.expr, metric: 'invalid', actual: NaN, passed: false };
    }
    const actual = metricValue(p.metric, summary);
    // Unmeasured (NaN) never passes — not even `!=`, which NaN would satisfy.
    const passed = !Number.isNaN(actual) && compare(actual, p.op, p.value);
    return { expr: spec.expr, metric: p.metric, actual, passed };
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
