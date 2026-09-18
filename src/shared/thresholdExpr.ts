/**
 * The threshold expression DSL, on its own so both sides of the wire can read
 * it: the server evaluates a finished run against it, and the browser reads the
 * same expressions as the service-level objective a capacity ramp is judged by.
 *
 * Kept free of any Node import — `web/` compiles this file directly.
 */

export const METRICS = [
  'min', 'avg', 'p90', 'p95', 'p99', 'max',
  'rps', 'tps', 'vus',
  'success_rate', 'error_rate',
  'total_requests', 'duration_s',
  // Kafka only — see KafkaRunSummary. Any other run reads them as unmeasured.
  'lag_max', 'lag_avg', 'lag_end', 'lag_final', 'lag_growth', 'lag_drain_s',
  'total_mb', 'mb_per_s',
  'msg_lag_avg', 'msg_lag_p50', 'msg_lag_p95', 'msg_lag_p99', 'msg_lag_max',
] as const;

export type MetricName = (typeof METRICS)[number];

export const OPS = ['<=', '>=', '<', '>', '==', '!='] as const;
export type Op = (typeof OPS)[number];

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

export function compare(actual: number, op: Op, expected: number): boolean {
  switch (op) {
    case '<': return actual < expected;
    case '<=': return actual <= expected;
    case '>': return actual > expected;
    case '>=': return actual >= expected;
    case '==': return actual === expected;
    case '!=': return actual !== expected;
  }
}
