import type { Aggregator } from "../metrics/aggregator.ts";
import type { CustomMetricResult, RunConfig } from "../shared/types.ts";

export interface RunnerContext {
  runId: string;
  config: RunConfig;
  agg: Aggregator;
  /** Push a line to the live log tail (also persisted). */
  log(level: "info" | "warn" | "error", line: string): void;
  /** Record a protocol-level failure, bucketed by kind. */
  error(kind: string, message: string): void;
  /** Resolves when the user pressed Stop. */
  signal: AbortSignal;
}

export interface RunnerResult {
  /** Runner-native pass/fail, when the tool has an opinion (k6 exit code). */
  nativeVerdict?: "pass" | "fail";
  /** Extra thresholds the runner evaluated itself. */
  nativeThresholds?: Array<{
    expr: string;
    metric: string;
    actual: number;
    passed: boolean;
  }>;
  /** Custom (non-built-in) metrics the runner's own summary reported. */
  nativeCustomMetrics?: CustomMetricResult[];
}

export interface Runner {
  readonly protocol: "rest" | "socket" | "kafka";
  /** Cheap capability probe shown in the UI before anyone hits Run. */
  available(): Promise<{ available: boolean; detail: string }>;
  run(ctx: RunnerContext): Promise<RunnerResult>;
}
