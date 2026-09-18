import type { Aggregator, ErrorBody } from "../metrics/aggregator.ts";
import type {
  CustomMetricResult,
  GroupInfo,
  KafkaRunSummary,
  RunConfig,
  ThresholdSpec,
} from "../shared/types.ts";

export interface RunnerContext {
  runId: string;
  config: RunConfig;
  agg: Aggregator;
  /** Push a line to the live log tail (also persisted). */
  log(level: "info" | "warn" | "error", line: string): void;
  /** Record a protocol-level failure, bucketed by kind. */
  error(kind: string, message: string): void;
  /**
   * Attach what the target actually replied to the bucket `kind`, so a failing
   * run shows the error payload and not just its status code. Does not count
   * as a failure on its own — `error()` does that.
   */
  errorBody(kind: string, body: ErrorBody): void;
  /** Kafka: push the latest per-partition lag snapshot to the live view. */
  lagSnapshot(groups: GroupInfo[]): void;
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
  /**
   * Thresholds declared by the script itself rather than the profile. Evaluated
   * server-side exactly like the profile's own.
   */
  extraThresholds?: ThresholdSpec[];
  /**
   * The part of the run that generated load, when it ended before the run did
   * (a post-load drain wait). Average throughput is taken over this instead.
   */
  loadDurationMs?: number;
  /** Kafka only: volume and consumer-lag rollup. */
  kafka?: KafkaRunSummary;
}

export interface Runner {
  readonly protocol: "rest" | "socket" | "kafka";
  /** Cheap capability probe shown in the UI before anyone hits Run. */
  available(): Promise<{ available: boolean; detail: string }>;
  run(ctx: RunnerContext): Promise<RunnerResult>;
}
