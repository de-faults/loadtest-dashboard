/**
 * Normalized metric contract.
 *
 * Every runner (k6 / artillery / kafka) emits these exact shapes so the UI,
 * threshold engine and CSV exporter never need to know which protocol ran.
 */

export type Protocol = 'rest' | 'socket' | 'kafka';
export type RunState = 'queued' | 'running' | 'passed' | 'failed' | 'error' | 'stopped';

export interface LatencyProfile {
  min: number;
  avg: number;
  p90: number;
  p95: number;
  p99: number;
  max: number;
}

/** One 1-second bucket of the run. */
export interface WindowMetrics {
  /** Wall-clock ms at the end of the bucket. */
  ts: number;
  /** Seconds since run start. */
  elapsed: number;
  requests: number;
  success: number;
  failed: number;
  /** REST / socket: requests per second. */
  rps: number;
  /** Kafka: acknowledged messages per second. */
  tps: number;
  /** Active virtual users / producers. */
  vus: number;
  latency: LatencyProfile;
  /** Kafka runs only: total consumer lag observed in this bucket. */
  consumerLag?: number;
}

export interface CheckResult {
  name: string;
  passed: number;
  failed: number;
  passRatePct: number;
}

export interface ThresholdResult {
  expr: string;
  metric: string;
  actual: number;
  passed: boolean;
}

export interface ErrorBucket {
  kind: string;
  count: number;
  sample: string;
}

export interface RunSummary {
  runId: string;
  protocol: Protocol;
  profileName: string;
  target: string;
  startedAt: number;
  endedAt: number;
  durationMs: number;
  totalRequests: number;
  totalSuccess: number;
  totalFailed: number;
  successRatePct: number;
  rpsAvg: number;
  rpsPeak: number;
  tpsAvg: number;
  tpsPeak: number;
  vusMax: number;
  /** Whole-run percentiles, merged from the histogram — never averaged per window. */
  latency: LatencyProfile;
  checks: CheckResult[];
  thresholds: ThresholdResult[];
  errors: ErrorBucket[];
  verdict: 'pass' | 'fail';
}

export type RunEvent =
  | { t: 'start'; runId: string; startedAt: number; protocol: Protocol; profileName: string; target: string }
  | { t: 'tick'; runId: string; window: WindowMetrics }
  | { t: 'check'; runId: string; name: string; passed: number; failed: number }
  | { t: 'error'; runId: string; ts: number; kind: string; message: string; count: number }
  | { t: 'log'; runId: string; ts: number; level: 'info' | 'warn' | 'error'; line: string }
  | { t: 'end'; runId: string; endedAt: number; state: RunState; summary: RunSummary }
  | { t: 'kafka-monitor'; runId: string | null; payload: KafkaMonitorPayload };

// ─── Run configuration ───────────────────────────────────────────────────────

export interface ThresholdSpec {
  /** e.g. "p95 < 500", "success_rate > 99" */
  expr: string;
}

export interface CheckSpec {
  name: string;
  /** status | body_contains | json_path | regex | latency_under */
  kind: 'status' | 'body_contains' | 'json_path' | 'regex' | 'latency_under';
  value: string;
  /** For json_path only. */
  path?: string;
  /** Minimum pass rate (%) for the run to count as passing. */
  minPassRatePct?: number;
}

export interface Stage {
  /** seconds */
  duration: number;
  target: number;
}

export interface RestConfig {
  url: string;
  method: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE' | 'HEAD';
  headers: Record<string, string>;
  body: string;
  bodyType: 'none' | 'json' | 'raw' | 'form';
  auth: { kind: 'none' | 'basic' | 'bearer'; username?: string; password?: string; token?: string };
  timeoutSec: number;
  followRedirects: boolean;
  insecureSkipTlsVerify: boolean;
  thinkTimeMs: number;
  /** 'stages' = ramping VUs, 'rate' = constant arrival rate. */
  loadModel: 'stages' | 'rate';
  stages: Stage[];
  rate: number;
  rateDurationSec: number;
  preAllocatedVUs: number;
}

export interface SocketFlowStep {
  kind: 'send' | 'think' | 'expect';
  /** payload for send, seconds for think, substring for expect */
  value: string;
}

export interface SocketConfig {
  url: string;
  headers: Record<string, string>;
  subprotocols: string[];
  phases: Array<{ name: string; durationSec: number; arrivalRate: number; rampTo?: number }>;
  flow: SocketFlowStep[];
  /** Measure round-trip by pairing each send with the next expect. */
  measureRoundTrip: boolean;
}

export interface KafkaConfig {
  bootstrapServers: string;
  topic: string;
  /** Raw librdkafka properties, merged last over everything else. */
  librdkafka: Record<string, string>;
  acks: '0' | '1' | 'all';
  compression: 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd';
  keyStrategy: 'none' | 'random' | 'fixed' | 'sequence';
  keyValue: string;
  payloadType: 'json' | 'raw' | 'random';
  payload: string;
  payloadSizeBytes: number;
  producers: number;
  targetRate: number;
  durationSec: number;
  /** 0 = unlimited, stop on duration instead. */
  maxMessages: number;
  latencyMode: 'produce-ack' | 'end-to-end';
  consumerGroup: string;
  /** Poll consumer-group lag during the run and chart it alongside TPS. */
  monitorLag: boolean;
}

/**
 * Bring-your-own script.
 *
 * `builtin`  — the run is driven by the UI-built configuration.
 * `inline`   — the script text is stored in the profile and written to a temp
 *              file at run time. Self-contained scripts only: relative imports
 *              and data files resolve against the temp directory, not your repo.
 * `path`     — an existing file on this host is executed where it lives, so
 *              relative imports, `open()` calls and CSV feeds keep working.
 */
export interface ScriptConfig {
  mode: 'builtin' | 'inline' | 'path';
  /** Script body for `inline`. */
  content: string;
  /** Absolute path for `path`. */
  path: string;
  /** Original filename, kept for display only. */
  filename: string;
}

export interface RunConfig {
  protocol: Protocol;
  script?: ScriptConfig;
  rest?: RestConfig;
  socket?: SocketConfig;
  kafka?: KafkaConfig;
  checks: CheckSpec[];
  thresholds: ThresholdSpec[];
}

export interface Profile {
  id: string;
  name: string;
  protocol: Protocol;
  config: RunConfig;
  createdAt: number;
  updatedAt: number;
}

export interface RunRow {
  id: string;
  profileId: string | null;
  profileName: string;
  protocol: Protocol;
  target: string;
  state: RunState;
  startedAt: number;
  endedAt: number | null;
  summary: RunSummary | null;
}

// ─── Kafka lag monitor ───────────────────────────────────────────────────────

export interface PartitionLag { partition: number; latest: number; committed: number; lag: number }
export interface GroupTopicLag { topic: string; totalLag: number; partitions: PartitionLag[] }
export interface GroupInfo { groupId: string; state: string; memberCount: number; topics: GroupTopicLag[]; totalLag: number }
export interface TopicInfo { name: string; partitionCount: number; endOffsetSum: number }

export interface KafkaMonitorPayload {
  timestamp: string;
  brokersLine: string;
  intervalSec: number;
  topics: TopicInfo[];
  groups: GroupInfo[];
  lagHistory: Record<string, number[]>;
  errors: string[];
}

// ─── Settings ────────────────────────────────────────────────────────────────

export interface AppSettings {
  language: 'en' | 'th';
  theme: 'dark' | 'light';
  csvDelimiter: ',' | ';' | '\t';
  csvLanguage: 'en' | 'th';
  k6Path: string;
  artilleryPath: string;
  retentionRuns: number;
  kafkaMonitorIntervalSec: number;
}

export interface RunnerAvailability {
  rest: { available: boolean; detail: string };
  socket: { available: boolean; detail: string };
  kafka: { available: boolean; detail: string };
}
