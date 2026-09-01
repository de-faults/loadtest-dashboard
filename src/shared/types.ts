/**
 * Normalized metric contract.
 *
 * Every runner (k6 / artillery / kafka) emits these exact shapes so the UI,
 * threshold engine and CSV exporter never need to know which protocol ran.
 */

export type Protocol = "rest" | "socket" | "kafka";
export type RunState =
  | "queued"
  | "running"
  | "passed"
  | "failed"
  | "error"
  | "stopped";

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

/**
 * Attribution for a failed response: which hop answered. A run aimed at a
 * service usually passes through a gateway (Azure APIM, APISIX/API7, an
 * ingress, a CDN) that can answer in the service's place, and a status code
 * alone never says which one did.
 */
export interface ErrorOrigin {
  /** `gateway`: a hop in front answered. `service`: the target itself did. */
  verdict: "gateway" | "service" | "unknown";
  /** Best name for whoever produced the response, when one is identifiable. */
  by?: string;
  /** The proxy detected in the path, whether or not it wrote this response. */
  gateway?: string;
  /** Header lines and body markers the verdict was drawn from. */
  evidence: string[];
  /** Correlation ids for finding this call in the gateway's own logs. */
  traceIds?: Record<string, string>;
  /** Address that actually answered — a gateway VIP is not the service host. */
  remoteIp?: string;
  remotePort?: number;
  proto?: string;
  /** Final URL, after any redirect. */
  url?: string;
}

export interface ErrorBucket {
  kind: string;
  count: number;
  sample: string;
  /**
   * The response the target actually sent for the first request that failed
   * this way — truncated. A status code alone rarely says why a load test is
   * failing; the error payload usually does.
   */
  body?: string;
  /** Content-Type of `body`, when the target sent one. */
  bodyContentType?: string;
  /**
   * Response headers of that same first failure. Values of credential-bearing
   * headers (Set-Cookie, Authorization, anything token/secret/key-shaped) are
   * stored as `***`: the summary is persisted and exported.
   */
  responseHeaders?: Record<string, string>;
  /** Characters the target sent, counted before truncation. */
  bodyChars?: number;
  /** `body` holds only the head of the payload — the rest was dropped. */
  bodyTruncated?: boolean;
  /** Who answered: the service under test, or a gateway in front of it. */
  origin?: ErrorOrigin;
}

/**
 * Any k6 metric outside the built-in set (custom Counter/Gauge/Rate/Trend from
 * a bring-your-own script). `values` is whatever numeric fields k6's own
 * summary reported for it (e.g. `count`/`rate` for a Counter, `p(95)` for a
 * Trend) — passed through as-is rather than guessing a fixed shape per type.
 */
export interface CustomMetricResult {
  name: string;
  type: string;
  values: Record<string, number>;
}

/**
 * How the load divided between named scenarios, for scripts that run more than
 * one. The fields present depend on what the tool reports: Artillery gives only
 * a virtual-user count per scenario, while k6 tags every sample with its
 * scenario, so its rows carry the scenario's own requests and percentile too.
 */
export interface ScenarioStat {
  name: string;
  /** Virtual users the scenario ran. */
  vusers?: number;
  /** Requests attributed to the scenario. */
  requests?: number;
  /** Share of this scenario's own requests that succeeded. */
  successRatePct?: number;
  /** p95 of this scenario's own requests. */
  p95?: number;
  /** Share of the run — by requests where they were counted, else by virtual users. */
  sharePct: number;
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
  /** Custom metrics from a bring-your-own script; empty for UI-built runs. */
  customMetrics: CustomMetricResult[];
  /** Load split across named scenarios; empty unless the runner reports one. */
  scenarios: ScenarioStat[];
  verdict: "pass" | "fail";
}

export type RunEvent =
  | {
      t: "start";
      runId: string;
      startedAt: number;
      protocol: Protocol;
      profileName: string;
      target: string;
    }
  | { t: "tick"; runId: string; window: WindowMetrics }
  | { t: "check"; runId: string; name: string; passed: number; failed: number }
  | {
      t: "error";
      runId: string;
      ts: number;
      kind: string;
      message: string;
      count: number;
      body?: string;
    }
  | {
      t: "log";
      runId: string;
      ts: number;
      level: "info" | "warn" | "error";
      line: string;
    }
  | {
      t: "end";
      runId: string;
      endedAt: number;
      state: RunState;
      summary: RunSummary;
    }
  | { t: "kafka-monitor"; runId: string | null; payload: KafkaMonitorPayload };

// ─── Run configuration ───────────────────────────────────────────────────────

export interface ThresholdSpec {
  /** e.g. "p95 < 500", "success_rate > 99" */
  expr: string;
}

export interface CheckSpec {
  name: string;
  /** status | body_contains | json_path | regex | latency_under */
  kind: "status" | "body_contains" | "json_path" | "regex" | "latency_under";
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
  method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD";
  headers: Record<string, string>;
  body: string;
  bodyType: "none" | "json" | "raw" | "form";
  auth: {
    kind: "none" | "basic" | "bearer";
    username?: string;
    password?: string;
    token?: string;
  };
  timeoutSec: number;
  followRedirects: boolean;
  insecureSkipTlsVerify: boolean;
  thinkTimeMs: number;
  /**
   * 'vus'    = a fixed number of VUs for a fixed duration (k6 vus/duration)
   * 'stages' = ramping VUs through a list of stages
   * 'rate'   = constant arrival rate, independent of response time
   */
  loadModel: "vus" | "stages" | "rate";
  /** loadModel 'vus' */
  vus: number;
  vusDurationSec: number;
  /** loadModel 'stages' */
  stages: Stage[];
  /** loadModel 'rate' */
  rate: number;
  rateDurationSec: number;
  preAllocatedVUs: number;
}

export interface SocketFlowStep {
  /**
   * ws engine:       send | think | expect
   * socket.io engine: emit | think | listen
   */
  kind: "send" | "think" | "expect" | "emit" | "listen";
  /** send/emit: payload · think: seconds · expect: substring */
  value: string;
  /** emit/listen: the Socket.IO event name (Artillery calls it the channel). */
  event?: string;
  /** emit: wait for the server's acknowledgement callback before continuing. */
  acknowledge?: boolean;
  /** emit-ack / listen: JSON path to assert on, e.g. "$.status". */
  matchPath?: string;
  /** Expected value at matchPath. Empty means "any response is fine". */
  matchValue?: string;
  /** Per-step namespace override; falls back to the connection namespace. */
  namespace?: string;
}

/**
 * One named flow in a socket profile. Every scenario shares the connection
 * settings and phases of its `SocketConfig`; Artillery picks one per virtual
 * user, so several of them split the same load between different journeys.
 */
export interface SocketScenario {
  name: string;
  /** Relative share of virtual users — Artillery's own `weight`. 1 when unset. */
  weight?: number;
  flow: SocketFlowStep[];
}

export interface SocketConfig {
  /** 'ws' = raw WebSocket, 'socketio' = Socket.IO (named events + acks). */
  engine: "ws" | "socketio";
  url: string;
  headers: Record<string, string>;
  subprotocols: string[];
  /** Socket.IO only: connection namespace, e.g. "/chat". */
  namespace: string;
  /** Socket.IO only: handshake query parameters (token, room, ...). */
  query: Record<string, string>;
  /** Socket.IO only: restrict transports, e.g. ["websocket"]. */
  transports: string[];
  phases: Array<{
    name: string;
    durationSec: number;
    arrivalRate: number;
    rampTo?: number;
  }>;
  scenarios: SocketScenario[];
  /**
   * Single-flow shape of profiles saved before scenarios existed. Never written
   * any more — read every flow through `socketScenarios()`, which migrates it.
   */
  flow?: SocketFlowStep[];
  /** Measure round-trip by pairing each send with the next expect. */
  measureRoundTrip: boolean;
}

export interface KafkaConfig {
  bootstrapServers: string;
  topic: string;
  /** Raw librdkafka properties, merged last over everything else. */
  librdkafka: Record<string, string>;
  acks: "0" | "1" | "all";
  compression: "none" | "gzip" | "snappy" | "lz4" | "zstd";
  keyStrategy: "none" | "random" | "fixed" | "sequence";
  keyValue: string;
  payloadType: "json" | "raw" | "random";
  payload: string;
  payloadSizeBytes: number;
  producers: number;
  targetRate: number;
  durationSec: number;
  /** 0 = unlimited, stop on duration instead. */
  maxMessages: number;
  latencyMode: "produce-ack" | "end-to-end";
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
  mode: "builtin" | "inline" | "path";
  /** Script body for `inline`. */
  content: string;
  /** Absolute path for `path`. */
  path: string;
  /** Original filename, kept for display only. */
  filename: string;
  /**
   * `__ENV` variables handed to the script (k6 `--env`, Artillery's process
   * environment). A parameterised script reads its target, rates and duration
   * from here, so one script serves every run without being edited.
   *
   * Absent on profiles saved before the field existed — read it through
   * `scriptEnv()`, which fills in the empty case.
   */
  env: Record<string, string>;
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

export interface PartitionLag {
  partition: number;
  latest: number;
  committed: number;
  lag: number;
}
export interface GroupTopicLag {
  topic: string;
  totalLag: number;
  partitions: PartitionLag[];
}
export interface GroupInfo {
  groupId: string;
  state: string;
  memberCount: number;
  topics: GroupTopicLag[];
  totalLag: number;
}
export interface TopicInfo {
  name: string;
  partitionCount: number;
  endOffsetSum: number;
}

/**
 * Broker authentication for the monitor. Values are passed through to
 * librdkafka; the password never leaves the server in any status or event.
 */
export interface KafkaAuth {
  securityProtocol: "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL";
  saslMechanism:
    | "PLAIN"
    | "SCRAM-SHA-256"
    | "SCRAM-SHA-512"
    | "GSSAPI"
    | "OAUTHBEARER";
  username: string;
  password: string;
  /** CA bundle path for SSL / SASL_SSL. */
  sslCaLocation: string;
  /** Skip broker certificate hostname verification. */
  sslSkipVerify: boolean;
  /** Anything else librdkafka accepts. */
  extra: Record<string, string>;
}

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
  language: "en" | "th";
  theme: "dark" | "light";
  csvDelimiter: "," | ";" | "\t";
  csvLanguage: "en" | "th";
  k6Path: string;
  artilleryPath: string;
  retentionRuns: number;
  kafkaMonitorIntervalSec: number;
}

// ─── External tools ──────────────────────────────────────────────────────────

export type ToolId = "k6" | "artillery";
/** Platforms with their own install recipes; anything else is "other". */
export type ToolPlatform = "darwin" | "win32" | "linux" | "other";

export interface ToolInstallMethod {
  id: string;
  /** Package manager behind it, e.g. "Homebrew". */
  label: string;
  /** The recipe as shell text — for the copy button, never sent back to run. */
  command: string;
  /** False when the dashboard cannot run it unattended; `reason` says why. */
  runnable: boolean;
  reason?: string;
  note?: string;
  needsSudo: boolean;
}

export interface ToolStatus {
  id: ToolId;
  label: string;
  /** The binary a run would spawn — the configured path, not just the name. */
  binPath: string;
  docsUrl: string;
  available: boolean;
  detail: string;
  methods: ToolInstallMethod[];
}

export interface ToolsInfo {
  platform: ToolPlatform;
  tools: ToolStatus[];
}

export interface InstallResult {
  /** The tool answered its version afterwards — the only proof that counts. */
  ok: boolean;
  /** Exit code of the last step, or null when it could not be spawned. */
  code: number | null;
  output: string[];
  status: ToolStatus;
}

export interface RunnerAvailability {
  rest: { available: boolean; detail: string };
  socket: { available: boolean; detail: string };
  kafka: { available: boolean; detail: string };
}
