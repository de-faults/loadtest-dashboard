import type { KafkaConfig, Protocol, RestConfig, RunConfig, SocketConfig } from './types.ts';

export const DEFAULT_REST: RestConfig = {
  url: 'https://httpbin.org/get',
  method: 'GET',
  headers: { Accept: 'application/json' },
  body: '',
  bodyType: 'none',
  auth: { kind: 'none' },
  timeoutSec: 30,
  followRedirects: true,
  insecureSkipTlsVerify: false,
  thinkTimeMs: 0,
  loadModel: 'vus',
  vus: 10,
  vusDurationSec: 60,
  stages: [
    { duration: 10, target: 10 },
    { duration: 30, target: 50 },
    { duration: 10, target: 0 },
  ],
  rate: 100,
  rateDurationSec: 60,
  preAllocatedVUs: 50,
};

export const DEFAULT_SOCKET: SocketConfig = {
  url: 'ws://localhost:8080',
  headers: {},
  subprotocols: [],
  phases: [
    { name: 'warmup', durationSec: 10, arrivalRate: 5 },
    { name: 'ramp', durationSec: 30, arrivalRate: 5, rampTo: 50 },
  ],
  flow: [
    { kind: 'send', value: '{"type":"ping"}' },
    { kind: 'think', value: '1' },
  ],
  measureRoundTrip: false,
};

export const DEFAULT_KAFKA: KafkaConfig = {
  bootstrapServers: 'localhost:9092',
  topic: 'loadtest',
  librdkafka: { 'linger.ms': '5', 'batch.size': '100000' },
  acks: '1',
  compression: 'none',
  keyStrategy: 'sequence',
  keyValue: '',
  payloadType: 'json',
  payload: '{"seq":{{seq}},"ts":{{ts}},"id":"{{uuid}}"}',
  payloadSizeBytes: 512,
  producers: 1,
  targetRate: 1000,
  durationSec: 60,
  maxMessages: 0,
  latencyMode: 'produce-ack',
  consumerGroup: '',
  monitorLag: true,
};

export function defaultConfig(protocol: Protocol): RunConfig {
  const base: RunConfig = {
    protocol,
    script: { mode: 'builtin', content: '', path: '', filename: '' },
    checks: [],
    thresholds: [{ expr: 'p95 < 500' }, { expr: 'success_rate > 99' }],
  };
  if (protocol === 'rest') {
    base.rest = structuredClone(DEFAULT_REST);
    base.checks = [{ name: 'status is 200', kind: 'status', value: '200', minPassRatePct: 99 }];
  }
  if (protocol === 'socket') {
    base.socket = structuredClone(DEFAULT_SOCKET);
  }
  if (protocol === 'kafka') {
    base.kafka = structuredClone(DEFAULT_KAFKA);
    base.checks = [{ name: 'ack under 200ms', kind: 'latency_under', value: '200', minPassRatePct: 95 }];
    base.thresholds = [{ expr: 'p99 < 1000' }, { expr: 'success_rate > 99.9' }];
  }
  return base;
}

/** librdkafka keys surfaced as suggestions in the UI; any key is still allowed. */
export const LIBRDKAFKA_HINTS = [
  'linger.ms', 'batch.size', 'batch.num.messages', 'compression.type', 'acks',
  'queue.buffering.max.messages', 'queue.buffering.max.kbytes', 'message.max.bytes',
  'request.timeout.ms', 'delivery.timeout.ms', 'retries', 'enable.idempotence',
  'security.protocol', 'sasl.mechanism', 'sasl.username', 'sasl.password',
  'ssl.ca.location', 'socket.keepalive.enable', 'client.id',
];

/** Common request headers offered as autocomplete; any header is still allowed. */
export const HTTP_HEADER_HINTS = [
  'Accept', 'Accept-Encoding', 'Accept-Language', 'Authorization', 'Cache-Control',
  'Connection', 'Content-Type', 'Cookie', 'Host', 'Idempotency-Key',
  'If-Match', 'If-None-Match', 'If-Modified-Since', 'Origin', 'Pragma', 'Range',
  'Referer', 'User-Agent', 'X-API-Key', 'X-Correlation-ID', 'X-Forwarded-For',
  'X-Request-ID', 'X-Trace-Id',
];

/**
 * Value autocomplete for headers whose values are a small known set. Lookup is
 * case-insensitive because header names are.
 */
export const HTTP_HEADER_VALUE_HINTS: Record<string, string[]> = {
  'accept': ['application/json', 'application/xml', 'text/plain', '*/*'],
  'content-type': [
    'application/json',
    'application/x-www-form-urlencoded',
    'multipart/form-data',
    'text/plain',
    'application/xml',
  ],
  'accept-encoding': ['gzip', 'gzip, deflate', 'gzip, deflate, br', 'identity'],
  'cache-control': ['no-cache', 'no-store', 'max-age=0'],
  'connection': ['keep-alive', 'close'],
  'authorization': ['Bearer ', 'Basic '],
};
