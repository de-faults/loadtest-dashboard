import { isAbsolute } from 'node:path';
import { z } from 'zod';

/** Server-side validation. The UI renders its forms from the same field set. */

const kv = z.record(z.string(), z.string());

export const restSchema = z.object({
  url: z.string().url(),
  method: z.enum(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD']),
  headers: kv,
  body: z.string(),
  bodyType: z.enum(['none', 'json', 'raw', 'form']),
  auth: z.object({
    kind: z.enum(['none', 'basic', 'bearer']),
    username: z.string().optional(),
    password: z.string().optional(),
    token: z.string().optional(),
  }),
  timeoutSec: z.number().int().min(1).max(600),
  followRedirects: z.boolean(),
  insecureSkipTlsVerify: z.boolean(),
  thinkTimeMs: z.number().int().min(0).max(60_000),
  loadModel: z.enum(['vus', 'stages', 'rate']),
  vus: z.number().int().min(1).max(100_000),
  vusDurationSec: z.number().int().min(1).max(86_400),
  stages: z.array(z.object({ duration: z.number().int().min(1), target: z.number().int().min(0) })).min(1),
  rate: z.number().int().min(1),
  rateDurationSec: z.number().int().min(1),
  preAllocatedVUs: z.number().int().min(1),
});

export const socketSchema = z.object({
  // Defaulted, not required: socket profiles saved before the Socket.IO engine
  // existed carry none of these fields and must keep loading.
  engine: z.enum(['ws', 'socketio']).default('ws'),
  // Socket.IO is reached over http(s); raw WebSocket over ws(s).
  url: z.string().regex(/^(wss?|https?):\/\//, 'must start with ws://, wss://, http:// or https://'),
  headers: kv,
  subprotocols: z.array(z.string()),
  namespace: z.string().max(200).default(''),
  query: kv.default({}),
  transports: z.array(z.string()).default([]),
  phases: z.array(z.object({
    name: z.string(),
    durationSec: z.number().int().min(1),
    arrivalRate: z.number().int().min(1),
    rampTo: z.number().int().min(1).optional(),
  })).min(1),
  flow: z.array(z.object({
    kind: z.enum(['send', 'think', 'expect', 'emit', 'listen']),
    value: z.string(),
    event: z.string().max(200).optional(),
    acknowledge: z.boolean().optional(),
    matchPath: z.string().max(200).optional(),
    matchValue: z.string().optional(),
    namespace: z.string().max(200).optional(),
  })),
  measureRoundTrip: z.boolean(),
});

export const kafkaSchema = z.object({
  bootstrapServers: z.string().min(1),
  topic: z.string().min(1),
  librdkafka: kv,
  acks: z.enum(['0', '1', 'all']),
  compression: z.enum(['none', 'gzip', 'snappy', 'lz4', 'zstd']),
  keyStrategy: z.enum(['none', 'random', 'fixed', 'sequence']),
  keyValue: z.string(),
  payloadType: z.enum(['json', 'raw', 'random']),
  payload: z.string(),
  payloadSizeBytes: z.number().int().min(1).max(10_000_000),
  producers: z.number().int().min(1).max(64),
  targetRate: z.number().int().min(1).max(1_000_000),
  durationSec: z.number().int().min(1).max(86_400),
  maxMessages: z.number().int().min(0),
  latencyMode: z.enum(['produce-ack', 'end-to-end']),
  consumerGroup: z.string(),
  monitorLag: z.boolean(),
});

export const checkSchema = z.object({
  name: z.string().min(1),
  kind: z.enum(['status', 'body_contains', 'json_path', 'regex', 'latency_under']),
  value: z.string(),
  path: z.string().optional(),
  minPassRatePct: z.number().min(0).max(100).optional(),
});

export const scriptSchema = z.object({
  mode: z.enum(['builtin', 'inline', 'path']),
  content: z.string().max(1_000_000),
  path: z.string().max(4096),
  filename: z.string().max(255),
});

export const runConfigSchema = z.object({
  protocol: z.enum(['rest', 'socket', 'kafka']),
  script: scriptSchema.optional(),
  rest: restSchema.optional(),
  socket: socketSchema.optional(),
  kafka: kafkaSchema.optional(),
  checks: z.array(checkSchema),
  thresholds: z.array(z.object({ expr: z.string().min(1) })),
}).superRefine((c, ctx) => {
  if (c.protocol === 'rest' && !c.rest) ctx.addIssue({ code: 'custom', message: 'rest config required' });
  if (c.protocol === 'socket' && !c.socket) ctx.addIssue({ code: 'custom', message: 'socket config required' });
  if (c.protocol === 'kafka' && !c.kafka) ctx.addIssue({ code: 'custom', message: 'kafka config required' });
  if (c.script?.mode === 'inline' && !c.script.content.trim()) {
    ctx.addIssue({ code: 'custom', message: 'inline script is empty' });
  }
  if (c.script?.mode === 'path') {
    if (!c.script.path.trim()) ctx.addIssue({ code: 'custom', message: 'script path is empty' });
    else if (!isAbsolute(c.script.path)) ctx.addIssue({ code: 'custom', message: 'script path must be absolute' });
  }
});

export const profileSchema = z.object({
  id: z.string().optional(),
  name: z.string().min(1).max(120),
  protocol: z.enum(['rest', 'socket', 'kafka']),
  config: runConfigSchema,
});

export const settingsSchema = z.object({
  language: z.enum(['en', 'th']).optional(),
  theme: z.enum(['dark', 'light']).optional(),
  csvDelimiter: z.enum([',', ';', '\t']).optional(),
  csvLanguage: z.enum(['en', 'th']).optional(),
  k6Path: z.string().min(1).optional(),
  artilleryPath: z.string().min(1).optional(),
  retentionRuns: z.number().int().min(1).max(10_000).optional(),
  kafkaMonitorIntervalSec: z.number().int().min(1).max(300).optional(),
});

export const kafkaAuthSchema = z.object({
  securityProtocol: z.enum(['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']),
  saslMechanism: z.enum(['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'GSSAPI', 'OAUTHBEARER']),
  username: z.string().max(500),
  password: z.string().max(2000),
  sslCaLocation: z.string().max(4096),
  sslSkipVerify: z.boolean(),
  extra: z.record(z.string(), z.string()),
});
