import { isAbsolute } from 'node:path';
import { z } from 'zod';

/** Server-side validation. The UI renders its forms from the same field set. */

/**
 * Names and values arrive with edge whitespace more often than anyone expects —
 * pasted URLs carry a newline, a copied header name a leading space, and both
 * are sent verbatim. Trimming happens here as well as in the form, so an import
 * or a direct API call is cleaned the same way.
 *
 * Deliberately *not* trimmed: passwords, request bodies, message payloads and
 * script content, where an edge space can be part of the value.
 */
const trimmed = z.string().trim();

const kv = z.record(z.string(), z.string()).transform((rec) => {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(rec)) {
    const key = k.trim();
    if (key) out[key] = v.trim();
  }
  return out;
});

export const restSchema = z.object({
  url: trimmed.url(),
  method: z.enum(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD']),
  headers: kv,
  body: z.string(),
  bodyType: z.enum(['none', 'json', 'raw', 'form']),
  auth: z.object({
    kind: z.enum(['none', 'basic', 'bearer']),
    username: trimmed.optional(),
    // The password keeps its edge whitespace; a token never has any on purpose.
    password: z.string().optional(),
    token: trimmed.optional(),
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

const socketFlowSchema = z.array(z.object({
  kind: z.enum(['send', 'think', 'expect', 'emit', 'listen']),
  value: z.string(),
  event: trimmed.max(200).optional(),
  acknowledge: z.boolean().optional(),
  matchPath: trimmed.max(200).optional(),
  matchValue: trimmed.optional(),
  namespace: trimmed.max(200).optional(),
}));

export const socketSchema = z.object({
  // Defaulted, not required: socket profiles saved before the Socket.IO engine
  // existed carry none of these fields and must keep loading.
  engine: z.enum(['ws', 'socketio']).default('ws'),
  // Socket.IO is reached over http(s); raw WebSocket over ws(s).
  url: trimmed.regex(/^(wss?|https?):\/\//, 'must start with ws://, wss://, http:// or https://'),
  headers: kv,
  subprotocols: z.array(trimmed),
  namespace: trimmed.max(200).default(''),
  query: kv.default({}),
  transports: z.array(trimmed).default([]),
  phases: z.array(z.object({
    name: trimmed,
    durationSec: z.number().int().min(1),
    arrivalRate: z.number().int().min(1),
    rampTo: z.number().int().min(1).optional(),
  })).min(1),
  // Legacy single-flow shape, folded into `scenarios` by the transform below so
  // nothing downstream has to know both shapes.
  flow: socketFlowSchema.optional(),
  scenarios: z.array(z.object({
    name: trimmed.max(120).default('socket'),
    weight: z.number().min(0).max(10_000).optional(),
    flow: socketFlowSchema,
  })).default([]),
  measureRoundTrip: z.boolean(),
}).transform(({ flow, ...rest }) => ({
  ...rest,
  scenarios: rest.scenarios.length ? rest.scenarios : [{ name: 'socket', flow: flow ?? [] }],
}));

export const kafkaSchema = z.object({
  bootstrapServers: trimmed.min(1),
  topic: trimmed.min(1),
  librdkafka: kv,
  acks: z.enum(['0', '1', 'all']),
  compression: z.enum(['none', 'gzip', 'snappy', 'lz4', 'zstd']),
  keyStrategy: z.enum(['none', 'random', 'fixed', 'sequence']),
  keyValue: trimmed,
  payloadType: z.enum(['json', 'raw', 'random']),
  payload: z.string(),
  payloadSizeBytes: z.number().int().min(1).max(10_000_000),
  producers: z.number().int().min(1).max(64),
  targetRate: z.number().int().min(1).max(1_000_000),
  durationSec: z.number().int().min(1).max(86_400),
  maxMessages: z.number().int().min(0),
  latencyMode: z.enum(['produce-ack', 'end-to-end']),
  consumerGroup: trimmed,
  monitorLag: z.boolean(),
});

export const checkSchema = z.object({
  name: trimmed.min(1),
  kind: z.enum(['status', 'body_contains', 'json_path', 'regex', 'latency_under']),
  value: trimmed,
  path: trimmed.optional(),
  minPassRatePct: z.number().min(0).max(100).optional(),
});

/** Names the runner sets itself; a script variable of the same name would collide. */
const RESERVED_ENV = new Set(['CFG', 'SUMMARY_OUT']);
const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
/** These become argv entries, so an unbounded value would blow the exec limit. */
const MAX_ENV_VALUE = 8192;

export const scriptSchema = z.object({
  mode: z.enum(['builtin', 'inline', 'path']),
  content: z.string().max(1_000_000),
  path: trimmed.max(4096),
  filename: trimmed.max(255),
  env: kv.default({}),
}).superRefine((s, ctx) => {
  for (const [key, value] of Object.entries(s.env)) {
    if (!ENV_NAME.test(key)) {
      ctx.addIssue({ code: 'custom', message: `invalid environment variable name: ${key}` });
    } else if (RESERVED_ENV.has(key)) {
      ctx.addIssue({ code: 'custom', message: `${key} is set by the runner and cannot be overridden` });
    }
    if (value.length > MAX_ENV_VALUE) {
      ctx.addIssue({ code: 'custom', message: `environment variable ${key} is too long (limit ${MAX_ENV_VALUE})` });
    }
  }
});

export const runConfigSchema = z.object({
  protocol: z.enum(['rest', 'socket', 'kafka']),
  script: scriptSchema.optional(),
  rest: restSchema.optional(),
  socket: socketSchema.optional(),
  kafka: kafkaSchema.optional(),
  checks: z.array(checkSchema),
  thresholds: z.array(z.object({ expr: trimmed.min(1) })),
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
  name: trimmed.min(1).max(120),
  protocol: z.enum(['rest', 'socket', 'kafka']),
  config: runConfigSchema,
});

export const settingsSchema = z.object({
  language: z.enum(['en', 'th']).optional(),
  theme: z.enum(['dark', 'light']).optional(),
  csvDelimiter: z.enum([',', ';', '\t']).optional(),
  csvLanguage: z.enum(['en', 'th']).optional(),
  k6Path: trimmed.min(1).optional(),
  artilleryPath: trimmed.min(1).optional(),
  retentionRuns: z.number().int().min(1).max(10_000).optional(),
  kafkaMonitorIntervalSec: z.number().int().min(1).max(300).optional(),
});

export const kafkaAuthSchema = z.object({
  securityProtocol: z.enum(['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']),
  saslMechanism: z.enum(['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'GSSAPI', 'OAUTHBEARER']),
  username: trimmed.max(500),
  password: z.string().max(2000),
  sslCaLocation: trimmed.max(4096),
  sslSkipVerify: z.boolean(),
  extra: z.record(z.string(), z.string()),
});
