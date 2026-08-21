import { createRequire } from 'node:module';
import { randomBytes, randomUUID } from 'node:crypto';
import type { Runner, RunnerContext, RunnerResult } from './types.ts';
import type { KafkaConfig } from '../shared/types.ts';
import { startLagSampler } from '../kafka/monitor.ts';
import { materializeScript, usesCustomScript } from './script.ts';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const require = createRequire(import.meta.url);

/**
 * Kafka load runner.
 *
 * No external binary: produces in-process with @confluentinc/kafka-javascript
 * (KafkaJS-compatible surface) while passing raw librdkafka properties straight
 * through, which is the whole reason for choosing this client over kafkajs.
 */

export interface KafkaJsModule {
  KafkaJS: {
    Kafka: new (config: Record<string, unknown>) => {
      producer(config?: Record<string, unknown>): KafkaProducer;
      consumer(config?: Record<string, unknown>): KafkaConsumer;
      admin(config?: Record<string, unknown>): unknown;
    };
  };
}

interface KafkaProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(record: {
    topic: string;
    messages: Array<{ key?: string | null; value: string | Buffer; headers?: Record<string, string> }>;
  }): Promise<unknown>;
}

interface KafkaConsumer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(opts: { topics: string[] }): Promise<void>;
  run(opts: { eachMessage: (p: { message: { headers?: Record<string, Buffer | string> } }) => Promise<void> }): Promise<void>;
}

const silentLogger = {
  info: () => {}, warn: () => {}, error: () => {}, debug: () => {},
  namespace: () => silentLogger, setLogLevel: () => {},
};

export function loadKafkaJs(): KafkaJsModule['KafkaJS'] {
  const mod = require('@confluentinc/kafka-javascript') as KafkaJsModule;
  return mod.KafkaJS;
}

export const kafkaRunner: Runner = {
  protocol: 'kafka',

  async available() {
    try {
      loadKafkaJs();
      const pkg = require('@confluentinc/kafka-javascript/package.json') as { version: string };
      return { available: true, detail: `@confluentinc/kafka-javascript ${pkg.version}` };
    } catch (err) {
      return { available: false, detail: `native binding unavailable: ${(err as Error).message.split('\n')[0]}` };
    }
  },

  async run(ctx: RunnerContext): Promise<RunnerResult> {
    const cfg = ctx.config.kafka;
    if (!cfg) throw new Error('kafka config missing');

    const KafkaJS = loadKafkaJs();
    const kafka = new KafkaJS.Kafka({ kafkaJS: { logger: silentLogger } });

    let generator: GeneratorModule | null = null;
    let scriptDir: string | null = null;
    if (usesCustomScript(ctx.config.script)) {
      scriptDir = await mkdtemp(join(tmpdir(), 'ltd-kafka-'));
      const file = await materializeScript(ctx.config.script, scriptDir, 'generator.mjs');
      generator = await loadGenerator(file);
      ctx.log('warn', `custom generator ${file} runs in-process with full Node privileges`);
    }

    const producerCount = Math.max(1, cfg.producers);
    const producers: KafkaProducer[] = [];
    for (let i = 0; i < producerCount; i++) {
      producers.push(kafka.producer(producerConfig(cfg)));
    }

    ctx.log('info', `connecting ${producerCount} producer(s) to ${cfg.bootstrapServers}`);
    await Promise.all(producers.map((p) => p.connect()));
    ctx.agg.setVus(producerCount);

    let consumer: KafkaConsumer | null = null;
    let consumerReady = false;
    if (cfg.latencyMode === 'end-to-end') {
      consumer = kafka.consumer({
        ...producerConfig(cfg),
        'group.id': cfg.consumerGroup || `ltd-${ctx.runId}`,
        kafkaJS: { fromBeginning: false },
      });
      await consumer.connect();
      await consumer.subscribe({ topics: [cfg.topic] });
      void consumer.run({
        eachMessage: async ({ message }) => {
          if (message.headers?.['ltd-warmup'] != null) { consumerReady = true; return; }
          const raw = message.headers?.['ltd-sent-at'];
          if (!raw) return;
          const sentAt = Number(Buffer.isBuffer(raw) ? raw.toString('utf8') : raw);
          if (!Number.isFinite(sentAt)) return;
          const latency = Date.now() - sentAt;
          ctx.agg.record({ ts: Date.now(), latencyMs: latency, ok: true });
          applyLatencyChecks(ctx, latency);
        },
      });
      ctx.log('info', `end-to-end mode: consuming ${cfg.topic} as ${cfg.consumerGroup || `ltd-${ctx.runId}`}`);
      ctx.log('warn', 'end-to-end latency assumes producer and consumer share a clock');

      // Joining a consumer group takes seconds. Without this handshake the
      // first seconds of every run measure rebalance time, not message latency.
      const warmupDeadline = Date.now() + 30_000;
      while (!consumerReady && Date.now() < warmupDeadline && !ctx.signal.aborted) {
        await producers[0].send({
          topic: cfg.topic,
          messages: [{ key: null, value: 'ltd-warmup', headers: { 'ltd-warmup': '1' } }],
        }).catch(() => {});
        await new Promise((r) => setTimeout(r, 250));
      }
      ctx.log(consumerReady ? 'info' : 'warn', consumerReady
        ? `consumer joined after ${Math.round((Date.now() - (warmupDeadline - 30_000)) / 100) / 10}s — starting load`
        : 'consumer did not join within 30s — latency will include rebalance time');
    }

    const stopLag = cfg.monitorLag
      ? startLagSampler(cfg, (lag) => ctx.agg.setLag(lag), (msg) => ctx.log('warn', msg))
      : null;

    // Reset the clock so the warm-up handshake is not billed to the run.
    ctx.agg.resetClock();
    const deadline = Date.now() + cfg.durationSec * 1000;
    const maxMessages = cfg.maxMessages > 0 ? cfg.maxMessages : Number.POSITIVE_INFINITY;
    const targetRate = Math.max(1, cfg.targetRate);
    const tickMs = 20;
    const perTick = Math.max(1, Math.round((targetRate * tickMs) / 1000));
    const maxInflight = Math.max(1000, targetRate * 2);

    let seq = 0;
    let delivered = 0;
    let inflight = 0;
    let credit = 0;
    let lastTick = Date.now();

    let generatorFailures = 0;
    const dispatch = (producer: KafkaProducer, producerIndex: number): void => {
      const n = ++seq;
      const sentAt = Date.now();
      let message: {
        key: string | null;
        value: string | Buffer;
        headers: Record<string, string>;
      };
      if (generator) {
        try {
          const out = generator.generate({ seq: n, ts: sentAt, producer: producerIndex });
          message = {
            key: out.key ?? null,
            value: typeof out.value === 'string' || Buffer.isBuffer(out.value)
              ? (out.value as string | Buffer)
              : JSON.stringify(out.value),
            // Timing headers are ours; a generator must not be able to shadow them.
            headers: { ...out.headers, 'ltd-sent-at': String(sentAt), 'ltd-seq': String(n) },
          };
        } catch (err) {
          // One bad message must not abort the run, but do not hide it either.
          if (++generatorFailures === 1) ctx.error('generator', (err as Error).message);
          ctx.agg.record({ ts: Date.now(), latencyMs: 0, ok: false });
          return;
        }
      } else {
        message = {
          key: buildKey(cfg, n),
          value: buildPayload(cfg, n, sentAt),
          headers: { 'ltd-sent-at': String(sentAt), 'ltd-seq': String(n) },
        };
      }
      inflight++;
      producer.send({ topic: cfg.topic, messages: [message] })
        .then(() => {
          delivered++;
          // In end-to-end mode the consumer owns the success series entirely.
          // Recording the ack here too would double-count the message and pad
          // the histogram with zero-latency samples.
          if (cfg.latencyMode !== 'produce-ack') return;
          const latency = Date.now() - sentAt;
          ctx.agg.record({ ts: Date.now(), latencyMs: latency, ok: true });
          applyLatencyChecks(ctx, latency);
        })
        .catch((err: Error) => {
          ctx.agg.record({ ts: Date.now(), latencyMs: Date.now() - sentAt, ok: false });
          ctx.error(errorKind(err), err.message);
        })
        .finally(() => { inflight--; });
    };

    await new Promise<void>((resolve) => {
      const timer = setInterval(() => {
        const now = Date.now();
        if (ctx.signal.aborted || now >= deadline || seq >= maxMessages) {
          clearInterval(timer);
          resolve();
          return;
        }
        // Token bucket: catch up if the event loop stalled, but never burst
        // beyond one second of target rate.
        credit += ((now - lastTick) / 1000) * targetRate;
        lastTick = now;
        credit = Math.min(credit, targetRate);

        let budget = Math.min(Math.floor(credit), perTick * 4);
        while (budget > 0 && inflight < maxInflight && seq < maxMessages) {
          dispatch(producers[seq % producerCount], seq % producerCount);
          credit--;
          budget--;
        }
        if (inflight >= maxInflight) {
          ctx.log('warn', `in-flight cap reached (${maxInflight}) — broker is backpressuring`);
        }
      }, tickMs);
    });

    ctx.log('info', `draining ${inflight} in-flight message(s)`);
    const drainUntil = Date.now() + 15_000;
    while (inflight > 0 && Date.now() < drainUntil) {
      await new Promise((r) => setTimeout(r, 100));
    }

    if (generatorFailures > 0) ctx.log('warn', `generator threw on ${generatorFailures} message(s)`);
    if (scriptDir) await rm(scriptDir, { recursive: true, force: true });
    stopLag?.();
    await Promise.all(producers.map((p) => p.disconnect().catch(() => {})));
    if (consumer) {
      // Give the consumer a moment to drain the tail before disconnecting.
      await new Promise((r) => setTimeout(r, 1000));
      await consumer.disconnect().catch(() => {});
    }

    ctx.log('info', `produced ${seq} message(s), ${delivered} acked, target rate ${targetRate}/s`);
    if (cfg.latencyMode === 'end-to-end') {
      // Totals come from the consumer here, so a tail still in flight at the
      // deadline shows up as a gap. Say so rather than letting it look like loss.
      ctx.log('info', `end-to-end totals count consumed messages: ${ctx.agg.totalRequests} of ${delivered} acked`);
    }
    return {};
  },
};

function producerConfig(cfg: KafkaConfig): Record<string, unknown> {
  return {
    'bootstrap.servers': cfg.bootstrapServers,
    'compression.type': cfg.compression,
    acks: cfg.acks === 'all' ? -1 : Number(cfg.acks),
    // Raw librdkafka properties win — that is the point of exposing them.
    ...cfg.librdkafka,
  };
}

function buildKey(cfg: KafkaConfig, seq: number): string | null {
  switch (cfg.keyStrategy) {
    case 'fixed': return cfg.keyValue || 'key';
    case 'sequence': return String(seq);
    case 'random': return randomUUID();
    default: return null;
  }
}

/** Shape a custom generator module must export. */
interface GeneratorModule {
  setup?: () => void | Promise<void>;
  generate: (ctx: { seq: number; ts: number; producer: number }) =>
    { value: unknown; key?: string | null; headers?: Record<string, string> };
}

/**
 * Load a user-supplied message generator.
 *
 * Unlike the k6 and Artillery scripts, which execute inside their own tool's
 * runtime in a separate process, this module is imported into the dashboard's
 * process and runs with full Node privileges. The UI says so before you enable it.
 */
async function loadGenerator(file: string): Promise<GeneratorModule> {
  // Cache-bust so editing the file between runs actually takes effect.
  const mod = await import(`${pathToFileURL(file).href}?v=${Date.now()}`) as Partial<GeneratorModule>;
  if (typeof mod.generate !== 'function') {
    throw new Error(`${file} does not export a generate() function`);
  }
  if (typeof mod.setup === 'function') await mod.setup();
  return mod as GeneratorModule;
}

function buildPayload(cfg: KafkaConfig, seq: number, ts: number): string | Buffer {
  if (cfg.payloadType === 'random') {
    return randomBytes(Math.max(1, cfg.payloadSizeBytes));
  }
  return cfg.payload
    .replaceAll('{{seq}}', String(seq))
    .replaceAll('{{ts}}', String(ts))
    .replaceAll('{{uuid}}', randomUUID());
}

function applyLatencyChecks(ctx: RunnerContext, latencyMs: number): void {
  for (const c of ctx.config.checks) {
    if (c.kind !== 'latency_under') continue;
    const limit = Number(c.value);
    if (latencyMs < limit) ctx.agg.addCheck(c.name, 1, 0);
    else ctx.agg.addCheck(c.name, 0, 1);
  }
}

function errorKind(err: Error & { code?: number | string }): string {
  if (err.code != null) return `kafka_${err.code}`;
  return 'kafka_error';
}
