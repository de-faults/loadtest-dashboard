import { createRequire } from 'node:module';
import { socketScenarios } from '../shared/defaults.ts';
import type { SocketConfig, SocketFlowStep } from '../shared/types.ts';
import { parseData } from './artillery.runner.ts';

/**
 * Single-connection probe: connect, send the first message of the flow, and
 * show what came back.
 *
 * A load test answers "how fast", never "did this work at all" — a run against
 * a target that refuses every connection looks much like one against a slow
 * target, and the acknowledgement payload, the thing that proves the server
 * actually answered, is never shown. This runs one virtual user's first step
 * and prints the raw reply.
 *
 * It is a diagnostic, not load: one connection, no retries, hard timeouts, and
 * the socket is always closed before returning.
 */

const require = createRequire(import.meta.url);

export type ProbeLevel = 'info' | 'ok' | 'warn' | 'error';
export interface ProbeLine { level: ProbeLevel; line: string; atMs: number }

export interface ProbeResult {
  ok: boolean;
  lines: ProbeLine[];
  /** Transport the connection actually settled on, for Socket.IO. */
  transport?: string;
  /** Raw acknowledgement or first inbound message, as text. */
  reply?: string;
  /** Actionable next step when the failure has a known shape. */
  suggestion?: string;
  /** Whether the socket ever opened, which decides if a retry is worth it. */
  connected?: boolean;
}

const MAX_REPLY = 4000;
const CONNECT_MS = 10_000;
const REPLY_MS = 10_000;

class Log {
  readonly lines: ProbeLine[] = [];
  private readonly t0 = Date.now();
  add(level: ProbeLevel, line: string): void {
    this.lines.push({ level, line, atMs: Date.now() - this.t0 });
  }
}

export async function probeSocket(cfg: SocketConfig): Promise<ProbeResult> {
  return cfg.engine === 'socketio' ? probeSocketIo(cfg) : probeWs(cfg);
}

// ─── Socket.IO ───────────────────────────────────────────────────────────────

interface IoSocket {
  id?: string;
  io: { engine: { transport: { name: string } }; on: (ev: string, fn: (...a: unknown[]) => void) => void };
  on: (ev: string, fn: (...a: unknown[]) => void) => void;
  onAny: (fn: (event: string, ...args: unknown[]) => void) => void;
  emit: (...args: unknown[]) => void;
  disconnect: () => void;
  connected: boolean;
}

async function probeSocketIo(cfg: SocketConfig): Promise<ProbeResult> {
  const log = new Log();
  let io: (url: string, opts: Record<string, unknown>) => IoSocket;
  try {
    io = (require('socket.io-client') as { io: typeof io }).io;
  } catch {
    log.add('error', 'socket.io-client is not installed — run: npm install socket.io-client');
    return { ok: false, lines: log.lines };
  }

  const explicitTransports = cfg.transports.length > 0;
  const first = await connectIo(cfg, log, cfg.transports.length ? cfg.transports : undefined);
  if (first.ok || first.connected) return first;

  // The reported failure looks exactly like this: polling errors while
  // websocket works. Worth proving rather than leaving the user to guess.
  if (!explicitTransports) {
    log.add('info', 'retrying with the websocket transport only…');
    const retry = await connectIo(cfg, log, ['websocket']);
    if (retry.ok) {
      retry.suggestion = 'Long-polling failed but websocket alone works. Set Transports to "websocket" in this '
        + 'profile. The target is most likely behind a proxy or load balancer that does not send a session\'s '
        + 'polling requests to the same backend.';
      return retry;
    }
  }

  // Both transports failed. Ask the server directly whether it is even there,
  // so the advice is about what actually happened rather than a guess.
  const reach = await handshakeCheck(cfg.url, log);
  return { ...first, lines: log.lines, suggestion: failureHint(reach, first.suggestion) };
}

interface Reach { status?: number; error?: string }

/**
 * Fetch the Socket.IO handshake endpoint the way the client's polling transport
 * would. The HTTP status separates "nothing is listening" from "something is
 * listening but not answering as Socket.IO".
 */
async function handshakeCheck(url: string, log: Log): Promise<Reach> {
  const target = `${url.replace(/\/+$/, '')}/socket.io/?EIO=4&transport=polling`;
  try {
    const res = await fetch(target, { signal: AbortSignal.timeout(5_000), redirect: 'manual' });
    const body = clip((await res.text()).trim(), 80);
    log.add(res.ok ? 'info' : 'warn', `handshake check → HTTP ${res.status}${body ? ` · ${body}` : ''}`);
    return { status: res.status };
  } catch (err) {
    const message = asError(err).message;
    log.add('warn', `handshake check failed: ${message}`);
    return { error: message };
  }
}

function failureHint(reach: Reach, fallback?: string): string | undefined {
  if (reach.error) {
    const m = reach.error.toLowerCase();
    if (m.includes('econnrefused')) return 'Nothing is listening on that host and port.';
    if (m.includes('enotfound') || m.includes('eai_again')) return 'The host name does not resolve from this machine.';
    if (m.includes('certificate') || m.includes('self-signed') || m.includes('self signed')) {
      return 'The server certificate was rejected by this machine.';
    }
    if (m.includes('timeout') || m.includes('aborted')) {
      return 'The host accepted nothing within 5s — a firewall or the wrong port would look like this.';
    }
    return fallback;
  }
  const status = reach.status ?? 0;
  if (status === 200) {
    return 'The server answers the Socket.IO handshake, so it is reachable — the connection failed after that. '
      + 'Check the namespace, and any auth the server expects in headers or query parameters.';
  }
  if (status === 404) {
    return 'The host is up but has no Socket.IO endpoint at /socket.io/. The server may be mounted under a '
      + 'custom path, or the URL may point at the wrong service.';
  }
  if (status === 401 || status === 403) {
    return `The handshake was rejected with HTTP ${status}. The server wants credentials — put them in the `
      + 'headers or query parameters of this profile.';
  }
  if (status >= 500) {
    return `The handshake returned HTTP ${status}, which comes from the server or a proxy in front of it, `
      + 'not from the socket layer.';
  }
  return fallback ?? `The handshake returned HTTP ${status}.`;
}

async function connectIo(
  cfg: SocketConfig, log: Log, transports: string[] | undefined,
): Promise<ProbeResult> {
  const io = (require('socket.io-client') as {
    io: (url: string, opts: Record<string, unknown>) => IoSocket;
  }).io;

  // Artillery targets `config.target + namespace`, so the probe must too.
  const target = cfg.url + (cfg.namespace || '');
  log.add('info', `connecting to ${target}${transports ? ` (transports: ${transports.join(', ')})` : ''}`);

  const socket = io(target, {
    reconnection: false,
    timeout: CONNECT_MS,
    forceNew: true,
    ...(transports ? { transports } : {}),
    ...(Object.keys(cfg.headers).length ? { extraHeaders: cfg.headers } : {}),
    ...(Object.keys(cfg.query).length ? { query: cfg.query } : {}),
  });

  const started = Date.now();
  try {
    await once(socket, 'connect', 'connect_error', CONNECT_MS, 'connect');
  } catch (err) {
    log.add('error', `connection failed: ${(err as Error).message}`);
    socket.disconnect();
    return { ok: false, connected: false, lines: log.lines, suggestion: connectHint((err as Error).message) };
  }

  const transport = socket.io.engine.transport.name;
  log.add('ok', `connected in ${Date.now() - started}ms over ${transport}${socket.id ? ` (id ${socket.id})` : ''}`);
  socket.io.on('upgrade', (t) => log.add('info', `transport upgraded to ${(t as { name: string }).name}`));

  const step = firstMessage(cfg);
  if (!step) {
    log.add('warn', 'the flow has no message to send — connection verified, nothing else to check');
    socket.disconnect();
    return { ok: true, connected: true, lines: log.lines, transport };
  }

  const event = step.event || 'message';
  const data = parseData(step.value);
  const result = step.acknowledge
    ? await emitAndAwaitAck(socket, event, data, log)
    : await emitAndListen(socket, event, data, log);

  socket.disconnect();
  log.add('info', 'disconnected');
  return { ...result, connected: true, lines: log.lines, transport };
}

/**
 * The first message any scenario sends. Scenarios share one connection, so
 * probing the earliest send is enough to prove the target answers at all — a
 * leading scenario that only thinks does not make the probe give up.
 */
function firstMessage(cfg: SocketConfig): SocketFlowStep | undefined {
  return socketScenarios(cfg)
    .flatMap((sc) => sc.flow)
    .find((s) => s.kind === 'emit' || s.kind === 'send');
}

async function emitAndAwaitAck(
  socket: IoSocket, event: string, data: unknown, log: Log,
): Promise<{ ok: boolean; reply?: string; suggestion?: string }> {
  log.add('info', `emit "${event}" ${preview(data)} — waiting for acknowledgement`);
  const started = Date.now();
  return new Promise((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      log.add('error', `no acknowledgement after ${REPLY_MS / 1000}s`);
      resolve({
        ok: false,
        suggestion: `The connection works but "${event}" was never acknowledged. `
          + 'Check that the server calls the acknowledgement callback for this event — '
          + 'a load test on this flow would hang on every virtual user.',
      });
    }, REPLY_MS);

    socket.emit(event, data, (...args: unknown[]) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      const reply = preview(args.length === 1 ? args[0] : args, MAX_REPLY);
      log.add('ok', `acknowledgement in ${Date.now() - started}ms: ${reply}`);
      resolve({ ok: true, reply });
    });
  });
}

async function emitAndListen(
  socket: IoSocket, event: string, data: unknown, log: Log,
): Promise<{ ok: boolean; reply?: string }> {
  log.add('info', `emit "${event}" ${preview(data)} — no acknowledgement requested, listening for a pushed event`);
  const started = Date.now();
  return new Promise((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      log.add('warn', 'no event arrived within 5s — the message was sent, but nothing came back');
      resolve({ ok: true });
    }, 5_000);

    socket.onAny((name: string, ...args: unknown[]) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      const reply = preview(args.length === 1 ? args[0] : args, MAX_REPLY);
      log.add('ok', `event "${name}" after ${Date.now() - started}ms: ${reply}`);
      resolve({ ok: true, reply });
    });
    socket.emit(event, data);
  });
}

// ─── Raw WebSocket ───────────────────────────────────────────────────────────

async function probeWs(cfg: SocketConfig): Promise<ProbeResult> {
  const log = new Log();
  log.add('info', `connecting to ${cfg.url}`);

  // `ws` carries custom headers; the built-in client cannot, and headers are
  // often where the auth lives — so say what was dropped rather than failing
  // for a reason that looks unrelated.
  let socket: WsLike;
  const started = Date.now();
  try {
    socket = openWs(cfg, log);
  } catch (err) {
    log.add('error', `connection failed: ${(err as Error).message}`);
    return { ok: false, lines: log.lines, suggestion: connectHint((err as Error).message) };
  }

  try {
    await once(socket, 'open', 'error', CONNECT_MS, 'open');
  } catch (err) {
    log.add('error', `connection failed: ${(err as Error).message}`);
    try { socket.close(); } catch { /* already gone */ }
    return { ok: false, lines: log.lines, suggestion: connectHint((err as Error).message) };
  }
  log.add('ok', `connected in ${Date.now() - started}ms`);

  const step = firstMessage(cfg);
  if (!step) {
    log.add('warn', 'the flow has no message to send — connection verified, nothing else to check');
    socket.close();
    return { ok: true, lines: log.lines };
  }

  log.add('info', `send ${preview(step.value)}`);
  const sentAt = Date.now();
  const reply = await new Promise<string | null>((resolve) => {
    const timer = setTimeout(() => resolve(null), 5_000);
    socket.addEventListener('message', (ev: { data: unknown }) => {
      clearTimeout(timer);
      resolve(typeof ev.data === 'string' ? ev.data : '[binary frame]');
    }, { once: true });
    socket.send(step.value);
  });

  if (reply === null) log.add('warn', 'no message arrived within 5s — the frame was sent, but nothing came back');
  else log.add('ok', `reply after ${Date.now() - sentAt}ms: ${clip(reply, MAX_REPLY)}`);

  socket.close();
  log.add('info', 'disconnected');
  return { ok: true, lines: log.lines, reply: reply ?? undefined };
}

interface WsLike {
  addEventListener: (ev: string, fn: (e: never) => void, opts?: { once: boolean }) => void;
  send: (data: string) => void;
  close: () => void;
}

function openWs(cfg: SocketConfig, log: Log): WsLike {
  const hasHeaders = Object.keys(cfg.headers).length > 0;
  try {
    const WS = require('ws') as new (url: string, protocols?: string[], opts?: unknown) => WsLike;
    return new WS(cfg.url, cfg.subprotocols.length ? cfg.subprotocols : undefined,
      hasHeaders ? { headers: cfg.headers } : undefined);
  } catch {
    if (hasHeaders) log.add('warn', 'the ws package is unavailable — connecting without the configured headers');
    return new WebSocket(cfg.url, cfg.subprotocols.length ? cfg.subprotocols : undefined) as unknown as WsLike;
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

type Emitterish = {
  on?: (ev: string, fn: (...a: unknown[]) => void) => void;
  addEventListener?: (ev: string, fn: (e: never) => void, opts?: { once: boolean }) => void;
};

/** Resolve on the success event, reject on the failure event or the timeout. */
function once(
  emitter: Emitterish, okEvent: string, errEvent: string, timeoutMs: number, what: string,
): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const done = (fn: () => void) => () => { if (!settled) { settled = true; clearTimeout(timer); fn(); } };
    const timer = setTimeout(
      done(() => reject(new Error(`timed out after ${timeoutMs / 1000}s waiting to ${what}`))),
      timeoutMs,
    );
    const okFn = done(resolve);
    const errFn = (err: unknown) => done(() => reject(asError(err)))();

    if (emitter.on) {
      emitter.on(okEvent, okFn);
      emitter.on(errEvent, errFn);
    } else {
      emitter.addEventListener?.(okEvent, okFn as (e: never) => void, { once: true });
      emitter.addEventListener?.(errEvent, errFn as (e: never) => void, { once: true });
    }
  });
}

function asError(err: unknown): Error {
  if (err instanceof Error) {
    // fetch() reports a flat "fetch failed" and keeps the real reason in cause.
    const cause = (err as { cause?: unknown }).cause;
    const causeMessage = cause instanceof Error ? cause.message : undefined;
    return causeMessage ? new Error(`${err.message}: ${causeMessage}`) : err;
  }
  const message = (err as { message?: string })?.message
    ?? (err as { error?: { message?: string } })?.error?.message;
  return new Error(message ?? String(err));
}

/** Turn an engine.io failure into the thing to go and check. */
function connectHint(message: string): string | undefined {
  const m = message.toLowerCase();
  if (m.includes('invalid namespace')) return 'The server has no such namespace. Check the Namespace field.';
  if (m.includes('econnrefused')) return 'Nothing is listening on that host and port.';
  if (m.includes('enotfound') || m.includes('eai_again')) return 'The host name does not resolve from this machine.';
  if (m.includes('certificate') || m.includes('self-signed') || m.includes('self signed')) {
    return 'The server certificate was rejected by this machine.';
  }
  if (m.includes('unauthorized') || m.includes('401') || m.includes('403')) {
    return 'The server rejected the handshake. Check the headers or query parameters it expects for auth.';
  }
  // Note for xhr/websocket transport errors: nothing is added here. This runs
  // server-side, where CORS plays no part, and the handshake check that follows
  // can tell the real causes apart.
  return undefined;
}

function preview(value: unknown, max = 300): string {
  const text = typeof value === 'string' ? value : safeJson(value);
  return clip(text, max);
}

function safeJson(value: unknown): string {
  try { return JSON.stringify(value) ?? String(value); } catch { return String(value); }
}

function clip(text: string, max: number): string {
  return text.length > max ? `${text.slice(0, max)}… (${text.length} chars)` : text;
}
