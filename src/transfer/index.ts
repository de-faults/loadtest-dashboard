import type { Protocol, RunConfig } from '../shared/types.ts';
import { fromK6Script, toK6Script, type ImportResult } from './k6.ts';
import { fromArtilleryScript, toArtilleryScript } from './artillery.ts';
import { fromKafkaScript, toKafkaScript } from './kafka.ts';

export type { ImportResult };

export const SCRIPT_FILENAME: Record<Protocol, string> = {
  rest: 'k6-script.js',
  socket: 'artillery-script.yml',
  kafka: 'kafka-script.yml',
};

export const SCRIPT_MIME: Record<Protocol, string> = {
  rest: 'text/javascript; charset=utf-8',
  socket: 'text/yaml; charset=utf-8',
  kafka: 'text/yaml; charset=utf-8',
};

export function exportScript(config: RunConfig): string {
  switch (config.protocol) {
    case 'rest': return toK6Script(config);
    case 'socket': return toArtilleryScript(config);
    case 'kafka': return toKafkaScript(config);
  }
}

export function importScript(protocol: Protocol, source: string): ImportResult {
  switch (protocol) {
    case 'rest': return fromK6Script(source);
    case 'socket': return fromArtilleryScript(source);
    case 'kafka': return fromKafkaScript(source);
  }
}

/**
 * Guess the protocol from the file itself, so the UI can accept a dropped file
 * without asking which runner it belongs to.
 */
export function detectProtocol(filename: string, source: string): Protocol | null {
  const head = source.slice(0, 4000);
  if (/^\s*protocol\s*:\s*['"]?kafka/m.test(head) || /bootstrapServers|bootstrap\.servers/.test(head)) return 'kafka';
  // A Kafka document need not name a broker — the load/message keys are enough
  // to tell it apart from an Artillery script.
  if (/^\s*topic\s*:/m.test(head) && /^\s*(targetRate|librdkafka|producers|payloadType|maxMessages)\s*:/m.test(head)) {
    return 'kafka';
  }
  if (/from\s+['"]k6/.test(head) || /require\(['"]k6/.test(head)) return 'rest';
  if (/^\s*scenarios\s*:/m.test(head) && /^\s*config\s*:/m.test(head)) return 'socket';
  if (/\.ya?ml$/i.test(filename)) return /engine\s*:\s*ws|arrivalRate/.test(head) ? 'socket' : 'kafka';
  if (/\.m?[jt]s$/i.test(filename)) return 'rest';
  return null;
}
