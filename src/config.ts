import { resolve } from 'node:path';

/**
 * Process-level config. Everything the *user* tunes lives in the settings table
 * (editable from the UI); only bind/port/token/data-dir are env-only, because
 * they are needed before the database exists.
 */

export const PORT = Number.parseInt(process.env.DASHBOARD_PORT ?? '4300', 10);
export const BIND = process.env.DASHBOARD_BIND ?? '127.0.0.1';
export const TOKEN = process.env.DASHBOARD_TOKEN ?? '';
export const DATA_DIR = resolve(process.env.DASHBOARD_DATA_DIR ?? './data');
/**
 * Selected profiles start together. They share this host, so their latency
 * figures do influence each other — the ceiling is here to keep a careless
 * selection from spawning more load generators than the machine can drive.
 */
export const MAX_CONCURRENT_RUNS = Math.max(
  1, Number.parseInt(process.env.DASHBOARD_MAX_RUNS ?? '8', 10) || 8,
);

/**
 * This service spawns processes and generates traffic. Exposing it off-loopback
 * without a token is a remote-exec surface, so refuse to start instead.
 */
export function assertSafeBinding(): void {
  const loopback = BIND === '127.0.0.1' || BIND === 'localhost' || BIND === '::1';
  if (!loopback && !TOKEN) {
    throw new Error(
      `Refusing to bind ${BIND} without DASHBOARD_TOKEN set. ` +
      `This dashboard spawns load generators; an open bind is a remote-execution surface.`,
    );
  }
}
