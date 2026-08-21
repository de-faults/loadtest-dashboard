import { useEffect, useRef, useState } from 'react';
import type { RunEvent } from '@shared/types.ts';
import { getToken } from './api.ts';

export type ConnState = 'connecting' | 'live' | 'down';

/**
 * EventSource with auto-reconnect.
 *
 * The server replays this run's buffered events on connect, so a refresh
 * mid-run redraws the whole timeline instead of starting from a blank chart.
 */
export function useEventStream(runId: string | null, onEvent: (ev: RunEvent) => void): ConnState {
  const [state, setState] = useState<ConnState>('connecting');
  const handler = useRef(onEvent);
  handler.current = onEvent;

  useEffect(() => {
    let es: EventSource | null = null;
    let retry: ReturnType<typeof setTimeout> | null = null;
    let closed = false;

    const connect = (): void => {
      if (closed) return;
      const url = new URL('/events', window.location.origin);
      if (runId) url.searchParams.set('runId', runId);
      const token = getToken();
      if (token) url.searchParams.set('token', token);

      es = new EventSource(url.toString());
      es.onopen = () => setState('live');
      es.onmessage = (e) => {
        try { handler.current(JSON.parse(e.data) as RunEvent); } catch { /* ignore malformed frame */ }
      };
      es.onerror = () => {
        setState('down');
        es?.close();
        retry = setTimeout(connect, 3000);
      };
    };

    connect();
    return () => {
      closed = true;
      if (retry) clearTimeout(retry);
      es?.close();
    };
  }, [runId]);

  return state;
}
