import { EventEmitter } from 'node:events';
import type { RunEvent } from './shared/types.ts';

/**
 * Fan-out for run events. Keeps a bounded ring buffer per run so a browser that
 * refreshes mid-run can replay the timeline via SSE `Last-Event-ID` instead of
 * starting from a blank chart.
 */

const RING = 5000;

class Bus extends EventEmitter {
  private buffers = new Map<string, Array<{ id: number; ev: RunEvent }>>();
  private seq = 0;

  publish(ev: RunEvent): void {
    const id = ++this.seq;
    const key = ev.t === 'kafka-monitor' ? '__monitor' : ev.runId;
    if (key) {
      const buf = this.buffers.get(key) ?? [];
      buf.push({ id, ev });
      if (buf.length > RING) buf.splice(0, buf.length - RING);
      this.buffers.set(key, buf);
    }
    this.emit('event', id, ev);
  }

  /** Events for `runId` newer than `afterId` (0 = whole buffer). */
  replay(runId: string, afterId = 0): Array<{ id: number; ev: RunEvent }> {
    return (this.buffers.get(runId) ?? []).filter((e) => e.id > afterId);
  }

  latestMonitor(): RunEvent | null {
    const buf = this.buffers.get('__monitor');
    return buf?.length ? buf[buf.length - 1].ev : null;
  }

  drop(runId: string): void { this.buffers.delete(runId); }
}

export const bus = new Bus();
bus.setMaxListeners(0);
