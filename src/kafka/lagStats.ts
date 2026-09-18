import { Histogram } from '../metrics/histogram.ts';
import type { GroupInfo, KafkaLagSummary, MessageLagGroup, PartitionLagPeak } from '../shared/types.ts';

/** Partitions kept in the summary. A hot partition is the point; 200 rows is not. */
const MAX_PARTITIONS = 20;

interface Sample { ts: number; lag: number; phase: 'load' | 'drain' }

/**
 * Accumulates the lag samples of one Kafka run into a `KafkaLagSummary`.
 *
 * Kept free of any broker I/O so the arithmetic can be reasoned about (and
 * tested) on its own: the sampler feeds it, the runner reads it.
 */
export class LagTracker {
  private readonly samples: Sample[] = [];
  private readonly groups = new Set<string>();
  private readonly peaks = new Map<string, PartitionLagPeak>();
  private phase: 'load' | 'drain' = 'load';
  private loadEndedAt: number | null = null;
  /** First drain sample at lag 0 — the moment the consumers caught up. */
  private caughtUpAt: number | null = null;

  constructor(private readonly runStartedAt: () => number) {}

  record(ts: number, groups: GroupInfo[]): void {
    const lag = groups.reduce((s, g) => s + g.totalLag, 0);
    this.samples.push({ ts, lag, phase: this.phase });
    if (this.phase === 'drain' && lag === 0 && this.caughtUpAt === null) this.caughtUpAt = ts;

    for (const g of groups) {
      // Every consumer group on the cluster is listed; only one that has
      // committed on the topic is actually being measured.
      if (g.topics.length === 0) continue;
      this.groups.add(g.groupId);
      for (const t of g.topics) {
        for (const p of t.partitions) {
          const key = `${g.groupId}|${t.topic}|${p.partition}`;
          const peak = this.peaks.get(key);
          if (peak) {
            if (p.lag > peak.maxLag) peak.maxLag = p.lag;
            peak.finalLag = p.lag;
          } else {
            this.peaks.set(key, { group: g.groupId, topic: t.topic, partition: p.partition, maxLag: p.lag, finalLag: p.lag });
          }
        }
      }
    }
  }

  /** Production has stopped; from here on samples measure the drain. */
  markLoadEnd(ts: number): void {
    this.phase = 'drain';
    this.loadEndedAt = ts;
  }

  /** A drain sample has seen lag 0: the consumers have caught up. */
  get caughtUp(): boolean { return this.caughtUpAt !== null; }
  /** A sample has been taken since the load ended. */
  get sampledSinceLoadEnd(): boolean {
    return this.loadEndedAt !== null && this.samples.some((s) => s.phase === 'drain');
  }

  /**
   * @param drainTimeoutSec  the wait that was asked for; 0 = none, so no drain block.
   */
  summary(drainTimeoutSec: number): KafkaLagSummary | null {
    // A group with no committed offsets on the topic yields samples of 0 that
    // name no group — that is "nothing to measure", not "lag was zero".
    if (this.samples.length === 0 || this.groups.size === 0) return null;

    const load = this.samples.filter((s) => s.phase === 'load');
    const first = this.samples[0];
    const last = this.samples[this.samples.length - 1];
    const lastLoad = load.length ? load[load.length - 1] : first;

    let max = first;
    for (const s of this.samples) if (s.lag > max.lag) max = s;

    const loadSec = (lastLoad.ts - first.ts) / 1000;
    const growthPerSec = loadSec > 0 ? (lastLoad.lag - first.lag) / loadSec : 0;
    const loadLags = (load.length ? load : [first]).map((s) => s.lag).sort((a, b) => a - b);
    const avg = loadLags.reduce((a, l) => a + l, 0) / loadLags.length;

    const drain = drainTimeoutSec > 0 && this.loadEndedAt !== null
      ? {
          timeoutSec: drainTimeoutSec,
          drained: this.caughtUpAt !== null,
          seconds: this.caughtUpAt !== null ? round2((this.caughtUpAt - this.loadEndedAt) / 1000) : null,
        }
      : null;

    const partitions = [...this.peaks.values()]
      .sort((a, b) => b.maxLag - a.maxLag || a.partition - b.partition)
      .slice(0, MAX_PARTITIONS);

    return {
      groups: [...this.groups].sort(),
      samples: this.samples.length,
      start: first.lag,
      max: max.lag,
      maxAtSec: round2(Math.max(0, max.ts - this.runStartedAt()) / 1000),
      min: loadLags[0],
      avg: round2(avg),
      median: median(loadLags),
      endOfLoad: lastLoad.lag,
      final: last.lag,
      growthPerSec: round2(growthPerSec),
      drain,
      partitions,
    };
  }
}

/** Median of an ascending-sorted, non-empty list. */
function median(sorted: number[]): number {
  const mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid] : round2((sorted[mid - 1] + sorted[mid]) / 2);
}

function round2(n: number): number { return Math.round(n * 100) / 100; }

// ─── Per-message lag ─────────────────────────────────────────────────────────

/**
 * Messages waiting to be seen consumed, across all partitions. Past this the
 * run stops tracking new ones rather than growing without bound: 16 bytes each,
 * so the cap is ~32 MB.
 */
const MAX_PENDING = 2_000_000;
/** Drop consumed entries off the front of a partition queue in chunks this big. */
const TRIM_CHUNK = 10_000;

/** Acked messages of one partition, in offset order. */
interface PartitionQueue {
  offsets: number[];
  sentAt: number[];
  /** Entries already trimmed off the front — array index = absolute index − base. */
  base: number;
}

interface GroupProgress {
  /** Absolute index of the first message this group has not yet consumed, per partition. */
  cursor: Map<number, number>;
  hist: Histogram;
}

/**
 * Time lag of every message: from the moment it was produced until the
 * consumer group's committed offset was seen past it.
 *
 * Only committed offsets are visible from outside the consumer, so a message
 * counts as consumed at the first lag sample after the commit — resolution is
 * the 1 s sample interval plus the group's own commit interval.
 */
export class MessageLagTracker {
  private readonly parts = new Map<number, PartitionQueue>();
  private readonly groups = new Map<string, GroupProgress>();
  private pendingTotal = 0;
  /** Acked messages not tracked: no usable offset (acks=0) or over the cap. */
  untracked = 0;

  constructor(
    private readonly topic: string,
    /** Every consumed message's lag, for the per-second chart. */
    private readonly onLag: (lagMs: number) => void,
  ) {}

  track(partition: number, offset: number, sentAt: number): void {
    if (!Number.isInteger(partition) || !Number.isFinite(offset) || offset < 0 || this.pendingTotal >= MAX_PENDING) {
      this.untracked++;
      return;
    }
    let q = this.parts.get(partition);
    if (!q) { q = { offsets: [], sentAt: [], base: 0 }; this.parts.set(partition, q); }
    // Delivery reports arrive in offset order per partition; insert in place if one does not.
    let i = q.offsets.length;
    while (i > 0 && q.offsets[i - 1] > offset) i--;
    q.offsets.splice(i, 0, offset);
    q.sentAt.splice(i, 0, sentAt);
    this.pendingTotal++;
  }

  /** Feed one lag sample: advance each group past every message its commit now covers. */
  observe(ts: number, groups: GroupInfo[]): void {
    for (const g of groups) {
      const topic = g.topics.find((t) => t.topic === this.topic);
      if (!topic) continue;
      let progress = this.groups.get(g.groupId);
      if (!progress) {
        progress = { cursor: new Map(), hist: new Histogram() };
        this.groups.set(g.groupId, progress);
      }
      for (const p of topic.partitions) {
        const q = this.parts.get(p.partition);
        if (!q) continue;
        let abs = Math.max(progress.cursor.get(p.partition) ?? 0, q.base);
        const end = q.base + q.offsets.length;
        while (abs < end && q.offsets[abs - q.base] < p.committed) {
          const lagMs = Math.max(0, ts - q.sentAt[abs - q.base]);
          progress.hist.record(lagMs);
          this.onLag(lagMs);
          abs++;
        }
        progress.cursor.set(p.partition, abs);
      }
    }
    this.trim();
  }

  /** Forget messages every tracked group has consumed. */
  private trim(): void {
    if (this.groups.size === 0) return;
    for (const [partition, q] of this.parts) {
      let slowest = Number.POSITIVE_INFINITY;
      for (const g of this.groups.values()) slowest = Math.min(slowest, g.cursor.get(partition) ?? q.base);
      const drop = slowest - q.base;
      if (drop < TRIM_CHUNK) continue;
      q.offsets.splice(0, drop);
      q.sentAt.splice(0, drop);
      q.base += drop;
      this.pendingTotal -= drop;
    }
  }

  summary(): MessageLagGroup[] {
    const tracked = [...this.parts.values()].reduce((s, q) => s + q.base + q.offsets.length, 0);
    return [...this.groups.entries()]
      .map(([group, g]) => {
        const prof = g.hist.profile();
        return {
          group,
          consumed: g.hist.count,
          pending: tracked - g.hist.count,
          ...prof,
          p50: round2(Math.min(g.hist.percentile(50), prof.max)),
        };
      })
      .sort((a, b) => a.group.localeCompare(b.group));
  }
}
