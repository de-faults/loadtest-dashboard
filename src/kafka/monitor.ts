import { createRequire } from 'node:module';
import { bus } from '../bus.ts';
import type { GroupInfo, GroupTopicLag, KafkaConfig, PartitionLag, TopicInfo } from '../shared/types.ts';

const require = createRequire(import.meta.url);

/**
 * Consumer-lag monitor, ported from the standalone Kafka web monitor.
 *
 * Two differences from the original: it publishes onto the shared event bus
 * instead of owning an HTTP server, and it sends *all* topics and groups —
 * filtering moved to the UI dropdowns, so env vars are only defaults.
 */

const MAX_HISTORY = 1200; // 1 hour at 3s

interface TopicOffsetEntry { partition: number; offset: string; high: string; low: string }
interface CommittedEntry { partition: number; offset: string }
interface CommittedTopicRow { topic: string; partitions: CommittedEntry[] }
interface GroupEntry { groupId: string; protocolType: string }
interface GroupDescription { groupId: string; state: string; members: Array<{ memberId: string }> }
interface ClusterInfo { brokers: Array<{ nodeId: number }>; controller: number; clusterId: string }

interface KafkaAdmin {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  listTopics(): Promise<string[]>;
  fetchTopicOffsets(topic: string): Promise<TopicOffsetEntry[]>;
  listGroups(): Promise<{ groups: GroupEntry[] }>;
  describeGroups(groupIds: string[]): Promise<{ groups: GroupDescription[] }>;
  fetchOffsets(opts: { groupId: string; topics: string[] }): Promise<CommittedTopicRow[]>;
  describeCluster(): Promise<ClusterInfo>;
}

const silentLogger = {
  info: () => {}, warn: () => {}, error: () => {}, debug: () => {},
  namespace: () => silentLogger, setLogLevel: () => {},
};

export function makeAdmin(bootstrapServers: string, extra: Record<string, string> = {}): KafkaAdmin {
  const { KafkaJS } = require('@confluentinc/kafka-javascript') as {
    KafkaJS: { Kafka: new (c: Record<string, unknown>) => { admin(c?: Record<string, unknown>): KafkaAdmin } };
  };
  const kafka = new KafkaJS.Kafka({ kafkaJS: { logger: silentLogger } });
  return kafka.admin({ 'bootstrap.servers': bootstrapServers, ...extra });
}

export async function collectTopics(admin: KafkaAdmin): Promise<{
  topicInfos: TopicInfo[];
  latestMap: Map<string, Map<number, number>>;
}> {
  const topics = (await admin.listTopics()).filter((t) => !t.startsWith('__')).sort();
  const topicInfos: TopicInfo[] = [];
  const latestMap = new Map<string, Map<number, number>>();

  await Promise.all(topics.map(async (topic) => {
    const offsets = await admin.fetchTopicOffsets(topic);
    const partMap = new Map<number, number>();
    let endOffsetSum = 0;
    for (const p of offsets) {
      const latest = Number.parseInt(p.high, 10);
      partMap.set(p.partition, latest);
      endOffsetSum += latest;
    }
    latestMap.set(topic, partMap);
    topicInfos.push({ name: topic, partitionCount: offsets.length, endOffsetSum });
  }));

  topicInfos.sort((a, b) => a.name.localeCompare(b.name));
  return { topicInfos, latestMap };
}

export async function collectGroups(
  admin: KafkaAdmin,
  monitoredTopics: string[],
  latestMap: Map<string, Map<number, number>>,
): Promise<GroupInfo[]> {
  const { groups: allGroups } = await admin.listGroups();
  const ids = allGroups.filter((g) => g.protocolType === 'consumer').map((g) => g.groupId).sort();
  if (ids.length === 0) return [];

  const { groups: descriptions } = await admin.describeGroups(ids);
  const descMap = new Map(descriptions.map((g) => [g.groupId, g]));

  const infos = await Promise.all(ids.map(async (groupId): Promise<GroupInfo> => {
    const desc = descMap.get(groupId);
    let committed: CommittedTopicRow[] = [];
    try {
      committed = await admin.fetchOffsets({ groupId, topics: monitoredTopics });
    } catch {
      // group has not committed to any monitored topic yet
    }

    const topics: GroupTopicLag[] = [];
    for (const row of committed) {
      const partLatest = latestMap.get(row.topic);
      if (!partLatest) continue;
      const partitions: PartitionLag[] = [];
      for (const p of row.partitions) {
        const committedOffset = Number.parseInt(p.offset, 10);
        if (committedOffset < 0) continue;
        const latest = partLatest.get(p.partition) ?? 0;
        partitions.push({
          partition: p.partition, latest, committed: committedOffset,
          lag: Math.max(0, latest - committedOffset),
        });
      }
      if (partitions.length === 0) continue;
      partitions.sort((a, b) => a.partition - b.partition);
      topics.push({ topic: row.topic, totalLag: partitions.reduce((s, p) => s + p.lag, 0), partitions });
    }

    topics.sort((a, b) => b.totalLag - a.totalLag);
    return {
      groupId,
      state: desc?.state ?? 'Unknown',
      memberCount: desc?.members.length ?? 0,
      topics,
      totalLag: topics.reduce((s, t) => s + t.totalLag, 0),
    };
  }));

  return infos.sort((a, b) => b.totalLag - a.totalLag);
}

// ─── Continuous monitor ──────────────────────────────────────────────────────

let current: { stop: () => void; bootstrapServers: string; intervalSec: number } | null = null;

export function monitorStatus(): { running: boolean; bootstrapServers: string | null; intervalSec: number } {
  return {
    running: current !== null,
    bootstrapServers: current?.bootstrapServers ?? null,
    intervalSec: current?.intervalSec ?? 0,
  };
}

export function stopMonitor(): void {
  current?.stop();
  current = null;
}

export function startMonitor(bootstrapServers: string, intervalSec: number): void {
  stopMonitor();
  const lagHistory = new Map<string, number[]>();
  let stopped = false;
  let admin: KafkaAdmin | null = null;

  const loop = async (): Promise<void> => {
    try {
      admin = makeAdmin(bootstrapServers);
      await admin.connect();
    } catch (err) {
      bus.publish({
        t: 'kafka-monitor', runId: null,
        payload: emptyPayload(bootstrapServers, intervalSec, [`connect: ${(err as Error).message}`]),
      });
      return;
    }

    while (!stopped) {
      const errors: string[] = [];
      let brokersLine = bootstrapServers;
      try {
        const cluster = await admin.describeCluster();
        brokersLine = `${cluster.brokers.length} broker${cluster.brokers.length !== 1 ? 's' : ''}`
          + `  controller=${cluster.controller}  id=${String(cluster.clusterId).slice(0, 8)}`;
      } catch { /* not available on every broker version */ }

      let topicInfos: TopicInfo[] = [];
      let latestMap = new Map<string, Map<number, number>>();
      try {
        ({ topicInfos, latestMap } = await collectTopics(admin));
      } catch (err) { errors.push(`collectTopics: ${(err as Error).message}`); }

      let groups: GroupInfo[] = [];
      try {
        groups = await collectGroups(admin, topicInfos.map((t) => t.name), latestMap);
      } catch (err) { errors.push(`collectGroups: ${(err as Error).message}`); }

      for (const g of groups) {
        const hist = lagHistory.get(g.groupId) ?? [];
        hist.push(g.totalLag);
        if (hist.length > MAX_HISTORY) hist.shift();
        lagHistory.set(g.groupId, hist);
      }

      bus.publish({
        t: 'kafka-monitor', runId: null,
        payload: {
          timestamp: new Date().toLocaleString('sv-SE'),
          brokersLine, intervalSec,
          topics: topicInfos, groups,
          lagHistory: Object.fromEntries(lagHistory),
          errors,
        },
      });

      await sleep(intervalSec * 1000, () => stopped);
    }
    await admin.disconnect().catch(() => {});
  };

  void loop();
  current = { bootstrapServers, intervalSec, stop: () => { stopped = true; } };
}

function emptyPayload(bootstrapServers: string, intervalSec: number, errors: string[]) {
  return {
    timestamp: new Date().toLocaleString('sv-SE'),
    brokersLine: bootstrapServers, intervalSec,
    topics: [], groups: [], lagHistory: {}, errors,
  };
}

/**
 * Per-run lag sampler. Feeds total lag for the run's topic into the run's
 * own timeline so lag spikes line up with the TPS chart on one x-axis.
 */
export function startLagSampler(
  cfg: KafkaConfig,
  onLag: (lag: number) => void,
  onWarn: (msg: string) => void,
): () => void {
  let stopped = false;
  let admin: KafkaAdmin | null = null;

  void (async () => {
    try {
      admin = makeAdmin(cfg.bootstrapServers, cfg.librdkafka);
      await admin.connect();
    } catch (err) {
      onWarn(`lag sampler disabled: ${(err as Error).message}`);
      return;
    }
    while (!stopped) {
      try {
        const offsets = await admin.fetchTopicOffsets(cfg.topic);
        const latestMap = new Map([[cfg.topic, new Map(offsets.map((o) => [o.partition, Number.parseInt(o.high, 10)]))]]);
        const groups = await collectGroups(admin, [cfg.topic], latestMap);
        const wanted = cfg.consumerGroup
          ? groups.filter((g) => g.groupId === cfg.consumerGroup)
          : groups;
        onLag(wanted.reduce((s, g) => s + g.totalLag, 0));
      } catch (err) {
        onWarn(`lag sample failed: ${(err as Error).message}`);
      }
      await sleep(1000, () => stopped);
    }
    await admin.disconnect().catch(() => {});
  })();

  return () => { stopped = true; };
}

function sleep(ms: number, isStopped: () => boolean): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => { clearInterval(poll); resolve(); }, ms);
    const poll = setInterval(() => {
      if (isStopped()) { clearTimeout(timer); clearInterval(poll); resolve(); }
    }, 200);
  });
}
