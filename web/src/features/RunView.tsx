import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type {
  ErrorBucket, ErrorOrigin, GroupInfo, KafkaRunSummary, MessageLagGroup, Profile, Protocol, RunEvent, RunState, RunSummary, WindowMetrics,
} from '@shared/types.ts';
import { analyzeCapacity, SAFE_FACTOR } from '@shared/capacity.ts';
import type { CapacityReport, CapacityStep } from '@shared/capacity.ts';
import { api, csvUrl, type RunDetail } from '../lib/api.ts';
import { useEventStream } from '../lib/sse.ts';
import { compact, dateTime, duration, ms, num, pct, timeOnly } from '../lib/format.ts';
import { Empty, Panel } from '../components/Panel.tsx';
import { Badge, Stat } from '../components/Stat.tsx';
import { COLORS, SERIES_PALETTE, TimeSeries } from '../components/TimeSeries.tsx';
import { KafkaGroupCards, lagClass, stateLabel } from '../components/KafkaGroups.tsx';

const MAX_LOGS = 400;

/**
 * One selected profile's run: its own timeline, logs and verdict.
 *
 * Runs happen side by side, so nothing here may be shared — a single set of
 * samples would interleave two profiles into one meaningless chart.
 */
interface Pane {
  runId: string;
  profileName: string;
  protocol: Protocol | null;
  samples: WindowMetrics[];
  logs: Array<{ ts: number; level: string; line: string }>;
  summary: RunSummary | null;
  state: RunState | null;
  live: { requests: number; success: number; failed: number };
  detail: RunDetail | null;
  /** Kafka: the latest per-partition lag snapshot, pushed live by the sampler. */
  lag: { ts: number; groups: GroupInfo[] } | null;
  /**
   * Kafka: total lag per group over time, built from the live snapshots. Kept
   * in the browser only, like the Kafka Monitor tab's own history.
   */
  lagHistory: LagHistory;
}

interface LagHistory {
  ts: number[];
  /** Aligned with `ts`; null where the group was absent from that snapshot. */
  byGroup: Record<string, Array<number | null>>;
}

/** One hour of 1 s snapshots. */
const MAX_LAG_HISTORY = 3600;
const NO_LAG_HISTORY: LagHistory = { ts: [], byGroup: {} };

function appendLagHistory(h: LagHistory, ts: number, groups: GroupInfo[]): LagHistory {
  const drop = h.ts.length >= MAX_LAG_HISTORY ? 1 : 0;
  const byGroup: LagHistory['byGroup'] = {};
  const seen = new Set<string>();
  for (const g of groups) {
    seen.add(g.groupId);
    // A group that appears mid-run has no past: pad its series with nulls.
    const prev = h.byGroup[g.groupId] ?? Array<number | null>(h.ts.length).fill(null);
    byGroup[g.groupId] = [...prev.slice(drop), g.totalLag];
  }
  for (const [id, values] of Object.entries(h.byGroup)) {
    if (!seen.has(id)) byGroup[id] = [...values.slice(drop), null];
  }
  return { ts: [...h.ts.slice(drop), ts], byGroup };
}

const NO_SAMPLES: WindowMetrics[] = [];
const NO_LOGS: Array<{ ts: number; level: string; line: string }> = [];
const NO_LIVE = { requests: 0, success: 0, failed: 0 };

function blankPane(runId: string, profileName: string, protocol: Protocol | null): Pane {
  return {
    runId, profileName, protocol,
    samples: [], logs: [], summary: null, state: 'running', live: { ...NO_LIVE }, detail: null,
    lag: null, lagHistory: NO_LAG_HISTORY,
  };
}

function paneFromDetail(d: RunDetail): Pane {
  return {
    runId: d.id,
    profileName: d.profileName,
    protocol: d.protocol,
    samples: d.samples,
    logs: d.logs.slice(-MAX_LOGS),
    summary: d.summary,
    state: d.state,
    live: d.summary
      ? { requests: d.summary.totalRequests, success: d.summary.totalSuccess, failed: d.summary.totalFailed }
      : d.samples.reduce(
          (acc, w) => ({
            requests: acc.requests + w.requests, success: acc.success + w.success, failed: acc.failed + w.failed,
          }),
          { ...NO_LIVE },
        ),
    detail: d,
    lag: null,
    lagHistory: NO_LAG_HISTORY,
  };
}

/**
 * What the target actually replied to the first request that failed this way.
 * Folded away: the table is a count of failure kinds, the payload is the thing
 * you open when the count is not enough.
 */
/**
 * Attribution line: which hop answered. When a gateway sits in front of the
 * service, this is the difference between "our service is broken" and "we never
 * reached it" — so it leads the panel, above the payload it explains.
 */
function ErrorOriginLine({ origin }: { origin: ErrorOrigin }) {
  const { t } = useTranslation();
  const traceIds = Object.entries(origin.traceIds ?? {});
  const tone = origin.verdict === 'gateway' ? 'warn' : origin.verdict === 'service' ? 'fail' : 'muted';
  return (
    <div className="err-origin">
      <div className="err-origin-head">
        <Badge tone={tone}>{t(`run.origin.${origin.verdict}`)}</Badge>
        <strong>{origin.by ?? t('run.origin.unnamed')}</strong>
        {origin.gateway && origin.gateway !== origin.by
          ? <span className="err-origin-via">{t('run.origin.through', { name: origin.gateway })}</span>
          : null}
      </div>
      <div className="err-origin-meta mono">
        {[
          origin.remoteIp ? `${origin.remoteIp}${origin.remotePort ? `:${origin.remotePort}` : ''}` : '',
          origin.proto ?? '',
          origin.url ?? '',
        ].filter(Boolean).join(' · ')}
      </div>
      {origin.evidence.length ? (
        <div className="err-origin-meta">
          <span className="err-origin-label">{t('run.origin.evidence')}</span>
          <span className="mono">{origin.evidence.join(' · ')}</span>
        </div>
      ) : null}
      {traceIds.length ? (
        <div className="err-origin-meta">
          <span className="err-origin-label">{t('run.origin.traceIds')}</span>
          <span className="mono">{traceIds.map(([k, v]) => `${k}: ${v}`).join(' · ')}</span>
        </div>
      ) : null}
    </div>
  );
}

function ErrorBody({ bucket }: { bucket: ErrorBucket }) {
  const { t } = useTranslation();
  const headers = Object.entries(bucket.responseHeaders ?? {});
  return (
    <details className="doc-block">
      <summary>
        {t('run.errorBody')}
        {bucket.bodyContentType ? ` · ${bucket.bodyContentType}` : ''}
        {bucket.bodyChars != null ? ` · ${num(bucket.bodyChars)} ch` : ''}
      </summary>
      {bucket.origin ? <ErrorOriginLine origin={bucket.origin} /> : null}
      {headers.length ? (
        <div className="err-headers">
          <div className="section-title">{t('run.errorHeaders')}</div>
          {headers.map(([name, value]) => (
            <div key={name} className="err-header mono">
              <span className="err-header-name">{name}</span>
              <span>{value}</span>
            </div>
          ))}
        </div>
      ) : null}
      {bucket.body ? (
        <pre className="logs err-body">{bucket.body}{bucket.bodyTruncated ? '\n…' : ''}</pre>
      ) : (
        <div className="doc-line">{t('run.errorBodyEmpty')}</div>
      )}
    </details>
  );
}

export function RunView(props: {
  runId: string | null;
  setRunId: (id: string | null) => void;
  profiles: Profile[];
  onError: (msg: string) => void;
}) {
  const { t } = useTranslation();
  const [selected, setSelected] = useState<string[]>([]);
  const [panes, setPanes] = useState<Pane[]>([]);
  const [starting, setStarting] = useState(false);
  const logRef = useRef<HTMLDivElement>(null);

  const pane = panes.find((p) => p.runId === props.runId) ?? null;
  const samples = pane?.samples ?? NO_SAMPLES;
  const logs = pane?.logs ?? NO_LOGS;
  const summary = pane?.summary ?? null;
  const state = pane?.state ?? null;
  const detail = pane?.detail ?? null;
  const live = pane?.live ?? NO_LIVE;
  const lagSnapshot = pane?.lag ?? null;
  const lagHistory = pane?.lagHistory ?? NO_LAG_HISTORY;
  const runningCount = panes.filter((p) => p.state === 'running').length;

  useEffect(() => {
    if (selected.length === 0 && props.profiles.length) setSelected([props.profiles[0].id]);
  }, [props.profiles]);

  /** Pull a run's persisted timeline into a tab, whether it is live or finished. */
  const openPane = useCallback(async (runId: string): Promise<void> => {
    try {
      const d = await api.run(runId);
      setPanes((prev) => (prev.some((p) => p.runId === runId)
        ? prev.map((p) => (p.runId === runId ? paneFromDetail(d) : p))
        : [...prev, paneFromDetail(d)]));
    } catch (e) {
      props.onError((e as Error).message);
    }
  }, []);

  // A reload mid-test must come back to every run still in flight, not just
  // whichever one the URL happened to hold.
  useEffect(() => {
    api.activeRuns()
      .then((rs) => Promise.all(rs.map((r) => openPane(r.runId))))
      .catch(() => { /* nothing running */ });
  }, [openPane]);

  // A run picked from history opens as its own tab.
  useEffect(() => {
    if (props.runId && !panes.some((p) => p.runId === props.runId)) void openPane(props.runId);
  }, [props.runId, panes, openPane]);

  // The stream carries every run at once, so each event is filed under the tab
  // it belongs to. Events for runs with no tab open are not ours to show.
  const onEvent = useCallback((ev: RunEvent) => {
    if (ev.t === 'kafka-monitor' || !ev.runId) return;
    const runId = ev.runId;
    setPanes((prev) => {
      const i = prev.findIndex((p) => p.runId === runId);
      if (i < 0) return prev;
      const p = prev[i];
      let next = p;
      switch (ev.t) {
        case 'start':
          next = { ...p, state: 'running' };
          break;
        case 'tick':
          next = {
            ...p,
            samples: [...p.samples, ev.window],
            live: {
              requests: p.live.requests + ev.window.requests,
              success: p.live.success + ev.window.success,
              failed: p.live.failed + ev.window.failed,
            },
          };
          break;
        case 'lag':
          next = {
            ...p,
            lag: { ts: ev.ts, groups: ev.groups },
            lagHistory: appendLagHistory(p.lagHistory, ev.ts, ev.groups),
          };
          break;
        case 'log':
          next = { ...p, logs: [...p.logs, { ts: ev.ts, level: ev.level, line: ev.line }].slice(-MAX_LOGS) };
          break;
        case 'end':
          next = {
            ...p,
            summary: ev.summary,
            state: ev.state,
            live: {
              requests: ev.summary.totalRequests,
              success: ev.summary.totalSuccess,
              failed: ev.summary.totalFailed,
            },
          };
          break;
        default:
          return prev;
      }
      const copy = [...prev];
      copy[i] = next;
      return copy;
    });
  }, []);

  const conn = useEventStream(null, onEvent);
  const isRunning = state === 'running';

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [logs.length]);

  const start = async (): Promise<void> => {
    if (selected.length === 0) return;
    setStarting(true);
    try {
      const res = await api.startRun({ profileIds: selected });
      // Finished tabs for the same profiles stay out of the way: a new run of a
      // profile replaces its previous tab rather than piling up.
      setPanes((prev) => {
        const names = new Set(res.runs.map((r) => r.profileName));
        const kept = prev.filter((p) => p.state === 'running' || !names.has(p.profileName));
        return [...kept, ...res.runs.map((r) => blankPane(r.runId, r.profileName, r.protocol))];
      });
      if (res.runs[0]) props.setRunId(res.runs[0].runId);
      if (res.failed.length) {
        props.onError(`${t('errors.runFailed')}: ${res.failed.map((f) => `${f.profileName} — ${f.error}`).join(' · ')}`);
      }
    } catch (e) {
      props.onError(`${t('errors.runFailed')}: ${(e as Error).message}`);
    } finally {
      setStarting(false);
    }
  };

  const toggleProfile = (id: string): void => {
    setSelected((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  };

  const stop = async (): Promise<void> => {
    if (!props.runId) return;
    try { await api.stopRun(props.runId); } catch (e) { props.onError((e as Error).message); }
  };

  const stopAll = async (): Promise<void> => {
    const running = panes.filter((p) => p.state === 'running');
    const results = await Promise.allSettled(running.map((p) => api.stopRun(p.runId)));
    const failed = results.filter((r) => r.status === 'rejected');
    if (failed.length) props.onError(`${t('run.stopAll')}: ${failed.length}/${running.length}`);
  };

  /** Close a tab. A run still in flight keeps going — closing is not stopping. */
  const closePane = (runId: string): void => {
    setPanes((prev) => {
      const next = prev.filter((p) => p.runId !== runId);
      if (runId === props.runId) props.setRunId(next.length ? next[next.length - 1].runId : null);
      return next;
    });
  };

  const latest = samples.length ? samples[samples.length - 1] : null;
  const elapsedMs = summary
    ? summary.durationMs
    : samples.length ? samples[samples.length - 1].elapsed * 1000 : 0;

  const successRate = summary
    ? summary.successRatePct
    : live.requests ? (live.success / live.requests) * 100 : 0;

  const latency = summary?.latency ?? latest?.latency ?? null;
  const isKafka = (detail?.protocol ?? summary?.protocol) === 'kafka';

  const x = useMemo(() => samples.map((s) => s.elapsed), [samples]);
  // Only plot TPS separately when it is actually a different measurement —
  // for Kafka and sockets it mirrors RPS and a duplicate line adds nothing.
  const tpsDiffers = useMemo(
    () => samples.some((s) => Math.abs(s.tps - s.rps) > 0.05),
    [samples],
  );
  const throughput = useMemo(() => [
    { label: t('metrics.rps'), color: COLORS.green, values: samples.map((s) => s.rps), fill: true },
    ...(tpsDiffers
      ? [{ label: t('metrics.tps'), color: COLORS.cyan, values: samples.map((s) => s.tps) }]
      : []),
    { label: t('metrics.vus'), color: COLORS.purple, values: samples.map((s) => s.vus), axis: 'right' as const },
  ], [samples, tpsDiffers, t]);

  const latencySeries = useMemo(() => [
    { label: 'p90', color: COLORS.cyan, values: samples.map((s) => s.latency.p90) },
    { label: 'p95', color: COLORS.yellow, values: samples.map((s) => s.latency.p95) },
    { label: 'p99', color: COLORS.orange, values: samples.map((s) => s.latency.p99) },
    { label: t('metrics.max'), color: COLORS.red, values: samples.map((s) => s.latency.max) },
  ], [samples, t]);

  const lagSeries = useMemo(() => [
    { label: t('metrics.consumerLag'), color: COLORS.orange, values: samples.map((s) => s.consumerLag ?? null), fill: true },
  ], [samples, t]);

  const msgLagSeries = useMemo(() => [
    { label: t('metrics.min'), color: COLORS.green, values: samples.map((s) => s.messageLag?.min ?? null) },
    { label: 'p50', color: COLORS.cyan, values: samples.map((s) => s.messageLag?.p50 ?? null) },
    { label: 'p95', color: COLORS.yellow, values: samples.map((s) => s.messageLag?.p95 ?? null) },
    { label: 'p99', color: COLORS.orange, values: samples.map((s) => s.messageLag?.p99 ?? null) },
    { label: t('metrics.max'), color: COLORS.red, values: samples.map((s) => s.messageLag?.max ?? null) },
    { label: t('run.msgLagCount'), color: COLORS.purple, values: samples.map((s) => s.messageLag?.count ?? null), axis: 'right' as const },
  ], [samples, t]);
  const hasMsgLag = samples.some((s) => s.messageLag != null) || (summary?.kafka?.messageLag?.length ?? 0) > 0;

  // Recomputed as the ramp advances: the steps already held are readable long
  // before the run ends, which is the point of watching a capacity test.
  const capacity = useMemo(
    () => analyzeCapacity(samples, { thresholds: detail?.config.thresholds }),
    [samples, detail],
  );

  const hasLag = samples.some((s) => s.consumerLag != null);
  // A Kafka run that asked for lag but produced none is a fact worth showing.
  // Hiding the panel made it look like the feature was missing.
  const wantsLag = isKafka && detail?.config.kafka?.monitorLag === true;
  // Live snapshots are not stored; a finished or reopened run falls back to the
  // per-partition final lag its summary kept.
  const monitorSnapshot = lagSnapshot ?? snapshotFromSummary(summary);

  return (
    <>
      <div className="grid" style={{ marginBottom: 10 }}>
        <div className="col-12">
          <section className="panel">
            <div className="panel-body ctl-bar">
              <span className={`dot ${conn === 'live' ? 'live' : conn === 'down' ? 'dead' : ''}`} />
              <span className="badge badge-info">{t('run.nSelected', { count: selected.length })}</span>
              {runningCount ? (
                <span className="badge badge-warn">{t('run.nRunning', { count: runningCount })}</span>
              ) : null}
              <span className="grow" />
              {state ? <Badge tone={stateTone(state)}>{t(`run.states.${state}`)}</Badge> : null}
              <button
                className="btn btn-primary"
                disabled={selected.length === 0 || starting}
                onClick={() => void start()}
              >▶ {t('common.start')}</button>
              {isRunning ? (
                <button className="btn btn-danger" onClick={() => void stop()}>■ {t('common.stop')}</button>
              ) : null}
              {runningCount > 1 ? (
                <button className="btn btn-danger" onClick={() => void stopAll()}>■■ {t('run.stopAll')}</button>
              ) : null}
              {summary ? (
                <a className="btn" href={csvUrl(`/api/runs/${props.runId}/export.csv`, { type: 'all' })} download>
                  ⇩ {t('common.export')}
                </a>
              ) : null}
            </div>
          </section>
        </div>
      </div>

      <div className="grid" style={{ marginBottom: 10 }}>
        <div className="col-12">
          <Panel
            title={t('run.selectProfiles')}
            actions={
              <>
                <button className="btn btn-sm"
                  onClick={() => setSelected(props.profiles.map((p) => p.id))}>{t('run.selectAll')}</button>
                <button className="btn btn-sm" disabled={selected.length === 0}
                  onClick={() => setSelected([])}>{t('run.selectNone')}</button>
              </>
            }
          >
            {props.profiles.length === 0 ? <Empty text={t('common.empty')} /> : (
              <div className="profile-pick">
                {props.profiles.map((p) => (
                  <label key={p.id} className={`pick-item${selected.includes(p.id) ? ' on' : ''}`}>
                    <input
                      type="checkbox"
                      className="checkbox"
                      checked={selected.includes(p.id)}
                      onChange={() => toggleProfile(p.id)}
                    />
                    <span className="pick-name">{p.name}</span>
                    <span className="badge badge-muted">{t(`protocol.${p.protocol}`)}</span>
                  </label>
                ))}
              </div>
            )}
            <div className="field-hint" style={{ marginTop: 6 }}>{t('run.runsConcurrently')}</div>
          </Panel>
        </div>

      </div>

      {panes.length ? (
        <div className="run-tabs" role="tablist">
          {panes.map((p) => (
            <div
              key={p.runId}
              role="tab"
              tabIndex={0}
              aria-selected={p.runId === props.runId}
              className={`run-tab${p.runId === props.runId ? ' on' : ''}`}
              onClick={() => props.setRunId(p.runId)}
              onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') props.setRunId(p.runId); }}
            >
              <span className={`dot ${p.state === 'running' ? 'live' : ''}`} />
              <span className="run-tab-name" title={p.profileName}>{p.profileName}</span>
              {p.state && p.state !== 'running'
                ? <Badge tone={stateTone(p.state)}>{t(`run.states.${p.state}`)}</Badge>
                : null}
              <button
                className="run-tab-x"
                title={t('run.closeTab')}
                aria-label={t('run.closeTab')}
                onClick={(e) => { e.stopPropagation(); closePane(p.runId); }}
              >✕</button>
            </div>
          ))}
        </div>
      ) : null}

      {!props.runId ? <Empty text={t('run.noRun')} /> : null}

      {props.runId ? (
        <>
          <div className="stat-row">
            <Stat label={t('metrics.totalRequests')} value={compact(live.requests)}
              sub={`${num(live.success)} ok · ${num(live.failed)} err`} tone="accent" />
            <Stat label={t('metrics.successRate')} value={pct(successRate)}
              tone={successRate >= 99 ? 'green' : successRate >= 95 ? 'yellow' : 'red'} />
            <Stat label={t('metrics.duration')} value={duration(elapsedMs)} tone="cyan" />
            <Stat
              label={t('metrics.rps')}
              value={compact(summary ? summary.rpsAvg : latest?.rps ?? 0)}
              sub={`${t('metrics.peak')} ${compact(summary?.rpsPeak ?? Math.max(0, ...samples.map((s) => s.rps)))}`}
              tone="green"
            />
            <Stat
              label={t('metrics.tps')}
              value={compact(summary ? summary.tpsAvg : latest?.tps ?? 0)}
              sub={`${t('metrics.peak')} ${compact(summary?.tpsPeak ?? Math.max(0, ...samples.map((s) => s.tps)))}`}
              tone="cyan"
            />
            <Stat label={t('metrics.vus')} value={num(summary ? summary.vusMax : latest?.vus ?? 0)} tone="purple" />
            <Stat label="p95" value={ms(latency?.p95)} sub={`p99 ${ms(latency?.p99)}`}
              tone={(latency?.p95 ?? 0) < 500 ? 'green' : 'yellow'} />
          </div>

          <div className="grid">
            <div className="col-8">
              <Panel title={t('metrics.throughput')}>
                {samples.length ? (
                  <TimeSeries x={x} series={throughput} height={230}
                    yLabel={t('metrics.rps')} rightLabel={t('metrics.vus')} />
                ) : <Empty text={t('common.empty')} />}
              </Panel>
            </div>
            <div className="col-4">
              <Panel title={t('metrics.latency')} flush>
                <div className="tbl-wrap">
                  <table>
                    <tbody>
                      {([
                        ['min', t('metrics.min')], ['avg', t('metrics.avg')],
                        ['p90', 'p90'], ['p95', 'p95'], ['p99', 'p99'], ['max', t('metrics.max')],
                      ] as const).map(([key, label]) => (
                        <tr key={key}>
                          <td>{label}</td>
                          <td className="r">{ms(latency ? latency[key] : null)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Panel>
            </div>

            <div className="col-12">
              <Panel title={t('metrics.latencyOverTime')}>
                {samples.length ? <TimeSeries x={x} series={latencySeries} height={220} yLabel={t('units.ms')} />
                  : <Empty text={t('common.empty')} />}
              </Panel>
            </div>

            {capacity ? (
              <div className="col-12">
                <CapacityPanel runId={props.runId} report={capacity} hasSummary={summary != null} />
              </div>
            ) : null}

            {hasLag || wantsLag ? (
              <div className="col-12">
                <Panel title={t('metrics.consumerLag')}>
                  {hasLag ? (
                    <>
                      <LagStats samples={samples} />
                      <TimeSeries x={x} series={lagSeries} height={190} />
                    </>
                  ) : <Empty text={t('metrics.noLagSamples')} />}
                </Panel>
              </div>
            ) : null}

            {isKafka && (wantsLag || monitorSnapshot) ? (
              <div className="col-12">
                <RunKafkaMonitor
                  snapshot={monitorSnapshot}
                  fromSummary={!lagSnapshot && monitorSnapshot != null}
                  history={lagHistory}
                  startedAt={summary?.startedAt ?? detail?.startedAt ?? lagHistory.ts[0] ?? Date.now()}
                  topic={detail?.config.kafka?.topic ?? ''}
                  live={state === 'running'}
                />
              </div>
            ) : null}

            {hasMsgLag ? (
              <div className="col-12">
                <Panel title={t('run.msgLagTitle')}>
                  <MessageLagLive samples={samples} />
                  <TimeSeries x={x} series={msgLagSeries} height={210} />
                  {summary?.kafka?.messageLag?.length
                    ? <MessageLagTable rows={summary.kafka.messageLag} untracked={summary.kafka.messageLagUntracked ?? 0} />
                    : null}
                  <div className="field-hint" style={{ paddingTop: 8 }}>{t('run.msgLagHint')}</div>
                </Panel>
              </div>
            ) : null}

            {summary?.kafka ? (
              <div className="col-12">
                <KafkaSummaryPanel kafka={summary.kafka} />
              </div>
            ) : null}

            <div className="col-6">
              <Panel title={t('run.checks')} flush>
                {summary?.checks.length ? (
                  <div className="tbl-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>{t('common.name')}</th>
                          <th className="r">{t('metrics.success')}</th>
                          <th className="r">{t('metrics.failed')}</th>
                          <th className="r">{t('run.passRate')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {summary.checks.map((c) => (
                          <tr key={c.name}>
                            <td>{c.name}</td>
                            <td className="r">{num(c.passed)}</td>
                            <td className="r">{num(c.failed)}</td>
                            <td className="r">
                              <span className={c.passRatePct >= 99 ? 'v-green' : c.passRatePct >= 90 ? 'v-yellow' : 'v-red'}>
                                {pct(c.passRatePct, 1)}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : <Empty text={t('common.empty')} />}
              </Panel>
            </div>

            <div className="col-6">
              <Panel
                title={t('run.thresholds')}
                actions={summary ? <Badge tone={summary.verdict === 'pass' ? 'pass' : 'fail'}>
                  {summary.verdict === 'pass' ? t('run.pass') : t('run.fail')}
                </Badge> : null}
                flush
              >
                {summary?.thresholds.length ? (
                  <div className="tbl-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>{t('run.expression')}</th>
                          <th className="r">{t('run.actual')}</th>
                          <th className="r">{t('run.verdict')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {summary.thresholds.map((th, i) => (
                          <tr key={`${th.expr}-${i}`}>
                            <td className="mono">{th.expr}</td>
                            <td className="r">{Number.isNaN(th.actual) ? '—' : num(th.actual, 2)}</td>
                            <td className="r">
                              <Badge tone={th.passed ? 'pass' : 'fail'}>{th.passed ? t('run.pass') : t('run.fail')}</Badge>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : <Empty text={t('common.empty')} />}
              </Panel>
            </div>

            {summary?.errors.length ? (
              <div className="col-6">
                <Panel title={t('run.errors')} flush>
                  <div className="tbl-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>{t('run.kind')}</th>
                          <th className="r">{t('run.count')}</th>
                          <th>{t('run.sample')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {summary.errors.map((e) => (
                          <tr key={e.kind}>
                            <td className="mono">{e.kind}</td>
                            <td className="r v-red">{num(e.count)}</td>
                            <td style={{ maxWidth: 320 }}>
                              <div className="ellipsis" title={e.sample}>{e.sample}</div>
                              {e.origin ? (
                                <div className="err-origin-tag">
                                  {t('run.origin.from', {
                                    name: e.origin.by ?? t(`run.origin.${e.origin.verdict}`),
                                  })}
                                </div>
                              ) : null}
                              {e.body != null ? <ErrorBody bucket={e} /> : null}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </Panel>
              </div>
            ) : null}

            {summary?.customMetrics?.length ? (
              <div className="col-6">
                <Panel title={t('run.customMetrics')} flush>
                  <div className="tbl-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>{t('common.name')}</th>
                          <th>{t('run.type')}</th>
                          <th>{t('run.key')}</th>
                          <th className="r">{t('run.value')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {summary.customMetrics.flatMap((m) => Object.entries(m.values).map(([key, value]) => (
                          <tr key={`${m.name}-${key}`}>
                            <td className="mono">{m.name}</td>
                            <td>{m.type}</td>
                            <td>{key}</td>
                            <td className="r">{num(value, Number.isInteger(value) ? 0 : 4)}</td>
                          </tr>
                        )))}
                      </tbody>
                    </table>
                  </div>
                </Panel>
              </div>
            ) : null}

            {/* One scenario is every ordinary run — saying so would be noise, so
                the split only appears once there is something to compare. */}
            {(summary?.scenarios?.length ?? 0) > 1 ? (
              <div className="col-6">
                <Panel title={t('run.scenarios')} flush>
                  <div className="tbl-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>{t('common.name')}</th>
                          <th className="r">{t('metrics.vus')}</th>
                          <th className="r">{t('metrics.requests')}</th>
                          <th className="r">{t('metrics.successRate')}</th>
                          <th className="r">{t('metrics.p95')}</th>
                          <th className="r">{t('run.share')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {summary!.scenarios.map((sc) => (
                          <tr key={sc.name}>
                            <td>{sc.name}</td>
                            <td className="r">{sc.vusers == null ? '—' : num(sc.vusers)}</td>
                            <td className="r">{sc.requests == null ? '—' : num(sc.requests)}</td>
                            <td className="r">{sc.successRatePct == null ? '—' : pct(sc.successRatePct, 2)}</td>
                            <td className="r">{sc.p95 == null ? '—' : `${num(sc.p95, 1)} ms`}</td>
                            <td className="r">{pct(sc.sharePct, 1)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div className="field-hint" style={{ padding: '8px 10px 0' }}>{t('run.scenariosHint')}</div>
                </Panel>
              </div>
            ) : null}

            <div className={summary?.errors.length ? 'col-6' : 'col-12'}>
              <Panel title={t('run.logs')}>
                <div className="logs" ref={logRef}>
                  {logs.length === 0 ? <Empty text={t('common.empty')} /> : logs.map((l, i) => (
                    <div key={i} className={`log-line ${l.level === 'warn' ? 'log-warn' : l.level === 'error' ? 'log-error' : ''}`}>
                      <span className="log-ts">{timeOnly(l.ts)}</span>{l.line}
                    </div>
                  ))}
                </div>
              </Panel>
            </div>

            {detail ? (
              <div className="col-12">
                <Panel
                  title={t('run.configSnapshot')}
                  actions={<span className="stat-sub">{dateTime(detail.startedAt)} · {detail.target}</span>}
                >
                  <pre className="logs" style={{ maxHeight: 240 }}>{JSON.stringify(detail.config, null, 2)}</pre>
                </Panel>
              </div>
            ) : null}
          </div>
        </>
      ) : null}
    </>
  );
}

/**
 * The capacity curve of a ramp: what each held load step actually served, and
 * the level the next performance test should be dimensioned at.
 *
 * Read from the recorded windows rather than the configured stages, so it works
 * the same for a UI-built ramp and a bring-your-own script with its own stages.
 */
/**
 * Current / min / max / mean / median of total consumer lag, over every sample
 * so far — recomputed on each tick, so it reads live during the run.
 */
function LagStats({ samples }: { samples: WindowMetrics[] }) {
  const { t } = useTranslation();
  const stats = useMemo(() => {
    const lags = samples.flatMap((s) => (s.consumerLag == null ? [] : [s.consumerLag]));
    if (lags.length === 0) return null;
    const sorted = [...lags].sort((a, b) => a - b);
    const mid = sorted.length >> 1;
    return {
      current: lags[lags.length - 1],
      min: sorted[0],
      max: sorted[sorted.length - 1],
      avg: lags.reduce((a, l) => a + l, 0) / lags.length,
      median: sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2,
    };
  }, [samples]);
  if (!stats) return null;
  return (
    <div className="tbl-wrap" style={{ marginBottom: 8 }}>
      <table>
        <thead>
          <tr>
            <th className="r">{t('run.lagCurrent')}</th>
            <th className="r">{t('metrics.min')}</th>
            <th className="r">{t('metrics.max')}</th>
            <th className="r">{t('run.lagMean')}</th>
            <th className="r">{t('run.lagMedian')}</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td className="r num">{num(stats.current)}</td>
            <td className="r num">{num(stats.min)}</td>
            <td className="r num">{num(stats.max)}</td>
            <td className="r num">{num(stats.avg, 1)}</td>
            <td className="r num">{num(stats.median, 1)}</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}

/**
 * Running totals of per-message lag from the 1 s buckets. Min, max and mean
 * combine exactly across buckets; percentiles do not, so those come from the
 * chart live and from the per-group table once the run ends.
 */
function MessageLagLive({ samples }: { samples: WindowMetrics[] }) {
  const { t } = useTranslation();
  const agg = useMemo(() => {
    let count = 0; let sum = 0; let min = Number.POSITIVE_INFINITY; let max = 0;
    for (const s of samples) {
      const m = s.messageLag;
      if (!m || m.count === 0) continue;
      count += m.count; sum += m.avg * m.count;
      if (m.min < min) min = m.min;
      if (m.max > max) max = m.max;
    }
    return count ? { count, min, max, avg: sum / count } : null;
  }, [samples]);
  if (!agg) return null;
  return (
    <div className="tbl-wrap" style={{ marginBottom: 8 }}>
      <table>
        <thead>
          <tr>
            <th className="r">{t('run.msgLagCount')}</th>
            <th className="r">{t('metrics.min')}</th>
            <th className="r">{t('metrics.max')}</th>
            <th className="r">{t('run.lagMean')}</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td className="r num">{num(agg.count)}</td>
            <td className="r num">{ms(agg.min)}</td>
            <td className="r num">{ms(agg.max)}</td>
            <td className="r num">{ms(agg.avg)}</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}

/** Whole-run per-message lag, one row per consumer group. */
function MessageLagTable({ rows, untracked }: { rows: MessageLagGroup[]; untracked: number }) {
  const { t } = useTranslation();
  return (
    <>
      <div className="section-title" style={{ marginTop: 10 }}>{t('run.msgLagByGroup')}</div>
      <div className="tbl-wrap">
        <table>
          <thead>
            <tr>
              <th>{t('run.lagGroups')}</th>
              <th className="r">{t('run.consumed')}</th>
              <th className="r">{t('run.pending')}</th>
              <th className="r">{t('metrics.min')}</th>
              <th className="r">{t('run.lagMean')}</th>
              <th className="r">p50</th>
              <th className="r">p90</th>
              <th className="r">p95</th>
              <th className="r">p99</th>
              <th className="r">{t('metrics.max')}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((m) => (
              <tr key={m.group}>
                <td className="mono">{m.group}</td>
                <td className="r num">{num(m.consumed)}</td>
                <td className={`r num ${m.pending > 0 ? 'v-red' : ''}`}>{num(m.pending)}</td>
                <td className="r num">{ms(m.min)}</td>
                <td className="r num">{ms(m.avg)}</td>
                <td className="r num">{ms(m.p50)}</td>
                <td className="r num">{ms(m.p90)}</td>
                <td className="r num">{ms(m.p95)}</td>
                <td className="r num">{ms(m.p99)}</td>
                <td className="r num">{ms(m.max)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {untracked > 0 ? (
        <div className="field-hint v-yellow" style={{ paddingTop: 6 }}>{t('run.msgLagUntracked', { count: untracked })}</div>
      ) : null}
    </>
  );
}

/**
 * The Kafka Monitor tab, scoped to this run: its topic and the consumer groups
 * the run samples. Fed by the run's own lag sampler, so it needs no separate
 * monitor started and follows the same group the run is judged on.
 */
function RunKafkaMonitor(props: {
  snapshot: { ts: number; groups: GroupInfo[] } | null;
  /** The snapshot was rebuilt from the run summary, not received live. */
  fromSummary: boolean;
  history: LagHistory;
  startedAt: number;
  topic: string;
  live: boolean;
}) {
  const { t } = useTranslation();
  const { snapshot, history } = props;

  const x = useMemo(() => history.ts.map((ts) => Math.round((ts - props.startedAt) / 100) / 10), [history, props.startedAt]);
  const series = useMemo(() => Object.entries(history.byGroup).map(([id, values], i) => ({
    label: id,
    color: SERIES_PALETTE[i % SERIES_PALETTE.length],
    values,
    fill: true,
  })), [history]);

  // The run's topic, as the monitor's topic table shows it — read from the
  // partitions of whichever group reported it (end offsets are per topic).
  const topicRow = useMemo(() => {
    const parts = new Map<number, number>();
    let lag = 0;
    for (const g of snapshot?.groups ?? []) {
      for (const tp of g.topics) {
        if (props.topic && tp.topic !== props.topic) continue;
        lag += tp.totalLag;
        for (const p of tp.partitions) parts.set(p.partition, Math.max(parts.get(p.partition) ?? 0, p.latest));
      }
    }
    return { partitions: parts.size, endOffsetSum: [...parts.values()].reduce((a, b) => a + b, 0), lag };
  }, [snapshot, props.topic]);

  if (!snapshot) {
    return (
      <Panel title={t('run.kafkaMonitor')}>
        <Empty text={props.live ? t('run.monitorWaiting') : t('metrics.noLagSamples')} />
      </Panel>
    );
  }

  return (
    <Panel
      title={t('run.kafkaMonitor')}
      actions={
        <span className="inline">
          <span className={`dot ${props.live && !props.fromSummary ? 'live' : ''}`} />
          <span className="stat-sub">
            {props.fromSummary ? t('run.monitorFromSummary') : timeOnly(snapshot.ts)}
          </span>
        </span>
      }
    >
      <div className="grid">
        <div className="col-4">
          <div className="tbl-wrap">
            <table>
              <thead>
                <tr>
                  <th>{t('kafka.topics')}</th>
                  <th className="r">{t('kafka.partitions')}</th>
                  <th className="r">{t('kafka.endOffset')}</th>
                  <th className="r">{t('kafka.lag')}</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td className="mono">{props.topic || '—'}</td>
                  <td className="r">{topicRow.partitions}</td>
                  <td className="r num">{compact(topicRow.endOffsetSum)}</td>
                  <td className={`r num ${lagClass(topicRow.lag)}`}>{num(topicRow.lag)}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
        <div className="col-8">
          <div className="section-title">{t('kafka.lagGraph')}</div>
          {series.length
            ? <TimeSeries x={x} series={series} height={200} yLabel={t('kafka.lag')} />
            : <Empty text={t('kafka.noLagYet')} />}
        </div>
        <div className="col-12">
          <div className="section-title">{`${t('kafka.groups')} (${snapshot.groups.length})`}</div>
          {snapshot.groups.length
            ? <KafkaGroupCards groups={snapshot.groups} />
            : <Empty text={t('kafka.noGroups')} />}
          {props.fromSummary ? null : (
            <details style={{ marginTop: 6 }}>
              <summary className="stat-sub">{t('run.lagLive')}</summary>
              <LagTable snapshot={snapshot} />
            </details>
          )}
        </div>
      </div>
    </Panel>
  );
}

/**
 * A final lag snapshot rebuilt from what the summary kept: final lag per
 * partition. Group state, members and offsets were not recorded — they read
 * as unknown rather than as zero.
 */
function snapshotFromSummary(summary: RunSummary | null): { ts: number; groups: GroupInfo[] } | null {
  const lag = summary?.kafka?.lag;
  if (!summary || !lag || lag.partitions.length === 0) return null;
  const groups = new Map<string, GroupInfo>();
  for (const p of lag.partitions) {
    const g = groups.get(p.group) ?? { groupId: p.group, state: 'Unknown', memberCount: -1, topics: [], totalLag: 0 };
    let tp = g.topics.find((x) => x.topic === p.topic);
    if (!tp) { tp = { topic: p.topic, totalLag: 0, partitions: [] }; g.topics.push(tp); }
    tp.partitions.push({ partition: p.partition, latest: NaN, committed: NaN, lag: p.finalLag });
    tp.totalLag += p.finalLag;
    g.totalLag += p.finalLag;
    groups.set(p.group, g);
  }
  return { ts: summary.endedAt, groups: [...groups.values()] };
}

/**
 * The latest lag snapshot, one row per partition, worst first. Live while the
 * run samples; after it ends the last snapshot stays, and the summary below
 * has the per-partition peaks.
 */
function LagTable({ snapshot }: { snapshot: { ts: number; groups: GroupInfo[] } }) {
  const { t } = useTranslation();
  const rows = useMemo(() => snapshot.groups
    .flatMap((g) => g.topics.flatMap((tp) => tp.partitions.map((p) => ({ g, topic: tp.topic, p }))))
    .sort((a, b) => b.p.lag - a.p.lag || a.p.partition - b.p.partition), [snapshot]);
  if (rows.length === 0) return null;
  return (
    <>
      <div className="tbl-wrap" style={{ maxHeight: 320, overflowY: 'auto' }}>
        <table>
          <thead>
            <tr>
              <th>{t('run.lagGroups')}</th>
              <th>{t('run.groupState')}</th>
              <th className="r">{t('run.members')}</th>
              <th>{t('config.topic')}</th>
              <th className="r">{t('run.partition')}</th>
              <th className="r">{t('run.endOffset')}</th>
              <th className="r">{t('run.committed')}</th>
              <th className="r">{t('run.lag')}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(({ g, topic, p }) => (
              <tr key={`${g.groupId}|${topic}|${p.partition}`}>
                <td className="mono">{g.groupId}</td>
                <td>{stateLabel(g.state)}</td>
                <td className="r">{g.memberCount}</td>
                <td className="mono">{topic}</td>
                <td className="r">{p.partition}</td>
                <td className="r num">{num(p.latest)}</td>
                <td className="r num">{num(p.committed)}</td>
                <td className={`r num ${p.lag > 0 ? 'v-yellow' : 'v-green'}`}>{num(p.lag)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

/**
 * What a Kafka run put on the topic, and how the consumers coped with it.
 * Lag is split into the load phase and the drain after it: a group that holds
 * steady under load but needs minutes to clear the backlog is a different
 * finding from one that falls behind for good.
 */
function KafkaSummaryPanel({ kafka }: { kafka: KafkaRunSummary }) {
  const { t } = useTranslation();
  const v = kafka.volume;
  const lag = kafka.lag;
  const mb = (bytes: number): string => (bytes >= 1e9 ? `${num(bytes / 1e9, 2)} GB` : `${num(bytes / 1e6, 2)} MB`);

  const drainText = (): string => {
    if (!lag?.drain) return t('run.lagDrainOff');
    if (lag.drain.seconds == null) return t('run.lagDrainNot', { sec: lag.drain.timeoutSec });
    return `${num(lag.drain.seconds, 1)} s`;
  };

  const volumeRows: Array<[string, string]> = [
    [t('run.messagesSent'), num(v.messagesSent)],
    [t('run.messagesAcked'), num(v.messagesAcked)],
    [t('run.dataAcked'), mb(v.bytesAcked)],
    [t('run.avgMessage'), `${num(v.avgMessageBytes)} B`],
    [t('run.mbPerSec'), num(v.mbPerSecAvg, 2)],
    [t('run.mbPerSecPeak'), num(v.mbPerSecPeak, 2)],
    [t('run.loadTime'), duration(v.loadDurationMs)],
    [t('run.stoppedBy'), t(`run.stopReason.${v.stoppedBy}`)],
  ];

  return (
    <Panel title={t('run.kafkaSummary')} flush>
      <div className="grid" style={{ padding: 10 }}>
        <div className="col-6">
          <div className="section-title">{t('run.volume')}</div>
          <div className="tbl-wrap">
            <table>
              <tbody>
                {volumeRows.map(([k, val]) => (
                  <tr key={k}><td>{k}</td><td className="r num">{val}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="col-6">
          <div className="section-title">{t('run.lag')}</div>
          {lag ? (
            <div className="tbl-wrap">
              <table>
                <tbody>
                  <tr><td>{t('run.lagGroups')}</td><td className="r mono">{lag.groups.join(', ')}</td></tr>
                  <tr><td>{t('run.lagStart')}</td><td className="r num">{num(lag.start)}</td></tr>
                  <tr><td>{t('run.lagMax')}</td><td className="r num">{num(lag.max)} <span className="stat-sub">@ {num(lag.maxAtSec, 1)} s</span></td></tr>
                  <tr><td>{t('run.lagMin')}</td><td className="r num">{num(lag.min)}</td></tr>
                  <tr><td>{t('run.lagAvg')}</td><td className="r num">{num(lag.avg, 1)}</td></tr>
                  <tr><td>{t('run.lagMedianLoad')}</td><td className="r num">{num(lag.median, 1)}</td></tr>
                  <tr><td>{t('run.lagEnd')}</td><td className="r num">{num(lag.endOfLoad)}</td></tr>
                  <tr><td>{t('run.lagFinal')}</td><td className="r num">{num(lag.final)}</td></tr>
                  <tr title={t('run.lagGrowthHint')}>
                    <td>{t('run.lagGrowth')}</td>
                    <td className={`r num ${lag.growthPerSec > 0 ? 'v-red' : 'v-green'}`}>{num(lag.growthPerSec, 1)}</td>
                  </tr>
                  <tr>
                    <td>{t('run.lagDrain')}</td>
                    <td className={`r num ${lag.drain && !lag.drain.drained ? 'v-red' : ''}`}>{drainText()}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          ) : <Empty text={t('run.lagNone')} />}
        </div>

        {lag?.partitions.length ? (
          <div className="col-12">
            <div className="section-title">{t('run.lagPartitions')}</div>
            <div className="tbl-wrap">
              <table>
                <thead>
                  <tr>
                    <th>{t('run.lagGroups')}</th>
                    <th>{t('config.topic')}</th>
                    <th className="r">{t('run.partition')}</th>
                    <th className="r">{t('run.maxLag')}</th>
                    <th className="r">{t('run.finalLag')}</th>
                  </tr>
                </thead>
                <tbody>
                  {lag.partitions.map((p) => (
                    <tr key={`${p.group}|${p.topic}|${p.partition}`}>
                      <td className="mono">{p.group}</td>
                      <td className="mono">{p.topic}</td>
                      <td className="r">{p.partition}</td>
                      <td className="r num">{num(p.maxLag)}</td>
                      <td className={`r num ${p.finalLag > 0 ? 'v-yellow' : ''}`}>{num(p.finalLag)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : null}
      </div>
    </Panel>
  );
}

function CapacityPanel(props: { runId: string; report: CapacityReport; hasSummary: boolean }) {
  const { t } = useTranslation();
  const { report } = props;
  const r = report.recommended;
  const sloEmpty = Object.keys(report.slo).length === 0;

  const x = report.steps.map((s) => s.vus);
  // A transaction that issues one request makes TPS a copy of RPS; a second
  // identical line would only crowd the curve.
  const tpsDiffers = report.steps.some((s) => Math.abs(s.tps - s.rps) > 0.05);
  const curve = [
    { label: t('metrics.rps'), color: COLORS.green, values: report.steps.map((s) => s.rps), fill: true },
    ...(tpsDiffers
      ? [{ label: t('metrics.tps'), color: COLORS.cyan, values: report.steps.map((s) => s.tps) }]
      : []),
    { label: 'p95', color: COLORS.yellow, values: report.steps.map((s) => s.p95), axis: 'right' as const },
  ];

  const marker = (st: CapacityStep): { label: string; tone: 'pass' | 'warn' | 'fail' } | null => {
    if (report.breaking?.index === st.index) return { label: t('capacity.breaking'), tone: 'fail' };
    if (report.knee?.index === st.index) return { label: t('capacity.knee'), tone: 'pass' };
    if (report.saturation?.index === st.index) return { label: t('capacity.saturation'), tone: 'warn' };
    return null;
  };

  return (
    <Panel
      title={t('capacity.title')}
      actions={props.hasSummary ? (
        <a className="btn btn-sm" href={csvUrl(`/api/runs/${props.runId}/export.csv`, { type: 'capacity' })} download>
          ⇩ {t('common.export')}
        </a>
      ) : null}
    >
      {r ? (
        <div className="stat-row">
          <Stat label={t('capacity.capacityVus')} value={num(r.vus)} tone="purple"
            sub={t(`capacity.limitedBy_${r.limitedBy}`)} />
          <Stat label={t('capacity.capacityRps')} value={compact(r.rps)} tone="green" />
          <Stat label={t('capacity.recommendedVus')} value={num(r.safeVus)} tone="cyan"
            sub={t('capacity.headroom', { pct: Math.round(SAFE_FACTOR * 100) })} />
          <Stat label={t('capacity.recommendedRps')} value={compact(r.safeRps)} tone="cyan" />
          <Stat label={t('capacity.breakingAt')} tone={report.breaking ? 'red' : 'green'}
            value={report.breaking ? num(report.breaking.vus) : '—'}
            sub={report.breaking ? report.breaking.breaches.join(' · ') : t('capacity.noBreach')} />
        </div>
      ) : null}

      <TimeSeries x={x} series={curve} height={200}
        yLabel={t('metrics.rps')} rightLabel={`p95 ${t('units.ms')}`} />

      <div className="tbl-wrap" style={{ marginTop: 8 }}>
        <table>
          <thead>
            <tr>
              <th className="r">{t('capacity.step')}</th>
              <th className="r">{t('metrics.vus')}</th>
              <th className="r">{t('capacity.hold')}</th>
              <th>{t('capacity.clock')}</th>
              <th className="r">{t('metrics.requests')}</th>
              <th className="r">{t('metrics.rps')}</th>
              <th className="r">{t('metrics.tps')}</th>
              <th className="r">{t('metrics.min')}</th>
              <th className="r">{t('metrics.avg')}</th>
              <th className="r">p90</th>
              <th className="r">p95</th>
              <th className="r">p99</th>
              <th className="r">{t('capacity.p95Worst')}</th>
              <th className="r">{t('capacity.errorRate')}</th>
              <th>{t('run.thresholds')}</th>
              <th>{t('capacity.marker')}</th>
            </tr>
          </thead>
          <tbody>
            {report.steps.map((st) => {
              const m = marker(st);
              return (
                <tr key={st.index}>
                  <td className="r">{st.index}</td>
                  <td className="r">{num(st.vus)}</td>
                  <td className="r">{num(st.durationSec)}s</td>
                  <td className="mono" title={`${dateTime(st.startTs)} → ${dateTime(st.endTs)}`}>
                    {timeOnly(st.startTs)}–{timeOnly(st.endTs)}
                  </td>
                  <td className="r">{num(st.requests)}</td>
                  <td className="r">{num(st.rps, 1)}</td>
                  <td className="r">{num(st.tps, 1)}</td>
                  <td className="r">{ms(st.latencyMin)}</td>
                  <td className="r">{ms(st.latencyAvg)}</td>
                  <td className="r">{ms(st.p90)}</td>
                  <td className="r">{ms(st.p95)}</td>
                  <td className="r">{ms(st.p99)}</td>
                  <td className="r">{ms(st.p95Worst)}</td>
                  <td className="r">
                    <span className={st.errorRatePct === 0 ? '' : st.errorRatePct < 1 ? 'v-yellow' : 'v-red'}>
                      {pct(st.errorRatePct, 2)}
                    </span>
                  </td>
                  <td>
                    <div className="chip-row">
                      {st.thresholds.length === 0 ? <span className="field-hint">—</span> : null}
                      {st.thresholds.map((th, i) => (
                        <span
                          key={`${th.expr}-${i}`}
                          className={`badge badge-${th.passed ? 'pass' : th.gates ? 'fail' : 'muted'}`}
                          title={`${th.metric} = ${Number.isNaN(th.actual) ? '—' : num(th.actual, 2)}${
                            th.gates ? '' : ` · ${t('capacity.notGating')}`}`}
                        >
                          {th.passed ? '✓' : '✗'} <span className="mono">{th.expr}</span>
                          {th.gates ? null : <span className="chip-goal">{t('capacity.goal')}</span>}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td>{m ? <Badge tone={m.tone}>{m.label}</Badge> : null}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="field-hint" style={{ marginTop: 6 }}>
        {sloEmpty ? t('capacity.sloNone') : t('capacity.hint')}
      </div>
    </Panel>
  );
}

function stateTone(s: RunState): 'pass' | 'fail' | 'warn' | 'info' | 'muted' {
  switch (s) {
    case 'passed': return 'pass';
    case 'failed': case 'error': return 'fail';
    case 'running': return 'info';
    case 'stopped': return 'warn';
    default: return 'muted';
  }
}
