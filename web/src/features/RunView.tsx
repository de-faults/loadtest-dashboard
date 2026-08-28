import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { Profile, Protocol, RunEvent, RunState, RunSummary, WindowMetrics } from '@shared/types.ts';
import { api, csvUrl, type RunDetail } from '../lib/api.ts';
import { useEventStream } from '../lib/sse.ts';
import { compact, dateTime, duration, ms, num, pct, timeOnly } from '../lib/format.ts';
import { Empty, Panel } from '../components/Panel.tsx';
import { Badge, Stat } from '../components/Stat.tsx';
import { COLORS, TimeSeries } from '../components/TimeSeries.tsx';

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
}

const NO_SAMPLES: WindowMetrics[] = [];
const NO_LOGS: Array<{ ts: number; level: string; line: string }> = [];
const NO_LIVE = { requests: 0, success: 0, failed: 0 };

function blankPane(runId: string, profileName: string, protocol: Protocol | null): Pane {
  return {
    runId, profileName, protocol,
    samples: [], logs: [], summary: null, state: 'running', live: { ...NO_LIVE }, detail: null,
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
  };
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

  const hasLag = samples.some((s) => s.consumerLag != null);
  // A Kafka run that asked for lag but produced none is a fact worth showing.
  // Hiding the panel made it look like the feature was missing.
  const wantsLag = isKafka && detail?.config.kafka?.monitorLag === true;

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

            {hasLag || wantsLag ? (
              <div className="col-12">
                <Panel title={t('metrics.consumerLag')}>
                  {hasLag
                    ? <TimeSeries x={x} series={lagSeries} height={190} />
                    : <Empty text={t('metrics.noLagSamples')} />}
                </Panel>
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
                            <td style={{ maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis' }}>{e.sample}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
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

function stateTone(s: RunState): 'pass' | 'fail' | 'warn' | 'info' | 'muted' {
  switch (s) {
    case 'passed': return 'pass';
    case 'failed': case 'error': return 'fail';
    case 'running': return 'info';
    case 'stopped': return 'warn';
    default: return 'muted';
  }
}
