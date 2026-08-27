import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { Profile, RunEvent, RunState, RunSummary, WindowMetrics } from '@shared/types.ts';
import { api, csvUrl, type RunDetail } from '../lib/api.ts';
import { useEventStream } from '../lib/sse.ts';
import { compact, dateTime, duration, ms, num, pct, timeOnly } from '../lib/format.ts';
import { Empty, Panel } from '../components/Panel.tsx';
import { Badge, Stat } from '../components/Stat.tsx';
import { COLORS, TimeSeries } from '../components/TimeSeries.tsx';

const MAX_LOGS = 400;

export function RunView(props: {
  runId: string | null;
  setRunId: (id: string | null) => void;
  profiles: Profile[];
  onError: (msg: string) => void;
}) {
  const { t } = useTranslation();
  const [selectedProfile, setSelectedProfile] = useState<string>('');
  const [samples, setSamples] = useState<WindowMetrics[]>([]);
  const [logs, setLogs] = useState<Array<{ ts: number; level: string; line: string }>>([]);
  const [summary, setSummary] = useState<RunSummary | null>(null);
  const [live, setLive] = useState<Partial<RunSummary> & { requests: number; success: number; failed: number }>(
    { requests: 0, success: 0, failed: 0 },
  );
  const [state, setState] = useState<RunState | null>(null);
  const [detail, setDetail] = useState<RunDetail | null>(null);
  const [starting, setStarting] = useState(false);
  const logRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!selectedProfile && props.profiles.length) setSelectedProfile(props.profiles[0].id);
  }, [props.profiles, selectedProfile]);

  // Load persisted state whenever the selected run changes (history click or refresh).
  useEffect(() => {
    setSamples([]); setLogs([]); setSummary(null); setDetail(null); setState(null);
    setLive({ requests: 0, success: 0, failed: 0 });
    if (!props.runId) return;
    let cancelled = false;
    api.run(props.runId)
      .then((d) => {
        if (cancelled) return;
        setDetail(d);
        setSamples(d.samples);
        setLogs(d.logs.slice(-MAX_LOGS));
        setSummary(d.summary);
        setState(d.state);
        if (d.summary) {
          setLive({
            requests: d.summary.totalRequests,
            success: d.summary.totalSuccess,
            failed: d.summary.totalFailed,
          });
        }
      })
      .catch((e: Error) => props.onError(e.message));
    return () => { cancelled = true; };
  }, [props.runId]);

  const onEvent = useCallback((ev: RunEvent) => {
    switch (ev.t) {
      case 'start':
        setState('running');
        break;
      case 'tick':
        setSamples((prev) => [...prev, ev.window]);
        setLive((prev) => ({
          requests: prev.requests + ev.window.requests,
          success: prev.success + ev.window.success,
          failed: prev.failed + ev.window.failed,
        }));
        break;
      case 'log':
        setLogs((prev) => [...prev, { ts: ev.ts, level: ev.level, line: ev.line }].slice(-MAX_LOGS));
        break;
      case 'end':
        setSummary(ev.summary);
        setState(ev.state);
        setLive({
          requests: ev.summary.totalRequests,
          success: ev.summary.totalSuccess,
          failed: ev.summary.totalFailed,
        });
        break;
      default:
        break;
    }
  }, []);

  const conn = useEventStream(props.runId, onEvent);
  const isRunning = state === 'running';

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [logs.length]);

  const start = async (): Promise<void> => {
    setStarting(true);
    try {
      const res = await api.startRun({ profileId: selectedProfile });
      props.setRunId(res.runId);
    } catch (e) {
      props.onError(`${t('errors.runFailed')}: ${(e as Error).message}`);
    } finally {
      setStarting(false);
    }
  };

  const stop = async (): Promise<void> => {
    if (!props.runId) return;
    try { await api.stopRun(props.runId); } catch (e) { props.onError((e as Error).message); }
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
  const profile = props.profiles.find((p) => p.id === selectedProfile);

  return (
    <>
      <div className="grid" style={{ marginBottom: 10 }}>
        <div className="col-12">
          <section className="panel">
            <div className="panel-body ctl-bar">
              <span className={`dot ${conn === 'live' ? 'live' : conn === 'down' ? 'dead' : ''}`} />
              <select
                style={{ flex: '1 1 220px' }}
                value={selectedProfile}
                onChange={(e) => setSelectedProfile(e.target.value)}
                disabled={isRunning}
              >
                {props.profiles.length === 0 ? <option value="">{t('common.empty')}</option> : null}
                {props.profiles.map((p) => (
                  <option key={p.id} value={p.id}>{p.name} · {t(`protocol.${p.protocol}`)}</option>
                ))}
              </select>
              {profile ? <span className="badge badge-info">{t(`protocol.${profile.protocol}`)}</span> : null}
              <span className="grow" />
              {state ? <Badge tone={stateTone(state)}>{t(`run.states.${state}`)}</Badge> : null}
              {isRunning ? (
                <button className="btn btn-danger" onClick={() => void stop()}>■ {t('common.stop')}</button>
              ) : (
                <button
                  className="btn btn-primary"
                  disabled={!selectedProfile || starting}
                  onClick={() => void start()}
                >▶ {t('common.start')}</button>
              )}
              {summary ? (
                <a className="btn" href={csvUrl(`/api/runs/${props.runId}/export.csv`, { type: 'all' })} download>
                  ⇩ {t('common.export')}
                </a>
              ) : null}
            </div>
          </section>
        </div>
      </div>

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
