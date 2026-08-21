import { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { KafkaMonitorPayload, RunEvent } from '@shared/types.ts';
import { api } from '../lib/api.ts';
import { useEventStream } from '../lib/sse.ts';
import { compact, num } from '../lib/format.ts';
import { Empty, Panel } from '../components/Panel.tsx';
import { Badge } from '../components/Stat.tsx';
import { SERIES_PALETTE, TimeSeries } from '../components/TimeSeries.tsx';

const WINDOWS = [120, 900, 1800, 3600];

export function KafkaMonitorView(props: { onError: (m: string) => void }) {
  const { t } = useTranslation();
  const [payload, setPayload] = useState<KafkaMonitorPayload | null>(null);
  const [status, setStatus] = useState<{ running: boolean; bootstrapServers: string | null; intervalSec: number } | null>(null);
  const [bootstrap, setBootstrap] = useState('localhost:9092');
  const [interval, setIntervalSec] = useState(3);
  const [windowSec, setWindowSec] = useState(120);
  // Filtering lives in the UI: the server streams everything, the dropdowns
  // narrow it. Env vars only seed the defaults.
  const [topicFilter, setTopicFilter] = useState<string>('');
  const [groupFilter, setGroupFilter] = useState<string>('');

  useEffect(() => {
    api.kafkaMonitor().then((s) => {
      setStatus(s);
      if (s.bootstrapServers) setBootstrap(s.bootstrapServers);
      if (s.intervalSec) setIntervalSec(s.intervalSec);
    }).catch(() => {});
  }, []);

  const onEvent = useCallback((ev: RunEvent) => {
    if (ev.t === 'kafka-monitor') setPayload(ev.payload);
  }, []);
  const conn = useEventStream(null, onEvent);

  const topics = useMemo(
    () => (payload?.topics ?? []).filter((x) => !topicFilter || x.name === topicFilter),
    [payload, topicFilter],
  );
  const groups = useMemo(
    () => (payload?.groups ?? []).filter((g) => !groupFilter || g.groupId === groupFilter),
    [payload, groupFilter],
  );

  const { x, series } = useMemo(() => {
    const history = payload?.lagHistory ?? {};
    const intervalSec = payload?.intervalSec || 3;
    const points = Math.max(1, Math.ceil(windowSec / intervalSec));
    const ids = Object.keys(history).filter((id) => !groupFilter || id === groupFilter);
    const len = Math.min(points, Math.max(1, ...ids.map((id) => history[id]?.length ?? 0)));
    return {
      x: Array.from({ length: len }, (_, i) => -((len - 1 - i) * intervalSec)),
      series: ids.map((id, i) => ({
        label: id,
        color: SERIES_PALETTE[i % SERIES_PALETTE.length],
        values: padStart(history[id]?.slice(-len) ?? [], len),
        fill: true,
      })),
    };
  }, [payload, windowSec, groupFilter]);

  async function toggleMonitor(): Promise<void> {
    try {
      if (status?.running) {
        setStatus(await api.stopKafkaMonitor() as typeof status);
        setPayload(null);
      } else {
        await api.startKafkaMonitor(bootstrap, interval);
        setStatus(await api.kafkaMonitor());
      }
    } catch (e) { props.onError((e as Error).message); }
  }

  return (
    <div className="grid">
      <div className="col-12">
        <section className="panel">
          <div className="panel-body ctl-bar">
            <span className={`dot ${status?.running && conn === 'live' ? 'live' : 'dead'}`} />
            <input value={bootstrap} onChange={(e) => setBootstrap(e.target.value)}
              disabled={status?.running} placeholder="localhost:9092" />
            <input type="number" min={1} value={interval} style={{ flex: '0 1 80px' }}
              onChange={(e) => setIntervalSec(Number(e.target.value))} disabled={status?.running}
              title={t('kafka.interval')} />
            <button className={`btn ${status?.running ? 'btn-danger' : 'btn-primary'}`} onClick={() => void toggleMonitor()}>
              {status?.running ? t('kafka.stop') : t('kafka.start')}
            </button>
            <select value={topicFilter} onChange={(e) => setTopicFilter(e.target.value)}>
              <option value="">{t('kafka.allTopics')}</option>
              {(payload?.topics ?? []).map((x) => <option key={x.name} value={x.name}>{x.name}</option>)}
            </select>
            <select value={groupFilter} onChange={(e) => setGroupFilter(e.target.value)}>
              <option value="">{t('kafka.allGroups')}</option>
              {(payload?.groups ?? []).map((g) => <option key={g.groupId} value={g.groupId}>{g.groupId}</option>)}
            </select>
          </div>
        </section>
      </div>

      {!status?.running && !payload ? (
        <div className="col-12"><Empty text={t('kafka.notRunning')} /></div>
      ) : null}

      {payload ? (
        <>
          {payload.errors.length ? (
            <div className="col-12">
              <Panel title={t('run.errors')}>
                {payload.errors.map((e, i) => <div key={i} className="log-line log-error">{e}</div>)}
              </Panel>
            </div>
          ) : null}

          <div className="col-4">
            <Panel title={`${t('kafka.topics')} (${topics.length})`} flush>
              <div className="tbl-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>{t('common.name')}</th>
                      <th className="r">{t('kafka.partitions')}</th>
                      <th className="r">{t('kafka.endOffset')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topics.map((x) => (
                      <tr key={x.name}>
                        <td>{x.name}</td>
                        <td className="r">{x.partitionCount}</td>
                        <td className="r">{compact(x.endOffsetSum)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Panel>
          </div>

          <div className="col-8">
            <Panel
              title={`${t('kafka.lagGraph')} · ${payload.brokersLine}`}
              actions={
                <div className="btn-group">
                  {WINDOWS.map((w) => (
                    <button key={w} className={`btn btn-sm ${windowSec === w ? 'active' : ''}`}
                      onClick={() => setWindowSec(w)}>
                      {w < 3600 ? `${w / 60 >= 1 ? `${w / 60}m` : `${w}s`}` : '1h'}
                    </button>
                  ))}
                </div>
              }
            >
              {series.length ? <TimeSeries x={x} series={series} height={230} yLabel={t('kafka.lag')} />
                : <Empty text={t('common.empty')} />}
            </Panel>
          </div>

          <div className="col-12">
            <Panel title={`${t('kafka.groups')} (${groups.length})`}>
              {groups.length === 0 ? <Empty text={t('common.empty')} /> : groups.map((g) => (
                <div key={g.groupId} style={{
                  border: '1px solid var(--border)', borderRadius: 4,
                  padding: 10, marginBottom: 8, background: 'var(--bg-alt)',
                }}>
                  <div className="inline" style={{ flexWrap: 'wrap', marginBottom: 8 }}>
                    <strong style={{ color: 'var(--accent)' }}>{g.groupId}</strong>
                    <Badge tone={g.state === 'Stable' ? 'pass' : g.state === 'Dead' ? 'fail' : 'warn'}>{g.state}</Badge>
                    <span className="stat-sub">{g.memberCount} {t('kafka.members')}</span>
                    <Badge tone={healthTone(g.totalLag)}>{t(`kafka.health.${healthKey(g.totalLag)}`)}</Badge>
                    <span className="spacer" style={{ flex: 1 }} />
                    <span className={`num ${lagClass(g.totalLag)}`} style={{ fontWeight: 600 }}>
                      {t('kafka.lag')} = {num(g.totalLag)}
                    </span>
                  </div>
                  {g.topics
                    .filter((x) => !topicFilter || x.topic === topicFilter)
                    .map((x) => {
                      const share = g.totalLag > 0 ? Math.round((x.totalLag / g.totalLag) * 100) : 0;
                      return (
                        <div key={x.topic} style={{ marginBottom: 6 }}>
                          <div className="lag-row">
                            <span className="lag-name" title={x.topic}>{x.topic}</span>
                            <div className="bar-track">
                              <div className="bar-fill" style={{
                                width: `${share}%`,
                                background: x.totalLag === 0 ? 'var(--green)' : x.totalLag < 1000 ? 'var(--yellow)' : 'var(--red)',
                              }} />
                            </div>
                            <span className="stat-sub lag-share">{share}%</span>
                            <span className={`num lag-total ${lagClass(x.totalLag)}`}>{num(x.totalLag)}</span>
                          </div>
                          <div className="pills" style={{ marginTop: 4 }}>
                            {x.partitions.map((p) => (
                              <span key={p.partition} className="pill">p{p.partition}: {compact(p.lag)}</span>
                            ))}
                          </div>
                        </div>
                      );
                    })}
                </div>
              ))}
            </Panel>
          </div>
        </>
      ) : null}
    </div>
  );
}

function padStart(values: number[], len: number): Array<number | null> {
  if (values.length >= len) return values.slice(-len);
  return [...Array<number | null>(len - values.length).fill(null), ...values];
}

function lagClass(lag: number): string {
  return lag === 0 ? 'v-green' : lag < 1000 ? 'v-yellow' : 'v-red';
}

function healthKey(lag: number): 'healthy' | 'caution' | 'warning' | 'critical' {
  if (lag === 0) return 'healthy';
  if (lag < 10_000) return 'caution';
  if (lag < 100_000) return 'warning';
  return 'critical';
}

function healthTone(lag: number): 'pass' | 'warn' | 'fail' {
  const k = healthKey(lag);
  return k === 'healthy' ? 'pass' : k === 'critical' ? 'fail' : 'warn';
}
