import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { KafkaAuth, KafkaMonitorPayload, RunEvent } from '@shared/types.ts';
import { api, type MonitorStatus } from '../lib/api.ts';
import { CheckField, KeyValueEditor, SelectField, TextField } from '../components/Fields.tsx';
import { useEventStream } from '../lib/sse.ts';
import { compact, num } from '../lib/format.ts';
import { Empty, Panel } from '../components/Panel.tsx';
import { Modal } from '../components/Modal.tsx';
import { Badge } from '../components/Stat.tsx';
import { SERIES_PALETTE, TimeSeries } from '../components/TimeSeries.tsx';

const WINDOWS = [120, 900, 1800, 3600];

export function KafkaMonitorView(props: { onError: (m: string) => void }) {
  const { t } = useTranslation();
  const [payload, setPayload] = useState<KafkaMonitorPayload | null>(null);
  const [status, setStatus] = useState<MonitorStatus | null>(null);
  const [showAuth, setShowAuth] = useState(false);
  const [auth, setAuth] = useState<KafkaAuth>({
    securityProtocol: 'PLAINTEXT', saslMechanism: 'PLAIN',
    username: '', password: '', sslCaLocation: '', sslSkipVerify: false, extra: {},
  });
  const [bootstrap, setBootstrap] = useState('localhost:9092');
  const [interval, setIntervalSec] = useState(3);
  const [windowSec, setWindowSec] = useState(120);
  // Filtering lives in the UI: the server streams everything, the dropdowns
  // narrow it. Env vars only seed the defaults.
  const [topicFilter, setTopicFilter] = useState<string>('');
  const [groupFilter, setGroupFilter] = useState<string>('');
  // Broker settings usually already exist somewhere — a client.properties, a
  // Helm values file, a KafkaJS snippet. Import parses any of them into the form.
  const [showImport, setShowImport] = useState(false);
  const [importText, setImportText] = useState('');
  const [importBusy, setImportBusy] = useState(false);
  const [importNote, setImportNote] = useState<string | null>(null);
  const [importWarnings, setImportWarnings] = useState<string[]>([]);
  const fileRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    api.kafkaMonitor().then((s) => {
      setStatus(s);
      if (s.bootstrapServers) setBootstrap(s.bootstrapServers);
      if (s.intervalSec) setIntervalSec(s.intervalSec);
      // The password is never sent back; everything else is restored so the
      // form reflects the monitor that is actually running.
      if (s.auth) {
        setAuth((prev) => ({ ...prev, ...s.auth!, password: '' }));
        setShowAuth(s.auth.securityProtocol !== 'PLAINTEXT');
      }
    }).catch(() => {});
  }, []);

  const onEvent = useCallback((ev: RunEvent) => {
    if (ev.t === 'kafka-monitor') setPayload(ev.payload);
  }, []);
  const conn = useEventStream(null, onEvent);
  const needsSasl = auth.securityProtocol === 'SASL_SSL' || auth.securityProtocol === 'SASL_PLAINTEXT';
  const usesTls = auth.securityProtocol === 'SSL' || auth.securityProtocol === 'SASL_SSL';

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
        setStatus(await api.stopKafkaMonitor());
        setPayload(null);
      } else {
        const useAuth = auth.securityProtocol !== 'PLAINTEXT'
          || auth.sslCaLocation !== '' || Object.keys(auth.extra).length > 0;
        setStatus(await api.startKafkaMonitor(bootstrap, interval, useAuth ? auth : null));
      }
    } catch (e) { props.onError((e as Error).message); }
  }

  async function applyImport(content: string): Promise<void> {
    if (!content.trim()) { props.onError(t('kafka.importEmpty')); return; }
    setImportBusy(true);
    try {
      const res = await api.importKafkaAuth(content);
      setAuth(res.auth);
      const notes = [t('kafka.importOk', { format: res.format.toUpperCase() })];
      if (res.bootstrapServers) {
        if (status?.running) notes.push(t('kafka.importBrokersHeld', { value: res.bootstrapServers }));
        else { setBootstrap(res.bootstrapServers); notes.push(t('kafka.importBrokers', { value: res.bootstrapServers })); }
      }
      if (res.auth.password) notes.push(t('kafka.importPassword'));
      setImportNote(notes.join(' · '));
      setImportWarnings(res.warnings);
      setShowImport(false);
      setImportText('');
      setShowAuth(true);
    } catch (e) {
      props.onError(`${t('kafka.importFailed')}: ${(e as Error).message}`);
    } finally {
      setImportBusy(false);
    }
  }

  async function onImportFile(file: File): Promise<void> {
    if (file.size > 200_000) { props.onError(t('kafka.importTooBig')); return; }
    await applyImport(await file.text());
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
            <button className={`btn btn-sm ${showAuth ? 'active' : ''}`} onClick={() => setShowAuth((v) => !v)}>
              🔒 {t('kafka.auth')}
            </button>
          </div>

          {showAuth ? (
            <div className="panel-body" style={{ borderTop: '1px solid var(--border)' }}>
              <div className="inline" style={{ marginBottom: 8 }}>
                <button className="btn btn-sm" disabled={importBusy} onClick={() => setShowImport(true)}>
                  ⇧ {t('kafka.importAuth')}
                </button>
                <span className="field-hint">{t('kafka.importAuthHint')}</span>
              </div>

              {importNote ? <div className="script-note">✓ {importNote}</div> : null}
              {importWarnings.length ? (
                <div className="script-warn">
                  <strong>{t('script.warnings', { count: importWarnings.length })}</strong>
                  <ul style={{ margin: '4px 0 0 16px' }}>
                    {importWarnings.map((w, i) => <li key={i}>{w}</li>)}
                  </ul>
                </div>
              ) : null}

              <div className="field-row">
                <SelectField
                  label={t('kafka.securityProtocol')} value={auth.securityProtocol}
                  hint={t('kafka.authHint')}
                  options={(['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'] as const).map((v) => ({ value: v, label: v }))}
                  onChange={(v) => setAuth({ ...auth, securityProtocol: v })}
                />
                {needsSasl ? (
                  <SelectField
                    label={t('kafka.saslMechanism')} value={auth.saslMechanism}
                    options={(['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'GSSAPI', 'OAUTHBEARER'] as const)
                      .map((v) => ({ value: v, label: v }))}
                    onChange={(v) => setAuth({ ...auth, saslMechanism: v })}
                  />
                ) : null}
              </div>

              {needsSasl ? (
                <div className="field-row">
                  <TextField label={t('kafka.username')} value={auth.username}
                    onChange={(v) => setAuth({ ...auth, username: v })} />
                  <TextField label={t('kafka.password')} type="password" value={auth.password}
                    hint={status?.auth?.hasPassword && !auth.password ? t('kafka.passwordKept') : undefined}
                    onChange={(v) => setAuth({ ...auth, password: v })} />
                </div>
              ) : null}

              {usesTls ? (
                <div className="field-row">
                  <TextField label={t('kafka.caLocation')} value={auth.sslCaLocation}
                    placeholder="/etc/ssl/certs/ca.pem"
                    onChange={(v) => setAuth({ ...auth, sslCaLocation: v })} />
                  <CheckField label={t('kafka.skipVerify')} value={auth.sslSkipVerify}
                    onChange={(v) => setAuth({ ...auth, sslSkipVerify: v })} />
                </div>
              ) : null}
              {usesTls && auth.sslSkipVerify ? (
                <div className="script-warn">⚠ {t('kafka.skipVerifyHint')}</div>
              ) : null}

              <KeyValueEditor
                label={t('kafka.extraProps')} value={auth.extra}
                onChange={(v) => setAuth({ ...auth, extra: v })}
                addLabel={t('common.add')} removeLabel={t('common.remove')}
                keyLabel={t('common.key')} valueLabel={t('common.value')}
              />
            </div>
          ) : null}
        </section>
      </div>

      {showImport ? (
        <Modal
          wide
          title={t('kafka.importTitle')}
          onClose={() => setShowImport(false)}
          footer={
            <>
              <button className="btn btn-primary" disabled={importBusy || !importText.trim()}
                onClick={() => void applyImport(importText)}>
                {t('kafka.importApply')}
              </button>
              <button className="btn" disabled={importBusy} onClick={() => fileRef.current?.click()}>
                {t('kafka.importFile')}
              </button>
              <button className="btn" disabled={!importText} onClick={() => setImportText('')}>
                {t('common.clear')}
              </button>
              <span className="spacer" style={{ flex: 1 }} />
              <button className="btn" onClick={() => setShowImport(false)}>{t('common.close')}</button>
            </>
          }
        >
          <div className="field-hint" style={{ marginBottom: 6 }}>{t('kafka.importHint')}</div>
          <textarea
            className="script-area"
            spellCheck={false}
            placeholder={t('kafka.importPlaceholder')}
            value={importText}
            onChange={(e) => setImportText(e.target.value)}
          />
          <div className="field-hint">{t('kafka.importPrivacy')}</div>
        </Modal>
      ) : null}

      <input
        ref={fileRef}
        type="file"
        accept=".json,.yml,.yaml,.properties,.conf,.cfg,.env,.txt"
        style={{ display: 'none' }}
        onChange={(e) => {
          const f = e.target.files?.[0];
          if (f) void onImportFile(f);
          e.target.value = '';
        }}
      />

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
              {series.length
                ? <TimeSeries x={x} series={series} height={230} yLabel={t('kafka.lag')} />
                : <Empty text={payload.groups.length ? t('kafka.noLagYet') : t('kafka.noGroups')} />}
            </Panel>
          </div>

          <div className="col-12">
            <Panel title={`${t('kafka.groups')} (${groups.length})`}>
              {groups.length === 0 ? (
                <Empty text={groupFilter ? t('kafka.noGroupMatch') : t('kafka.noGroups')} />
              ) : groups.map((g) => (
                <div key={g.groupId} style={{
                  border: '1px solid var(--border)', borderRadius: 4,
                  padding: 10, marginBottom: 8, background: 'var(--bg-alt)',
                }}>
                  <div className="inline" style={{ flexWrap: 'wrap', marginBottom: 8 }}>
                    <strong style={{ color: 'var(--accent)' }}>{g.groupId}</strong>
                    <Badge tone={stateTone(g.state)}>{stateLabel(g.state)}</Badge>
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

/**
 * Kafka reports group state as a numeric enum over this client, so the badge
 * showed a bare "5" and the Stable/Dead comparisons never matched.
 */
const GROUP_STATES: Record<string, string> = {
  '0': 'Unknown', '1': 'PreparingRebalance', '2': 'CompletingRebalance',
  '3': 'Stable', '4': 'Dead', '5': 'Empty',
};

function stateLabel(state: string | number): string {
  return GROUP_STATES[String(state)] ?? String(state);
}

function stateTone(state: string | number): 'pass' | 'fail' | 'warn' | 'muted' {
  switch (stateLabel(state)) {
    case 'Stable': return 'pass';
    case 'Dead': return 'fail';
    case 'Empty': case 'Unknown': return 'muted';
    default: return 'warn';
  }
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
