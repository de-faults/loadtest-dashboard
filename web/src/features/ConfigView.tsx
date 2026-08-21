import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type {
  CheckSpec, KafkaConfig, Profile, Protocol, RestConfig, RunConfig, SocketConfig,
  SocketFlowStep, Stage,
} from '@shared/types.ts';
import { api } from '../lib/api.ts';
import { Empty, Panel } from '../components/Panel.tsx';
import {
  CheckField, KeyValueEditor, NumberField, SelectField, TextAreaField, TextField,
} from '../components/Fields.tsx';
import { ScriptEditor } from '../components/ScriptEditor.tsx';

const PROTOCOLS: Protocol[] = ['rest', 'socket', 'kafka'];

export function ConfigView(props: {
  profiles: Profile[];
  reload: () => void;
  onError: (msg: string) => void;
  onSaved: (msg: string) => void;
}) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState<{ id?: string; name: string; protocol: Protocol; config: RunConfig } | null>(null);
  const [hints, setHints] = useState<string[]>([]);
  const [headerHints, setHeaderHints] = useState<string[]>([]);
  const [headerValueHints, setHeaderValueHints] = useState<Record<string, string[]>>({});

  useEffect(() => {
    api.meta()
      .then((m) => {
        setHints(m.librdkafkaHints);
        setHeaderHints(m.headerHints ?? []);
        setHeaderValueHints(m.headerValueHints ?? {});
      })
      .catch(() => { setHints([]); setHeaderHints([]); });
  }, []);

  useEffect(() => {
    if (!editing && props.profiles.length) select(props.profiles[0]);
    // Only seed the editor once the profile list first arrives.
  }, [props.profiles]);

  function select(p: Profile): void {
    setEditing({ id: p.id, name: p.name, protocol: p.protocol, config: structuredClone(p.config) });
  }

  async function createNew(protocol: Protocol): Promise<void> {
    try {
      const config = await api.defaults(protocol);
      setEditing({ name: `${t('common.newProfile')} · ${protocol}`, protocol, config });
    } catch (e) { props.onError((e as Error).message); }
  }

  async function save(): Promise<void> {
    if (!editing) return;
    try {
      const saved = await api.saveProfile(editing);
      setEditing({ id: saved.id, name: saved.name, protocol: saved.protocol, config: saved.config });
      props.reload();
      props.onSaved(t('common.saved'));
    } catch (e) { props.onError(`${t('errors.saveFailed')}: ${(e as Error).message}`); }
  }

  async function remove(id: string): Promise<void> {
    if (!confirm(t('common.confirmDelete'))) return;
    try {
      await api.deleteProfile(id);
      setEditing(null);
      props.reload();
    } catch (e) { props.onError((e as Error).message); }
  }

  function patchConfig(patch: Partial<RunConfig>): void {
    if (!editing) return;
    setEditing({ ...editing, config: { ...editing.config, ...patch } });
  }

  return (
    <div className="grid">
      <div className="col-3">
        <Panel
          title={t('config.profiles')}
          actions={
            <select
              style={{ width: 'auto' }}
              value=""
              onChange={(e) => { if (e.target.value) void createNew(e.target.value as Protocol); }}
            >
              <option value="">+ {t('common.newProfile')}</option>
              {PROTOCOLS.map((p) => <option key={p} value={p}>{t(`protocol.${p}`)}</option>)}
            </select>
          }
          flush
        >
          {props.profiles.length === 0 ? <Empty text={t('common.empty')} /> : (
            <div className="tbl-wrap">
              <table>
                <tbody>
                  {props.profiles.map((p) => (
                    <tr
                      key={p.id}
                      className="clickable"
                      style={p.id === editing?.id ? { background: 'var(--accent-soft)' } : undefined}
                      onClick={() => select(p)}
                    >
                      <td>
                        <div>{p.name}</div>
                        <div className="stat-sub">{t(`protocol.${p.protocol}`)}</div>
                      </td>
                      <td className="r">
                        <button className="btn btn-sm" onClick={(e) => { e.stopPropagation(); void remove(p.id); }}>✕</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Panel>
      </div>

      <div className="col-9">
        {!editing ? <Empty text={t('common.empty')} /> : (
          <Panel
            title={editing.name}
            actions={
              <>
                <button className="btn btn-primary btn-sm" onClick={() => void save()}>{t('common.save')}</button>
              </>
            }
          >
            <div className="field-row">
              <TextField label={t('common.name')} value={editing.name}
                onChange={(name) => setEditing({ ...editing, name })} />
              <SelectField
                label={t('common.protocol')}
                value={editing.protocol}
                options={PROTOCOLS.map((p) => ({ value: p, label: t(`protocol.${p}`) }))}
                onChange={(protocol) => { void createNew(protocol); }}
              />
            </div>

            {editing.protocol === 'rest' && editing.config.rest ? (
              <RestForm
                value={editing.config.rest}
                headerHints={headerHints}
                headerValueHints={headerValueHints}
                onChange={(rest) => patchConfig({ rest })}
              />
            ) : null}

            {editing.protocol === 'socket' && editing.config.socket ? (
              <SocketForm
                value={editing.config.socket}
                headerHints={headerHints}
                headerValueHints={headerValueHints}
                onChange={(socket) => patchConfig({ socket })}
              />
            ) : null}

            {editing.protocol === 'kafka' && editing.config.kafka ? (
              <KafkaForm
                value={editing.config.kafka}
                hints={hints}
                onChange={(kafka) => patchConfig({ kafka })}
              />
            ) : null}

            <ScriptEditor
              protocol={editing.protocol}
              profileName={editing.name}
              config={editing.config}
              script={editing.config.script ?? { mode: 'builtin', content: '', path: '', filename: '' }}
              onScriptChange={(script) => patchConfig({ script })}
              onImported={(protocol, config) => setEditing({ ...editing, protocol, config })}
              onError={props.onError}
            />

            <ChecksForm
              protocol={editing.protocol}
              checks={editing.config.checks}
              onChange={(checks) => patchConfig({ checks })}
            />

            <div className="section-title">{t('config.thresholdsTitle')}</div>
            <TextAreaField
              label={t('run.thresholds')}
              hint={t('config.thresholdHint')}
              rows={4}
              value={editing.config.thresholds.map((x) => x.expr).join('\n')}
              onChange={(text) => patchConfig({
                thresholds: text.split('\n').map((s) => s.trim()).filter(Boolean).map((expr) => ({ expr })),
              })}
            />
          </Panel>
        )}
      </div>
    </div>
  );
}

// ─── REST ────────────────────────────────────────────────────────────────────

function RestForm({ value, headerHints, headerValueHints, onChange }: {
  value: RestConfig;
  headerHints: string[];
  headerValueHints: Record<string, string[]>;
  onChange: (v: RestConfig) => void;
}) {
  const { t } = useTranslation();
  const set = <K extends keyof RestConfig>(k: K, v: RestConfig[K]): void => onChange({ ...value, [k]: v });

  return (
    <>
      <div className="section-title">{t('config.request')}</div>
      <div className="field-row">
        <SelectField label={t('config.method')} value={value.method}
          options={(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD'] as const).map((m) => ({ value: m, label: m }))}
          onChange={(v) => set('method', v)} />
        <TextField label={t('config.url')} value={value.url} onChange={(v) => set('url', v)} />
      </div>
      <KeyValueEditor
        label={t('config.headers')} value={value.headers} onChange={(v) => set('headers', v)}
        suggestions={headerHints} valueSuggestions={headerValueHints}
        addLabel={t('common.add')} removeLabel={t('common.remove')}
        keyLabel={t('common.key')} valueLabel={t('common.value')}
      />
      <div className="field-row">
        <SelectField label={t('config.bodyType')} value={value.bodyType}
          options={(['none', 'json', 'raw', 'form'] as const).map((b) => ({ value: b, label: b }))}
          onChange={(v) => set('bodyType', v)} />
        <NumberField label={t('config.timeout')} value={value.timeoutSec} min={1}
          onChange={(v) => set('timeoutSec', v)} />
        <NumberField label={t('config.thinkTime')} value={value.thinkTimeMs} min={0}
          onChange={(v) => set('thinkTimeMs', v)} />
      </div>
      {value.bodyType !== 'none' ? (
        <TextAreaField label={t('config.body')} value={value.body} onChange={(v) => set('body', v)} />
      ) : null}

      <div className="section-title">{t('config.auth')}</div>
      <div className="field-row">
        <SelectField label={t('common.type')} value={value.auth.kind}
          options={[
            { value: 'none' as const, label: t('common.none') },
            { value: 'basic' as const, label: 'Basic' },
            { value: 'bearer' as const, label: 'Bearer' },
          ]}
          onChange={(kind) => set('auth', { ...value.auth, kind })} />
        {value.auth.kind === 'basic' ? (
          <>
            <TextField label="Username" value={value.auth.username ?? ''}
              onChange={(v) => set('auth', { ...value.auth, username: v })} />
            <TextField label="Password" type="password" value={value.auth.password ?? ''}
              onChange={(v) => set('auth', { ...value.auth, password: v })} />
          </>
        ) : null}
        {value.auth.kind === 'bearer' ? (
          <TextField label="Token" type="password" value={value.auth.token ?? ''}
            onChange={(v) => set('auth', { ...value.auth, token: v })} />
        ) : null}
      </div>

      <div className="section-title">{t('config.load')}</div>
      <div className="field-row">
        <SelectField label={t('config.loadModel')} value={value.loadModel}
          options={[
            { value: 'stages' as const, label: t('config.stages') },
            { value: 'rate' as const, label: t('config.rate') },
          ]}
          onChange={(v) => set('loadModel', v)} />
      </div>

      {value.loadModel === 'stages' ? (
        <div className="field">
          <span className="field-label">{t('config.stages')}</span>
          {value.stages.map((s, i) => {
            const patch = (next: Partial<Stage>): void => {
              const stages = [...value.stages];
              stages[i] = { ...s, ...next };
              set('stages', stages);
            };
            return (
              <div key={i} className="row-item">
                <div className="row-fields">
                  <NumberField label={t('config.stageDuration')} value={s.duration} min={1}
                    onChange={(v) => patch({ duration: v })} />
                  <NumberField label={t('config.stageTarget')} value={s.target} min={0}
                    onChange={(v) => patch({ target: v })} />
                </div>
                <button className="btn btn-sm row-del" title={t('common.remove')}
                  onClick={() => set('stages', value.stages.filter((_, j) => j !== i))}>✕</button>
              </div>
            );
          })}
          <button className="btn btn-sm" style={{ alignSelf: 'flex-start' }}
            onClick={() => set('stages', [...value.stages, { duration: 30, target: 50 }])}>+ {t('common.add')}</button>
        </div>
      ) : (
        <div className="field-row">
          <NumberField label={t('config.rate')} value={value.rate} min={1} onChange={(v) => set('rate', v)} />
          <NumberField label={t('config.rateDuration')} value={value.rateDurationSec} min={1}
            onChange={(v) => set('rateDurationSec', v)} />
          <NumberField label={t('config.preAllocatedVUs')} value={value.preAllocatedVUs} min={1}
            onChange={(v) => set('preAllocatedVUs', v)} />
        </div>
      )}

      <div className="section-title">{t('config.advanced')}</div>
      <div className="field-row">
        <CheckField label={t('config.followRedirects')} value={value.followRedirects}
          onChange={(v) => set('followRedirects', v)} />
        <CheckField label={t('config.skipTls')} value={value.insecureSkipTlsVerify}
          onChange={(v) => set('insecureSkipTlsVerify', v)} />
      </div>
    </>
  );
}

// ─── WebSocket ───────────────────────────────────────────────────────────────

function SocketForm({ value, headerHints, headerValueHints, onChange }: {
  value: SocketConfig;
  headerHints: string[];
  headerValueHints: Record<string, string[]>;
  onChange: (v: SocketConfig) => void;
}) {
  const { t } = useTranslation();
  const set = <K extends keyof SocketConfig>(k: K, v: SocketConfig[K]): void => onChange({ ...value, [k]: v });

  return (
    <>
      <div className="section-title">{t('config.general')}</div>
      <div className="field-row">
        <TextField label={t('config.url')} value={value.url} onChange={(v) => set('url', v)} />
        <TextField label={t('config.subprotocols')} value={value.subprotocols.join(',')}
          onChange={(v) => set('subprotocols', v.split(',').map((s) => s.trim()).filter(Boolean))} />
      </div>
      <KeyValueEditor
        label={t('config.headers')} value={value.headers} onChange={(v) => set('headers', v)}
        suggestions={headerHints} valueSuggestions={headerValueHints}
        addLabel={t('common.add')} removeLabel={t('common.remove')}
        keyLabel={t('common.key')} valueLabel={t('common.value')}
      />
      <CheckField label={t('config.measureRtt')} value={value.measureRoundTrip}
        onChange={(v) => set('measureRoundTrip', v)} />

      <div className="section-title">{t('config.phases')}</div>
      {value.phases.map((p, i) => {
        const patch = (next: Partial<typeof p>): void => {
          const phases = [...value.phases];
          phases[i] = { ...p, ...next };
          set('phases', phases);
        };
        return (
          <div key={i} className="row-item">
            <div className="row-fields">
              <TextField label={t('common.name')} value={p.name} onChange={(v) => patch({ name: v })} />
              <NumberField label={t('config.stageDuration')} value={p.durationSec} min={1}
                onChange={(v) => patch({ durationSec: v })} />
              <NumberField label={t('config.arrivalRate')} value={p.arrivalRate} min={1}
                onChange={(v) => patch({ arrivalRate: v })} />
              <NumberField label={t('config.rampTo')} value={p.rampTo ?? 0} min={0}
                onChange={(v) => patch({ rampTo: v || undefined })} />
            </div>
            <button className="btn btn-sm row-del" title={t('common.remove')}
              onClick={() => set('phases', value.phases.filter((_, j) => j !== i))}>✕</button>
          </div>
        );
      })}
      <button className="btn btn-sm"
        onClick={() => set('phases', [...value.phases, { name: 'phase', durationSec: 30, arrivalRate: 10 }])}>
        + {t('common.add')}
      </button>

      <div className="section-title">{t('config.flow')}</div>
      {value.flow.map((s, i) => {
        const patch = (next: Partial<SocketFlowStep>): void => {
          const flow = [...value.flow];
          flow[i] = { ...s, ...next };
          set('flow', flow);
        };
        return (
          <div key={i} className="flow-row">
            <select value={s.kind} onChange={(e) => patch({ kind: e.target.value as SocketFlowStep['kind'] })}>
              <option value="send">send</option>
              <option value="think">think</option>
              <option value="expect">expect</option>
            </select>
            <input
              value={s.value}
              placeholder={s.kind === 'think' ? '1' : s.kind === 'expect' ? 'pong' : '{"type":"ping"}'}
              onChange={(e) => patch({ value: e.target.value })}
            />
            <button className="btn btn-sm" title={t('common.remove')}
              onClick={() => set('flow', value.flow.filter((_, j) => j !== i))}>✕</button>
          </div>
        );
      })}
      <button className="btn btn-sm" onClick={() => set('flow', [...value.flow, { kind: 'send', value: '' }])}>
        + {t('common.add')}
      </button>
    </>
  );
}

// ─── Kafka ───────────────────────────────────────────────────────────────────

function KafkaForm({ value, hints, onChange }: {
  value: KafkaConfig; hints: string[]; onChange: (v: KafkaConfig) => void;
}) {
  const { t } = useTranslation();
  const set = <K extends keyof KafkaConfig>(k: K, v: KafkaConfig[K]): void => onChange({ ...value, [k]: v });

  return (
    <>
      <div className="section-title">{t('config.general')}</div>
      <div className="field-row">
        <TextField label={t('config.bootstrapServers')} value={value.bootstrapServers}
          onChange={(v) => set('bootstrapServers', v)} />
        <TextField label={t('config.topic')} value={value.topic} onChange={(v) => set('topic', v)} />
      </div>
      <div className="field-row">
        <SelectField label={t('config.acks')} value={value.acks}
          options={[{ value: '0' as const, label: '0' }, { value: '1' as const, label: '1' }, { value: 'all' as const, label: 'all' }]}
          onChange={(v) => set('acks', v)} />
        <SelectField label={t('config.compression')} value={value.compression}
          options={(['none', 'gzip', 'snappy', 'lz4', 'zstd'] as const).map((c) => ({ value: c, label: c }))}
          onChange={(v) => set('compression', v)} />
        <NumberField label={t('config.producers')} value={value.producers} min={1} max={64}
          onChange={(v) => set('producers', v)} />
      </div>

      <div className="section-title">{t('config.load')}</div>
      <div className="field-row">
        <NumberField label={t('config.targetRate')} value={value.targetRate} min={1}
          onChange={(v) => set('targetRate', v)} />
        <NumberField label={t('config.durationSec')} value={value.durationSec} min={1}
          onChange={(v) => set('durationSec', v)} />
        <NumberField label={t('config.maxMessages')} value={value.maxMessages} min={0}
          onChange={(v) => set('maxMessages', v)} />
      </div>

      <div className="section-title">{t('config.payload')}</div>
      <div className="field-row">
        <SelectField label={t('config.payloadType')} value={value.payloadType}
          options={(['json', 'raw', 'random'] as const).map((p) => ({ value: p, label: p }))}
          onChange={(v) => set('payloadType', v)} />
        <SelectField label={t('config.keyStrategy')} value={value.keyStrategy}
          options={(['none', 'random', 'fixed', 'sequence'] as const).map((k) => ({ value: k, label: k }))}
          onChange={(v) => set('keyStrategy', v)} />
        {value.keyStrategy === 'fixed' ? (
          <TextField label={t('config.keyValue')} value={value.keyValue} onChange={(v) => set('keyValue', v)} />
        ) : null}
        {value.payloadType === 'random' ? (
          <NumberField label={t('config.payloadSize')} value={value.payloadSizeBytes} min={1}
            onChange={(v) => set('payloadSizeBytes', v)} />
        ) : null}
      </div>
      {value.payloadType !== 'random' ? (
        <TextAreaField label={t('config.payload')} hint={t('config.payloadHint')}
          value={value.payload} onChange={(v) => set('payload', v)} />
      ) : null}

      <div className="section-title">{t('config.advanced')}</div>
      <div className="field-row">
        <SelectField label={t('config.latencyMode')} value={value.latencyMode}
          hint={value.latencyMode === 'end-to-end' ? t('config.latencyModeHint') : undefined}
          options={[
            { value: 'produce-ack' as const, label: t('config.latencyModeAck') },
            { value: 'end-to-end' as const, label: t('config.latencyModeE2E') },
          ]}
          onChange={(v) => set('latencyMode', v)} />
        <TextField label={t('config.consumerGroup')} value={value.consumerGroup}
          onChange={(v) => set('consumerGroup', v)} />
      </div>
      <CheckField label={t('config.monitorLag')} value={value.monitorLag} onChange={(v) => set('monitorLag', v)} />

      <KeyValueEditor
        label={t('config.librdkafka')}
        hint={t('config.librdkafkaHint')}
        suggestions={hints}
        value={value.librdkafka}
        onChange={(v) => set('librdkafka', v)}
        addLabel={t('common.add')} removeLabel={t('common.remove')}
        keyLabel={t('common.key')} valueLabel={t('common.value')}
      />
    </>
  );
}

// ─── Checks ──────────────────────────────────────────────────────────────────

function ChecksForm({ protocol, checks, onChange }: {
  protocol: Protocol; checks: CheckSpec[]; onChange: (c: CheckSpec[]) => void;
}) {
  const { t } = useTranslation();
  // Kafka has no response body to inspect — only latency checks apply there.
  const kinds = protocol === 'kafka'
    ? (['latency_under'] as const)
    : (['status', 'body_contains', 'json_path', 'regex', 'latency_under'] as const);

  return (
    <>
      <div className="section-title">{t('config.checksTitle')}</div>
      {checks.map((c, i) => {
        const patch = (next: Partial<CheckSpec>): void => {
          const copy = [...checks];
          copy[i] = { ...c, ...next };
          onChange(copy);
        };
        return (
          <div key={i} className="row-item">
            <div className="row-fields">
              <TextField label={t('common.name')} value={c.name} onChange={(v) => patch({ name: v })} />
              <SelectField label={t('config.checkKind')} value={c.kind}
                options={kinds.map((k) => ({ value: k, label: t(`config.checkKinds.${k}`) }))}
                onChange={(v) => patch({ kind: v })} />
              <TextField label={t('config.checkValue')} value={c.value}
                placeholder={c.kind === 'status' ? '200|201' : undefined}
                onChange={(v) => patch({ value: v })} />
              {c.kind === 'json_path' ? (
                <TextField label={t('config.checkPath')} value={c.path ?? ''}
                  onChange={(v) => patch({ path: v })} />
              ) : null}
              <NumberField label={t('config.minPassRate')} value={c.minPassRatePct ?? 100} min={0} max={100}
                onChange={(v) => patch({ minPassRatePct: v })} />
            </div>
            <button className="btn btn-sm row-del" title={t('common.remove')}
              onClick={() => onChange(checks.filter((_, j) => j !== i))}>✕</button>
            {/* Full width: squeezed into a 170px column this wrapped to three
                lines and knocked the whole row out of alignment. */}
            {c.kind === 'status' ? <div className="row-note">{t('config.statusHint')}</div> : null}
          </div>
        );
      })}
      <button
        className="btn btn-sm"
        onClick={() => onChange([...checks, {
          name: `check ${checks.length + 1}`,
          kind: kinds[0],
          value: protocol === 'kafka' ? '200' : '200',
          minPassRatePct: 99,
        }])}
      >+ {t('common.add')}</button>
    </>
  );
}
