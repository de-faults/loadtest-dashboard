import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type {
  AppSettings, RunnerAvailability, ToolStatus, ToolsInfo,
} from '@shared/types.ts';
import { api } from '../lib/api.ts';
import { Panel } from '../components/Panel.tsx';
import { Badge } from '../components/Stat.tsx';
import { NumberField, SelectField, TextField } from '../components/Fields.tsx';

const PLATFORM_LABEL: Record<ToolsInfo['platform'], string> = {
  darwin: 'macOS', win32: 'Windows', linux: 'Linux', other: '—',
};

/**
 * Install a missing runner without leaving the dashboard.
 *
 * Each button runs one fixed server-side recipe for this platform. Recipes the
 * server cannot run unattended (a sudo prompt has nobody to answer it) come
 * back disabled with the command exposed for copying instead.
 */
function ToolCard(props: {
  tool: ToolStatus;
  onStatus: (s: ToolStatus) => void;
  onError: (m: string) => void;
  onSaved: (m: string) => void;
}) {
  const { t } = useTranslation();
  const { tool } = props;
  const [busy, setBusy] = useState<string | null>(null);
  const [output, setOutput] = useState<string[]>([]);
  const [copied, setCopied] = useState<string | null>(null);

  async function install(methodId: string): Promise<void> {
    setBusy(methodId);
    setOutput([]);
    try {
      const res = await api.installTool(tool.id, methodId);
      setOutput(res.output);
      props.onStatus(res.status);
      if (res.ok) props.onSaved(t('settings.installed', { tool: tool.label }));
      else props.onError(t('settings.installFailed', { tool: tool.label }));
    } catch (e) {
      props.onError((e as Error).message);
    } finally {
      setBusy(null);
    }
  }

  async function copy(methodId: string, command: string): Promise<void> {
    try {
      await navigator.clipboard.writeText(command);
      setCopied(methodId);
      setTimeout(() => setCopied(null), 1500);
    } catch (e) {
      props.onError((e as Error).message);
    }
  }

  return (
    <div className="tool-card">
      <div className="tool-head">
        <strong>{tool.label}</strong>
        <Badge tone={tool.available ? 'pass' : 'fail'}>
          {tool.available ? t('common.available') : t('common.unavailable')}
        </Badge>
        <span className="mono tool-detail">{tool.detail}</span>
        <a className="tool-docs" href={tool.docsUrl} target="_blank" rel="noreferrer">{t('settings.docs')}</a>
      </div>

      {tool.methods.length === 0 ? (
        <div className="doc-line">{t('settings.noMethod')}</div>
      ) : tool.methods.map((m) => (
        <div key={m.id} className="tool-method">
          <div className="tool-method-head">
            <button
              className="btn btn-primary btn-sm"
              disabled={!m.runnable || busy !== null}
              title={m.reason ?? ''}
              onClick={() => void install(m.id)}
            >
              {busy === m.id ? t('settings.installing') : `${t('settings.install')} — ${m.label}`}
            </button>
            <button className="btn btn-sm" onClick={() => void copy(m.id, m.command)}>
              {copied === m.id ? t('settings.copied') : t('settings.copy')}
            </button>
            {m.reason ? <span className="tool-reason">{m.reason}</span> : null}
          </div>
          <pre className="logs err-body tool-cmd">{m.command}</pre>
          {m.note ? <div className="doc-line">{m.note}</div> : null}
        </div>
      ))}

      {output.length ? (
        <details className="doc-block" open>
          <summary>{t('settings.installOutput')}</summary>
          <pre className="logs err-body">{output.join('\n')}</pre>
        </details>
      ) : null}
    </div>
  );
}

export function SettingsView(props: { onError: (m: string) => void; onSaved: (m: string) => void }) {
  const { t } = useTranslation();
  const [settings, setSettings] = useState<AppSettings | null>(null);
  const [avail, setAvail] = useState<RunnerAvailability | null>(null);
  const [tools, setTools] = useState<ToolsInfo | null>(null);

  useEffect(() => {
    api.settings().then(setSettings).catch((e: Error) => props.onError(e.message));
    api.availability().then(setAvail).catch(() => setAvail(null));
    api.tools().then(setTools).catch(() => setTools(null));
  }, []);

  async function save(): Promise<void> {
    if (!settings) return;
    try {
      setSettings(await api.saveSettings(settings));
      setAvail(await api.availability());
      setTools(await api.tools().catch(() => null));
      props.onSaved(t('common.saved'));
    } catch (e) { props.onError((e as Error).message); }
  }

  if (!settings) return <div className="empty">{t('common.loading')}</div>;
  const set = <K extends keyof AppSettings>(k: K, v: AppSettings[K]): void => setSettings({ ...settings, [k]: v });

  return (
    <div className="grid">
      <div className="col-6">
        <Panel
          title={t('settings.title')}
          actions={<button className="btn btn-primary btn-sm" onClick={() => void save()}>{t('common.save')}</button>}
        >
          <div className="section-title">{t('settings.binaries')}</div>
          <TextField label={t('settings.k6Path')} value={settings.k6Path} onChange={(v) => set('k6Path', v)} />
          <TextField label={t('settings.artilleryPath')} value={settings.artilleryPath}
            onChange={(v) => set('artilleryPath', v)} />

          <div className="section-title">{t('settings.csv')}</div>
          <div className="field-row">
            <SelectField label={t('settings.csvDelimiter')} value={settings.csvDelimiter}
              options={[
                { value: ',' as const, label: t('settings.comma') },
                { value: ';' as const, label: t('settings.semicolon') },
                { value: '\t' as const, label: t('settings.tab') },
              ]}
              onChange={(v) => set('csvDelimiter', v)} />
            <SelectField label={t('settings.csvLanguage')} value={settings.csvLanguage}
              options={[{ value: 'en' as const, label: 'English' }, { value: 'th' as const, label: 'ไทย' }]}
              onChange={(v) => set('csvLanguage', v)} />
          </div>

          <div className="section-title">{t('config.advanced')}</div>
          <div className="field-row">
            <NumberField label={t('settings.retention')} value={settings.retentionRuns} min={1}
              onChange={(v) => set('retentionRuns', v)} />
            <NumberField label={t('settings.kafkaInterval')} value={settings.kafkaMonitorIntervalSec} min={1}
              onChange={(v) => set('kafkaMonitorIntervalSec', v)} />
          </div>
        </Panel>
      </div>

      <div className="col-6">
        <Panel title={t('settings.runners')} flush>
          <div className="tbl-wrap">
            <table>
              <tbody>
                {(['rest', 'socket', 'kafka'] as const).map((p) => (
                  <tr key={p}>
                    <td>{t(`protocol.${p}`)}</td>
                    <td>
                      <Badge tone={avail?.[p].available ? 'pass' : 'fail'}>
                        {avail?.[p].available ? t('common.available') : t('common.unavailable')}
                      </Badge>
                    </td>
                    <td className="mono" style={{ color: 'var(--text-faint)' }}>{avail?.[p].detail ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel
          title={`${t('settings.tools')}${tools ? ` · ${PLATFORM_LABEL[tools.platform]}` : ''}`}
          actions={
            <button
              className="btn btn-sm"
              onClick={() => void api.tools().then(setTools).catch(() => setTools(null))}
            >
              {t('common.refresh')}
            </button>
          }
        >
          <div className="doc-line">{t('settings.toolsHint')}</div>
          {tools?.tools.map((tool) => (
            <ToolCard
              key={tool.id}
              tool={tool}
              onError={props.onError}
              onSaved={props.onSaved}
              onStatus={(s) => {
                setTools((prev) => (prev
                  ? { ...prev, tools: prev.tools.map((x) => (x.id === s.id ? s : x)) }
                  : prev));
                void api.availability().then(setAvail).catch(() => undefined);
              }}
            />
          ))}
        </Panel>
      </div>
    </div>
  );
}
