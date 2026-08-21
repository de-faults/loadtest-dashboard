import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { AppSettings, RunnerAvailability } from '@shared/types.ts';
import { api } from '../lib/api.ts';
import { Panel } from '../components/Panel.tsx';
import { Badge } from '../components/Stat.tsx';
import { NumberField, SelectField, TextField } from '../components/Fields.tsx';

export function SettingsView(props: { onError: (m: string) => void; onSaved: (m: string) => void }) {
  const { t } = useTranslation();
  const [settings, setSettings] = useState<AppSettings | null>(null);
  const [avail, setAvail] = useState<RunnerAvailability | null>(null);

  useEffect(() => {
    api.settings().then(setSettings).catch((e: Error) => props.onError(e.message));
    api.availability().then(setAvail).catch(() => setAvail(null));
  }, []);

  async function save(): Promise<void> {
    if (!settings) return;
    try {
      setSettings(await api.saveSettings(settings));
      setAvail(await api.availability());
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
      </div>
    </div>
  );
}
