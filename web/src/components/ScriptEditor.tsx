import { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { Protocol, RunConfig, ScriptConfig } from '@shared/types.ts';
import { api } from '../lib/api.ts';
import { CheckField, TextField } from './Fields.tsx';

const MAX_BYTES = 1_000_000;

const ACCEPT: Record<Protocol, string> = {
  rest: '.js,.mjs,.ts,.json',
  socket: '.yml,.yaml,.json',
  kafka: '.yml,.yaml,.json',
};

/**
 * Import a script into the form, or export the form as a script.
 *
 * Import parses — it does not store the text and it never executes the file.
 * Anything the form cannot represent is reported as a warning rather than
 * dropped quietly, so the mapping is never silently lossy.
 */
export function ScriptEditor(props: {
  protocol: Protocol;
  profileName: string;
  config: RunConfig;
  script: ScriptConfig;
  onImported: (protocol: Protocol, config: RunConfig) => void;
  onScriptChange: (v: ScriptConfig) => void;
  onError: (msg: string) => void;
}) {
  const { t } = useTranslation();
  const fileRef = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false);
  const [warnings, setWarnings] = useState<string[]>([]);
  const [note, setNote] = useState<string | null>(null);

  async function onFile(file: File): Promise<void> {
    if (file.size > MAX_BYTES) { props.onError(t('script.tooBig')); return; }
    setBusy(true);
    setWarnings([]);
    setNote(null);
    try {
      const content = await file.text();
      // Let the server sniff the protocol; fall back to the current one.
      let res = await api.importScript({ content, filename: file.name, protocol: 'auto' })
        .catch(() => api.importScript({ content, filename: file.name, protocol: props.protocol }));
      setWarnings(res.warnings);
      setNote(t('script.imported', { file: file.name, protocol: t(`protocol.${res.protocol}`) }));
      props.onImported(res.protocol, res.config);
    } catch (e) {
      props.onError(`${t('script.importFailed')}: ${(e as Error).message}`);
    } finally {
      setBusy(false);
    }
  }

  async function exportScript(): Promise<void> {
    setBusy(true);
    try {
      const { blob, filename } = await api.exportScript(props.config, props.profileName);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      props.onError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  const usingExternal = props.script.mode === 'path';

  return (
    <>
      <div className="section-title">{t('script.title')}</div>

      <div className="inline" style={{ marginBottom: 8 }}>
        <button className="btn btn-sm" disabled={busy} onClick={() => fileRef.current?.click()}>
          ⇧ {t('script.import')}
        </button>
        <button className="btn btn-sm" disabled={busy} onClick={() => void exportScript()}>
          ⇩ {t('script.export')}
        </button>
        <span className="field-hint">{t(`script.hint_${props.protocol}`)}</span>
      </div>

      <input
        ref={fileRef}
        type="file"
        accept={ACCEPT[props.protocol]}
        style={{ display: 'none' }}
        onChange={(e) => {
          const f = e.target.files?.[0];
          if (f) void onFile(f);
          e.target.value = '';
        }}
      />

      {note ? <div className="script-note">✓ {note}</div> : null}

      {warnings.length ? (
        <div className="script-warn">
          <strong>{t('script.warnings', { count: warnings.length })}</strong>
          <ul style={{ margin: '4px 0 0 16px' }}>
            {warnings.map((w, i) => <li key={i}>{w}</li>)}
          </ul>
        </div>
      ) : null}

      <CheckField
        label={t('script.useExternal')}
        value={usingExternal}
        onChange={(on) => props.onScriptChange({
          ...props.script,
          mode: on ? 'path' : 'builtin',
          content: '',
        })}
      />
      {usingExternal ? (
        <TextField
          label={t('script.filePath')}
          hint={t('script.pathHint')}
          value={props.script.path}
          placeholder="/Users/me/loadtests/checkout.js"
          onChange={(path) => props.onScriptChange({ ...props.script, path })}
        />
      ) : null}
    </>
  );
}
