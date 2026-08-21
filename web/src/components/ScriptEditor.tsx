import { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { Protocol, ScriptConfig } from '@shared/types.ts';
import { api } from '../lib/api.ts';
import { SelectField, TextField } from './Fields.tsx';

const MAX_BYTES = 1_000_000;

const ACCEPT: Record<Protocol, string> = {
  rest: '.js,.mjs,.ts',
  socket: '.yml,.yaml',
  kafka: '.mjs,.js',
};

export function ScriptEditor(props: {
  protocol: Protocol;
  value: ScriptConfig;
  onChange: (v: ScriptConfig) => void;
  onError: (msg: string) => void;
}) {
  const { t } = useTranslation();
  const fileRef = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false);
  const v = props.value;
  const set = (patch: Partial<ScriptConfig>): void => props.onChange({ ...v, ...patch });

  async function onFile(file: File): Promise<void> {
    if (file.size > MAX_BYTES) { props.onError(t('script.tooBig')); return; }
    // Read in the browser: the file never needs to be uploaded anywhere, it is
    // stored in the profile and written to a temp file at run time.
    const content = await file.text();
    set({ mode: 'inline', content, filename: file.name });
  }

  async function loadExample(): Promise<void> {
    setBusy(true);
    try {
      const ex = await api.example(props.protocol);
      set({ mode: 'inline', content: ex.content, filename: ex.filename });
    } catch (e) {
      props.onError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  const modeHint = v.mode === 'builtin' ? t('script.builtinHint')
    : v.mode === 'inline' ? t('script.inlineHint')
    : t('script.pathHint');

  const protocolHint = props.protocol === 'rest' ? t('script.restHint')
    : props.protocol === 'socket' ? t('script.socketHint')
    : null;

  return (
    <>
      <div className="section-title">{t('script.title')}</div>

      <div className="field-row">
        <SelectField
          label={t('script.mode')}
          value={v.mode}
          hint={modeHint}
          options={[
            { value: 'builtin' as const, label: t('script.builtin') },
            { value: 'inline' as const, label: t('script.inline') },
            { value: 'path' as const, label: t('script.path') },
          ]}
          onChange={(mode) => set({ mode })}
        />
      </div>

      {v.mode === 'builtin' ? null : (
        <>
          {protocolHint ? <div className="field-hint" style={{ marginBottom: 8 }}>{protocolHint}</div> : null}
          {props.protocol === 'kafka' ? (
            <div className="script-warn">⚠ {t('script.kafkaWarn')}</div>
          ) : null}

          {v.mode === 'path' ? (
            <TextField
              label={t('script.filePath')}
              value={v.path}
              placeholder="/Users/me/loadtests/checkout.js"
              onChange={(path) => set({ path })}
            />
          ) : (
            <>
              <div className="inline" style={{ marginBottom: 6 }}>
                <button className="btn btn-sm" disabled={busy} onClick={() => fileRef.current?.click()}>
                  ⇧ {t('script.import')}
                </button>
                <button className="btn btn-sm" disabled={busy} onClick={() => void loadExample()}>
                  {t('script.loadExample')}
                </button>
                {v.filename ? <span className="badge badge-muted">{v.filename}</span> : null}
                <span className="field-hint">
                  {new Blob([v.content]).size.toLocaleString()} B
                </span>
              </div>
              <input
                ref={fileRef}
                type="file"
                accept={ACCEPT[props.protocol]}
                style={{ display: 'none' }}
                onChange={(e) => {
                  const f = e.target.files?.[0];
                  if (f) void onFile(f);
                  // Reset so re-picking the same file fires change again.
                  e.target.value = '';
                }}
              />
              <label className="field">
                <span className="field-label">{t('script.content')}</span>
                <textarea
                  className="script-area"
                  spellCheck={false}
                  value={v.content}
                  onChange={(e) => set({ content: e.target.value })}
                />
              </label>
            </>
          )}
        </>
      )}
    </>
  );
}
