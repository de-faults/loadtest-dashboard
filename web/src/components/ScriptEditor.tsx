import { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { Protocol, RunConfig, ScriptConfig } from '@shared/types.ts';
import { api } from '../lib/api.ts';
import { KeyValueEditor, SelectField, TextField } from './Fields.tsx';
import { Modal } from './Modal.tsx';

const MAX_BYTES = 1_000_000;

const ACCEPT: Record<Protocol, string> = {
  rest: '.js,.mjs,.ts,.json',
  socket: '.yml,.yaml,.json',
  kafka: '.yml,.yaml,.json',
};

type Source = 'auto' | Protocol;

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
  const [showPaste, setShowPaste] = useState(false);
  const [pasted, setPasted] = useState('');
  const [source, setSource] = useState<Source>('auto');
  const [preview, setPreview] = useState<{ text: string; filename: string } | null>(null);
  const [copied, setCopied] = useState(false);

  async function importContent(content: string, label: string): Promise<void> {
    if (!content.trim()) { props.onError(t('script.empty')); return; }
    if (new Blob([content]).size > MAX_BYTES) { props.onError(t('script.tooBig')); return; }
    setBusy(true);
    setWarnings([]);
    setNote(null);
    try {
      const res = await api.importScript({ content, filename: label, protocol: source });
      setWarnings(res.warnings);
      setNote(t('script.imported', { file: label, protocol: t(`protocol.${res.protocol}`) }));
      props.onImported(res.protocol, res.config);
    } catch (e) {
      // Detection failing is a normal outcome for a pasted fragment — say what
      // to do about it rather than only what went wrong.
      const msg = (e as Error).message;
      props.onError(source === 'auto'
        ? `${t('script.importFailed')}: ${msg} — ${t('script.pickProtocol')}`
        : `${t('script.importFailed')}: ${msg}`);
    } finally {
      setBusy(false);
    }
  }

  async function onFile(file: File): Promise<void> {
    if (file.size > MAX_BYTES) { props.onError(t('script.tooBig')); return; }
    await importContent(await file.text(), file.name);
  }

  async function exportScript(): Promise<void> {
    setBusy(true);
    setCopied(false);
    try {
      const { blob, filename } = await api.exportScript(props.config, props.profileName);
      setPreview({ text: await blob.text(), filename });
    } catch (e) {
      props.onError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  function download(): void {
    if (!preview) return;
    const blob = new Blob([preview.text], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = preview.filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function copy(): Promise<void> {
    if (!preview) return;
    try {
      await navigator.clipboard.writeText(preview.text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // clipboard API needs a secure context and permission; fall back to a
      // hidden textarea so copy still works over plain http on a LAN address.
      const ta = document.createElement('textarea');
      ta.value = preview.text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      const ok = document.execCommand('copy');
      document.body.removeChild(ta);
      if (ok) { setCopied(true); setTimeout(() => setCopied(false), 2000); }
      else props.onError(t('script.copyFailed'));
    }
  }

  /** Load the current form into the paste box so it can be edited and re-imported. */
  async function loadCurrent(): Promise<void> {
    setBusy(true);
    try {
      const { blob } = await api.exportScript(props.config, props.profileName);
      setPasted(await blob.text());
      setShowPaste(true);
    } catch (e) {
      props.onError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <div className="section-title">{t('script.title')}</div>

      <div className="inline" style={{ marginBottom: 8 }}>
        <button className="btn btn-sm" disabled={busy} onClick={() => fileRef.current?.click()}>
          ⇧ {t('script.import')}
        </button>
        <button className={`btn btn-sm ${showPaste ? 'active' : ''}`} disabled={busy}
          onClick={() => setShowPaste((v) => !v)}>
          ⌨ {t('script.paste')}
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

      {showPaste ? (
        <div className="paste-box">
          <div className="inline" style={{ marginBottom: 6 }}>
            <select
              style={{ width: 'auto' }}
              value={source}
              onChange={(e) => setSource(e.target.value as Source)}
              title={t('script.sourceProtocol')}
            >
              <option value="auto">{t('script.autoDetect')}</option>
              <option value="rest">{t('protocol.rest')}</option>
              <option value="socket">{t('protocol.socket')}</option>
              <option value="kafka">{t('protocol.kafka')}</option>
            </select>
            <button className="btn btn-sm btn-primary" disabled={busy || !pasted.trim()}
              onClick={() => void importContent(pasted, t('script.pastedLabel'))}>
              {t('script.importPasted')}
            </button>
            <button
              className="btn btn-sm"
              disabled={!pasted.trim()}
              title={t('script.runPastedHint')}
              onClick={() => {
                props.onScriptChange({
                  ...props.script, mode: 'inline', content: pasted,
                  filename: props.script.filename || 'script.js',
                });
                setNote(t('script.runPastedNote'));
                setWarnings([]);
              }}
            >
              ▷ {t('script.runPasted')}
            </button>
            <button className="btn btn-sm" disabled={busy} onClick={() => void loadCurrent()}>
              {t('script.loadCurrent')}
            </button>
            <button className="btn btn-sm" disabled={!pasted} onClick={() => setPasted('')}>
              {t('common.clear')}
            </button>
            <span className="field-hint">{new Blob([pasted]).size.toLocaleString()} B</span>
          </div>
          <textarea
            className="script-area"
            spellCheck={false}
            placeholder={t('script.pastePlaceholder')}
            value={pasted}
            onChange={(e) => setPasted(e.target.value)}
          />
          <div className="field-hint">{t('script.pasteHint')}</div>
        </div>
      ) : null}

      {note ? <div className="script-note">✓ {note}</div> : null}

      {warnings.length ? (
        <div className="script-warn">
          <strong>{t('script.warnings', { count: warnings.length })}</strong>
          <ul style={{ margin: '4px 0 0 16px' }}>
            {warnings.map((w, i) => <li key={i}>{w}</li>)}
          </ul>
        </div>
      ) : null}

      <SelectField
        label={t('script.mode')}
        value={props.script.mode}
        options={[
          { value: 'builtin' as const, label: t('script.modeBuiltin') },
          { value: 'inline' as const, label: t('script.modeInline') },
          { value: 'path' as const, label: t('script.modePath') },
        ]}
        onChange={(mode) => props.onScriptChange({ ...props.script, mode })}
      />

      {props.script.mode === 'inline' ? (
        <div className="field">
          <span className="field-label">{t('script.inlineLabel')}</span>
          <textarea
            className="script-area"
            spellCheck={false}
            placeholder={t('script.inlinePlaceholder')}
            value={props.script.content}
            onChange={(e) => props.onScriptChange({ ...props.script, content: e.target.value })}
          />
          <div className="field-hint">{t('script.inlineHint')}</div>
        </div>
      ) : null}

      {props.script.mode === 'path' ? (
        <TextField
          label={t('script.filePath')}
          hint={t('script.pathHint')}
          value={props.script.path}
          placeholder="/Users/me/loadtests/checkout.js"
          onChange={(path) => props.onScriptChange({ ...props.script, path })}
        />
      ) : null}

      {/* Kafka generators are imported into this process, not spawned, so they
          have no environment of their own to receive. */}
      {props.script.mode !== 'builtin' && props.protocol !== 'kafka' ? (
        <KeyValueEditor
          label={t('script.envTitle')}
          hint={t('script.envHint')}
          value={props.script.env ?? {}}
          onChange={(env) => props.onScriptChange({ ...props.script, env })}
          addLabel={t('common.add')}
          removeLabel={t('common.remove')}
          keyLabel={t('common.key')}
          valueLabel={t('common.value')}
        />
      ) : null}

      {preview ? (
        <Modal
          wide
          title={preview.filename}
          onClose={() => setPreview(null)}
          footer={
            <>
              <button className="btn btn-primary" onClick={() => void copy()}>
                {copied ? `✓ ${t('script.copied')}` : `⧉ ${t('script.copy')}`}
              </button>
              <button className="btn" onClick={download}>⇩ {t('script.download')}</button>
              <span className="spacer" />
              <span className="field-hint">
                {preview.text.split('\n').length} {t('script.lines')} ·{' '}
                {new Blob([preview.text]).size.toLocaleString()} B
              </span>
              <button className="btn" onClick={() => setPreview(null)}>{t('common.close')}</button>
            </>
          }
        >
          <pre className="code-view">{preview.text}</pre>
        </Modal>
      ) : null}
    </>
  );
}
