import type { ReactNode } from 'react';

export function Field(props: { label: string; hint?: string; children: ReactNode }) {
  return (
    <label className="field">
      <span className="field-label">{props.label}</span>
      {props.children}
      {props.hint ? <span className="field-hint">{props.hint}</span> : null}
    </label>
  );
}

export function TextField(props: {
  label: string; value: string; onChange: (v: string) => void;
  hint?: string; placeholder?: string; type?: string;
}) {
  return (
    <Field label={props.label} hint={props.hint}>
      <input
        type={props.type ?? 'text'}
        value={props.value}
        placeholder={props.placeholder}
        onChange={(e) => props.onChange(e.target.value)}
      />
    </Field>
  );
}

export function NumberField(props: {
  label: string; value: number; onChange: (v: number) => void;
  hint?: string; min?: number; max?: number;
}) {
  return (
    <Field label={props.label} hint={props.hint}>
      <input
        type="number"
        value={Number.isFinite(props.value) ? props.value : 0}
        min={props.min}
        max={props.max}
        onChange={(e) => props.onChange(Number(e.target.value))}
      />
    </Field>
  );
}

export function SelectField<T extends string>(props: {
  label: string; value: T; options: Array<{ value: T; label: string }>;
  onChange: (v: T) => void; hint?: string;
}) {
  return (
    <Field label={props.label} hint={props.hint}>
      <select value={props.value} onChange={(e) => props.onChange(e.target.value as T)}>
        {props.options.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
      </select>
    </Field>
  );
}

export function CheckField(props: { label: string; value: boolean; onChange: (v: boolean) => void }) {
  return (
    <label className="field inline" style={{ flexDirection: 'row', alignItems: 'center' }}>
      <input
        className="checkbox"
        type="checkbox"
        checked={props.value}
        onChange={(e) => props.onChange(e.target.checked)}
      />
      <span className="field-label">{props.label}</span>
    </label>
  );
}

export function TextAreaField(props: {
  label: string; value: string; onChange: (v: string) => void; hint?: string; rows?: number;
}) {
  return (
    <Field label={props.label} hint={props.hint}>
      <textarea rows={props.rows ?? 4} value={props.value} onChange={(e) => props.onChange(e.target.value)} />
    </Field>
  );
}

/**
 * Free-form key/value editor. Used for HTTP headers and for raw librdkafka
 * properties — the latter deliberately accepts *any* key, since passthrough is
 * the reason this client was chosen.
 */
export function KeyValueEditor(props: {
  label: string;
  value: Record<string, string>;
  onChange: (v: Record<string, string>) => void;
  hint?: string;
  suggestions?: string[];
  /** Value autocomplete per key, looked up case-insensitively. */
  valueSuggestions?: Record<string, string[]>;
  addLabel: string;
  removeLabel: string;
  keyLabel: string;
  valueLabel: string;
}) {
  const entries = Object.entries(props.value);
  const listId = `kv-${props.label.replace(/\W/g, '')}`;
  const hasBlankKey = entries.some(([k]) => k.trim() === '');

  const update = (index: number, key: string, val: string): void => {
    const next: Record<string, string> = {};
    entries.forEach(([k, v], i) => {
      if (i === index) { if (key) next[key] = val; }
      else next[k] = v;
    });
    props.onChange(next);
  };

  return (
    <div className="field">
      <span className="field-label">{props.label}</span>
      {props.suggestions?.length ? (
        <datalist id={listId}>
          {props.suggestions.map((s) => <option key={s} value={s} />)}
        </datalist>
      ) : null}
      {entries.map(([k, v], i) => {
        const valueOpts = props.valueSuggestions?.[k.trim().toLowerCase()];
        const valueListId = `${listId}-v-${i}`;
        return (
          <div key={i} className="kv-row">
            <input
              value={k}
              list={props.suggestions?.length ? listId : undefined}
              placeholder={props.keyLabel}
              onChange={(e) => update(i, e.target.value, v)}
            />
            {valueOpts?.length ? (
              <datalist id={valueListId}>
                {valueOpts.map((o) => <option key={o} value={o} />)}
              </datalist>
            ) : null}
            <input
              value={v}
              list={valueOpts?.length ? valueListId : undefined}
              placeholder={props.valueLabel}
              onChange={(e) => update(i, k, e.target.value)}
            />
            <button
              type="button"
              className="btn btn-sm"
              title={props.removeLabel}
              onClick={() => {
                const next = { ...props.value };
                delete next[k];
                props.onChange(next);
              }}
            >✕</button>
          </div>
        );
      })}
      <button
        type="button"
        className="btn btn-sm"
        style={{ alignSelf: 'flex-start' }}
        /* Rows are keyed by name, so a second blank row would collide with the
           first and silently do nothing. Block it instead of looking broken. */
        disabled={hasBlankKey}
        onClick={() => props.onChange({ ...props.value, '': '' })}
      >+ {props.addLabel}</button>
      {props.hint ? <span className="field-hint">{props.hint}</span> : null}
    </div>
  );
}
