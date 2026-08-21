import { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { Profile } from '@shared/types.ts';
import { api } from './lib/api.ts';
import { setLanguage, type Language } from './i18n/index.ts';
import { RunView } from './features/RunView.tsx';
import { ConfigView } from './features/ConfigView.tsx';
import { HistoryView } from './features/HistoryView.tsx';
import { KafkaMonitorView } from './features/KafkaMonitorView.tsx';
import { SettingsView } from './features/SettingsView.tsx';

type View = 'run' | 'config' | 'history' | 'kafka' | 'settings';

// Distinct glyphs on purpose: ⚙ and ⛭ are near-identical at 15px, which made
// "Configuration" and "Settings" indistinguishable in the collapsed rail.
const NAV: Array<{ id: View; icon: string }> = [
  { id: 'run', icon: '▶' },
  { id: 'config', icon: '✎' },
  { id: 'history', icon: '⏱' },
  { id: 'kafka', icon: '⚡' },
  { id: 'settings', icon: '⚙' },
];

const THEME_KEY = 'ltd.theme';
const RAIL_KEY = 'ltd.railCollapsed';

export function App() {
  const { t, i18n } = useTranslation();
  const [view, setView] = useState<View>('run');
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [runId, setRunId] = useState<string | null>(null);
  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);
  const [theme, setTheme] = useState<'dark' | 'light'>(
    () => (localStorage.getItem(THEME_KEY) === 'light' ? 'light' : 'dark'),
  );
  const [railCollapsed, setRailCollapsed] = useState(
    () => localStorage.getItem(RAIL_KEY) === '1',
  );

  useEffect(() => { localStorage.setItem(RAIL_KEY, railCollapsed ? '1' : '0'); }, [railCollapsed]);

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem(THEME_KEY, theme);
  }, [theme]);

  const reloadProfiles = useCallback(() => {
    api.profiles().then(setProfiles).catch((e: Error) => notify(e.message, false));
  }, []);

  useEffect(() => {
    reloadProfiles();
    // Reattach to a run that is already in flight (e.g. after a page refresh).
    api.activeRuns().then((runs) => { if (runs.length) setRunId(runs[0].runId); }).catch(() => {});
  }, [reloadProfiles]);

  function notify(msg: string, ok = false): void {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 6000);
  }

  const onError = useCallback((msg: string) => notify(msg, false), []);
  const onSaved = useCallback((msg: string) => notify(msg, true), []);

  return (
    <div className={`app${railCollapsed ? ' rail-collapsed' : ''}`}>
      <nav className="rail">
        <div className="rail-top">
          <div className="rail-logo">⚡</div>
          <span className="rail-brand">{t('app.brand')}</span>
          <button
            className="rail-toggle"
            title={t(railCollapsed ? 'nav.expand' : 'nav.collapse')}
            aria-label={t(railCollapsed ? 'nav.expand' : 'nav.collapse')}
            aria-expanded={!railCollapsed}
            onClick={() => setRailCollapsed((v) => !v)}
          >{railCollapsed ? '»' : '«'}</button>
        </div>
        {NAV.map((n) => (
          <button
            key={n.id}
            className={`rail-btn ${view === n.id ? 'active' : ''}`}
            /* Collapsed, the icon alone is a guessing game — keep the tooltip. */
            title={t(`nav.${n.id}`)}
            onClick={() => setView(n.id)}
          >
            <span className="rail-icon">{n.icon}</span>
            <span className="rail-label">{t(`nav.${n.id}`)}</span>
          </button>
        ))}
        <div className="rail-spacer" />
        <div className="rail-foot">v0.1.0</div>
      </nav>

      <div className="main">
        <header className="topbar">
          <h1>{t('app.title')}</h1>
          <span className="badge badge-muted">{t(`nav.${view}`)}</span>
          <span className="spacer" />
          <div className="btn-group">
            {(['en', 'th'] as Language[]).map((l) => (
              <button
                key={l}
                className={`btn btn-sm ${i18n.language === l ? 'active' : ''}`}
                onClick={() => setLanguage(l)}
              >{l === 'en' ? 'EN' : 'ไทย'}</button>
            ))}
          </div>
          <button className="btn btn-sm" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>
            {theme === 'dark' ? '☾' : '☀'} {t(`common.${theme}`)}
          </button>
        </header>

        <main className="content">
          {view === 'run' ? (
            <RunView runId={runId} setRunId={setRunId} profiles={profiles} onError={onError} />
          ) : null}
          {view === 'config' ? (
            <ConfigView profiles={profiles} reload={reloadProfiles} onError={onError} onSaved={onSaved} />
          ) : null}
          {view === 'history' ? (
            <HistoryView openRun={(id) => { setRunId(id); setView('run'); }} onError={onError} />
          ) : null}
          {view === 'kafka' ? <KafkaMonitorView onError={onError} /> : null}
          {view === 'settings' ? <SettingsView onError={onError} onSaved={onSaved} /> : null}
        </main>
      </div>

      {toast ? <div className={`toast ${toast.ok ? 'ok' : ''}`}>{toast.msg}</div> : null}
    </div>
  );
}
