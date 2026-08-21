import { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { RunRow } from '@shared/types.ts';
import { api, csvUrl } from '../lib/api.ts';
import { compact, dateTime, duration, ms, pct } from '../lib/format.ts';
import { Empty, Panel } from '../components/Panel.tsx';
import { Badge } from '../components/Stat.tsx';
import { SERIES_PALETTE, TimeSeries } from '../components/TimeSeries.tsx';

export function HistoryView(props: { openRun: (id: string) => void; onError: (m: string) => void }) {
  const { t } = useTranslation();
  const [runs, setRuns] = useState<RunRow[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [compare, setCompare] = useState<Array<{ id: string; name: string; x: number[]; rps: number[]; p95: number[] }>>([]);

  const load = (): void => {
    api.runs().then(setRuns).catch((e: Error) => props.onError(e.message));
  };
  useEffect(load, []);

  const toggle = (id: string): void => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  async function buildCompare(): Promise<void> {
    const ids = [...selected];
    try {
      const details = await Promise.all(ids.map((id) => api.run(id)));
      setCompare(details.map((d) => ({
        id: d.id,
        name: `${d.profileName} · ${dateTime(d.startedAt)}`,
        x: d.samples.map((s) => s.elapsed),
        rps: d.samples.map((s) => s.rps),
        p95: d.samples.map((s) => s.latency.p95),
      })));
    } catch (e) { props.onError((e as Error).message); }
  }

  async function remove(id: string): Promise<void> {
    if (!confirm(t('common.confirmDelete'))) return;
    try { await api.deleteRun(id); load(); } catch (e) { props.onError((e as Error).message); }
  }

  // Runs have independent time bases; align on elapsed seconds, pad with null.
  const { x, rpsSeries, p95Series } = useMemo(() => {
    const maxLen = Math.max(0, ...compare.map((c) => c.x.length));
    const axis = Array.from({ length: maxLen }, (_, i) => i);
    const pad = (arr: number[]): Array<number | null> =>
      axis.map((_, i) => (i < arr.length ? arr[i] : null));
    return {
      x: axis,
      rpsSeries: compare.map((c, i) => ({
        label: c.name, color: SERIES_PALETTE[i % SERIES_PALETTE.length], values: pad(c.rps),
      })),
      p95Series: compare.map((c, i) => ({
        label: c.name, color: SERIES_PALETTE[i % SERIES_PALETTE.length], values: pad(c.p95),
      })),
    };
  }, [compare]);

  return (
    <div className="grid">
      <div className="col-12">
        <Panel
          title={t('history.title')}
          actions={
            <>
              <span className="stat-sub">{t('history.selected', { count: selected.size })}</span>
              <button className="btn btn-sm" disabled={selected.size < 2} onClick={() => void buildCompare()}>
                {t('history.compare')}
              </button>
              <a
                className="btn btn-sm"
                href={csvUrl('/api/export/runs.csv', { ids: selected.size ? [...selected].join(',') : undefined })}
                download
              >⇩ {selected.size ? t('history.exportSelected') : t('history.exportAll')}</a>
            </>
          }
          flush
        >
          {runs.length === 0 ? <Empty text={t('common.empty')} /> : (
            <div className="tbl-wrap">
              <table>
                <thead>
                  <tr>
                    <th style={{ width: 28 }} />
                    <th>{t('common.profile')}</th>
                    <th>{t('common.protocol')}</th>
                    <th>{t('history.started')}</th>
                    <th className="r">{t('history.duration')}</th>
                    <th className="r">{t('metrics.totalRequests')}</th>
                    <th className="r">{t('metrics.successRate')}</th>
                    <th className="r">{t('metrics.rps')}</th>
                    <th className="r">p95</th>
                    <th>{t('run.state')}</th>
                    <th className="r">{t('common.actions')}</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((r) => (
                    <tr key={r.id} className="clickable" onClick={() => props.openRun(r.id)}>
                      <td onClick={(e) => e.stopPropagation()}>
                        <input className="checkbox" type="checkbox"
                          checked={selected.has(r.id)} onChange={() => toggle(r.id)} />
                      </td>
                      <td>{r.profileName}</td>
                      <td><span className="badge badge-muted">{t(`protocol.${r.protocol}`)}</span></td>
                      <td>{dateTime(r.startedAt)}</td>
                      <td className="r">{duration(r.summary?.durationMs ?? (r.endedAt ? r.endedAt - r.startedAt : null))}</td>
                      <td className="r">{compact(r.summary?.totalRequests)}</td>
                      <td className="r">{r.summary ? pct(r.summary.successRatePct, 1) : '—'}</td>
                      <td className="r">{compact(r.summary?.rpsAvg)}</td>
                      <td className="r">{ms(r.summary?.latency.p95)}</td>
                      <td>
                        <Badge tone={r.state === 'passed' ? 'pass' : r.state === 'running' ? 'info'
                          : r.state === 'stopped' ? 'warn' : 'fail'}>
                          {t(`run.states.${r.state}`)}
                        </Badge>
                      </td>
                      <td className="r" onClick={(e) => e.stopPropagation()}>
                        <a className="btn btn-sm" href={csvUrl(`/api/runs/${r.id}/export.csv`, { type: 'all' })} download>⇩</a>
                        {' '}
                        <button className="btn btn-sm" onClick={() => void remove(r.id)}>✕</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Panel>
      </div>

      {compare.length ? (
        <>
          <div className="col-6">
            <Panel title={`${t('history.compare')} · ${t('metrics.rps')}`}>
              <TimeSeries x={x} series={rpsSeries} height={230} yLabel={t('metrics.rps')} />
            </Panel>
          </div>
          <div className="col-6">
            <Panel title={`${t('history.compare')} · p95`}>
              <TimeSeries x={x} series={p95Series} height={230} yLabel={t('units.ms')} />
            </Panel>
          </div>
        </>
      ) : null}
    </div>
  );
}
