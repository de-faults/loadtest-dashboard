import { useTranslation } from 'react-i18next';
import type { GroupInfo } from '@shared/types.ts';
import { compact, num } from '../lib/format.ts';
import { Badge } from './Stat.tsx';

/**
 * One card per consumer group: state, health, total lag, and each topic's
 * share of it with per-partition lag. Shared by the Kafka Monitor tab and a
 * Kafka run's dashboard, so both read the same way.
 */
export function KafkaGroupCards({ groups, topicFilter }: { groups: GroupInfo[]; topicFilter?: string }) {
  const { t } = useTranslation();
  return (
    <>
      {groups.map((g) => (
        <div key={g.groupId} style={{
          border: '1px solid var(--border)', borderRadius: 4,
          padding: 10, marginBottom: 8, background: 'var(--bg-alt)',
        }}>
          <div className="inline" style={{ flexWrap: 'wrap', marginBottom: 8 }}>
            <strong style={{ color: 'var(--accent)' }}>{g.groupId}</strong>
            <Badge tone={stateTone(g.state)}>{stateLabel(g.state)}</Badge>
            {/* -1: not recorded (a snapshot rebuilt from a run summary). */}
            {g.memberCount >= 0 ? <span className="stat-sub">{g.memberCount} {t('kafka.members')}</span> : null}
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
    </>
  );
}

/**
 * Kafka reports group state as a numeric enum over this client, so the badge
 * showed a bare "5" and the Stable/Dead comparisons never matched.
 */
const GROUP_STATES: Record<string, string> = {
  '0': 'Unknown', '1': 'PreparingRebalance', '2': 'CompletingRebalance',
  '3': 'Stable', '4': 'Dead', '5': 'Empty',
};

export function stateLabel(state: string | number): string {
  return GROUP_STATES[String(state)] ?? String(state);
}

export function stateTone(state: string | number): 'pass' | 'fail' | 'warn' | 'muted' {
  switch (stateLabel(state)) {
    case 'Stable': return 'pass';
    case 'Dead': return 'fail';
    case 'Empty': case 'Unknown': return 'muted';
    default: return 'warn';
  }
}

export function lagClass(lag: number): string {
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
