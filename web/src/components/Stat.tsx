import type { ReactNode } from 'react';

export type StatTone = 'accent' | 'green' | 'yellow' | 'red' | 'cyan' | 'purple';

export function Stat(props: { label: string; value: ReactNode; sub?: ReactNode; tone?: StatTone }) {
  return (
    <section className="panel">
      <div className="stat">
        <span className="stat-label">{props.label}</span>
        <span className={`stat-value num v-${props.tone ?? 'accent'}`}>{props.value}</span>
        <span className="stat-sub">{props.sub ?? ' '}</span>
      </div>
    </section>
  );
}

export function Badge({ tone, children }: { tone: 'pass' | 'fail' | 'warn' | 'info' | 'muted'; children: ReactNode }) {
  return <span className={`badge badge-${tone}`}>{children}</span>;
}
