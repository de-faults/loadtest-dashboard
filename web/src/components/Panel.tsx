import type { ReactNode } from 'react';

export function Panel(props: {
  title: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  flush?: boolean;
}) {
  return (
    <section className={`panel ${props.className ?? ''}`}>
      <header className="panel-head">
        <span>{props.title}</span>
        <span className="spacer" />
        {props.actions}
      </header>
      <div className={`panel-body${props.flush ? ' flush' : ''}`}>{props.children}</div>
    </section>
  );
}

export function Empty({ text }: { text: string }) {
  return <div className="empty">{text}</div>;
}
