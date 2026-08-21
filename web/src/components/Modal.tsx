import { useEffect, useRef, type ReactNode } from 'react';

/**
 * Dialog with the usual dismissal affordances: Escape, backdrop click, and a
 * close button. Focus moves into the dialog on open and returns to whatever
 * was focused before, so keyboard users are not dropped at the top of the page.
 */
export function Modal(props: {
  title: ReactNode;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
  wide?: boolean;
}) {
  const panelRef = useRef<HTMLDivElement>(null);
  const restoreRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    restoreRef.current = document.activeElement as HTMLElement | null;
    panelRef.current?.focus();

    const onKey = (e: KeyboardEvent): void => {
      if (e.key === 'Escape') { e.stopPropagation(); props.onClose(); }
    };
    document.addEventListener('keydown', onKey);

    // The page behind must not scroll while a dialog is open.
    const previous = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    return () => {
      document.removeEventListener('keydown', onKey);
      document.body.style.overflow = previous;
      restoreRef.current?.focus?.();
    };
  }, [props.onClose]);

  return (
    <div
      className="modal-backdrop"
      onMouseDown={(e) => { if (e.target === e.currentTarget) props.onClose(); }}
    >
      <div
        className={`modal-panel${props.wide ? ' modal-wide' : ''}`}
        role="dialog"
        aria-modal="true"
        tabIndex={-1}
        ref={panelRef}
      >
        <header className="modal-head">
          <span className="modal-title">{props.title}</span>
          <span className="spacer" />
          <button className="modal-x" onClick={props.onClose} aria-label="Close">✕</button>
        </header>
        <div className="modal-body">{props.children}</div>
        {props.footer ? <footer className="modal-foot">{props.footer}</footer> : null}
      </div>
    </div>
  );
}
