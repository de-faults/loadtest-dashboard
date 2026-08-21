import i18n from '../i18n/index.ts';

/**
 * Thai locale formatting, with one deliberate exception: digits.
 *
 * `th-TH` defaults to Thai numerals in some environments, which makes a metrics
 * table unreadable for ops. `-u-nu-latn` pins Latin digits while keeping Thai
 * grouping, month names and date order.
 */
function locale(): string {
  return i18n.language === 'th' ? 'th-TH-u-nu-latn' : 'en-US';
}

export function num(n: number | null | undefined, digits = 0): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return n.toLocaleString(locale(), { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

export function compact(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  if (Math.abs(n) >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(1)}k`;
  return num(n, Number.isInteger(n) ? 0 : 1);
}

export function ms(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  if (n >= 1000) return `${num(n / 1000, 2)} s`;
  if (n >= 10) return `${num(n, 0)} ms`;
  return `${num(n, 2)} ms`;
}

export function pct(n: number | null | undefined, digits = 2): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${num(n, digits)}%`;
}

export function duration(msTotal: number | null | undefined): string {
  if (msTotal == null || !Number.isFinite(msTotal)) return '—';
  const total = Math.floor(msTotal / 1000);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const unit = i18n.t('units.sec');
  if (h > 0) return `${h}h ${m}m ${s}${unit === 's' ? 's' : ''}`;
  if (m > 0) return `${m}m ${s}${unit === 's' ? 's' : ''}`;
  return i18n.language === 'th' ? `${s} ${unit}` : `${s}s`;
}

export function dateTime(msEpoch: number | null | undefined): string {
  if (!msEpoch) return '—';
  return new Date(msEpoch).toLocaleString(locale(), {
    year: 'numeric', month: 'short', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
}

export function timeOnly(msEpoch: number): string {
  return new Date(msEpoch).toLocaleTimeString(locale(), {
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
}
