import { useEffect, useRef } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { compact } from '../lib/format.ts';

export interface Series {
  label: string;
  color: string;
  values: Array<number | null>;
  /** Plot on the right-hand axis (e.g. VUs against RPS). */
  axis?: 'left' | 'right';
  fill?: boolean;
}

/**
 * uPlot rather than Chart.js: a 30-minute run at 1s resolution is ~1800 points
 * per series and redraws every second — Chart.js drops frames well before that.
 */
export function TimeSeries(props: {
  x: number[];
  series: Series[];
  height?: number;
  yLabel?: string;
  rightLabel?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const plot = useRef<uPlot | null>(null);
  const cfgKey = props.series.map((s) => `${s.label}:${s.axis ?? 'left'}`).join('|');

  useEffect(() => {
    if (!ref.current) return;
    const el = ref.current;
    const css = getComputedStyle(document.documentElement);
    const grid = css.getPropertyValue('--border').trim() || '#2c3235';
    const text = css.getPropertyValue('--text-weak').trim() || '#9fa7b3';
    const hasRight = props.series.some((s) => s.axis === 'right');

    const opts: uPlot.Options = {
      width: el.clientWidth || 600,
      height: props.height ?? 220,
      padding: [8, 10, 0, 0],
      cursor: { drag: { x: true, y: false } },
      legend: { live: true },
      scales: { x: { time: false }, y: {}, y2: {} },
      axes: [
        { stroke: text, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid } },
        {
          stroke: text, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid },
          label: props.yLabel, labelSize: props.yLabel ? 18 : 0,
          values: (_u, splits) => splits.map((v) => compact(v)),
        },
        ...(hasRight
          ? [{
              scale: 'y2', side: 1 as const, stroke: text, grid: { show: false },
              label: props.rightLabel, labelSize: props.rightLabel ? 18 : 0,
              values: (_u: uPlot, splits: number[]) => splits.map((v) => compact(v)),
            }]
          : []),
      ],
      series: [
        { label: 's' },
        ...props.series.map((s) => ({
          label: s.label,
          stroke: s.color,
          width: 1.6,
          scale: s.axis === 'right' ? 'y2' : 'y',
          fill: s.fill ? `${s.color}22` : undefined,
          points: { show: false },
        })),
      ],
    };

    const data = [props.x, ...props.series.map((s) => s.values)] as unknown as uPlot.AlignedData;
    plot.current = new uPlot(opts, data, el);

    const ro = new ResizeObserver(() => {
      plot.current?.setSize({ width: el.clientWidth, height: props.height ?? 220 });
    });
    ro.observe(el);

    return () => { ro.disconnect(); plot.current?.destroy(); plot.current = null; };
    // Rebuild only when the series *shape* changes; data updates below.
  }, [cfgKey, props.height, props.yLabel, props.rightLabel]);

  useEffect(() => {
    if (!plot.current) return;
    plot.current.setData([props.x, ...props.series.map((s) => s.values)] as unknown as uPlot.AlignedData);
  }, [props.x, props.series]);

  return <div className="chart" ref={ref} />;
}

export const COLORS = {
  blue: '#3d71d9', green: '#73bf69', yellow: '#f2cc0c', red: '#f2495c',
  orange: '#ff9830', purple: '#b877d9', cyan: '#5ac8fa', pink: '#f56ec1',
};

export const SERIES_PALETTE = [
  COLORS.blue, COLORS.green, COLORS.yellow, COLORS.orange,
  COLORS.purple, COLORS.cyan, COLORS.red, COLORS.pink,
];
