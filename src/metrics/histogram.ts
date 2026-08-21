/**
 * Log-linear latency histogram (HdrHistogram-style).
 *
 * Bounded memory regardless of request count, ~1% relative error, and
 * mergeable — which is the property that matters: whole-run percentiles come
 * from merging buckets, never from averaging per-window percentiles.
 *
 * Layout: values are bucketed by exponent (power of two) and a linear
 * sub-bucket index inside it. SUB_BITS=7 → 128 sub-buckets per octave.
 */

const SUB_BITS = 7;
const SUB_COUNT = 1 << SUB_BITS; // 128
const OCTAVES = 40; // covers up to ~2^40 units

function bucketIndex(value: number): number {
  const v = Math.floor(value);
  if (v < SUB_COUNT) return v;
  // v lands in [2^exp, 2^(exp+1)); scale it so the sub-bucket index is in [128, 256).
  const exp = 31 - Math.clz32(v);
  const scale = Math.pow(2, exp - SUB_BITS);
  const sub = Math.floor(v / scale);
  return SUB_COUNT + (exp - SUB_BITS) * SUB_COUNT + (sub - SUB_COUNT);
}

/** Inverse of bucketIndex — midpoint of the bucket's value range. */
function indexToValue(index: number): number {
  if (index < SUB_COUNT) return index;
  const rel = index - SUB_COUNT;
  const octave = rel >> SUB_BITS;
  const sub = (rel & (SUB_COUNT - 1)) + SUB_COUNT;
  const scale = Math.pow(2, octave);
  return sub * scale + scale / 2;
}

export class Histogram {
  private counts = new Int32Array(SUB_COUNT + OCTAVES * SUB_COUNT);
  private _count = 0;
  private _sum = 0;
  private _min = Number.POSITIVE_INFINITY;
  private _max = 0;

  /** value in milliseconds (fractional ok — scaled to microseconds internally). */
  record(valueMs: number): void {
    if (!Number.isFinite(valueMs) || valueMs < 0) return;
    const micros = Math.round(valueMs * 1000);
    const idx = bucketIndex(micros);
    if (idx >= this.counts.length) return;
    this.counts[idx]++;
    this._count++;
    this._sum += valueMs;
    if (valueMs < this._min) this._min = valueMs;
    if (valueMs > this._max) this._max = valueMs;
  }

  /**
   * Record `weight` observations of the same value.
   *
   * Needed for runners that report pre-aggregated quantiles instead of raw
   * samples (Artillery), where the distribution is reconstructed piecewise.
   */
  recordWeighted(valueMs: number, weight: number): void {
    if (!Number.isFinite(valueMs) || valueMs < 0 || weight <= 0) return;
    const idx = bucketIndex(Math.round(valueMs * 1000));
    if (idx >= this.counts.length) return;
    this.counts[idx] += weight;
    this._count += weight;
    this._sum += valueMs * weight;
    if (valueMs < this._min) this._min = valueMs;
    if (valueMs > this._max) this._max = valueMs;
  }

  get count(): number { return this._count; }

  merge(other: Histogram): void {
    for (let i = 0; i < this.counts.length; i++) this.counts[i] += other.counts[i];
    this._count += other._count;
    this._sum += other._sum;
    if (other._min < this._min) this._min = other._min;
    if (other._max > this._max) this._max = other._max;
  }

  percentile(p: number): number {
    if (this._count === 0) return 0;
    const rank = Math.ceil((p / 100) * this._count);
    let seen = 0;
    for (let i = 0; i < this.counts.length; i++) {
      seen += this.counts[i];
      if (seen >= rank) return indexToValue(i) / 1000;
    }
    return this._max;
  }

  profile(): { min: number; avg: number; p90: number; p95: number; p99: number; max: number } {
    if (this._count === 0) return { min: 0, avg: 0, p90: 0, p95: 0, p99: 0, max: 0 };
    return {
      min: round3(this._min),
      avg: round3(this._sum / this._count),
      // Clamp to observed max: bucket midpoints can overshoot the true maximum.
      p90: round3(Math.min(this.percentile(90), this._max)),
      p95: round3(Math.min(this.percentile(95), this._max)),
      p99: round3(Math.min(this.percentile(99), this._max)),
      max: round3(this._max),
    };
  }

  /** Non-empty buckets as [valueMs, count] — for the distribution chart. */
  buckets(): Array<[number, number]> {
    const out: Array<[number, number]> = [];
    for (let i = 0; i < this.counts.length; i++) {
      if (this.counts[i] > 0) out.push([round3(indexToValue(i) / 1000), this.counts[i]]);
    }
    return out;
  }

  /** Compact serialization: sparse index:count pairs + scalars. */
  serialize(): string {
    const pairs: number[] = [];
    for (let i = 0; i < this.counts.length; i++) {
      if (this.counts[i] > 0) { pairs.push(i, this.counts[i]); }
    }
    return JSON.stringify({ c: this._count, s: this._sum, mn: this._count ? this._min : 0, mx: this._max, p: pairs });
  }

  static deserialize(blob: string): Histogram {
    const h = new Histogram();
    try {
      const o = JSON.parse(blob) as { c: number; s: number; mn: number; mx: number; p: number[] };
      h._count = o.c; h._sum = o.s; h._min = o.c ? o.mn : Number.POSITIVE_INFINITY; h._max = o.mx;
      for (let i = 0; i < o.p.length; i += 2) h.counts[o.p[i]] = o.p[i + 1];
    } catch { /* corrupt blob → empty histogram, run detail still renders */ }
    return h;
  }
}

function round3(n: number): number { return Math.round(n * 1000) / 1000; }
