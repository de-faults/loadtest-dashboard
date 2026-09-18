import * as store from "../store/db.ts";
import { analyzeCapacity } from "../shared/capacity.ts";
import type { CapacityReport } from "../shared/capacity.ts";
import type {
  AppSettings,
  RunSummary,
  WindowMetrics,
} from "../shared/types.ts";

/**
 * CSV report generation.
 *
 * Three details that matter and are easy to get wrong:
 *  - UTF-8 BOM, or Excel renders Thai as mojibake.
 *  - Formula-injection guard: error text comes from the system under test.
 *  - Stable internal column keys; only the *header labels* are localized.
 */

const BOM = "﻿";

export const SUMMARY_COLUMNS = [
  "run_id",
  "profile",
  "protocol",
  "target",
  "started_at",
  "ended_at",
  "duration_s",
  "total_requests",
  "success",
  "failed",
  "success_rate_pct",
  "rps_avg",
  "rps_peak",
  "tps_avg",
  "tps_peak",
  "vus_max",
  "lat_min_ms",
  "lat_avg_ms",
  "lat_p90_ms",
  "lat_p95_ms",
  "lat_p99_ms",
  "lat_max_ms",
  "checks_passed",
  "checks_failed",
  "thresholds_passed",
  "thresholds_failed",
  "verdict",
] as const;

export const TIMESERIES_COLUMNS = [
  "ts",
  "elapsed_s",
  "requests",
  "success",
  "failed",
  "rps",
  "tps",
  "vus",
  "p90_ms",
  "p95_ms",
  "p99_ms",
  "max_ms",
  "consumer_lag",
  "msg_lag_count",
  "msg_lag_min_ms",
  "msg_lag_avg_ms",
  "msg_lag_p50_ms",
  "msg_lag_p95_ms",
  "msg_lag_p99_ms",
  "msg_lag_max_ms",
] as const;

const HEADERS_TH: Record<string, string> = {
  run_id: "รหัสการทดสอบ",
  profile: "โปรไฟล์",
  protocol: "โปรโตคอล",
  target: "เป้าหมาย",
  started_at: "เริ่มเมื่อ",
  ended_at: "สิ้นสุดเมื่อ",
  duration_s: "ระยะเวลา (วินาที)",
  total_requests: "จำนวนคำขอทั้งหมด",
  success: "สำเร็จ",
  failed: "ล้มเหลว",
  success_rate_pct: "อัตราสำเร็จ (%)",
  rps_avg: "RPS เฉลี่ย",
  rps_peak: "RPS สูงสุด",
  tps_avg: "TPS เฉลี่ย",
  tps_peak: "TPS สูงสุด",
  vus_max: "ผู้ใช้เสมือนสูงสุด",
  lat_min_ms: "หน่วงต่ำสุด (ms)",
  lat_avg_ms: "หน่วงเฉลี่ย (ms)",
  lat_p90_ms: "หน่วง p90 (ms)",
  lat_p95_ms: "หน่วง p95 (ms)",
  lat_p99_ms: "หน่วง p99 (ms)",
  lat_max_ms: "หน่วงสูงสุด (ms)",
  checks_passed: "ตรวจผ่าน",
  checks_failed: "ตรวจไม่ผ่าน",
  thresholds_passed: "เกณฑ์ผ่าน",
  thresholds_failed: "เกณฑ์ไม่ผ่าน",
  verdict: "ผลลัพธ์",
  ts: "เวลา",
  elapsed_s: "เวลาที่ผ่านไป (วินาที)",
  requests: "คำขอ",
  rps: "RPS",
  tps: "TPS",
  vus: "ผู้ใช้เสมือน",
  p90_ms: "p90 (ms)",
  p95_ms: "p95 (ms)",
  p99_ms: "p99 (ms)",
  max_ms: "สูงสุด (ms)",
  consumer_lag: "ความล่าช้าของคอนซูมเมอร์",
  name: "ชื่อ",
  passed: "ผ่าน",
  pass_rate_pct: "อัตราผ่าน (%)",
  expr: "เงื่อนไข",
  metric: "ตัววัด",
  actual: "ค่าที่วัดได้",
  kind: "ประเภท",
  count: "จำนวน",
  sample: "ตัวอย่างข้อความ",
  body: "เนื้อหาที่ตอบกลับ",
  content_type: "ชนิดเนื้อหา",
  chars: "จำนวนอักขระ",
  response_headers: "เฮดเดอร์ที่ตอบกลับ",
  answered_by: "ผู้ตอบกลับ",
  origin: "แหล่งที่มาของ error",
  origin_evidence: "หลักฐาน",
  answered_from: "ที่อยู่ที่ตอบกลับ",
  trace_ids: "รหัสอ้างอิง",
  truncated: "ตัดข้อความ",
  type: "ประเภท",
  key: "รายการ",
  value: "ค่า",
  scenario: "ซีนาริโอ",
  vusers: "ผู้ใช้เสมือน",
  share_pct: "สัดส่วน (%)",
  step: "ขั้นที่",
  hold_s: "ระยะเวลาที่วัด (วินาที)",
  start_s: "เริ่มที่วินาที",
  end_s: "จบที่วินาที",
  error_rate_pct: "อัตราผิดพลาด (%)",
  rps_peak_step: "RPS สูงสุดในขั้น",
  avg_ms: "หน่วงเฉลี่ย (ms)",
  p95_worst_ms: "p95 แย่ที่สุด (ms)",
  within_slo: "อยู่ในเกณฑ์",
  breaches: "เกณฑ์ที่หลุด",
  marker: "จุดสำคัญ",
  tps_peak_step: "TPS สูงสุดในขั้น",
  gates_step: "ใช้ตัดสินขั้นนี้",
  start_at: "เริ่มวัดเมื่อ",
  end_at: "หยุดวัดเมื่อ",
  clock: "ช่วงเวลา",
  section: "หมวด",
  group: "คอนซูมเมอร์กรุ๊ป",
  topic: "หัวข้อ",
  partition: "พาร์ทิชัน",
  max_lag: "ความล่าช้าสูงสุด",
  final_lag: "ความล่าช้าสุดท้าย",
  msg_lag_count: "ข้อความที่ถูกคอนซูม",
  msg_lag_min_ms: "lag ต่อข้อความ ต่ำสุด (ms)",
  msg_lag_avg_ms: "lag ต่อข้อความ เฉลี่ย (ms)",
  msg_lag_p50_ms: "lag ต่อข้อความ p50 (ms)",
  msg_lag_p95_ms: "lag ต่อข้อความ p95 (ms)",
  msg_lag_p99_ms: "lag ต่อข้อความ p99 (ms)",
  msg_lag_max_ms: "lag ต่อข้อความ สูงสุด (ms)",
  consumed: "คอนซูมแล้ว",
  pending: "ค้างอยู่",
};

function header(key: string, lang: "en" | "th"): string {
  return lang === "th" ? (HEADERS_TH[key] ?? key) : key;
}

/**
 * Excel and LibreOffice execute a leading =, +, -, @ (and tab/CR) as a formula.
 * Error strings originate from the target system, so they are attacker-shaped.
 */
function guard(value: string): string {
  return /^[=+\-@\t\r]/.test(value) ? `'${value}` : value;
}

function cell(v: unknown, delim: string): string {
  if (v == null) return "";
  const s = guard(String(v));
  return s.includes(delim) ||
    s.includes('"') ||
    s.includes("\n") ||
    s.includes("\r")
    ? `"${s.replaceAll('"', '""')}"`
    : s;
}

function row(values: unknown[], delim: string): string {
  return values.map((v) => cell(v, delim)).join(delim) + "\r\n";
}

function iso(ms: number | null | undefined): string {
  return ms ? new Date(ms).toISOString() : "";
}

export interface CsvOptions {
  delimiter: string;
  language: "en" | "th";
}

export function optionsFrom(
  settings: AppSettings,
  override?: Partial<CsvOptions>,
): CsvOptions {
  return {
    delimiter: override?.delimiter ?? settings.csvDelimiter,
    language: override?.language ?? settings.csvLanguage,
  };
}

export function summaryCsv(summaries: RunSummary[], o: CsvOptions): string {
  let out =
    BOM +
    row(
      SUMMARY_COLUMNS.map((c) => header(c, o.language)),
      o.delimiter,
    );
  for (const s of summaries) {
    const checksPassed = s.checks.reduce((a, c) => a + c.passed, 0);
    const checksFailed = s.checks.reduce((a, c) => a + c.failed, 0);
    out += row(
      [
        s.runId,
        s.profileName,
        s.protocol,
        s.target,
        iso(s.startedAt),
        iso(s.endedAt),
        round(s.durationMs / 1000),
        s.totalRequests,
        s.totalSuccess,
        s.totalFailed,
        s.successRatePct,
        s.rpsAvg,
        s.rpsPeak,
        s.tpsAvg,
        s.tpsPeak,
        s.vusMax,
        s.latency.min,
        s.latency.avg,
        s.latency.p90,
        s.latency.p95,
        s.latency.p99,
        s.latency.max,
        checksPassed,
        checksFailed,
        s.thresholds.filter((t) => t.passed).length,
        s.thresholds.filter((t) => !t.passed).length,
        s.verdict,
      ],
      o.delimiter,
    );
  }
  return out;
}

export function timeseriesCsv(samples: WindowMetrics[], o: CsvOptions): string {
  let out =
    BOM +
    row(
      TIMESERIES_COLUMNS.map((c) => header(c, o.language)),
      o.delimiter,
    );
  for (const w of samples) {
    out += row(
      [
        iso(w.ts),
        w.elapsed,
        w.requests,
        w.success,
        w.failed,
        w.rps,
        w.tps,
        w.vus,
        w.latency.p90,
        w.latency.p95,
        w.latency.p99,
        w.latency.max,
        w.consumerLag ?? "",
        w.messageLag?.count ?? "",
        w.messageLag?.min ?? "",
        w.messageLag?.avg ?? "",
        w.messageLag?.p50 ?? "",
        w.messageLag?.p95 ?? "",
        w.messageLag?.p99 ?? "",
        w.messageLag?.max ?? "",
      ],
      o.delimiter,
    );
  }
  return out;
}

export function checksCsv(s: RunSummary, o: CsvOptions): string {
  let out =
    BOM +
    row(
      ["run_id", "name", "passed", "failed", "pass_rate_pct"].map((c) =>
        header(c, o.language),
      ),
      o.delimiter,
    );
  for (const c of s.checks)
    out += row(
      [s.runId, c.name, c.passed, c.failed, c.passRatePct],
      o.delimiter,
    );
  return out;
}

export function thresholdsCsv(s: RunSummary, o: CsvOptions): string {
  let out =
    BOM +
    row(
      ["run_id", "expr", "metric", "actual", "passed"].map((c) =>
        header(c, o.language),
      ),
      o.delimiter,
    );
  for (const t of s.thresholds)
    out += row(
      [
        s.runId,
        t.expr,
        t.metric,
        Number.isNaN(t.actual) ? "" : t.actual,
        t.passed,
      ],
      o.delimiter,
    );
  return out;
}

export function errorsCsv(s: RunSummary, o: CsvOptions): string {
  let out =
    BOM +
    row(
      [
        "run_id",
        "kind",
        "count",
        "sample",
        "body",
        "content_type",
        "chars",
        "truncated",
        "response_headers",
        "origin",
        "answered_by",
        "answered_from",
        "origin_evidence",
        "trace_ids",
      ].map((c) => header(c, o.language)),
      o.delimiter,
    );
  for (const e of s.errors)
    out += row(
      [
        s.runId,
        e.kind,
        e.count,
        e.sample,
        e.body ?? "",
        e.bodyContentType ?? "",
        e.bodyChars ?? "",
        e.body == null ? "" : Boolean(e.bodyTruncated),
        Object.entries(e.responseHeaders ?? {})
          .map(([k, v]) => `${k}: ${v}`)
          .join("\n"),
        e.origin?.verdict ?? "",
        e.origin?.by ?? "",
        e.origin?.remoteIp
          ? `${e.origin.remoteIp}${e.origin.remotePort ? `:${e.origin.remotePort}` : ""}`
          : "",
        (e.origin?.evidence ?? []).join("\n"),
        Object.entries(e.origin?.traceIds ?? {})
          .map(([k, v]) => `${k}: ${v}`)
          .join("\n"),
      ],
      o.delimiter,
    );
  return out;
}

/** One row per (metric, reported value key) — the value set differs by metric type (Counter/Rate/Gauge/Trend). */
export function customMetricsCsv(s: RunSummary, o: CsvOptions): string {
  let out =
    BOM +
    row(
      ["run_id", "name", "type", "key", "value"].map((c) =>
        header(c, o.language),
      ),
      o.delimiter,
    );
  for (const m of s.customMetrics) {
    for (const [key, value] of Object.entries(m.values))
      out += row([s.runId, m.name, m.type, key, value], o.delimiter);
  }
  return out;
}

/**
 * How the load split across named scenarios. Empty for runners that report no
 * split, and for runs recorded before the split was captured.
 */
export function scenariosCsv(s: RunSummary, o: CsvOptions): string {
  let out =
    BOM +
    row(
      [
        "run_id",
        "scenario",
        "vusers",
        "requests",
        "success_rate_pct",
        "p95_ms",
        "share_pct",
      ].map((c) => header(c, o.language)),
      o.delimiter,
    );
  // A column the runner does not report stays empty rather than reading as a
  // zero the tool never measured.
  for (const sc of s.scenarios ?? [])
    out += row(
      [
        s.runId,
        sc.name,
        sc.vusers ?? "",
        sc.requests ?? "",
        sc.successRatePct ?? "",
        sc.p95 ?? "",
        sc.sharePct,
      ],
      o.delimiter,
    );
  return out;
}

export const CAPACITY_COLUMNS = [
  "run_id",
  "step",
  "vus",
  "start_s",
  "end_s",
  "start_at",
  "end_at",
  "hold_s",
  "requests",
  "failed",
  "error_rate_pct",
  "rps",
  "rps_peak_step",
  "tps",
  "tps_peak_step",
  "min_ms",
  "avg_ms",
  "p90_ms",
  "p95_ms",
  "p99_ms",
  "p95_worst_ms",
  "max_ms",
  "within_slo",
  "breaches",
  "marker",
] as const;

/**
 * The capacity curve, one row per held load step, plus the conclusion drawn
 * from it as trailing `# capacity_result` rows — the numbers a follow-up
 * performance test is dimensioned from.
 */
export function capacityCsv(
  runId: string,
  report: CapacityReport,
  o: CsvOptions,
): string {
  let out =
    BOM +
    row(
      CAPACITY_COLUMNS.map((c) => header(c, o.language)),
      o.delimiter,
    );
  for (const st of report.steps) {
    const marker = [
      report.knee?.index === st.index ? "knee" : "",
      report.saturation?.index === st.index ? "saturation" : "",
      report.breaking?.index === st.index ? "breaking" : "",
    ]
      .filter(Boolean)
      .join("+");
    out += row(
      [
        runId,
        st.index,
        st.vus,
        st.startElapsed,
        st.endElapsed,
        iso(st.startTs),
        iso(st.endTs),
        st.durationSec,
        st.requests,
        st.failed,
        st.errorRatePct,
        st.rps,
        st.rpsPeak,
        st.tps,
        st.tpsPeak,
        st.latencyMin,
        st.latencyAvg,
        st.p90,
        st.p95,
        st.p99,
        st.p95Worst,
        st.max,
        st.passed,
        st.breaches.join("; "),
        marker,
      ],
      o.delimiter,
    );
  }
  // Every threshold measured against every step: which level the profile's own
  // pass/fail rules first stopped being met at is the whole point of the ramp.
  if (report.steps.some((st) => st.thresholds.length)) {
    out +=
      "\r\n# capacity_thresholds\r\n" +
      row(
        ["step", "vus", "expr", "metric", "actual", "passed", "gates_step"].map(
          (c) => header(c, o.language),
        ),
        o.delimiter,
      );
    for (const st of report.steps)
      for (const th of st.thresholds)
        out += row(
          [
            st.index,
            st.vus,
            th.expr,
            th.metric,
            Number.isNaN(th.actual) ? "" : th.actual,
            th.passed,
            th.gates,
          ],
          o.delimiter,
        );
  }
  if (report.recommended) {
    const r = report.recommended;
    out +=
      "\r\n# capacity_result\r\n" +
      row(["key", "value"].map((c) => header(c, o.language)), o.delimiter) +
      row(["capacity_vus", r.vus], o.delimiter) +
      row(["capacity_rps", r.rps], o.delimiter) +
      row(["recommended_vus", r.safeVus], o.delimiter) +
      row(["recommended_rps", r.safeRps], o.delimiter) +
      row(["limited_by", r.limitedBy], o.delimiter);
  }
  return out;
}

/**
 * The capacity report for a stored run, or null when the run never held more
 * than one load level — a flat run has no curve to read.
 */
export function capacityReportFor(runId: string): CapacityReport | null {
  const run = store.getRun(runId);
  if (!run) return null;
  return analyzeCapacity(store.getSamples(runId), {
    thresholds: run.config.thresholds,
  });
}

/**
 * Kafka volume and consumer-lag rollup as `section,key,value` rows, then the
 * per-partition peaks. Empty (header only) for any other protocol.
 */
export function kafkaCsv(s: RunSummary, o: CsvOptions): string {
  let out =
    BOM +
    row(
      ["run_id", "section", "key", "value"].map((c) => header(c, o.language)),
      o.delimiter,
    );
  const k = s.kafka;
  if (!k) return out;
  for (const [key, value] of Object.entries(k.volume))
    out += row([s.runId, "volume", key, value], o.delimiter);
  const lag = k.lag;
  if (!lag) return out;
  const flat: Array<[string, unknown]> = [
    ["groups", lag.groups.join(" ")],
    ["samples", lag.samples],
    ["start", lag.start],
    ["max", lag.max],
    ["max_at_s", lag.maxAtSec],
    ["min", lag.min],
    ["avg", lag.avg],
    ["median", lag.median],
    ["end_of_load", lag.endOfLoad],
    ["final", lag.final],
    ["growth_per_s", lag.growthPerSec],
    ["drain_timeout_s", lag.drain?.timeoutSec ?? ""],
    ["drained", lag.drain ? lag.drain.drained : ""],
    ["drain_s", lag.drain?.seconds ?? ""],
  ];
  for (const [key, value] of flat)
    out += row([s.runId, "lag", key, value], o.delimiter);

  out +=
    "\r\n" +
    row(
      ["run_id", "group", "topic", "partition", "max_lag", "final_lag"].map(
        (c) => header(c, o.language),
      ),
      o.delimiter,
    );
  for (const p of lag.partitions)
    out += row(
      [s.runId, p.group, p.topic, p.partition, p.maxLag, p.finalLag],
      o.delimiter,
    );

  if (k.messageLag?.length) {
    out +=
      "\r\n" +
      row(
        ["run_id", "group", "consumed", "pending", "msg_lag_min_ms", "msg_lag_avg_ms",
          "msg_lag_p50_ms", "msg_lag_p95_ms", "msg_lag_p99_ms", "msg_lag_max_ms"].map(
          (c) => header(c, o.language),
        ),
        o.delimiter,
      );
    for (const m of k.messageLag)
      out += row(
        [s.runId, m.group, m.consumed, m.pending, m.min, m.avg, m.p50, m.p95, m.p99, m.max],
        o.delimiter,
      );
  }
  return out;
}

/** Everything for one run in a single file, as `# section` blocks. */
export function fullReportCsv(runId: string, o: CsvOptions): string | null {
  const run = store.getRun(runId);
  if (!run?.summary) return null;
  const s = run.summary;
  const parts = [
    "# summary\r\n" + summaryCsv([s], o).slice(BOM.length),
    "# thresholds\r\n" + thresholdsCsv(s, o).slice(BOM.length),
    "# checks\r\n" + checksCsv(s, o).slice(BOM.length),
    "# errors\r\n" + errorsCsv(s, o).slice(BOM.length),
    "# custom_metrics\r\n" + customMetricsCsv(s, o).slice(BOM.length),
    "# scenarios\r\n" + scenariosCsv(s, o).slice(BOM.length),
    ...(s.kafka ? ["# kafka\r\n" + kafkaCsv(s, o).slice(BOM.length)] : []),
    "# timeseries\r\n" +
      timeseriesCsv(store.getSamples(runId), o).slice(BOM.length),
  ];
  const capacity = capacityReportFor(runId);
  if (capacity)
    parts.splice(
      6,
      0,
      "# capacity\r\n" + capacityCsv(runId, capacity, o).slice(BOM.length),
    );
  return BOM + parts.join("\r\n");
}

function round(n: number): number {
  return Math.round(n * 100) / 100;
}
