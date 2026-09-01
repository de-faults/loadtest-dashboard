import * as store from "../store/db.ts";
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
    "# timeseries\r\n" +
      timeseriesCsv(store.getSamples(runId), o).slice(BOM.length),
  ];
  return BOM + parts.join("\r\n");
}

function round(n: number): number {
  return Math.round(n * 100) / 100;
}
