import { spawn } from "node:child_process";
import { mkdtemp, readdir, rm, writeFile, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { getSettings } from "../store/db.ts";
import { toK6Thresholds } from "../metrics/thresholds.ts";
import { tailLines } from "./tail.ts";
import { materializeScript, scriptEnv, usesCustomScript } from "./script.ts";
import type { Runner, RunnerContext, RunnerResult } from "./types.ts";
import type { RestConfig } from "../shared/types.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const SCRIPT = join(HERE, "assets", "k6-script.js");

/** k6 exits 99 when a threshold was crossed. */
const K6_THRESHOLD_EXIT = 99;

interface K6Point {
  type: string;
  metric: string;
  data?: { time: string; value: number; tags?: Record<string, string> };
}

export const k6Runner: Runner = {
  protocol: "rest",

  async available() {
    const bin = getSettings().k6Path;
    return probe(bin, ["version"]);
  },

  async run(ctx: RunnerContext): Promise<RunnerResult> {
    const cfg = ctx.config.rest;
    if (!cfg) throw new Error("rest config missing");

    const dir = await mkdtemp(join(tmpdir(), "ltd-k6-"));
    const cfgPath = join(dir, "config.json");
    const summaryPath = join(dir, "summary.json");
    const outPath = join(dir, "metrics.ndjson");

    const { thresholds } = toK6Thresholds(ctx.config.thresholds);
    await writeFile(
      cfgPath,
      JSON.stringify(buildScriptConfig(cfg, ctx, thresholds)),
      "utf8",
    );

    // A custom script owns its own options and thresholds; the config file is
    // still handed over via __ENV.CFG in case the script wants to read it.
    const env = scriptEnv(ctx.config.script);

    let scriptPath = SCRIPT;
    if (usesCustomScript(ctx.config.script)) {
      scriptPath = await materializeScript(ctx.config.script, dir, "script.js");
      ctx.log("info", `custom k6 script: ${scriptPath}`);
      // A `path` script carries no content in the profile, so lint what is
      // actually about to run rather than an empty string.
      const text =
        ctx.config.script.mode === "path"
          ? await readFile(scriptPath, "utf8").catch(() => "")
          : ctx.config.script.content;
      for (const tip of suggestK6Script(text, Object.keys(env))) {
        ctx.log(tip.level, `suggestion: ${tip.message}`);
      }
    }

    const bin = getSettings().k6Path;
    // argv array + shell:false — user input never reaches a shell.
    const args = [
      "run",
      "--out",
      `json=${outPath}`,
      "--quiet",
      "--no-color",
      "--env",
      `CFG=${cfgPath}`,
      "--env",
      `SUMMARY_OUT=${summaryPath}`,
      ...Object.entries(env).flatMap(([k, v]) => ["--env", `${k}=${v}`]),
      scriptPath,
    ];
    // A script variable can hold a token, so the command line is logged with
    // the user's own values masked — names are enough to debug a run.
    ctx.log("info", `k6 ${maskEnvValues(args, new Set(Object.keys(env))).join(" ")}`);

    const child = spawn(bin, args, {
      shell: false,
      stdio: ["ignore", "pipe", "pipe"],
      // k6 resolves the file outputs of handleSummary() against the working
      // directory. Running from the run's own temp dir keeps a custom script's
      // summary where readK6Summary can still find it, instead of dropping it
      // into whatever directory the dashboard was started from.
      cwd: dir,
    });
    let exited = false;
    let exitCode: number | null = null;

    const onAbort = () => {
      child.kill("SIGINT");
    };
    ctx.signal.addEventListener("abort", onAbort, { once: true });

    child.stdout.on("data", (b: Buffer) =>
      lines(b).forEach((l) => ctx.log("info", l)),
    );
    child.stderr.on("data", (b: Buffer) =>
      lines(b).forEach((l) => ctx.log("warn", l)),
    );

    const exitPromise = new Promise<number | null>((resolve) => {
      child.on("close", (code) => {
        exited = true;
        exitCode = code;
        resolve(code);
      });
      child.on("error", (err) => {
        exited = true;
        ctx.error("spawn", `${bin}: ${err.message}`);
        resolve(null);
      });
    });

    const tailPromise = tailLines(outPath, (line) => consumePoint(line, ctx), {
      done: () => exited,
    });

    await exitPromise;
    await tailPromise;
    ctx.signal.removeEventListener("abort", onAbort);

    const result: RunnerResult = {
      nativeVerdict:
        exitCode === 0
          ? "pass"
          : exitCode === K6_THRESHOLD_EXIT
            ? "fail"
            : undefined,
    };

    const summary = await readK6Summary(dir, summaryPath, ctx);
    if (summary) applyNativeSummary(summary, ctx, result);
    else ctx.log("warn", "k6 summary not produced — live metrics only");

    await rm(dir, { recursive: true, force: true });
    return result;
  },
};

/** Variable names whose value is a credential often enough to never log it. */
const SECRET_ENV_NAME = /TOKEN|SECRET|PASS|KEY|AUTH|CREDENTIAL|COOKIE/i;

/**
 * The command line as logged. A script variable holding a target URL or a rate
 * is the most useful thing in the log; one holding a credential must never
 * reach it, and the run log is persisted and exported.
 */
function maskEnvValues(args: string[], names: Set<string>): string[] {
  return args.map((arg, i) => {
    if (i === 0 || args[i - 1] !== "--env") return arg;
    const eq = arg.indexOf("=");
    if (eq <= 0) return arg;
    const name = arg.slice(0, eq);
    return names.has(name) && SECRET_ENV_NAME.test(name)
      ? `${name}=***`
      : arg;
  });
}

/**
 * k6's end-of-run summary.
 *
 * The built-in script — and any custom one that follows the suggestion — writes
 * it to `SUMMARY_OUT`. Scripts written for a CLI run instead carry their own
 * `handleSummary()` returning a fixed filename; those land in the run's temp
 * directory (see `cwd` above), so the thresholds and custom metrics of a
 * bring-your-own script are still recovered rather than lost.
 */
async function readK6Summary(
  dir: string,
  summaryPath: string,
  ctx: RunnerContext,
): Promise<K6Summary | null> {
  try {
    return JSON.parse(await readFile(summaryPath, "utf8")) as K6Summary;
  } catch {
    // Not written — fall back to whatever the script produced on its own.
  }

  let names: string[];
  try {
    names = await readdir(dir);
  } catch {
    return null;
  }
  for (const name of names) {
    // config.json is ours; metrics.ndjson is the raw point stream.
    if (!name.endsWith(".json") || name === "config.json") continue;
    try {
      const parsed = JSON.parse(
        await readFile(join(dir, name), "utf8"),
      ) as K6Summary;
      if (parsed && typeof parsed.metrics === "object" && parsed.metrics) {
        ctx.log("info", `summary read from the script's own output: ${name}`);
        return parsed;
      }
    } catch {
      // Not JSON, or not a summary — keep looking.
    }
  }
  return null;
}

function lines(b: Buffer): string[] {
  return b
    .toString("utf8")
    .split("\n")
    .map((s) => s.trimEnd())
    .filter(Boolean);
}

function consumePoint(line: string, ctx: RunnerContext): void {
  let p: K6Point;
  try {
    p = JSON.parse(line) as K6Point;
  } catch {
    return;
  }
  if (p.type !== "Point" || !p.data) return;
  const { value, tags } = p.data;
  const ts = Date.parse(p.data.time) || Date.now();

  switch (p.metric) {
    case "http_req_duration": {
      // k6 tags each duration point with expected_response, so one point
      // carries both the latency and the pass/fail verdict. Works the same
      // for any custom script — it's set by k6's http module, not user code.
      const ok = tags?.expected_response !== "false";
      ctx.agg.record({ ts, latencyMs: value, ok });
      // k6 tags every sample with the scenario that issued it, so unlike
      // artillery the split carries the scenario's own latency, not a headcount.
      if (tags?.scenario) ctx.agg.recordScenarioSample(tags.scenario, value, ok);
      if (!ok) {
        const kind = tags?.error_code
          ? `http_${tags.error_code}`
          : `status_${tags?.status ?? "unknown"}`;
        ctx.error(
          kind,
          tags?.error || `unexpected response status ${tags?.status ?? "?"}`,
        );
      }
      break;
    }
    case "iterations":
      // One iteration = one transaction. A scenario issuing several requests
      // per iteration makes TPS meaningfully lower than RPS.
      ctx.agg.recordTransaction(value || 1);
      break;
    case "vus":
      ctx.agg.setVus(Math.round(value));
      break;
    case "checks": {
      const name = tags?.check ?? "check";
      if (value === 1) ctx.agg.addCheck(name, 1, 0);
      else ctx.agg.addCheck(name, 0, 1);
      break;
    }
    case "dropped_iterations":
      // Only emitted by arrival-rate executors when preAllocated/maxVUs ran out —
      // a silent capacity problem that otherwise never surfaces mid-run.
      if (value > 0) {
        ctx.error(
          "dropped_iterations",
          `k6 dropped ${value} iteration(s): arrival rate exceeded available VUs — raise preAllocatedVUs/maxVUs`,
        );
      }
      break;
    // Every other metric (http_reqs, http_req_failed, http_req_* timing
    // breakdown, data_sent/received, vus_max, and any custom Counter/Gauge/
    // Rate/Trend a bring-your-own script defines) is still fully captured —
    // just from k6's own end-of-run summary (applyNativeSummary below)
    // rather than the live stream, since that's where k6 reports their
    // final values/thresholds anyway.
    default:
      break;
  }
}

interface K6Summary {
  metrics?: Record<
    string,
    {
      type?: string;
      values?: Record<string, number>;
      thresholds?: Record<string, { ok: boolean }>;
    }
  >;
}

/** Built-in k6 metrics, already surfaced through dedicated fields elsewhere. */
const CORE_K6_METRICS = new Set([
  "http_reqs",
  "http_req_duration",
  "http_req_failed",
  "http_req_blocked",
  "http_req_connecting",
  "http_req_tls_handshaking",
  "http_req_sending",
  "http_req_waiting",
  "http_req_receiving",
  "iterations",
  "iteration_duration",
  "vus",
  "vus_max",
  "checks",
  "data_sent",
  "data_received",
  "dropped_iterations",
]);

/** k6's own summary is authoritative for thresholds it evaluated. */
function applyNativeSummary(
  sum: K6Summary,
  ctx: RunnerContext,
  result: RunnerResult,
): void {
  const native: RunnerResult["nativeThresholds"] = [];
  const custom: RunnerResult["nativeCustomMetrics"] = [];
  for (const [metric, m] of Object.entries(sum.metrics ?? {})) {
    for (const [expr, r] of Object.entries(m.thresholds ?? {})) {
      native.push({
        expr: `${metric}: ${expr}`,
        metric,
        actual: NaN,
        passed: r.ok,
      });
    }
    // Anything outside the built-in set is a custom Counter/Gauge/Rate/Trend
    // from a bring-your-own script — pass its reported values through as-is
    // rather than guessing a fixed shape per metric type. k6 names a submetric
    // `parent{tag:value}`, so the check is on the parent: a slice of
    // http_req_duration is not a custom metric, but one of a custom trend is.
    const base = metric.includes("{")
      ? metric.slice(0, metric.indexOf("{"))
      : metric;
    if (!CORE_K6_METRICS.has(base) && m.values) {
      const values: Record<string, number> = {};
      for (const [k, v] of Object.entries(m.values))
        if (typeof v === "number") values[k] = v;
      if (Object.keys(values).length)
        custom.push({ name: metric, type: m.type ?? "unknown", values });
    }
  }
  if (native.length) result.nativeThresholds = native;
  if (custom.length) result.nativeCustomMetrics = custom;
  const reqs = sum.metrics?.http_reqs?.values?.count;
  if (typeof reqs === "number")
    ctx.log("info", `k6 reported ${reqs} http requests`);
}

interface ScriptTip {
  level: "info" | "warn";
  message: string;
}

/**
 * Heuristic lint for bring-your-own scripts. The dashboard only ever sees what
 * k6 chooses to report, so gaps that make it *look* broken (empty summary,
 * silently-dropped iterations, un-thresholded custom metrics) are worth
 * flagging up front rather than discovered after a long run.
 */
export function suggestK6Script(
  content: string,
  envNames: string[] = [],
): ScriptTip[] {
  const tips: ScriptTip[] = [];

  // A script parameterised through __ENV silently falls back to its own
  // defaults when a variable is missing — a 30-minute run against the wrong
  // host looks perfectly healthy until someone reads the target.
  const referenced = new Set(
    [...content.matchAll(/__ENV\.([A-Za-z_][A-Za-z0-9_]*)/g)].map((m) => m[1]),
  );
  const provided = new Set([...envNames, "CFG", "SUMMARY_OUT"]);
  const missing = [...referenced].filter((n) => !provided.has(n));
  if (missing.length) {
    tips.push({
      level: "info",
      message: `script reads __ENV ${missing.join(", ")} \u2014 not set for this run, so the script\u2019s own defaults apply; set them under environment variables to drive the run from the dashboard`,
    });
  }

  const hasHandleSummary = /export\s+function\s+handleSummary/.test(content);
  if (!hasHandleSummary) {
    tips.push({
      level: "warn",
      message:
        "no handleSummary() export found \u2014 k6\u2019s end-of-run summary (thresholds, custom metric totals) will not reach the dashboard; add one that writes JSON.stringify(data) to __ENV.SUMMARY_OUT",
    });
  } else if (!content.includes("SUMMARY_OUT")) {
    tips.push({
      level: "info",
      message:
        "handleSummary() does not write to __ENV.SUMMARY_OUT \u2014 the dashboard falls back to any summary JSON the script leaves in the run directory, but writing to that path is what guarantees it is found",
    });
  }

  if (hasHandleSummary && /stdout\s*:\s*JSON\.stringify/.test(content)) {
    tips.push({
      level: "info",
      message:
        "handleSummary() writes the whole summary to stdout \u2014 every line of it lands in this run\u2019s log; drop the stdout key (or use textSummary) to keep the log readable",
    });
  }

  if (!/\bthresholds\s*:/.test(content)) {
    tips.push({
      level: "info",
      message:
        "no options.thresholds \u2014 the run cannot fail on its own metrics; consider adding some so a bad run comes back as fail instead of pass",
    });
  }

  if (
    /constant-arrival-rate/.test(content) &&
    !/dropped_iterations/.test(content)
  ) {
    tips.push({
      level: "info",
      message:
        "constant-arrival-rate executor without a dropped_iterations threshold \u2014 exhausted VUs drop iterations silently; add thresholds: { dropped_iterations: ['count==0'] }",
    });
  }

  const customMetrics = [
    ...content.matchAll(
      /new\s+(Counter|Rate|Gauge|Trend)\s*\(\s*['"]([\w.-]+)['"]/g,
    ),
  ];
  for (const [, kind, name] of customMetrics) {
    const mentions = content.split(name).length - 1;
    if (mentions < 2) {
      tips.push({
        level: "info",
        message: `custom ${kind.toLowerCase()} '${name}' has no threshold \u2014 the dashboard will still show its final value, but nothing gates pass/fail on it`,
      });
    }
  }

  return tips;
}

function buildScriptConfig(
  cfg: RestConfig,
  ctx: RunnerContext,
  thresholds: Record<string, string[]>,
) {
  const headers: Record<string, string> = { ...cfg.headers };
  if (cfg.auth.kind === "bearer" && cfg.auth.token)
    headers.Authorization = `Bearer ${cfg.auth.token}`;
  if (cfg.auth.kind === "basic" && cfg.auth.username) {
    headers.Authorization = `Basic ${Buffer.from(`${cfg.auth.username}:${cfg.auth.password ?? ""}`).toString("base64")}`;
  }
  if (cfg.bodyType === "json" && !headers["Content-Type"])
    headers["Content-Type"] = "application/json";
  if (cfg.bodyType === "form" && !headers["Content-Type"])
    headers["Content-Type"] = "application/x-www-form-urlencoded";

  const k6Options: Record<string, unknown> = {
    insecureSkipTLSVerify: cfg.insecureSkipTlsVerify,
    thresholds,
    summaryTrendStats: ["min", "avg", "p(90)", "p(95)", "p(99)", "max"],
  };

  if (cfg.loadModel === "vus") {
    k6Options.vus = cfg.vus;
    k6Options.duration = `${cfg.vusDurationSec}s`;
  } else if (cfg.loadModel === "rate") {
    k6Options.scenarios = {
      constant_rate: {
        executor: "constant-arrival-rate",
        rate: cfg.rate,
        timeUnit: "1s",
        duration: `${cfg.rateDurationSec}s`,
        preAllocatedVUs: cfg.preAllocatedVUs,
        maxVUs: Math.max(cfg.preAllocatedVUs * 4, cfg.rate),
      },
    };
  } else {
    k6Options.stages = cfg.stages.map((s) => ({
      duration: `${s.duration}s`,
      target: s.target,
    }));
  }

  return {
    url: cfg.url,
    method: cfg.method,
    headers,
    body: cfg.bodyType === "none" ? "" : cfg.body,
    timeoutSec: cfg.timeoutSec,
    followRedirects: cfg.followRedirects,
    thinkTimeMs: cfg.thinkTimeMs,
    checks: ctx.config.checks,
    k6Options,
  };
}

export async function probe(
  bin: string,
  args: string[],
): Promise<{ available: boolean; detail: string }> {
  return new Promise((resolve) => {
    const child = spawn(bin, args, {
      shell: false,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    child.stdout.on("data", (b: Buffer) => {
      out += b.toString();
    });
    child.stderr.on("data", (b: Buffer) => {
      out += b.toString();
    });
    child.on("error", (err) =>
      resolve({
        available: false,
        detail: `${bin} not found (${err.message})`,
      }),
    );
    child.on("close", (code) =>
      resolve(
        code === 0
          ? { available: true, detail: out.split("\n")[0].trim() || bin }
          : { available: false, detail: `${bin} exited ${code}` },
      ),
    );
  });
}
