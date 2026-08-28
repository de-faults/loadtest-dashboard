// Custom k6 script. Runs in k6's own JS runtime — full k6 API available:
// multiple scenarios/executors, custom Counter/Rate/Gauge/Trend metrics,
// per-scenario tags, etc. all work as-is.
//
// What the dashboard picks up, and from where:
//  - Live charts (RPS/VUs/latency/checks): streamed straight from k6's own
//    `--out json` points, so they work for ANY scenario/exec you define.
//  - Custom metrics + threshold verdicts: read from handleSummary()'s output
//    at the END of the run — that's why __ENV.SUMMARY_OUT below is required,
//    not optional. Skip it and the dashboard only ever shows live data, never
//    a final verdict for your own metrics.
//  - dropped_iterations (arrival-rate executors running out of VUs) is
//    surfaced as a live error the moment it happens.
//
// Two env vars are injected:
//   __ENV.CFG          path to the profile's JSON config (optional to use)
//   __ENV.SUMMARY_OUT  where to write the end-of-run summary (required)
import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate } from "k6/metrics";

const postErrors = new Rate("post_errors");
const getErrors = new Rate("get_errors");
const postRequests = new Counter("post_requests");

export const options = {
  scenarios: {
    ramp_get: {
      executor: "ramping-vus",
      exec: "getRequest",
      startVUs: 0,
      stages: [
        { duration: "10s", target: 10 },
        { duration: "20s", target: 25 },
        { duration: "5s", target: 0 },
      ],
    },
    steady_post: {
      executor: "constant-arrival-rate",
      exec: "postRequest",
      rate: 20,
      timeUnit: "1s",
      duration: "35s",
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<500"],
    http_req_failed: ["rate<0.01"],
    post_errors: ["rate<0.01"],
    get_errors: ["rate<0.01"],
    dropped_iterations: ["count==0"],
  },
};

export function getRequest() {
  const res = http.get("http://127.0.0.1:4399/api/test");
  const ok = check(res, {
    "status is 2xx": (r) => r.status >= 200 && r.status < 300,
    "body is json": (r) =>
      String(r.headers["Content-Type"] || "").includes("json"),
    "under 500ms": (r) => r.timings.duration < 500,
  });
  getErrors.add(!ok);
  sleep(0.1);
}

export function postRequest() {
  const res = http.post(
    "http://127.0.0.1:4399/api/test",
    JSON.stringify({ ping: true }),
    {
      headers: { "Content-Type": "application/json" },
    },
  );
  const ok = check(res, {
    "status is 2xx": (r) => r.status >= 200 && r.status < 300,
  });
  postRequests.add(1);
  postErrors.add(!ok);
}

// Required: this is the only way the dashboard learns k6's own threshold
// verdicts and any custom metric's final value.
export function handleSummary(data) {
  const out = {};
  out[__ENV.SUMMARY_OUT] = JSON.stringify(data);
  return out;
}
