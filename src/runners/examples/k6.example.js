// Custom k6 script. Runs in k6's own JS runtime — full k6 API available.
//
// The dashboard still collects every metric k6 emits (http_req_duration, vus,
// checks, ...), so the live charts, latency profile and CSV report work exactly
// as they do for a UI-built test. Your own options.thresholds are evaluated by
// k6; the profile's thresholds are evaluated on top of the collected metrics.
//
// Two env vars are injected:
//   __ENV.CFG          path to the profile's JSON config (optional to use)
//   __ENV.SUMMARY_OUT  where to write the end-of-run summary
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 10 },
    { duration: '20s', target: 25 },
    { duration: '5s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const res = http.get('http://127.0.0.1:4399/api/test');

  check(res, {
    'status is 2xx': (r) => r.status >= 200 && r.status < 300,
    'body is json': (r) => String(r.headers['Content-Type'] || '').includes('json'),
    'under 500ms': (r) => r.timings.duration < 500,
  });

  sleep(0.1);
}

// Keep this so the dashboard can read k6's own threshold verdicts.
export function handleSummary(data) {
  const out = {};
  out[__ENV.SUMMARY_OUT] = JSON.stringify(data);
  return out;
}
