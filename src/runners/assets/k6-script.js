/* eslint-disable */
/**
 * Generic k6 driver.
 *
 * Config is NOT templated into this file — it is read from a JSON file whose
 * path arrives in __ENV.CFG. Templating UI input into a script would be a
 * script-injection hole; this way the payload is inert data.
 */
import http from 'k6/http';
import encoding from 'k6/encoding';
import { check, sleep } from 'k6';

const CFG = JSON.parse(open(__ENV.CFG));

export const options = CFG.k6Options;

function jsonPath(obj, path) {
  let cur = obj;
  for (const part of String(path).split('.')) {
    if (cur == null) return undefined;
    cur = cur[part];
  }
  return cur;
}

/**
 * Status matcher accepting a union, not just one code.
 *
 *   200            single
 *   200|201        union (comma also works: 200,201)
 *   2xx            wildcard class
 *   200-204        inclusive range
 *   200|2xx,300-302  any mix of the above
 *
 * Anything unparseable becomes a never-match rather than a silent always-pass,
 * so a typo shows up as a failing check instead of a green run that proved nothing.
 */
function parseStatusMatcher(spec) {
  var parts = String(spec).split(/[|,]/);
  var tests = [];
  for (var i = 0; i < parts.length; i++) {
    var p = parts[i].replace(/^\s+|\s+$/g, '');
    if (!p) continue;
    var range = /^([1-5]\d{2})\s*-\s*([1-5]\d{2})$/.exec(p);
    if (range) { tests.push(makeRange(Number(range[1]), Number(range[2]))); continue; }
    var wild = /^([1-5])[xX]{2}$/.exec(p);
    if (wild) { tests.push(makeRange(Number(wild[1]) * 100, Number(wild[1]) * 100 + 99)); continue; }
    var exact = /^([1-5]\d{2})$/.exec(p);
    if (exact) { tests.push(makeExact(Number(exact[1]))); continue; }
    tests.push(function () { return false; });
  }
  if (tests.length === 0) return function () { return false; };
  return function (status) {
    for (var j = 0; j < tests.length; j++) if (tests[j](status)) return true;
    return false;
  };
}

function makeRange(lo, hi) {
  return function (status) { return status >= lo && status <= hi; };
}

function makeExact(v) {
  return function (status) { return status === v; };
}

function buildChecks() {
  const out = {};
  for (const spec of CFG.checks || []) {
    const value = spec.value;
    const path = spec.path;
    switch (spec.kind) {
      case 'status': {
        // Compiled once at init, not per request.
        const matches = parseStatusMatcher(value);
        out[spec.name] = (r) => matches(Number(r.status));
        break;
      }
      case 'body_contains':
        out[spec.name] = (r) => String(r.body || '').indexOf(value) !== -1;
        break;
      case 'regex':
        out[spec.name] = (r) => new RegExp(value).test(String(r.body || ''));
        break;
      case 'json_path':
        out[spec.name] = (r) => {
          try { return String(jsonPath(r.json(), path)) === String(value); }
          catch (e) { return false; }
        };
        break;
      case 'latency_under':
        out[spec.name] = (r) => r.timings.duration < Number(value);
        break;
      default:
        break;
    }
  }
  return out;
}

const CHECKS = buildChecks();

/**
 * Error-payload capture.
 *
 * A status code alone rarely explains a failing load test — the target's own
 * error body does. k6's JSON metric stream carries only tags, so the body is
 * handed to the dashboard out-of-band: one marker line per new failure kind,
 * which the runner parses out of the log instead of showing it raw.
 *
 * Bounded on purpose: first occurrence of each kind, at most `maxSamples` per
 * VU, body truncated to `maxChars`. A 500-VU run must not turn its own log
 * into the payload.
 *
 * The payload is base64 so k6's own log formatter cannot mangle it: console
 * output is wrapped as `msg="…"` with Go-style escaping, which turns raw JSON
 * containing quotes or newlines into something the runner cannot parse back.
 */
const ERR_MARK = '@@LTD_ERRBODY@@';
const ERR_CFG = CFG.errorBody || {};
const ERR_MAX_CHARS = Number(ERR_CFG.maxChars) || 2000;
const ERR_MAX_SAMPLES = Number(ERR_CFG.maxSamples) || 2;
const ERR_MAX_HEADERS = Number(ERR_CFG.maxHeaders) || 40;
const ERR_HEADER_CHARS = Number(ERR_CFG.headerChars) || 300;
const ERR_ENABLED = ERR_CFG.enabled !== false;
const errSeen = {};
let errCount = 0;

function errorKind(res) {
  // Same naming the runner derives from the metric stream, so the body lands
  // on the bucket the failure was counted in.
  if (res.error_code) return 'http_' + res.error_code;
  return 'status_' + (res.status || 'unknown');
}

function headerOf(res, name) {
  const h = res.headers || {};
  return h[name] || h[name.toLowerCase()] || '';
}

/**
 * Response headers, capped both in count and per-value length. A proxy or a
 * gateway usually explains a failure in its headers long before the body does.
 * Values are redacted on the runner side, not here — see maskErrorHeaders.
 */
function collectHeaders(res) {
  const out = {};
  const src = res.headers || {};
  let n = 0;
  for (const name in src) {
    if (n >= ERR_MAX_HEADERS) break;
    n++;
    const v = String(src[name]);
    out[name] = v.length > ERR_HEADER_CHARS ? v.slice(0, ERR_HEADER_CHARS) : v;
  }
  return out;
}

function reportErrorBody(res) {
  if (!ERR_ENABLED || errCount >= ERR_MAX_SAMPLES) return;
  const kind = errorKind(res);
  if (errSeen[kind]) return;
  errSeen[kind] = true;
  errCount++;
  const raw = res.body == null ? '' : String(res.body);
  const truncated = raw.length > ERR_MAX_CHARS;
  // Characters, not bytes: k6 hands the script a decoded string and does not
  // carry the target's Content-Length through, so a byte count would be a guess.
  const payload = JSON.stringify({
    kind: kind,
    status: res.status,
    error: res.error || '',
    contentType: headerOf(res, 'Content-Type'),
    headers: collectHeaders(res),
    // Who answered. A gateway VIP is not the service host, and after a redirect
    // the final URL is not the one the profile targets.
    remoteIp: res.remote_ip || '',
    remotePort: res.remote_port || 0,
    proto: res.proto || '',
    finalUrl: res.url || '',
    chars: raw.length,
    truncated: truncated,
    body: truncated ? raw.slice(0, ERR_MAX_CHARS) : raw,
  });
  console.log(ERR_MARK + encoding.b64encode(payload));
}

export default function () {
  const params = {
    headers: CFG.headers || {},
    timeout: (CFG.timeoutSec || 30) + 's',
    redirects: CFG.followRedirects ? 10 : 0,
  };
  const body = CFG.body && CFG.body.length ? CFG.body : null;
  const res = http.request(CFG.method || 'GET', CFG.url, body, params);
  // k6's default expected response is 2xx/3xx; anything else — including a
  // status of 0 from a transport error — is what the dashboard counts failed.
  if (!(res.status >= 200 && res.status < 400)) reportErrorBody(res);
  check(res, CHECKS);
  if (CFG.thinkTimeMs > 0) sleep(CFG.thinkTimeMs / 1000);
}

export function handleSummary(data) {
  const out = {};
  out[__ENV.SUMMARY_OUT] = JSON.stringify(data);
  return out;
}
