/* eslint-disable */
/**
 * Generic k6 driver.
 *
 * Config is NOT templated into this file — it is read from a JSON file whose
 * path arrives in __ENV.CFG. Templating UI input into a script would be a
 * script-injection hole; this way the payload is inert data.
 */
import http from 'k6/http';
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

export default function () {
  const params = {
    headers: CFG.headers || {},
    timeout: (CFG.timeoutSec || 30) + 's',
    redirects: CFG.followRedirects ? 10 : 0,
  };
  const body = CFG.body && CFG.body.length ? CFG.body : null;
  const res = http.request(CFG.method || 'GET', CFG.url, body, params);
  check(res, CHECKS);
  if (CFG.thinkTimeMs > 0) sleep(CFG.thinkTimeMs / 1000);
}

export function handleSummary(data) {
  const out = {};
  out[__ENV.SUMMARY_OUT] = JSON.stringify(data);
  return out;
}
