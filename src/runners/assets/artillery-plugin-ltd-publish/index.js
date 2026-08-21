/* eslint-disable */
/**
 * Artillery output plugin — pushes interim stats to the dashboard.
 *
 * Artillery only writes `--output` once the run finishes, so a live chart needs
 * a side channel. This plugin forwards every `stats` tick to the dashboard's
 * loopback ingest endpoint.
 *
 * Loaded via ARTILLERY_PLUGIN_PATH pointing at this directory.
 */
const http = require('http');

module.exports.LEGACY_METRICS_FORMAT = false;

module.exports.Plugin = class LtdPublish {
  constructor(script, events) {
    const opts = (script.config.plugins || {})['ltd-publish'] || {};
    this.url = opts.url;
    this.runId = opts.runId;
    this.pending = 0;

    events.on('stats', (stats) => {
      const report = typeof stats.report === 'function' ? stats.report() : stats;
      this.post({ kind: 'stats', report });
    });

    events.on('done', (stats) => {
      const report = typeof stats.report === 'function' ? stats.report() : stats;
      this.post({ kind: 'done', report });
    });
  }

  post(body) {
    if (!this.url) return;
    let u;
    try { u = new URL(this.url); } catch (e) { return; }
    const payload = Buffer.from(JSON.stringify(body), 'utf8');
    const req = http.request(
      {
        hostname: u.hostname,
        port: u.port,
        path: u.pathname + u.search,
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': payload.length },
      },
      (res) => { res.resume(); },
    );
    // Never let a dashboard hiccup kill the load test.
    req.on('error', () => {});
    req.write(payload);
    req.end();
  }

  cleanup(done) {
    setTimeout(() => done(null), 200);
  }
};
