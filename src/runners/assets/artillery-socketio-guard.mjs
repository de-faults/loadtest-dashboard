/**
 * Preloaded into every Artillery process (main and workers) via --import.
 *
 * Artillery's Socket.IO engine registers three one-shot handlers that share a
 * single callback:
 *
 *   socket.once('connect',       () => cb(null, socket));
 *   socket.once('connect_error', (err) => cb(err, null));
 *   socket.once('error',         (err) => cb(err, socket));
 *
 * A transport that fails *after* a successful connect — the target restarting,
 * a proxy closing an idle connection, a flaky network — fires 'error' once the
 * 'connect' callback has already run. async then throws "Callback was already
 * called", which kills the whole worker: every virtual user it was running
 * disappears and the report file is never written, so a 10-minute test ends
 * with no results at all.
 *
 * Guarding the callback turns that into what it should have been: one virtual
 * user seeing a connection error, counted and reported, while the test carries
 * on. Nothing else about the engine's behaviour changes.
 *
 * Upstream: artillery/dist/lib/core/engine_socketio.js, loadContextSocket().
 *
 * This runs before Artillery starts, so it must never throw — a failed patch
 * has to leave the process running exactly as it would have without it.
 */

const log = (msg) => process.stderr.write(`[ltd-socketio-guard] ${msg}\n`);

async function resolveEngine() {
  const { createRequire } = await import('node:module');
  const { pathToFileURL } = await import('node:url');
  const { dirname, join } = await import('node:path');

  // artillery's package exports do not expose the engine, so the file is
  // imported by path: resolve the package root, then reach into it.
  // A globally installed artillery is not reachable from this file's own
  // node_modules, so its binary (passed by the runner) is tried as a second root.
  const roots = [import.meta.url, process.env.LTD_ARTILLERY_MAIN].filter(Boolean);
  for (const root of roots) {
    try {
      const require = createRequire(root.startsWith('file:') ? root : pathToFileURL(root).href);
      const pkg = require.resolve('artillery/package.json');
      const file = join(dirname(pkg), 'dist', 'lib', 'core', 'engine_socketio.js');
      return await import(pathToFileURL(file).href);
    } catch { /* try the next resolution root */ }
  }
  return null;
}

try {
  const mod = await resolveEngine();
  const proto = mod?.default?.prototype;

  if (typeof proto?.loadContextSocket !== 'function') {
    // A version whose shape we do not recognise is left alone.
    if (process.env.LTD_GUARD_DEBUG) log('engine not patched: loadContextSocket not found');
  } else if (!proto.loadContextSocket.__ltdGuarded) {
    const original = proto.loadContextSocket;
    function guarded(namespace, context, cb) {
      let called = false;
      return original.call(this, namespace, context, function once(err, socket) {
        if (called) return undefined;
        called = true;
        return cb(err, socket);
      });
    }
    guarded.__ltdGuarded = true;
    proto.loadContextSocket = guarded;
    if (process.env.LTD_GUARD_DEBUG) log('engine patched');
  }
} catch (err) {
  log(`not applied: ${err?.message ?? err}`);
}
