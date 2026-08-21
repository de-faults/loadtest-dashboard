import Fastify from 'fastify';
import cors from '@fastify/cors';
import fastifyStatic from '@fastify/static';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { assertSafeBinding, BIND, PORT, TOKEN } from './config.ts';
import { registerRoutes } from './api/routes.ts';
import { stopAll } from './runners/manager.ts';
import { stopMonitor } from './kafka/monitor.ts';

const HERE = dirname(fileURLToPath(import.meta.url));
const WEB_DIST = join(HERE, '..', 'web', 'dist');

assertSafeBinding();

const app = Fastify({ logger: false, bodyLimit: 8 * 1024 * 1024 });

await app.register(cors, { origin: true });
registerRoutes(app);

if (existsSync(WEB_DIST)) {
  await app.register(fastifyStatic, { root: WEB_DIST });
  // SPA fallback: anything not an API route serves index.html.
  app.setNotFoundHandler((req, reply) => {
    if (req.url.startsWith('/api') || req.url.startsWith('/_ingest') || req.url === '/events') {
      return reply.code(404).send({ error: 'not found' });
    }
    return reply.sendFile('index.html');
  });
}

await app.listen({ port: PORT, host: BIND });

const bar = '─'.repeat(58);
console.log(`\n${bar}`);
console.log('  LOADTEST DASHBOARD');
console.log(bar);
console.log(`  UI      : http://${BIND === '0.0.0.0' ? 'localhost' : BIND}:${PORT}`);
console.log(`  Static  : ${existsSync(WEB_DIST) ? WEB_DIST : 'not built — run `npm run build` or `npm run dev:web`'}`);
console.log(`  Auth    : ${TOKEN ? 'token required' : 'none (loopback only)'}`);
console.log(`${bar}\n`);

let shuttingDown = false;
async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\n[${signal}] stopping runs and closing…`);
  // Orphaned k6/artillery children would keep hammering the target.
  stopAll();
  stopMonitor();
  await new Promise((r) => setTimeout(r, 500));
  await app.close();
  process.exit(0);
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));
