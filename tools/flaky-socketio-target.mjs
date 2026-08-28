/**
 * Socket.IO target that accepts connections normally, then drops every live
 * transport a few seconds in and keeps dropping them.
 *
 * This is the condition a real target produces when it restarts, is rolled, or
 * sits behind a proxy that closes idle connections — and the one that used to
 * kill an Artillery worker outright (see src/runners/assets/artillery-socketio-guard.mjs).
 * Keep it around: it is the only reliable way to reproduce that failure.
 *
 *   node tools/flaky-socketio-target.mjs        # port 4397, drops after 4s
 *   KILL_AFTER_MS=10000 PORT=5000 node tools/flaky-socketio-target.mjs
 */
import { createServer } from 'node:http';
import { Server } from 'socket.io';

const PORT = Number(process.env.PORT ?? 4397);
const KILL_AFTER = Number(process.env.KILL_AFTER_MS ?? 4000);
const http = createServer();
const raw = new Set();
http.on('connection', (s) => { raw.add(s); s.on('close', () => raw.delete(s)); });
const io = new Server(http, { cors: { origin: '*' } });

io.on('connection', (socket) => {
  socket.on('order.create', (data, ack) => {
    setTimeout(() => { if (typeof ack === 'function') ack({ status: 'ok' }); }, 10);
  });
});
http.listen(PORT, () => console.log(`flaky socket.io target on ${PORT}`));

setTimeout(() => {
  console.log(`destroying ${raw.size} live sockets`);
  for (const s of raw) s.destroy();
  setInterval(() => { for (const s of raw) s.destroy(); }, 300);
}, KILL_AFTER);
