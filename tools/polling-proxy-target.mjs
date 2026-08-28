/**
 * A Socket.IO server behind a proxy that breaks HTTP long-polling while
 * leaving WebSocket upgrades alone.
 *
 * This is what a load balancer without session affinity does to Socket.IO: the
 * handshake succeeds, then the next polling request lands on a different
 * backend and the client sees "xhr poll error" while websocket-only works
 * perfectly. It is the shape of a real reported failure, and the case the
 * connection probe's websocket retry is there to identify.
 *
 *   node tools/polling-proxy-target.mjs      # proxy on 4394, real server on 4393
 */
import { createServer, request as httpRequest } from 'node:http';
import { connect } from 'node:net';
import { Server } from 'socket.io';

const PROXY_PORT = Number(process.env.PORT ?? 4394);
const UPSTREAM_PORT = Number(process.env.UPSTREAM_PORT ?? 4393);

const upstream = createServer();
const io = new Server(upstream, { cors: { origin: '*' } });
io.on('connection', (socket) => {
  socket.on('order.create', (data, ack) => {
    setTimeout(() => ack?.({ status: 'ok', id: `ord-${Math.random().toString(36).slice(2, 8)}`, echo: data }), 12);
  });
});
upstream.listen(UPSTREAM_PORT);

const proxy = createServer((req, res) => {
  if (req.url?.includes('transport=polling')) {
    res.writeHead(502, { 'content-type': 'text/plain' });
    res.end('bad gateway: this backend has no session for that sid');
    return;
  }
  const out = httpRequest(
    { port: UPSTREAM_PORT, host: '127.0.0.1', path: req.url, method: req.method, headers: req.headers },
    (up) => { res.writeHead(up.statusCode ?? 502, up.headers); up.pipe(res); },
  );
  out.on('error', () => { res.writeHead(502); res.end('bad gateway'); });
  req.pipe(out);
});

// WebSocket upgrades are tunnelled through untouched.
proxy.on('upgrade', (req, socket, head) => {
  const up = connect(UPSTREAM_PORT, '127.0.0.1', () => {
    up.write(`${req.method} ${req.url} HTTP/1.1\r\n`
      + Object.entries(req.headers).map(([k, v]) => `${k}: ${v}\r\n`).join('')
      + '\r\n');
    if (head?.length) up.write(head);
    up.pipe(socket);
    socket.pipe(up);
  });
  up.on('error', () => socket.destroy());
  socket.on('error', () => up.destroy());
});

proxy.listen(PROXY_PORT, () => console.log(
  `socket.io behind a polling-breaking proxy on ${PROXY_PORT} (upstream ${UPSTREAM_PORT})`,
));
