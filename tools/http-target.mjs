/**
 * Throwaway HTTP target for exercising the REST runner without an external
 * service. Fails every 25th request so success-rate and error buckets get hit.
 */
import http from 'node:http';

const PORT = Number(process.env.PORT ?? 4399);
let n = 0;

http.createServer((req, res) => {
  n++;
  const fail = n % 25 === 0;
  setTimeout(() => {
    res.writeHead(fail ? 500 : 200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: !fail, seq: n }));
  }, 5 + Math.random() * 40);
}).listen(PORT, '127.0.0.1', () => console.log(`http target on ${PORT}`));
