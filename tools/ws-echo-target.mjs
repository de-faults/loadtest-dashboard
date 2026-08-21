import pkg from 'ws';
const WSServer = pkg.WebSocketServer ?? pkg.Server;
const wss = new WSServer({ port: 4398 });
wss.on('connection', (ws) => {
  ws.on('message', (data) => {
    setTimeout(() => { try { ws.send(`echo:${data}`); } catch { /* closed */ } }, 5 + Math.random() * 25);
  });
});
console.log('ws target on 4398');
