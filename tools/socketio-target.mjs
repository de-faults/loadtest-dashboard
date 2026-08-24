/**
 * Throwaway Socket.IO target: echoes on an ack callback and pushes a reply
 * event, so both the acknowledge and listen paths can be exercised.
 */
import { createServer } from 'node:http';
import { Server } from 'socket.io';

const PORT = Number(process.env.PORT ?? 4396);
const io = new Server(createServer().listen(PORT), { cors: { origin: '*' } });

io.on('connection', (socket) => {
  socket.on('order.create', (data, ack) => {
    const reply = { status: 'ok', id: `ord-${Math.random().toString(36).slice(2, 8)}`, echo: data };
    setTimeout(() => {
      if (typeof ack === 'function') ack(reply);
      socket.emit('order.created', reply);
    }, 5 + Math.random() * 20);
  });
});

console.log(`socket.io target on ${PORT}`);
