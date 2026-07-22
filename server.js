import { WebSocketServer } from 'ws';
import WebSocket from 'ws';
const PORT = process.env.PORT || 5555;
const wss = new WebSocketServer({ port: PORT });
console.log('JASS relay running on port ' + PORT);
wss.on('connection', (client) => {
  const ups = {};
  client.on('message', (raw) => {
    const { op, symbol, tf, providerSymbol } = JSON.parse(raw);
    if (op !== 'subscribe') return;
    const key = symbol + tf;
    if (ups[key]) ups[key].close();
    const up = new WebSocket(`wss://stream.binance.com:9443/ws/${providerSymbol.toLowerCase()}@kline_${tf.toLowerCase()}`);
    ups[key] = up;
    up.on('message', (m) => {
      const k = JSON.parse(m).k;
      client.send(JSON.stringify({ symbol, tf,
        candle:{ t:k.t, o:+k.o, h:+k.h, l:+k.l, c:+k.c, v:+k.v }, closed:k.x }));
    });
  });
  client.on('close', () => Object.values(ups).forEach(u => u.close()));
});
