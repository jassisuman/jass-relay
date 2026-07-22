import { WebSocketServer } from 'ws';
import WebSocket from 'ws';

const PORT = process.env.PORT || 5555;
const TWELVE_KEY = process.env.TWELVE_KEY || 'P430199692d5d4c6baf3b4107c7d1d260';

// tf -> Twelve Data interval
const TD_INT = { '1m':'1min','3m':'5min','5m':'5min','15m':'15min','30m':'30min',
  '1H':'1h','2H':'2h','4H':'4h','1D':'1day','1W':'1week','1M':'1month' };

const wss = new WebSocketServer({ port: PORT });
console.log('JASS relay running on port ' + PORT);

wss.on('connection', (client) => {
  const ups = {};      // binance sockets
  const polls = {};     // twelve-data timers

  client.on('message', async (raw) => {
    let d; try { d = JSON.parse(raw); } catch { return; }
    if (d.op !== 'subscribe') return;
    const { symbol, tf, providerSymbol } = d;
    const key = symbol + tf;

    // clean any existing feed for this key
    if (ups[key]) { ups[key].close(); delete ups[key]; }
    if (polls[key]) { clearInterval(polls[key]); delete polls[key]; }

    const isCrypto = /USDT$/i.test(providerSymbol || '');

    if (isCrypto) {
      // ---- Binance live WebSocket (per-tick) ----
      const up = new WebSocket(`wss://stream.binance.com:9443/ws/${providerSymbol.toLowerCase()}@kline_${tf.toLowerCase()}`);
      ups[key] = up;
      up.on('message', (m) => {
        try {
          const k = JSON.parse(m).k;
          client.send(JSON.stringify({ symbol, tf,
            candle:{ t:k.t, o:+k.o, h:+k.h, l:+k.l, c:+k.c, v:+k.v }, closed:k.x }));
        } catch {}
      });
    } else {
      // ---- Twelve Data (gold / forex / stocks) via polling ----
      const interval = TD_INT[tf] || '15min';
      const fetchSeries = async () => {
        try {
          const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(symbol)}&interval=${interval}&outputsize=60&order=ASC&apikey=${TWELVE_KEY}`;
          const r = await fetch(url);
          const j = await r.json();
          if (!j.values) return; // e.g. rate limit or bad symbol
          const candles = j.values.map(v => ({ o:+v.open, h:+v.high, l:+v.low, c:+v.close }));
          client.send(JSON.stringify({ symbol, tf, candles }));
        } catch {}
      };
      fetchSeries();
      polls[key] = setInterval(fetchSeries, 5000); // refresh every 5s
    }
  });

  client.on('close', () => {
    Object.values(ups).forEach(u => u.close());
    Object.values(polls).forEach(t => clearInterval(t));
  });
});
