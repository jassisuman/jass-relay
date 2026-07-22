import http from 'http';
import { WebSocketServer } from 'ws';
import WebSocket from 'ws';

const PORT = process.env.PORT || 5555;
const TWELVE_KEY = process.env.TWELVE_KEY || '430199692d5d4c6baf3b4107c7d1d260';

const TD_INT = { '1m':'1min','3m':'5min','5m':'5min','15m':'15min','30m':'30min',
  '1H':'1h','2H':'2h','4H':'4h','1D':'1day','1W':'1week','1M':'1month' };

const CONN = {};
const cors = (res) => { res.setHeader('Access-Control-Allow-Origin','*'); res.setHeader('Access-Control-Allow-Headers','Content-Type'); res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS'); };
const body = (req) => new Promise(r => { let d=''; req.on('data',c=>d+=c); req.on('end',()=>{ try{r(JSON.parse(d||'{}'));}catch{r({});} }); });

const server = http.createServer(async (req,res) => {
  cors(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, 'http://x');
  res.setHeader('Content-Type','application/json');

  if (url.pathname === '/status') return res.end(JSON.stringify(CONN));

  if (url.pathname === '/connect' && req.method === 'POST') {
    const { name, key } = await body(req);
    if (!name) { res.writeHead(400); return res.end(JSON.stringify({error:'name required'})); }
    let ok = true;
    try { if (/binance/i.test(name)) { const r = await fetch('https://api.binance.com/api/v3/ping'); ok = r.ok; } } catch { ok = false; }
    const status = ok ? 'CONNECTED' : 'FAILED';
    CONN[name] = { status, hasKey: !!key, ts: Date.now() };
    res.writeHead(ok?200:502); return res.end(JSON.stringify({ name, status }));
  }
  if (url.pathname === '/test' && req.method === 'POST') {
    const { name } = await body(req);
    const t0 = Date.now(); let ok = true;
    try { if (/binance/i.test(name)) { const r = await fetch('https://api.binance.com/api/v3/ping'); ok = r.ok; } } catch { ok = false; }
    return res.end(JSON.stringify({ name, ok, latency: Date.now()-t0 }));
  }
  if (url.pathname === '/disconnect' && req.method === 'POST') {
    const { name } = await body(req); delete CONN[name];
    return res.end(JSON.stringify({ name, status:'NOT_CONFIGURED' }));
  }
  if (url.pathname === '/candles') {
    const symbol = url.searchParams.get('symbol');
    const tf = url.searchParams.get('tf');
    const providerSymbol = url.searchParams.get('providerSymbol') || '';
    try {
      if (/USDT$/i.test(providerSymbol)) {
        const r = await fetch(`https://api.binance.com/api/v3/klines?symbol=${providerSymbol}&interval=${tf.toLowerCase()}&limit=60`);
        const j = await r.json();
        const candles = j.map(k => ({ o:+k[1], h:+k[2], l:+k[3], c:+k[4] }));
        return res.end(JSON.stringify({ symbol, tf, candles }));
      } else {
        const interval = TD_INT[tf] || '15min';
        const r = await fetch(`https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(symbol)}&interval=${interval}&outputsize=60&order=ASC&apikey=${TWELVE_KEY}`);
        const j = await r.json();
        const candles = (j.values||[]).map(v => ({ o:+v.open, h:+v.high, l:+v.low, c:+v.close }));
        return res.end(JSON.stringify({ symbol, tf, candles }));
      }
    } catch { return res.end(JSON.stringify({ symbol, tf, candles:[] })); }
  }
  res.writeHead(404); res.end('{}');
});

const wss = new WebSocketServer({ server });
server.listen(PORT, () => console.log('JASS relay + API on port ' + PORT));

wss.on('connection', (client) => {
  const ups = {};
  const polls = {};
  client.on('message', async (raw) => {
    let d; try { d = JSON.parse(raw); } catch { return; }
    if (d.op !== 'subscribe') return;
    const { symbol, tf, providerSymbol } = d;
    const key = symbol + tf;
    if (ups[key]) { ups[key].close(); delete ups[key]; }
    if (polls[key]) { clearInterval(polls[key]); delete polls[key]; }
    const isCrypto = /USDT$/i.test(providerSymbol || '');
    if (isCrypto) {
      const up = new WebSocket(`wss://stream.binance.com:9443/ws/${providerSymbol.toLowerCase()}@kline_${tf.toLowerCase()}`);
      ups[key] = up;
      up.on('message', (m) => { try { const k = JSON.parse(m).k; client.send(JSON.stringify({ symbol, tf, candle:{ t:k.t, o:+k.o, h:+k.h, l:+k.l, c:+k.c, v:+k.v }, closed:k.x })); } catch {} });
    } else {
      const interval = TD_INT[tf] || '15min';
      const fetchSeries = async () => {
        try {
          const u = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(symbol)}&interval=${interval}&outputsize=60&order=ASC&apikey=${TWELVE_KEY}`;
          const r = await fetch(u); const j = await r.json();
          if (!j.values) return;
          const candles = j.values.map(v => ({ o:+v.open, h:+v.high, l:+v.low, c:+v.close }));
          client.send(JSON.stringify({ symbol, tf, candles }));
        } catch {}
      };
      fetchSeries();
      polls[key] = setInterval(fetchSeries, 5000);
    }
  });
  client.on('close', () => { Object.values(ups).forEach(u => u.close()); Object.values(polls).forEach(t => clearInterval(t)); });
});
