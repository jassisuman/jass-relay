mport http from 'http';
import { WebSocketServer } from 'ws';
import WebSocket from 'ws';
import ccxt from 'ccxt';

const PORT = process.env.PORT || 5555;
const TWELVE_KEY = process.env.TWELVE_KEY || '430199692d5d4c6baf3b4107c7d1d260';

const TD_INT = { '1m':'1min','3m':'5min','5m':'5min','15m':'15min','30m':'30min',
  '1H':'1h','2H':'2h','4H':'4h','1D':'1day','1W':'1week','1M':'1month' };

// name (as shown in the dashboard) -> ccxt exchange id
const CCXT_ID = { 'Binance':'binance','Bybit':'bybit','OKX':'okx','Kraken':'kraken','Coinbase':'coinbase','KuCoin':'kucoin' };

const CONN = {};   // name -> { status, key, secret, client }
const cors = (res) => { res.setHeader('Access-Control-Allow-Origin','*'); res.setHeader('Access-Control-Allow-Headers','Content-Type'); res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS'); };
const body = (req) => new Promise(r => { let d=''; req.on('data',c=>d+=c); req.on('end',()=>{ try{r(JSON.parse(d||'{}'));}catch{r({});} }); });

function makeClient(name, key, secret) {
  const id = CCXT_ID[name]; if (!id) return null;
  const Ex = ccxt[id];
  return new Ex({ apiKey:key, secret, enableRateLimit:true });
}

const server = http.createServer(async (req,res) => {
  cors(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, 'http://x');
  res.setHeader('Content-Type','application/json');

  if (url.pathname === '/status') {
    const out = {}; Object.keys(CONN).forEach(n => out[n] = { status:CONN[n].status });
    return res.end(JSON.stringify(out));
  }

  if (url.pathname === '/connect' && req.method === 'POST') {
    const { name, key, secret } = await body(req);
    if (!name) { res.writeHead(400); return res.end(JSON.stringify({error:'name required'})); }
    let status = 'CONNECTED';
    try {
      const client = makeClient(name, key, secret);
      if (client && key) { await client.fetchBalance(); }   // real auth check
      CONN[name] = { status, key, secret, client };
    } catch (e) { status = 'FAILED'; CONN[name] = { status }; }
    res.writeHead(status==='FAILED'?502:200); return res.end(JSON.stringify({ name, status }));
  }

  if (url.pathname === '/test' && req.method === 'POST') {
    const { name } = await body(req);
    const t0 = Date.now(); let ok = true;
    try { const c = CONN[name]; if (c?.client) await c.client.fetchBalance(); else { const r = await fetch('https://api.binance.com/api/v3/ping'); ok = r.ok; } }
    catch { ok = false; }
    return res.end(JSON.stringify({ name, ok, latency: Date.now()-t0 }));
  }

  if (url.pathname === '/disconnect' && req.method === 'POST') {
    const { name } = await body(req); delete CONN[name];
    return res.end(JSON.stringify({ name, status:'NOT_CONFIGURED' }));
  }

  if (url.pathname === '/account') {
    const name = url.searchParams.get('name');
    try {
      const c = CONN[name]; if (!c?.client) return res.end(JSON.stringify({ equity:0, pnl:0, positions:[] }));
      const bal = await c.client.fetchBalance();
      const equity = +(bal.total?.USDT ?? bal.total?.USD ?? Object.values(bal.total||{}).reduce((a,b)=>a+(+b||0),0)) || 0;
      let positions = [];
      try {
        const ps = await c.client.fetchPositions();
        positions = (ps||[]).filter(p => +p.contracts>0 || +p.notional>0).map(p => ({
          symbol:p.symbol, side:(p.side||'').toUpperCase()==='LONG'?'LONG':'SHORT',
          size:p.contracts ?? p.contractSize ?? '', entry:p.entryPrice, mark:p.markPrice,
          pnl:+p.unrealizedPnl||0, pnlPct:+p.percentage||0, stop:p.stopLossPrice||'—'
        }));
      } catch {}
      const pnl = positions.reduce((a,p)=>a+(+p.pnl||0),0);
      return res.end(JSON.stringify({ equity, pnl, positions }));
    } catch { return res.end(JSON.stringify({ equity:0, pnl:0, positions:[] })); }
  }

  if (url.pathname === '/candles') {
    const symbol = url.searchParams.get('symbol');
    const tf = url.searchParams.get('tf');
    const providerSymbol = url.searchParams.get('providerSymbol') || '';
    const limit = Math.min(parseInt(url.searchParams.get('limit'))||60, 1000);
    try {
      if (/USDT$/i.test(providerSymbol)) {
        const r = await fetch(`https://api.binance.com/api/v3/klines?symbol=${providerSymbol}&interval=${tf.toLowerCase()}&limit=${limit}`);
        const j = await r.json();
        return res.end(JSON.stringify({ symbol, tf, candles: j.map(k => ({ o:+k[1], h:+k[2], l:+k[3], c:+k[4] })) }));
      } else {
        const interval = TD_INT[tf] || '15min';
        const r = await fetch(`https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(symbol)}&interval=${interval}&outputsize=${limit}&order=ASC&apikey=${TWELVE_KEY}`);
        const j = await r.json();
        return res.end(JSON.stringify({ symbol, tf, candles: (j.values||[]).map(v => ({ o:+v.open, h:+v.high, l:+v.low, c:+v.close })) }));
      }
    } catch { return res.end(JSON.stringify({ symbol, tf, candles:[] })); }
  }

  res.writeHead(404); res.end('{}');
});

const wss = new WebSocketServer({ server });
server.listen(PORT, () => console.log('JASS relay + API on port ' + PORT));

wss.on('connection', (client) => {
  const ups = {}, polls = {};
  client.on('message', async (raw) => {
    let d; try { d = JSON.parse(raw); } catch { return; }
    if (d.op !== 'subscribe') return;
    const { symbol, tf, providerSymbol } = d;
    const key = symbol + tf;
    if (ups[key]) { ups[key].close(); delete ups[key]; }
    if (polls[key]) { clearInterval(polls[key]); delete polls[key]; }
    if (/USDT$/i.test(providerSymbol || '')) {
      const up = new WebSocket(`wss://stream.binance.com:9443/ws/${providerSymbol.toLowerCase()}@kline_${tf.toLowerCase()}`);
      ups[key] = up;
      up.on('message', (m) => { try { const k = JSON.parse(m).k; client.send(JSON.stringify({ symbol, tf, candle:{ t:k.t, o:+k.o, h:+k.h, l:+k.l, c:+k.c, v:+k.v }, closed:k.x })); } catch {} });
    } else {
      const interval = TD_INT[tf] || '15min';
      const fetchSeries = async () => {
        try { const u = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(symbol)}&interval=${interval}&outputsize=60&order=ASC&apikey=${TWELVE_KEY}`;
          const r = await fetch(u); const j = await r.json(); if (!j.values) return;
          client.send(JSON.stringify({ symbol, tf, candles: j.values.map(v => ({ o:+v.open, h:+v.high, l:+v.low, c:+v.close })) })); } catch {}
      };
      fetchSeries(); polls[key] = setInterval(fetchSeries, 5000);
    }
  });
  client.on('close', () => { Object.values(ups).forEach(u => u.close()); Object.values(polls).forEach(t => clearInterval(t)); });
});
